package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.*;

public class Neo4jDatabase extends AbstractDatabase {
    // db connection info
    String url = "bolt://localhost:7687";
    //	            + "?zeroDateTimeBehavior=convertToNull&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    String login = "neo4j";
    String password = "root";
    Driver driver;

    public Neo4jDatabase () {
        System.out.println("init connection");
		this.driver = GraphDatabase.driver(this.url, AuthTokens.basic(this.login, this.password));
    }

    @Override
    public List<Movie> getAllMovies () {
        try (Session session = this.driver.session()) {
            final String statement = "MATCH (m:Movie)-[:CATEGORIZED_AS]->(g:Genre) RETURN m, collect(g) as genres";
            return session.readTransaction(tx -> tx.run(statement, Map.of())).list(record -> {
                Value movieNode = record.get("m");
                List<Genre> genres = record.get("genres")
                        .asList(node -> new Genre(node.get("id").asInt(), node.get("name").asString()));

                return new Movie(movieNode.get("id").asInt(), movieNode.get("title").asString(), genres);
            });
        }
    }

    @Override
    public List<Movie> getMoviesRatedByUser (int userId) {
        try (Session session = this.driver.session()) {
            String statement = "MATCH (u:User {id:" + userId
                    + "})-[r]->(m:Movie) -[:CATEGORIZED_AS]->(g:Genre) WHERE TYPE(r) = \"RATED\" RETURN m, collect(g) as genres";
            return session.readTransaction(tx -> tx.run(statement, Map.of())).list(record -> {
                Value movieNode = record.get("m");
                List<Genre> genres = record.get("genres")
                        .asList(node -> new Genre(node.get("id").asInt(), node.get("name").asString()));

                return new Movie(movieNode.get("id").asInt(), movieNode.get("title").asString(), genres);
            });
        }
    }

    @Override
    public List<Rating> getRatingsFromUser (int userId) {
        try (Session session = this.driver.session()) {
            String statement = "MATCH (u:User {id:" + userId
                    + "})-[r]->(m:Movie) -[:CATEGORIZED_AS]->(g:Genre) WHERE TYPE(r) = \"RATED\" RETURN m, r, collect(g) as genres";
            return session.readTransaction(tx -> tx.run(statement, Map.of())).list(record -> {
                Value movieNode = record.get("m");
                Value ratedNode = record.get("r");
                List<Genre> genres = record.get("genres")
                        .asList(node -> new Genre(node.get("id").asInt(), node.get("name").asString()));
                Movie movie = new Movie(movieNode.get("id").asInt(), movieNode.get("title").asString(), genres);
                return new Rating(movie, userId, ratedNode.get("note").asInt());
            });
        }
    }

    @Override
    public void addOrUpdateRating (Rating rating) {
        try (Session session = this.driver.session()) {
            String statement = "MATCH (u:User {id:" + rating.getUserId() + "}),(m:Movie {id:" + rating.getMovieId()
                    + "}) " + "MERGE (u)-[r:RATED]->(m)" + "ON MATCH SET r = {note:" + rating.getScore() + ",timestamp:"
                    + System.currentTimeMillis() + "}" + "ON CREATE SET r = {note:" + rating.getScore() + ",timestamp:"
                    + System.currentTimeMillis() + "}" + "RETURN r";
            System.out.println(statement);
            session.readTransaction(tx -> tx.run(statement));
        }
        // String statement = "MATCH (u:User {id:"+
        // rating.getUserId()+"})-[r:RATED]->(m:Movie {id:"+ rating.getMovieId()+"}) SET
        // r = {note:"+rating.getScore()+",timestamp:"+ System.currentTimeMillis()+"}
        // RETURN r";
    }

    @Override
    public List<Rating> processRecommendationsForUser (int userId, int processingMode) {
        try (Session session = this.driver.session()) {

            String statement = this.getStatementForRecom(userId, processingMode);
            return session.readTransaction(tx -> tx.run(statement, Map.of())).list(record -> {
                Value movieNode = record.get("movie");
                Value ratedNode = record.get("rated");
                List<Genre> genres = null;
                Movie movie = new Movie(movieNode.get("id").asInt(), movieNode.get("title").asString(), genres);
                return new Rating(movie, record.get("watched_by").asInt(), ratedNode.asInt());
            });
        }
        // TODO: process recommendations for specified user exploiting other users
        // ratings
        // use different methods depending on processingMode parameter

    }

    public String getStatementForRecom (int userId, int processingMode) {
        String statement = "";
        switch (processingMode) {
            case 1:
                statement = "MATCH (target_user:User {id :" + userId + "})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User) " +
                        "WITH other_user, count(distinct m.title) AS num_common_movies, target_user " +
                        "ORDER BY num_common_movies DESC " +
                        "LIMIT 1 " +
                        "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie) " +
                        "WHERE NOT ((target_user)-[:RATED]->(m2)) " +
                        "RETURN m2 AS movie, rat_other_user AS rated, other_user.id AS watched_by " +
                        "ORDER BY rat_other_user.note DESC ";
                break;
            case 2:
                statement = "MATCH (target_user:User {id :" + userId + "})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User) " +
                        "WITH other_user, count(distinct m.title) AS num_common_movies, target_user " +
                        "ORDER BY num_common_movies DESC " +
                        "LIMIT 5 " +
                        "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie) " +
                        "WHERE NOT ((target_user)-[:RATED]->(m2)) " +
                        "RETURN m2 AS movie, toInteger(avg(rat_other_user.note)) AS rated, count(other_user.id) AS watched_by " +
                        "ORDER BY rated DESC, watched_by DESC";
                break;
            case 3:
                statement = "MATCH (target_user:User {id :" + userId + "})-[r1:RATED]->(m:Movie) <-[r2:RATED]-(other_user:User) " +
                        "WITH other_user, count(distinct m.title) AS num_common_movies, target_user,  (4-abs(r1.note - r2.note)) as score" +
                        "ORDER BY num_common_movies DESC " +
                        "LIMIT 5 " +
                        "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie) " +
                        "WHERE NOT ((target_user)-[:RATED]->(m2)) " +
                        "RETURN m2 AS movie, toInteger(avg(rat_other_user.note)) AS rated, count(other_user.id) AS watched_by, avg(score) " +
                        "ORDER BY avg(score) desc, rated DESC, watched_by DESC";
            default:
                statement = "MATCH (target_user:User {id :" + userId + "})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User) " +
                        "WITH other_user, count(distinct m.title) AS num_common_movies, target_user " +
                        "ORDER BY num_common_movies DESC " +
                        "LIMIT 1 " +
                        "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie) " +
                        "WHERE NOT ((target_user)-[:RATED]->(m2)) " +
                        "RETURN m2 AS movie, rat_other_user AS rated, other_user.id AS watched_by " +
                        "ORDER BY rat_other_user.note DESC ";
                break;
        }
        return statement;
    }
}
