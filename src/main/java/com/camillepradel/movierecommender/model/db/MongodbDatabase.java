package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.*;

import java.util.*;

public class MongodbDatabase extends AbstractDatabase {
    DB db = null;

    public MongodbDatabase () {
        try {
            /* Connect to MongoDB */
            MongoClient mongo = new MongoClient("localhost", 27017);

            /* Get database */
            this.db = mongo.getDB("MovieRecommenderDB");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Genre> getListGenre (int movieID) {
        List<Genre> genreList = new ArrayList<Genre>();
        if (this.db != null) {
            try {
                DBCollection mov_genreCollection = this.db.getCollection("mov_genre");
                DBCollection genresCollection = this.db.getCollection("genres");

                //Recupere les genre de movieID
                BasicDBObject mov_genreFilter = new BasicDBObject();
                mov_genreFilter.put("mov_id", movieID);
                DBCursor cursor = mov_genreCollection.find(mov_genreFilter);

                while (cursor.hasNext()) {
                    DBObject mov_genre = cursor.next();

                    //Recupere l'id du genre
                    BasicDBObject genreFilter = new BasicDBObject();
                    genreFilter.put("id", Integer.parseInt(mov_genre.get("genre").toString()));
                    DBObject currentGenre = genresCollection.findOne(genreFilter);

                    genreList.add(new Genre(Integer.parseInt(currentGenre.get("id").toString()), currentGenre.get("name").toString()));
                }
                return genreList;
            } catch (Exception e) {
                System.out.println("Erreur lors de la creation de la liste des genre du movie : " + movieID);
                e.printStackTrace();
            }
        }
        return null;
    }

    public Movie generateMovie (DBObject currentMovie) {
        try {
            int movieId = Integer.parseInt(currentMovie.get("id").toString());
            String movieTitle = currentMovie.get("title").toString();
            List<Genre> listeGenre = this.getListGenre(movieId);
            Movie movie = new Movie(movieId, movieTitle, listeGenre);
            return movie;
        } catch (NumberFormatException e) {
            System.out.println("Erreur création film lors du parseInt");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Movie> getAllMovies () {
        List<Movie> movies = new LinkedList<Movie>();
        if (this.db != null) {
            try {
                DBCollection moviesCollection = this.db.getCollection("movies");
                DBCursor cursor = moviesCollection.find();
                while (cursor.hasNext()) {
                    DBObject currentMovie = cursor.next();
                    movies.add(this.generateMovie(currentMovie));
                }
            } catch (Exception e) {
                System.out.println("Erreur getAllMovies()");
                e.printStackTrace();
            }
        }
        return movies;
    }

    @Override
    public List<Movie> getMoviesRatedByUser (int userId) {
        List<Movie> movies = new LinkedList<Movie>();
        if (this.db != null) {
            try {
                DBCollection ratingsCollection = this.db.getCollection("ratings");
                DBCollection moviesCollection = this.db.getCollection("movies");

                //Récupère les infos de userId
                BasicDBObject userFilter = new BasicDBObject();
                userFilter.put("user_id", userId);
                DBCursor cursor = ratingsCollection.find(userFilter);

                while (cursor.hasNext()) {
                    DBObject currentRating = cursor.next();
                    //Récupère les movies de userId
                    BasicDBObject ratingMoviesFilter = new BasicDBObject();
                    ratingMoviesFilter.put("id", Integer.parseInt(currentRating.get("mov_id").toString()));
                    DBObject currentMovie = moviesCollection.findOne(ratingMoviesFilter);
                    movies.add(this.generateMovie(currentMovie));
                }
                return movies;
            } catch (Exception e) {
                System.out.println("Erreur getMoviesRatedByUser(" + userId + ")");
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public List<Rating> getRatingsFromUser (int userId) {
        List<Rating> ratings = new LinkedList<Rating>();
        if (this.db != null) {
            try {
                DBCollection ratingsCollection = this.db.getCollection("ratings");
                DBCollection moviesCollection = this.db.getCollection("movies");

                //Récupère les infos de userId
                BasicDBObject userFilter = new BasicDBObject();
                userFilter.put("user_id", userId);
                DBCursor cursor = ratingsCollection.find(userFilter);

                while (cursor.hasNext()) {
                    DBObject currentRating = cursor.next();
                    //Récupère les movies de userId
                    BasicDBObject ratingMoviesFilter = new BasicDBObject();
                    ratingMoviesFilter.put("id", Integer.parseInt(currentRating.get("mov_id").toString()));
                    DBObject currentMovie = moviesCollection.findOne(ratingMoviesFilter);
                    ratings.add(new Rating(this.generateMovie(currentMovie), userId, Integer.parseInt(currentRating.get("rating").toString())));
                }
                return ratings;
            } catch (Exception e) {
                System.out.println("Erreur getRatingsFromUser(" + userId + ")");
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void addOrUpdateRating (Rating rating) {

        if (this.db != null) {
            try {
                DBCollection ratingsCollection = this.db.getCollection("ratings");

                //Récupère le rating pour le userId rating.getUserId() sur le film rating.getMovieId()
                BasicDBObject ratingFilter = new BasicDBObject();
                ratingFilter.put("user_id", rating.getUserId());
                ratingFilter.put("mov_id", rating.getMovieId());
                DBObject currentRating = ratingsCollection.findOne(ratingFilter);

                //Pour le timestamp
                Date date = new Date();
                long time = date.getTime();

                if (currentRating == null) {//si le rating existe pas
                    //on crée le rating
                    BasicDBObject createRating = new BasicDBObject();
                    createRating.append("user_id", rating.getUserId());
                    createRating.append("mov_id", rating.getMovieId());
                    createRating.append("rating", rating.getScore());
                    createRating.append("timestamp", time);

                    ratingsCollection.insert(createRating);
                } else {//si le rating existe
                    //on fait l'update
                    BasicDBObject updateRating = new BasicDBObject();
                    updateRating.append("rating", rating.getScore());
                    updateRating.append("timestamp", time);

                    BasicDBObject setQuery = new BasicDBObject();
                    setQuery.append("$set", updateRating);

                    ratingsCollection.update(ratingFilter, setQuery);
                }
            } catch (Exception e) {
                System.out.println("Erreur addOrUpdateRating : movieId = " + rating.getMovieId() + "  userId = " + rating.getUserId());
                e.printStackTrace();
            }
        }
    }

    @Override
    public List<Rating> processRecommendationsForUser (int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }
}
