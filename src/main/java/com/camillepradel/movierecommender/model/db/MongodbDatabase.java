package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
        // TODO: write query to retrieve all movies rated by user with id userId
        List<Movie> movies = new LinkedList<Movie>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})));
        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser (int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        List<Rating> ratings = new LinkedList<Rating>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        return ratings;
    }

    @Override
    public void addOrUpdateRating (Rating rating) {
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
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
