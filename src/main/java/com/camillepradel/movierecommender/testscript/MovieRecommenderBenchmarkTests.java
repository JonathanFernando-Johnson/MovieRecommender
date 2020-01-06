package com.camillepradel.movierecommender.testscript;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class MovieRecommenderBenchmarkTests {

    static List<String> resultats = new ArrayList<String>();

    public static void main (String[] args) {

        long debutTestBenchmark = System.nanoTime();

        testSurUrl("http://localhost:8080/MovieRecommender/movies?user_id=");
        testSurUrl("http://localhost:8080/MovieRecommender/movieratings?user_id=");
        testSurUrl("http://localhost:8080/MovieRecommender/recommendations?processing_mode=0&user_id=");
        testSurUrl("http://localhost:8080/MovieRecommender/recommendations?processing_mode=1&user_id=");
        testSurUrl("http://localhost:8080/MovieRecommender/recommendations?processing_mode=2&user_id=");

        long finTestBenchmark = System.nanoTime();
        double tempsTestBenchmark = (double) (finTestBenchmark - debutTestBenchmark) / 1000000000;
        resultats.add("Durée globale du benchmark : " + tempsTestBenchmark);

        try {
            PrintWriter out = new PrintWriter("testBenchmarkLogs.log");

            for (String currentResult : resultats) {
                System.out.println(currentResult);
                out.println(currentResult);
            }

            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    private static void testSurUrl (String urlStart) {
        resultats.add("----    Test " + urlStart + "    ----\n");

        List<Integer> userIdsToTest = new ArrayList<Integer>();
        userIdsToTest.add(1);// Massive data
        userIdsToTest.add(63);//Low data

        for (int nbIter = 1; nbIter < 1001; nbIter = nbIter * 10) {
            long debutTempsThread = System.nanoTime();

            for (int i = 0; i < nbIter; i++) {
                resultats.add("\tItération n° " + i);
                long debutTempsIteration = System.nanoTime();
                for (Integer currentUserId : userIdsToTest) {
                    resultats.add("\t\tUtilisateur n° " + currentUserId);
                    long debutTempsUtilisateur = System.nanoTime();

                    URL u;
                    InputStream is = null;
                    DataInputStream dis;

                    try {
                        u = new URL(urlStart + currentUserId);
                        is = u.openStream();
                        dis = new DataInputStream(new BufferedInputStream(is));
                        while (dis.readLine() != null) {
                        }
                    } catch (MalformedURLException mue) {
                        resultats.add("Ouch - a MalformedURLException happened.");
                        resultats.add(mue.getMessage());
                    } catch (IOException ioe) {
                        resultats.add("Oops- an IOException happened.");
                        resultats.add(ioe.getMessage());
                    } finally {
                        try {
                            is.close();
                        } catch (IOException ioe) {
                            resultats.add("Oops- an IOException happened.");
                        }
                    }

                    long finTempsUtilisateur = System.nanoTime();
                    double tempsUtilisateur = (double) (finTempsUtilisateur - debutTempsUtilisateur) / 1000000000;
                    resultats.add("\t\t\tTemps passé pour l'utilisateur en cours : " + tempsUtilisateur);
                    resultats.add("\t\tFin test pour utilisateur " + currentUserId);
                }

                long finTempsIteration = System.nanoTime();
                double tempsIteration = (double) (finTempsIteration - debutTempsIteration) / 1000000000;
                resultats.add("\tTemps passé pour l'itération en cours : " + tempsIteration);
                resultats.add("\tFin itération " + i + "\n");
            }

            long finTempsThread = System.nanoTime();
            double tempsThread = (double) (finTempsThread - debutTempsThread) / 1000000000;
            resultats.add("\nTemps pour effectuer " + nbIter + " requêtes sur un thread: " + tempsThread + "s\n");
        }
        resultats.add("Fin test " + urlStart + "\n");
    }
}