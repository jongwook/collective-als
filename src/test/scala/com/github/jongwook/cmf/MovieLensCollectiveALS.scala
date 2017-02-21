package com.github.jongwook.cmf

import com.kakao.cuesheet.CueSheet
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD

case class MovieGenreMapping(movieId: Int, genreId: Int, rating: Float)

object MovieLensCollectiveALS extends CueSheet {{
  import spark.implicits._

  val data = MovieLens.load("ml-latest-small")

  val Seq(train, test) = Utils.splitChronologically(data.ratings, Seq(0.99, 0.01))

  val allGenres = data.movies.flatMap(_.genres.split('|')).distinct().collect().sorted
  val genreCode = allGenres.zipWithIndex.toMap

  val genreData = data.movies.flatMap {
    case MovieLensMovie(movieId, _, genres) =>
      genres.split('|').map { genre =>
        val genreId = genreCode(genre)
        MovieGenreMapping(movieId, genreId, 1.0f)
      }
  }

  //val als = new CollectiveALS("userId", "movieId")
  val als = new CollectiveALS("userId", "movieId", "genreId")
    .setMaxIter(20)
    .setRegParam(0.01)
    .setRatingCol("rating")

  val model = als.fit(("userId", "movieId") -> train, ("movieId", "genreId") -> genreData)
  //val model = als.fit(("userId", "movieId") -> train)
  val predicted = model.predict(test)

  val predictedRDD: RDD[((Int, Int), Double)] = predicted.map {
    row => ((row.getAs[Int]("userId"), row.getAs[Int]("movieId")), row.getAs[Float]("prediction").toDouble)
  }.rdd

  val testRDD: RDD[((Int, Int), Double)] = test.map {
    case MovieLensRating(userId, movieId, rating, _) => ((userId, movieId), rating.toDouble)
  }.rdd

  val pairs = predictedRDD.join(testRDD).values
  val validPairs = pairs.filter { case (a, b) => !a.isNaN && !b.isNaN }

  val metrics = new RegressionMetrics(validPairs, false)

  println(f"RMSE = ${metrics.rootMeanSquaredError}%8f   MAE = ${metrics.meanAbsoluteError}%8f")
}}
