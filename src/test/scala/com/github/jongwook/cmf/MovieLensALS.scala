package com.github.jongwook.cmf

import com.kakao.cuesheet.CueSheet
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD

object MovieLensALS extends CueSheet {{
  import spark.implicits._

  val data = MovieLens.load("ml-latest-small")

  val Seq(train, test) = Utils.splitChronologically(data.ratings, Seq(0.99, 0.01))

  val als = new ALS()
    .setMaxIter(20)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")

  val model = als.fit(train)
  val predicted = model.transform(test)

  val predictedRDD: RDD[((Int, Int), Double)] = predicted.map {
    row => ((row.getAs[Int]("userId"), row.getAs[Int]("movieId")), row.getAs[Float]("prediction").toDouble)
  }.rdd

  val testRDD: RDD[((Int, Int), Double)] = test.map {
    case MovieLensRating(userId, movieId, rating, _) => ((userId, movieId), rating.toDouble)
  }.rdd

  val pairs = predictedRDD.join(testRDD).values

  println(s"pairs = ${pairs.count()}")

  pairs.collect().foreach(println)

  val validPairs = pairs.filter { case (a, b) => !a.isNaN && !b.isNaN }

  val metrics = new RegressionMetrics(validPairs, false)


  println(s"RMSE = ${metrics.rootMeanSquaredError}")
  println(s"MAE = ${metrics.meanAbsoluteError}")
}}
