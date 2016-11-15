package com.github.jongwook.cmf

import com.github.jongwook.SparkRankingMetrics
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object CollectiveALSTest extends App {{
  implicit val spark = SparkSession.builder().master("local[8]").getOrCreate()
  implicit val sc = spark.sparkContext

  val data = MovieLens.load("src/test/resources/ml-latest-small")

  val Seq(train, test) = Utils.splitChronologically(data.ratings, Seq(0.99, 0.01))

  val als = new ALS()
    .setMaxIter(20)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")

  val model = als.fit(train)
  val predicted = model.transform(test)

  val metrics = SparkRankingMetrics(predicted, test.toDF)
  metrics.setUserCol("userId")
  metrics.setItemCol("movieId")
  metrics.setRatingCol("rating")
  metrics.setPredictionCol("prediction")


  val methods = Map[String, SparkRankingMetrics => Seq[Int] => Seq[Double]](
    "Precision" -> { m => m.precisionAt },
    "Recall" -> { m => m.recallAt },
    "F1" -> { m => m.f1At },
    "NDCG" -> { m => m.ndcgAt },
    "MAP" -> { m => m.mapAt }
  )

  val ats = Seq(5, 10, 20, 50, 100)
  println("|            |         @5 |       @10 |       @20 |       @50 |      @100 |")
  for ((metric, method) <- methods) {
    val header = "| %10s | ".format(metric)
    val fields = for (value <- method(metrics)(ats)) yield {
      "%10.6f |".format(value)
    }
    (header +: fields).foreach(print)
    println()
  }
}}
