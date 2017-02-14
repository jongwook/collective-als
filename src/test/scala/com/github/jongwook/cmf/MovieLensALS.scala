package com.github.jongwook.cmf

import com.github.jongwook.SparkRankingMetrics
import com.kakao.cuesheet.CueSheet
import org.apache.spark.ml.recommendation.ALS

import scala.collection.mutable.ArrayBuffer

object MovieLensALS extends CueSheet {{

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
  val lines = ArrayBuffer[String]()
  lines += "|            |         @5 |       @10 |       @20 |       @50 |      @100 |"
  for ((metric, method) <- methods) {
    val header = "| %10s | ".format(metric)
    val fields = for (value <- method(metrics)(ats)) yield {
      "%10.6f |".format(value)
    }
    lines += (header +: fields).mkString
  }

  println(lines.mkString("\n"))
}}
