package com.github.jongwook.cmf

import java.util.Date

import com.github.jongwook.SparkRankingMetrics
import com.kakao.cuesheet.CueSheet

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

case class IHRLiveThumb(profile_id: Int, content_id: Int, thumb: Float, timestamp: Long)

object IHRCollectiveALS extends CueSheet({
  val conf = new org.apache.hadoop.conf.Configuration(false)
  conf.addResource(Thread.currentThread().getContextClassLoader.getResourceAsStream("dumbo/hive-site.xml"))
  conf.iterator().toSeq.map {
    e => ("spark.hadoop." + e.getKey, e.getValue)
  }
}: _*) {{

  logger.warn(s"Hive Metastore: ${spark.sqlContext.sparkContext.hadoopConfiguration.get("hive.metastore.uris")}")

  import spark.implicits._

  System.err.println(spark.sql("show databases").collect().foreach(println))

  spark.sqlContext.sql("use jongwook")

  val liveThumbs = spark.sqlContext.table("fct_live_thumbs").repartition(2000).map {
    row => IHRLiveThumb(row.getAs[Int]("profile_id"), row.getAs[Int]("content_id"), row.getAs[String]("type").toFloat * 2 - 1, row.getAs[Date]("create_dt").getTime)
  }.cache()

  val Seq(train, test) = Utils.splitChronologically(liveThumbs, Seq(0.99, 0.01))

  val als = new CollectiveALS("profile_id", "content_id")
    .setMaxIter(20)
    .setRegParam(0.01)
    .setRatingCol("thumb")

  val model = als.fit(("profile_id", "content_id") -> train)
  val predicted = model.predict(test)

  val metrics = SparkRankingMetrics(predicted, test.toDF)
  metrics.setUserCol("profile_id")
  metrics.setItemCol("content_id")
  metrics.setRatingCol("thumb")
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
