package com.github.jongwook.cmf

import java.util.Date

import com.github.jongwook.SparkRankingMetrics
import com.kakao.cuesheet.CueSheet
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

case class IHRCollectiveALSConfig(regParam: Double = 0.01, maxIter: Int = 10)



object IHRCollectiveALS extends CueSheet({
  val conf = new org.apache.hadoop.conf.Configuration(false)
  conf.addResource(Thread.currentThread().getContextClassLoader.getResourceAsStream("dumbo/hive-site.xml"))
  conf.iterator().toSeq.map {
    e => ("spark.hadoop." + e.getKey, e.getValue)
  }
}: _*) {{

  val parser = new scopt.OptionParser[IHRCollectiveALSConfig]("IHRCollectiveALS") {
    head("IHRCollectiveALS")
    opt[Double]('r', "reg-param").action( (r, c) => c.copy(regParam = r) ).text("regularization parameter")
    opt[Int]('i', "max-iter").action( (i, c) => c.copy(maxIter = i) ).text("max iterations")
  }

  val config = parser.parse(args, IHRCollectiveALSConfig()).get

  System.setProperty("scopt.config", config.toString)
  logger.warn(s"Hive Metastore: ${spark.sqlContext.sparkContext.hadoopConfiguration.get("hive.metastore.uris")}")

  import spark.implicits._

  System.err.println(spark.sql("show databases").collect().foreach(println))

  spark.sqlContext.sql("use jongwook")

  val liveThumbs = spark.table("fct_live_thumbs").repartition(2000).map {
    row => LiveThumb(row.getAs[Int]("profile_id"), row.getAs[Int]("content_id"), row.getAs[String]("type").toFloat * 2 - 1, row.getAs[Date]("create_dt").getTime)
  }.cache()

  val Seq(train, test) = Utils.splitChronologically(liveThumbs, Seq(0.99, 0.01))

  val dimTrack = spark.table("dim_track").repartition(2000).map {
    row => DimTrack(row.getAs[Int]("product_id"), row.getAs[Int]("artist_id"), 1.0f)
  }.filter {
    row => row.artist_id != -1 && row.content_id != -1
  }.cache()

  val als = new CollectiveALS("profile_id", "content_id", "artist_id")
    .setRank(100)
    .setCheckpointInterval(3)
    .setMaxIter(config.maxIter)
    .setRegParam(config.regParam)
    .setRatingCol("thumb")

  val model = als.fit(("profile_id", "content_id") -> train, ("content_id", "artist_id") -> dimTrack)
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

  val result = lines.mkString("\n")
  println(result)

  val fs = FileSystem.get(sc.hadoopConfiguration)
  val out = fs.create(new Path(s"collective-als/reg=${config.regParam},iter=${config.maxIter}.txt"))
  out.write(result.getBytes("UTF-8"))
  out.close()
}}
