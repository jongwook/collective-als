package com.github.jongwook.cmf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CollectiveALSTest extends App {
  implicit val spark = SparkSession.builder().master("local[8]").getOrCreate()
  implicit val sc = spark.sparkContext

  val data = MovieLens.load("src/test/resources/ml-latest-small")

  val Seq(train, test) = Utils.splitChronologically(data.ratings, Seq(0.99, 0.01))
  println(train.count())
  println(test.count())

  println(train.select(min("timestamp"), max("timestamp")).show())
  println(test.select(min("timestamp"), max("timestamp")).show())
}
