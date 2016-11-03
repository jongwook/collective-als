package com.github.jongwook.cmf

import org.apache.spark.sql.{Encoder, Dataset}

object Utils {
  def splitChronologically[T](data: Dataset[T], weights: Seq[Double], timeColumn: String = "timestamp"): Seq[Dataset[T]] = {
    val encoderField = data.getClass.getDeclaredField("exprEnc")
    encoderField.setAccessible(true)
    implicit val encoder = encoderField.get(data).asInstanceOf[Encoder[T]]

    val schema = data.schema
    val index = schema.fieldIndex(timeColumn)
    val rdd = data.toDF.rdd.map { row => (row.getAs[Number](index).doubleValue, row) }
    val ranked = rdd.sortByKey().values.zipWithIndex

    val count = data.count().toDouble
    val sum = weights.sum
    val bounds = weights.map(_ / sum).scanLeft(0.0)(_ + _).sliding(2).toSeq

    bounds.map {
      case Seq(l, u) =>
        val lower = l * count
        val upper = u * count
        val split = ranked.filter {
          case (row, rank) => lower <= rank && rank < upper
        }.map {
          case (row, rank) => row
        }
        data.sqlContext.createDataFrame(split, schema).as[T]
    }
  }

}
