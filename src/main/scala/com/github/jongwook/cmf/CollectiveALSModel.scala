package com.github.jongwook.cmf

import java.{ util => ju }

import com.github.fommil.netlib.BLAS.{ getInstance => blas }
import com.github.jongwook.cmf.spark.SchemaUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DoubleType, FloatType, StructType }
import org.apache.spark.sql.{ DataFrame, Dataset }

class CollectiveALSModel(rank: Int, factors: DataFrame*) extends Serializable {

  private val cols = new Array[String](factors.size)
  cols(0) = "user"
  cols(1) = "item"

  def userCol: String = cols(0)
  def itemCol: String = cols(1)
  def entityCol(index: Int): String = cols(index)

  var predictionCol: String = "prediction"

  def setUserCol(value: String): this.type = { cols(0) = value; this }
  def setItemCol(value: String): this.type = { cols(1) = value; this }
  def setEntityCol(index: Int, value: String): this.type = { cols(index) = value; this }
  def setEntityCols(values: Seq[String]): this.type = {
    require(values.length == factors.size, s"There should be exactly ${factors.size} columns")
    System.arraycopy(values.toArray, 0, cols, 0, values.length)
    this
  }

  def factorMap: Map[String, DataFrame] = cols.toList.zip(factors.toList).toMap

  def setPredictionCol(value: String): this.type = { predictionCol = value; this }

  val checkedCast = udf { (n: Double) =>
    if (n > Int.MaxValue || n < Int.MinValue) {
      throw new IllegalArgumentException(s"ALS only supports values in Integer range for columns " +
        s"$userCol and $itemCol. Value $n was out of Integer range.")
    } else {
      n.toInt
    }
  }

  def predict(dataset: Dataset[_], leftEntity: String = cols(0), rightEntity: String = cols(1)): DataFrame = {
    val Seq(leftFactors, rightFactors) = Seq(leftEntity, rightEntity).map { entity =>
      cols.indexOf(entity) match {
        case -1 => throw new IllegalArgumentException(s"Unknown entity: $entity")
        case index => factors(index)
      }
    }

    transformSchema(dataset.schema)
    // Register a UDF for DataFrame, and then
    // create a new column named map(predictionCol) by running the predict UDF.
    val predict = udf { (userFeatures: Seq[Float], itemFeatures: Seq[Float]) =>
      if (userFeatures != null && itemFeatures != null) {
        blas.sdot(rank, userFeatures.toArray, 1, itemFeatures.toArray, 1)
      } else {
        Float.NaN
      }
    }
    dataset
      .join(
        leftFactors,
        checkedCast(dataset(leftEntity).cast(DoubleType)) === leftFactors("id"), "left"
      )
      .join(
        rightFactors,
        checkedCast(dataset(rightEntity).cast(DoubleType)) === rightFactors("id"), "left"
      )
      .select(
        dataset("*"),
        predict(leftFactors("features"), rightFactors("features")).as(predictionCol)
      )
  }

  def transformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, userCol)
    SchemaUtils.checkNumericType(schema, itemCol)
    SchemaUtils.appendColumn(schema, predictionCol, FloatType)
  }

}
