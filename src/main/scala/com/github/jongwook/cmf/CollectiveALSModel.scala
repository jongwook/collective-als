package com.github.jongwook.cmf

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class CollectiveALSModel(override val uid: String, rank: Int, userFactors: DataFrame, itemFactors: DataFrame) extends Model[CollectiveALSModel] {

  var userCol: String = "user"
  var itemCol: String = "item"
  var predictionCol: String = "prediction"

  def setUserCol(value: String): this.type = { userCol = value; this }
  def setItemCol(value: String): this.type = { itemCol = value; this }
  def setPredictionCol(value: String): this.type = { predictionCol = value; this }

  val checkedCast = udf { (n: Double) =>
    if (n > Int.MaxValue || n < Int.MinValue) {
      throw new IllegalArgumentException(s"ALS only supports values in Integer range for columns " +
        s"$userCol and $itemCol. Value $n was out of Integer range.")
    } else {
      n.toInt
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
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
      .join(userFactors,
        checkedCast(dataset(userCol).cast(DoubleType)) === userFactors("id"), "left")
      .join(itemFactors,
        checkedCast(dataset(itemCol).cast(DoubleType)) === itemFactors("id"), "left")
      .select(dataset("*"),
        predict(userFactors("features"), itemFactors("features")).as(predictionCol))
  }

  override def transformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, userCol)
    SchemaUtils.checkNumericType(schema, itemCol)
    SchemaUtils.appendColumn(schema, predictionCol, FloatType)
  }

  override def copy(extra: ParamMap): CollectiveALSModel = {
    val copied = new CollectiveALSModel(uid, rank, userFactors, itemFactors)
    copyValues(copied, extra).setParent(parent)
  }
}
