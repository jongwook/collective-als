package com.github.jongwook.cmf

import org.apache.spark.annotation.{Since, DeveloperApi}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType


class CollectiveALS(override val uid: String) extends Estimator[CollectiveALSModel] {

  def this() = this(Identifiable.randomUID("collective-als"))

  var rank: Int = 10
  var numUserBlocks: Int = 10
  var numItemBlocks: Int = 10
  var implicitPrefs: Boolean = false
  var alpha: Double = 1.0
  var userCol: String = "user"
  var itemCol: String = "item"
  var ratingCol: String = "rating"
  var predictionCol: String = "prediction"
  var maxIter: Int = 10
  var regParam: Double = 0.1
  var nonnegative: Boolean = false
  var checkpointInterval: Int = 10
  var seed: Long = this.getClass.getName.hashCode.toLong
  var intermediateStorageLevel: String = "MEMORY_AND_DISK"
  var finalStorageLevel: String = "MEMORY_AND_DISK"

  def setRank(value: Int): this.type = { rank = value; this }
  def setNumUserBlocks(value: Int): this.type = { numUserBlocks = value; this }
  def setNumItemBlocks(value: Int): this.type = { numItemBlocks = value; this }
  def setImplicitPrefs(value: Boolean): this.type = { implicitPrefs = value; this }
  def setAlpha(value: Double): this.type = { alpha = value; this }
  def setUserCol(value: String): this.type = { userCol = value; this }
  def setItemCol(value: String): this.type = { itemCol = value; this }
  def setRatingCol(value: String): this.type = { ratingCol = value; this }
  def setPredictionCol(value: String): this.type = { predictionCol = value; this }
  def setMaxIter(value: Int): this.type = { maxIter = value; this }
  def setRegParam(value: Double): this.type = { regParam = value; this }
  def setNonnegative(value: Boolean): this.type = { nonnegative = value; this }
  def setCheckpointInterval(value: Int): this.type = { checkpointInterval = value; this }
  def setSeed(value: Long): this.type = { seed = value; this }
  def setIntermediateStorageLevel(value: String): this.type = { intermediateStorageLevel = value; this }
  def setFinalStorageLevel(value: String): this.type = { finalStorageLevel = value; this }

  def setNumBlocks(value: Int): this.type = {
    numUserBlocks = value
    numItemBlocks = value
    this
  }

  override def fit(dataset: Dataset[_]): CollectiveALSModel = ???

  override def copy(extra: ParamMap): Estimator[CollectiveALSModel] = ???

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

}
