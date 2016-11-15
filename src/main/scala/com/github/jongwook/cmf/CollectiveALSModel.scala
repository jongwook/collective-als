package com.github.jongwook.cmf

import org.apache.spark.annotation.{Since, DeveloperApi}
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class CollectiveALSModel(override val uid: String) extends Model[CollectiveALSModel] {

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): CollectiveALSModel = ???

}
