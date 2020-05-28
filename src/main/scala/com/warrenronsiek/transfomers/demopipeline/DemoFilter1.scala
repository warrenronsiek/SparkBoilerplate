package com.warrenronsiek.transfomers.demopipeline
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import com.warrenronsiek.transfomers.generic.AllPurposeTransformer


class DemoFilter1 extends AllPurposeTransformer {
  import sqlContext.implicits._

  override def transform(dataset: Dataset[_]): DataFrame = dataset.filter($"species" =!= "Iris-setosa").toDF()
  override def transformSchema(schema: StructType): StructType = schema

}
