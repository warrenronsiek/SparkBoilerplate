package com.warrenronsiek.transfomers.mediasalariesclean

import org.apache.spark.sql.functions.{lower, split}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import com.warrenronsiek.transfomers.generic.AllPurposeTransformer

class CleanGenderEthnicity extends AllPurposeTransformer{
  import sqlContext.implicits._

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn("gender_ethnicity_lower", lower($"gender_ethnicity"))
      .withColumn("gender_ethnicity_split", split($"gender_ethnicity_lower", """[\s/]"""))
      .drop("gender_ethnicity", "gender_ethnicity_lower")
      .selectExpr("""transform(gender_ethnicity_split, x -> regexp_replace(x, "[^a-zA-Z]", "")) as gender_ethnicity""", "*")
      .drop("gender_ethnicity_split")
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("title", ArrayType(StringType), nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("salary", DoubleType, nullable = true),
      StructField("gender_ethnicity", ArrayType(StringType), nullable = true),
      StructField("experience", DoubleType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))
  }
}
