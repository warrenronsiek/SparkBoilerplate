package transfomers.mediasalaries

import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import transfomers.generic.AllPurposeTransformer

class CleanExperience extends AllPurposeTransformer {
  import sqlContext.implicits._

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn("exp_yrs_dbl", $"experience_years".cast(DoubleType))
      .drop("experience_years")
      .withColumnRenamed("exp_yrs_dbl", "experience")
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("title", ArrayType(StringType), nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("salary", StringType, nullable = true),
      StructField("gender_ethnicity", StringType, nullable = true),
      StructField("experience", DoubleType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))
  }

}
