package transfomers.mediasalariesclean

import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import transfomers.generic.AllPurposeTransformer

class CleanSalary extends AllPurposeTransformer{
  import sqlContext.implicits._

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset
      .withColumn("salary_no_cents", regexp_replace($"salary", """\..*""", ""))
      .withColumn("salary_only_digit", regexp_replace($"salary_no_cents", "[^0-9]", ""))
      .withColumn("salary_double", $"salary_only_digit".cast(DoubleType))
      .drop("salary", "salary_only_digit", "salary_no_cents")
      .withColumnRenamed("salary_double", "salary")
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("title", ArrayType(StringType), nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("salary", DoubleType, nullable = true),
      StructField("gender_ethnicity", StringType, nullable = true),
      StructField("experience", DoubleType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))
  }
}
