package transfomers.mediasalariesclean

import org.apache.spark.sql.functions.{lower, split}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import transfomers.generic.AllPurposeTransformer

class CleanTitles extends AllPurposeTransformer {
  import sqlContext.implicits._
  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn("title_lower", lower($"title"))
      .withColumn("title_split", split($"title_lower", """[\s/]"""))
      .drop("title", "title_lower")
      .selectExpr("""transform(title_split, x -> regexp_replace(x, "[^a-zA-Z]", "")) as title""", "*")
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("title", ArrayType(StringType), nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("salary", StringType, nullable = true),
      StructField("gender_ethnicity", StringType, nullable = true),
      StructField("experience_years", StringType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))
  }
}
