package transfomers.mediasalariesclean

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import transfomers.generic.AllPurposeTransformer

class DropBadCols extends AllPurposeTransformer{
  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.drop("work_history", "duties")
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("title", StringType, nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("salary", StringType, nullable = true),
      StructField("gender_ethnicity", StringType, nullable = true),
      StructField("experience_years", StringType, nullable = true),
      StructField("location", StringType, nullable = true)
    ))
  }
}
