package schemas

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object MediaSalaries {
  val schema = StructType(Array(
    StructField("title", StringType, nullable = true),
    StructField("company", StringType, nullable = true),
    StructField("salary", StringType, nullable = true),
    StructField("gender_ethnicity", StringType, nullable = true),
    StructField("experience_years", StringType, nullable = true),
    StructField("location", StringType, nullable = true),
    StructField("duties", StringType, nullable = true),
    StructField("work_history", StringType, nullable = true)
  ))
}
