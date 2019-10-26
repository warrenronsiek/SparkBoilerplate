package schemas

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Iris {
  val schema = StructType(Array(
    StructField("petal_length", DoubleType, nullable = false),
    StructField("petal_width", DoubleType, nullable = false),
    StructField("sepal_length", DoubleType, nullable = false),
    StructField("sepal_width", DoubleType, nullable = false),
    StructField("species", StringType, nullable = false)
  ))
}
