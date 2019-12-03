package transfomers.mediasalariesclean

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, monotonically_increasing_id, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import transfomers.generic.AllPurposeTransformer

class CreateEthnicity extends AllPurposeTransformer {
  import sqlContext.implicits._

  val ethnicityStandardizer: UserDefinedFunction = udf((genEth: String) => {
    genEth match {
      case r""".*white.*""" => "white"
      case r""".*caucasian.*""" => "white"
      case r""".*american.*""" => "white"
      case r""".*euro.*""" => "white"

      case r""".*hispanic.*""" => "hispanic"
      case r""".*latin.*""" => "hispanic"
      case r""".*mexican.*""" => "hispanic"

      case r""".*asian.*""" => "asian"
      case r""".*chinese.*""" => "asian"
      case r""".*korean.*""" => "asian"
      case r""".*viet.*""" => "asian"
      case r""".*bengali.*""" => "asian"

      case r""".*black.*""" => "black"
      case r""".*african.*""" => "black"
      case r""".*oc.*""" => "black"

      case _ => "unknown/other"
    }
  })

  val rowReduction: (Row, Row) => Row = (r1: Row, r2: Row) => {if (r1(2) == "unknown/other") r2 else r1}


  override def transform(dataset: Dataset[_]): DataFrame = {
    val ds1 = dataset
      .withColumn("id", monotonically_increasing_id())
    val rdd = ds1
      .select($"id", explode($"gender_ethnicity") as "ge")
      .withColumn("ethnicity", ethnicityStandardizer($"ge"))
      .rdd
      .keyBy(row => row.get(0))
      .reduceByKey(rowReduction)
      .map(_._2)

    val df2 = spark.createDataFrame(rdd, StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("genEth", StringType, nullable = true),
      StructField("ethnicity", StringType, nullable = true)
    )))

    ds1.join(df2, Seq("id"), "left").drop("genEth", "id")
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("title", ArrayType(StringType), nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("salary", DoubleType, nullable = true),
      StructField("gender_ethnicity", ArrayType(StringType), nullable = true),
      StructField("experience", DoubleType, nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("ethnicity", StringType, nullable = true)
    ))
  }

}
