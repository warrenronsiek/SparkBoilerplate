package transfomers.mediasalaries

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lower, regexp_replace, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import transfomers.generic.AllPurposeTransformer

class CleanLocation extends AllPurposeTransformer{
  import sqlContext.implicits._

  val locationStandardizer: UserDefinedFunction = udf((loc: String) => {
    loc match {
      case "yonkers" => "nyc"
      case "the bronx" => "nyc"
      case "manhattan" => "nyc"
      case "maryland" => "dc"
      case r""".*n.*y.*c.*""" => "nyc"
      case r""".*new y.*""" => "nyc"
      case r"""ny.*""" => "nyc"
      case r"""s.*f.*""" => "sf"
      case r"""w.*dc.*""" => "dc"
      case r"""dc.*""" => "dc"
      case r"""wash.*""" => "dc"
      case r"""brooklyn""" => "nyc"
      case r""".*bay.*""" => "sf"
      case _ => "other"
    }
  })

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn("location_lower", lower($"location"))
      .withColumn("location_no_state", regexp_replace($"location_lower", ",.*", ""))
      .withColumn("location_no_punct", regexp_replace($"location_no_state", "[^a-zA-Z ]", ""))
      .withColumn("location_no_tail", regexp_replace($"location_no_punct", """\s+$""", ""))
      .withColumn("location_standard", locationStandardizer($"location_no_tail"))
      .drop("location_lower", "location_no_state", "location_no_punct", "location_no_tail", "location")
      .withColumnRenamed("location_standard", "location")
  }

  override def transformSchema(schema: StructType): StructType = schema
}
