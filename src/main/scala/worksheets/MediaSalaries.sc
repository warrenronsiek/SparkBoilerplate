import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

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

val spark: SparkSession = SparkSession
  .builder
  .master("local")
  .appName("worksheet")
  .getOrCreate()

import spark.sqlContext.implicits._

implicit class Regex(sc: StringContext) {
  def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
}

val locationStandardizer = udf((loc: String) => {
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

val df = spark.read.schema(schema).csv("/Users/warrenronsiek/Projects/SparkBoilerplate/src/test/resources/real_media_salaries.csv")
  .drop("work_history", "duties")
  .withColumn("title_lower", lower($"title"))
  .withColumn("title_split", split($"title_lower", """[\s/]"""))
  .drop("title", "title_lower")
  .selectExpr("""transform(title_split, x -> regexp_replace(x, "[^a-zA-Z]", "")) as title""", "*")
  .drop("title_split")
  .withColumn("exp_yrs_dbl", $"experience_years".cast(DoubleType))
  .drop("experience_years")
  .withColumnRenamed("exp_yrs_dbl", "experience")
  .withColumn("location_lower", lower($"location"))
  .withColumn("location_no_state", regexp_replace($"location_lower", ",.*", ""))
  .withColumn("location_no_punct", regexp_replace($"location_no_state", "[^a-zA-Z ]", ""))
  .withColumn("location_no_tail", regexp_replace($"location_no_punct", """\s+$""", ""))
  .withColumn("location_standard", locationStandardizer($"location_no_tail"))
  .drop("location_lower", "location_no_state", "location_no_punct", "location_no_tail", "location")
  .withColumnRenamed("location_standard", "location")

df
  //  .groupBy("location_no_tail", "location_standard").count.orderBy($"count".desc)
  //  .where($"location_standard" === "other").show(300)
  .show(3)