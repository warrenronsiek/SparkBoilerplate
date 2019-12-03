import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
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

val ethnicityStandardizer = udf((genEth: String) => {
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

val df: DataFrame = spark.read.schema(schema).csv("/Users/warrenronsiek/Projects/SparkBoilerplate/src/test/resources/real_media_salaries.csv")
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
  .withColumn("salary_no_cents", regexp_replace($"salary", """\..*""", ""))
  .withColumn("salary_only_digit", regexp_replace($"salary_no_cents", "[^0-9]", ""))
  .withColumn("salary_double", $"salary_only_digit".cast(DoubleType))
  .drop("salary", "salary_only_digit", "salary_no_cents")
  .withColumnRenamed("salary_double", "salary")
  .withColumn("gender_ethnicity_lower", lower($"gender_ethnicity"))
  .withColumn("gender_ethnicity_split", split($"gender_ethnicity_lower", """[\s/]"""))
  .drop("gender_ethnicity", "gender_ethnicity_lower")
  .selectExpr("""transform(gender_ethnicity_split, x -> regexp_replace(x, "[^a-zA-Z]", "")) as gender_ethnicity""", "*")
  .drop("gender_ethnicity_split")
  .withColumn("id", monotonically_increasing_id())

val rowReduction = (r1: Row, r2: Row) => {if (r1(2) == "unknown/other") r2 else r1}

//  .map(row => GenderMap(row(0) match {case i: Int => i}, row(1) match {case g: String => g}, row(2) match {case ge: String => ge}))

val rdd = df.select($"id", explode($"gender_ethnicity") as "ge")
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

val df3 = df.join(df2, Seq("id"), "left")

df3.show(110)
