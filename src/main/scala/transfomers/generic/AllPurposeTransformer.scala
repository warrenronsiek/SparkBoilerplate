package transfomers.generic
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.{SQLContext, SparkSession}

abstract class AllPurposeTransformer extends Transformer with Params {
  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  val uid: String = Identifiable.randomUID(this.getClass.getName).toString
  val sqlContext: SQLContext = SparkSession.builder.getOrCreate().sqlContext
}
