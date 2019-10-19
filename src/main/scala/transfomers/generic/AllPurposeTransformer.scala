package transfomers.generic
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.SQLContext
import utils.SQLContextSingleton

trait AllPurposeTransformer extends Transformer with Params with SQLContextSingleton {
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  val uid: String = Identifiable.randomUID(this.getClass.getName).toString

  val sqlContext: SQLContext = getSqlContext
}
