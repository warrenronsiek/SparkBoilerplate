package transfomers.demopipeline

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus, CheckWithLastConstraintFilterable}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import transfomers.generic.{AllPurposeTransformer, VerificationTransformer}
import org.apache.log4j.Logger

class DemoFilter1Validator extends VerificationTransformer {

  val verifications: Array[CheckWithLastConstraintFilterable] = Array(
    Check(CheckLevel.Error, "verify the DemoFilter1 removed all the Iris-setosa")
      .isContainedIn("species", Array("Iris-versicolor", "Iris-virginica"))
  )

  override def transformSchema(schema: StructType): StructType = schema
}
