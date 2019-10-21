package transfomers.demopipeline

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import transfomers.generic.AllPurposeTransformer
import org.apache.log4j.Logger

class DemoFilter1Validator extends AllPurposeTransformer {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF
    val verificationResult = VerificationSuite().onData(df).addCheck(Check(CheckLevel.Error, "verify the DemoFilter1")
      .isContainedIn("species", Array("Iris-versicolor", "Iris-virginica"))).run()

    val logger = Logger.getLogger("DemoPipelineLogger")
    if (verificationResult.status == CheckStatus.Success) {
      logger.info("Iris data successfully filtered")
    } else {
      logger.error("Found errors:\n")
      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }
      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result => logger.error(s"${result.constraint}: ${result.message.get}") }
      throw new Error("Verification Failed")
    }
    df
  }

  override def transformSchema(schema: StructType): StructType = schema
}
