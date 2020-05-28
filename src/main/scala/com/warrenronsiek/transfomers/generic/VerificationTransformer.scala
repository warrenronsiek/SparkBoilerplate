package com.warrenronsiek.transfomers.generic

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{CheckStatus, CheckWithLastConstraintFilterable}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

abstract class VerificationTransformer extends AllPurposeTransformer {
  val verifications: Array[CheckWithLastConstraintFilterable]

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF
    val verificationSuite = VerificationSuite().onData(df)
    verifications foreach(check => verificationSuite.addCheck(check))
    val verificationResult = verificationSuite.run()

    val logger = Logger.getLogger("DemoPipelineLogger")
    if (verificationResult.status == CheckStatus.Success) {
      logger.info("Verification Passed")
    } else {
      logger.error("Found errors:\n")
      verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result => logger.error(s"${result.constraint}: ${result.message.get}") }
      throw new Error("Verification Failed")
    }
    df
  }

  override def transformSchema(schema: StructType): StructType = schema
}
