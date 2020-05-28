package com.warrenronsiek.transfomers.demopipeline

import com.amazon.deequ.checks.{Check, CheckLevel, CheckWithLastConstraintFilterable}
import com.warrenronsiek.transfomers.generic.VerificationTransformer

class DemoFilter1Validator extends VerificationTransformer {
  val verifications: Array[CheckWithLastConstraintFilterable] = Array(
    Check(CheckLevel.Error, "verify the DemoFilter1 removed all the Iris-setosa")
      .isContainedIn("species", Array("Iris-versicolor", "Iris-virginica"))
  )
}
