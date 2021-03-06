package com.warrenronsiek

package object cli {
  case class EMRParams(
                        subnet: String,
                        name: String,
                        logUri: String,
                        serviceRole: String,
                        instanceRole: String,
                        key: String,
                        instanceCount: Int,
                        instanceType: String,
                        bidPrice: Option[Double] = None
                      )

  case class EC2Info(memory: Double, cores: Int)
}
