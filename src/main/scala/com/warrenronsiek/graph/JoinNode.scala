package com.warrenronsiek.graph

import org.apache.spark.sql.Dataset

class JoinNode[I1, I2, O](i1: Dataset[I1], i2: Dataset[I2]) {


  lazy val transform: (Dataset[I1], Dataset[I2]) => Dataset[O] = ???

  def flatMap = transform(i1, i2)

  def ~>()
}

object JoinNode {
  def apply[I1, I2, O](i1: Dataset[I1], i2: Dataset[I2]): JoinNode[I1, I2, O] = new JoinNode(i1, i2)
}
