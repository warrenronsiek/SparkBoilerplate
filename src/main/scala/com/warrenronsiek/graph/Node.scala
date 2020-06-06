package com.warrenronsiek.graph

import org.apache.spark.sql.Dataset

class Node[I, O](i : Dataset[I]){

  lazy val transform: Dataset[I] => Dataset[O] = ???

  def flatMap = this.transform(i)

  def ~>[OI, OF](n: Node[O, OI]) = new Node[OI, OF](n.transform(this.transform(i)))

  def ~>[I2, O2](jn: JoinNode[O, I2, O2]) = (n: Node) => jn.

}

object Node {
  def apply[I, O](i: Dataset[I]): Node[I, O] = new Node[I, O](i)
}