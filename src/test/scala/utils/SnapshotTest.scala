package utils

import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.GenSeq


trait SnapshotTest extends SparkProvider {
  import spark.sqlContext.implicits._
//  private val resourceDir = this.getClass.getResource(
//    (this.getClass.getPackage.getName + this.getClass.getName.toLowerCase()).replace('.', '/'))
//  private val snapshotFolder = new File(resourceDir.getPath)

//  private def saveSnapshot(snapshotName: String, dataFrame: DataFrame): Unit = {
//    dataFrame.write.parquet(Array(snapshotFolder.getAbsolutePath, snapshotName).mkString("/"))
//  }
//
//  private def readSnapshot(snapshotName: String): DataFrame = {
//    spark.read.parquet(Array(snapshotFolder.getAbsolutePath, snapshotName).mkString("/"))
//  }

  class Diff[T <: GenSeq[T]](newVal: T, snapshotVal: T) {
    def this(noDiff: T) = {
      this(noDiff, noDiff)
    }

    val diff: GenSeq[T] =  newVal diff snapshotVal
  }

  private def colCompare[T]() = udf((newVal: T, snapshotVal: T) => {
    if (newVal == snapshotVal) {
      newVal
    } else {
      new Diff[T](newVal, snapshotVal)
    }
  })

  def compareSnapshot(newDF:DataFrame, snapshotDF:DataFrame, sortBy:List[String]): Any = {
    val hd::tail = sortBy
//    val newSorted = newDF.sort(hd, tail:_*).withColumn("index", monotonically_increasing_id())
//    val snapshotSorted = snapshotDF.sort(hd, tail:_*).withColumn("index", monotonically_increasing_id())
//    newSorted.join(snapshotSorted, $"index").columns

  }
}
