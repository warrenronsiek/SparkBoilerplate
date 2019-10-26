package utils

import java.io.File
import java.net.URL

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

import scala.collection.GenSeq


trait SnapshotTest extends SparkProvider {

  import spark.sqlContext.implicits._

  val logger: Logger = Logger.getLogger("TestLogger")

  private val resource: URL = this.getClass.getResource("/" + this.getClass.getName.toLowerCase().replace('.', '/'))
  private val resourceDir = new File(resource.getPath)
  if (!(resourceDir.exists())){
    resourceDir.mkdirs()
  }
  System.out.println(resourceDir.toString)

  private def saveSnapshot(snapshotName: String, dataFrame: DataFrame): Unit = {
    dataFrame.write.parquet(Array(resource.getPath, snapshotName).mkString("/"))
  }
  private def readSnapshot(snapshotName: String): DataFrame = {
    spark.read.parquet(Array(resource.getPath, snapshotName).mkString("/"))
  }

  def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, sortBy: List[String]): Boolean = {
    val hd :: tail = sortBy
    val newSorted = newDF.sort(hd, tail: _*).withColumn("index", monotonically_increasing_id())
    val snapshotSorted = snapshotDF.sort(hd, tail: _*).withColumn("index", monotonically_increasing_id())
    val diffRows = newSorted.union(snapshotSorted).except(newSorted.intersect(snapshotSorted))
    if (diffRows.count() > 0) {
      logger.error("ERROR: snapshot matching failure")
      diffRows.show()
      return false
    }
    true
  }
}
