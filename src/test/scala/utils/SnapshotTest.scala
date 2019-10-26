package utils

import java.io.File
import java.net.URL

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import scala.reflect.io.Directory

trait SnapshotTest extends SparkProvider {

  import spark.sqlContext.implicits._

  val logger: Logger = Logger.getLogger("TestLogger")
  private val testResources: String = List(System.getProperty("user.dir"), "src", "test", "resources").mkString("/")
  private val resourcePath: String = this.getClass.getName.toLowerCase().replace('.', '/')

  private def saveSnapshot(snapshotName: String, dataFrame: DataFrame): Unit = {
    val dir = new Directory(new File(Array(testResources, resourcePath, snapshotName).mkString("/")))
    dir.deleteRecursively()
    dataFrame.write.parquet(Array(testResources, resourcePath, snapshotName).mkString("/"))
  }
  private def readSnapshot(snapshotName: String): DataFrame = {
    spark.read.parquet(Array(testResources, resourcePath, snapshotName).mkString("/"))
  }

  private def compareSnapshot(newDF: DataFrame, snapshotDF: DataFrame, sortBy: List[String]): Boolean = {
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

  def assertSnapshotNoDiffRows(snapshotName:String, dataFrame: DataFrame, sortBy: List[String]): Boolean = {
    try {
      val snapshot = readSnapshot(snapshotName)
      compareSnapshot(dataFrame, snapshot, sortBy)
    } catch {
      case ex: AnalysisException =>
        logger.info("Snapshot does not exist, creating it.")
        saveSnapshot(snapshotName, dataFrame)
        true
      case _ => false
    }
  }
}
