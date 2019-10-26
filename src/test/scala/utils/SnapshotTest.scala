package utils

import java.io.File
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, Dataset}
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
      .alias("new")
    val snapshotSorted = snapshotDF.sort(hd, tail: _*).withColumn("index", monotonically_increasing_id())
      .alias("snap")
    val joined = newSorted.join(snapshotSorted, "index")
    val mismatchedCols: Array[Dataset[Row]] = newSorted.columns.filter(colname => colname != "index").map(colname =>
      joined
        .select($"index",
          col(s"new.$colname").alias(s"new_$colname"),
          col(s"snap.$colname").alias(s"snap_$colname"))
        .where(col(s"new.$colname") =!= col(s"snap.$colname"))
    )
    if (mismatchedCols.length > 0) {
      logger.error("ERROR: snapshot matching failure")
      mismatchedCols.filter(dataset => dataset.count() > 0).foreach(dataset => dataset.show())
      return false
    }
    true
  }

  def assertSnapshot(snapshotName: String, dataFrame: DataFrame, sortBy: List[String]): Boolean = {
    try {
      val snapshot = readSnapshot(snapshotName)
      compareSnapshot(dataFrame, snapshot, sortBy)
    } catch {
      case ex: AnalysisException if ex.message.contains("Path does not exist") =>
        System.out.println(ex)
        logger.info("Snapshot does not exist, creating it.")
        saveSnapshot(snapshotName, dataFrame)
        true
      case _ => false
    }
  }
}
