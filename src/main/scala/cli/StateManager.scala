package cli

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer


class StateManager(implicit stateName: String = "sparkcli") {
  case class ClusterRecord(clusterId: String, createTime: String, clusterName: String)

  val connection: Connection = DriverManager.getConnection(s"jdbc:sqlite:$stateName")
  val statement: Statement = connection.createStatement
  statement.executeUpdate(
    """create table if not exists sparkclusters (
      |cluster_id string,
      |create_time timestamp,
      |cluster_name string)""".stripMargin)

  def addCluster(clusterId: String, name: String) = {
    statement.executeUpdate(
      s"""insert into sparkclusters
         |(cluster_id, create_time, cluster_name)
         |values ('$clusterId', '${LocalDateTime.now.toString}', '$name')""".stripMargin)
  }

  def removeAll() = {
    statement.executeUpdate(s"""delete from sparkclusters""")
  }

  def removeCluster(clusterName: String) = {
    statement.executeUpdate(s"""delete from sparkclusters where cluster_name = '$clusterName'""")
  }

  def getClusters(): List[ClusterRecord] = {
    val clusters: ResultSet = statement.executeQuery(s"select * from sparkclusters")
    var clusterList: ListBuffer[ClusterRecord] = ListBuffer()
    while (clusters.next()) {
      clusterList += ClusterRecord(
        clusters.getString("cluster_id"),
        clusters.getString("create_time"),
        clusters.getString("cluster_name"))
    }
    clusterList.toList
  }

  def printClusters() = {
    getClusters().map(x => println(x))
  }

  override def toString: String = {
    getClusters().map(x => x.toString).toString()
  }
}

object StateManager {
  var sm: StateManager = null
  def apply(implicit stateName: String = "sparkcli"): StateManager = {
    if (sm == null) {
      sm = new StateManager()(stateName)
    }
    sm
  }
}
