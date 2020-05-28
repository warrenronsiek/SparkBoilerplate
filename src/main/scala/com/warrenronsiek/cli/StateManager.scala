package com.warrenronsiek.cli

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer


class StateManager(implicit stateName: String = "sparkcli") {
  case class ClusterRecord(clusterId: String, createTime: String, clusterName: String)
  case class JobRecord(clusterId: String, jobId: String)
  Class.forName("org.sqlite.JDBC")

  val connection: Connection = DriverManager.getConnection(s"jdbc:sqlite:$stateName")
  val statement: Statement = connection.createStatement
  statement.executeUpdate(
    """create table if not exists sparkclusters (
      |cluster_id string,
      |create_time timestamp,
      |cluster_name string)""".stripMargin)
  statement.executeUpdate(
    """create table if not exists sparkjobs (
      |cluster_id string,
      |job_id string
      |)""".stripMargin
  )

  def addCluster(clusterId: String, name: String) = {
    statement.executeUpdate(
      s"""insert into sparkclusters
         |(cluster_id, create_time, cluster_name)
         |values ('$clusterId', '${LocalDateTime.now.toString}', '$name')""".stripMargin)
  }

  def addJob(clusterId: String, jobId: String) = {
    statement.executeUpdate(
      s"""insert into sparkjobs
         |(cluster_id, job_id)
         |values ('$clusterId', '$jobId')""".stripMargin)
  }

  def removeJob(jobId: String) = {
    statement.executeUpdate(s"""delete from sparkjobs where job_id = '$jobId'""")
  }

  def removeAll() = {
    statement.executeUpdate("""delete from sparkclusters""")
    statement.executeUpdate("""delete from sparkjobs""")
  }

  def removeCluster(clusterName: String) = {
    statement.executeUpdate(s"""delete from sparkclusters where cluster_name = '$clusterName'""")
  }

  def removeClusterById(clusterId: String) = {
    statement.executeUpdate(s"""delete from sparkclusters where cluster_id = '$clusterId'""")
  }

  def getClusters(): List[ClusterRecord] = {
    val clusters: ResultSet = statement.executeQuery(s"select * from sparkclusters order by create_time desc")
    var clusterList: ListBuffer[ClusterRecord] = ListBuffer()
    while (clusters.next()) {
      clusterList += ClusterRecord(
        clusters.getString("cluster_id"),
        clusters.getString("create_time"),
        clusters.getString("cluster_name"))
    }
    clusterList.toList
  }

  def getJobs(clusterId: String):List[JobRecord] = {
    val jobs: ResultSet = statement.executeQuery(s"select * from sparkjobs")
    var jobList: ListBuffer[JobRecord] = ListBuffer()
    while (jobs.next()) {
      jobList += JobRecord(
        jobs.getString("cluster_id"),
        jobs.getString("job_id"))
    }
    jobList.toList
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
