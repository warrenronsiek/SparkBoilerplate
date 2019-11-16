package integration

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import cli.{EMRManager, StateManager}
import com.amazonaws.services.elasticmapreduce.model.{DescribeClusterRequest, DescribeClusterResult, DescribeStepRequest, DescribeStepResult, TerminateJobFlowsRequest}
import com.amazonaws.services.elasticmapreduce.waiters.AmazonElasticMapReduceWaiters
import com.amazonaws.waiters.WaiterParameters

import scala.collection.JavaConverters._


class CLIEntryPointSuite extends FlatSpec with BeforeAndAfterAll {

  val emrManager = new EMRManager()
  val state = new StateManager()
  state.removeAll()

  "cli" should "create cluster" in {
    CLIEntryPoint.main(Array("create-cluster", "-c", "demopipelineemr.conf"))
    val clusterId: String = new StateManager().getClusters().head.clusterId
    try {
      new AmazonElasticMapReduceWaiters(emrManager.emr)
        .clusterRunning()
        .run(new WaiterParameters(new DescribeClusterRequest().withClusterId(clusterId))).wait(6000)
    } catch {
      case ex: java.lang.IllegalMonitorStateException =>
      // Caused by not joining waiter threads to the main thread correctly.
      // Gets thrown even when waiter works as intended. Ignoring because dealing with it is too time consuming right now.
    }
    val desc: DescribeClusterResult = emrManager.emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId))
    assert(Set("WAITING", "RUNNING").contains(desc.getCluster.getStatus.getState))
  }

  it should "submit jobs" in {
    val clusterId = state.getClusters().head.clusterId
    CLIEntryPoint.main(Array("spark-submit", "-p", "DemoPipeline", "-c", "demopipeline.conf"))
    val jobId = state.getJobs(clusterId).head.jobId
    val desc: DescribeStepResult =
      emrManager.emr.describeStep(new DescribeStepRequest().withClusterId(clusterId).withStepId(jobId))
    assert(desc.getStep.getStatus.getState == "PENDING")
  }

  it should "terminate clusters" in {
    val clusterId = state.getClusters().head.clusterId
    CLIEntryPoint.main(Array("terminate-cluster", "-c", clusterId))
    val desc: DescribeClusterResult = emrManager.emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId))
    assert(Set("TERMINATED", "TERMINATING").contains(desc.getCluster.getStatus.getState))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val ids = state.getClusters() map (cr => cr.clusterId)
    emrManager.emr.terminateJobFlows(new TerminateJobFlowsRequest(ids.asJava))
  }

}
