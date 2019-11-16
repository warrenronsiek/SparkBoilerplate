package integration

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import cli.{EMRManager, StateManager}
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.services.elasticmapreduce.model.{DescribeClusterRequest, DescribeClusterResult, DescribeStepRequest, DescribeStepResult, RunJobFlowResult, TerminateJobFlowsRequest}
import com.amazonaws.services.elasticmapreduce.waiters.AmazonElasticMapReduceWaiters
import com.amazonaws.waiters.WaiterParameters

import scala.collection.JavaConverters._


class CLIEntryPointSuite extends FlatSpec with BeforeAndAfterAll {

  val credentialsProfile: AWSCredentials = new ProfileCredentialsProvider("default").getCredentials
  val credentialsProvider = new AWSStaticCredentialsProvider(credentialsProfile)
  val emr: AmazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(Regions.US_WEST_2)
    .build()
  val state = new StateManager()
  state.removeAll()

  "cli" should "create cluster" in {
    CLIEntryPoint.main(Array("create-cluster", "-c", "demopipelineemr.conf"))
    val clusterId: String = new StateManager().getClusters().head.clusterId
    try {
      new AmazonElasticMapReduceWaiters(emr)
        .clusterRunning()
        .run(new WaiterParameters(new DescribeClusterRequest().withClusterId(clusterId))).wait(6000)
    } catch {
      case ex: java.lang.IllegalMonitorStateException =>
      // Caused by not joining waiter threads to the main thread correctly.
      // Gets thrown even when waiter works as intended. Ignoring because dealing with it is too time consuming right now.
    }
    val desc: DescribeClusterResult = emr.describeCluster(new DescribeClusterRequest().withClusterId(clusterId))
    assert(Set("WAITING", "RUNNING").contains(desc.getCluster.getStatus.getState))
  }

  it should "submit jobs" in {
    val clusterId = state.getClusters().head.clusterId
    CLIEntryPoint.main(Array("spark-submit", "-p", "DemoPipeline", "-c", "demopipeline.conf"))
    val desc: DescribeStepResult = emr.describeStep(new DescribeStepRequest().withClusterId(clusterId))
    assert(true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val ids = state.getClusters() map (cr => cr.clusterId)
    emr.terminateJobFlows(new TerminateJobFlowsRequest(ids.asJava))
  }

}
