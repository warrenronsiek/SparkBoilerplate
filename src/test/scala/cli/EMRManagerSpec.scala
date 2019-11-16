package cli

import com.amazonaws.services.elasticmapreduce.waiters.AmazonElasticMapReduceWaiters
import com.amazonaws.services.elasticmapreduce.model.{DescribeClusterRequest, DescribeClusterResult, RunJobFlowResult, TerminateJobFlowsRequest}
import com.amazonaws.waiters.WaiterParameters
import org.scalatest.{BeforeAndAfterEach, FlatSpec, ParallelTestExecution}
import scala.collection.JavaConverters._

class EMRManagerSpec extends FlatSpec with BeforeAndAfterEach with ParallelTestExecution {

  val params = EMRParams(
    subnet = "subnet-b0711ec9",
    name = "test",
    logUri = "s3://spark-boilerplate/logs/",
    serviceRole = "emr-default-role",
    instanceRole = "emr-default-instance-profile",
    key = "warren-laptop",
    instanceCount = 2,
    instanceType = "m5.xlarge")

  val spotParams = EMRParams(
    subnet = "subnet-b0711ec9",
    name = "test",
    logUri = "s3://spark-boilerplate/logs/",
    serviceRole = "emr-default-role",
    instanceRole = "emr-default-instance-profile",
    key = "warren-laptop",
    instanceCount = 2,
    instanceType = "m5.xlarge",
    bidPrice = Some(.40)
  )

  case class ClusterBuild(factory: EMRManager, result: RunJobFlowResult)

  def buildCluster(params: EMRParams): ClusterBuild = {
    val factory = new EMRManager(params)
    val result: RunJobFlowResult = factory.build
    try {
      val waiter: Unit = new AmazonElasticMapReduceWaiters(factory.emr)
        .clusterRunning()
        .run(new WaiterParameters(new DescribeClusterRequest().withClusterId(result.getJobFlowId))).wait(6000)
    }
    catch {
      case ex: java.lang.IllegalMonitorStateException =>
      // Caused by not joining waiter threads to the main thread correctly.
      // Gets thrown even when waiter works as intended. Ignoring because dealing with it is too time consuming right now.
    }
    ClusterBuild(factory, result)
  }

  "emr cluster" should "have state in WAITING, RUNNING" in {
    val builder = buildCluster(params)
    val desc: DescribeClusterResult = builder.factory.emr.describeCluster(new DescribeClusterRequest()
      .withClusterId(builder.result.getJobFlowId))
    try {
      assert(Set("WAITING", "RUNNING").contains(desc.getCluster.getStatus.getState))
    } finally {
      builder.factory.emr.terminateJobFlows(new TerminateJobFlowsRequest(List(builder.result.getJobFlowId).asJava))
    }

  }

  it should "have state in WAITING, RUNNING when using spot" in {
    val builder = buildCluster(spotParams)
    val desc: DescribeClusterResult = builder.factory.emr.describeCluster(new DescribeClusterRequest()
      .withClusterId(builder.result.getJobFlowId))
    try {
      assert(Set("WAITING", "RUNNING").contains(desc.getCluster.getStatus.getState))
    } finally {
      builder.factory.emr.terminateJobFlows(new TerminateJobFlowsRequest(List(builder.result.getJobFlowId).asJava))
    }
  }
}
