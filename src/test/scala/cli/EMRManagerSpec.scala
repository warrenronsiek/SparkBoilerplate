package cli

import com.amazonaws.services.elasticmapreduce.waiters.AmazonElasticMapReduceWaiters
import com.amazonaws.services.elasticmapreduce.model.{DescribeClusterRequest, DescribeClusterResult, RunJobFlowResult, TerminateJobFlowsRequest}
import com.amazonaws.waiters.WaiterParameters
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite, Suites}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class EMRManagerSpec extends FlatSpec with BeforeAndAfterAll {
  val params = EMRParams(
    subnet = "subnet-b0711ec9",
    name = "test",
    logUri = "s3://spark-boilerplate/logs/",
    serviceRole = "emr-default-role",
    instanceRole = "emr-default-instance-profile",
    key = "warren-laptop",
    instanceCount = 2,
    instanceType = "m4.xlarge")

  val spotParams = EMRParams(
    subnet = "subnet-b0711ec9",
    name = "test",
    logUri = "s3://spark-boilerplate/logs/",
    serviceRole = "emr-default-role",
    instanceRole = "emr-default-instance-profile",
    key = "warren-laptop",
    instanceCount = 2,
    instanceType = "m4.xlarge",
    bidPrice = Some(.40)
  )

  case class ClusterBuild(factory: EMRManager, result: RunJobFlowResult)

  var jobFlowIds: ListBuffer[String] = ListBuffer()

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
    }
    jobFlowIds += result.getJobFlowId
    ClusterBuild(factory, result)
  }

  "emr cluster" should "have state WAITING" in {
    val builder = buildCluster(params)
    val desc: DescribeClusterResult = builder.factory.emr.describeCluster(new DescribeClusterRequest()
      .withClusterId(builder.result.getJobFlowId))
    assert(desc.getCluster.getStatus.getState == "WAITING")
  }

//  "emr spot cluster" should "have state RUNNING" in {
//    val builder = buildCluster(spotParams)
//    val desc: DescribeClusterResult = builder.factory.emr.describeCluster(new DescribeClusterRequest()
//      .withClusterId(builder.result.getJobFlowId))
//    assert(desc.getCluster.getStatus.getState == "RUNNING")
//  }

  override def afterAll(): Unit = {
    super.afterAll()
    val terminator = new EMRManager()
    jobFlowIds foreach { x =>
      terminator.terminate(x)
    }
  }
}
