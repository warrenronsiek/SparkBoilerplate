package cli
import com.amazonaws.services.elasticmapreduce.waiters.AmazonElasticMapReduceWaiters
import com.amazonaws.services.elasticmapreduce.model.{DescribeClusterRequest, DescribeClusterResult, RunJobFlowResult, TerminateJobFlowsRequest}
import com.amazonaws.waiters.WaiterParameters
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite, Suites}
import scala.collection.JavaConverters._

class EMRManagerSpec extends FlatSpec with BeforeAndAfterAll {
  val params = EMRParams(
    subnet = "subnet-b0711ec9",
    name = "test",
    logUri = "s3://spark-boilerplate/logs/",
    serviceRole = "emr-default-role",
    instanceRole = "emr-default-instance-profile",
    key = "warren-laptop",
    instanceCount = 1,
    instanceType = "m4.xlarge")

  val factory = new EMRManager(params)
  val result: RunJobFlowResult = factory.build
  val waiter: Unit = new AmazonElasticMapReduceWaiters(factory.emr)
    .clusterRunning()
    .run(new WaiterParameters(new DescribeClusterRequest().withClusterId(result.getJobFlowId)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    waiter.wait(6000)
  }

  "emr cluster" should "have state RUNNING" in {
    val desc: DescribeClusterResult = factory.emr.describeCluster(new DescribeClusterRequest().withClusterId(result.getJobFlowId))
    println(desc.getCluster.getStatus.getState)
    assert(desc.getCluster.getStatus.getState == "WAITING")
  }

//  override def afterAll(): Unit = {
//    super.afterAll()
//    factory.emr.terminateJobFlows(new TerminateJobFlowsRequest(List(result.getJobFlowId).asJava))
//  }
}
