package cli

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticmapreduce.model.{Application, Configuration, JobFlowInstancesConfig, RunJobFlowRequest, RunJobFlowResult, StepConfig}
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import scala.collection.JavaConverters._


class EMRFactory(emrParams: EMRParams) {
  val credentials_profile: AWSCredentials = try {
    new ProfileCredentialsProvider("default").getCredentials
  } catch {
    case ex: Throwable =>
      throw new AmazonClientException(
        """Cannot load credentials from .aws/credentials file.
          |Make sure that the credentials file exists and that the profile name is defined within it.""".stripMargin,
        ex)
  }
  val emr: AmazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
    .withRegion(Regions.US_WEST_2)
    .build()

  val stepFactory = new StepFactory();
  val enabledebugging: StepConfig = new StepConfig()
    .withName("Enable debugging")
    .withActionOnFailure("TERMINATE_JOB_FLOW")
    .withHadoopJarStep(stepFactory.newEnableDebuggingStep())
  val apps = List(
    new Application().withName("Hive"),
    new Application().withName("Spark"),
    new Application().withName("Ganglia"),
    new Application().withName("Zeppelin")
  )

  val availableCoresPerNode: Int = EC2Data.ec2types(emrParams.instanceType).cores - 1
  val totalAvailableCores: Int = availableCoresPerNode * emrParams.instanceCount // -1 for the node manager's 1 core
  val coresPerExecutor: Int = List(3, 4, 5).reduce((a, b) => {
    if (b % totalAvailableCores == 0) b
    else if (a % totalAvailableCores < b % totalAvailableCores) a else b
  })
  val numExecutors: Int = Math.floor(totalAvailableCores / coresPerExecutor).toInt
  val availableMemoryPerNode: Double = EC2Data.ec2types(emrParams.instanceType).memory - 1 // -1 because 1 gb reserved for node manager
  val totalAvailableMemory: Double = availableMemoryPerNode * emrParams.instanceCount
  val memPerExecutor: Double = Math.floor(totalAvailableMemory / numExecutors)

  val instancesConfig: JobFlowInstancesConfig = new JobFlowInstancesConfig()
    .withEc2SubnetId(emrParams.subnet)
    .withEc2KeyName(emrParams.key)
    .withInstanceCount(emrParams.instanceCount)
    .withKeepJobFlowAliveWhenNoSteps(true)
    .withMasterInstanceType(emrParams.instanceType)
    .withSlaveInstanceType(emrParams.instanceType)
  val configuration: Configuration = new Configuration()
    .withClassification("spark-defaults")
    .withProperties(Map(
      "spark.executor.memory" -> s"${memPerExecutor}g",
      "spark.executor.instances" -> s"$numExecutors",
      "spark.executor.cores" -> s"$coresPerExecutor",
      "spark.default.parallelism" -> s"$totalAvailableCores"
    ).asJava)
    .withClassification("capacity-scheduler")
    .withProperties(Map(
      "yarn.scheduler.capacity.resource-calculator" -> "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    ).asJava)
    .withClassification("yarn-site")
    .withProperties(Map(
      "yarn.nodemanager.resource.memory-mb" -> s"$availableMemoryPerNode",
      "yarn.nodemanager.resource.cpu-vcores" -> s"$availableCoresPerNode"
    ).asJava)


  val request: RunJobFlowRequest = new RunJobFlowRequest()
    .withName(emrParams.name)
    .withReleaseLabel("emr-5.27.0")
    .withSteps(enabledebugging)
    .withApplications(apps: _*)
    .withLogUri(emrParams.logUri)
    .withServiceRole(emrParams.serviceRole)
    .withJobFlowRole(emrParams.instanceRole)
    .withConfigurations(configuration)
    .withInstances(instancesConfig);

  def build: RunJobFlowResult = emr.runJobFlow(request)

}
