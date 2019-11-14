package cli

import java.io.File

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticmapreduce.model.{AddJobFlowStepsRequest, Application, Configuration, HadoopJarStepConfig, InstanceGroupConfig, JobFlowInstancesConfig, RunJobFlowRequest, RunJobFlowResult, StepConfig, TerminateJobFlowsRequest, TerminateJobFlowsResult}
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.services.s3.model.PutObjectResult
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.eclipse.jgit.lib.{ConfigConstants, RepositoryBuilder}

import scala.collection.JavaConverters._


class EMRManager(emrParams: EMRParams) {
  val stateManager = StateManager()

  def this() = this(EMRParams("", "", "", "", "", "", 0, "")) //TODO: this is a hack to be able to use the credentials profile in the constructor to terminate clusters even if no params are passed. This should be refactored.

  private val credentialsProfile: AWSCredentials = try {
    new ProfileCredentialsProvider("default").getCredentials
  } catch {
    case ex: Throwable =>
      throw new AmazonClientException(
        """Cannot load credentials from .aws/credentials file.
          |Make sure that the credentials file exists and that the profile name is defined within it.""".stripMargin,
        ex)
  }
  private val credentialsProvider = new AWSStaticCredentialsProvider(credentialsProfile)
  val emr: AmazonElasticMapReduce = AmazonElasticMapReduceClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(Regions.US_WEST_2)
    .build()
  private val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(Regions.US_WEST_2)
    .build()

  private val stepFactory = new StepFactory("elasticmapreduce");
  private val enabledebugging: StepConfig = new StepConfig()
    .withName("Enable debugging")
    .withActionOnFailure("TERMINATE_JOB_FLOW")
    .withHadoopJarStep(stepFactory.newEnableDebuggingStep())
  private val apps = List(
    new Application().withName("Hadoop"),
    new Application().withName("Hive"),
    new Application().withName("Spark"),
    new Application().withName("Zeppelin")
  )

  private val availableCoresPerNode: Int = EC2Data.ec2types(emrParams.instanceType).cores - 1 // -1 for the node manager's 1 core
  private val totalAvailableCores: Int = availableCoresPerNode * emrParams.instanceCount
  private val coresPerExecutor: Int = List(3, 4, 5).reduce((a, b) => {
    if (b % totalAvailableCores == 0) b
    else if (a % totalAvailableCores < b % totalAvailableCores) a else b
  })
  private val numExecutors: Int = Math.floor(totalAvailableCores / coresPerExecutor).toInt
  private val availableMemoryPerNode: Double = EC2Data.ec2types(emrParams.instanceType).memory - 1 // -1 because 1 gb reserved for node manager
  private val totalAvailableMemory: Double = availableMemoryPerNode * emrParams.instanceCount
  private val memPerExecutor: Double = Math.floor(totalAvailableMemory / numExecutors)

  private val instancesConfig: JobFlowInstancesConfig = new JobFlowInstancesConfig()
    .withEc2SubnetId(emrParams.subnet)
    .withEc2KeyName(emrParams.key)
    .withKeepJobFlowAliveWhenNoSteps(true)

    emrParams.bidPrice match {
      case Some(price) =>
        instancesConfig
          .withInstanceGroups(
            new InstanceGroupConfig("MASTER", emrParams.instanceType, 1)
              .withMarket("SPOT")
              .withBidPrice(price.toString),
            new InstanceGroupConfig("CORE", emrParams.instanceType, emrParams.instanceCount - 1)
              .withMarket("SPOT")
              .withBidPrice(price.toString)
          )
      case None =>
        instancesConfig
          .withMasterInstanceType(emrParams.instanceType)
          .withSlaveInstanceType(emrParams.instanceType)
          .withInstanceCount(emrParams.instanceCount)
    }

  private val configuration: Configuration = new Configuration()
    .withClassification("spark-defaults")
    .withProperties(Map(
      "spark.executor.memory" -> s"${memPerExecutor}g",
      "spark.executor.instances" -> s"$numExecutors",
      "spark.executor.cores" -> s"$coresPerExecutor",
      "spark.default.parallelism" -> s"$totalAvailableCores"
    ).asJava)
//    .withClassification("capacity-scheduler")
//    .withProperties(Map(
//      "yarn.scheduler.capacity.resource-calculator" -> "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
//    ).asJava)
//    .withClassification("yarn-site")
//    .withProperties(Map(
//      "yarn.nodemanager.resource.memory-mb" -> s"$availableMemoryPerNode",
//      "yarn.nodemanager.resource.cpu-vcores" -> s"$availableCoresPerNode"
//    ).asJava)


  private val request: RunJobFlowRequest = new RunJobFlowRequest()
    .withName(emrParams.name)
    .withReleaseLabel("emr-5.28.0")
    .withSteps(enabledebugging)
    .withApplications(apps: _*)
    .withLogUri(emrParams.logUri)
    .withServiceRole(emrParams.serviceRole)
    .withJobFlowRole(emrParams.instanceRole)
    .withConfigurations(configuration)
    .withInstances(instancesConfig)

  def build: RunJobFlowResult = {
    val result = emr.runJobFlow(request)
    stateManager.addCluster(result.getJobFlowId, emrParams.name)
    result
  }

  def terminate(clusterId: String): TerminateJobFlowsResult = {
    val result = emr.terminateJobFlows(new TerminateJobFlowsRequest(List(clusterId).asJava))
    stateManager.removeCluster(emrParams.name)
    result
  }

  def terminate(): TerminateJobFlowsResult = {
    val clusterIds = stateManager.getClusters().map(cluster => cluster.clusterId)
    val result = emr.terminateJobFlows(new TerminateJobFlowsRequest(clusterIds.asJava))
    val removals = stateManager.removeAll()
    result
  }

  def submitJob(pipelineName: String) = {
    val repo = new RepositoryBuilder().readEnvironment().findGitDir().build()
    val branch: String = repo.getBranch
    val user: String = repo.getConfig.getString(ConfigConstants.CONFIG_USER_SECTION,"user", "name")
    val localJarPath = List(System.getProperty("user.dir"), "target", "scala-2.11", "sparkboilerplate_2.11-0.1.jar").mkString("/")
    val remoteJarPath = List("jars", user, s"$branch.jar").mkString("/")
    val putJar: PutObjectResult = s3.putObject("spark-boilerplate", remoteJarPath, new File(localJarPath))
    emr.addJobFlowSteps(new AddJobFlowStepsRequest(stateManager.getClusters().head.clusterId)
      .withSteps(new StepConfig(pipelineName, new HadoopJarStepConfig()
        .withJar(remoteJarPath))))
  }

}
