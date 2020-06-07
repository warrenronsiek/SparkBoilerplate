package com.warrenronsiek.cli

import java.io.File
import java.util.concurrent.Future

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.event.{ProgressEvent, ProgressListener}
import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticmapreduce.model.{AddJobFlowStepsRequest, AddJobFlowStepsResult, Application, Configuration, HadoopJarStepConfig, InstanceGroupConfig, JobFlowInstancesConfig, RunJobFlowRequest, RunJobFlowResult, StepConfig, TerminateJobFlowsRequest, TerminateJobFlowsResult}
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceAsync, AmazonElasticMapReduceAsyncClientBuilder, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, PutObjectResult}
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.Config
import org.eclipse.jgit.lib.{ConfigConstants, RepositoryBuilder}
import org.apache.log4j.Logger
import com.warrenronsiek.utils.{ResourceReader, ResourceStream}
import scala.collection.JavaConverters._

class EMRManager {
  val logger: Logger = Logger.getLogger(this.getClass)
  val stateManager = StateManager()
  val config: Config = new ResourceReader("emrmanager.conf").config

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
  val emrAsync: AmazonElasticMapReduceAsync =  AmazonElasticMapReduceAsyncClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(Regions.US_WEST_2)
    .build()
  private val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(Regions.US_WEST_2)
    .build()
  private val transferManager: TransferManager = TransferManagerBuilder.standard()
    .withS3Client(s3)
    .build()
  private val stepFactory = new StepFactory("elasticmapreduce")



  def build(emrParams: EMRParams): RunJobFlowResult = {
    val enabledebugging: StepConfig = new StepConfig()
      .withName("Enable debugging")
      .withActionOnFailure("TERMINATE_JOB_FLOW")
      .withHadoopJarStep(stepFactory.newEnableDebuggingStep())

    val apps = List(
      new Application().withName("Hadoop"),
      new Application().withName("Hive"),
      new Application().withName("Spark"),
      new Application().withName("Zeppelin")
    )

    val availableCoresPerNode: Int = EC2Data.ec2types(emrParams.instanceType).cores - 1 // -1 for the node manager's 1 core
    val totalAvailableCores: Int = availableCoresPerNode * emrParams.instanceCount
    val coresPerExecutor: Int = {
      val optimalCoresPerExecutor: Int = List(3, 4, 5, 6).reduce((a, b) => {
        if (b % totalAvailableCores == 0) b
        else if (a % totalAvailableCores < b % totalAvailableCores) a else b
      })
      if (optimalCoresPerExecutor > availableCoresPerNode) {
        availableCoresPerNode
      } else {
        optimalCoresPerExecutor
      }
    }
    logger.info("coresPerExecutor: " + coresPerExecutor)
    val numExecutors: Int = Math.floor(totalAvailableCores / coresPerExecutor).toInt
    logger.info("numExecutors: " + numExecutors)
    val availableMemoryPerNode: Double = EC2Data.ec2types(emrParams.instanceType).memory - 1 // -1 because 1 gb reserved for node manager
    logger.info("availableMemoryPerNode: " + availableMemoryPerNode)
    val totalAvailableMemory: Double = availableMemoryPerNode * emrParams.instanceCount
    logger.info("totalAvailableMemory: " + totalAvailableMemory)
    val memPerExecutor: Double = {
      val optimalMemPerExecutor = Math.floor(totalAvailableMemory / numExecutors)
      if (optimalMemPerExecutor > availableMemoryPerNode) {
        availableMemoryPerNode
      } else {
        optimalMemPerExecutor
      }
    }
    logger.info("memPerExecutor: " + memPerExecutor)
    val instancesConfig: JobFlowInstancesConfig = new JobFlowInstancesConfig()
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
    val configuration: Configuration = new Configuration()
      .withClassification("spark-defaults")
      .withProperties(Map(
        "spark.executor.memory" -> s"${memPerExecutor}g",
        "spark.executor.instances" -> s"$numExecutors",
        "spark.executor.cores" -> s"$coresPerExecutor",
        "spark.default.parallelism" -> s"$totalAvailableCores",
        "spark.sql.shuffle.partitions" -> s"$totalAvailableCores"
      ).asJava)
      .withClassification("capacity-scheduler")
      .withProperties(Map(
        "yarn.scheduler.increment-allocation-vcores" -> "1",
        "yarn.scheduler.capacity.resource-calculator" -> "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
      ).asJava)
      .withClassification("spark")
      .withProperties(Map("maximizeResourceAllocation" -> "true").asJava)
      .withClassification("yarn-site")
      .withProperties(Map(
        "log-aggregation-enable" -> "true"
//        "yarn.nodemanager.resource.memory-mb" -> s"${availableMemoryPerNode * 1024}",
//        "yarn.nodemanager.resource.cpu-vcores" -> s"$availableCoresPerNode",
//        "yarn.scheduler.maximum-allocation-mb" -> s"${memPerExecutor * 1024}"
      ).asJava)
    val request: RunJobFlowRequest = new RunJobFlowRequest()
      .withName(emrParams.name)
      .withReleaseLabel("emr-6.0.0")
      .withSteps(enabledebugging)
      .withApplications(apps: _*)
      .withLogUri(emrParams.logUri)
      .withServiceRole(emrParams.serviceRole)
      .withJobFlowRole(emrParams.instanceRole)
      .withConfigurations(configuration)
      .withInstances(instancesConfig)

    val result = emr.runJobFlow(request)
    stateManager.addCluster(result.getJobFlowId, emrParams.name)
    result
  }

  def terminate(clusterId: String): TerminateJobFlowsResult = {
    val result = emr.terminateJobFlows(new TerminateJobFlowsRequest(List(clusterId).asJava))
    stateManager.removeClusterById(clusterId)
    result
  }

  def terminate(): TerminateJobFlowsResult = {
    val clusterIds = stateManager.getClusters().map(cluster => cluster.clusterId)
    val result = emr.terminateJobFlows(new TerminateJobFlowsRequest(clusterIds.asJava))
    stateManager.removeAll()
    result
  }

  def submitLocalJob(pipelineName: String, configFileName: String): Unit = {
    val repo = new RepositoryBuilder().readEnvironment().findGitDir().build()
    val branch: String = repo.getBranch
    val user: String = repo.getConfig.getString(ConfigConstants.CONFIG_USER_SECTION, null, "name")

    /**
     * Upload the jar
     */

    val localJarPath = List(System.getProperty("user.dir"), "target", "scala-2.11", "SparkBoilerplate.jar").mkString("/")
    val remoteJarPath = List("jars", user, s"$branch.jar").mkString("/")
    val putObjectRequest = new PutObjectRequest("spark-boilerplate", remoteJarPath, new File(localJarPath))
    var transferredBytes: Long = 0
    putObjectRequest.setGeneralProgressListener(new ProgressListener {
      override def progressChanged(progressEvent: ProgressEvent): Unit = {
        transferredBytes += progressEvent.getBytesTransferred
        logger.debug(s"Transferred MB: ${transferredBytes / 1000000}")
      }
    })
    val upload = transferManager.upload(putObjectRequest)
    upload.waitForCompletion()
    logger.info("Completed jar upload")

    /**
     * Upload the script that copies the jar to the cluster
     */

    val clusterId = stateManager.getClusters().head.clusterId
    val copyJarsScriptLocation = s"s3://${config.getString("s3.staging-bucket-name")}/scripts/copy_jars.sh"
    val metadata: ObjectMetadata = new ObjectMetadata()
    metadata.setContentLength(19)
    s3.putObject(new PutObjectRequest(
      config.getString("s3.staging-bucket-name"),
      copyJarsScriptLocation,
      new ResourceStream("copy_jars.sh").stream,
      metadata
    ))
    logger.info(s"uploaded the script to s3://${config.getString("s3.staging-bucket-name")}/$copyJarsScriptLocation")

    /**
     * Create EMR steps that run the job to copy the jar
     */

    val copyJars: StepConfig = new StepConfig()
      .withName("Copy execution jar")
      .withActionOnFailure("TERMINATE_JOB_FLOW")
      .withHadoopJarStep(stepFactory
        .newScriptRunnerStep(copyJarsScriptLocation)
        .withArgs(s"s3://${config.getString("s3.staging-bucket-name")}/" + remoteJarPath, "/home/hadoop/" + remoteJarPath.split("/").last))
    logger.info("Submitted copy jar step")
    emr.addJobFlowSteps(new AddJobFlowStepsRequest(clusterId).withSteps(copyJars))

    /**
     * Add the steps to run the jar
     */

    val jobFlowStep = emr.addJobFlowSteps(
      new AddJobFlowStepsRequest(clusterId)
        .withSteps(new StepConfig(pipelineName, new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs("spark-submit")
          .withArgs("--class", "CLIEntryPoint")
          .withArgs("--master", "yarn")
          .withArgs("--deploy-mode", "cluster")
          //        .withArgs("--properties-file", "spark-defaults.conf")
          .withArgs(s"../../../../../../home/hadoop/$branch.jar")
          .withArgs("run-pipeline")
          .withArgs("-p", pipelineName)
          .withArgs("-c", configFileName))
          .withActionOnFailure("CONTINUE")))
    stateManager.addJob(clusterId, jobFlowStep.getStepIds.asScala.head)
    logger.info("Submitted job step")
  }
}
