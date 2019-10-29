package cli

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticmapreduce.model.{Application, JobFlowInstancesConfig, RunJobFlowRequest, RunJobFlowResult, StepConfig}
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}

class EMRFactory {
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

  val request: RunJobFlowRequest = new RunJobFlowRequest()
    .withName("MyClusterCreatedFromJava")
    .withReleaseLabel("emr-5.20.0") // specifies the EMR release version label, we recommend the latest release
    .withSteps(enabledebugging)
    .withApplications(apps : _*)
    .withLogUri("s3://path/to/my/emr/logs") // a URI in S3 for log files is required when debugging is enabled
    .withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
    .withJobFlowRole("EMR_EC2_DefaultRole") // replace the default with a custom EMR role for the EC2 instance profile if one is used
    .withInstances(new JobFlowInstancesConfig()
      .withEc2SubnetId("subnet-12ab34c56")
      .withEc2KeyName("myEc2Key")
      .withInstanceCount(3)
      .withKeepJobFlowAliveWhenNoSteps(true)
      .withMasterInstanceType("m4.large")
      .withSlaveInstanceType("m4.large"));

  val result: RunJobFlowResult = emr.runJobFlow(request);
  System.out.println("The cluster ID is " + result.toString);

}
