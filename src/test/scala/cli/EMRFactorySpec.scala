package cli

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Suite, Suites}
import cli.EMRFactory

class EMRFactorySpec extends Suites {

  val params = EMRParams("subnet-b0711ec9", "test", "s3://spark-boilerplate/logs/", "emr-default-role",
    "emr-default-instance-role", "warren-laptop", 1, "m4.xlarge")

  val factory = new EMRFactory(params)



  println("foo")

}
