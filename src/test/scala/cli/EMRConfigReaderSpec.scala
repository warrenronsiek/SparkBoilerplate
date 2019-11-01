package cli

import org.scalatest.FlatSpec


class EMRConfigReaderSpec extends FlatSpec{
  val emrParams: EMRParams = new EMRConfigReader(
    List(System.getProperty("user.dir"), "src", "test", "resources", "test.conf").mkString("/")).getParams

  "emr params" should "parse subnet" in {
    assert(emrParams.subnet == "subnet-b0711ec9")
  }

  it should "be named test" in {
    assert(emrParams.name == "test")
  }

  it should "have the logUri" in {
    assert(emrParams.logUri == "s3://spark-boilerplate/logs/")
  }

  it should "have the service role" in {
    assert(emrParams.serviceRole == "emr-default-role")
  }

  it should "have the instance role" in {
    assert(emrParams.instanceRole == "emr-default-instance-role")
  }

  it should "have the key" in {
    assert(emrParams.key == "warren-laptop")
  }

  it should "have instance count" in {
    assert(emrParams.instanceCount == 1)
  }

  it should "have instance type" in {
    assert(emrParams.instanceType == "m4.xlarge")
  }
}
