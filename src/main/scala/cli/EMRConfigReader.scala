package cli

import com.sun.tools.corba.se.idl.InvalidArgument
import net.ceedubs.ficus.Ficus._
import com.typesafe.config.{Config, ConfigFactory}
import EC2Data.ec2types

class EMRConfigReader(configPath: String) {

  System.setProperty("config.file", configPath)
  ConfigFactory.invalidateCaches()
  val config: Config = ConfigFactory.load()

  private def validateSubnet(subnet: String): String = {
    """subnet-\S+""".r.findFirstIn(subnet.toString) match {
      case Some(subnet: String) => subnet
      case None => throw new InvalidArgument("Subnet string has incorrect format")
    }
  }

  private def validateLogUri(logUri: String): String = {
    """s3://\S+""".r.findFirstIn(logUri.toString) match {
      case Some(logUri: String) => logUri
      case None => throw new InvalidArgument("LogUri is incorrect")
    }
  }

  private def validateInstanceType(instanceType: String): String = {
    if (ec2types.keys.toIterator.contains(instanceType)) {
      instanceType
    } else {
      throw new InvalidArgument("Invalid instance type")
    }
  }

  val getParams: EMRParams = EMRParams(
    validateSubnet(config.as[String]("subnet")),
    config.as[String]("name"),
    validateLogUri(config.as[String]("logUri")),
    config.as[String]("serviceRole"),
    config.as[String]("instanceRole"),
    config.as[String]("key"),
    config.as[Int]("instanceCount"),
    validateInstanceType(config.as[String]("instanceType")),
    config.as[Option[Double]]("bidPrice")
  )
}
