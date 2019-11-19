package utils

import java.io.{File, InputStream}
import java.nio.file.Files
import java.util.jar.JarFile

import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}

/** The point of this class is to read various configs from the resources directory. The reason it is necessary is that
 * the standard .getResource method doesn't work after being compiled into a jar. Instead, you need to switch between
 * .getResource and .getResourceAsStream depending on whether or not the class has been compiled.
 *
 * @param configName the name of the configuration file, including extension, not including directory name
 */
class ResourceReader(configName: String) {

  private val file = new File(getClass.getResource("/" + configName).getPath)
  val config: Config = if (!file.exists) {
    val jarFile = new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath)
    val jar = new JarFile(jarFile)
    val configs = jar.entries().asScala
      .filter(!_.isDirectory)
      .filter(entry => entry.getName.contains(configName))
      .map(entry => entry.toString).toList
    try {
      val cfg_input = getClass.getResourceAsStream("/" + configs.head)
      val cfg = scala.io.Source.fromInputStream(cfg_input).mkString
      ConfigFactory.parseString(cfg)
    } catch {
      case ex: NullPointerException => throw new Error("couldn't get the config")
    }
  } else {
    ConfigFactory.parseFile(file)
  }
}
