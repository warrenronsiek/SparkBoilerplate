package com.warrenronsiek.utils

import java.io.{File, FileInputStream, InputStream}
import java.util.jar.JarFile

import scala.collection.JavaConverters._
import com.typesafe.config.Config

class ResourceStream(fileName: String) {
  private val file = new File(getClass.getResource("/" + fileName).getPath)
  val stream: InputStream = if (!file.exists) {
    val jarFile = new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath)
    val jar = new JarFile(jarFile)
    val file: String = jar.entries().asScala
      .filter(!_.isDirectory)
      .filter(entry => entry.getName.contains(fileName))
      .map(entry => entry.toString).toList.head
    getClass.getResourceAsStream("/" + file)
  } else {
    new FileInputStream(file)
  }
}
