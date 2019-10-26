name := "SparkBoilerplate"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
//  "software.amazon.awssdk" % "aws-sdk-java" % "2.0.0-preview-1",
  "org.rogach" %% "scallop" % "3.3.1",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "com.amazon.deequ" % "deequ" % "1.0.2",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)