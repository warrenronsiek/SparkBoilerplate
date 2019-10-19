name := "SparkBoilerplate"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "software.amazon.awssdk" % "aws-sdk-java" % "2.0.0-preview-1"
)