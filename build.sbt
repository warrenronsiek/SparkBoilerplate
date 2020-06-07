name := "SparkBoilerplate"

version := "0.1"
resolvers += Resolver.bintrayRepo("elderresearch", "OSS")
scalaVersion := "2.11.12"
organization := "com.warren-r"
mainClass in (Compile, run) := Some("scala.CLIEntryPoint")
mainClass in (Compile, packageBin) := Some("scala.CLIEntryPoint")


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.4"  % "provided",
  "com.amazonaws" % "aws-java-sdk" % "1.11.659",  //the aws-java-sdk v2 has a netty dependency that conflicts with spark
  "org.rogach" %% "scallop" % "3.3.1",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "com.amazon.deequ" % "deequ" % "1.0.2",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "com.iheart" %% "ficus" % "1.4.7",
  "org.xerial" % "sqlite-jdbc" % "3.28.0",
  "org.eclipse.jgit" % "org.eclipse.jgit" % "5.5.1.201910021850-r"
)

test in assembly := {}

assemblyJarName in assembly := "SparkBoilerplate.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}