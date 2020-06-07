run:
	sbt -J-Xms2048m -J-Xmx2048m assembly
	chmod 754 ./target/scala-2.11/SparkBoilerplate.jar
	java -jar ./target/scala-2.11/SparkBoilerplate.jar create-cluster -c demopipelineemr.conf
#	java -jar ./target/scala-2.11/SparkBoilerplate-assembly-0.1.jar spark-submit-local -p DemoPipeline -c demopipeline.conf
