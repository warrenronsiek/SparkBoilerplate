run:
	sbt -J-Xms2048m -J-Xmx2048m assembly
	chmod 754 ./target/scala-2.11/SparkBoilerplate-assembly-0.1.jar
	java -jar ./target/scala-2.11/SparkBoilerplate-assembly-0.1.jar create-cluster -c demopipelineemr.conf
# 	scala ./target/scala-2.11/SparkBoilerplate-assembly-0.1.jar spark-submit-local -p $(pipeline) -c $(pipeconfig)
