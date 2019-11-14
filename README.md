SparkBoilerplate
================

Simple spark project template to be used/referenced in future development.

Features (WIP)
--------------
- [x] runtime/integration testing with deequ
- [x] snapshot based unit testing
- [ ] command line interface for launching jobs and creating clusters
- [ ] transformers for AWS Athena - query intermediate tables in Athena
- [ ] pipeline S3 checkpoints save state to s3 and, in the event of failure, re-run the pipeline from the last checkpoint
- [ ] local docker container for profiling integration tests
- [ ] airflow pipeline orchestration
- [ ] CI/CD

Testing with Deequ
------------------
[Deequ](https://github.com/awslabs/deequ) is a data testing library from AWS. The idea is to use it in integration
tests to see if the data output from pipelines makes sense, but more importantly, Deequ is used to run tests _during runtime_.
The idea is that changes is pipeline logic may not be noticeable just on test data, and that these changes can have
unforeseen consequences on your live data. Having Deequ do testing as part of the pipeline can works to mitigate
these issues.

Snapshot Testing
----------------
Instead of painstakingly writing out the passing criteria for a test on a dataframe, the idea is to save
a copy of the dataframe and then compare all future test rus against the copy. Writing tests like this
is much less of a chore and encourages high test converge. This is particularly true for unit tests where 
using Deequ doesn't make much sense. 

CLI Tool (IN PROGRESS)
--------
The compiled jar can be called (with various arguments) to create EMR clusters and submit jobs that are part of the
project to the cluster. This is extremely good for multiple teams where each person can spin up their own clusters to
run/test their jobs with whatever modifications they have made. Its also helpful for launching clusters from other 
environments. E.g. as part of integration or pipeline tests.
