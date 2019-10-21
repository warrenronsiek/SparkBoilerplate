SparkBoilerplate
================

Simple spark project template to be used/referenced in future development.

Features (WIP)
--------
- [x] testing with deequ
  - [x] deequ in integration tests
  - [x] deequ as a pipeline runtime validation mixin
- [ ] Snapshot based dataframe testing
- [ ] Transformers for AWS Athena - query intermediate tables in Athena
- [ ] Pipeline S3 checkpoints save state to s3 and, in the event of failure, re-run the pipeline from the last checkpoint
- [ ] Local docker container for profiling integration tests
- [ ] Command line interface for launching jobs and creating clusters

Testing with Deequ
------------------
[Deequ](https://github.com/awslabs/deequ) is a data testing library from AWS. The idea is to use it in integration
tests to see if the data output from pipelines makes sense, but more importantly, Deequ is used to run tests _during runtime_.
The idea is that changes is pipeline logic may not be noticeable just on test data, and that these changes can have
unforeseen consequences on your live data. Having Deequ do testing as part of the pipeline can works to mitigate
these issues.
