SparkBoilerplate
================

Simple spark project template to be used/referenced in future development.

Features (WIP)
--------
- [ ] testing with deequ
  - [x] deequ in integration tests
  - [ ] deequ as a pipeline runtime validation mixin
- [ ] Snapshot based dataframe testing
- [ ] Transformers for AWS Athena - query intermediate tables in Athena
- [ ] Pipeline S3 checkpoints save state to s3 and, in the event of failure, re-run the pipeline from the last checkpoint
- [ ] Local docker container for profiling integration tests
- [ ] Command line interface for launching jobs and creating clusters