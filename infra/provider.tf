terraform {
  backend "s3" {
    bucket = "spark-boilerplate"
    key = "infra"
    region = "us-west-2"
  }
}

provider "aws" {
  region = "us-west-2"
}