resource "aws_iam_role" "emr_role" {
  name = "emr-default-role"
  assume_role_policy = <<-ARP
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "elasticmapreduce.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
ARP
}

resource "aws_iam_policy" "emr_policy" {
  name = "emr-default-policy"
  policy = <<-POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:PutRolePolicy",
        "iam:CreateInstanceProfile",
        "iam:AddRoleToInstanceProfile",
        "iam:ListRoles",
        "iam:GetPolicy",
        "iam:GetInstanceProfile",
        "iam:GetPolicyVersion",
        "iam:AttachRolePolicy",
        "iam:PassRole"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:*",
        "cloudwatch:*",
        "s3:*",
        "application-autoscaling:*"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "*"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:CreateServiceLinkedRole",
      "Resource": "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot*",
      "Condition": {
          "StringLike": {
              "iam:AWSServiceName": "spot.amazonaws.com"
          }
      }
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "emr" {
  policy_arn = aws_iam_policy.emr_policy.arn
  role = aws_iam_role.emr_role.name
}

resource "aws_iam_role" "emr_instance_role" {
  name = "emr-default-instance-role"
  assume_role_policy = <<-ARP
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
ARP
}

resource "aws_iam_policy" "emr_instance_policy" {
  name = "emr-default-instance-policy"
  policy = <<-POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::spark-boilerplate",
        "arn:aws:s3:::spark-boilerplate/*",
        "arn:aws:s3:::elasticmapreduce/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "*"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:*",
        "ec2:Describe*",
        "elasticmapreduce:*",
        "kinesis:*",
        "rds:Describe*",
        "sdb:*",
        "sns:*",
        "sqs:*",
        "glue:*"
      ],
      "Resource": "*"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "emr_instance" {
  policy_arn = aws_iam_policy.emr_instance_policy.arn
  role = aws_iam_role.emr_instance_role.name
}

resource "aws_iam_instance_profile" "emr_instance_profile" {
  name = "emr-default-instance-profile"
  role = aws_iam_role.emr_instance_role.name
}