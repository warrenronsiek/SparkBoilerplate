package com.warrenronsiek.cli

case object EC2Data {

  val ec2types: Map[String, EC2Info] = Map(
    "m4.4xlarge" -> EC2Info(64.0, 16),
    "m4.large" -> EC2Info(8.0, 2),
    "m4.xlarge" -> EC2Info(16.0, 4),
    "m4.2xlarge" -> EC2Info(32.0, 8),
    "m4.10xlarge" -> EC2Info(160.0, 40),
    "m4.16xlarge" -> EC2Info(256.0, 64),

    "m5.4xlarge" -> EC2Info(64.0, 16),
    "m5.xlarge" -> EC2Info(16.0, 4),
    "m5.8xlarge" -> EC2Info(128.0, 32),
    "m5.2xlarge" -> EC2Info(32.0, 8),
    "m5.24xlarge" -> EC2Info(384.0, 96),
    "m5.16xlarge" -> EC2Info(256.0, 64),
    "m5.12xlarge" -> EC2Info(192.0, 48),

    "m5a.4xlarge" -> EC2Info(64.0, 16),
    "m5a.xlarge" -> EC2Info(16.0, 4),
    "m5a.2xlarge" -> EC2Info(32.0, 8),
    "m5a.24xlarge" -> EC2Info(384.0, 96),
    "m5a.12xlarge" -> EC2Info(192.0, 48),

    "m5d.4xlarge" -> EC2Info(64.0, 16),
    "m5d.xlarge" -> EC2Info(16.0, 4),
    "m5d.2xlarge" -> EC2Info(32.0, 8),
    "m5d.24xlarge" -> EC2Info(384.0, 96),
    "m5d.12xlarge" -> EC2Info(192.0, 48),

    "c4.4xlarge" -> EC2Info(30.0, 16),
    "c4.large" -> EC2Info(3.75, 2),
    "c4.xlarge" -> EC2Info(7.5, 4),
    "c4.8xlarge" -> EC2Info(60.0, 36),
    "c4.2xlarge" -> EC2Info(15.0, 8),

    "c5.4xlarge" -> EC2Info(32.0, 16),
    "c5.xlarge" -> EC2Info(8.0, 4),
    "c5.2xlarge" -> EC2Info(16.0, 8),
    "c5.9xlarge" -> EC2Info(72.0, 36),
    "c5.18xlarge" -> EC2Info(144.0, 72),

    "c5d.4xlarge" -> EC2Info(32.0, 16),
    "c5d.xlarge" -> EC2Info(8.0, 4),
    "c5d.2xlarge" -> EC2Info(16.0, 8),
    "c5d.9xlarge" -> EC2Info(72.0, 36),
    "c5d.18xlarge" -> EC2Info(144.0, 72),

    "c5n.4xlarge" -> EC2Info(42.0, 16),
    "c5n.xlarge" -> EC2Info(10.5, 4),
    "c5n.2xlarge" -> EC2Info(21.0, 8),
    "c5n.9xlarge" -> EC2Info(96.0, 36),
    "c5n.18xlarge" -> EC2Info(192.0, 72),

    "z1d.xlarge" -> EC2Info(32.0, 4),
    "z1d.2xlarge" -> EC2Info(64.0, 8),
    "z1d.6xlarge" -> EC2Info(192.0, 24),
    "z1d.3xlarge" -> EC2Info(96.0, 12),
    "z1d.12xlarge" -> EC2Info(384.0, 48),

    "r3.4xlarge" -> EC2Info(122.0, 16),
    "r3.large" -> EC2Info(15.25, 2),
    "r3.xlarge" -> EC2Info(30.5, 4),
    "r3.8xlarge" -> EC2Info(244.0, 32),
    "r3.2xlarge" -> EC2Info(61.0, 8),

    "r4.4xlarge" -> EC2Info(122.0, 16),
    "r4.large" -> EC2Info(15.25, 2),
    "r4.xlarge" -> EC2Info(30.5, 4),
    "r4.8xlarge" -> EC2Info(244.0, 32),
    "r4.2xlarge" -> EC2Info(61.0, 8),
    "r4.16xlarge" -> EC2Info(488.0, 64),

    "r5.4xlarge" -> EC2Info(128.0, 16),
    "r5.xlarge" -> EC2Info(32.0, 4),
    "r5.2xlarge" -> EC2Info(64.0, 8),
    "r5.12xlarge" -> EC2Info(384.0, 48),

    "r5a.4xlarge" -> EC2Info(128.0, 16),
    "r5a.xlarge" -> EC2Info(32.0, 4),
    "r5a.2xlarge" -> EC2Info(64.0, 8),
    "r5a.24xlarge" -> EC2Info(768.0, 96),
    "r5a.12xlarge" -> EC2Info(384.0, 48),

    "r5d.4xlarge" -> EC2Info(128.0, 16),
    "r5d.xlarge" -> EC2Info(32.0, 4),
    "r5d.2xlarge" -> EC2Info(64.0, 8),
    "r5d.24xlarge" -> EC2Info(768.0, 96),
    "r5d.12xlarge" -> EC2Info(384.0, 48),

    "h1.4xlarge" -> EC2Info(64.0, 16),
    "h1.8xlarge" -> EC2Info(128.0, 32),
    "h1.16xlarge" -> EC2Info(32.0, 8),

    "i3.4xlarge" -> EC2Info(122.0, 16),
    "i3.xlarge" -> EC2Info(30.5, 4),
    "i3.8xlarge" -> EC2Info(244.0, 32),
    "i3.2xlarge" -> EC2Info(61.0, 8),
    "i3.16xlarge" -> EC2Info(488.0, 64),

    "i3en.xlarge" -> EC2Info(32.0, 4),
    "i3en.2xlarge" -> EC2Info(64.0, 8),
    "i3en.6xlarge" -> EC2Info(192.0, 24),
    "i3en.3xlarge" -> EC2Info(96.0, 12),
    "i3en.24xlarge" -> EC2Info(768.0, 96),
    "i3en.12xlarge" -> EC2Info(384.0, 48),

    "d2.4xlarge" -> EC2Info(122.0, 16),
    "d2.xlarge" -> EC2Info(30.5, 4),
    "d2.8xlarge" -> EC2Info(244.0, 36),
    "d2.2xlarge" -> EC2Info(61.0, 8)
  )
}
