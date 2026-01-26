resource "aws_kinesis_stream" "supermarket_stream" {
  name             = "supermarket-sales-stream"
  shard_count      = 1
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Environment = "Dev"
    Project     = "Supermarket-Lakehouse"
  }
}