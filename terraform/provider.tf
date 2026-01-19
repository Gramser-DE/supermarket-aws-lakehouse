provider "aws" {
  region = var.aws_region

  s3_use_path_style = true
  
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3       = "http://localstack:4566"
    kinesis  = "http://localstack:4566"
  }
}