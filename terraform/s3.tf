resource "aws_s3_bucket" "bronze" {
  bucket = "supermarket-bronze"
}

resource "aws_s3_bucket" "silver" {
  bucket = "supermarket-silver"
}

resource "aws_s3_bucket" "gold" {
  bucket = "supermarket-gold"
}