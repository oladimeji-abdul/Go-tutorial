resource "aws_s3_bucket" "backend" {
  bucket = "infra-sh-backend"

  tags = local.tags
}