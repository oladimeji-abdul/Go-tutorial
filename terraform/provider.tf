terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "us-east-1"
}

terraform {
  backend "s3" {
    bucket         = "infra-sh-backend"
    region         = "us-east-1"                   # Ensure this matches your bucket's region
    dynamodb_table = "terraform-lock-table"        # Optional, for state locking
    encrypt        = true                          # Encrypt state at rest
  }
}
