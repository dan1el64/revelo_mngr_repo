terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Skip flags derived from endpoint_url: when set, skip_* = true; when empty, skip_* = false.
locals {
  is_local = var.aws_endpoint != null
}

provider "aws" {
  region                      = var.aws_region
  skip_credentials_validation = local.is_local
  skip_metadata_api_check     = local.is_local
  skip_requesting_account_id  = local.is_local
  s3_use_path_style           = local.is_local


  endpoints {
    s3 = var.aws_endpoint
  }
}

resource "aws_s3_bucket" "main" {
  bucket = "${var.name_prefix}-bucket"

  tags = {
    Name    = "${var.name_prefix}-bucket"
    Project = var.name_prefix
  }
}
