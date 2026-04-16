variable "aws_region" {
  description = "AWS region (set via TF_VAR_aws_region or default)."
  type        = string
  default     = "us-east-1"
}

variable "aws_endpoint" {
  description = "Optional AWS service endpoint override. Null uses the provider defaults."
  type        = string
  default     = null
}

variable "aws_access_key_id" {
  description = "AWS access key ID"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
  sensitive   = true
}
