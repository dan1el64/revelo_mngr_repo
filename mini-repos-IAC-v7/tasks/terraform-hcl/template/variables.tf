variable "aws_region" {
  description = "AWS region (set via TF_VAR_aws_region or default)."
  type        = string
  default     = "us-east-1"
}

variable "aws_endpoint" {
  description = "Optional AWS endpoint override for local emulation (e.g., http://localhost:4566). Null = real AWS endpoints."
  type        = string
  default     = null
}

variable "name_prefix" {
  description = "Random prefix set by the harness to avoid name collisions."
  type        = string
}