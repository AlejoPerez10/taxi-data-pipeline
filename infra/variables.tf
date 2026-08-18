variable "bucket_name" {
  description = "Nombre del bucket S3 para el data lake de taxis"
  type        = string
  default     = "nyc-taxi-data-lake"
}

variable "aws_region" {
  description = "Región de AWS (simulada por LocalStack)"
  type        = string
  default     = "us-east-1"
}