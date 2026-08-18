output "bucket_name" {
  description = "Nombre del bucket creado"
  value       = aws_s3_bucket.taxi_data_lake.bucket
}

output "bucket_arn" {
  description = "ARN del bucket creado"
  value       = aws_s3_bucket.taxi_data_lake.arn
}