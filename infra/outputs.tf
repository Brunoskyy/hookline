output "api_url" {
  value = aws_apigatewayv2_api.hookline.api_endpoint
}

output "work_queue_url" {
  value = aws_sqs_queue.work.url
}

output "database_address" {
  value = aws_db_instance.hookline.address
}
