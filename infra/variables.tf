variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "prod"
}

variable "vpc_id" {
  description = "VPC for the database and the Lambda functions."
  type        = string
}

variable "private_subnet_ids" {
  description = "At least two private subnets with a NAT gateway: the worker has to reach the internet to deliver webhooks."
  type        = list(string)
}

variable "lambda_zip" {
  description = "Deployment package built by infra/build.sh."
  type        = string
  default     = "../dist/lambda.zip"
}

variable "api_key" {
  description = "Bearer token for the API and password for the dashboard."
  type        = string
  sensitive   = true
}

variable "db_password" {
  type      = string
  sensitive = true
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "max_attempts" {
  type    = number
  default = 8
}
