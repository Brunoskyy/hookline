data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "hookline" {
  name_prefix        = "hookline-"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy_attachment" "vpc" {
  role       = aws_iam_role.hookline.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "queue" {
  statement {
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.work.arn]
  }
}

resource "aws_iam_role_policy" "queue" {
  role   = aws_iam_role.hookline.id
  policy = data.aws_iam_policy_document.queue.json
}

locals {
  environment = {
    HOOKLINE_DATABASE_URL  = local.database_url
    HOOKLINE_API_KEY       = var.api_key
    HOOKLINE_QUEUE         = "sqs"
    HOOKLINE_SQS_QUEUE_URL = aws_sqs_queue.work.url
    HOOKLINE_AWS_REGION    = var.region
    HOOKLINE_MAX_ATTEMPTS  = tostring(var.max_attempts)
  }
}

resource "aws_lambda_function" "api" {
  function_name    = "hookline-${var.environment}-api"
  role             = aws_iam_role.hookline.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "hookline.aws.api_handler"
  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)
  memory_size      = 512
  timeout          = 15

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = local.environment
  }
}

resource "aws_lambda_function" "worker" {
  function_name    = "hookline-${var.environment}-worker"
  role             = aws_iam_role.hookline.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "hookline.aws.sqs_handler"
  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)
  memory_size      = 512
  timeout          = 60

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = local.environment
  }
}

resource "aws_lambda_event_source_mapping" "worker" {
  event_source_arn        = aws_sqs_queue.work.arn
  function_name           = aws_lambda_function.worker.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    # Caps concurrent deliveries so a burst cannot exhaust the database's connections.
    maximum_concurrency = 20
  }
}
