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
  worker_timeout = 60

  environment = {
    HOOKLINE_DATABASE_URL           = local.database_url
    HOOKLINE_API_KEY                = var.api_key
    HOOKLINE_QUEUE                  = "sqs"
    HOOKLINE_SQS_QUEUE_URL          = aws_sqs_queue.work.url
    HOOKLINE_AWS_REGION             = var.region
    HOOKLINE_MAX_ATTEMPTS           = tostring(var.max_attempts)
    HOOKLINE_LAMBDA_TIMEOUT_SECONDS = tostring(local.worker_timeout)
    # Every Lambda container has its own pool. Small pools, no overflow, so the total below
    # is a hard ceiling.
    HOOKLINE_DB_POOL_SIZE    = tostring(var.db_pool_per_container)
    HOOKLINE_DB_MAX_OVERFLOW = "0"
  }

  # The most connections the functions can hold at once: every worker container, every API
  # container and the reconciler, each with a full pool. It has to stay under the database's
  # max_connections with room for migrations and a psql session.
  db_connection_budget = var.db_pool_per_container * (
    var.worker_max_concurrency + var.api_reserved_concurrency + 1
  )
}

resource "terraform_data" "connection_budget" {
  lifecycle {
    precondition {
      condition     = local.db_connection_budget <= var.db_max_connections - 10
      error_message = "Lambda pools could open more connections than the database allows. Lower the concurrency or the pool size, or put RDS Proxy in front."
    }
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
  # Also the API's share of the database connection budget.
  reserved_concurrent_executions = var.api_reserved_concurrency

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
  timeout          = local.worker_timeout

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
    maximum_concurrency = var.worker_max_concurrency
  }
}

# Re-queues deliveries whose message went missing: a send that failed after the commit, or a
# message that ended in the poison queue. The database says what is owed; this makes sure
# the queue hears about it.
resource "aws_lambda_function" "reconciler" {
  function_name                  = "hookline-${var.environment}-reconciler"
  role                           = aws_iam_role.hookline.arn
  runtime                        = "python3.13"
  architectures                  = ["arm64"]
  handler                        = "hookline.aws.reconcile_handler"
  filename                       = var.lambda_zip
  source_code_hash               = filebase64sha256(var.lambda_zip)
  memory_size                    = 256
  timeout                        = 60
  reserved_concurrent_executions = 1

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = local.environment
  }
}

resource "aws_cloudwatch_event_rule" "reconcile" {
  name_prefix         = "hookline-reconcile-"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "reconcile" {
  rule = aws_cloudwatch_event_rule.reconcile.name
  arn  = aws_lambda_function.reconciler.arn
}

resource "aws_lambda_permission" "reconcile" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reconciler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reconcile.arn
}
