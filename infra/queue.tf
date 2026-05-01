# The work queue carries delivery ids only. The database decides what is due; this queue only
# decides when a worker wakes up.
resource "aws_sqs_queue" "dead" {
  name                      = "hookline-${var.environment}-poison"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "work" {
  name                       = "hookline-${var.environment}-work"
  visibility_timeout_seconds = 90 # above the worker's timeout
  message_retention_seconds  = 345600

  # Messages the worker cannot process at all (not delivery failures, which the database
  # handles) land here after five tries instead of looping forever.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead.arn
    maxReceiveCount     = 5
  })
}
