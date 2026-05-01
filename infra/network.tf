resource "aws_security_group" "lambda" {
  name_prefix = "hookline-lambda-"
  vpc_id      = var.vpc_id
  description = "Hookline functions: out to the database and to subscriber endpoints."

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "db" {
  name_prefix = "hookline-db-"
  vpc_id      = var.vpc_id
  description = "Postgres, reachable only from the Hookline functions."

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }
}
