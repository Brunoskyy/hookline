resource "aws_db_subnet_group" "hookline" {
  name_prefix = "hookline-"
  subnet_ids  = var.private_subnet_ids
}

resource "aws_db_instance" "hookline" {
  identifier_prefix         = "hookline-"
  engine                    = "postgres"
  engine_version            = "17"
  instance_class            = var.db_instance_class
  allocated_storage         = 20
  storage_encrypted         = true
  db_name                   = "hookline"
  username                  = "hookline"
  password                  = var.db_password
  db_subnet_group_name      = aws_db_subnet_group.hookline.name
  vpc_security_group_ids    = [aws_security_group.db.id]
  backup_retention_period   = 7
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "hookline-final"
}

locals {
  database_url = "postgresql+asyncpg://hookline:${var.db_password}@${aws_db_instance.hookline.address}:5432/hookline"
}
