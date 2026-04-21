resource "aws_secretsmanager_secret" "db_credentials" {
  name = "${var.app_name}/db-credentials"
}

resource "aws_secretsmanager_secret" "openai" {
  name = "${var.app_name}/openai"
}

resource "aws_secretsmanager_secret" "brightdata_serp" {
  name = "${var.app_name}/brightdata-serp"
}

resource "aws_secretsmanager_secret" "brightdata_fetch" {
  name = "${var.app_name}/brightdata-fetch"
}
