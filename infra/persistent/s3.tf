resource "aws_s3_bucket" "html_cache" {
  bucket = "${var.app_name}-html-cache"
}
