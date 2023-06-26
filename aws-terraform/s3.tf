
resource "aws_s3_bucket" "mbition-challenge-data" {
  bucket = "mbition-challenge-data"
    tags = {
        Environment = "prod"
      }
}

resource "aws_s3_bucket_versioning" "aws_s3_bucket_versioning" {
  bucket = aws_s3_bucket.mbition-challenge-data.id
  versioning_configuration {
    status = "Disabled"
  }
}


locals {
  data_path = "../data-integration/data/"
  app_package = "../data-integration/dist/"
}

resource "aws_s3_object" "bootstrap_files" {
  for_each = fileset(local.data_path, "**/*.json")
  bucket = aws_s3_bucket.mbition-challenge-data.id
  key    = "data/${each.key}"
  source = "${local.data_path}/${each.value}"
  source_hash   = filemd5("${local.data_path}/${each.value}")
}

resource "aws_s3_object" "bootstrap_pyspark_zip" {
  for_each = fileset(local.app_package, "*")
  bucket = aws_s3_bucket.mbition-challenge-data.id
  key    = "artefact/${each.key}"
  source = "${local.app_package}/${each.value}"
  source_hash   = filemd5("${local.app_package}/${each.value}")
}
