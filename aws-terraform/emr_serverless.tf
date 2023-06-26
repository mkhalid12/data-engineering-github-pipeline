
# resource "aws_s3_bucket" "emr_studio_bucket {
#   bucket = "mbition-challenge-data"
#     tags = {
#         Environment = "prod"
#       }
# }


# resource "aws_emr_studio" "emr-studio" {
#   auth_mode                   = "IAM"
#   default_s3_location         = "s3://${aws_s3_bucket.emr_studio_bucket.bucket}/emr"
#   name                        = "my-studio"
#   service_role                = aws_iam_role.emr_studio_role.arn
#   subnet_ids                  = [aws_subnet.click_logger_emr_public_subnet1.id]
#   vpc_id                      = aws_vpc.click_logger_emr_vpc.id
#   workspace_security_group_id = aws_security_group.click_logger_emr_security_group.id
# }


resource "aws_emrserverless_application" "example" {
  name          = "example"
  release_label = "emr-6.11.0"
  type          = "spark"

  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2 vCPU"
        memory = "4 GB"
        disk = "20GB"
      }
    }
  }
    maximum_capacity {
    cpu    = "2 vCPU"
    memory = "8 GB"
    disk = "20GB"
  }
}

