resource "aws_glue_job" "order_job" {
  name     = "order_job"
  role_arn = "arn:aws:iam::123456:role/AWSGlueServiceRoleDefault"

  command {
    script_location = "s3://aws-glue-scripts-123456-us-east-1/felipe.rodrigues/coxa_order_test2.py"
  }
}