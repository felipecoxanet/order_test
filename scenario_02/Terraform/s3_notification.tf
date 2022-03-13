module "bucket_raw_notificarions" {
  source = "github.com/terraform-aws-modules/terraform-aws-s3-bucket//modules/notification?ref=v2.1.0"

  bucket = lower("gn-datalake-${var.tags.Environment}-raw")
  lambda_notifications = {
    lambda1 = {
      function_arn  = module.lambda_function.lambda_function_arn
      function_name = module.lambda_function.lambda_function_name
      events        = ["s3:ObjectCreated:*"]
      filter_prefix = "files/coxa_test/"
    }
  }
}
