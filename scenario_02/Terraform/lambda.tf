module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "coxa_test_order_2"
  description   = "My awesome lambda function"
  handler       = "index.lambda_handler"
  runtime       = "python3.9"

  source_path = "src/lambda-function1.py"

  policies = [
    "arn:aws:iam::590284024382:policy/datalake-pipeline-development-databricks-job-policy",
    "arn:aws:iam::590284024382:policy/datalake-pipeline-development-databricks-job-kms-policy"
    ]

  tags = var.tags
}