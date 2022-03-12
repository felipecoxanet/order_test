data "template_file" "s3_databricks" {
  template = file("${path.module}/policies/s3_bucket_policy_databricks.json")

  vars = {
    bucket = lower("gn-datalake-databricks-${var.tags.Environment}")
  }
}

data "template_file" "kms_s3_bucket_databricks" {
  template = file("${path.module}/policies/kms_s3_bucket_databricks_policy.json")

  vars = {
    iam = module.role_databricks_job.iam_role_name
  }
}

data "template_file" "iam_databricks_job" {
  template = file("${path.module}/policies/iam_databricks_job.json")

  vars = {
    bucket_raw          = lower("gn-datalake-${var.tags.Environment}-raw")
    bucket_raw_prod     = "gn-datalake-production-raw"
    bucket_transformed  = lower("gn-datalake-${var.tags.Environment}-transformed")
    bucket_intermediate = lower("gn-datalake-${var.tags.Environment}-intermediate")
    bucket_analytic     = lower("gn-datalake-${var.tags.Environment}-analytic")
    bucket_databricks   = lower("gn-datalake-databricks-${var.tags.Environment}")
  }
}

data "template_file" "iam_databricks_job_kms" {
  template = file("${path.module}/policies/iam_databricks_job_kms.json")

  vars = {
    kms_dev_arn  = aws_kms_key.datalake_kms_dev.arn
    kms_prod_arn = data.aws_kms_alias.prod_kms.target_key_arn
  }
}

data "template_file" "iam_databricks_secrets_policy" {
  template = file("${path.module}/policies/iam_databricks_secrets_policy.json")
}

data "template_file" "iam_databricks_root" {
  template = file("${path.module}/policies/iam_databricks_root.json")

  vars = {
    role   = module.role_databricks_job.iam_role_name
    vpc_id = module.vpc_datalake.vpc_id
  }
}

data "aws_kms_alias" "prod_kms" {
  name = "alias/datalake_kms_prod"
}

data "aws_availability_zones" "available" {}
