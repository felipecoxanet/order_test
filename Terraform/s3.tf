module "bucket_databricks" {
  source = "github.com/terraform-aws-modules/terraform-aws-s3-bucket?ref=v2.1.0"

  acl                     = null
  attach_policy           = true
  bucket                  = lower("gn-datalake-databricks-${var.tags.Environment}")
  create_bucket           = true
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  policy                  = data.template_file.s3_databricks.rendered
  restrict_public_buckets = true
  tags                    = var.tags
}

module "bucket_datalake_raw" {
  source = "github.com/terraform-aws-modules/terraform-aws-s3-bucket?ref=v2.1.0"

  acl                     = null
  bucket                  = lower("gn-datalake-${var.tags.Environment}-raw")
  create_bucket           = true
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
  tags                    = var.tags

  server_side_encryption_configuration = {
    rule = {
      bucket_key_enabled = true
      apply_server_side_encryption_by_default = {
        kms_master_key_id = aws_kms_key.datalake_kms_dev.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }
  logging = {
    target_bucket = module.bucket_logs.s3_bucket_id
    target_prefix = lower("log/gn-datalake-${var.tags.Environment}-raw")
  }
}

module "bucket_datalake_transformed" {
  source = "github.com/terraform-aws-modules/terraform-aws-s3-bucket?ref=v2.1.0"

  acl                     = null
  bucket                  = lower("gn-datalake-${var.tags.Environment}-transformed")
  create_bucket           = true
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
  tags                    = var.tags

  server_side_encryption_configuration = {
    rule = {
      bucket_key_enabled = true
      apply_server_side_encryption_by_default = {
        kms_master_key_id = aws_kms_key.datalake_kms_dev.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }
  logging = {
    target_bucket = module.bucket_logs.s3_bucket_id
    target_prefix = lower("log/gn-datalake-${var.tags.Environment}-transformed")
  }
}

module "bucket_datalake_analytic" {
  source = "github.com/terraform-aws-modules/terraform-aws-s3-bucket?ref=v2.1.0"

  acl                     = null
  bucket                  = lower("gn-datalake-${var.tags.Environment}-analytic")
  create_bucket           = true
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
  tags                    = var.tags

  server_side_encryption_configuration = {
    rule = {
      bucket_key_enabled = true
      apply_server_side_encryption_by_default = {
        kms_master_key_id = aws_kms_key.datalake_kms_dev.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }
  logging = {
    target_bucket = module.bucket_logs.s3_bucket_id
    target_prefix = lower("log/gn-datalake-${var.tags.Environment}-analytic")
  }
}

module "bucket_logs" {
  source = "github.com/terraform-aws-modules/terraform-aws-s3-bucket?ref=v2.1.0"

  acl                     = null
  bucket                  = lower("gn-datalake-audit-logs-${var.tags.Environment}")
  create_bucket           = true
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
  tags                    = var.tags
}

resource "aws_kms_key" "datalake_kms_dev" {
  description             = "This key is used for to encrypt ${var.tags.Name} bucket objects"
  deletion_window_in_days = 7 # change for prod to 30
  enable_key_rotation     = true
  policy                  = data.template_file.kms_s3_bucket_databricks.rendered
  tags                    = var.tags

  depends_on = [module.role_databricks_job]
}

resource "aws_kms_alias" "datalake_alias" {
  name          = "alias/datalake_kms_dev"
  target_key_id = aws_kms_key.datalake_kms_dev.key_id
}
