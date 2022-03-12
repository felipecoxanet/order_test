module "policy_databricks_root" {
  source = "github.com/terraform-aws-modules/terraform-aws-iam//modules/iam-policy?ref=v4.2.0"

  description = "Policy to projet ${var.tags.Name}-${var.tags.Service}"
  name        = lower("${var.tags.Name}-${var.tags.Service}-${var.tags.Environment}-databricks-root-policy")
  path        = "/"
  policy      = data.template_file.iam_databricks_root.rendered

  tags = var.tags
}

module "role_cross_account" {
  source = "github.com/traveloka/terraform-aws-iam-role.git//modules/external?ref=v4.0.0"

  role_name                  = lower("${var.tags.Name}-${var.tags.Service}-${var.tags.Environment}-databricks-root-role")
  role_path                  = "/"
  role_description           = "Role for Databricks"
  role_force_detach_policies = "true"
  role_max_session_duration  = "43200"

  account_id  = "123456798"
  external_id = "xxxxxx-yyyyyy-xxxxxx-wwww-zzzzzzz"

  product_domain = "tst"
  environment    = var.tags.Environment

  depends_on = [
    module.policy_databricks_root,
    module.role_databricks_job
  ]
}

resource "aws_iam_role_policy_attachment" "role_cross_account" {
  role       = module.role_cross_account.role_name
  policy_arn = module.policy_databricks_root.arn
}

module "policy_databricks_job" {
  source = "github.com/terraform-aws-modules/terraform-aws-iam//modules/iam-policy?ref=v4.2.0"

  description = "Policy to projet ${var.tags.Name}-${var.tags.Service}"
  name        = lower("${var.tags.Name}-${var.tags.Service}-${var.tags.Environment}-databricks-job-policy")
  path        = "/"
  policy      = data.template_file.iam_databricks_job.rendered

  tags = var.tags

}

module "policy_databricks_job_kms" {
  source = "github.com/terraform-aws-modules/terraform-aws-iam//modules/iam-policy?ref=v4.2.0"

  description = "Policy to projet ${var.tags.Name}-${var.tags.Service}"
  name        = lower("${var.tags.Name}-${var.tags.Service}-${var.tags.Environment}-databricks-job-kms-policy")
  path        = "/"
  policy      = data.template_file.iam_databricks_job_kms.rendered

  tags = var.tags

}

module "policy_databricks_secrets" {
  source = "github.com/terraform-aws-modules/terraform-aws-iam//modules/iam-policy?ref=v4.2.0"

  description = "Policy to projet ${var.tags.Name}-${var.tags.Service}"
  name        = lower("${var.tags.Name}-${var.tags.Service}-${var.tags.Environment}-databricks-secrets-policy")
  path        = "/"
  policy      = data.template_file.iam_databricks_secrets_policy.rendered

  tags = var.tags

}

module "role_databricks_job" {
  source = "github.com/terraform-aws-modules/terraform-aws-iam//modules/iam-assumable-role?ref=v4.2.0"

  create_role           = true
  role_requires_mfa     = false
  role_name             = lower("${var.tags.Name}-${var.tags.Service}-${var.tags.Environment}-databricks-job-role")
  trusted_role_services = ["ec2.amazonaws.com"]

  tags = var.tags
}

resource "aws_iam_instance_profile" "instance_profile" {
  name = module.role_databricks_job.iam_role_name
  role = module.role_databricks_job.iam_role_name
}

resource "aws_iam_role_policy_attachment" "role_databricks_job" {
  role       = module.role_databricks_job.iam_role_name
  policy_arn = module.policy_databricks_job.arn
}

resource "aws_iam_role_policy_attachment" "role_databricks_job_kms" {
  role       = module.role_databricks_job.iam_role_name
  policy_arn = module.policy_databricks_job_kms.arn
}

resource "aws_iam_role_policy_attachment" "role_databricks_secrets" {
  role       = module.role_databricks_job.iam_role_name
  policy_arn = module.policy_databricks_secrets.arn
}
