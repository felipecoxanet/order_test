variable "tags" {
  description = "Create the default tags for organizing services with Amazon Web Service"

  default = {
    Name        = "Datalake"
    Service     = "Pipeline"
    Owner       = "Coxa"
    Environment = "Development"
    Creator     = "Terraform"
  }
}
