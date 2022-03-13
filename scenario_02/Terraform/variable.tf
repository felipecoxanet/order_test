variable "tags" {
  description = "Create the default tags for organizing services with Amazon Web Service"

  default = {
    Name        = "Datalake"
    Service     = "Pipeline"
    Owner       = "Data Products"
    Environment = "Development"
    Creator     = "Terraform"
  }
}
