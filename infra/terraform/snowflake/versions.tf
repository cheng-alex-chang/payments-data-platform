terraform {
  required_version = ">= 1.10" # S3-native state locking (use_lockfile)

  # Remote state: versioned S3 bucket + S3-native lockfile (no DynamoDB table needed on
  # TF >= 1.10). The bucket is bootstrapped outside this state (chicken-and-egg): a
  # versioned, public-access-blocked bucket created once via the AWS API. CI is unaffected
  # -- its `init -backend=false` skips backend initialization entirely.
  backend "s3" {
    bucket       = "payments-tfstate-alexchang-7f3k2"
    key          = "snowflake/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
  }

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
