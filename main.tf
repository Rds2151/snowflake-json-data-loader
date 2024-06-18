provider "aws" {
  profile = "default"
  region  = "ap-south-1"
}

variable "bucket-name" {
  default = "testbucket093"
}

variable "bucket-arn" {
  default = "arn:aws:s3:::testbucket093"
}

variable "job-name" {
  default = "snow_json_data_loader"
}

variable "file-name" {
  default = "snow_json_data_loader.py"
}

variable "load-file-name" {
  default = "sample.json"
}

# Upload the Glue script to the specified S3 bucket
resource "aws_s3_object" "upload-glue-script" {
  bucket = var.bucket-name
  key    = "scripts/${var.file-name}"
  source = var.file-name
}

# Upload the Snowflake connector dependency to the specified S3 bucket
resource "aws_s3_object" "glue_dependencies" {
  bucket = var.bucket-name
  key    = "dependencies/snowflake_connector_python-2.3.8-py3-none-any.whl"
  source = "snowflake_connector_python-2.3.8-py3-none-any.whl"
}

# Create an IAM role for Glue
resource "aws_iam_role" "glue_role" {
  name = "glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Create an IAM policy for Glue to access S3 and CloudWatch
resource "aws_iam_policy" "glue_policy" {
  name        = "glue-policy"
  description = "Policy for Glue to access S3 and CloudWatch"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject"
        ],
        Resource = [
          "${var.bucket-arn}",
          "${var.bucket-arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      }
    ]
  })
}

# Attach the policy to the Glue role
resource "aws_iam_role_policy_attachment" "glue_role_policy_attach" {
  role      = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_policy.arn
}

# Create the Glue job
resource "aws_glue_job" "glue-job" {
  name        = var.job-name
  role_arn    = aws_iam_role.glue_role.arn
  description = "A Python script to automate the process of loading JSON data from an S3 bucket into a Snowflake data warehouse."
  max_retries = 1
  glue_version = "3.0"

  default_arguments = {
    "--LOAD_FILE"                     = "s3://${var.bucket-name}/${var.load-file-name}"
    "--SNOWFLAKE_STAGE_NAME"          = "snow_stage_crox"
    "--SNOWFLAKE_WAREHOUSE"           = "COMPUTE_WH"
    "--SNOWFLAKE_DATABASE"            = "MY_DB"
    "--SNOWFLAKE_SCHEMA"              = "MY_SCHEMA"
    "--SNOWFLAKE_FILE_FORMAT_NAME"    = "json_format_0"
    "--SNOWFLAKE_STORAGE_INT_NAME"    = "S3_BUCKET"
    "--SNOWFLAKE_RAW_TABLE_NAME"      = "json_raw_table"
    "--S3_STORAGE_AWS_ROLE_ARN"       = aws_iam_role.glue_role.arn
    "--S3_STORAGE_ALLOWED_LOCATIONS"  = "s3://${var.bucket-name}/"
    "--additional-python-modules"     = "s3://${var.bucket-name}/dependencies/snowflake_connector_python-2.3.8-py3-none-any.whl"
  }

  command {
    script_location = "s3://${var.bucket-name}/scripts/${var.file-name}"
    python_version  = "3"
  }
}
