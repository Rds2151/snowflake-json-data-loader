import snowflake.connector as sf
import json
import boto3
import os
import sys

from awsglue.utils import getResolvedOptions

# Function to retrieve secrets from AWS Secrets Manager
def get_secret(secret_name):
    region_name = "ap-south-1"  # Change this to your region

    # Create a Secrets Manager client
    client = boto3.client('secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise Exception(f"Error retrieving secret: {str(e)}")

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

# Load Snowflake credentials from Secrets Manager
secret_name = "snowflake_credentials"
snowflake_data = get_secret(secret_name)

# Get the Glue parameters
args = getResolvedOptions(sys.argv, [
    'LOAD_FILE',
    'SNOWFLAKE_STAGE_NAME',
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    'SNOWFLAKE_FILE_FORMAT_NAME',
    'SNOWFLAKE_STORAGE_INT_NAME',
    'SNOWFLAKE_RAW_TABLE_NAME',
    'S3_STORAGE_AWS_ROLE_ARN',
    'S3_STORAGE_ALLOWED_LOCATIONS'
])

# Snowflake environment variables
snowflake_stage_name = args['SNOWFLAKE_STAGE_NAME']
snowflake_file_format_name = args['SNOWFLAKE_FILE_FORMAT_NAME']
snowflake_storage_int = args['SNOWFLAKE_STORAGE_INT_NAME']
snowflake_json_raw_table_name = args['SNOWFLAKE_RAW_TABLE_NAME']

# S3 environment variables
load_file = args['LOAD_FILE']
s3_storage_arn = args['S3_STORAGE_AWS_ROLE_ARN']
s3_storage_allowed = args['S3_STORAGE_ALLOWED_LOCATIONS']

print("Connecting to Snowflake...")
# Establish Snowflake connection
sf_conn_obj = sf.connect(
    user=snowflake_data['SNOWFLAKE_USERNAME'],
    password=snowflake_data['SNOWFLAKE_PASSWORD'],
    account=snowflake_data['SNOWFLAKE_ACCOUNT'],
    warehouse=args['SNOWFLAKE_WAREHOUSE'],
    database=args['SNOWFLAKE_DATABASE'],
    schema=args['SNOWFLAKE_SCHEMA']
)

print("Connected Successfully.")
sf_cursor_obj = sf_conn_obj.cursor()

try:
    # File Format
    print("Creating File format...")
    sf_cursor_obj.execute(f"create or replace file format {snowflake_file_format_name} type=json")
    result = sf_cursor_obj.fetchone()
    print(result[0])
    
    # Raw table for JSON data
    print("Creating raw table for JSON data...")
    sf_cursor_obj.execute(f"create or replace table {snowflake_json_raw_table_name} (json_data variant)")
    result = sf_cursor_obj.fetchone()
    print(result[0])

    # External Stage
    print("Creating external stage using the S3 bucket...")
    sf_cursor_obj.execute(f"CREATE OR REPLACE STAGE {snowflake_stage_name} STORAGE_INTEGRATION={snowflake_storage_int} URL='{s3_storage_allowed}' FILE_FORMAT='{snowflake_file_format_name}'")
    result = sf_cursor_obj.fetchone()
    print(result[0])

    # Copy JSON data from S3 to Raw table
    print("Copying JSON data from S3 bucket to raw table...")
    sf_cursor_obj.execute(f"Copy into {snowflake_json_raw_table_name} from @{snowflake_stage_name}/{load_file} FILE_FORMAT='{snowflake_file_format_name}';")
    result = sf_cursor_obj.fetchone()
    print(result)
except Exception as e:
    print(e)
finally:
    sf_cursor_obj.close()
    sf_conn_obj.close()
