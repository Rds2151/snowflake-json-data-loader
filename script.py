import snowflake.connector as sf
from dotenv import load_dotenv
import json
import os
import re

# Load environment variables
load_dotenv()

# Snowflake credentials JSON string and file 
load_file_path = os.getenv('LOAD_FILE')
snowflake_cred_file = os.getenv('SNOWFLAKE_CRED_FILE')
with open(snowflake_cred_file, 'r') as file:
    snowflake_data = json.load(file)

# Snowflake environment variables
snowflake_stage_name = os.getenv('SNOWFLAKE_STAGE_NAME')
snowflake_file_format_name = os.getenv('SNOWFLAKE_FILE_FORMAT_NAME')
snowflake_storage_int = os.getenv('SNOWFLAKE_STORAGE_INT_NAME')
snowflake_json_raw_table_name = os.getenv('SNOWFLAKE_RAW_TABLE_NAME')

# S3 environment variables
s3_storage_arn = os.getenv('S3_STORAGE_AWS_ROLE_ARN')
s3_storage_allowed = os.getenv('S3_STORAGE_ALLOWED_LOCATIONS')

# Validate environment variables
if not all([load_file_path, snowflake_cred_file, snowflake_stage_name, snowflake_json_raw_table_name, snowflake_file_format_name, snowflake_storage_int, s3_storage_arn, s3_storage_allowed]):
    raise ValueError("Some environment variables are missing.")

with open(load_file_path, 'r') as file:
    data = json.load(file)

print("Connecting to the snowflake...")
# Establish Snowflake connection
sf_conn_obj = sf.connect(
    user=snowflake_data['user'],
    password=snowflake_data['password'],
    account=snowflake_data['account'],
    warehouse=snowflake_data['warehouse'],
    database=snowflake_data['database'],
    schema=snowflake_data['schema']
)

print("Connected Successfully.")
sf_cursor_obj = sf_conn_obj.cursor()

try:
    # File Format
    print("Creating File format...")
    sf_cursor_obj.execute(f"create or replace file format {snowflake_file_format_name} type=json")
    result = sf_cursor_obj.fetchone()
    print(result[0])
    
    
    # Raw table for json data
    print("Creating raw table for json data...")
    sf_cursor_obj.execute(f"create or replace table {snowflake_json_raw_table_name} (json_data variant)")
    result = sf_cursor_obj.fetchone()
    print(result[0])

    # External Stage
    print("Creating external stage using the S3 bucket...")
    sf_cursor_obj.execute(f"CREATE OR REPLACE STAGE {snowflake_stage_name} STORAGE_INTEGRATION={snowflake_storage_int} URL='{s3_storage_allowed}' FILE_FORMAT='{snowflake_file_format_name}'")
    result = sf_cursor_obj.fetchone()
    print(result[0])

    
    # Copy json data from S3 to Raw table
    print("Copying json data from S3 bucket to raw table...")
    sf_cursor_obj.execute(f"Copy into {snowflake_json_raw_table_name} from @{snowflake_stage_name}/sample.json FILE_FORMAT='{snowflake_file_format_name}';")
    result = sf_cursor_obj.fetchone()
    print(result)
except Exception as e:
    print(e)
finally:
    sf_cursor_obj.close()

sf_conn_obj.close()