# Snowflake JSON Data Loader

A Python script to automate the process of loading JSON data from an S3 bucket into a Snowflake data warehouse. 

## Description

This script performs the following tasks:
1. Loads environment variables from a `.env` file for configuration and credentials.
2. Establishes a connection to Snowflake using the provided credentials.
3. Creates a JSON file format in Snowflake.
4. Creates a raw table to store JSON data.
5. Creates an external stage using an S3 bucket.
6. Copies JSON data from the S3 bucket to the raw table in Snowflake.

## Prerequisites

- Python 3.6 or above
- `snowflake-connector-python` package
- `python-dotenv` package
- Snowflake account and S3 bucket with appropriate permissions

## Environment Variables

Create a `.env` file in the root directory of your project and add the following variables:

```env
LOAD_FILE=path/to/your/json/file
SNOWFLAKE_CRED_FILE=path/to/your/snowflake/credentials.json
SNOWFLAKE_STAGE_NAME=your_snowflake_stage_name
SNOWFLAKE_FILE_FORMAT_NAME=your_file_format_name
SNOWFLAKE_STORAGE_INT_NAME=your_storage_integration_name
SNOWFLAKE_RAW_TABLE_NAME=your_raw_table_name
S3_STORAGE_AWS_ROLE_ARN=your_s3_storage_aws_role_arn
S3_STORAGE_ALLOWED_LOCATIONS=your_s3_storage_allowed_locations
```

## Snowflake Credentials JSON Format

Ensure your Snowflake credentials JSON file (`SNOWFLAKE_CRED_FILE`) is formatted as follows:

```json
{
    "user": "your_snowflake_user",
    "password": "your_snowflake_password",
    "account": "your_snowflake_account",
    "warehouse": "your_snowflake_warehouse",
    "database": "your_snowflake_database",
    "schema": "your_snowflake_schema"
}
```

## Usage

1. **Create a virtual environment:**
    ```bash
    python -m venv myenv
    ```

2. **Activate the virtual environment:**
    - On Windows:
        ```bash
        myenv\Scripts\activate
        ```
    - On macOS and Linux:
        ```bash
        source myenv/bin/activate
        ```

3. **Install the required packages:**
    ```bash
    pip install snowflake-connector-python python-dotenv
    ```

4. **Run the script:**
    ```bash
    python script.py
    ```

## Contributing

Feel free to submit issues or pull requests if you have any suggestions or improvements.

## License

This project is licensed under the MIT License.

This README provides a clear explanation of the repository's purpose, setup instructions, environment variable configurations, and the full script. It serves as a good starting point for documentation on GitHub.