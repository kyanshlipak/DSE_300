import os
import boto3
from dotenv import load_dotenv

def load_aws_credentials(filepath="aws_credentials.env"):
    load_dotenv(dotenv_path=filepath)

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.getenv("AWS_SESSION_TOKEN")

    if not all([aws_access_key_id, aws_secret_access_key, aws_session_token]):
        raise ValueError("Missing one or more AWS credential variables.")

    return {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "aws_session_token": aws_session_token
    }

def upload_file_to_s3(local_file_path, bucket_name, s3_key, creds):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        aws_session_token=creds["aws_session_token"]
    )

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"✅ Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload file to S3: {e}")
        raise

if __name__ == "__main__":
    # Constants
    BUCKET_NAME = 'de300spring2025'
    S3_FOLDER = 'MOSES_group/'
    #LOCAL_DIR = 'LCD_data/'
    LOCAL_DIR = ''
    
    LOCAL_FILE_NAME = 'get_files.py'
    LOCAL_FILE_NAME = 'save_torrent.py'
    #LOCAL_FILE_NAME = 'LCD_Data.torrent'
    #LOCAL_FILE_NAME = 'testfile.torrent'
    # Full paths
    local_file_path = os.path.join(LOCAL_DIR, LOCAL_FILE_NAME)
    s3_key = os.path.join(S3_FOLDER, LOCAL_FILE_NAME)

    # Ensure the file exists
    if not os.path.isfile(local_file_path):
        raise FileNotFoundError(f"{local_file_path} not found.")

    # Load AWS credentials and upload
    creds = load_aws_credentials()
    upload_file_to_s3(local_file_path, BUCKET_NAME, s3_key, creds)

