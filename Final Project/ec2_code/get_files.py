import os
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

creds = load_aws_credentials()

import os
import boto3

def download_s3_folder_to_local(bucket_name, s3_folder, local_dir, creds):
    """Download all files from an S3 folder to a local directory."""
    s3_resource = boto3.resource('s3',
                                 aws_access_key_id=creds['aws_access_key_id'],
                                 aws_secret_access_key=creds['aws_secret_access_key'],
                                 aws_session_token=creds['aws_session_token'])
    bucket = s3_resource.Bucket(bucket_name)

    for i, obj in enumerate(bucket.objects.filter(Prefix=s3_folder)):
        #if i >= 10: break
        #if i < 103: continue

        s3_key = obj.key
        # Skip if it's a directory marker (not common in S3, but just in case)
        if s3_key.endswith("/"):
            continue

        relative_path = os.path.relpath(s3_key, s3_folder)
        local_file_path = os.path.join(local_dir, relative_path)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        bucket.download_file(s3_key, local_file_path)
        print(f"Downloaded s3://{bucket_name}/{s3_key} to {local_file_path}")


BUCKET_NAME = 'de300spring2025'   # Replace with your bucket name
S3_FOLDER = 'MOSES_group/'             # The folder path in S3
LOCAL_DIR = 'LCD_data/'      # Local directory to save files


download_s3_folder_to_local(BUCKET_NAME, S3_FOLDER, LOCAL_DIR, creds)
