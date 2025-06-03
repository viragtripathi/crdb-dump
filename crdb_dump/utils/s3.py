import boto3

def get_s3_client(endpoint_url=None, access_key=None, secret_key=None):
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

def upload_file_to_s3(s3, bucket, key, local_path):
    s3.upload_file(local_path, bucket, key)

def download_file_from_s3(s3, bucket, key, local_path):
    s3.download_file(bucket, key, local_path)
