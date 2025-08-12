import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access AWS credentials and role information
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_ROLE_ARN = os.getenv('AWS_ROLE_ARN')
AWS_ROLE_SESSION_NAME = os.getenv('AWS_ROLE_SESSION_NAME')
AWS_ROLE_DURATION_SECONDS = int(os.getenv('AWS_ROLE_DURATION_SECONDS'))

# Migration buckets
AWS_ORIGIN_BUCKET = os.getenv('AWS_ORIGIN_BUCKET')
AWS_ORIGIN_PREFIX = os.getenv('AWS_ORIGIN_PREFIX')
AWS_DESTINATION_BUCKET = os.getenv('AWS_DESTINATION_BUCKET')
AWS_DESTINATION_PREFIX = os.getenv('AWS_DESTINATION_PREFIX')


def assume_role():
    # Create an STS client
    sts_client = boto3.client(
        'sts',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    try:
        # Assume the specified role
        response = sts_client.assume_role(
            RoleArn=AWS_ROLE_ARN,
            RoleSessionName=AWS_ROLE_SESSION_NAME,
            DurationSeconds=AWS_ROLE_DURATION_SECONDS
        )

        return response['Credentials']

    except Exception as e:
        print(f"Error assuming role: {str(e)}")
        raise


def list_s3_files(s3_client, bucket, prefix, limit=10):
    try:
        print(f"\nListing first {limit} files in s3://{bucket}/{prefix}")
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=limit
        )

        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"- {obj['Key']}")
        else:
            print("No files found")

    except Exception as e:
        print(f"Error listing files in {bucket}/{prefix}: {str(e)}")


def main():
    # Verify that credentials and role information are loaded
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ROLE_ARN, AWS_ROLE_SESSION_NAME]):
        raise ValueError(
            "Required AWS credentials or role information not found in environment variables")

    # Assume the role and get temporary credentials
    credentials = assume_role()

    # Create an S3 client using the assumed role credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    # List files in both locations
    list_s3_files(
        s3_client,
        AWS_ORIGIN_BUCKET,
        AWS_ORIGIN_PREFIX
    )

    list_s3_files(
        s3_client,
        AWS_DESTINATION_BUCKET,
        AWS_DESTINATION_PREFIX
    )


if __name__ == "__main__":
    main()
