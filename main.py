import os
import json
import boto3
import time
import threading
import urllib.request
from urllib.parse import urlparse
from pathlib import Path
from dotenv import load_dotenv
import multiprocessing

# Load environment variables from .env file
load_dotenv()


class S3ClientManager:
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            with self._lock:
                if not self._initialized:

                    # Access AWS credentials and role information
                    self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
                    self.aws_secret_access_key = os.getenv(
                        'AWS_SECRET_ACCESS_KEY')
                    self.aws_role_arn = os.getenv('AWS_ROLE_ARN')
                    self.aws_role_session_name = os.getenv(
                        'AWS_ROLE_SESSION_NAME')
                    self.aws_role_duration_seconds = int(
                        os.getenv('AWS_ROLE_DURATION_SECONDS'))

                    # Initialize the client as None
                    self._s3_client = None
                    self._initialized = True

    def get_client(self):
        """
        Returns an S3 client with assumed role credentials.
        Creates a new client if none exists or if credentials have expired.
        """
        if self._s3_client is None:
            self._s3_client = self._create_client()
        return self._s3_client

    def _create_client(self):
        """
        Creates a new S3 client with assumed role credentials.
        """
        # Create an STS client
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        # Assume the role
        response = sts_client.assume_role(
            RoleArn=self.aws_role_arn,
            RoleSessionName=self.aws_role_session_name,
            DurationSeconds=self.aws_role_duration_seconds
        )

        credentials = response['Credentials']

        # Create and return S3 client
        return boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )


def download_file(file_info):
    """
    Downloads a single file from S3 and returns its status.
    """
    try:
        # Get the S3 client from the singleton manager
        s3_client = S3ClientManager().get_client()

        file_key = file_info['key']
        bucket = file_info['bucket']
        origin_dir = file_info['origin_dir']

        local_filename = os.path.basename(file_key)

        if not local_filename:  # Skip if it's a directory
            return None

        local_path = os.path.join(origin_dir, local_filename)

        # Check if file already exists
        if os.path.exists(local_path):
            print(f"Skipping {file_key} - already exists locally")
            return {'status': 'skipped', 'path': local_path}

        print(f"Downloading {file_key} to {local_path}")
        s3_client.download_file(
            Bucket=bucket,
            Key=file_key,
            Filename=local_path
        )
        return {'status': 'downloaded', 'path': local_path}
    except Exception as e:
        print(f"Error downloading {file_key}: {str(e)}")
        return {'status': 'error', 'path': local_path, 'error': str(e)}


def download_origin_files(bucket, prefix, n=100, processes=12):
    """
    Downloads files from the origin bucket to a local 'origin' directory using multiple processes.
    Skips files that already exist in the directory.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix (folder path)
        n: Number of files to download (default: 100, use -1 for all files)
        processes: Number of parallel download processes (default: 12)

    Returns:
        list: List of downloaded file paths
    """
    try:
        # Get the S3 client from the singleton manager
        s3_client = S3ClientManager().get_client()

        # Create origin directory if it doesn't exist
        origin_dir = "origin"
        os.makedirs(origin_dir, exist_ok=True)

        # Initialize pagination parameters
        paginator = s3_client.get_paginator('list_objects_v2')
        pagination_config = {'PageSize': 1000}  # Adjust batch size as needed

        # If n is not -1, limit the total number of files
        if n != -1:
            pagination_config['MaxItems'] = n

        # Collect all file information first
        all_files = []
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig=pagination_config
        ):
            if 'Contents' in page:
                # Prepare file info with all necessary data
                file_infos = [{
                    'key': obj['Key'],
                    'bucket': bucket,
                    'origin_dir': origin_dir
                } for obj in page['Contents']]
                all_files.extend(file_infos)
                if n != -1 and len(all_files) >= n:
                    all_files = all_files[:n]
                    break

        if not all_files:
            print("No files found in the bucket")
            return []

        # Create a process pool
        pool = multiprocessing.Pool(processes=processes)

        # Download files in parallel
        results = pool.map(download_file, all_files)

        # Close the pool
        pool.close()
        pool.join()

        # Process results
        downloaded_files = []
        skipped_files = []
        error_files = []

        for result in results:
            if result is None:
                continue
            if result['status'] == 'downloaded':
                downloaded_files.append(result['path'])
            elif result['status'] == 'skipped':
                skipped_files.append(result['path'])
            else:
                error_files.append(result['path'])

        # Print summary
        print(f"\nDownload Summary:")
        print(
            f"Successfully downloaded {len(downloaded_files)} files to {origin_dir}/")
        if skipped_files:
            print(f"Skipped {len(skipped_files)} existing files")
        if error_files:
            print(f"Failed to download {len(error_files)} files")

        return downloaded_files

    except Exception as e:
        print(f"Error in download process: {str(e)}")
        raise


def download_url(url, destination_path):
    """
    Downloads a file from a URL to a specified path.
    """
    try:
        urllib.request.urlretrieve(url, destination_path)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
        return False


def process_verification(file_info):
    """
    Processes a single verification file and downloads associated media.
    """
    filename = file_info['filename']
    origin_dir = file_info['origin_dir']
    destination_dir = file_info['destination_dir']

    try:
        # Extract ID from filename
        verification_id = filename.split('-')[1].split('.')[0]
        print(f"Processing ID: {verification_id}...")
    except IndexError:
        print(f"Error: Invalid filename format: {filename}")
        return {'status': 'error', 'error': 'Invalid filename format', 'filename': filename}

    # Create directory for this verification if it doesn't exist
    verification_dir = os.path.join(destination_dir, verification_id)
    if os.path.exists(verification_dir):
        return {'status': 'skipped', 'message': f"Directory already exists for {verification_id}"}

    try:
        os.makedirs(verification_dir)
    except Exception as e:
        return {'status': 'error', 'error': f"Failed to create directory: {str(e)}", 'filename': filename}

    # Read the JSON file
    json_path = os.path.join(origin_dir, filename)
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return {'status': 'error', 'error': f"Invalid JSON: {str(e)}", 'filename': filename}
    except Exception as e:
        return {'status': 'error', 'error': f"Failed to read file: {str(e)}", 'filename': filename}

    downloads_succeeded = True
    # Process document photos
    if 'documents' in data:
        for doc in data['documents']:
            # Check if photos array exists and is not empty
            if 'photos' in doc and doc['photos']:
                # Handle front photo if available
                if len(doc['photos']) >= 1:
                    if not download_url(doc['photos'][0], os.path.join(verification_dir, 'doc_front.jpg')):
                        downloads_succeeded = False

                # Handle back photo if available
                if len(doc['photos']) >= 2:
                    if not download_url(doc['photos'][1], os.path.join(verification_dir, 'doc_back.jpg')):
                        downloads_succeeded = False

    # Process steps data
    if 'steps' in data:
        for step in data['steps']:
            if 'data' in step:
                step_data = step['data']

                if 'selfieUrl' in step_data:
                    if not download_url(step_data['selfieUrl'], os.path.join(verification_dir, 'selfie.jpg')):
                        downloads_succeeded = False

                if 'spriteUrl' in step_data:
                    if not download_url(step_data['spriteUrl'], os.path.join(verification_dir, 'sprite.jpg')):
                        downloads_succeeded = False

                if 'videoUrl' in step_data:
                    video_url = step_data['videoUrl']
                    ext = os.path.splitext(urlparse(video_url).path)[
                        1] or '.mp4'
                    if not download_url(video_url, os.path.join(verification_dir, f'video{ext}')):
                        downloads_succeeded = False

    # Copy the JSON file as data.json (compressed)
    try:
        json_destination = os.path.join(verification_dir, 'data.json')
        with open(json_path, 'r') as src, open(json_destination, 'w') as dst:
            json.dump(json.load(src), dst, separators=(',', ':'))
    except Exception as e:
        return {'status': 'error', 'error': f"Failed to copy JSON file: {str(e)}", 'filename': filename}

    status = 'success' if downloads_succeeded else 'partial_success'
    return {
        'status': status,
        'verification_id': verification_id,
        'filename': filename,
        'message': 'All files processed successfully' if downloads_succeeded else 'Some downloads failed'
    }


def process_verification_files(processes=24):
    """
    Processes verification files from the origin directory and organizes their contents
    in a destination directory structure using multiple processes.
    """
    start_time = time.time()

    # Create destination directory if it doesn't exist
    destination_dir = "destination"
    os.makedirs(destination_dir, exist_ok=True)

    # Get all JSON files from origin directory
    origin_dir = "origin"
    if not os.path.exists(origin_dir):
        print(f"Error: Origin directory '{origin_dir}' does not exist")
        return

    # Collect all verification files
    verification_files = []
    for filename in os.listdir(origin_dir):
        if filename.endswith('.json'):
            verification_files.append({
                'filename': filename,
                'origin_dir': origin_dir,
                'destination_dir': destination_dir
            })

    if not verification_files:
        print("No verification files found in the origin directory")
        return

    print(f"Found {len(verification_files)} verification files to process")

    # Create a process pool
    pool = multiprocessing.Pool(processes=processes)

    try:
        # Process files in parallel
        results = pool.map(process_verification, verification_files)

        # Close the pool
        pool.close()
        pool.join()

        # Process results
        success_count = 0
        partial_success_count = 0
        skipped_count = 0
        error_count = 0

        for result in results:
            if result['status'] == 'success':
                success_count += 1
                print(f"Successfully processed {result['filename']}")
            elif result['status'] == 'partial_success':
                partial_success_count += 1
                print(
                    f"Partially processed {result['filename']} (some downloads failed)")
            elif result['status'] == 'skipped':
                skipped_count += 1
                print(f"Skipped {result['filename']} - {result['message']}")
            else:  # error
                error_count += 1
                print(
                    f"Error processing {result['filename']}: {result['error']}")

        # Print summary
        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print(f"\nProcessing Summary:")
        print(f"Successfully processed: {success_count}")
        print(f"Partially processed: {partial_success_count}")
        print(f"Skipped: {skipped_count}")
        print(f"Failed: {error_count}")
        print(f"Total time: {minutes} minutes {seconds} seconds")

    except Exception as e:
        print(f"Error in parallel processing: {str(e)}")
        pool.terminate()
        raise


def main():
    aws_origin_bucket = os.getenv('AWS_ORIGIN_BUCKET')
    aws_origin_prefix = os.getenv('AWS_ORIGIN_PREFIX')

    # Download files from origin bucket if needed
    # download_origin_files(
    #     bucket=aws_origin_bucket,
    #     prefix=aws_origin_prefix,
    #     n=1000,  # or any number you want, use -1 for all files
    #     processes=12  # number of parallel processes
    # )

    # Process verification files
    process_verification_files()


if __name__ == "__main__":
    main()
