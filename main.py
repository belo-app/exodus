import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


def main():
    # Verify that credentials are loaded
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise ValueError("AWS credentials not found in environment variables")
    print(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}")
    print(f"AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}")


if __name__ == "__main__":
    main()
