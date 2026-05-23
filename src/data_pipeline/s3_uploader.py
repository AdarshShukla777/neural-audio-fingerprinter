import boto3
import os
import logging
from typing import List, Set
from botocore.exceptions import NoCredentialsError

logger = logging.getLogger("S3Uploader")

class S3Uploader:
    def __init__(self, bucket_name: str, region_name: str = "us-east-1"):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region_name)

    def upload_file(self, file_path: str, s3_folder: str, object_name: str = None) -> str:
        """
        Uploads a file to an S3 bucket and returns the S3 Key.
        """
        if object_name is None:
            object_name = os.path.basename(file_path)

        # Construct full S3 Key (Folder + Filename)
        s3_key = f"{s3_folder.rstrip('/')}/{object_name}"

        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            logger.info(f"✅ Uploaded to S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
        except FileNotFoundError:
            logger.error("The file was not found")
            raise
        except NoCredentialsError:
            logger.error("Credentials not available")
            raise
        except Exception as e:
            logger.error(f"S3 Upload Error: {e}")
            raise

    def list_all_files(self, prefix: str) -> Set[str]:
        """
        Lists all files in the bucket with the given prefix.
        Returns a Set of filenames (keys) for fast comparison.
        """
        logger.info(f"Listing all files in bucket '{self.bucket_name}' with prefix '{prefix}'...")
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        all_keys = set()
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    all_keys.add(obj["Key"])
        
        logger.info(f"Found {len(all_keys)} files in S3.")
        return all_keys