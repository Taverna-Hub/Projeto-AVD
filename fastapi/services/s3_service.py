"""
Service for uploading files to AWS S3.
"""
import os
from pathlib import Path
from typing import Optional, Dict
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import logging

logger = logging.getLogger(__name__)


class S3Service:
    """Service to upload files to AWS S3."""
    
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_region: str = "us-east-1",
        s3_prefix: Optional[str] = None,
    ):
        """
        Initialize S3Service.
        
        Args:
            bucket_name: Name of the S3 bucket
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_region: AWS region
            s3_prefix: Optional prefix for organizing files in S3
        """
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix or ""
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
    
    def upload_file(
        self,
        local_file_path: str,
        s3_key: Optional[str] = None,
        preserve_structure: bool = True,
    ) -> Dict[str, str]:
        """
        Upload a single file to S3.
        
        Args:
            local_file_path: Path to the local file
            s3_key: Optional S3 key (object name). If not provided, uses relative path
            preserve_structure: If True, preserves directory structure in S3 key
            
        Returns:
            Dictionary with upload result:
            {
                "success": bool,
                "s3_key": str,
                "message": str
            }
        """
        file_path = Path(local_file_path)
        
        if not file_path.exists():
            return {
                "success": False,
                "s3_key": None,
                "message": f"File not found: {local_file_path}",
            }
        
        # Determine S3 key
        if s3_key is None:
            if preserve_structure:
                # Use relative path as S3 key
                s3_key = str(file_path.name)
            else:
                s3_key = file_path.name
        
        # Add prefix if specified
        if self.s3_prefix:
            s3_key = f"{self.s3_prefix}/{s3_key}".strip("/")
        
        try:
            # Upload file
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key,
                ExtraArgs={"ContentType": "text/csv"},  # CSV files
            )
            
            logger.info(f"Successfully uploaded {local_file_path} to s3://{self.bucket_name}/{s3_key}")
            
            return {
                "success": True,
                "s3_key": s3_key,
                "message": f"File uploaded successfully to s3://{self.bucket_name}/{s3_key}",
            }
        
        except ClientError as e:
            error_msg = f"AWS ClientError uploading {local_file_path}: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_key": s3_key,
                "message": error_msg,
            }
        
        except BotoCoreError as e:
            error_msg = f"AWS BotoCoreError uploading {local_file_path}: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_key": s3_key,
                "message": error_msg,
            }
        
        except Exception as e:
            error_msg = f"Unexpected error uploading {local_file_path}: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_key": s3_key,
                "message": error_msg,
            }
    
    def upload_file_with_structure(
        self,
        local_file_path: str,
        relative_path: str,
    ) -> Dict[str, str]:
        """
        Upload a file preserving the directory structure.
        
        Args:
            local_file_path: Path to the local file
            relative_path: Relative path from data directory (e.g., "2024/file.csv")
            
        Returns:
            Dictionary with upload result
        """
        # Use relative_path as S3 key to preserve structure
        s3_key = relative_path
        
        # Add prefix if specified
        if self.s3_prefix:
            s3_key = f"{self.s3_prefix}/{s3_key}".strip("/")
        
        return self.upload_file(local_file_path, s3_key=s3_key, preserve_structure=False)
    
    def check_bucket_exists(self) -> bool:
        """
        Check if the S3 bucket exists and is accessible.
        
        Returns:
            True if bucket exists and is accessible, False otherwise
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError:
            return False

