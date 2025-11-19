import os
import boto3
from pathlib import Path
from loguru import logger
from botocore.exceptions import ClientError

from packages.ingestion.extractors.base import BaseExtractor

class S3Extractor(BaseExtractor):
    """Downloads Parquet files from S3."""

    def __init__(self, output_dir: Path):
        super().__init__(output_dir)
        self.s3_endpoint = os.getenv('INGESTION_S3_ENDPOINT')
        self.s3_access_key = os.getenv('INGESTION_S3_ACCESS_KEY')
        self.s3_secret_key = os.getenv('INGESTION_S3_SECRET_KEY')
        self.s3_bucket = os.getenv('INGESTION_S3_BUCKET')
        self.s3_region = os.getenv('INGESTION_S3_REGION', 'us-east-1')
        
        if not all([self.s3_endpoint, self.s3_access_key, self.s3_secret_key, self.s3_bucket]):
             # Optional: could raise error here, but maybe user wants to use other extractors
             logger.warning("S3 configuration incomplete. S3Extractor might fail if used.")

    def extract(self, network: str, processing_date: str, window_days: int) -> Path:
        logger.info(f"Starting S3 extraction for {network}/{processing_date}/{window_days}d")
        
        s3 = boto3.client(
            's3',
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            region_name=self.s3_region
        )
        
        # Path structure matches ExportBatchTask: 
        # snapshots/{network}/{processing_date}/{window_days}
        s3_prefix = f"snapshots/{network}/{processing_date}/{window_days}"
        
        try:
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=s3_prefix)
            
            downloaded_count = 0
            
            for page in pages:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    filename = os.path.basename(key)
                    
                    if not filename.endswith('.parquet'):
                        continue
                        
                    local_path = self.output_dir / filename
                    
                    logger.info(f"Downloading s3://{self.s3_bucket}/{key} to {local_path}")
                    s3.download_file(self.s3_bucket, key, str(local_path))
                    downloaded_count += 1
            
            if downloaded_count == 0:
                logger.warning(f"No files found in S3 at {s3_prefix}")
            else:
                logger.success(f"Downloaded {downloaded_count} files from S3")
                
        except ClientError as e:
            logger.error(f"S3 extraction failed: {e}")
            raise
            
        return self.output_dir