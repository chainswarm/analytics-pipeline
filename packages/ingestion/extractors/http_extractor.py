import os
import requests
import pandas as pd
from pathlib import Path
from loguru import logger
from packages.ingestion.extractors.base import BaseExtractor

class HttpExtractor(BaseExtractor):
    """Extracts data from HTTP API to local Parquet columns."""

    def __init__(self, output_dir: Path):
        super().__init__(output_dir)
        self.base_url = os.getenv('INGESTION_HTTP_URL')
        self.api_key = os.getenv('INGESTION_HTTP_API_KEY')

    def extract(self, network: str, processing_date: str, window_days: int) -> Path:
        # Implementation of HTTP extraction using Export endpoints
        
        if not self.base_url:
             logger.warning("HTTP extraction disabled: INGESTION_HTTP_URL not set")
             return self.output_dir

        logger.info(f"Starting HTTP extraction from {self.base_url} for {network}")
        
        # Calculate dates
        from datetime import datetime, timedelta
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d')
        end_date = processing_date
        start_date = (date_obj - timedelta(days=window_days)).strftime('%Y-%m-%d')

        # Define export tasks mapping: (endpoint, query_params, output_filename)
        exports = [
            (
                "/api/v1/export/transfers",
                {
                    "start_date": start_date, 
                    "end_date": end_date
                },
                "transfers.parquet"
            ),
            (
                "/api/v1/export/money-flows",
                {
                    "start_date": start_date, 
                    "end_date": end_date
                },
                "money_flows.parquet"
            ),
            (
                "/api/v1/export/prices",
                {
                    "start_date": start_date, 
                    "end_date": end_date
                },
                "asset_prices.parquet"
            ),
             (
                "/api/v1/export/assets",
                {},
                "assets.parquet"
            )
        ]
        
        headers = {'Accept': 'application/x-parquet'}
        if self.api_key:
            headers['Authorization'] = f"Bearer {self.api_key}"
            
        for endpoint, params, filename in exports:
            url = f"{self.base_url.rstrip('/')}{endpoint}"
            try:
                logger.info(f"Requesting export from {url} into {filename}...")
                
                # Use GET for export endpoints with query params
                response = requests.get(url, params=params, headers=headers, stream=True)
                
                if response.status_code == 404:
                    logger.warning(f"Endpoint not found: {url} (skipping {filename})")
                    continue
                    
                response.raise_for_status()
                
                # We expect a parquet stream (application/octet-stream or application/x-parquet)
                # Directly write the stream to the output file
                output_path = self.output_dir / filename
                
                total_bytes = 0
                with open(output_path, 'wb') as f:
                     for chunk in response.iter_content(chunk_size=8192):
                         if chunk:
                             f.write(chunk)
                             total_bytes += len(chunk)
                             
                logger.info(f"Downloaded {filename} ({total_bytes / 1024 / 1024:.2f} MB)")
                    
            except Exception as e:
                logger.error(f"Failed request to {url}: {e}")
                # Could raise here if strict consistency is required
        
        return self.output_dir