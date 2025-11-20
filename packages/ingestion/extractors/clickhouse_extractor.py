import os
import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime, timedelta
from clickhouse_connect import get_client

from packages.ingestion.extractors.base import BaseExtractor

class ClickHouseExtractor(BaseExtractor):
    """Extracts data from a remote ClickHouse instance to local Parquet."""

    def __init__(self, output_dir: Path):
        super().__init__(output_dir)
        
        self.host = os.getenv('INGESTION_REMOTE_CH_HOST')
        self.port = int(os.getenv('INGESTION_REMOTE_CH_PORT', '8123'))
        self.user = os.getenv('INGESTION_REMOTE_CH_USER', 'default')
        self.password = os.getenv('INGESTION_REMOTE_CH_PASSWORD', '')
        self.database = os.getenv('INGESTION_REMOTE_CH_DATABASE', 'default')
        
        self.enabled = os.getenv('INGESTION_REMOTE_CH_ENABLED', 'true').lower() == 'true'

    def extract(self, network: str, processing_date: str, window_days: int) -> Path:
        if not self.enabled:
             logger.info("Remote ClickHouse extraction disabled")
             return self.output_dir

        logger.info(f"Starting remote ClickHouse extraction for {network}/{processing_date}")
        
        client = get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database
        )
        
        # Calculate timestamps for query filtering
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d')
        end_timestamp = int(date_obj.timestamp() * 1000)
        start_timestamp = int((date_obj - timedelta(days=window_days)).timestamp() * 1000)
        
        tables_to_extract = [
            ('core_transfers', 'transfers.parquet'),
            ('core_asset_prices', 'asset_prices.parquet'),
            ('core_assets', 'assets.parquet'),
            ('core_address_labels', 'address_labels.parquet'),
        ]
        
        for table_name, file_name in tables_to_extract:
            try:
                query = f"""
                SELECT *
                FROM {table_name}
                WHERE block_timestamp >= {start_timestamp}
                  AND block_timestamp < {end_timestamp}
                """
                
                if table_name == 'core_assets':
                    # Assets might not have block_timestamp or we want all of them
                    query = f"SELECT * FROM {table_name} WHERE network = '{network}'"

                if table_name == 'core_address_labels':
                    # Address labels are network specific but not necessarily time-bound for this window
                    query = f"SELECT * FROM {table_name} WHERE network = '{network}'"
                
                if table_name == 'core_asset_prices':
                    # Price table uses price_date
                     query = f"""
                     SELECT * 
                     FROM {table_name} 
                     WHERE price_date >= toDate('{processing_date}') - {window_days}
                       AND price_date <= toDate('{processing_date}')
                     """

                logger.info(f"Extracting {table_name}...")
                df = client.query_df(query)
                
                if not df.empty:
                    output_path = self.output_dir / file_name
                    df.to_parquet(output_path, index=False)
                    logger.info(f"Exported {len(df)} rows to {file_name}")
                else:
                    logger.warning(f"No data found for {table_name}")
                    
            except Exception as e:
                logger.error(f"Failed to extract {table_name}: {e}")
                # Continue to next table or raise? 
                # Raise ensures we don't have partial data
                raise e
                
        return self.output_dir