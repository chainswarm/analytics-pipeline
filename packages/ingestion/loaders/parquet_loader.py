import os
from pathlib import Path
from typing import List
import pandas as pd
from clickhouse_connect.driver import Client
from loguru import logger

from packages.utils.decorators import log_errors

class ParquetLoader:
    """Loads Parquet files into Local ClickHouse tables."""

    def __init__(self, client: Client):
        self.client = client

    @log_errors
    def load_directory(self, directory_path: Path) -> dict:
        """
        Scans directory for .parquet files and inserts them into ClickHouse.
        
        Files must be named like '{table_name}.parquet' or '{table_name}_{suffix}.parquet'.
        Specifically looks for:
        - money_flows.parquet -> core_money_flows (or temp table)
        - transfers.parquet -> core_transfers
        - asset_prices.parquet -> core_asset_prices
        - assets.parquet -> core_assets
        """
        stats = {}
        
        if not directory_path.exists():
            logger.warning(f"Ingestion directory not found: {directory_path}")
            return stats

        for file_path in directory_path.glob("*.parquet"):
            table_name = self._map_filename_to_table(file_path.name)
            if not table_name:
                logger.warning(f"Skipping unknown file: {file_path.name}")
                continue
            
            rows_inserted = self.load_file(file_path, table_name)
            stats[table_name] = stats.get(table_name, 0) + rows_inserted
            
        return stats

    def load_file(self, file_path: Path, table_name: str) -> int:
        """Loads a single parquet file into ClickHouse."""
        try:
            df = pd.read_parquet(file_path)
            if df.empty:
                logger.info(f"Skipping empty file: {file_path.name}")
                return 0
                
            # Ensure columns match ClickHouse schema expectations if needed
            # For now, we assume the Parquet schema matches the table schema

            # Handle core_asset_prices specifically due to daily partitioning limit (max 100 partitions per insert)
            if table_name == 'core_asset_prices' and 'price_date' in df.columns:
                # Deduplicate before processing to avoid exact replicas
                initial_count = len(df)
                df = df.drop_duplicates()
                if len(df) < initial_count:
                    logger.info(f"Dropped {initial_count - len(df)} duplicate rows from {file_path.name}")

                unique_dates = df['price_date'].unique()
                if len(unique_dates) > 50:
                    logger.info(f"Large date range detected ({len(unique_dates)} days) for {table_name}. Chunking inserts...")
                    chunk_size = 50
                    total_rows = 0
                    
                    # Process in chunks of dates to avoid too many partitions per insert block
                    for i in range(0, len(unique_dates), chunk_size):
                        date_chunk = unique_dates[i:i + chunk_size]
                        df_chunk = df[df['price_date'].isin(date_chunk)]
                        
                        self.client.insert_df(
                            table=table_name,
                            df=df_chunk,
                            database=self.client.database
                        )
                        logger.debug(f"Inserted chunk {i//chunk_size + 1}: {len(df_chunk)} rows ({len(date_chunk)} days)")
                        total_rows += len(df_chunk)
                        
                    logger.info(f"Loaded {total_rows} rows from {file_path.name} into {table_name} (chunked)")
                    return total_rows

            # Standard insert for other tables or small batches
            # Use clickhouse-connect's insert method which handles pandas DataFrames efficiently
            self.client.insert_df(
                table=table_name,
                df=df,
                database=self.client.database
            )
            
            logger.info(f"Loaded {len(df)} rows from {file_path.name} into {table_name}")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load {file_path.name} into {table_name}: {e}")
            raise e

    def _map_filename_to_table(self, filename: str) -> str:
        """Maps parquet filenames to ClickHouse table names."""

        if filename.startswith("transfers"):
            return "core_transfers"
            
        if filename.startswith("asset_prices"):
            return "core_asset_prices"
            
        if filename.startswith("assets"):
            return "core_assets"

        if filename.startswith("address_labels"):
            return "core_address_labels"

        return None
