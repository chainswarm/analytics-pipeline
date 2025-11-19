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
        # Mapping based on data-pipeline export standard
        if filename.startswith("money_flows"):
            # Depending on implementation, we might load into core_transfers (source of truth) 
            # or a specific cache table. 
            # Reviewing code: core_transfers is the base. money_flows is derived.
            # The export_batch_task exports: money_flows, transfers, asset_prices, assets.
            # We want to populate the 'core_*' tables.
            return "core_money_flows_mv" # Or core_money_flows if it was a table. 
            # Wait, core_money_flows is a View/MV. We can't insert into it directly usually?
            # Actually, core_money_flows_mv IS a SummingMergeTree, so we CAN insert.
            
        if filename.startswith("transfers"):
            return "core_transfers"
            
        if filename.startswith("asset_prices"):
            return "core_asset_prices"
            
        if filename.startswith("assets"):
            return "core_assets"
            
        if "address_labels" in filename:
            return "core_address_labels"
            
        return None
