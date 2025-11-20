import tempfile
import shutil
from pathlib import Path
from loguru import logger
from clickhouse_connect.driver import Client

from packages.ingestion.extractors.base import BaseExtractor
from packages.ingestion.extractors.s3_extractor import S3Extractor
from packages.ingestion.extractors.clickhouse_extractor import ClickHouseExtractor
from packages.ingestion.extractors.http_extractor import HttpExtractor
from packages.ingestion.loaders.parquet_loader import ParquetLoader
from packages.utils import calculate_time_window

class IngestionService:
    """Orchestrates the data ingestion process."""

    def __init__(self, client: Client, ingestion_source: str):
        self.client = client
        self.ingestion_source = ingestion_source.upper()
        self.loader = ParquetLoader(client)

    def _truncate_core_tables(self):
        """
        Truncates core tables and materialized views to prepare for full ingestion.
        This ensures a clean state and that MVs (like money_flows) are rebuilt correctly on insert.
        """
        logger.info("Truncating core tables and materialized views...")

        tables_to_truncate = [
            # Base tables
            'core_transfers',
            'core_assets',
            'core_asset_prices',
            'core_address_labels',

            # Materialized Views (must also be truncated so they don't hold old data)
            'core_money_flows_mv',
            'core_money_flows_daily_mv',
            'core_money_flows_weekly_mv'
        ]

        for table in tables_to_truncate:
            try:
                # Check if table exists
                exists_query = f"EXISTS TABLE {table}"
                if not self.client.query(exists_query).result_rows[0][0]:
                    logger.debug(f"Skipping truncate for {table} (not found)")
                    continue

                query = f"TRUNCATE TABLE {table}"
                self.client.command(query)
                logger.info(f"Truncated {table}")
            except Exception as e:
                logger.warning(f"Truncate failed for {table}: {e}")

    def run(self, network: str, processing_date: str, window_days: int):
        """
        Executes the full ingestion workflow:
        1. Create temp directory
        2. Extract data to temp dir (Universal format)
        3. Load data from temp dir (Loader)
        4. Cleanup
        """
        logger.info(f"Starting ingestion for {network}/{processing_date} from {self.ingestion_source}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            extractor = self._get_extractor(temp_path)
            if not extractor:
                raise ValueError(f"Unknown ingestion source {self.ingestion_source}")

            try:
                # Truncate tables before ingestion (replacing time-window cleanup)
                self._truncate_core_tables()

                output_path = extractor.extract(network, processing_date, window_days)
                
                logger.info(f"Extraction complete. Loading data from {output_path}")
                stats = self.loader.load_directory(output_path)
                
                logger.success(f"Ingestion complete. Loaded stats: {stats}")
                
            except Exception as e:
                logger.error(f"Ingestion failed: {e}")
                raise e

    def _get_extractor(self, output_dir: Path) -> BaseExtractor:
        if self.ingestion_source == 'S3':
            return S3Extractor(output_dir)
        elif self.ingestion_source == 'CLICKHOUSE':
            return ClickHouseExtractor(output_dir)
        elif self.ingestion_source == 'HTTP':
            return HttpExtractor(output_dir)
        else:
            return None