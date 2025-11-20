import re
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

    def _truncate_base_tables(self):
        """Truncates base core tables."""
        logger.info("Truncating base core tables...")

        base_tables = [
            'core_transfers',
            'core_assets',
            'core_asset_prices',
            'core_address_labels'
        ]

        for table in base_tables:
            try:
                if self._table_exists(table):
                    self.client.command(f"TRUNCATE TABLE {table}")
                    logger.info(f"Truncated {table}")
            except Exception as e:
                logger.warning(f"Truncate failed for {table}: {e}")

    def _execute_schema_file(self, file_path: str):
        """Reads and executes SQL statements from a schema file."""
        from pathlib import Path
        # Assuming file_path is relative to project root or packages/storage/schema
        # If relative path provided, try to resolve from current working dir or known locations

        # Fix: Robust path resolution relative to this file location
        # This ensures it works regardless of where the script is run from
        current_file = Path(__file__).resolve()
        # analytics-pipeline/packages/ingestion/service.py -> analytics-pipeline/packages/storage/schema/
        package_root = current_file.parent.parent
        
        # We expect file_path to be something like 'packages/storage/schema/core_money_flows.sql'
        # OR just 'storage/schema/core_money_flows.sql' if we are lucky,
        # BUT the input is 'packages/storage/schema/core_money_flows.sql'
        
        # Try relative to package root (packages/)
        # If input is 'packages/storage...', we need to strip 'packages/' or go up one more level?
        # Let's try to find the schema directory directly relative to 'ingestion' sibling 'storage'
        
        potential_paths = [
            Path(file_path), # Absolute or relative to CWD
            package_root.parent / file_path, # relative to analytics-pipeline/
            package_root / file_path.replace('packages/', ''), # relative to packages/
            current_file.parent.parent.parent / file_path, # relative to analytics-pipeline root if CWD is different
            # Hardcoded relative path from service.py to schema dir
            current_file.parent.parent / 'storage' / 'schema' / Path(file_path).name
        ]
        
        schema_path = None
        for p in potential_paths:
            if p.exists():
                schema_path = p
                break

        if not schema_path:
            logger.error(f"Schema file not found: {file_path}, checked: {[str(p) for p in potential_paths]}")
            return

        logger.info(f"Executing schema file: {schema_path}")
        try:
            with open(schema_path, 'r') as f:
                sql_content = f.read()
            
            # specialized split logic for ClickHouse SQL files which might contain semi-colons in strings
            # For simple schema files, splitting by ';' usually works.
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            for statement in statements:
                try:
                    self.client.command(statement)
                except Exception as e:
                    logger.warning(f"Failed to execute statement: {statement[:50]}... Error: {e}")
            
            logger.success(f"Executed schema file {schema_path}")

        except Exception as e:
            logger.error(f"Failed to execute schema file {schema_path}: {e}")

    def _table_exists(self, table: str) -> bool:
        return self.client.query(f"EXISTS TABLE {table}").result_rows[0][0] == 1

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
                # 1. Truncate Base Tables
                self._truncate_base_tables()

                # 2. Recreate MVs (Drop & Create) to ensure clean state
                # This truncates implicitly by dropping
                self._execute_schema_file('packages/storage/schema/core_money_flows.sql')

                # 3. Extract & Load
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