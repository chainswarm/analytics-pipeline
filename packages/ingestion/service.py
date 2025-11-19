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

class IngestionService:
    """Orchestrates the data ingestion process."""

    def __init__(self, client: Client, ingestion_source: str):
        self.client = client
        self.ingestion_source = ingestion_source.upper()
        self.loader = ParquetLoader(client)

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
                logger.error(f"Unknown ingestion source: {self.ingestion_source}")
                return

            try:
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