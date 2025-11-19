from abc import ABC, abstractmethod
from pathlib import Path

class BaseExtractor(ABC):
    """Abstract base class for data extractors."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def extract(self, network: str, processing_date: str, window_days: int) -> Path:
        """
        Extracts data for the given parameters and saves it as Parquet files.
        
        Args:
            network: Blockchain network name (e.g., 'torus')
            processing_date: Date string 'YYYY-MM-DD'
            window_days: Number of days in the window
            
        Returns:
            Path to the directory containing extracted parquet files (usually self.output_dir)
        """
        pass