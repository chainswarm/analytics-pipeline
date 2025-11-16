from dataclasses import dataclass
from typing import List, Optional


@dataclass
class BaseTaskContext:
    """Unified task context with all parameters for pipeline tasks."""
    
    # Core parameters (required)
    network: str
    window_days: Optional[int] = None
    processing_date: Optional[str] = None
    
    # Backfill parameters (optional)
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    # Task-specific parameters (optional)
    batch_size: Optional[int] = None
    min_edge_weight: float = 100.0
    sampling_percentage: float = 0.0

    # Graph parameters (optional)
    chain_min_length: int = 3
    chain_max_length: int = 100


@dataclass
class BaseTaskResult:
    network: str
    window_days: int
    processing_date: str
    status: str


@dataclass
class PriceTaskResult:
    network: str
    status: str