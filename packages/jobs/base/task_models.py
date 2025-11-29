from dataclasses import dataclass
from typing import Optional
from chainswarm_core.jobs import BaseTaskContext as CoreBaseTaskContext, BaseTaskResult


@dataclass
class AnalyticsTaskContext(CoreBaseTaskContext):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    batch_size: Optional[int] = None
    min_edge_weight: float = 100.0
    sampling_percentage: float = 0.0
    chain_min_length: int = 3
    chain_max_length: int = 100


__all__ = [
    'AnalyticsTaskContext',
    'BaseTaskResult',
]