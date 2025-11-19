from typing import Optional, Dict
from pydantic import BaseModel, Field

class DateRange(BaseModel):
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format", example="2023-10-01")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format", example="2023-10-05")

class PipelineRunRequest(BaseModel):
    network: str = Field(..., description="Network name (e.g., 'torus', 'torus-bench-1'). Determines database isolation.")
    date_range: DateRange
    window_days: int = Field(1, description="Processing window size in days", example=1)
    config: Optional[Dict] = Field(
        default_factory=dict, 
        description="Optional configuration overrides (e.g., {'source': 'S3', 'path': '...'})"
    )

class PipelineRunResponse(BaseModel):
    message: str
    task_ids: list[str]
    network: str
    processed_dates: list[str]