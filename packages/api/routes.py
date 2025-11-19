from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException
from celery.result import AsyncResult

from packages.api.models import PipelineRunRequest, PipelineRunResponse
from packages.jobs.tasks.daily_analytics_pipeline_task import daily_analytics_pipeline_task

router = APIRouter()

@router.post("/pipelines/run", response_model=PipelineRunResponse)
async def trigger_pipeline_run(request: PipelineRunRequest):
    """
    Trigger the Daily Analytics Pipeline for a specified date range and network.
    
    This endpoint is designed for on-demand execution, benchmarking, or backfilling.
    Each date in the range will trigger a separate DailyAnalyticsPipelineTask.
    
    Args:
        request: PipelineRunRequest containing network, date_range, etc.
        
    Returns:
        PipelineRunResponse with list of triggering task IDs.
    """
    try:
        start_date = datetime.strptime(request.date_range.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(request.date_range.end_date, "%Y-%m-%d")
        
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
            
        current_date = start_date
        task_ids = []
        processed_dates = []
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            # Trigger Celery task for this specific date and network
            task = daily_analytics_pipeline_task.delay(
                network=request.network,
                window_days=request.window_days,
                processing_date=date_str,
                batch_size=1000, # Default batch size, could be exposed in config
                source_config=request.config
            )
            
            task_ids.append(str(task.id))
            processed_dates.append(date_str)
            
            current_date += timedelta(days=1)
            
        return PipelineRunResponse(
            message=f"Triggered {len(task_ids)} pipeline tasks for network '{request.network}'",
            task_ids=task_ids,
            network=request.network,
            processed_dates=processed_dates
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pipelines/status/{task_id}")
async def get_pipeline_status(task_id: str):
    """Check status of a specific Celery task."""
    task_result = AsyncResult(task_id)
    return {
        "task_id": task_id,
        "status": task_result.status,
        "result": str(task_result.result) if task_result.ready() else None
    }