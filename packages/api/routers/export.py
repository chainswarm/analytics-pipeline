import os
import tempfile
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from loguru import logger

from packages.storage.repositories import get_connection_params, ClientFactory
from packages.storage.repositories.feature_repository import FeatureRepository
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.storage.repositories.computation_audit_repository import ComputationAuditRepository
from packages.api.models import PaginatedResponse

router = APIRouter(tags=["export"])

@router.get("/export/features", response_model=PaginatedResponse, responses={200: {"content": {"application/x-parquet": {}}}})
async def export_features(
    request: Request,
    network: str = Query(..., description="Network name"),
    window_days: int = Query(180, description="Window size"),
    processing_date: str = Query(..., description="Processing date in YYYY-MM-DD format"),
    limit: int = Query(1000, description="Row limit (for JSON)"),
    offset: int = Query(0, description="Offset (for JSON)")
):
    """
    Export features from analyzers_features table.
    Supports Parquet export (Stream) or JSON pagination based on Accept header.
    """
    try:
        params = get_connection_params(network)
        accept = request.headers.get('accept', '')
        
        client_factory = ClientFactory(params)
        with client_factory.client_context() as client:
            
            # Parquet Export Mode (Unlimited)
            if 'application/x-parquet' in accept or 'application/octet-stream' in accept:
                # Parse date for safety check
                dt_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
                
                query = f"""
                    SELECT *
                    FROM analyzers_features
                    WHERE window_days = {window_days} AND processing_date = '{dt_obj}'
                """
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
                    logger.info(f"Exporting features to {tmp.name}")
                    df = client.query_df(query)
                    
                    # If empty, still create valid parquet file
                    df.to_parquet(tmp.name, index=False, compression='snappy')
                    return FileResponse(tmp.name, media_type='application/octet-stream', filename=f"features_{network}_{window_days}_{processing_date}.parquet")

            # JSON Pagination Mode
            repository = FeatureRepository(client)
            features = repository.get_all_features(
                window_days=window_days,
                processing_date=processing_date,
                limit=limit,
                offset=offset
            )
            
            total_count = repository.get_window_features_count(
                window_days=window_days,
                processing_date=processing_date
            )
            
            return PaginatedResponse(
                rows=features,
                row_count=len(features), # Count of current page
                offset=offset,
                limit=limit,
                has_more=(offset + len(features) < total_count)
            )
                
    except Exception as e:
        logger.error(f"Failed to export features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/patterns", response_model=PaginatedResponse, responses={200: {"content": {"application/x-parquet": {}}}})
async def export_patterns(
    request: Request,
    network: str = Query(..., description="Network name"),
    window_days: int = Query(180, description="Window size"),
    processing_date: str = Query(..., description="Processing date in YYYY-MM-DD format"),
    limit: int = Query(1000, description="Row limit (for JSON)"),
    offset: int = Query(0, description="Offset (for JSON)")
):
    """
    Export patterns from the pattern detection system.
    
    **Pattern Storage Architecture**:
    Patterns are stored in 5 specialized tables based on pattern type:
    - analyzers_patterns_cycle (cycle patterns)
    - analyzers_patterns_layering (layering paths)
    - analyzers_patterns_network (smurfing networks)
    - analyzers_patterns_proximity (proximity risk)
    - analyzers_patterns_motif (fan-in/fan-out motifs)
    
    This endpoint queries the 'analyzers_pattern_detections' VIEW, which provides
    backward-compatible UNION ALL access to all specialized tables. This architecture:
    - Eliminates NULL columns for better storage efficiency
    - Enables pattern-specific indexing for faster queries
    - Maintains full backward compatibility for API consumers
    
    **Response Format**:
    - Parquet: Set Accept header to 'application/x-parquet' for bulk export
    - JSON: Default response with pagination support
    
    All pattern types are available through this single endpoint.
    See packages/storage/schema/README.md for architecture details.
    """
    try:
        params = get_connection_params(network)
        accept = request.headers.get('accept', '')
        
        client_factory = ClientFactory(params)
        with client_factory.client_context() as client:

            # Parquet Export Mode (Unlimited)
            if 'application/x-parquet' in accept or 'application/octet-stream' in accept:
                dt_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
                
                query = f"""
                    SELECT *
                    FROM analyzers_pattern_detections
                    WHERE window_days = {window_days} AND processing_date = '{dt_obj}'
                """
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
                    logger.info(f"Exporting patterns to {tmp.name}")
                    df = client.query_df(query)
                    
                    df.to_parquet(tmp.name, index=False, compression='snappy')
                    return FileResponse(tmp.name, media_type='application/octet-stream', filename=f"patterns_{network}_{window_days}_{processing_date}.parquet")

            # JSON Pagination Mode
            repository = StructuralPatternRepository(client)
            patterns = repository.get_deduplicated_patterns(
                window_days=window_days,
                processing_date=processing_date,
                limit=limit,
                offset=offset
            )
            
            total_count = repository.get_deduplicated_patterns_count(
                window_days=window_days,
                processing_date=processing_date
            )
            
            return PaginatedResponse(
                rows=patterns,
                row_count=len(patterns),
                offset=offset,
                limit=limit,
                has_more=(offset + len(patterns) < total_count)
            )
                
    except Exception as e:
        logger.error(f"Failed to export patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/audit/computation-logs", response_model=PaginatedResponse)
async def get_computation_audit_logs(
    network: str = Query(..., description="Network name"),
    limit: int = Query(100, description="Row limit"),
    offset: int = Query(0, description="Offset for pagination")
):
    """
    Get computation audit logs showing successful analyzer processing runs.
    
    Returns logs ordered by processing_date DESC, window_days DESC.
    Useful for tracking pipeline execution history and performance metrics.
    
    **Response includes**:
    - window_days: Analysis window size
    - processing_date: Date when analysis was run
    - created_at: Pipeline start time
    - end_at: Pipeline end time
    - duration_seconds: Total execution time
    
    **Example Response**:
    ```json
    {
      "rows": [
        {
          "window_days": 180,
          "processing_date": "2024-01-15",
          "created_at": "2024-01-15T10:00:00.000",
          "end_at": "2024-01-15T10:45:30.000",
          "duration_seconds": 2730
        }
      ],
      "row_count": 1,
      "offset": 0,
      "limit": 100,
      "has_more": false
    }
    ```
    """
    try:
        params = get_connection_params(network)
        client_factory = ClientFactory(params)
        
        with client_factory.client_context() as client:
            repository = ComputationAuditRepository(client)
            
            logs = repository.get_audit_logs(limit=limit, offset=offset)
            total_count = repository.get_audit_logs_count()
            
            return PaginatedResponse(
                rows=logs,
                row_count=len(logs),
                offset=offset,
                limit=limit,
                has_more=(offset + len(logs) < total_count)
            )
                
    except Exception as e:
        logger.error(f"Failed to fetch computation audit logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))