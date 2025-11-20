import os
import tempfile
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from loguru import logger

from packages.storage.repositories import get_connection_params, ClientFactory

router = APIRouter(tags=["export"])

@router.get("/export/features", response_class=FileResponse)
async def export_features(
    request: Request,
    network: str = Query(..., description="Network name"),
    window_days: int = Query(180, description="Window size"),
    limit: int = Query(100000, description="Row limit")
):
    """
    Export features from analyzers_features table to Parquet.
    Used by ML Pipeline for ingestion.
    """
    try:
        # Re-construct get_connection_params logic for dynamic db (analytics_{network})
        # We can reuse existing get_connection_params which already handles the naming if implemented correctly
        # But need to ensure we pass the correct network identifier
        
        params = get_connection_params(network)
        
        client_factory = ClientFactory(params)
        with client_factory.client_context() as client:
            query = f"""
                SELECT *
                FROM analyzers_features
                WHERE window_days = {window_days}
                LIMIT {limit}
            """
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
                logger.info(f"Exporting features to {tmp.name}")
                df = client.query_df(query)
                
                if df.empty:
                    # Even empty, return schema
                    pass
                    
                df.to_parquet(tmp.name, index=False, compression='snappy')
                return FileResponse(tmp.name, media_type='application/octet-stream', filename=f"features_{network}_{window_days}.parquet")
                
    except Exception as e:
        logger.error(f"Failed to export features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/patterns", response_class=FileResponse)
async def export_patterns(
    request: Request,
    network: str = Query(..., description="Network name"),
    window_days: int = Query(180, description="Window size"),
    limit: int = Query(100000, description="Row limit")
):
    """
    Export patterns from analyzers_pattern_detections table to Parquet.
    Used by ML Pipeline for ingestion.
    """
    try:
        params = get_connection_params(network)
        
        client_factory = ClientFactory(params)
        with client_factory.client_context() as client:
            query = f"""
                SELECT *
                FROM analyzers_pattern_detections
                WHERE window_days = {window_days}
                LIMIT {limit}
            """
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
                logger.info(f"Exporting patterns to {tmp.name}")
                df = client.query_df(query)
                
                if df.empty:
                    pass
                    
                df.to_parquet(tmp.name, index=False, compression='snappy')
                return FileResponse(tmp.name, media_type='application/octet-stream', filename=f"patterns_{network}_{window_days}.parquet")
                
    except Exception as e:
        logger.error(f"Failed to export patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))