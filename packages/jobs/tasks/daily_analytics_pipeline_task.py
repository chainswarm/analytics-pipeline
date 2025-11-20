from datetime import datetime
from loguru import logger
from celery_singleton import Singleton

from packages.jobs.celery_app import celery_app
from packages.jobs.base.base_task import BaseDataPipelineTask
from packages.jobs.base.task_models import BaseTaskContext

# Import sub-tasks
from packages.jobs.tasks.ingest_batch_task import IngestBatchTask
from packages.jobs.tasks.initialize_analyzers_task import InitializeAnalyzersTask
from packages.jobs.tasks.build_features_task import BuildFeaturesTask
from packages.jobs.tasks.detect_structural_patterns_task import DetectStructuralPatternsTask
from packages.jobs.tasks.log_computation_audit_task import LogComputationAuditTask


class DailyAnalyticsPipelineTask(BaseDataPipelineTask, Singleton):
    """
    Orchestrates the full daily analytics pipeline sequence:
    1. Ingest Data (from configured source to ClickHouse)
    2. Initialize Analyzers (Schemas & DB check)
    3. Build Features (Graph & ML features)
    4. Detect Structural Patterns (SCC, Money Laundry Patterns)
    5. Audit Log
    """
    
    def execute_task(self, context: BaseTaskContext):
        processing_date = context.processing_date
        network = context.network
        
        logger.info(f"Starting Daily Analytics Pipeline for {network} on {processing_date}")
        
        try:
            # 1. Ingest Data
            # Note: IngestBatchTask handles source resolution and core table cleanup internally
            logger.info("Ingesting Batch Data")
            IngestBatchTask().execute_task(context)

            # 2. Initialize Analyzers
            # Handles creating analytics_{network} DB and migrating all schemas (core + analyzers)
            logger.info("Initializing Analyzers Schema")
            InitializeAnalyzersTask().execute_task(context)
            
            # 3. Build Features
            # Cleans up feature features partition before building
            logger.info("Building Features")
            BuildFeaturesTask().execute_task(context)
            
            # 4. Detect Patterns
            # Cleans up patterns partition before detecting
            logger.info("Detecting Structural Patterns")
            DetectStructuralPatternsTask().execute_task(context)
            
            # 5. Log Audit
            logger.info("Loging Computation Audit")
            
            # Create audit context with start time
            audit_context = BaseTaskContext(
                network=network,
                window_days=context.window_days,
                processing_date=processing_date
            )
            # Assuming the pipeline started roughly when this execute_task started
            # Ideally passed from outside, but good enough for now
            audit_context.pipeline_started_at = datetime.now() 
            
            LogComputationAuditTask().execute_task(audit_context)
            
            logger.success(f"Daily Analytics Pipeline completed successfully for {network} on {processing_date}")
            return {
                "status": "success",
                "network": network,
                "date": processing_date
            }

        except Exception as e:
            logger.error(f"Daily Analytics Pipeline failed: {str(e)}")
            raise e


@celery_app.task(
    bind=True,
    base=DailyAnalyticsPipelineTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 3,
        'countdown': 300
    },
    time_limit=14400, # 4 hours
    soft_time_limit=14000
)
def daily_analytics_pipeline_task(
    self,
    network: str,
    window_days: int,
    processing_date: str,
    batch_size: int = 1000,
    source_config: dict = None
):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        processing_date=processing_date,
        batch_size=batch_size
    )
    # We could extend BaseTaskContext to hold source_config if needed for IngestBatchTask override
    
    return self.run(context)