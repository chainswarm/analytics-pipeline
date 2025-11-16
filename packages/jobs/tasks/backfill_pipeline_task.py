from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
from celery_singleton import Singleton
from packages.jobs.celery_app import celery_app
from packages.jobs.base.base_task import BaseDataPipelineTask
from packages.jobs.base.task_models import BaseTaskContext
from packages.jobs.tasks import InitializeAnalyzersTask
from packages.jobs.tasks.build_features_task import BuildFeaturesTask
from packages.jobs.tasks.detect_structural_patterns_task import DetectStructuralPatternsTask
from packages.jobs.tasks.detect_typologies_task import DetectTypologiesTask
from packages.jobs.tasks.log_computation_audit_task import LogComputationAuditTask


class BackfillPipelineTask(BaseDataPipelineTask, Singleton):
    
    def execute_task(self, context: BaseTaskContext):
        start_date = datetime.strptime(context.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(context.end_date, "%Y-%m-%d")
        
        if start_date > end_date:
            raise ValueError(f"start_date {context.start_date} must be before end_date {context.end_date}")
        
        current_date = start_date
        total_days = (end_date - start_date).days + 1
        processed_days = 0
        
        logger.info(f"Starting Backfill Pipeline for {context.network} from {context.start_date} to {context.end_date} ({total_days} days)")
        
        while current_date <= end_date:
            processing_date_str = current_date.strftime("%Y-%m-%d")
            processed_days += 1
            date_started_at = datetime.now()
            
            logger.info(f"Processing date {processing_date_str} ({processed_days}/{total_days})")
            
            date_context = BaseTaskContext(
                network=context.network,
                window_days=context.window_days,
                processing_date=processing_date_str,
                min_edge_weight=context.min_edge_weight,
                sampling_percentage=context.sampling_percentage,
                batch_size=context.batch_size,
            )
            
            logger.info(f"Step 1/5: Initialize Analyzers for {processing_date_str}")
            initializer_analyzers = InitializeAnalyzersTask()
            initializer_analyzers.execute_task(date_context)
            
            logger.info(f"Step 2/5: Build Features for {processing_date_str}")
            features_task = BuildFeaturesTask()
            features_task.execute_task(date_context)
            
            logger.info(f"Step 3/5: Detect Structural Patterns for {processing_date_str}")
            structural_patterns_task = DetectStructuralPatternsTask()
            structural_patterns_task.execute_task(date_context)
            
            logger.info(f"Step 4/5: Detect Typologies for {processing_date_str}")
            typologies_task = DetectTypologiesTask()
            typologies_task.execute_task(date_context)
            
            logger.info(f"Step 5/5: Log Computation Audit for {processing_date_str}")
            audit_context = BaseTaskContext(
                network=context.network,
                window_days=context.window_days,
                processing_date=processing_date_str
            )
            audit_context.pipeline_started_at = date_started_at
            audit_task = LogComputationAuditTask()
            audit_task.execute_task(audit_context)
            
            logger.success(f"Completed processing for {processing_date_str} ({processed_days}/{total_days})")
            
            current_date += timedelta(days=1)
        
        logger.success(f"Backfill Pipeline completed for {context.network}: processed {processed_days} days from {context.start_date} to {context.end_date}")


@celery_app.task(
    bind=True,
    base=BackfillPipelineTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 24,
        'countdown': 600
    },
    time_limit=7200,
    soft_time_limit=7080
)
def backfill_pipeline_task(
    self,
    network: str,
    start_date: str,
    end_date: str,
    window_days: int = 7,
    min_edge_weight: float = 100.0,
    sampling_percentage: float = 0.0,
    model_network: str = None,
    model_window_days: int = None,
    model_processing_date: str = None,
    batch_size: int = None,
    providers: list = None
):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        min_edge_weight=min_edge_weight,
        sampling_percentage=sampling_percentage,
        model_network=model_network,
        model_window_days=model_window_days,
        model_processing_date=model_processing_date,
        batch_size=batch_size,
        providers=providers
    )
    context.start_date = start_date
    context.end_date = end_date
    
    return self.run(context)

