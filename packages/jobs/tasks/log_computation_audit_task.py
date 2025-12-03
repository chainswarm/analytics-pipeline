from datetime import datetime

from chainswarm_core import ClientFactory
from chainswarm_core.db import get_connection_params
from loguru import logger
from celery_singleton import Singleton
from chainswarm_core.jobs import BaseTask, BaseTaskContext
from packages.jobs.celery_app import celery_app
from packages.storage import DATABASE_PREFIX
from packages.storage.repositories.computation_audit_repository import ComputationAuditRepository


class LogComputationAuditTask(BaseTask, Singleton):

    def execute_task(self, context: BaseTaskContext):
        connection_params = get_connection_params(context.network, database_prefix=DATABASE_PREFIX)
        client_factory = ClientFactory(connection_params)
        
        with client_factory.client_context() as client:
            computation_audit_repository = ComputationAuditRepository(client)
            
            computation_audit_repository.log_completion(
                window_days=context.window_days,
                processing_date=context.processing_date,
                created_at=datetime.now(),
                end_at=datetime.now()
            )
            
            logger.info(
                "Logged computation audit",
                extra={
                    "network": context.network,
                    "window_days": context.window_days,
                    "processing_date": context.processing_date
                }
            )


@celery_app.task(
    bind=True,
    base=LogComputationAuditTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 3,
        'countdown': 60
    },
    time_limit=300,
    soft_time_limit=280
)
def log_computation_audit_task(
    self,
    network: str,
    window_days: int,
    processing_date: str,
    pipeline_started_at: datetime
):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        processing_date=processing_date
    )
    context.pipeline_started_at = pipeline_started_at
    
    return self.run(context)