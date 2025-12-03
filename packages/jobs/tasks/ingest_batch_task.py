import os

from chainswarm_core import get_connection_params, ClientFactory
from loguru import logger
from celery_singleton import Singleton
from chainswarm_core.jobs import BaseTask, BaseTaskContext
from packages.jobs.celery_app import celery_app
from packages.ingestion.service import IngestionService
from packages.storage import DATABASE_PREFIX


class IngestBatchTask(BaseTask, Singleton):

    def execute_task(self, context: BaseTaskContext):
        ingestion_source = os.getenv('INGESTION_SOURCE_TYPE', 'DIRECTORY')
        
        logger.info(
            "Starting batch ingestion",
            extra={
                "network": context.network,
                "window_days": context.window_days,
                "processing_date": context.processing_date,
                "source": ingestion_source
            }
        )

        connection_params = get_connection_params(context.network, database_prefix=DATABASE_PREFIX)
        client_factory = ClientFactory(connection_params)

        with client_factory.client_context() as client:
            service = IngestionService(client, ingestion_source)
            service.run(context.network, context.processing_date, context.window_days)

@celery_app.task(
    bind=True,
    base=IngestBatchTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 5,
        'countdown': 60
    },
    time_limit=3600,
    soft_time_limit=3500
)
def ingest_batch_task(
    self,
    network: str,
    window_days: int,
    processing_date: str
):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        processing_date=processing_date
    )
    
    return self.run(context)