from dotenv import load_dotenv
from celery_singleton import Singleton

from packages.jobs.base.task_models import BaseTaskContext
from packages.jobs.celery_app import celery_app
from packages.jobs.base.base_task import BaseDataPipelineTask
from packages.storage.repositories import get_connection_params, ClientFactory, MigrateSchema
from packages import setup_logger


class InitializeAnalyzersTask(BaseDataPipelineTask, Singleton):

    def execute_task(self, context: BaseTaskContext):
        service_name = f'analytics-{context.network}-initialize-analyzers'
        setup_logger(service_name)

        # 1. Get params (which now defaults to analytics_{network})
        connection_params = get_connection_params(context.network)
        
        # 2. Ensure Database Exists (connects to default DB to create target)
        from packages.storage.repositories import create_database
        create_database(connection_params)
        
        # 3. Connect to target DB and run migrations
        client_factory = ClientFactory(connection_params)
        with client_factory.client_context() as client:
            migrate_schema = MigrateSchema(client)
            
            # Run core migrations first (for independent ingestion/isolation)
            migrate_schema.run_core_migrations()
            
            # Run analyzer migrations
            migrate_schema.run_analyzer_migrations()


@celery_app.task(
    bind=True,
    base=InitializeAnalyzersTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 24,
        'countdown': 600
    },
    time_limit=7200,
    soft_time_limit=7080
)
def initialize_analyzers_task(self, network: str, window_days: int, processing_date: str):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        processing_date=processing_date
    )

    return self.run(context)

