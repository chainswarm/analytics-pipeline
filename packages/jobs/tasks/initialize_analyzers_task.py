from celery_singleton import Singleton
from chainswarm_core.jobs import BaseTask, BaseTaskContext
from chainswarm_core.observability import setup_logger
from packages.jobs.celery_app import celery_app
from packages.storage.repositories import get_connection_params, ClientFactory, MigrateSchema


class InitializeAnalyzersTask(BaseTask, Singleton):

    def execute_task(self, context: BaseTaskContext):
        connection_params = get_connection_params(context.network)
        
        from packages.storage.repositories import create_database
        create_database(connection_params)

        client_factory = ClientFactory(connection_params)
        with client_factory.client_context() as client:
            migrate_schema = MigrateSchema(client)
            migrate_schema.run_core_migrations()
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

