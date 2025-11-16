from dotenv import load_dotenv
from loguru import logger
from celery_singleton import Singleton

from packages.analyzers.typologies import TypologyDetector
from packages.jobs.base.task_models import BaseTaskContext
from packages.jobs.celery_app import celery_app
from packages.jobs.base.base_task import BaseDataPipelineTask
from packages.storage.repositories import get_connection_params, ClientFactory
from packages.storage.repositories.alerts_repository import AlertsRepository
from packages.storage.repositories.alert_cluster_repository import AlertClusterRepository
from packages.storage.repositories.feature_repository import FeatureRepository
from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages import setup_logger
from packages.utils import calculate_time_window

class DetectTypologiesTask(BaseDataPipelineTask, Singleton):

    def execute_task(self, context: BaseTaskContext):

        service_name = f'analytics-{context.network}-detect-typologies'
        setup_logger(service_name)

        connection_params = get_connection_params(context.network)

        client_factory = ClientFactory(connection_params)
        with client_factory.client_context() as client:
            alerts_repository = AlertsRepository(client)
            alert_cluster_repository = AlertClusterRepository(client)
            feature_repository = FeatureRepository(client)
            money_flows_repository = MoneyFlowsRepository(client)
            structural_pattern_repository = StructuralPatternRepository(client)

            logger.info(f"Cleaning partitions for window_days={context.window_days}, processing_date={context.processing_date}")
            alerts_repository.delete_partition(context.window_days, context.processing_date)
            alert_cluster_repository.delete_partition(context.window_days, context.processing_date)

            start_timestamp, end_timestamp = calculate_time_window(context.window_days, context.processing_date)

            logger.info("Starting typology detection")
            detector = TypologyDetector(
                alerts_repository=alerts_repository,
                alert_cluster_repository=alert_cluster_repository,
                feature_repository=feature_repository,
                money_flows_repository=money_flows_repository,
                structural_pattern_repository=structural_pattern_repository,
                window_days=context.window_days,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                network=context.network,
                processing_date=context.processing_date
            )

            detector.detect_typologies()
            logger.success('Typology detection complete')

@celery_app.task(
    bind=True,
    base=DetectTypologiesTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 24,
        'countdown': 600
    },
    time_limit=7200,
    soft_time_limit=7080
)
def detect_typologies_task(self, network: str, window_days: int, processing_date: str):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        processing_date=processing_date
    )
    
    return self.run(context)
