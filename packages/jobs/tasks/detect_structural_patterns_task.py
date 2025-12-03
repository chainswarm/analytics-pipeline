from chainswarm_core import ClientFactory
from chainswarm_core.db import get_connection_params
from loguru import logger
from celery_singleton import Singleton
from chainswarm_core.jobs import BaseTask, BaseTaskContext
from packages.analyzers.structural.structural_pattern_analyzer import StructuralPatternAnalyzer
from packages.jobs.celery_app import celery_app
from packages.storage import DATABASE_PREFIX
from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.storage.repositories.address_label_repository import AddressLabelRepository
from packages.utils import calculate_time_window


class DetectStructuralPatternsTask(BaseTask, Singleton):

    def execute_task(self, context: BaseTaskContext):
        connection_params = get_connection_params(context.network, database_prefix=DATABASE_PREFIX)

        client_factory = ClientFactory(connection_params)
        with client_factory.client_context() as client:
            pattern_repository = StructuralPatternRepository(client)
            money_flows_repository = MoneyFlowsRepository(client)
            address_label_repository = AddressLabelRepository(client)

            logger.info(f"Cleaning partition for window_days={context.window_days}, processing_date={context.processing_date}")
            pattern_repository.delete_partition(context.window_days, context.processing_date)

            start_timestamp, end_timestamp = calculate_time_window(context.window_days, context.processing_date)

            logger.info("Starting structural pattern analysis for AML detection")
            structural_analyzer = StructuralPatternAnalyzer(
                money_flows_repository=money_flows_repository,
                pattern_repository=pattern_repository,
                address_label_repository=address_label_repository,
                window_days=context.window_days,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                network=context.network,
            )

            structural_analyzer.analyze_structural_patterns()
            logger.success("Structural pattern analysis completed successfully")

@celery_app.task(
    bind=True,
    base=DetectStructuralPatternsTask,
    autoretry_for=(Exception,),
    retry_kwargs={
        'max_retries': 24,
        'countdown': 600
    },
    time_limit=7200,
    soft_time_limit=7080
)
def detect_structural_patterns_task(
    self,
    network: str,
    window_days: int,
    processing_date: str
):
    context = BaseTaskContext(
        network=network,
        window_days=window_days,
        processing_date=processing_date,
    )

    return self.run(context)

