import io
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Iterable
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from loguru import logger

from packages.storage.repositories.base_repository import BaseRepository
from packages.storage.repositories.feature_repository import FeatureRepository
from packages.storage.repositories.computation_audit_repository import ComputationAuditRepository


class MigrateSchema:
    """ClickHouse schema migration manager for analytics pipeline"""
    
    def __init__(self, client: Client):
        self.client = client

    def run_core_migrations(self):
        """Execute core schema migrations (required for ingestion isolation)"""
        core_schemas = [
            "core_assets.sql",
            "core_asset_prices.sql",
            "core_transfers.sql",
            "core_money_flows.sql",
            "core_address_labels.sql"
        ]

        for schema_file in core_schemas:
            try:
                apply_schema(self.client, schema_file)
                logger.info(f"Executed core schema {schema_file}")
            except FileNotFoundError:
                logger.warning(f"Core schema {schema_file} not found, skipping")

    def run_analyzer_migrations(self):
        """Execute analyzer schemas for analytics pipeline"""
        
        analyzer_schemas = [
            "analyzers_features.sql",
            "analyzers_patterns_cycle.sql",
            "analyzers_patterns_layering.sql",
            "analyzers_patterns_network.sql",
            "analyzers_patterns_proximity.sql",
            "analyzers_patterns_motif.sql",
            "analyzers_patterns_burst.sql",
            "analyzers_patterns_threshold.sql",
            "analyzers_pattern_detections_view.sql",  # Must be AFTER all table files
            "analyzers_computation_audit.sql",
        ]
        
        for schema_file in analyzer_schemas:
            apply_schema(self.client, schema_file)
            logger.info(f"Executed {schema_file}")