from pathlib import Path
from chainswarm_core.db import BaseMigrateSchema


class MigrateSchema(BaseMigrateSchema):
    """ClickHouse schema migration manager for analytics pipeline"""

    core_schemas = [
        "core_assets.sql",
        "core_asset_prices.sql",
        "core_transfers.sql",
        "core_money_flows.sql",
        "core_address_labels.sql",
    ]

    analyzer_schemas = [
        "analyzers_features.sql",
        "analyzers_patterns_cycle.sql",
        "analyzers_patterns_layering.sql",
        "analyzers_patterns_network.sql",
        "analyzers_patterns_proximity.sql",
        "analyzers_patterns_motif.sql",
        "analyzers_patterns_burst.sql",
        "analyzers_patterns_threshold.sql",
        "analyzers_pattern_detections_view.sql",
        "analyzers_computation_audit.sql",
    ]

    def get_project_schema_dir(self) -> Path:
        return Path(__file__).parent / "schema"

    def run_core_migrations(self) -> None:
        self.run_schemas_from_dir(self.core_schemas, self.get_project_schema_dir())

    def run_analyzer_migrations(self) -> None:
        self.run_schemas_from_dir(self.analyzer_schemas, self.get_project_schema_dir())


__all__ = ["MigrateSchema"]