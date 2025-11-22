import time
import json
import uuid
from typing import Dict, List, Optional, Any
from decimal import Decimal
from clickhouse_connect.driver import Client
from loguru import logger

from packages.storage.repositories.base_repository import BaseRepository
from packages.storage.repositories.utils import row_to_dict
from packages.storage.constants import PatternTypes, DetectionMethods


class StructuralPatternRepository(BaseRepository):
    
    # Mapping of pattern types to their specialized tables
    PATTERN_TYPE_TABLES = {
        PatternTypes.CYCLE: 'analyzers_patterns_cycle',
        PatternTypes.LAYERING_PATH: 'analyzers_patterns_layering',
        PatternTypes.SMURFING_NETWORK: 'analyzers_patterns_network',
        PatternTypes.PROXIMITY_RISK: 'analyzers_patterns_proximity',
        PatternTypes.MOTIF_FANIN: 'analyzers_patterns_motif',
        PatternTypes.MOTIF_FANOUT: 'analyzers_patterns_motif',
        PatternTypes.TEMPORAL_BURST: 'analyzers_patterns_burst',
        PatternTypes.THRESHOLD_EVASION: 'analyzers_patterns_threshold',
    }

    @classmethod
    def table_name(cls) -> str:
        return "analyzers_pattern_detections"

    def __init__(self, client: Client):
        super().__init__(client)
        self.pattern_detections_table = "analyzers_pattern_detections"

    def delete_partition(self, window_days: int, processing_date: str) -> None:
        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        params = {
            'window_days': window_days,
            'processing_date': date_obj
        }
        
        # Delete from all specialized tables
        unique_tables = set(self.PATTERN_TYPE_TABLES.values())
        for table_name in unique_tables:
            query = f"""
            ALTER TABLE {table_name}
            DELETE WHERE window_days = %(window_days)s AND processing_date = %(processing_date)s
            """
            self.client.command(query, parameters=params)
            logger.info(f"Deleted partition for window_days={window_days}, processing_date={processing_date} from {table_name}")

    def get_high_risk_deduplicated_patterns(
        self,
        window_days: int,
        processing_date: str,
        min_risk_score: float = 0.5
    ) -> List[Dict]:
        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        query = f"""
        SELECT *
        FROM {self.pattern_detections_table}
        WHERE window_days = {window_days}
          AND processing_date = '{date_obj}'
        """
        
        result = self.client.query(query)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def insert_deduplicated_patterns(self, patterns: List[Dict], window_days: int, processing_date: str) -> None:
        if not patterns:
            raise ValueError("insert_deduplicated_patterns called with empty patterns list")

        from datetime import datetime
        from collections import defaultdict
        
        batch_size = 1000
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        logger.info(f"Inserting {len(patterns)} deduplicated patterns into specialized tables")
        
        # Group patterns by pattern type
        patterns_by_type = defaultdict(list)
        for pattern in patterns:
            pattern_type = pattern['pattern_type']
            patterns_by_type[pattern_type].append(pattern)
        
        # Insert each pattern type into its specialized table
        for pattern_type, type_patterns in patterns_by_type.items():
            if pattern_type not in self.PATTERN_TYPE_TABLES:
                logger.warning(f"Unknown pattern type '{pattern_type}', skipping {len(type_patterns)} patterns")
                continue
                
            table_name = self.PATTERN_TYPE_TABLES[pattern_type]
            logger.info(f"Inserting {len(type_patterns)} patterns of type '{pattern_type}' into {table_name}")
            
            # Define column names once per pattern type
            if pattern_type == PatternTypes.CYCLE:
                column_names = [
                    'window_days', 'processing_date',
                    'pattern_id', 'pattern_type', 'pattern_hash',
                    'addresses_involved', 'address_roles',
                    'cycle_path', 'cycle_length', 'cycle_volume_usd',
                ]
            elif pattern_type == PatternTypes.LAYERING_PATH:
                column_names = [
                    'window_days', 'processing_date',
                    'pattern_id', 'pattern_type', 'pattern_hash',
                    'addresses_involved', 'address_roles',
                    'layering_path', 'path_depth', 'path_volume_usd', 'source_address', 'destination_address',
                ]
            elif pattern_type == PatternTypes.SMURFING_NETWORK:
                column_names = [
                    'window_days', 'processing_date',
                    'pattern_id', 'pattern_type', 'pattern_hash',
                    'addresses_involved', 'address_roles',
                    'network_members', 'network_size', 'network_density', 'hub_addresses',
                ]
            elif pattern_type == PatternTypes.PROXIMITY_RISK:
                column_names = [
                    'window_days', 'processing_date',
                    'pattern_id', 'pattern_type', 'pattern_hash',
                    'addresses_involved', 'address_roles',
                    'risk_source_address', 'distance_to_risk',
                ]
            elif pattern_type in [PatternTypes.MOTIF_FANIN, PatternTypes.MOTIF_FANOUT]:
                column_names = [
                    'window_days', 'processing_date',
                    'pattern_id', 'pattern_type', 'pattern_hash',
                    'addresses_involved', 'address_roles',
                    'motif_type', 'motif_center_address', 'motif_participant_count',
                ]
            elif pattern_type == PatternTypes.TEMPORAL_BURST:
                column_names = [
                    'window_days', 'processing_date',
                    'pattern_id', 'pattern_type', 'pattern_hash',
                    'addresses_involved', 'address_roles',
                    'burst_address', 'burst_start_timestamp', 'burst_end_timestamp',
                    'burst_duration_seconds', 'burst_transaction_count', 'burst_volume_usd',
                    'normal_tx_rate', 'burst_tx_rate', 'burst_intensity', 'z_score',
                    'hourly_distribution', 'peak_hours',
                ]
            elif pattern_type == PatternTypes.THRESHOLD_EVASION:
                column_names = [
                    'window_days', 'processing_date',
                    'pattern_id', 'pattern_type', 'pattern_hash',
                    'addresses_involved', 'address_roles',
                    'primary_address', 'threshold_value', 'threshold_type',
                    'transactions_near_threshold', 'avg_transaction_size', 'max_transaction_size',
                    'size_consistency', 'clustering_score',
                    'unique_days', 'avg_daily_transactions', 'temporal_spread_score',
                    'threshold_avoidance_score',
                ]
            else:
                logger.warning(f"Unhandled pattern type '{pattern_type}' in column definition")
                continue
            
            # Add common temporal/evidence fields to column names
            column_names.extend([
                'detection_timestamp', 'pattern_start_time', 'pattern_end_time', 'pattern_duration_hours',
                'evidence_transaction_count', 'evidence_volume_usd', 'detection_method',
                '_version'
            ])
            
            # Process in batches
            for i in range(0, len(type_patterns), batch_size):
                batch = type_patterns[i:i + batch_size]
                batch_data = []
                
                for pattern in batch:
                    # Common fields for all pattern types
                    row = [
                        window_days,
                        date_obj,
                        pattern['pattern_id'],
                        pattern['pattern_type'],
                        pattern['pattern_hash'],
                        pattern['addresses_involved'],
                        pattern['address_roles'],
                    ]
                    
                    # Add pattern-specific fields based on type
                    if pattern_type == PatternTypes.CYCLE:
                        row.extend([
                            pattern.get('cycle_path', []),
                            int(pattern.get('cycle_length', 0)),
                            str(pattern.get('cycle_volume_usd', 0)),
                        ])
                    elif pattern_type == PatternTypes.LAYERING_PATH:
                        row.extend([
                            pattern.get('layering_path', []),
                            int(pattern.get('path_depth', 0)),
                            str(pattern.get('path_volume_usd', 0)),
                            pattern.get('source_address', ''),
                            pattern.get('destination_address', ''),
                        ])
                    elif pattern_type == PatternTypes.SMURFING_NETWORK:
                        row.extend([
                            pattern.get('network_members', []),
                            int(pattern.get('network_size', 0)),
                            float(pattern.get('network_density', 0.0)),
                            pattern.get('hub_addresses', []),
                        ])
                    elif pattern_type == PatternTypes.PROXIMITY_RISK:
                        row.extend([
                            pattern.get('risk_source_address', ''),
                            int(pattern.get('distance_to_risk', 0)),
                        ])
                    elif pattern_type in [PatternTypes.MOTIF_FANIN, PatternTypes.MOTIF_FANOUT]:
                        row.extend([
                            pattern.get('motif_type', ''),
                            pattern.get('motif_center_address', ''),
                            int(pattern.get('motif_participant_count', 0)),
                        ])
                    elif pattern_type == PatternTypes.TEMPORAL_BURST:
                        row.extend([
                            pattern.get('burst_address', ''),
                            int(pattern.get('burst_start_timestamp', 0)),
                            int(pattern.get('burst_end_timestamp', 0)),
                            int(pattern.get('burst_duration_seconds', 0)),
                            int(pattern.get('burst_transaction_count', 0)),
                            str(pattern.get('burst_volume_usd', 0)),
                            float(pattern.get('normal_tx_rate', 0.0)),
                            float(pattern.get('burst_tx_rate', 0.0)),
                            float(pattern.get('burst_intensity', 0.0)),
                            float(pattern.get('z_score', 0.0)),
                            pattern.get('hourly_distribution', []),
                            pattern.get('peak_hours', []),
                        ])
                    elif pattern_type == PatternTypes.THRESHOLD_EVASION:
                        row.extend([
                            pattern.get('primary_address', ''),
                            str(pattern.get('threshold_value', 0)),
                            pattern.get('threshold_type', ''),
                            int(pattern.get('transactions_near_threshold', 0)),
                            str(pattern.get('avg_transaction_size', 0)),
                            str(pattern.get('max_transaction_size', 0)),
                            float(pattern.get('size_consistency', 0.0)),
                            float(pattern.get('clustering_score', 0.0)),
                            int(pattern.get('unique_days', 0)),
                            float(pattern.get('avg_daily_transactions', 0.0)),
                            float(pattern.get('temporal_spread_score', 0.0)),
                            float(pattern.get('threshold_avoidance_score', 0.0)),
                        ])
                    
                    # Add common temporal and evidence fields
                    row.extend([
                        int(pattern.get('detection_timestamp', int(time.time()))),
                        int(pattern.get('pattern_start_time', 0)),
                        int(pattern.get('pattern_end_time', 0)),
                        int(pattern.get('pattern_duration_hours', 0)),
                        int(pattern.get('evidence_transaction_count', 0)),
                        str(pattern.get('evidence_volume_usd', 0)),
                        pattern.get('detection_method', DetectionMethods.SCC_ANALYSIS),
                        self._generate_version(),
                    ])
                    
                    batch_data.append(row)
                
                if batch_data:
                    self.client.insert(
                        table_name,
                        batch_data,
                        column_names=column_names
                    )

    def get_deduplicated_patterns(
        self,
        window_days: int,
        processing_date: str,
        pattern_type: Optional[str] = None,
        limit: int = 1_000_000,
        offset: int = 0
    ) -> List[Dict]:
        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        where_conditions = [
            f"window_days = {window_days}",
            f"processing_date = '{date_obj}'"
        ]
        
        if pattern_type:
            where_conditions.append(f"pattern_type = '{pattern_type}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT *
        FROM {self.pattern_detections_table}
        WHERE {where_clause}
        LIMIT {limit} OFFSET {offset}
        """
        
        result = self.client.query(query)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def get_deduplicated_patterns_count(
        self,
        window_days: int,
        processing_date: str,
        pattern_type: Optional[str] = None
    ) -> int:
        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        where_conditions = [
            f"window_days = {window_days}",
            f"processing_date = '{date_obj}'"
        ]
        
        if pattern_type:
            where_conditions.append(f"pattern_type = '{pattern_type}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT count()
        FROM {self.pattern_detections_table}
        WHERE {where_clause}
        """
        
        result = self.client.query(query)
        return int(result.result_rows[0][0])