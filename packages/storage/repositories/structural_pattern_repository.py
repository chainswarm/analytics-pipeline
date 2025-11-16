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

    @classmethod
    def table_name(cls) -> str:
        return "analyzers_pattern_detections"

    def __init__(self, client: Client):
        super().__init__(client)
        self.pattern_detections_table = "analyzers_pattern_detections"

    def delete_partition(self, window_days: int, processing_date: str) -> None:
        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        query = f"""
        ALTER TABLE {self.pattern_detections_table}
        DELETE WHERE window_days = %(window_days)s AND processing_date = %(processing_date)s
        """
        
        params = {
            'window_days': window_days,
            'processing_date': date_obj
        }
        
        self.client.command(query, parameters=params)
        logger.info(f"Deleted partition for window_days={window_days}, processing_date={processing_date} from {self.pattern_detections_table}")

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
          AND risk_score >= {min_risk_score}
        ORDER BY risk_score DESC, severity_score DESC
        """
        
        result = self.client.query(query)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def insert_deduplicated_patterns(self, patterns: List[Dict], window_days: int, processing_date: str) -> None:
        if not patterns:
            raise ValueError("insert_deduplicated_patterns called with empty patterns list")

        from datetime import datetime
        
        batch_size = 1000
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        logger.info(f"Inserting {len(patterns)} deduplicated patterns into {self.pattern_detections_table}")
        
        for i in range(0, len(patterns), batch_size):
            batch = patterns[i:i + batch_size]
            
            batch_data = []
            for pattern in batch:
                batch_data.append([
                    window_days,
                    date_obj,
                    pattern['pattern_id'],
                    pattern['pattern_type'],
                    pattern['pattern_hash'],
                    pattern['addresses_involved'],
                    pattern['address_roles'],
                    float(pattern.get('severity_score', 0.0)),
                    float(pattern.get('confidence_score', 0.0)),
                    float(pattern.get('risk_score', 0.0)),
                    pattern.get('cycle_path', []),
                    int(pattern.get('cycle_length', 0)),
                    str(pattern.get('cycle_volume_usd', 0)),
                    pattern.get('layering_path', []),
                    int(pattern.get('path_depth', 0)),
                    str(pattern.get('path_volume_usd', 0)),
                    pattern.get('source_address', ''),
                    pattern.get('destination_address', ''),
                    pattern.get('network_members', []),
                    int(pattern.get('network_size', 0)),
                    float(pattern.get('network_density', 0.0)),
                    pattern.get('hub_addresses', []),
                    pattern.get('risk_source_address', ''),
                    int(pattern.get('distance_to_risk', 0)),
                    float(pattern.get('risk_propagation_score', 0.0)),
                    pattern.get('motif_type', ''),
                    pattern.get('motif_center_address', ''),
                    int(pattern.get('motif_participant_count', 0)),
                    int(pattern.get('detection_timestamp', int(time.time()))),
                    int(pattern.get('pattern_start_time', 0)),
                    int(pattern.get('pattern_end_time', 0)),
                    int(pattern.get('pattern_duration_hours', 0)),
                    int(pattern.get('evidence_transaction_count', 0)),
                    str(pattern.get('evidence_volume_usd', 0)),
                    pattern.get('detection_method', DetectionMethods.SCC_ANALYSIS),
                    float(pattern.get('anomaly_score', 0.0)),
                    self._generate_version(),
                ])
            
            column_names = [
                'window_days', 'processing_date',
                'pattern_id', 'pattern_type', 'pattern_hash',
                'addresses_involved', 'address_roles',
                'severity_score', 'confidence_score', 'risk_score',
                'cycle_path', 'cycle_length', 'cycle_volume_usd',
                'layering_path', 'path_depth', 'path_volume_usd', 'source_address', 'destination_address',
                'network_members', 'network_size', 'network_density', 'hub_addresses',
                'risk_source_address', 'distance_to_risk', 'risk_propagation_score',
                'motif_type', 'motif_center_address', 'motif_participant_count',
                'detection_timestamp', 'pattern_start_time', 'pattern_end_time', 'pattern_duration_hours',
                'evidence_transaction_count', 'evidence_volume_usd', 'detection_method',
                'anomaly_score',
                '_version'
            ]
            
            self.client.insert(
                self.pattern_detections_table,
                batch_data,
                column_names=column_names
            )

    def get_deduplicated_patterns(
        self,
        window_days: int,
        processing_date: str,
        pattern_type: Optional[str] = None
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
        ORDER BY severity_score DESC, risk_score DESC
        """
        
        result = self.client.query(query)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]
        
        return stats