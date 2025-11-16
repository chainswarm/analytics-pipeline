import time
from typing import List, Optional, Dict, Union, Any
from clickhouse_connect.driver import Client

from packages.storage.repositories.base_repository import BaseRepository
from packages.storage.repositories.utils import row_to_dict
from packages.storage.constants import Severities, AddressTypes


class AlertsRepository(BaseRepository):
    """Simple alerts repository using string-based operations."""

    @classmethod
    def table_name(cls) -> str:
        return "analyzers_alerts"

    def __init__(self, client: Client):
        super().__init__(client)
        self.table_name = "analyzers_alerts"

    def delete_partition(self, window_days: int, processing_date: str) -> None:
        from datetime import datetime
        from loguru import logger
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        query = f"""
        ALTER TABLE {self.table_name}
        DELETE WHERE window_days = %(window_days)s AND processing_date = %(processing_date)s
        """
        
        params = {
            'window_days': window_days,
            'processing_date': date_obj
        }
        
        self.client.command(query, parameters=params)
        logger.info(f"Deleted partition for window_days={window_days}, processing_date={processing_date} from {self.table_name}")

    def insert_alerts(self, alerts: List[Dict], window_days: int, processing_date: str):
        """Insert alerts with simple string values."""
        batch_size = 1000
        version = self._generate_version()
        
        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()

        for i in range(0, len(alerts), batch_size):
            batch = alerts[i:i + batch_size]

            batch_data = []
            for alert in batch:
                # Use simple string values
                severity = alert.get('severity', Severities.MEDIUM)
                suspected_type = alert.get('suspected_address_type', AddressTypes.UNKNOWN)
                
                # Convert AlertIn to row data matching the schema field order
                batch_data.append([
                    window_days,
                    date_obj,
                    alert['alert_id'],
                    alert['address'],
                    alert.get('typology_type', ''),
                    severity,
                    suspected_type,
                    alert.get('confidence_score', 0.5),
                    alert.get('description', ''),
                    alert.get('volume_usd', 0.0),
                    alert.get('evidence_json', '{}'),
                    alert.get('risk_indicators', []),
                    version,
                ])

            self.client.insert(
                self.table_name,
                batch_data,
                column_names=[
                    'window_days', 'processing_date',
                    'alert_id', 'address', 'typology_type',
                    'severity', 'suspected_address_type', 'alert_confidence_score', 'description', 'volume_usd', 'evidence_json',
                    'risk_indicators', '_version'
                ]
            )

    def insert_alert(self,
                    alert_id: str,
                    address: str,
                    typology_type: str,
                    severity: str,
                    alert_confidence_score: float,
                    description: str = '',
                    volume_usd: float = 0.0,
                    evidence_json: str = '{}',
                    suspected_address_type: str = AddressTypes.UNKNOWN,
                    window_days: int = 30,
                    processing_date: str = None) -> None:
        
        from datetime import datetime
        if processing_date is None:
            processing_date = datetime.now().strftime('%Y-%m-%d')
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        query = f"""
        INSERT INTO {self.table_name} (
            window_days, processing_date,
            alert_id, address, typology_type,
            severity, suspected_address_type, alert_confidence_score, description, volume_usd, evidence_json,
            risk_indicators, _version
        ) VALUES (
            %(window_days)s, %(processing_date)s,
            %(alert_id)s, %(address)s, %(typology_type)s,
            %(severity)s, %(suspected_address_type)s, %(alert_confidence_score)s, %(description)s, %(volume_usd)s, %(evidence_json)s,
            %(risk_indicators)s, %(version)s
        )
        """
        
        parameters = {
            'window_days': window_days,
            'processing_date': date_obj,
            'alert_id': alert_id,
            'address': address,
            'typology_type': typology_type,
            'severity': severity,
            'suspected_address_type': suspected_address_type,
            'alert_confidence_score': alert_confidence_score,
            'description': description,
            'volume_usd': volume_usd,
            'evidence_json': evidence_json,
            'risk_indicators': [],
            'version': self._generate_version()
        }
        
        self.client.command(query, parameters=parameters)

    def get_all_alerts(self, window_days: int = None, processing_date: str = None, limit: int = 100000) -> List[Dict]:
        where_clauses = []
        params = {'limit': limit}
        
        if window_days is not None:
            where_clauses.append("window_days = %(window_days)s")
            params['window_days'] = window_days
            
        if processing_date is not None:
            from datetime import datetime
            date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
            where_clauses.append("processing_date = %(processing_date)s")
            params['processing_date'] = date_obj
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY alert_id DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def get_alerts_by_address(self, address: str, window_days: int = None, processing_date: str = None, limit: int = 100) -> List[Dict]:
        where_clauses = ["address = %(address)s"]
        params = {'address': address, 'limit': limit}
        
        if window_days is not None:
            where_clauses.append("window_days = %(window_days)s")
            params['window_days'] = window_days
            
        if processing_date is not None:
            from datetime import datetime
            date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
            where_clauses.append("processing_date = %(processing_date)s")
            params['processing_date'] = date_obj
        
        where_clause = " AND ".join(where_clauses)
        
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY alert_id DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def get_alert_by_id(self, alert_id: str) -> Optional[Dict]:
        """Get alert by ID, returning dictionary or None if not found."""
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE alert_id = %(alert_id)s
        ORDER BY _version DESC
        LIMIT 1
        """
        
        result = self.client.query(query, parameters={'alert_id': alert_id})
        
        if not result.result_rows:
            return None
            
        return row_to_dict(result.result_rows[0], result.column_names)

    def get_alerts_by_severity(self, severity: str, window_days: int = None, processing_date: str = None, limit: int = 1000) -> List[Dict]:
        where_clauses = ["severity = %(severity)s"]
        params = {'severity': severity, 'limit': limit}
        
        if window_days is not None:
            where_clauses.append("window_days = %(window_days)s")
            params['window_days'] = window_days
            
        if processing_date is not None:
            from datetime import datetime
            date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
            where_clauses.append("processing_date = %(processing_date)s")
            params['processing_date'] = date_obj
        
        where_clause = " AND ".join(where_clauses)
        
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE {where_clause}
        ORDER BY alert_id DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def get_alert_severity_statistics(self, window_days: int = None, processing_date: str = None) -> Dict[str, Any]:
        where_clauses = []
        params = {}
        
        if window_days is not None:
            where_clauses.append("window_days = %(window_days)s")
            params['window_days'] = window_days
            
        if processing_date is not None:
            from datetime import datetime
            date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
            where_clauses.append("processing_date = %(processing_date)s")
            params['processing_date'] = date_obj
        
        where_clause = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
        SELECT
            severity,
            count() as count,
            avg(alert_confidence_score) as avg_confidence
        FROM {self.table_name}
        {where_clause}
        GROUP BY severity ORDER BY severity DESC
        """
        
        result = self.client.query(query, parameters=params)
        
        statistics = {}
        for row in result.result_rows:
            severity_val, count, avg_confidence = row
            statistics[str(severity_val)] = {
                'severity': severity_val,
                'count': count,
                'avg_confidence': avg_confidence
            }
            
        return statistics
