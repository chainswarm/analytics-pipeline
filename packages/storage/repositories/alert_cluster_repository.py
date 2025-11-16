from __future__ import annotations

import time
from typing import List, Dict, Optional
from clickhouse_connect.driver import Client

from packages.storage.repositories.base_repository import BaseRepository
from packages.storage.repositories.utils import row_to_dict
from packages.utils.decorators import log_errors


class AlertClusterRepository(BaseRepository):

    @classmethod
    def table_name(cls) -> str:
        return "analyzers_alert_clusters"

    def __init__(self, client: Client):
        super().__init__(client)
        self.table_name = "analyzers_alert_clusters"

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

    @log_errors
    def get_clusters_for_address(self, address: str, days_back: int = 30) -> List[Dict]:
        """
        Get alert clusters for a specific address within the specified time window.

        Args:
            address: The address to search for
            days_back: Number of days to look back (default: 30)

        Returns:
            List of dictionaries containing cluster data
        """
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE has(addresses_involved, %(address)s)
        """

        start_timestamp = int((time.time() - (days_back * 24 * 60 * 60)) * 1000)

        params = {
            'address': address,
        }

        result = self.client.query(query, parameters=params)
        return result.result_rows

    @log_errors
    def get_clusters_by_addresses(self, addresses: List[str], start_timestamp: int = None, end_timestamp: int = None, network: str = None) -> List[Dict]:
        """
        Get alert clusters for multiple addresses within the specified time window.

        Args:
            addresses: List of addresses to search for
            start_timestamp: Optional start timestamp filter
            end_timestamp: Optional end timestamp filter
            network: Optional network filter (not used in current schema)

        Returns:
            List of dictionaries containing cluster data
        """
        if not addresses:
            return []

        # Create a tuple for the array parameter
        params = {'addresses': addresses}

        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE hasAny(addresses_involved, %(addresses)s)
        """

        result = self.client.query(query, parameters=params)
        return result.result_rows

    @log_errors
    def create_cluster(self, cluster_data: Dict, window_days: int, processing_date: str) -> str:
        """
        Create a new alert cluster.
        
        Args:
            cluster_data: Dictionary containing cluster information with keys:
                - cluster_id (optional): Cluster identifier
                - cluster_type: Type of cluster (same_entity, same_pattern, etc.)
                - primary_alert_id: Representative alert ID
                - related_alert_ids: List of all alert IDs in cluster
                - addresses_involved: List of addresses in cluster
                - total_alerts: Number of alerts in cluster
                - total_volume_usd: Total USD volume
                - severity_max: Highest severity in cluster
                - confidence_avg: Average confidence score
                - earliest_alert_timestamp: First alert timestamp
                - latest_alert_timestamp: Most recent alert timestamp
            window_days: Time window in days
            processing_date: Processing date in YYYY-MM-DD format
            
        Returns:
            The cluster_id of the created cluster
        """
        from datetime import datetime
        
        cluster_id = cluster_data.get('cluster_id', f"cluster_{int(time.time() * 1000)}")
        current_timestamp = int(time.time() * 1000)
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        payload = [(
            window_days,
            date_obj,
            cluster_id,
            cluster_data.get('cluster_type', ''),
            cluster_data.get('primary_alert_id', ''),
            cluster_data.get('related_alert_ids', []),
            cluster_data.get('addresses_involved', []),
            cluster_data.get('total_alerts', 0),
            cluster_data.get('total_volume_usd', 0.0),
            cluster_data.get('severity_max', 'medium'),
            cluster_data.get('confidence_avg', 0.0),
            cluster_data.get('earliest_alert_timestamp', current_timestamp),
            cluster_data.get('latest_alert_timestamp', current_timestamp),
            self._generate_version()
        )]
        
        column_names = [
            'window_days', 'processing_date',
            'cluster_id', 'cluster_type', 'primary_alert_id', 'related_alert_ids',
            'addresses_involved', 'total_alerts', 'total_volume_usd', 'severity_max',
            'confidence_avg', 'earliest_alert_timestamp', 'latest_alert_timestamp',
            '_version'
        ]
        
        self.client.insert(self.table_name, payload, column_names=column_names)
        return cluster_id

    @log_errors
    def update_cluster(self, cluster_id: str, updates: Dict) -> None:
        """
        Update existing cluster with new data.
        
        Args:
            cluster_id: The cluster ID to update
            updates: Dictionary containing fields to update
        """
        current_timestamp = int(time.time() * 1000)
        
        set_clauses = []
        params = {'cluster_id': cluster_id, 'version': self._generate_version()}
        
        if 'related_alert_ids' in updates:
            set_clauses.append('related_alert_ids = %(related_alert_ids)s')
            params['related_alert_ids'] = updates['related_alert_ids']
        
        if 'addresses_involved' in updates:
            set_clauses.append('addresses_involved = %(addresses_involved)s')
            params['addresses_involved'] = updates['addresses_involved']
        
        if 'total_alerts' in updates:
            set_clauses.append('total_alerts = %(total_alerts)s')
            params['total_alerts'] = updates['total_alerts']
        
        if 'total_volume_usd' in updates:
            set_clauses.append('total_volume_usd = %(total_volume_usd)s')
            params['total_volume_usd'] = updates['total_volume_usd']
        
        if 'severity_max' in updates:
            set_clauses.append('severity_max = %(severity_max)s')
            params['severity_max'] = updates['severity_max']
        
        if 'confidence_avg' in updates:
            set_clauses.append('confidence_avg = %(confidence_avg)s')
            params['confidence_avg'] = updates['confidence_avg']
        
        if 'latest_alert_timestamp' in updates:
            set_clauses.append('latest_alert_timestamp = %(latest_alert_timestamp)s')
            params['latest_alert_timestamp'] = updates['latest_alert_timestamp']

        set_clauses.append('_version = %(version)s')
        
        if not set_clauses:
            return
        
        query = f"""
        ALTER TABLE {self.table_name}
        UPDATE {', '.join(set_clauses)}
        WHERE cluster_id = %(cluster_id)s
        """
        
        self.client.command(query, parameters=params)

    @log_errors
    def get_cluster_by_id(self, cluster_id: str) -> Dict:
        """
        Get cluster by ID.
        
        Args:
            cluster_id: The cluster ID to retrieve
            
        Returns:
            Dictionary containing cluster data or None if not found
        """
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE cluster_id = %(cluster_id)s
        ORDER BY _version DESC
        LIMIT 1
        """
        
        result = self.client.query(query, parameters={'cluster_id': cluster_id})
        
        if not result.result_rows:
            return None
        
        return row_to_dict(result.result_rows[0], result.column_names)

    @log_errors
    def get_clusters_by_type(self, cluster_type: str) -> List[Dict]:
        """
        Get all alert clusters of a specific type.
        
        Args:
            cluster_type: The type of cluster to search for
            
        Returns:
            List of dictionaries containing cluster data
        """
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE cluster_type = %(cluster_type)s
        """
        
        params = {'cluster_type': cluster_type}
        
        result = self.client.query(query, parameters=params)

    @log_errors
    def get_all_clusters(self, window_days: int = None, processing_date: str = None, limit: int = 100000) -> List[Dict]:
        """
        Get all alert clusters optionally filtered by window_days and processing_date.
        
        Args:
            window_days: Optional time window filter
            processing_date: Optional processing date filter (YYYY-MM-DD format)
            limit: Maximum number of clusters to return (default: 100000)
            
        Returns:
            List of dictionaries containing cluster data
        """
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
        ORDER BY cluster_id DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]
        return result.result_rows