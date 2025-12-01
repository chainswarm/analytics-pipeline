from datetime import datetime
from clickhouse_connect.driver import Client

from chainswarm_core.db import BaseRepository


class ComputationAuditRepository(BaseRepository):

    @classmethod
    def table_name(cls) -> str:
        return "analyzers_computation_audit"

    def __init__(self, client: Client):
        super().__init__(client)
        self.table_name = "analyzers_computation_audit"

    def log_completion(
        self,
        window_days: int,
        processing_date: str,
        created_at: datetime,
        end_at: datetime
    ) -> None:
        duration_seconds = int((end_at - created_at).total_seconds())
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        query = f"""
        INSERT INTO {self.table_name} (
            window_days,
            processing_date,
            created_at,
            end_at,
            duration_seconds,
            _version
        ) VALUES (
            %(window_days)s,
            %(processing_date)s,
            %(created_at)s,
            %(end_at)s,
            %(duration_seconds)s,
            %(version)s
        )
        """
        
        parameters = {
            'window_days': window_days,
            'processing_date': date_obj,
            'created_at': created_at,
            'end_at': end_at,
            'duration_seconds': duration_seconds,
            'version': self._generate_version()
        }
        
        self.client.command(query, parameters=parameters)
    
    def get_audit_logs(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> list[dict]:
        """
        Get computation audit logs ordered by processing_date DESC, window_days DESC.
        
        Args:
            limit: Maximum number of rows to return
            offset: Number of rows to skip for pagination
            
        Returns:
            List of audit log dictionaries
        """
        query = f"""
            SELECT
                window_days,
                processing_date,
                created_at,
                end_at,
                duration_seconds
            FROM {self.table_name} FINAL
            ORDER BY processing_date DESC, window_days DESC
            LIMIT {limit}
            OFFSET {offset}
        """
        
        result = self.client.query(query)
        return list(result.named_results())
    
    def get_audit_logs_count(self) -> int:
        """
        Get total count of audit logs.
        
        Returns:
            Total number of audit log entries
        """
        query = f"SELECT COUNT(*) as count FROM {self.table_name} FINAL"
        result = self.client.query(query)
        return result.first_row[0] if result.row_count > 0 else 0