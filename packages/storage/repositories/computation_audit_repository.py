from datetime import datetime
from clickhouse_connect.driver import Client

from packages.storage.repositories.base_repository import BaseRepository


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