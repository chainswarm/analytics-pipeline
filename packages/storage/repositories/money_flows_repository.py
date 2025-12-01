from __future__ import annotations

from typing import Dict, List
from decimal import Decimal

from clickhouse_connect.driver import Client

from chainswarm_core.observability import log_errors
from chainswarm_core.db import BaseRepository, row_to_dict


class MoneyFlowsRepository(BaseRepository):

    @classmethod
    def table_name(cls) -> str:
        return "core_money_flows_view"

    def __init__(self, client: Client, table_name: str = None):
        super().__init__(client)
        self.table_name = table_name if table_name else "core_money_flows_view"

    @log_errors
    def get_flows_by_volume(self, min_usd: Decimal, limit: int = 1000) -> List[Dict]:

        params = {
            "min_usd": str(min_usd),
            "limit": int(limit),
        }
        
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE amount_usd_sum >= %(min_usd)s
        ORDER BY amount_usd_sum DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        return (row_to_dict(row, result.column_names) for row in result.result_rows)

    @log_errors
    def get_flows_by_address(self, address: str, start_ts: int, end_ts: int, is_outgoing: bool) -> List[Dict]:
        if is_outgoing:
            where_clause = "from_address = %(address)s"
        else:
            where_clause = "to_address = %(address)s"

        params = {
            "address": address,
            "start_ts": start_ts,
            "end_ts": end_ts,
        }

        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE {where_clause}
          AND last_seen_timestamp >= %(start_ts)s
          AND first_seen_timestamp <= %(end_ts)s
        """

        result = self.client.query(query, parameters=params)
        return (row_to_dict(row, result.column_names) for row in result.result_rows)

    @log_errors
    def get_flows_from_addresses(self, from_addresses: List[str], start_ts: int, end_ts: int) -> List[Dict]:
        params = {
            "from_addresses": from_addresses,
            "start_ts": start_ts,
            "end_ts": end_ts,
        }

        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE from_address IN (%(from_addresses)s)
          AND last_seen_timestamp >= %(start_ts)s
          AND first_seen_timestamp <= %(end_ts)s
        """

        result = self.client.query(query, parameters=params)
        return (row_to_dict(row, result.column_names) for row in result.result_rows)

    @log_errors
    def count_flows(self) -> int:

        query = f"SELECT count() FROM {self.table_name}"

        result = self.client.query(query)
        count = result.result_rows[0][0]
        return int(count)

    @log_errors
    def get_time_range(self) -> tuple[int, int]:
        """Get the actual min/max timestamps from windowed table data."""

        query = f"""
        SELECT
            min(first_seen_timestamp) as min_ts,
            max(last_seen_timestamp) as max_ts
        FROM {self.table_name}
        """
        
        result = self.client.query(query)
        if not result.result_rows:
            raise ValueError(f"No data found in windowed table {self.table_name}")
        
        row = result.result_rows[0]
        min_ts, max_ts = row[0], row[1]
        
        if min_ts is None or max_ts is None:
            raise ValueError(f"NULL timestamps found in windowed table {self.table_name}")

        return int(min_ts), int(max_ts)

    @log_errors
    def get_addresses(self) -> List[str]:
        query = f"""
                SELECT DISTINCT address
                FROM (
                    SELECT from_address as address FROM {self.table_name}
                    UNION ALL
                    SELECT to_address as address FROM {self.table_name}
                )
                ORDER BY address
            """

        result = self.client.query(query)
        addresses = [row[0] for row in result.result_rows] if result.result_rows else []
        return addresses

    @log_errors
    def get_flows_for_address(self, address: str) -> List[Dict]:
        """Get all money flows for a single address (both incoming and outgoing)."""
        
        params = {"address": address}
        
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE from_address = %(address)s
           OR to_address = %(address)s
        ORDER BY first_seen_timestamp ASC
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_node_volumes(self, addresses: List[str]) -> Dict[str, float]:
        """Calculate total volume per node using database aggregation for optimal performance.
        
        This method aggregates USD volumes directly in ClickHouse instead of fetching
        all individual flow records and calculating client-side, providing significant
        performance improvements for k-hop graph analysis.
        
        Args:
            addresses: List of addresses to calculate volumes for
            
        Returns:
            Dictionary mapping address to total USD volume (both incoming and outgoing)
        """
        if not addresses:
            return {}
        
        params = {"addresses": addresses}
        
        # Use UNION ALL to aggregate both incoming and outgoing flows for each address
        # This leverages ClickHouse's columnar storage and aggregation optimizations
        query = f"""
        SELECT
            address,
            SUM(amount_usd_sum) as total_volume
        FROM (
            SELECT from_address as address, amount_usd_sum
            FROM {self.table_name}
            WHERE from_address IN %(addresses)s
            
            UNION ALL
            
            SELECT to_address as address, amount_usd_sum
            FROM {self.table_name}
            WHERE to_address IN %(addresses)s
        )
        GROUP BY address
        ORDER BY address
        """
        
        result = self.client.query(query, parameters=params)
        
        # Convert result to dictionary with float values
        volume_dict = {}
        for row in result.result_rows:
            address = row[0]
            total_volume = float(row[1]) if row[1] is not None else 0.0
            volume_dict[address] = total_volume
        
        # Ensure all requested addresses are included (with 0.0 if no flows found)
        for address in addresses:
            if address not in volume_dict:
                volume_dict[address] = 0.0
        
        return volume_dict

    @log_errors
    def get_flows_for_addresses(self, addresses: List[str]) -> List[Dict]:
        """Get all money flows for multiple addresses in a single query to avoid N+1 problem."""
        
        if not addresses:
            return []
        
        params = {"addresses": addresses}
        
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE from_address IN %(addresses)s
           OR to_address IN %(addresses)s
        ORDER BY first_seen_timestamp ASC
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_fresh_to_exchange_flows(
        self,
        fresh_addresses: List[str],
        network: str,
        start_ts: int,
        end_ts: int
    ) -> List[Dict]:
        """
        Get flows from fresh addresses to known exchanges using optimized 3-way JOIN.
        Eliminates N+1 query pattern by joining money_flows with core_address_labels.
        
        Args:
            fresh_addresses: List of fresh addresses to check
            network: Network identifier
            start_ts: Start timestamp
            end_ts: End timestamp
            
        Returns:
            List of flows with exchange label information
        """
        if not fresh_addresses:
            return []
        
        params = {
            'fresh_addresses': fresh_addresses,
            'network': network,
            'start_ts': start_ts,
            'end_ts': end_ts
        }
        
        query = f"""
        SELECT
            mf.from_address,
            mf.to_address,
            mf.amount_usd_sum,
            al.label,
            al.address_type
        FROM {self.table_name} mf
        INNER JOIN core_address_labels al
            ON mf.to_address = al.address
            AND al.network = %(network)s
            AND al.address_type = 'exchange'
        WHERE mf.from_address IN %(fresh_addresses)s
            AND mf.first_seen_timestamp >= %(start_ts)s
            AND mf.last_seen_timestamp <= %(end_ts)s
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_flows_by_address(self, address: str) -> List[Dict]:
        """Get all money flows for an address (simpler version for API)."""
        params = {"address": address}
        
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE from_address = %(address)s
           OR to_address = %(address)s
        ORDER BY first_seen_timestamp DESC
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_windowed_flows_from_transfers(
        self,
        start_timestamp_ms: int,
        end_timestamp_ms: int,
        limit: int = 1_000_000
    ) -> List[Dict]:
        """
        Query transfers and aggregate flows for time window.
        
        Executes same logic as core_money_flows_view but with time filter.
        This replaces pre-computed analyzers_money_flows tables with query-time aggregation.
        
        Args:
            start_timestamp_ms: Start of time window in milliseconds
            end_timestamp_ms: End of time window in milliseconds
            limit: Maximum number of flows to return (default 1M)
            
        Returns:
            List of aggregated money flows with reciprocity and patterns
        """
        params = {
            "start_ts": int(start_timestamp_ms),
            "end_ts": int(end_timestamp_ms),
            "limit": int(limit)
        }
        
        query = """
        WITH pair_totals AS (
            SELECT
                from_address,
                to_address,
                count() as tx_count,
                sum(amount) as amount_sum,
                sum(amount_usd) as amount_usd_sum,
                min(block_timestamp) as first_seen_timestamp,
                max(block_timestamp) as last_seen_timestamp,
                uniq(asset_contract) as unique_assets,
                argMax(asset_symbol, amount_usd) as dominant_asset,
                arrayMap(h -> countEqual(groupArray(toHour(toDateTime(intDiv(block_timestamp, 1000)))), h), range(24)) as hourly_pattern,
                arrayMap(d -> countEqual(groupArray(toDayOfWeek(toDateTime(intDiv(block_timestamp, 1000)))), d), range(1, 8)) as weekly_pattern
            FROM core_transfers
            WHERE block_timestamp >= %(start_ts)s
              AND block_timestamp < %(end_ts)s
            GROUP BY from_address, to_address
        ),
        reciprocity AS (
            SELECT
                a.from_address,
                a.to_address,
                CASE
                    WHEN b.amount_usd_sum > 0 AND a.amount_usd_sum > 0
                    THEN toFloat64(least(a.amount_usd_sum, b.amount_usd_sum)) / toFloat64(greatest(a.amount_usd_sum, b.amount_usd_sum))
                    ELSE 0
                END as reciprocity_ratio,
                b.amount_usd_sum > 0 as is_bidirectional
            FROM pair_totals a
            LEFT JOIN pair_totals b
                ON a.from_address = b.to_address
                AND a.to_address = b.from_address
        )
        SELECT
            pt.from_address,
            pt.to_address,
            pt.tx_count,
            pt.amount_sum,
            pt.amount_usd_sum,
            pt.first_seen_timestamp,
            pt.last_seen_timestamp,
            toUInt32(greatest(1, intDiv(pt.last_seen_timestamp - pt.first_seen_timestamp, 86400000))) as active_days,
            CASE WHEN pt.tx_count > 0 THEN pt.amount_usd_sum / pt.tx_count ELSE 0 END as avg_tx_size_usd,
            pt.unique_assets,
            pt.dominant_asset,
            pt.hourly_pattern,
            pt.weekly_pattern,
            CAST(r.reciprocity_ratio AS Float32) as reciprocity_ratio,
            r.is_bidirectional
        FROM pair_totals pt
        LEFT JOIN reciprocity r
            ON pt.from_address = r.from_address
            AND pt.to_address = r.to_address
        ORDER BY pt.amount_usd_sum DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]