from __future__ import annotations

from typing import Iterable, Dict, List, Optional, Any, Union, Tuple

from chainswarm_core import DEFAULT_ASSET_CONTRACT
from clickhouse_connect.driver import Client
from chainswarm_core.observability import log_errors
from chainswarm_core.db import BaseRepository, row_to_dict


class TransferRepository(BaseRepository):

    def __init__(self, client: Client):
        super().__init__(client)
        self._order_by = "block_timestamp, tx_id, event_index, edge_index"

    @log_errors
    def insert_transfers(self, rows: Iterable[Union[Dict[str, Any], Dict]]):

        rows = list(rows)
        if not rows:
            return 0

        version = self._generate_version()
        payload = []
        for r in rows:
            payload.append((
                r["tx_id"],
                r.get("event_index", '0'),
                r.get("edge_index", '0'),
                r["block_height"],
                r["block_timestamp"],
                r["from_address"],
                r["to_address"],
                r["asset_symbol"],
                r.get("asset_contract", DEFAULT_ASSET_CONTRACT),
                r["amount"],
                r["fee"],
                r["amount_usd"],
                version
            ))

        self.client.insert('core_transfers', payload, column_names=[
            "tx_id",
            "event_index",
            "edge_index",
            "block_height",
            "block_timestamp",
            "from_address",
            "to_address",
            "asset_symbol",
            "asset_contract",
            "amount",
            "fee",
            "amount_usd",
            "_version",
        ])

    @log_errors  
    def latest_block_height(self) -> Optional[int]:
        block_height = self.client.command(f"SELECT coalesce(max(block_height), 0) as block_height FROM core_transfers")
        return block_height

    @log_errors
    def fetch_time_range(
            self,
            start_timestamp_ms: int,
            end_timestamp_ms: int,
            *,
            from_address: Optional[str] = None,
            to_address: Optional[str] = None,
            asset_contract: Optional[str] = None,
            asset_symbol: Optional[str] = None,
            limit: int = 100000,
    ) -> List[Dict]:
        conds = ["block_timestamp >= %(t0)s", "block_timestamp < %(t1)s"]
        params: Dict[str, Any] = {
            "t0": int(start_timestamp_ms), 
            "t1": int(end_timestamp_ms), 
            "lim": int(limit)
        }

        if from_address:
            conds.append("from_address = %(fa)s")
            params["fa"] = from_address
        if to_address:
            conds.append("to_address = %(ta)s")
            params["ta"] = to_address
        if asset_contract:
            conds.append("asset_contract = %(ac)s")
            params["ac"] = asset_contract
        if asset_symbol:
            conds.append("asset_symbol = %(asym)s")
            params["asym"] = asset_symbol

        q = f"""
        SELECT *
        FROM core_transfers
        WHERE {" AND ".join(conds)}
        ORDER BY {self._order_by}
        LIMIT %(lim)s
        """
        
        result = self.client.query(q, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_distinct_assets_with_first_seen(self, network: str = None) -> List[Dict[str, Any]]:

        q = f"""
        SELECT
            asset_symbol,
            asset_contract,
            min(block_timestamp) as first_seen_timestamp
        FROM core_transfers
        WHERE asset_symbol != ''
        AND asset_contract != ''
        GROUP BY asset_symbol, asset_contract
        ORDER BY first_seen_timestamp
        """
        
        result = self.client.query(q)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_address_amounts_for_statistics(self, address: str, start_ts: int, end_ts: int) -> List[float]:
        query = """
            SELECT amount
            FROM core_transfers
            WHERE (from_address = %(address)s OR to_address = %(address)s)
              AND block_timestamp >= %(start_ts)s AND block_timestamp <= %(end_ts)s
              AND amount > 0
        """
        
        result = self.client.query(query, parameters={
            'address': address, 'start_ts': start_ts, 'end_ts': end_ts
        })
        
        return [row_to_dict(row, result.column_names)['amount'] for row in result.result_rows]

    @log_errors
    def get_address_temporal_data(self, address: str, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:

        query = """
            SELECT
                block_timestamp,
                toHour(toDateTime(intDiv(block_timestamp, 1000))) as hour,
                toDayOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as day_of_week,
                toDate(toDateTime(intDiv(block_timestamp, 1000))) as date,
                amount
            FROM core_transfers
            WHERE (from_address = %(address)s OR to_address = %(address)s)
              AND block_timestamp >= %(start_ts)s AND block_timestamp <= %(end_ts)s
            ORDER BY block_timestamp
        """
        
        result = self.client.query(query, parameters={
            'address': address, 'start_ts': start_ts, 'end_ts': end_ts
        })
        
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_address_behavioral_pattern_data(self, address: str, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
        """Get transaction data for behavioral pattern analysis."""
        query = """
            SELECT
                amount,
                toHour(toDateTime(intDiv(block_timestamp, 1000))) as hour,
                toDayOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as day_of_week
            FROM core_transfers
            WHERE (from_address = %(address)s OR to_address = %(address)s)
              AND block_timestamp >= %(start_ts)s AND block_timestamp <= %(end_ts)s
              AND amount > 0
        """
        
        result = self.client.query(query, parameters={
            'address': address, 'start_ts': start_ts, 'end_ts': end_ts
        })
        
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    @log_errors
    def get_bulk_address_amount_moments(self, addresses: List[str], start_ts: int, end_ts: int) -> Dict[str, Dict[str, Any]]:
        if not addresses:
            return {}
        params: Dict[str, Any] = {
            "t0": int(start_ts),
            "t1": int(end_ts),
        }
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        q = f"""
        WITH t AS (
            SELECT
                CASE
                    WHEN from_address IN ({address_list}) THEN from_address
                    ELSE to_address
                END AS address,
                toFloat64(amount) AS amt
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s AND block_timestamp <= %(t1)s
              AND amount > 0
        )
        SELECT
            address,
            count() AS n,
            sum(amt) AS s1,
            sum(amt * amt) AS s2,
            sum(amt * amt * amt) AS s3,
            sum(amt * amt * amt * amt) AS s4
        FROM t
        GROUP BY address
        """
        result = self.client.query(q, parameters=params)
        out: Dict[str, Dict[str, Any]] = {}
        for addr in addresses:
            out[addr] = {'n': 0, 's1': 0.0, 's2': 0.0, 's3': 0.0, 's4': 0.0}
        for row in result.result_rows:
            addr = row[0]
            out[addr] = {
                'n': int(row[1]) if row[1] is not None else 0,
                's1': float(row[2]) if row[2] is not None else 0.0,
                's2': float(row[3]) if row[3] is not None else 0.0,
                's3': float(row[4]) if row[4] is not None else 0.0,
                's4': float(row[5]) if row[5] is not None else 0.0,
            }
        return out

    @log_errors
    def get_bulk_address_behavioral_counters(self, addresses: List[str], start_ts: int, end_ts: int) -> Dict[str, Dict[str, Any]]:
        if not addresses:
            return {}
        params: Dict[str, Any] = {
            "t0": int(start_ts),
            "t1": int(end_ts),
        }
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        q = f"""
        WITH t AS (
            SELECT
                CASE
                    WHEN from_address IN ({address_list}) THEN from_address
                    ELSE to_address
                END AS address,
                toFloat64(amount) AS amt,
                toHour(toDateTime(block_timestamp / 1000)) AS hour_of_day,
                toDayOfWeek(toDateTime(block_timestamp / 1000)) AS day_of_week
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s AND block_timestamp <= %(t1)s
              AND amount > 0
        )
        SELECT
            address,
            count() AS total_tx_pos_amount,
            sum( if(toUInt64(amt) %% 100 = 0, 1, 0) ) AS round_number_count,
            sum( if(amt < 1000, 1, 0) ) AS small_amount_count,
            sumIf(1, (hour_of_day <= 5) OR (day_of_week IN (6,7))) AS unusual_tx_count,
            sumIf(1, day_of_week IN (6,7)) AS weekend_tx_count,
            sumIf(1, hour_of_day <= 5) AS night_tx_count
        FROM t
        GROUP BY address
        """
        result = self.client.query(q, parameters=params)
        out: Dict[str, Dict[str, Any]] = {}
        for addr in addresses:
            out[addr] = {
                'total_tx_pos_amount': 0,
                'round_number_count': 0,
                'small_amount_count': 0,
                'unusual_tx_count': 0,
                'weekend_tx_count': 0,
                'night_tx_count': 0
            }
        for row in result.result_rows:
            addr = row[0]
            out[addr] = {
                'total_tx_pos_amount': int(row[1]) if row[1] is not None else 0,
                'round_number_count': int(row[2]) if row[2] is not None else 0,
                'small_amount_count': int(row[3]) if row[3] is not None else 0,
                'unusual_tx_count': int(row[4]) if row[4] is not None else 0,
                'weekend_tx_count': int(row[5]) if row[5] is not None else 0,
                'night_tx_count': int(row[6]) if row[6] is not None else 0
            }
        return out

    @log_errors
    def get_bulk_address_hourly_volumes(self, addresses: List[str], start_ts: int, end_ts: int) -> Dict[str, List[float]]:
        if not addresses:
            return {}
        params: Dict[str, Any] = {
            "t0": int(start_ts),
            "t1": int(end_ts),
        }
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        q = f"""
        WITH t AS (
            SELECT
                CASE
                    WHEN from_address IN ({address_list}) THEN from_address
                    ELSE to_address
                END AS address,
                toHour(toDateTime(block_timestamp / 1000)) AS hour_of_day,
                toFloat64(amount) AS amt
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s AND block_timestamp <= %(t1)s
              AND amount > 0
        ),
        agg AS (
            SELECT address, hour_of_day, sum(amt) AS vol
            FROM t
            GROUP BY address, hour_of_day
        )
        SELECT
            a.address,
            arrayMap(h -> coalesce(
                arrayFirst(x -> x.1 = h, groupArray(tuple(hour_of_day, vol))).2, 0.0
            ), range(24)) AS hourly_volumes
        FROM (SELECT DISTINCT address FROM t) a
        LEFT JOIN agg USING (address)
        GROUP BY a.address
        """
        result = self.client.query(q, parameters=params)
        out: Dict[str, List[float]] = {}
        for addr in addresses:
            out[addr] = [0.0] * 24
        for row in result.result_rows:
            addr = row[0]
            vols = list(row[1]) if row[1] is not None else [0.0] * 24
            if len(vols) != 24:
                vols = (vols + [0.0] * 24)[:24]
            out[addr] = [float(v) for v in vols]
        return out

    @log_errors
    def get_bulk_address_interevent_stats(self, addresses: List[str], start_ts: int, end_ts: int) -> Dict[str, Dict[str, Any]]:
        if not addresses:
            return {}
        params: Dict[str, Any] = {"t0": int(start_ts), "t1": int(end_ts)}
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        q = f"""
        WITH t AS (
            SELECT
                CASE WHEN from_address IN ({address_list}) THEN from_address ELSE to_address END AS address,
                toUInt64(block_timestamp) AS ts
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s AND block_timestamp <= %(t1)s
        ),
        agg AS (
            SELECT
                address,
                arraySort(groupArray(ts)) AS ts_arr
            FROM t
            GROUP BY address
        ),
        stats AS (
            SELECT
                address,
                arrayMap(
                    (x, y) -> (toFloat64(x) - toFloat64(y)) / 1000.0,
                    arraySlice(ts_arr, 2, greatest(length(ts_arr) - 1, 0)),
                    arraySlice(ts_arr, 1, greatest(length(ts_arr) - 1, 0))
                ) AS diffs
            FROM agg
        )
        SELECT
            address,
            if(length(diffs) > 0, arrayReduce('avg', diffs), toFloat64(0)) AS mean_inter_s,
            if(length(diffs) > 1, arrayReduce('stddevSamp', diffs), toFloat64(0)) AS std_inter_s,
            length(diffs) AS n
        FROM stats
        """
        result = self.client.query(q, parameters=params)
        out: Dict[str, Dict[str, Any]] = {}
        for addr in addresses:
            out[addr] = {'mean_inter_s': 0.0, 'std_inter_s': 0.0, 'n': 0}
        for row in result.result_rows:
            addr = row[0]
            out[addr] = {
                'mean_inter_s': float(row[1]) if row[1] is not None else 0.0,
                'std_inter_s': float(row[2]) if row[2] is not None else 0.0,
                'n': int(row[3]) if row[3] is not None else 0
            }
        return out

    @log_errors
    def get_bulk_address_outlier_counts(self, addresses: List[str], start_ts: int, end_ts: int) -> Dict[str, int]:
        if not addresses:
            return {}
        params: Dict[str, Any] = {"t0": int(start_ts), "t1": int(end_ts)}
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        q = f"""
        WITH t AS (
            SELECT
                CASE WHEN from_address IN ({address_list}) THEN from_address ELSE to_address END AS address,
                toFloat64(amount) AS amt
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s AND block_timestamp <= %(t1)s
              AND amount > 0
        ),
        q AS (
            SELECT address, quantileTDigest(0.99)(amt) AS q99
            FROM t
            GROUP BY address
        )
        SELECT
            t.address,
            countIf(t.amt > q.q99) AS outliers
        FROM t
        INNER JOIN q USING (address)
        GROUP BY t.address
        """
        result = self.client.query(q, parameters=params)
        out: Dict[str, int] = {}
        for addr in addresses:
            out[addr] = 0
        for row in result.result_rows:
            out[row[0]] = int(row[1]) if row[1] is not None else 0
        return out

    @log_errors
    def get_transfers_for_window(
        self,
        start_timestamp: int,
        end_timestamp: int,
        limit: int = 100000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get all transfers for a specific time window."""
        query = """
            SELECT
                tx_id,
                event_index,
                edge_index,
                block_height,
                block_timestamp,
                from_address,
                to_address,
                asset_symbol,
                asset_contract,
                amount,
                fee
            FROM core_transfers
            WHERE block_timestamp >= %(start_ts)s
              AND block_timestamp < %(end_ts)s
            ORDER BY block_timestamp ASC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        
        result = self.client.query(query, parameters={
            'start_ts': start_timestamp,
            'end_ts': end_timestamp,
            'limit': limit,
            'offset': offset
        })
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

