from __future__ import annotations
import time
import datetime as _dt
from typing import Any, Dict, List, Optional, Tuple

from clickhouse_connect.driver import Client
from loguru import logger

from chainswarm_core.observability import log_errors


def _validate_temporal_patterns(patterns_dict: Dict[str, Any], pattern_type: str = "temporal") -> Dict[str, Any]:
    """Validate and fix temporal pattern arrays to ensure consistency."""
    
    # Validate hourly patterns (should be 24 elements)
    hourly_keys = [k for k in patterns_dict.keys() if 'hourly' in k.lower()]
    for key in hourly_keys:
        if key in patterns_dict and isinstance(patterns_dict[key], list):
            original_len = len(patterns_dict[key])
            if original_len != 24:
                patterns_dict[key] = (patterns_dict[key] + [0] * 24)[:24]

    # Validate daily/weekly patterns (should be 7 elements)
    daily_keys = [k for k in patterns_dict.keys() if any(x in k.lower() for x in ['daily', 'weekly'])]
    for key in daily_keys:
        if key in patterns_dict and isinstance(patterns_dict[key], list):
            original_len = len(patterns_dict[key])
            if original_len != 7:
                patterns_dict[key] = (patterns_dict[key] + [0] * 7)[:7]

    # Validate all values are non-negative integers
    for key, value in patterns_dict.items():
        if isinstance(value, list):
            patterns_dict[key] = [max(0, int(v)) if isinstance(v, (int, float)) else 0 for v in value]
    
    return patterns_dict


class TransferAggregationRepository:

    def __init__(self, client: Client):
        self.client = client

    def _extract_network_from_connection(self) -> str:
        """Extract network name from connection parameters."""
        database_name = self.client.database
        if database_name in ['torus', 'bittensor', 'polkadot']:
            return database_name
        return 'torus'  # Default fallback

    @log_errors
    def money_flows_aggregates_usd(
        self,
        *,
        network: str,
        start_timestamp_ms: int,
        end_timestamp_ms: int,
        min_tx_count: int = 1,
        min_usd_sum: Optional[float] = None,
        limit: int = 1_000_000,
    ) -> List[Dict[str, Any]]:

        params: Dict[str, Any] = {
            "start_ts": int(start_timestamp_ms),
            "end_ts": int(end_timestamp_ms),
            "min_count": int(min_tx_count),
            "lim": int(limit),
        }

        usd_filter = ""
        if min_usd_sum is not None:
            usd_filter = "AND amount_usd_sum >= %(min_usd)s"
            params["min_usd"] = float(min_usd_sum)
        
        query = f"""
        WITH
        latest_prices AS (
            SELECT
                asset_contract,
                argMax(price_usd, price_date) as price_usd
            FROM core_asset_prices
            WHERE price_date <= today()
            GROUP BY asset_contract
        ),
        pair_flows AS (
            SELECT
                from_address,
                to_address,
                count() as tx_count,
                sum(amount) as amount_sum,
                sum(amount * COALESCE(p.price_usd, 0)) as amount_usd_sum,
                min(block_timestamp) as first_seen_timestamp,
                max(block_timestamp) as last_seen_timestamp,
                uniq(asset_contract) as unique_assets,
                argMax(asset_symbol, amount * COALESCE(p.price_usd, 0)) as dominant_asset,
                arrayMap(h -> toUInt16(countIf(toHour(toDateTime(block_timestamp / 1000)) = h)), range(24)) as hourly_pattern,
                arrayMap(d -> toUInt16(countIf(toDayOfWeek(toDateTime(block_timestamp / 1000)) = d + 1)), range(7)) as weekly_pattern
            FROM core_transfers t
            LEFT JOIN latest_prices p ON t.asset_contract = p.asset_contract
            WHERE block_timestamp >= %(start_ts)s
              AND block_timestamp < %(end_ts)s
            GROUP BY from_address, to_address
            HAVING tx_count >= %(min_count)s
        ),
        reciprocity AS (
            SELECT
                a.from_address,
                a.to_address,
                CASE
                    WHEN b.amount_usd_sum > 0 AND a.amount_usd_sum > 0
                    THEN least(a.amount_usd_sum, b.amount_usd_sum) / greatest(a.amount_usd_sum, b.amount_usd_sum)
                    ELSE 0
                END as reciprocity_ratio,
                b.amount_usd_sum > 0 as is_bidirectional
            FROM pair_flows a
            LEFT JOIN pair_flows b
                ON a.from_address = b.to_address
                AND a.to_address = b.from_address
        )
        SELECT
            pf.from_address,
            pf.to_address,
            pf.tx_count,
            pf.amount_sum,
            pf.amount_usd_sum,
            pf.first_seen_timestamp,
            pf.last_seen_timestamp,
            toUInt32(greatest(1, intDiv(pf.last_seen_timestamp - pf.first_seen_timestamp, 86400000))) as active_days,
            CASE WHEN pf.tx_count > 0 THEN pf.amount_usd_sum / pf.tx_count ELSE 0 END as avg_tx_size_usd,
            pf.unique_assets,
            pf.dominant_asset,
            pf.hourly_pattern,
            pf.weekly_pattern,
            CAST(r.reciprocity_ratio AS Float32) as reciprocity_ratio,
            r.is_bidirectional
        FROM pair_flows pf
        LEFT JOIN reciprocity r
            ON pf.from_address = r.from_address
            AND pf.to_address = r.to_address
        {usd_filter}
        ORDER BY pf.amount_usd_sum DESC
        LIMIT %(lim)s
        """
        
        result = self.client.query(query, parameters=params)
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        
        logger.info(f"Retrieved {len(rows)} windowed money flows for window [{start_timestamp_ms}, {end_timestamp_ms})")
        return rows

    @log_errors
    def pair_aggregates_by_asset(
        self,
        *,
        start_timestamp_ms: int,
        end_timestamp_ms: int,
        min_tx_count: int = 1,
        limit: int = 1_000_000,
    ) -> List[Dict[str, Any]]:

        params: Dict[str, Any] = {
            "t0": int(start_timestamp_ms),
            "t1": int(end_timestamp_ms),
            "min_tx": int(min_tx_count),
            "lim": int(limit),
        }

        q = f"""
        SELECT
            from_address,
            to_address,
            asset_contract,
            asset_symbol,
            count()                 AS tx_count,
            sum(amount)             AS amount_sum,
            sum(fee)                AS fee_sum,
            min(block_timestamp)    AS first_seen_timestamp,
            max(block_timestamp)    AS last_seen_timestamp
        FROM core_transfers
        WHERE block_timestamp >= %(t0)s
          AND block_timestamp <  %(t1)s
        GROUP BY from_address, to_address, asset_contract, asset_symbol
        HAVING tx_count >= %(min_tx)s
        ORDER BY tx_count DESC, amount_sum DESC
        LIMIT %(lim)s
        """
        
        result = self.client.query(q, parameters=params)
        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        logger.debug(f"pair_aggregates_by_asset window [{start_timestamp_ms}, {end_timestamp_ms}) -> {len(rows)} rows")
        return rows

    @log_errors
    def distinct_addresses_in_range(
        self,
        *,
        start_timestamp_ms: int,
        end_timestamp_ms: int,
        limit: int = 10_000_000,
    ) -> List[str]:

        params = {"t0": int(start_timestamp_ms), "t1": int(end_timestamp_ms), "lim": int(limit)}
        
        q = f"""
        SELECT address
        FROM (
            SELECT from_address AS address
            FROM core_transfers
            WHERE block_timestamp >= %(t0)s AND block_timestamp < %(t1)s
            UNION ALL
            SELECT to_address AS address
            FROM core_transfers
            WHERE block_timestamp >= %(t0)s AND block_timestamp < %(t1)s
        )
        GROUP BY address
        LIMIT %(lim)s
        """
        
        result = self.client.query(q, parameters=params)
        return [row[0] for row in result.result_rows]

    @log_errors
    def top_pairs_for_snapshot(
        self,
        *,
        start_timestamp_ms: int,
        end_timestamp_ms: int,
        min_tx_count: int = 1,
        min_usd_sum: Optional[float] = None,
        limit_pairs: int = 1_000_000,
    ) -> List[Dict[str, Any]]:

        return self.pair_aggregates_usd(
            start_timestamp_ms=start_timestamp_ms,
            end_timestamp_ms=end_timestamp_ms,
            min_tx_count=min_tx_count,
            min_usd_sum=min_usd_sum,
            limit=limit_pairs,
        )
 
    @log_errors
    def get_bulk_address_temporal_patterns(
        self,
        *,
        addresses: List[str],
        start_timestamp_ms: int,
        end_timestamp_ms: int
    ) -> Dict[str, Dict[str, Any]]:
        """Get temporal patterns for multiple addresses in a single query."""

        if not addresses:
            return {}
        
        params = {
            "t0": int(start_timestamp_ms),
            "t1": int(end_timestamp_ms),
        }
        
        # Build address list for ClickHouse IN clause
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        
        q = f"""
        WITH address_transactions AS (
            SELECT
                CASE WHEN from_address IN ({address_list}) THEN from_address ELSE to_address END AS address,
                toHour(toDateTime(block_timestamp / 1000)) AS hour_of_day,
                toDayOfWeek(toDateTime(block_timestamp / 1000)) AS day_of_week
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s
              AND block_timestamp < %(t1)s
        ),
        all_addresses AS (
            SELECT DISTINCT address FROM address_transactions
        ),
        hourly_agg AS (
            SELECT
                address,
                groupArray(tuple(hour_of_day, tx_count)) AS hourly_data,
                argMax(hour_of_day, tx_count) AS peak_activity_hour
            FROM (
                SELECT address, hour_of_day, count() as tx_count
                FROM address_transactions
                GROUP BY address, hour_of_day
            )
            GROUP BY address
        ),
        daily_agg AS (
            SELECT
                address,
                groupArray(tuple(day_of_week, tx_count)) AS daily_data,
                argMax(day_of_week, tx_count) - 1 AS peak_activity_day  -- Convert to 0-based
            FROM (
                SELECT address, day_of_week, count() as tx_count
                FROM address_transactions
                GROUP BY address, day_of_week
            )
            GROUP BY address
        )
        SELECT
            aa.address,
            -- Build hourly activity array using arrayMap
            arrayMap(h -> coalesce(
                arrayFirst(x -> x.1 = h, coalesce(ha.hourly_data, []))
                .2, 0),
                range(24)
            ) AS hourly_activity,
            -- Build daily activity array using arrayMap
            arrayMap(d -> coalesce(
                arrayFirst(x -> x.1 = d + 1, coalesce(da.daily_data, []))
                .2, 0),
                range(7)
            ) AS daily_activity,
            coalesce(ha.peak_activity_hour, 0) AS peak_activity_hour,
            coalesce(da.peak_activity_day, 0) AS peak_activity_day
        FROM all_addresses aa
        LEFT JOIN hourly_agg ha ON aa.address = ha.address
        LEFT JOIN daily_agg da ON aa.address = da.address
        """
        
        result_set = self.client.query(q, parameters=params)
        
        # Build result dictionary, including addresses with no data (default patterns)
        result = {}
        
        # Initialize all requested addresses with default patterns
        for addr in addresses:
            result[addr] = {
                'hourly_activity': [0] * 24,
                'daily_activity': [0] * 7,
                'peak_activity_hour': 0,
                'peak_activity_day': 0
            }
        
        # Update with actual data from query results
        for row in result_set.result_rows:
            address = row[0]
            
            # Ensure arrays are always exactly the correct length
            hourly_activity = list(row[1]) if row[1] else [0] * 24
            original_hourly_len = len(hourly_activity)
            if len(hourly_activity) != 24:
                hourly_activity = (hourly_activity + [0] * 24)[:24]  # Pad or truncate to 24

            daily_activity = list(row[2]) if row[2] else [0] * 7
            original_daily_len = len(daily_activity)
            if len(daily_activity) != 7:
                daily_activity = (daily_activity + [0] * 7)[:7]  # Pad or truncate to 7

            peak_activity_hour = row[3] if row[3] is not None else 0
            peak_activity_day = row[4] if row[4] is not None else 0
            
            address_result = {
                'hourly_activity': hourly_activity,
                'daily_activity': daily_activity,
                'peak_activity_hour': peak_activity_hour,
                'peak_activity_day': peak_activity_day
            }
            
            result[address] = _validate_temporal_patterns(address_result, f"bulk_address_patterns_{address}")

        return result

    @log_errors
    def calculate_structuring_score(self, address: str, start_timestamp_ms: int, end_timestamp_ms: int) -> float:

        params = {
            'address': address,
            'start_ts': start_timestamp_ms,
            'end_ts': end_timestamp_ms
        }
        
        query = f"""
            SELECT amount
            FROM core_transfers
            WHERE (from_address = %(address)s OR to_address = %(address)s)
              AND block_timestamp >= %(start_ts)s
              AND block_timestamp <= %(end_ts)s
              AND amount > 0
        """
        
        result = self.client.query(query, parameters=params)
        
        if not result.result_rows:
            return 0.0
        
        # Convert amounts to Decimal for precise calculations
        from decimal import Decimal
        amounts = [Decimal(str(row[0])) for row in result.result_rows]
        
        # Common reporting thresholds as Decimal (in native token units)
        thresholds = [Decimal('10000'), Decimal('3000'), Decimal('1000'), Decimal('500')]
        
        structuring_count = 0
        threshold_buffer = Decimal('0.05')  # 5% buffer under threshold
        
        # Count transactions that fall just below reporting thresholds
        for amount in amounts:
            for threshold in thresholds:
                lower_bound = threshold * (Decimal('1') - threshold_buffer)
                upper_bound = threshold
                if lower_bound <= amount < upper_bound:
                    structuring_count += 1
                    break
        
        structuring_score = structuring_count / len(amounts)

        return float(structuring_score)

    @log_errors
    def get_money_flows_metrics(
            self,
            *,
            money_flow_table_name: str,
            start_timestamp_ms: int,
            end_timestamp_ms: int
    ) -> Dict[str, Any]:

        params = {
            "table": money_flow_table_name,
            "t0": int(start_timestamp_ms),
            "t1": int(end_timestamp_ms),
        }

        q = f"""
        SELECT
            count() AS money_flows_count,
            sum(amount_usd_sum) AS total_volume_usd,
            sum(tx_count) AS total_tx_count,
            min(window_start_timestamp) AS first_timestamp,
            max(window_end_timestamp) AS last_timestamp
        FROM %(table)s
        WHERE window_start_timestamp >= %(t0)s
          AND window_end_timestamp <= %(t1)s
        """

        result = self.client.query(q, parameters=params)
        if not result.result_rows:
            return {
                'money_flows_count': 0,
                'total_volume_usd': 0.0,
                'total_tx_count': 0,
                'first_timestamp': start_timestamp_ms,
                'last_timestamp': end_timestamp_ms
            }

        row = result.result_rows[0]
        return {
            'money_flows_count': int(row[0]) if row[0] is not None else 0,
            'total_volume_usd': float(row[1]) if row[1] is not None else 0.0,
            'total_tx_count': int(row[2]) if row[2] is not None else 0,
            'first_timestamp': int(row[3]) if row[3] is not None else start_timestamp_ms,
            'last_timestamp': int(row[4]) if row[4] is not None else end_timestamp_ms
        }

    @log_errors
    def get_features_metrics(
            self,
            *,
            feature_table_name: str,
            start_timestamp_ms: int,
            end_timestamp_ms: int
    ) -> Dict[str, Any]:
        """Get metrics about the features aggregation."""

        params = {
            "table": feature_table_name,
            "t0": int(start_timestamp_ms),
            "t1": int(end_timestamp_ms),
        }

        q = f"""
        SELECT
            count() AS address_profiles_count,
            avg(total_volume_usd) AS avg_volume_per_address,
            max(total_volume_usd) AS max_volume_per_address,
            min(total_volume_usd) AS min_volume_per_address
        FROM %(table)s
        WHERE _version >= %(t0)s
          AND _version <= %(t1)s
        """

        result = self.client.query(q, parameters=params)
        if not result.result_rows:
            return {
                'address_profiles_count': 0,
                'avg_volume_per_address': 0.0,
                'max_volume_per_address': 0.0,
                'min_volume_per_address': 0.0
            }

        row = result.result_rows[0]
        return {
            'address_profiles_count': int(row[0]) if row[0] is not None else 0,
            'avg_volume_per_address': float(row[1]) if row[1] is not None else 0.0,
            'max_volume_per_address': float(row[2]) if row[2] is not None else 0.0,
            'min_volume_per_address': float(row[3]) if row[3] is not None else 0.0
        }

    @log_errors
    def get_aggregation_summary(
            self,
            *,
            money_flow_table_name: str,
            feature_table_name: str,
            start_timestamp_ms: int,
            end_timestamp_ms: int
    ) -> Dict[str, Any]:

        money_flows_metrics = self.get_money_flows_metrics(
            money_flow_table_name=money_flow_table_name,
            start_timestamp_ms=start_timestamp_ms,
            end_timestamp_ms=end_timestamp_ms
        )

        features_metrics = self.get_features_metrics(
            feature_table_name=feature_table_name,
            start_timestamp_ms=start_timestamp_ms,
            end_timestamp_ms=end_timestamp_ms
        )

        return {
            'money_flows': money_flows_metrics,
            'features': features_metrics,
            'aggregation_window_days': (end_timestamp_ms - start_timestamp_ms) / (24 * 60 * 60 * 1000)
        }
    @log_errors
    def get_bulk_address_temporal_summaries(
        self,
        *,
        addresses: List[str],
        start_timestamp_ms: int,
        end_timestamp_ms: int
    ) -> Dict[str, Dict[str, Any]]:
        if not addresses:
            return {}

        params: Dict[str, Any] = {
            "t0": int(start_timestamp_ms),
            "t1": int(end_timestamp_ms),
        }

        address_list = ", ".join([f"'{addr}'" for addr in addresses])

        q = f"""
        WITH address_transactions AS (
            SELECT
                CASE
                    WHEN from_address IN ({address_list}) THEN from_address
                    ELSE to_address
                END AS address,
                block_timestamp AS ts,
                toHour(toDateTime(block_timestamp / 1000)) AS hour_of_day,
                toDayOfWeek(toDateTime(block_timestamp / 1000)) AS day_of_week,
                toDate(toDateTime(block_timestamp / 1000)) AS d,
                toFloat64(amount) AS amount
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s
              AND block_timestamp < %(t1)s
        )
        SELECT
            address,
            min(ts) AS first_timestamp,
            max(ts) AS last_timestamp,
            count() AS total_tx_count,
            uniq(d) AS distinct_activity_days,
            sum(amount) AS total_volume,
            sumIf(1, day_of_week IN (6,7)) AS weekend_tx_count,
            sumIf(1, hour_of_day <= 5) AS night_tx_count
        FROM address_transactions
        GROUP BY address
        """

        query_result = self.client.query(q, parameters=params)

        result: Dict[str, Dict[str, Any]] = {}
        # Initialize defaults for all requested addresses
        for addr in addresses:
            result[addr] = {
                'first_timestamp': int(start_timestamp_ms),
                'last_timestamp': int(end_timestamp_ms),
                'total_tx_count': 0,
                'distinct_activity_days': 0,
                'total_volume': 0.0,
                'weekend_tx_count': 0,
                'night_tx_count': 0
            }

        # Update with actual data
        for row in query_result.result_rows:
            addr = row[0]
            result[addr] = {
                'first_timestamp': int(row[1]) if row[1] is not None else int(start_timestamp_ms),
                'last_timestamp': int(row[2]) if row[2] is not None else int(end_timestamp_ms),
                'total_tx_count': int(row[3]) if row[3] is not None else 0,
                'distinct_activity_days': int(row[4]) if row[4] is not None else 0,
                'total_volume': float(row[5]) if row[5] is not None else 0.0,
                'weekend_tx_count': int(row[6]) if row[6] is not None else 0,
                'night_tx_count': int(row[7]) if row[7] is not None else 0
            }

        logger.debug(f"Bulk temporal summaries: queried {len(addresses)} addresses, returned {len(query_result.result_rows)} with data")
        return result
    @log_errors
    def get_bulk_address_reciprocity_stats(
        self,
        *,
        addresses: List[str],
        start_timestamp_ms: int,
        end_timestamp_ms: int
    ) -> Dict[str, Dict[str, Any]]:
        if not addresses:
            return {}
        params: Dict[str, Any] = {
            "t0": int(start_timestamp_ms),
            "t1": int(end_timestamp_ms),
        }
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        q = f"""
        WITH base AS (
            SELECT
                from_address,
                to_address,
                toFloat64(amount) AS amt
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s
              AND block_timestamp <  %(t1)s
        ),
        pair_agg AS (
            SELECT
                from_address,
                to_address,
                sum(amt) AS vol
            FROM base
            GROUP BY from_address, to_address
        ),
        reciprocal_pairs AS (
            SELECT
                a.from_address,
                a.to_address,
                a.vol
            FROM pair_agg a
            INNER JOIN pair_agg b
              ON a.from_address = b.to_address
             AND a.to_address   = b.from_address
        ),
        totals AS (
            SELECT address, sum(vol) AS total_volume
            FROM (
                SELECT from_address AS address, vol FROM pair_agg
                UNION ALL
                SELECT to_address   AS address, vol FROM pair_agg
            )
            GROUP BY address
        ),
        recips AS (
            SELECT address, sum(vol) AS reciprocal_volume
            FROM (
                SELECT from_address AS address, vol FROM reciprocal_pairs
                UNION ALL
                SELECT to_address   AS address, vol FROM reciprocal_pairs
            )
            GROUP BY address
        )
        SELECT
            t.address,
            toFloat64(t.total_volume) AS total_volume,
            toFloat64(ifNull(r.reciprocal_volume, toFloat64(0))) AS reciprocal_volume
        FROM totals t
        LEFT JOIN recips r USING (address)
        """
        query_result = self.client.query(q, parameters=params)
        result: Dict[str, Dict[str, Any]] = {}
        for addr in addresses:
            result[addr] = {'total_volume': 0.0, 'reciprocal_volume': 0.0}
        for row in query_result.result_rows:
            addr = row[0]
            result[addr] = {
                'total_volume': float(row[1]) if row[1] is not None else 0.0,
                'reciprocal_volume': float(row[2]) if row[2] is not None else 0.0
            }
        return result

    @log_errors
    def get_bulk_address_counterparty_stability(
        self,
        *,
        addresses: List[str],
        start_timestamp_ms: int,
        end_timestamp_ms: int,
        buckets: int = 4,
        top_k: int = 10
    ) -> Dict[str, float]:
        if not addresses:
            return {}
        params: Dict[str, Any] = {
            "t0": int(start_timestamp_ms),
            "t1": int(end_timestamp_ms),
            "buckets": int(max(buckets, 1)),
            "topk": int(max(top_k, 1)),
        }
        address_list = ", ".join([f"'{addr}'" for addr in addresses])
        q = f"""
        WITH denom AS (
            SELECT toFloat64(greatest(%(t1)s - %(t0)s, 1)) AS d
        ),
        base AS (
            SELECT
                CASE WHEN from_address IN ({address_list}) THEN from_address ELSE to_address END AS address,
                CASE WHEN from_address IN ({address_list}) THEN to_address   ELSE from_address   END AS counterparty,
                block_timestamp AS ts,
                toFloat64(amount) AS amt
            FROM core_transfers
            WHERE (from_address IN ({address_list}) OR to_address IN ({address_list}))
              AND block_timestamp >= %(t0)s
              AND block_timestamp <  %(t1)s
        ),
        with_idx AS (
            SELECT
                address,
                counterparty,
                toInt32(floor(%(buckets)s * toFloat64(ts - %(t0)s) / (SELECT d FROM denom))) AS idx,
                amt
            FROM base
        ),
        agg AS (
            SELECT
                address,
                idx,
                counterparty,
                sum(amt) AS vol
            FROM with_idx
            GROUP BY address, idx, counterparty
        ),
        ranked AS (
            SELECT
                address,
                idx,
                counterparty,
                vol,
                row_number() OVER (PARTITION BY address, idx ORDER BY vol DESC) AS rn
            FROM agg
        ),
        top AS (
            SELECT address, idx, counterparty
            FROM ranked
            WHERE rn <= %(topk)s
        ),
        sizes AS (
            SELECT address, idx, count() AS sz
            FROM top
            GROUP BY address, idx
        ),
        inter AS (
            SELECT a.address, a.idx AS i, count() AS inter_cnt
            FROM top a
            INNER JOIN top b
              ON a.address = b.address
             AND b.idx = a.idx + 1
             AND a.counterparty = b.counterparty
            GROUP BY a.address, i
        ),
        unions AS (
            SELECT
                i.address AS addr,
                i.i AS bucket_idx,
                toInt64(coalesce(sa.sz, 0)) + toInt64(coalesce(sb.sz, 0)) - toInt64(coalesce(i.inter_cnt, 0)) AS union_sz,
                toInt64(coalesce(i.inter_cnt, 0)) AS inter_sz
            FROM inter i
            LEFT JOIN sizes sa ON sa.address = i.address AND sa.idx = i.i
            LEFT JOIN sizes sb ON sb.address = i.address AND sb.idx = i.i + 1
        ),
        stab AS (
            SELECT
                addr,
                avg( toFloat64(inter_sz) / greatest(toFloat64(union_sz), 1.0) ) AS stability
            FROM unions
            GROUP BY addr
        )
        SELECT addr AS address, toFloat64(ifNull(stability, toFloat64(0))) AS stability FROM stab
        """
        query_result = self.client.query(q, parameters=params)
        result: Dict[str, float] = {}
        for addr in addresses:
            result[addr] = 0.0
        for row in query_result.result_rows:
            result[row[0]] = float(row[1]) if row[1] is not None else 0.0
        return result