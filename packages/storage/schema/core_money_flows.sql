-- =============================================================================
-- MONEY FLOWS: Multi-Resolution Time Series
-- =============================================================================
-- Purpose: Flow aggregation at multiple time granularities for different use cases
--
-- TIER 1: All-Time Aggregation (core_money_flows_mv)
--   - Global first/last seen timestamps
--   - Used by: Memgraph sync, lifetime analytics
--   - Retention: All history
--
-- TIER 2: Daily Time Series (core_money_flows_daily_mv)
--   - Daily aggregations with 90-day TTL
--   - Used by: ML features, sliding windows (7/30/90 days), daily pipeline
--   - Retention: Last 90 days (auto-expires via TTL)
--
-- TIER 3: Weekly Time Series (core_money_flows_weekly_mv)
--   - Weekly aggregations, no TTL
--   - Used by: Historical analysis, backtesting, yearly trends
--   - Retention: All history
--
-- Pattern: MV stores data with ENGINE, views add reciprocity calculations
-- USD values: Pre-calculated in core_transfers (no price joins needed)
-- =============================================================================

-- =============================================================================
-- TIER 1: ALL-TIME MONEY FLOWS
-- =============================================================================

-- Materialized View: Stores aggregated data with historical USD values
CREATE MATERIALIZED VIEW IF NOT EXISTS core_money_flows_mv
ENGINE = SummingMergeTree((
    tx_count,
    amount_sum,
    amount_usd_sum
))
PARTITION BY toYYYYMM(toDate(toDateTime(intDiv(last_seen_timestamp, 1000))))
ORDER BY (from_address, to_address, asset_contract)
SETTINGS index_granularity = 8192
AS
SELECT
    from_address,
    to_address,
    asset_contract,
    asset_symbol,
    
    count() as tx_count,
    sum(amount) as amount_sum,
    sum(amount_usd) as amount_usd_sum,
    
    min(block_timestamp) as first_seen_timestamp,
    max(block_timestamp) as last_seen_timestamp,
    
    arrayMap(h -> countEqual(groupArray(toHour(toDateTime(intDiv(block_timestamp, 1000)))), h), range(24)) as hours_list,
    arrayMap(d -> countEqual(groupArray(toDayOfWeek(toDateTime(intDiv(block_timestamp, 1000)))), d), range(1, 8)) as days_list

FROM core_transfers
GROUP BY from_address, to_address, asset_contract, asset_symbol;

-- View: Aggregates by pair with reciprocity calculations
CREATE OR REPLACE VIEW core_money_flows_view AS
WITH
pair_totals AS (
    SELECT
        from_address,
        to_address,
        sum(tx_count) AS pair_tx_count,
        sum(amount_sum) AS pair_amount,
        sum(amount_usd_sum) AS pair_amount_usd,
        min(first_seen_timestamp) as first_seen_timestamp,
        max(last_seen_timestamp) as last_seen_timestamp,
        uniq(asset_contract) as unique_assets,
        argMax(asset_symbol, amount_usd_sum) as dominant_asset,
        arrayMap(i -> toUInt16(arraySum(arrayMap(x -> x[i + 1], groupArray(hours_list)))), range(24)) as hourly_pattern,
        arrayMap(i -> toUInt16(arraySum(arrayMap(x -> x[i + 1], groupArray(days_list)))), range(7)) as weekly_pattern
    FROM core_money_flows_mv
    GROUP BY from_address, to_address
),
reciprocity AS (
    SELECT
        a.from_address,
        a.to_address,
        CASE
            WHEN b.pair_amount_usd > 0 AND a.pair_amount_usd > 0
            THEN toFloat64(least(a.pair_amount_usd, b.pair_amount_usd)) / toFloat64(greatest(a.pair_amount_usd, b.pair_amount_usd))
            ELSE 0
        END as reciprocity_ratio,
        b.pair_amount_usd > 0 as is_bidirectional
    FROM pair_totals a
    LEFT JOIN pair_totals b
        ON a.from_address = b.to_address
        AND a.to_address = b.from_address
)
SELECT
    pt.from_address,
    pt.to_address,
    pt.pair_tx_count as tx_count,
    pt.pair_amount as amount_sum,
    pt.pair_amount_usd as amount_usd_sum,
    pt.first_seen_timestamp,
    pt.last_seen_timestamp,
    toUInt32(greatest(1, intDiv(pt.last_seen_timestamp - pt.first_seen_timestamp, 86400000))) as active_days,
    CASE WHEN pt.pair_tx_count > 0 THEN pt.pair_amount_usd / pt.pair_tx_count ELSE 0 END as avg_tx_size_usd,
    pt.unique_assets,
    pt.dominant_asset,
    pt.hourly_pattern,
    pt.weekly_pattern,
    CAST(r.reciprocity_ratio AS Float32) as reciprocity_ratio,
    r.is_bidirectional
FROM pair_totals pt
LEFT JOIN reciprocity r
    ON pt.from_address = r.from_address
    AND pt.to_address = r.to_address;

-- =============================================================================
-- TIER 2: DAILY MONEY FLOWS (90-Day TTL)
-- =============================================================================

-- Materialized View: Daily aggregations with automatic 90-day cleanup
CREATE MATERIALIZED VIEW IF NOT EXISTS core_money_flows_daily_mv
ENGINE = SummingMergeTree((
    tx_count,
    amount_sum,
    amount_usd_sum
))
PARTITION BY toYYYYMM(activity_date)
ORDER BY (from_address, to_address, asset_contract, activity_date)
TTL activity_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192
AS
SELECT
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as activity_date,
    from_address,
    to_address,
    asset_contract,
    asset_symbol,
    
    count() as tx_count,
    sum(amount) as amount_sum,
    sum(amount_usd) as amount_usd_sum,
    
    min(block_timestamp) as first_seen_timestamp,
    max(block_timestamp) as last_seen_timestamp,
    
    arrayMap(h -> countEqual(groupArray(toHour(toDateTime(intDiv(block_timestamp, 1000)))), h), range(24)) as hours_list,
    arrayMap(d -> countEqual(groupArray(toDayOfWeek(toDateTime(intDiv(block_timestamp, 1000)))), d), range(1, 8)) as days_list

FROM core_transfers
GROUP BY activity_date, from_address, to_address, asset_contract, asset_symbol;

-- View: Daily aggregates by pair with reciprocity calculations
CREATE OR REPLACE VIEW core_money_flows_daily_view AS
WITH
pair_totals AS (
    SELECT
        activity_date,
        from_address,
        to_address,
        sum(tx_count) AS pair_tx_count,
        sum(amount_sum) AS pair_amount,
        sum(amount_usd_sum) AS pair_amount_usd,
        min(first_seen_timestamp) as first_seen_timestamp,
        max(last_seen_timestamp) as last_seen_timestamp,
        uniq(asset_contract) as unique_assets,
        argMax(asset_symbol, amount_usd_sum) as dominant_asset,
        arrayMap(i -> toUInt16(arraySum(arrayMap(x -> x[i + 1], groupArray(hours_list)))), range(24)) as hourly_pattern,
        arrayMap(i -> toUInt16(arraySum(arrayMap(x -> x[i + 1], groupArray(days_list)))), range(7)) as weekly_pattern
    FROM core_money_flows_daily_mv
    GROUP BY activity_date, from_address, to_address
),
reciprocity AS (
    SELECT
        a.activity_date,
        a.from_address,
        a.to_address,
        CASE
            WHEN b.pair_amount_usd > 0 AND a.pair_amount_usd > 0
            THEN toFloat64(least(a.pair_amount_usd, b.pair_amount_usd)) / toFloat64(greatest(a.pair_amount_usd, b.pair_amount_usd))
            ELSE 0
        END as reciprocity_ratio,
        b.pair_amount_usd > 0 as is_bidirectional
    FROM pair_totals a
    LEFT JOIN pair_totals b
        ON a.activity_date = b.activity_date
        AND a.from_address = b.to_address
        AND a.to_address = b.from_address
)
SELECT
    pt.activity_date,
    pt.from_address,
    pt.to_address,
    pt.pair_tx_count as tx_count,
    pt.pair_amount as amount_sum,
    pt.pair_amount_usd as amount_usd_sum,
    pt.first_seen_timestamp,
    pt.last_seen_timestamp,
    toUInt32(greatest(1, intDiv(pt.last_seen_timestamp - pt.first_seen_timestamp, 86400000))) as active_days,
    CASE WHEN pt.pair_tx_count > 0 THEN pt.pair_amount_usd / pt.pair_tx_count ELSE 0 END as avg_tx_size_usd,
    pt.unique_assets,
    pt.dominant_asset,
    pt.hourly_pattern,
    pt.weekly_pattern,
    CAST(r.reciprocity_ratio AS Float32) as reciprocity_ratio,
    r.is_bidirectional
FROM pair_totals pt
LEFT JOIN reciprocity r
    ON pt.activity_date = r.activity_date
    AND pt.from_address = r.from_address
    AND pt.to_address = r.to_address;

-- =============================================================================
-- TIER 3: WEEKLY MONEY FLOWS (No TTL, All History)
-- =============================================================================

-- Materialized View: Weekly aggregations for historical analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS core_money_flows_weekly_mv
ENGINE = SummingMergeTree((
    tx_count,
    amount_sum,
    amount_usd_sum
))
PARTITION BY (year, week_number)
ORDER BY (from_address, to_address, asset_contract, week_start_date)
SETTINGS index_granularity = 8192
AS
SELECT
    toYear(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as year,
    toWeek(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as week_number,
    toMonday(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as week_start_date,
    toMonday(toDate(toDateTime(intDiv(block_timestamp, 1000)))) + INTERVAL 6 DAY as week_end_date,
    from_address,
    to_address,
    asset_contract,
    asset_symbol,
    
    count() as tx_count,
    sum(amount) as amount_sum,
    sum(amount_usd) as amount_usd_sum,
    
    min(block_timestamp) as first_seen_timestamp,
    max(block_timestamp) as last_seen_timestamp,
    
    arrayMap(h -> countEqual(groupArray(toHour(toDateTime(intDiv(block_timestamp, 1000)))), h), range(24)) as hours_list,
    arrayMap(d -> countEqual(groupArray(toDayOfWeek(toDateTime(intDiv(block_timestamp, 1000)))), d), range(1, 8)) as days_list

FROM core_transfers
GROUP BY year, week_number, week_start_date, week_end_date, from_address, to_address, asset_contract, asset_symbol;

-- View: Weekly aggregates by pair with reciprocity calculations
CREATE OR REPLACE VIEW core_money_flows_weekly_view AS
WITH
pair_totals AS (
    SELECT
        year,
        week_number,
        week_start_date,
        week_end_date,
        from_address,
        to_address,
        sum(tx_count) AS pair_tx_count,
        sum(amount_sum) AS pair_amount,
        sum(amount_usd_sum) AS pair_amount_usd,
        min(first_seen_timestamp) as first_seen_timestamp,
        max(last_seen_timestamp) as last_seen_timestamp,
        uniq(asset_contract) as unique_assets,
        argMax(asset_symbol, amount_usd_sum) as dominant_asset,
        arrayMap(i -> toUInt16(arraySum(arrayMap(x -> x[i + 1], groupArray(hours_list)))), range(24)) as hourly_pattern,
        arrayMap(i -> toUInt16(arraySum(arrayMap(x -> x[i + 1], groupArray(days_list)))), range(7)) as weekly_pattern
    FROM core_money_flows_weekly_mv
    GROUP BY year, week_number, week_start_date, week_end_date, from_address, to_address
),
reciprocity AS (
    SELECT
        a.year,
        a.week_number,
        a.from_address,
        a.to_address,
        CASE
            WHEN b.pair_amount_usd > 0 AND a.pair_amount_usd > 0
            THEN toFloat64(least(a.pair_amount_usd, b.pair_amount_usd)) / toFloat64(greatest(a.pair_amount_usd, b.pair_amount_usd))
            ELSE 0
        END as reciprocity_ratio,
        b.pair_amount_usd > 0 as is_bidirectional
    FROM pair_totals a
    LEFT JOIN pair_totals b
        ON a.year = b.year
        AND a.week_number = b.week_number
        AND a.from_address = b.to_address
        AND a.to_address = b.from_address
)
SELECT
    pt.year,
    pt.week_number,
    pt.week_start_date,
    pt.week_end_date,
    pt.from_address,
    pt.to_address,
    pt.pair_tx_count as tx_count,
    pt.pair_amount as amount_sum,
    pt.pair_amount_usd as amount_usd_sum,
    pt.first_seen_timestamp,
    pt.last_seen_timestamp,
    toUInt32(greatest(1, intDiv(pt.last_seen_timestamp - pt.first_seen_timestamp, 86400000))) as active_days,
    CASE WHEN pt.pair_tx_count > 0 THEN pt.pair_amount_usd / pt.pair_tx_count ELSE 0 END as avg_tx_size_usd,
    pt.unique_assets,
    pt.dominant_asset,
    pt.hourly_pattern,
    pt.weekly_pattern,
    CAST(r.reciprocity_ratio AS Float32) as reciprocity_ratio,
    r.is_bidirectional
FROM pair_totals pt
LEFT JOIN reciprocity r
    ON pt.year = r.year
    AND pt.week_number = r.week_number
    AND pt.from_address = r.from_address
    AND pt.to_address = r.to_address;