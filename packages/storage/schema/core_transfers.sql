/* =========================
   SINGLE-CHAIN BALANCE TRANSFERS
   =========================
   Each row is an EDGE (from_address -> to_address) for a single transfer event.
   Keys are protocol-agnostic:
     - tx_id:       EVM tx hash / Substrate extrinsic hash / UTXO txid
     - event_index: EVM log_index / Substrate event_idx / per-tx edge index
     - edge_index:  optional disambiguator when one logical event yields multiple edges
   ========================= */

CREATE TABLE IF NOT EXISTS core_transfers
(
    tx_id String,                          -- 0x64 (EVM/Substrate common) or 64-hex (UTXO)
    event_index String DEFAULT '0',          -- log_index / event_idx / per-tx edge index
    edge_index  String DEFAULT '0',          -- extra disambiguator if you split flows (UTXO)

    block_height UInt32,
    block_timestamp UInt64,                -- ms since epoch

    from_address String,
    to_address   String,

    asset_symbol   String,
    asset_contract String DEFAULT 'native',-- 'native' or 0x40 for ERC20-like assets

    amount Decimal128(18),
    amount_usd Decimal128(18),   -- USD value at transaction time (MANDATORY - indexer must wait for price)
    fee    Decimal128(18),

    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(toDateTime(intDiv(block_timestamp, 1000)))
ORDER BY (block_height, tx_id, event_index, edge_index, asset_contract)
SETTINGS index_granularity = 8192;

/* Helpful indexes */
ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_tx_id            tx_id            TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_event_index      event_index      TYPE minmax GRANULARITY 4;
ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_edge_index       edge_index       TYPE minmax GRANULARITY 4;

ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_block_height     block_height     TYPE minmax GRANULARITY 4;
ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_block_timestamp  block_timestamp  TYPE minmax GRANULARITY 4;

ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_from_address     from_address     TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_to_address       to_address       TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_asset_symbol     asset_symbol     TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_asset_contract   asset_contract   TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE core_transfers ADD INDEX IF NOT EXISTS idx_version          _version         TYPE minmax GRANULARITY 4;


CREATE MATERIALIZED VIEW IF NOT EXISTS core_transfers_volume_series_mv_internal
ENGINE = SummingMergeTree((
    transaction_count,
    unique_senders,
    unique_receivers,
    total_volume,
    total_volume_usd,
    total_fees,
    unique_address_pairs,
    active_addresses,
    hour_0_tx_count,
    hour_1_tx_count,
    hour_2_tx_count,
    hour_3_tx_count,
    blocks_in_period,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    volume_lt_01,
    volume_01_to_1,
    volume_1_to_10,
    volume_10_to_100,
    volume_100_to_1k,
    volume_1k_to_10k,
    volume_gte_10k,
    volume_usd_lt_100,
    volume_usd_100_to_1k,
    volume_usd_1k_to_10k,
    volume_usd_10k_to_100k,
    volume_usd_100k_to_1m,
    volume_usd_gte_1m
))
PARTITION BY toYYYYMM(period_start)
ORDER BY (period_start, asset_symbol, asset_contract)
SETTINGS index_granularity = 8192
AS
SELECT
    toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400) as period_start,
    toDateTime((intDiv(intDiv(block_timestamp, 1000), 14400) + 1) * 14400) as period_end,
    asset_symbol,
    asset_contract,
    count() as transaction_count,
    uniqExact(from_address) as unique_senders,
    uniqExact(to_address) as unique_receivers,
    sum(amount) as total_volume,
    sum(amount_usd) as total_volume_usd,
    sum(fee) as total_fees,
    uniq(from_address, to_address) as unique_address_pairs,
    uniqExact(from_address) + uniqExact(to_address) as active_addresses,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400))) as hour_0_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400)) + 1) as hour_1_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400)) + 2) as hour_2_tx_count,
    countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) = toHour(toDateTime(intDiv(intDiv(block_timestamp, 1000), 14400) * 14400)) + 3) as hour_3_tx_count,
    max(block_height) - min(block_height) + 1 as blocks_in_period,
    countIf(amount < 0.1) as tx_count_lt_01,
    countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
    countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
    countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
    countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
    countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
    countIf(amount >= 10000) as tx_count_gte_10k,
    sumIf(amount, amount < 0.1) as volume_lt_01,
    sumIf(amount, amount >= 0.1 AND amount < 1) as volume_01_to_1,
    sumIf(amount, amount >= 1 AND amount < 10) as volume_1_to_10,
    sumIf(amount, amount >= 10 AND amount < 100) as volume_10_to_100,
    sumIf(amount, amount >= 100 AND amount < 1000) as volume_100_to_1k,
    sumIf(amount, amount >= 1000 AND amount < 10000) as volume_1k_to_10k,
    sumIf(amount, amount >= 10000) as volume_gte_10k,
    sumIf(amount_usd, amount_usd < 100) as volume_usd_lt_100,
    sumIf(amount_usd, amount_usd >= 100 AND amount_usd < 1000) as volume_usd_100_to_1k,
    sumIf(amount_usd, amount_usd >= 1000 AND amount_usd < 10000) as volume_usd_1k_to_10k,
    sumIf(amount_usd, amount_usd >= 10000 AND amount_usd < 100000) as volume_usd_10k_to_100k,
    sumIf(amount_usd, amount_usd >= 100000 AND amount_usd < 1000000) as volume_usd_100k_to_1m,
    sumIf(amount_usd, amount_usd >= 1000000) as volume_usd_gte_1m,
    argMax(block_height, block_timestamp) as latest_block_height,
    argMin(block_height, block_timestamp) as earliest_block_height,
    argMax(amount, block_timestamp) as max_transfer_amount,
    argMin(amount, block_timestamp) as min_transfer_amount
FROM core_transfers
GROUP BY period_start, period_end, asset_symbol, asset_contract;

CREATE VIEW IF NOT EXISTS core_transfers_volume_series_view AS
SELECT
    period_start,
    period_end,
    asset_symbol,
    asset_contract,
    transaction_count,
    unique_senders,
    unique_receivers,
    total_volume,
    total_volume_usd,
    total_fees,
    CASE WHEN transaction_count > 0 THEN total_volume / transaction_count ELSE 0 END as avg_transfer_amount,
    CASE WHEN transaction_count > 0 THEN total_volume_usd / transaction_count ELSE 0 END as avg_transfer_amount_usd,
    max_transfer_amount,
    min_transfer_amount,
    CASE WHEN transaction_count > 0 THEN total_fees / transaction_count ELSE 0 END as avg_fee,
    unique_address_pairs,
    active_addresses,
    CASE WHEN active_addresses > 1 THEN toFloat64(unique_address_pairs) / (toFloat64(active_addresses) * toFloat64(active_addresses - 1) / 2.0) ELSE 0.0 END as network_density,
    toHour(period_start) as period_hour,
    hour_0_tx_count,
    hour_1_tx_count,
    hour_2_tx_count,
    hour_3_tx_count,
    earliest_block_height as period_start_block,
    latest_block_height as period_end_block,
    blocks_in_period,
    tx_count_lt_01,
    tx_count_01_to_1,
    tx_count_1_to_10,
    tx_count_10_to_100,
    tx_count_100_to_1k,
    tx_count_1k_to_10k,
    tx_count_gte_10k,
    volume_lt_01,
    volume_01_to_1,
    volume_1_to_10,
    volume_10_to_100,
    volume_100_to_1k,
    volume_1k_to_10k,
    volume_gte_10k,
    volume_usd_lt_100,
    volume_usd_100_to_1k,
    volume_usd_1k_to_10k,
    volume_usd_10k_to_100k,
    volume_usd_100k_to_1m,
    volume_usd_gte_1m
FROM core_transfers_volume_series_mv_internal
ORDER BY period_start DESC, asset_symbol, asset_contract;

-- =============================================================================
-- MATERIALIZED VIEWS: Address-Level Time Series
-- Provide directional flow tracking with fixed time windows (daily, weekly, monthly)
-- =============================================================================

-- Daily Address Activity
CREATE MATERIALIZED VIEW IF NOT EXISTS core_transfers_address_daily_internal
ENGINE = SummingMergeTree((
    volume_in,
    volume_out,
    volume_in_usd,
    volume_out_usd,
    transaction_count_in,
    transaction_count_out,
    fees_paid
))
PARTITION BY toYYYYMM(date)
ORDER BY (address, asset_symbol, asset_contract, date)
SETTINGS index_granularity = 8192
AS
SELECT
    arrayJoin([from_address, to_address]) as address,
    asset_symbol,
    asset_contract,
    toDate(toDateTime(intDiv(block_timestamp, 1000))) as date,
    multiIf(address = to_address, amount, 0) as volume_in,
    multiIf(address = from_address, amount, 0) as volume_out,
    multiIf(address = to_address, amount_usd, 0) as volume_in_usd,
    multiIf(address = from_address, amount_usd, 0) as volume_out_usd,
    multiIf(address = to_address, 1, 0) as transaction_count_in,
    multiIf(address = from_address, 1, 0) as transaction_count_out,
    multiIf(address = from_address, fee, 0) as fees_paid
FROM core_transfers;

CREATE VIEW IF NOT EXISTS core_transfers_address_daily_view AS
SELECT
    address,
    asset_symbol,
    asset_contract,
    date,
    volume_in,
    volume_out,
    volume_in_usd,
    volume_out_usd,
    volume_in + volume_out as total_volume,
    volume_in_usd + volume_out_usd as total_volume_usd,
    volume_out - volume_in as net_volume,
    volume_out_usd - volume_in_usd as net_volume_usd,
    transaction_count_in,
    transaction_count_out,
    transaction_count_in + transaction_count_out as total_transactions,
    fees_paid,
    CASE WHEN transaction_count_out > 0 THEN fees_paid / transaction_count_out ELSE 0 END as avg_fee_per_tx
FROM core_transfers_address_daily_internal
ORDER BY address, asset_symbol, asset_contract, date DESC;

-- Weekly Address Activity
CREATE MATERIALIZED VIEW IF NOT EXISTS core_transfers_address_weekly_internal
ENGINE = SummingMergeTree((
    volume_in,
    volume_out,
    volume_in_usd,
    volume_out_usd,
    transaction_count_in,
    transaction_count_out,
    fees_paid
))
PARTITION BY toYYYYMM(week_start)
ORDER BY (address, asset_symbol, asset_contract, week_start)
SETTINGS index_granularity = 8192
AS
SELECT
    arrayJoin([from_address, to_address]) as address,
    asset_symbol,
    asset_contract,
    toStartOfWeek(toDateTime(intDiv(block_timestamp, 1000))) as week_start,
    multiIf(address = to_address, amount, 0) as volume_in,
    multiIf(address = from_address, amount, 0) as volume_out,
    multiIf(address = to_address, amount_usd, 0) as volume_in_usd,
    multiIf(address = from_address, amount_usd, 0) as volume_out_usd,
    multiIf(address = to_address, 1, 0) as transaction_count_in,
    multiIf(address = from_address, 1, 0) as transaction_count_out,
    multiIf(address = from_address, fee, 0) as fees_paid
FROM core_transfers;

CREATE VIEW IF NOT EXISTS core_transfers_address_weekly_view AS
SELECT
    address,
    asset_symbol,
    asset_contract,
    week_start,
    volume_in,
    volume_out,
    volume_in_usd,
    volume_out_usd,
    volume_in + volume_out as total_volume,
    volume_in_usd + volume_out_usd as total_volume_usd,
    volume_out - volume_in as net_volume,
    volume_out_usd - volume_in_usd as net_volume_usd,
    transaction_count_in,
    transaction_count_out,
    transaction_count_in + transaction_count_out as total_transactions,
    fees_paid,
    CASE WHEN transaction_count_out > 0 THEN fees_paid / transaction_count_out ELSE 0 END as avg_fee_per_tx
FROM core_transfers_address_weekly_internal
ORDER BY address, asset_symbol, asset_contract, week_start DESC;

-- Monthly Address Activity
CREATE MATERIALIZED VIEW IF NOT EXISTS core_transfers_address_monthly_internal
ENGINE = SummingMergeTree((
    volume_in,
    volume_out,
    volume_in_usd,
    volume_out_usd,
    transaction_count_in,
    transaction_count_out,
    fees_paid
))
PARTITION BY toYYYYMM(month_start)
ORDER BY (address, asset_symbol, asset_contract, month_start)
SETTINGS index_granularity = 8192
AS
SELECT
    arrayJoin([from_address, to_address]) as address,
    asset_symbol,
    asset_contract,
    toStartOfMonth(toDateTime(intDiv(block_timestamp, 1000))) as month_start,
    multiIf(address = to_address, amount, 0) as volume_in,
    multiIf(address = from_address, amount, 0) as volume_out,
    multiIf(address = to_address, amount_usd, 0) as volume_in_usd,
    multiIf(address = from_address, amount_usd, 0) as volume_out_usd,
    multiIf(address = to_address, 1, 0) as transaction_count_in,
    multiIf(address = from_address, 1, 0) as transaction_count_out,
    multiIf(address = from_address, fee, 0) as fees_paid
FROM core_transfers;

CREATE VIEW IF NOT EXISTS core_transfers_address_monthly_view AS
SELECT
    address,
    asset_symbol,
    asset_contract,
    month_start,
    volume_in,
    volume_out,
    volume_in_usd,
    volume_out_usd,
    volume_in + volume_out as total_volume,
    volume_in_usd + volume_out_usd as total_volume_usd,
    volume_out - volume_in as net_volume,
    volume_out_usd - volume_in_usd as net_volume_usd,
    transaction_count_in,
    transaction_count_out,
    transaction_count_in + transaction_count_out as total_transactions,
    fees_paid,
    CASE WHEN transaction_count_out > 0 THEN fees_paid / transaction_count_out ELSE 0 END as avg_fee_per_tx
FROM core_transfers_address_monthly_internal
ORDER BY address, asset_symbol, asset_contract, month_start DESC;

-- =============================================================================
-- VIEW: Address Analytics with Asset Verification
-- Provides comprehensive address analytics with asset verification status
-- =============================================================================

CREATE VIEW IF NOT EXISTS core_transfers_address_analytics_view AS
WITH
outgoing_metrics AS (
    SELECT
        from_address as address,
        asset_symbol,
        asset_contract,
        count() as outgoing_count,
        sum(amount) as total_sent,
        sum(fee) as total_fees_paid,
        uniq(to_address) as unique_recipients,
        min(block_timestamp) as first_activity,
        max(block_timestamp) as last_activity,
        uniq(toDate(toDateTime(intDiv(block_timestamp, 1000)))) as active_days,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 0 AND 5) as night_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 6 AND 11) as morning_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 12 AND 17) as afternoon_transactions,
        countIf(toHour(toDateTime(intDiv(block_timestamp, 1000))) BETWEEN 18 AND 23) as evening_transactions,
        countIf(amount < 0.1) as tx_count_lt_01,
        countIf(amount >= 0.1 AND amount < 1) as tx_count_01_to_1,
        countIf(amount >= 1 AND amount < 10) as tx_count_1_to_10,
        countIf(amount >= 10 AND amount < 100) as tx_count_10_to_100,
        countIf(amount >= 100 AND amount < 1000) as tx_count_100_to_1k,
        countIf(amount >= 1000 AND amount < 10000) as tx_count_1k_to_10k,
        countIf(amount >= 10000) as tx_count_gte_10k,
        varPop(amount) as sent_amount_variance
    FROM core_transfers
    GROUP BY from_address, asset_symbol, asset_contract
),
incoming_metrics AS (
    SELECT
        to_address as address,
        asset_symbol,
        asset_contract,
        count() as incoming_count,
        sum(amount) as total_received,
        uniq(from_address) as unique_senders,
        varPop(amount) as received_amount_variance
    FROM core_transfers
    GROUP BY to_address, asset_symbol, asset_contract
),
asset_info AS (
    SELECT DISTINCT
        asset_contract,
        any(verified) as asset_verified,
        any(asset_symbol) as asset_name
    FROM core_assets
    GROUP BY asset_contract
),
combined_addresses AS (
    SELECT DISTINCT
        address,
        asset_symbol,
        asset_contract
    FROM (
        SELECT address, asset_symbol, asset_contract FROM outgoing_metrics
        UNION ALL
        SELECT address, asset_symbol, asset_contract FROM incoming_metrics
    )
)
SELECT
    ca.address AS address,
    ca.asset_symbol AS asset_symbol,
    ca.asset_contract AS asset_contract,
    asset_verified,
    asset_name,
    (COALESCE(outgoing_count, 0) + COALESCE(incoming_count, 0)) as total_transactions,
    COALESCE(outgoing_count, 0) as outgoing_count,
    COALESCE(incoming_count, 0) as incoming_count,
    COALESCE(total_sent, 0) as total_sent,
    COALESCE(total_received, 0) as total_received,
    COALESCE(total_sent, 0) + COALESCE(total_received, 0) as total_volume,
    COALESCE(unique_recipients, 0) as unique_recipients,
    COALESCE(unique_senders, 0) as unique_senders,
    COALESCE(first_activity, 0) as first_activity,
    COALESCE(last_activity, 0) as last_activity,
    COALESCE(last_activity, 0) - COALESCE(first_activity, 0) as activity_span_seconds,
    COALESCE(total_fees_paid, 0) as total_fees_paid,
    CASE WHEN COALESCE(outgoing_count, 0) > 0 THEN COALESCE(total_fees_paid, 0) / COALESCE(outgoing_count, 0) ELSE 0 END as avg_fee_paid,
    COALESCE(night_transactions, 0) as night_transactions,
    COALESCE(morning_transactions, 0) as morning_transactions,
    COALESCE(afternoon_transactions, 0) as afternoon_transactions,
    COALESCE(evening_transactions, 0) as evening_transactions,
    COALESCE(tx_count_lt_01, 0) as tx_count_lt_01,
    COALESCE(tx_count_01_to_1, 0) as tx_count_01_to_1,
    COALESCE(tx_count_1_to_10, 0) as tx_count_1_to_10,
    COALESCE(tx_count_10_to_100, 0) as tx_count_10_to_100,
    COALESCE(tx_count_100_to_1k, 0) as tx_count_100_to_1k,
    COALESCE(tx_count_1k_to_10k, 0) as tx_count_1k_to_10k,
    COALESCE(tx_count_gte_10k, 0) as tx_count_gte_10k,
    COALESCE(sent_amount_variance, 0) as sent_amount_variance,
    COALESCE(received_amount_variance, 0) as received_amount_variance,
    COALESCE(active_days, 0) as active_days
FROM combined_addresses ca
LEFT JOIN outgoing_metrics o ON ca.address = o.address AND ca.asset_symbol = o.asset_symbol AND ca.asset_contract = o.asset_contract
LEFT JOIN incoming_metrics i ON ca.address = i.address AND ca.asset_symbol = i.asset_symbol AND ca.asset_contract = i.asset_contract
LEFT JOIN asset_info a ON ca.asset_contract = a.asset_contract
WHERE (COALESCE(o.outgoing_count, 0) + COALESCE(i.incoming_count, 0)) > 0;

-- =============================================================================
-- VIEW: Volume Trends with Rolling Averages
-- Provides rolling average calculations for trend analysis
-- =============================================================================

CREATE VIEW IF NOT EXISTS core_transfers_volume_trends_view AS
SELECT
    period_start,
    asset_symbol,
    asset_contract,
    total_volume,
    transaction_count,
    avg(total_volume) OVER (
        PARTITION BY asset_symbol, asset_contract
        ORDER BY period_start
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7_period_avg_volume,
    avg(transaction_count) OVER (
        PARTITION BY asset_symbol, asset_contract
        ORDER BY period_start
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7_period_avg_tx_count,
    avg(total_volume) OVER (
        PARTITION BY asset_symbol, asset_contract
        ORDER BY period_start
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_30_period_avg_volume
FROM core_transfers_volume_series_mv_internal
ORDER BY period_start DESC, asset_symbol, asset_contract;