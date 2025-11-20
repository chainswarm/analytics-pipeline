DROP TABLE IF EXISTS core_money_flows_mv;
DROP VIEW IF EXISTS core_money_flows_view;

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