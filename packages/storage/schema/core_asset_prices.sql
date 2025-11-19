/* =========================
   ASSET PRICES (per asset_contract)
   ========================= */

CREATE TABLE IF NOT EXISTS core_asset_prices (
    asset_symbol String,
    asset_contract String DEFAULT 'native',
    price_date Date32,                -- one row per date
    price_usd Decimal128(18),
    source String,
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY price_date
ORDER BY (asset_symbol, asset_contract, source, price_date)
SETTINGS index_granularity = 8192;

ALTER TABLE core_asset_prices ADD INDEX IF NOT EXISTS idx_asset_contract asset_contract TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_asset_prices ADD INDEX IF NOT EXISTS idx_asset_symbol   asset_symbol   TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_asset_prices ADD INDEX IF NOT EXISTS idx_price_date     price_date     TYPE minmax GRANULARITY 4;