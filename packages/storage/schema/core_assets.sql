
CREATE TABLE IF NOT EXISTS core_assets (
    asset_symbol String,
    asset_contract String DEFAULT 'native',
    network String,                    -- 'torus', 'bittensor', 'polkadot', 'ethereum', etc.
    verified Boolean DEFAULT false,    -- verification status
    verification_source String DEFAULT '', -- 'substrate_native', 'etherscan', 'manual'
    first_seen_timestamp UInt64,       -- when first discovered in transfers
    last_scanned_block_height UInt64 DEFAULT 0, -- asset pricing indexer progress tracking
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY network
ORDER BY (network, asset_contract, asset_symbol)
SETTINGS index_granularity = 8192;

/* Asset table indexes */
ALTER TABLE core_assets ADD INDEX IF NOT EXISTS idx_asset_contract asset_contract TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_assets ADD INDEX IF NOT EXISTS idx_asset_symbol   asset_symbol   TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_assets ADD INDEX IF NOT EXISTS idx_network        network        TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_assets ADD INDEX IF NOT EXISTS idx_verified       verified       TYPE minmax GRANULARITY 4;
ALTER TABLE core_assets ADD INDEX IF NOT EXISTS idx_first_seen     first_seen_timestamp TYPE minmax GRANULARITY 4;