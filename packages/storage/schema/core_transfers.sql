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