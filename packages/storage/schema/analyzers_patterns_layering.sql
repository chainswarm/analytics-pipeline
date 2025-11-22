-- =============================================================================
-- analyzers_patterns_layering: Layering path pattern detections
-- =============================================================================
-- Stores layering path patterns separately for optimized storage and queries
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_patterns_layering (
    -- Time series dimensions (ESSENTIAL for A/B testing)
    window_days UInt16,
    processing_date Date,
    
    -- Stable pattern identifiers
    pattern_id String,
    pattern_type String DEFAULT 'layering_path',
    pattern_hash String,
    
    -- Single record for all involved addresses (NO DUPLICATION)
    addresses_involved Array(String),
    address_roles Array(String),
    
    -- Layering-specific data
    layering_path Array(String),
    path_depth UInt32,
    path_volume_usd Decimal128(18),
    source_address String,
    destination_address String,
    
    -- Temporal information
    detection_timestamp UInt64,
    pattern_start_time UInt64,
    pattern_end_time UInt64,
    pattern_duration_hours UInt32,
    
    -- Evidence
    evidence_transaction_count UInt32,
    evidence_volume_usd Decimal128(18),
    detection_method String,
    
    -- Administrative
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (window_days, processing_date, pattern_id)
SETTINGS index_granularity = 8192;

-- Indexes for pattern queries
ALTER TABLE analyzers_patterns_layering ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_patterns_layering ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_patterns_layering ADD INDEX IF NOT EXISTS idx_pattern_id pattern_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_patterns_layering ADD INDEX IF NOT EXISTS idx_path_depth path_depth TYPE minmax GRANULARITY 4;