-- =============================================================================
-- analyzers_patterns_proximity: Proximity risk pattern detections
-- =============================================================================
-- Stores proximity risk patterns separately for optimized storage and queries
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_patterns_proximity (
    -- Time series dimensions (ESSENTIAL for A/B testing)
    window_days UInt16,
    processing_date Date,
    
    -- Stable pattern identifiers
    pattern_id String,
    pattern_type String DEFAULT 'proximity_risk',
    pattern_hash String,
    
    -- Single record for all involved addresses (NO DUPLICATION)
    addresses_involved Array(String),
    address_roles Array(String),
    
    -- Proximity-specific data
    risk_source_address String,
    distance_to_risk UInt32,
    
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
ALTER TABLE analyzers_patterns_proximity ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_patterns_proximity ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_patterns_proximity ADD INDEX IF NOT EXISTS idx_pattern_id pattern_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_patterns_proximity ADD INDEX IF NOT EXISTS idx_risk_source_address risk_source_address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_patterns_proximity ADD INDEX IF NOT EXISTS idx_distance_to_risk distance_to_risk TYPE minmax GRANULARITY 4;