-- =============================================================================
-- analyzers_patterns_threshold: Threshold evasion pattern detections
-- =============================================================================
-- Stores threshold evasion patterns for detecting structured avoidance of limits
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_patterns_threshold (
    -- Time series dimensions (ESSENTIAL for A/B testing)
    window_days UInt16,
    processing_date Date,
    
    -- Stable pattern identifiers
    pattern_id String,
    pattern_type String DEFAULT 'threshold_evasion',
    pattern_hash String,
    
    -- Single record for all involved addresses (NO DUPLICATION)
    addresses_involved Array(String),
    address_roles Array(String),
    
    -- Threshold-specific data
    primary_address String,
    threshold_value Decimal128(18),
    threshold_type String,
    
    -- Clustering analysis
    transactions_near_threshold UInt32,
    avg_transaction_size Decimal128(18),
    max_transaction_size Decimal128(18),
    size_consistency Float64,
    clustering_score Float64,
    
    -- Temporal analysis
    unique_days UInt32,
    avg_daily_transactions Float64,
    temporal_spread_score Float64,
    
    -- Overall analysis
    threshold_avoidance_score Float64,
    
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
ALTER TABLE analyzers_patterns_threshold ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_patterns_threshold ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_patterns_threshold ADD INDEX IF NOT EXISTS idx_pattern_id pattern_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_patterns_threshold ADD INDEX IF NOT EXISTS idx_primary_address primary_address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_patterns_threshold ADD INDEX IF NOT EXISTS idx_threshold_avoidance_score threshold_avoidance_score TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_patterns_threshold ADD INDEX IF NOT EXISTS idx_clustering_score clustering_score TYPE minmax GRANULARITY 4;