-- =============================================================================
-- analyzers_patterns_burst: Temporal burst pattern detections
-- =============================================================================
-- Stores temporal burst patterns for detecting sudden spikes in activity
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_patterns_burst (
    -- Time series dimensions (ESSENTIAL for A/B testing)
    window_days UInt16,
    processing_date Date,
    
    -- Stable pattern identifiers
    pattern_id String,
    pattern_type String DEFAULT 'temporal_burst',
    pattern_hash String,
    
    -- Single record for all involved addresses (NO DUPLICATION)
    addresses_involved Array(String),
    address_roles Array(String),
    
    -- Burst-specific data
    burst_address String,
    burst_start_timestamp UInt64,
    burst_end_timestamp UInt64,
    burst_duration_seconds UInt32,
    burst_transaction_count UInt32,
    burst_volume_usd Decimal128(18),
    
    -- Statistical analysis
    normal_tx_rate Float64,
    burst_tx_rate Float64,
    burst_intensity Float64,
    z_score Float64,
    
    -- Temporal distribution
    hourly_distribution Array(UInt32),
    peak_hours Array(UInt8),
    
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
ALTER TABLE analyzers_patterns_burst ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_patterns_burst ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_patterns_burst ADD INDEX IF NOT EXISTS idx_pattern_id pattern_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_patterns_burst ADD INDEX IF NOT EXISTS idx_burst_address burst_address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_patterns_burst ADD INDEX IF NOT EXISTS idx_z_score z_score TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_patterns_burst ADD INDEX IF NOT EXISTS idx_burst_intensity burst_intensity TYPE minmax GRANULARITY 4;