-- =============================================================================
-- analyzers_pattern_detections: Pure snapshot pattern storage
-- =============================================================================
-- Time-series snapshots: One deduplicated pattern per (window_days, processing_date)
-- Stable pattern_id via hash enables cross-window tracking when needed
-- No evolution tracking - compute on-demand from snapshots if required
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_pattern_detections (
    -- Time series dimensions (ESSENTIAL for A/B testing)
    window_days UInt16,
    processing_date Date,
    
    -- Stable pattern identifiers
    pattern_id String,
    pattern_type String,
    pattern_hash String,
    
    -- Single record for all involved addresses (NO DUPLICATION)
    addresses_involved Array(String),
    address_roles Array(String),
    
    -- Pattern metrics
    severity_score Float32,
    confidence_score Float32,
    risk_score Float32,
    
    -- Pattern-specific data (deduplicated)
    cycle_path Array(String) DEFAULT [],
    cycle_length UInt32 DEFAULT 0,
    cycle_volume_usd Decimal128(18) DEFAULT 0,
    
    layering_path Array(String) DEFAULT [],
    path_depth UInt32 DEFAULT 0,
    path_volume_usd Decimal128(18) DEFAULT 0,
    source_address String DEFAULT '',
    destination_address String DEFAULT '',
    
    network_members Array(String) DEFAULT [],
    network_size UInt32 DEFAULT 0,
    network_density Float32 DEFAULT 0,
    hub_addresses Array(String) DEFAULT [],
    
    risk_source_address String DEFAULT '',
    distance_to_risk UInt32 DEFAULT 0,
    risk_propagation_score Float32 DEFAULT 0,
    
    motif_type String DEFAULT '',
    motif_center_address String DEFAULT '',
    motif_participant_count UInt32 DEFAULT 0,
    
    -- Temporal information
    detection_timestamp UInt64,
    pattern_start_time UInt64,
    pattern_end_time UInt64,
    pattern_duration_hours UInt32,
    
    -- Evidence
    evidence_transaction_count UInt32,
    evidence_volume_usd Decimal128(18),
    detection_method String,
    
    -- Quality metrics
    anomaly_score Float32,
    
    -- Administrative
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (window_days, processing_date, pattern_type, pattern_id)
SETTINGS index_granularity = 8192;

-- Indexes for pattern queries
ALTER TABLE analyzers_pattern_detections ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_pattern_detections ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_pattern_detections ADD INDEX IF NOT EXISTS idx_pattern_type pattern_type TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_pattern_detections ADD INDEX IF NOT EXISTS idx_pattern_id pattern_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_pattern_detections ADD INDEX IF NOT EXISTS idx_severity_score severity_score TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_pattern_detections ADD INDEX IF NOT EXISTS idx_risk_score risk_score TYPE minmax GRANULARITY 4;

CREATE MATERIALIZED VIEW IF NOT EXISTS analyzers_pattern_address_mv
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (window_days, processing_date, address, pattern_type, pattern_id)
SETTINGS index_granularity = 8192
AS SELECT
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    arrayJoin(addresses_involved) as address,
    arrayElement(
        address_roles,
        arrayFirstIndex(x -> x = arrayJoin(addresses_involved), addresses_involved)
    ) as role,
    severity_score,
    confidence_score,
    risk_score,
    _version
FROM analyzers_pattern_detections;