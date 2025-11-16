-- =============================================================================
-- ALERTS SCHEMA - AML Typology Detection Alerts
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_alerts (
    -- Time series dimensions
    window_days UInt16,
    processing_date Date,
    
    -- Primary identifiers
    alert_id String,
    address String,
    
    -- Alert classification
    typology_type String,
    
    -- Pattern reference (if pattern-based alert)
    pattern_id String DEFAULT '',
    pattern_type String DEFAULT '',
    
    -- Alert details with simple string fields
    severity String DEFAULT 'medium',
    suspected_address_type String DEFAULT 'unknown',
    suspected_address_subtype String DEFAULT '',
    alert_confidence_score Float32,
    description String,
    volume_usd Decimal128(18) DEFAULT 0,
    
    -- Evidence and context
    evidence_json String,                   -- Detailed evidence (JSON)
    risk_indicators Array(String),         -- Risk indicators triggered

    -- Version tracking
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (window_days, processing_date, alert_id, typology_type)
SETTINGS index_granularity = 8192;

-- Performance indexes for string-based queries
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_severity severity TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_typology_type typology_type TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_suspected_address_type suspected_address_type TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_suspected_address_subtype suspected_address_subtype TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_alert_id alert_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_pattern_id pattern_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alerts ADD INDEX IF NOT EXISTS idx_pattern_type pattern_type TYPE bloom_filter(0.01) GRANULARITY 4;