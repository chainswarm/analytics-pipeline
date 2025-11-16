-- =============================================================================
-- COMPUTATION AUDIT - Tracks successful analyzer processing runs
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_computation_audit (
    window_days UInt16,
    processing_date Date,
    created_at DateTime64(3),
    end_at DateTime64(3),
    duration_seconds UInt32,
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (window_days, processing_date, created_at)
SETTINGS index_granularity = 8192;

ALTER TABLE analyzers_computation_audit ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_computation_audit ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_computation_audit ADD INDEX IF NOT EXISTS idx_created_at created_at TYPE minmax GRANULARITY 4;