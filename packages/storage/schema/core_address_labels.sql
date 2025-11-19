-- =============================================================================
-- ADDRESS LABELS SCHEMA - Known Address Labels with Simple Strings
-- =============================================================================
-- This table stores labels for known addresses using simple string fields
-- for clean, maintainable classification.
-- =============================================================================

CREATE TABLE IF NOT EXISTS core_address_labels (
    -- Primary identifiers
    network String,
    network_type String DEFAULT 'substrate',
    address String,
    
    -- Label details with simple string fields
    label String,
    address_type String DEFAULT 'unknown',
    address_subtype String DEFAULT '',
    trust_level String DEFAULT 'unverified',
    source String,
    
    -- Simple derived fields
    risk_level String DEFAULT 'medium',
    confidence_score Float32 DEFAULT 0.5,
    
    -- Timestamps
    created_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    updated_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY network
ORDER BY (network, address, label)
SETTINGS index_granularity = 8192;

-- Performance indexes for string-based queries
ALTER TABLE core_address_labels ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_address_labels ADD INDEX IF NOT EXISTS idx_network network TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_address_labels ADD INDEX IF NOT EXISTS idx_address_type address_type TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_address_labels ADD INDEX IF NOT EXISTS idx_address_subtype address_subtype TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_address_labels ADD INDEX IF NOT EXISTS idx_trust_level trust_level TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE core_address_labels ADD INDEX IF NOT EXISTS idx_risk_level risk_level TYPE bloom_filter(0.01) GRANULARITY 4;

-- =============================================================================
-- ADDRESS LABELS VIEW - Aggregated view for ML training and risk scoring
-- =============================================================================
-- This VIEW returns ONE label per address with highest confidence score
-- Use this VIEW in ML training and risk scoring, NOT the base table

CREATE VIEW IF NOT EXISTS core_address_labels_view AS
SELECT
    network,
    address,
    network_type,
    label,
    address_type,
    address_subtype,
    trust_level,
    source,
    risk_level,
    confidence_score,
    created_timestamp,
    updated_timestamp
FROM (
    SELECT
        network,
        address,
        network_type,
        label,
        address_type,
        address_subtype,
        trust_level,
        source,
        risk_level,
        confidence_score,
        created_timestamp,
        updated_timestamp,
        row_number() OVER (PARTITION BY network, address ORDER BY confidence_score DESC, _version DESC) AS rn
    FROM core_address_labels
) WHERE rn = 1;

-- =============================================================================
-- LABEL IMPORTER STATE TABLE - Tracks importer progress
-- =============================================================================
-- This table stores state for address label importers

CREATE TABLE IF NOT EXISTS core_label_importer_state (
    network String,
    importer_name String,
    importer_type String,
    
    state_data String,
    
    last_run_timestamp UInt64,
    status String,
    error_message String DEFAULT '',
    
    created_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    updated_timestamp UInt64 DEFAULT toUnixTimestamp64Milli(now64()),
    
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY network
ORDER BY (network, importer_name)
SETTINGS index_granularity = 8192;