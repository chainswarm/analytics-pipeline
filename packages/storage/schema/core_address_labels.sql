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