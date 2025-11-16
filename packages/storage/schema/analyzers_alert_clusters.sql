/* =========================
   ALERT CLUSTERS
   =========================
   Groups related alerts for deduplication and investigation efficiency.
   Clusters can be based on same entity, pattern, network, or time proximity.
   ========================= */

CREATE OR REPLACE TABLE analyzers_alert_clusters (
    -- Time series dimensions
    window_days UInt16,
    processing_date Date,
    
    cluster_id String,
    cluster_type String,
    
    -- For same_entity clusters
    primary_address String DEFAULT '',
    
    -- For pattern clusters
    pattern_id String DEFAULT '',
    
    -- Cluster details
    primary_alert_id String,             -- Representative alert for cluster
    related_alert_ids Array(String),     -- All alerts in cluster
    addresses_involved Array(String),    -- All addresses in cluster
    
    -- Cluster metrics
    total_alerts UInt32,                 -- Number of alerts in cluster
    total_volume_usd Decimal128(18),    -- Total USD volume involved
    severity_max String DEFAULT 'medium',  -- Highest severity: low, medium, high, critical
    confidence_avg Float32,              -- Average confidence score
    
    -- Time range
    earliest_alert_timestamp UInt64,     -- First alert in cluster
    latest_alert_timestamp UInt64,       -- Most recent alert in cluster
    
    -- Metadata
    _version UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY (cluster_type, toYYYYMM(processing_date))
ORDER BY (window_days, processing_date, cluster_type, cluster_id)
SETTINGS index_granularity = 8192;

-- Indexes for cluster management
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_cluster_type cluster_type TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_severity_max severity_max TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_primary_alert_id primary_alert_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_cluster_id cluster_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_primary_address primary_address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_alert_clusters ADD INDEX IF NOT EXISTS idx_pattern_id pattern_id TYPE bloom_filter(0.01) GRANULARITY 4;