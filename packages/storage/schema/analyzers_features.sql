-- Phase 3A Feature Tables - ML Feature Engineering Infrastructure
-- Generated for Phase 3A: Foundation Infrastructure for ML Feature Builder

-- =============================================================================
-- analyzers_features: Comprehensive ML features for graph analysis
-- =============================================================================

CREATE OR REPLACE TABLE analyzers_features (
    -- Time series dimensions
    window_days UInt16,
    processing_date Date,

    -- Primary identifier
    address String,

    -- Node topology features (from address panel data)
    degree_in UInt32,                   -- Number of unique senders
    degree_out UInt32,                  -- Number of unique receivers
    degree_total UInt32,                -- Total degree (in + out)
    unique_counterparties UInt32,       -- Distinct addresses interacted with

    -- Volume features (USD normalized)
    total_in_usd Decimal128(18),        -- Total incoming value
    total_out_usd Decimal128(18),       -- Total outgoing value
    net_flow_usd Decimal128(18),        -- Net flow (in - out)
    total_volume_usd Decimal128(18),    -- Total volume (in + out)
    avg_tx_in_usd Decimal128(18),       -- Average incoming transaction size
    avg_tx_out_usd Decimal128(18),      -- Average outgoing transaction size
    median_tx_in_usd Decimal128(18),    -- Median incoming transaction size
    median_tx_out_usd Decimal128(18),   -- Median outgoing transaction size
    max_tx_usd Decimal128(18),          -- Maximum single transaction value
    min_tx_usd Decimal128(18),          -- Minimum single transaction value

    -- Statistical distribution features
    amount_variance Float64,            -- Variance of transaction amounts
    amount_skewness Float64,            -- Skewness of transaction amounts
    amount_kurtosis Float64,            -- Kurtosis of transaction amounts
    volume_std Float64,                 -- Standard deviation of volumes
    volume_cv Float64,                  -- Coefficient of variation
    flow_concentration Float64,         -- Gini coefficient of flow distribution

    -- Transaction count features
    tx_in_count UInt64,                 -- Total incoming transactions
    tx_out_count UInt64,                -- Total outgoing transactions
    tx_total_count UInt64,              -- Total transaction count

    -- Temporal features
    activity_days UInt32,               -- Days with activity in window
    activity_span_days UInt32,          -- Days between first and last activity
    avg_daily_volume_usd Decimal128(18), -- Average daily volume
    peak_hour UInt8,                    -- Most active hour (0-23)
    peak_day UInt8,                     -- Most active day (0-6, Mon=0)
    regularity_score Float32,           -- Activity regularity score (0-1)
    burst_factor Float32,               -- Ratio of peak to average activity

    -- Flow characteristics (enhanced with ADDRESS_PANEL data)
    reciprocity_ratio Float32,          -- Bidirectional flow ratio (0-1)
    flow_diversity Float32,             -- Shannon entropy of flow distribution
    counterparty_concentration Float32, -- Concentration of counterparty interactions
    concentration_ratio Float32,        -- Gini coefficient of counterparty distribution
    velocity_score Float32,             -- Transaction frequency score (0-1)
    structuring_score Float32,          -- Small transaction clustering score (0-1)

    -- Asset diversity features (enhanced with ADDRESS_PANEL data)
    unique_assets_in UInt32,            -- Different assets received
    unique_assets_out UInt32,           -- Different assets sent
    dominant_asset_in String,           -- Most received asset (by USD)
    dominant_asset_out String,          -- Most sent asset (by USD)
    asset_diversity_score Float32,      -- Asset interaction diversity

    -- Behavioral pattern features (enhanced with ADDRESS_PANEL data)
    hourly_activity Array(UInt16),      -- 24-hour activity histogram [0-23]
    daily_activity Array(UInt16),       -- 7-day activity histogram [0-6]
    peak_activity_hour UInt8,           -- Most active hour (0-23)
    peak_activity_day UInt8,            -- Most active day (0-6, Mon=0)
    hourly_entropy Float32,             -- Entropy of hourly activity pattern
    daily_entropy Float32,              -- Entropy of daily activity pattern
    weekend_transaction_ratio Float32,  -- Weekend vs weekday activity ratio
    night_transaction_ratio Float32,    -- Night vs day activity ratio
    small_transaction_ratio Float32,    -- Small transaction clustering ratio
    consistency_score Float32,          -- Transaction timing consistency

    -- Graph algorithm features (placeholders for Phase 4)
    pagerank Float32,                   -- PageRank centrality score
    betweenness Float32,                -- Betweenness centrality
    closeness Float32,                  -- Closeness centrality
    clustering_coefficient Float32,     -- Local clustering coefficient
    kcore UInt32,                       -- K-core decomposition result
    community_id UInt32,                -- Community detection result
    centrality_score Float32,           -- Combined centrality measure

    -- k-hop neighborhood features
    khop1_count UInt32,                 -- 1-hop neighbors count
    khop2_count UInt32,                 -- 2-hop neighbors count
    khop3_count UInt32,                 -- 3-hop neighbors count
    khop1_volume_usd Decimal128(18),    -- 1-hop neighborhood volume
    khop2_volume_usd Decimal128(18),    -- 2-hop neighborhood volume
    khop3_volume_usd Decimal128(18),    -- 3-hop neighborhood volume

    -- Advanced flow features
    flow_reciprocity_entropy Float32,   -- Entropy of reciprocal flows
    counterparty_stability Float32,     -- Stability of counterparty relationships
    flow_burstiness Float32,            -- Temporal burstiness of flows
    transaction_regularity Float32,     -- Regularity of transaction timing
    amount_predictability Float32,      -- Predictability of transaction amounts

    -- Temporal classification features (observations only, not ML-derived)
    is_new_address Boolean,             -- First seen in current window
    is_dormant_reactivated Boolean,     -- Previously inactive, now active


    -- Supporting metrics for classification
    unique_recipients_count UInt32,     -- Number of unique addresses received funds from
    unique_senders_count UInt32,        -- Number of unique addresses sent funds to

    -- Feature quality and metadata
    completeness_score Float32,         -- Feature completeness score (0-1)
    quality_score Float32,              -- Overall feature quality score (0-1)
    outlier_score Float32,              -- Statistical outlier score (0-1)
    confidence_score Float32,           -- Confidence in feature calculations

    -- Temporal metadata
    first_activity_timestamp UInt64,    -- First transaction in window
    last_activity_timestamp UInt64,     -- Last transaction in window

    _version UInt64                     -- For ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY (window_days, toYYYYMM(processing_date))
ORDER BY (window_days, processing_date, address)
SETTINGS index_granularity = 8192;

-- Indexes for ML feature queries
ALTER TABLE analyzers_features ADD INDEX IF NOT EXISTS idx_processing_date processing_date TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_features ADD INDEX IF NOT EXISTS idx_window_days window_days TYPE set(0) GRANULARITY 4;
ALTER TABLE analyzers_features ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analyzers_features ADD INDEX IF NOT EXISTS idx_total_volume_usd total_volume_usd TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_features ADD INDEX IF NOT EXISTS idx_degree_total degree_total TYPE minmax GRANULARITY 4;
ALTER TABLE analyzers_features ADD INDEX IF NOT EXISTS idx_quality_score quality_score TYPE minmax GRANULARITY 4;