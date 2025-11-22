-- =============================================================================
-- analyzers_pattern_detections: Backward-compatible view
-- =============================================================================
-- UNION ALL view combining all specialized pattern tables
-- Maintains compatibility with existing queries while providing optimized storage
-- =============================================================================

CREATE OR REPLACE VIEW analyzers_pattern_detections AS

-- Cycle patterns
SELECT
    -- Common fields
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    pattern_hash,
    addresses_involved,
    address_roles,
    
    -- Cycle fields (real)
    cycle_path,
    cycle_length,
    cycle_volume_usd,
    
    -- Layering fields (NULL)
    [] AS layering_path,
    0 AS path_depth,
    0 AS path_volume_usd,
    '' AS source_address,
    '' AS destination_address,
    
    -- Network fields (NULL)
    [] AS network_members,
    0 AS network_size,
    0.0 AS network_density,
    [] AS hub_addresses,
    
    -- Proximity fields (NULL)
    '' AS risk_source_address,
    0 AS distance_to_risk,
    
    -- Motif fields (NULL)
    '' AS motif_type,
    '' AS motif_center_address,
    0 AS motif_participant_count,
    
    -- Burst fields (NULL)
    '' AS burst_address,
    0 AS burst_start_timestamp,
    0 AS burst_end_timestamp,
    0 AS burst_duration_seconds,
    0 AS burst_transaction_count,
    0 AS burst_volume_usd,
    0.0 AS normal_tx_rate,
    0.0 AS burst_tx_rate,
    0.0 AS burst_intensity,
    0.0 AS z_score,
    [] AS hourly_distribution,
    [] AS peak_hours,
    
    -- Threshold fields (NULL)
    '' AS primary_address,
    0 AS threshold_value,
    '' AS threshold_type,
    0 AS transactions_near_threshold,
    0 AS avg_transaction_size,
    0 AS max_transaction_size,
    0.0 AS size_consistency,
    0.0 AS clustering_score,
    0 AS unique_days,
    0.0 AS avg_daily_transactions,
    0.0 AS temporal_spread_score,
    0.0 AS threshold_avoidance_score,
    
    -- Common temporal/evidence fields
    detection_timestamp,
    pattern_start_time,
    pattern_end_time,
    pattern_duration_hours,
    evidence_transaction_count,
    evidence_volume_usd,
    detection_method,
    _version
FROM analyzers_patterns_cycle

UNION ALL

-- Layering patterns
SELECT
    -- Common fields
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    pattern_hash,
    addresses_involved,
    address_roles,
    
    -- Cycle fields (NULL)
    [] AS cycle_path,
    0 AS cycle_length,
    0 AS cycle_volume_usd,
    
    -- Layering fields (real)
    layering_path,
    path_depth,
    path_volume_usd,
    source_address,
    destination_address,
    
    -- Network fields (NULL)
    [] AS network_members,
    0 AS network_size,
    0.0 AS network_density,
    [] AS hub_addresses,
    
    -- Proximity fields (NULL)
    '' AS risk_source_address,
    0 AS distance_to_risk,
    
    -- Motif fields (NULL)
    '' AS motif_type,
    '' AS motif_center_address,
    0 AS motif_participant_count,
    
    -- Burst fields (NULL)
    '' AS burst_address,
    0 AS burst_start_timestamp,
    0 AS burst_end_timestamp,
    0 AS burst_duration_seconds,
    0 AS burst_transaction_count,
    0 AS burst_volume_usd,
    0.0 AS normal_tx_rate,
    0.0 AS burst_tx_rate,
    0.0 AS burst_intensity,
    0.0 AS z_score,
    [] AS hourly_distribution,
    [] AS peak_hours,
    
    -- Threshold fields (NULL)
    '' AS primary_address,
    0 AS threshold_value,
    '' AS threshold_type,
    0 AS transactions_near_threshold,
    0 AS avg_transaction_size,
    0 AS max_transaction_size,
    0.0 AS size_consistency,
    0.0 AS clustering_score,
    0 AS unique_days,
    0.0 AS avg_daily_transactions,
    0.0 AS temporal_spread_score,
    0.0 AS threshold_avoidance_score,
    
    -- Common temporal/evidence fields
    detection_timestamp,
    pattern_start_time,
    pattern_end_time,
    pattern_duration_hours,
    evidence_transaction_count,
    evidence_volume_usd,
    detection_method,
    _version
FROM analyzers_patterns_layering

UNION ALL

-- Network patterns
SELECT
    -- Common fields
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    pattern_hash,
    addresses_involved,
    address_roles,
    
    -- Cycle fields (NULL)
    [] AS cycle_path,
    0 AS cycle_length,
    0 AS cycle_volume_usd,
    
    -- Layering fields (NULL)
    [] AS layering_path,
    0 AS path_depth,
    0 AS path_volume_usd,
    '' AS source_address,
    '' AS destination_address,
    
    -- Network fields (real)
    network_members,
    network_size,
    network_density,
    hub_addresses,
    
    -- Proximity fields (NULL)
    '' AS risk_source_address,
    0 AS distance_to_risk,
    
    -- Motif fields (NULL)
    '' AS motif_type,
    '' AS motif_center_address,
    0 AS motif_participant_count,
    
    -- Burst fields (NULL)
    '' AS burst_address,
    0 AS burst_start_timestamp,
    0 AS burst_end_timestamp,
    0 AS burst_duration_seconds,
    0 AS burst_transaction_count,
    0 AS burst_volume_usd,
    0.0 AS normal_tx_rate,
    0.0 AS burst_tx_rate,
    0.0 AS burst_intensity,
    0.0 AS z_score,
    [] AS hourly_distribution,
    [] AS peak_hours,
    
    -- Threshold fields (NULL)
    '' AS primary_address,
    0 AS threshold_value,
    '' AS threshold_type,
    0 AS transactions_near_threshold,
    0 AS avg_transaction_size,
    0 AS max_transaction_size,
    0.0 AS size_consistency,
    0.0 AS clustering_score,
    0 AS unique_days,
    0.0 AS avg_daily_transactions,
    0.0 AS temporal_spread_score,
    0.0 AS threshold_avoidance_score,
    
    -- Common temporal/evidence fields
    detection_timestamp,
    pattern_start_time,
    pattern_end_time,
    pattern_duration_hours,
    evidence_transaction_count,
    evidence_volume_usd,
    detection_method,
    _version
FROM analyzers_patterns_network

UNION ALL

-- Proximity patterns
SELECT
    -- Common fields
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    pattern_hash,
    addresses_involved,
    address_roles,
    
    -- Cycle fields (NULL)
    [] AS cycle_path,
    0 AS cycle_length,
    0 AS cycle_volume_usd,
    
    -- Layering fields (NULL)
    [] AS layering_path,
    0 AS path_depth,
    0 AS path_volume_usd,
    '' AS source_address,
    '' AS destination_address,
    
    -- Network fields (NULL)
    [] AS network_members,
    0 AS network_size,
    0.0 AS network_density,
    [] AS hub_addresses,
    
    -- Proximity fields (real)
    risk_source_address,
    distance_to_risk,
    
    -- Motif fields (NULL)
    '' AS motif_type,
    '' AS motif_center_address,
    0 AS motif_participant_count,
    
    -- Burst fields (NULL)
    '' AS burst_address,
    0 AS burst_start_timestamp,
    0 AS burst_end_timestamp,
    0 AS burst_duration_seconds,
    0 AS burst_transaction_count,
    0 AS burst_volume_usd,
    0.0 AS normal_tx_rate,
    0.0 AS burst_tx_rate,
    0.0 AS burst_intensity,
    0.0 AS z_score,
    [] AS hourly_distribution,
    [] AS peak_hours,
    
    -- Threshold fields (NULL)
    '' AS primary_address,
    0 AS threshold_value,
    '' AS threshold_type,
    0 AS transactions_near_threshold,
    0 AS avg_transaction_size,
    0 AS max_transaction_size,
    0.0 AS size_consistency,
    0.0 AS clustering_score,
    0 AS unique_days,
    0.0 AS avg_daily_transactions,
    0.0 AS temporal_spread_score,
    0.0 AS threshold_avoidance_score,
    
    -- Common temporal/evidence fields
    detection_timestamp,
    pattern_start_time,
    pattern_end_time,
    pattern_duration_hours,
    evidence_transaction_count,
    evidence_volume_usd,
    detection_method,
    _version
FROM analyzers_patterns_proximity

UNION ALL

-- Motif patterns (fan-in and fan-out)
SELECT
    -- Common fields
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    pattern_hash,
    addresses_involved,
    address_roles,
    
    -- Cycle fields (NULL)
    [] AS cycle_path,
    0 AS cycle_length,
    0 AS cycle_volume_usd,
    
    -- Layering fields (NULL)
    [] AS layering_path,
    0 AS path_depth,
    0 AS path_volume_usd,
    '' AS source_address,
    '' AS destination_address,
    
    -- Network fields (NULL)
    [] AS network_members,
    0 AS network_size,
    0.0 AS network_density,
    [] AS hub_addresses,
    
    -- Proximity fields (NULL)
    '' AS risk_source_address,
    0 AS distance_to_risk,
    
    -- Motif fields (real)
    motif_type,
    motif_center_address,
    motif_participant_count,
    
    -- Burst fields (NULL)
    '' AS burst_address,
    0 AS burst_start_timestamp,
    0 AS burst_end_timestamp,
    0 AS burst_duration_seconds,
    0 AS burst_transaction_count,
    0 AS burst_volume_usd,
    0.0 AS normal_tx_rate,
    0.0 AS burst_tx_rate,
    0.0 AS burst_intensity,
    0.0 AS z_score,
    [] AS hourly_distribution,
    [] AS peak_hours,
    
    -- Threshold fields (NULL)
    '' AS primary_address,
    0 AS threshold_value,
    '' AS threshold_type,
    0 AS transactions_near_threshold,
    0 AS avg_transaction_size,
    0 AS max_transaction_size,
    0.0 AS size_consistency,
    0.0 AS clustering_score,
    0 AS unique_days,
    0.0 AS avg_daily_transactions,
    0.0 AS temporal_spread_score,
    0.0 AS threshold_avoidance_score,
    
    -- Common temporal/evidence fields
    detection_timestamp,
    pattern_start_time,
    pattern_end_time,
    pattern_duration_hours,
    evidence_transaction_count,
    evidence_volume_usd,
    detection_method,
    _version
FROM analyzers_patterns_motif

UNION ALL

-- Burst patterns
SELECT
    -- Common fields
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    pattern_hash,
    addresses_involved,
    address_roles,
    
    -- Cycle fields (NULL)
    [] AS cycle_path,
    0 AS cycle_length,
    0 AS cycle_volume_usd,
    
    -- Layering fields (NULL)
    [] AS layering_path,
    0 AS path_depth,
    0 AS path_volume_usd,
    '' AS source_address,
    '' AS destination_address,
    
    -- Network fields (NULL)
    [] AS network_members,
    0 AS network_size,
    0.0 AS network_density,
    [] AS hub_addresses,
    
    -- Proximity fields (NULL)
    '' AS risk_source_address,
    0 AS distance_to_risk,
    
    -- Motif fields (NULL)
    '' AS motif_type,
    '' AS motif_center_address,
    0 AS motif_participant_count,
    
    -- Burst fields (real)
    burst_address,
    burst_start_timestamp,
    burst_end_timestamp,
    burst_duration_seconds,
    burst_transaction_count,
    burst_volume_usd,
    normal_tx_rate,
    burst_tx_rate,
    burst_intensity,
    z_score,
    hourly_distribution,
    peak_hours,
    
    -- Threshold fields (NULL)
    '' AS primary_address,
    0 AS threshold_value,
    '' AS threshold_type,
    0 AS transactions_near_threshold,
    0 AS avg_transaction_size,
    0 AS max_transaction_size,
    0.0 AS size_consistency,
    0.0 AS clustering_score,
    0 AS unique_days,
    0.0 AS avg_daily_transactions,
    0.0 AS temporal_spread_score,
    0.0 AS threshold_avoidance_score,
    
    -- Common temporal/evidence fields
    detection_timestamp,
    pattern_start_time,
    pattern_end_time,
    pattern_duration_hours,
    evidence_transaction_count,
    evidence_volume_usd,
    detection_method,
    _version
FROM analyzers_patterns_burst

UNION ALL

-- Threshold evasion patterns
SELECT
    -- Common fields
    window_days,
    processing_date,
    pattern_id,
    pattern_type,
    pattern_hash,
    addresses_involved,
    address_roles,
    
    -- Cycle fields (NULL)
    [] AS cycle_path,
    0 AS cycle_length,
    0 AS cycle_volume_usd,
    
    -- Layering fields (NULL)
    [] AS layering_path,
    0 AS path_depth,
    0 AS path_volume_usd,
    '' AS source_address,
    '' AS destination_address,
    
    -- Network fields (NULL)
    [] AS network_members,
    0 AS network_size,
    0.0 AS network_density,
    [] AS hub_addresses,
    
    -- Proximity fields (NULL)
    '' AS risk_source_address,
    0 AS distance_to_risk,
    
    -- Motif fields (NULL)
    '' AS motif_type,
    '' AS motif_center_address,
    0 AS motif_participant_count,
    
    -- Burst fields (NULL)
    '' AS burst_address,
    0 AS burst_start_timestamp,
    0 AS burst_end_timestamp,
    0 AS burst_duration_seconds,
    0 AS burst_transaction_count,
    0 AS burst_volume_usd,
    0.0 AS normal_tx_rate,
    0.0 AS burst_tx_rate,
    0.0 AS burst_intensity,
    0.0 AS z_score,
    [] AS hourly_distribution,
    [] AS peak_hours,
    
    -- Threshold fields (real)
    primary_address,
    threshold_value,
    threshold_type,
    transactions_near_threshold,
    avg_transaction_size,
    max_transaction_size,
    size_consistency,
    clustering_score,
    unique_days,
    avg_daily_transactions,
    temporal_spread_score,
    threshold_avoidance_score,
    
    -- Common temporal/evidence fields
    detection_timestamp,
    pattern_start_time,
    pattern_end_time,
    pattern_duration_hours,
    evidence_transaction_count,
    evidence_volume_usd,
    detection_method,
    _version
FROM analyzers_patterns_threshold;