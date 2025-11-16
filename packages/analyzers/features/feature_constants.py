# Feature subset constants for anomaly detection

# Feature subsets for multi-dimensional anomaly detection using Isolation Forest
# These are used by AnomalyScorer to generate 4 distinct anomaly score features

BEHAVIORAL_TEMPORAL_FEATURES = [
    'total_in_usd', 'total_out_usd', 'net_flow_usd', 'total_volume_usd',
    'avg_tx_in_usd', 'avg_tx_out_usd', 'max_tx_usd', 'min_tx_usd',
    'amount_variance', 'amount_skewness', 'amount_kurtosis',
    'volume_std', 'volume_cv',
    'tx_in_count', 'tx_out_count', 'tx_total_count',
    'activity_days', 'activity_span_days', 'avg_daily_volume_usd',
    'peak_hour', 'peak_day', 'regularity_score', 'burst_factor',
    'velocity_score', 'hourly_entropy', 'daily_entropy',
    'weekend_transaction_ratio', 'night_transaction_ratio', 'consistency_score',
    'structuring_score', 'round_number_ratio', 'unusual_timing_score'
]

GRAPH_FEATURES = [
    'pagerank', 'betweenness', 'closeness', 'clustering_coefficient',
    'kcore', 'centrality_score', 'degree_total'
]

NEIGHBORHOOD_FEATURES = [
    'khop1_count', 'khop2_count', 'khop3_count',
    'khop1_volume_usd', 'khop2_volume_usd', 'khop3_volume_usd'
]