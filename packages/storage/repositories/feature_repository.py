from typing import Dict, List, Optional, Any
from decimal import Decimal
from clickhouse_connect.driver import Client
from loguru import logger
from packages.storage.repositories.base_repository import BaseRepository
from packages.storage.repositories.utils import row_to_dict


class FeatureRepository(BaseRepository):

    @classmethod
    def table_name(cls) -> str:
        return "analyzers_features"

    def __init__(self, client: Client):
        super().__init__(client)
        self.features_table_name = "analyzers_features"

    def delete_partition(self, window_days: int, processing_date: str) -> None:
        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
        
        query = f"""
        ALTER TABLE {self.features_table_name}
        DELETE WHERE window_days = %(window_days)s AND processing_date = %(processing_date)s
        """
        
        params = {
            'window_days': window_days,
            'processing_date': date_obj
        }
        
        self.client.command(query, parameters=params)
        logger.info(f"Deleted partition for window_days={window_days}, processing_date={processing_date} from {self.features_table_name}")

    # ------------- Feature Write Operations --------------------------------------

    def insert_features(self, features: List[Dict], window_days: int, processing_date: str):

        if not features:
            raise ValueError("insert_features called with empty features list - programming error")

        batch_size = 1000

        logger.info(f"Inserting {len(features)} ML features into {self.features_table_name}")

        from datetime import datetime
        date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()

        for i in range(0, len(features), batch_size):
            batch = features[i:i + batch_size]
            
            # Prepare batch data for ClickHouse client
            batch_data = []
            for idx, feature in enumerate(batch):

                batch_data.append([
                    # Time series dimensions
                    window_days,
                    date_obj,
                    
                    # Core identifiers and node features
                    feature['address'],
                    int(feature['degree_in']),
                    int(feature['degree_out']),
                    int(feature['degree_total']),
                    int(feature['unique_counterparties']),
                    
                    # Volume features (Decimal for financial precision)
                    str(feature['total_in_usd']),
                    str(feature['total_out_usd']),
                    str(feature['net_flow_usd']),
                    str(feature['total_volume_usd']),
                    str(feature['avg_tx_in_usd']),
                    str(feature['avg_tx_out_usd']),
                    str(feature['median_tx_in_usd']),
                    str(feature['median_tx_out_usd']),
                    str(feature['max_tx_usd']),
                    str(feature['min_tx_usd']),
                    
                    # Statistical features
                    float(feature['amount_variance']),
                    float(feature['amount_skewness']),
                    float(feature['amount_kurtosis']),
                    float(feature['volume_std']),
                    float(feature['volume_cv']),
                    float(feature['flow_concentration']),
                    
                    # Transaction counts
                    int(feature['tx_in_count']),
                    int(feature['tx_out_count']),
                    int(feature['tx_total_count']),
                    
                    # Temporal features
                    int(feature['activity_days']),
                    int(feature['activity_span_days']),
                    str(feature['avg_daily_volume_usd']) if isinstance(feature['avg_daily_volume_usd'], Decimal) else str(Decimal(str(feature['avg_daily_volume_usd']))),  # Financial precision
                    int(feature['peak_hour']),
                    int(feature['peak_day']),
                    float(feature['regularity_score']),
                    float(feature['burst_factor']),
                    
                    # Flow characteristics
                    float(feature['reciprocity_ratio']),
                    float(feature['flow_diversity']),
                    float(feature['counterparty_concentration']),
                    float(feature['velocity_score']),
                    float(feature['structuring_score']),
                    
                    # Behavioral pattern features
                    float(feature['hourly_entropy']),
                    float(feature['daily_entropy']),
                    float(feature['weekend_transaction_ratio']),
                    float(feature['night_transaction_ratio']),
                    float(feature['consistency_score']),
                    
                    # Temporal classification features (observations only)
                    bool(feature['is_new_address']),
                    bool(feature['is_dormant_reactivated']),
                    
                    # Supporting metrics
                    int(feature['unique_recipients_count']),
                    int(feature['unique_senders_count']),
                    
                    # GRAPH ANALYTICS FEATURES (MANDATORY - matching schema exactly)
                    float(feature.get('pagerank', 0.0)),
                    float(feature.get('betweenness', 0.0)),
                    float(feature.get('closeness', 0.0)),  # Schema has this
                    float(feature.get('clustering_coefficient', 0.0)),
                    int(feature.get('kcore', 0)),
                    int(feature.get('community_id', 0)),
                    float(feature.get('centrality_score', 0.0)),  # Schema has this
                    
                    # K-hop neighborhood features (matching schema)
                    int(feature.get('khop1_count', 0)),
                    int(feature.get('khop2_count', 0)),
                    int(feature.get('khop3_count', 0)),
                    str(feature.get('khop1_volume_usd', 0.0)),  # Decimal128 in schema
                    str(feature.get('khop2_volume_usd', 0.0)),  # Decimal128 in schema
                    str(feature.get('khop3_volume_usd', 0.0)),  # Decimal128 in schema
                    
                    # Advanced flow features (from schema)
                    float(feature.get('flow_reciprocity_entropy', 0.0)),
                    float(feature.get('counterparty_stability', 0.0)),
                    float(feature.get('flow_burstiness', 0.0)),
                    float(feature.get('transaction_regularity', 0.0)),
                    float(feature.get('amount_predictability', 0.0)),

                    
                    # Temporal metadata (from schema)
                    int(feature.get('first_activity_timestamp', 0)),
                    int(feature.get('last_activity_timestamp', 0)),

                    # Asset diversity (schema)
                    int(feature.get('unique_assets_in', 0)),
                    int(feature.get('unique_assets_out', 0)),
                    str(feature.get('dominant_asset_in', '')),
                    str(feature.get('dominant_asset_out', '')),
                    float(feature.get('asset_diversity_score', 0.0)),

                    # Behavioral pattern arrays and peaks
                    [int(x) for x in feature.get('hourly_activity', [])],
                    [int(x) for x in feature.get('daily_activity', [])],
                    int(feature.get('peak_activity_hour', 0)),
                    int(feature.get('peak_activity_day', 0)),

                    # Additional flow/behavioral metrics
                    float(feature.get('small_transaction_ratio', 0.0)),
                    float(feature.get('concentration_ratio', 0.0)),

                    # Metadata
                    self._generate_version(),
                ])
            
            # Column names matching schema exactly (features.sql) with MANDATORY graph analytics
            column_names = [
                # Time series dimensions
                'window_days', 'processing_date',
                # Core identifiers and node features
                'address', 'degree_in', 'degree_out', 'degree_total', 'unique_counterparties',
                # Volume features
                'total_in_usd', 'total_out_usd', 'net_flow_usd', 'total_volume_usd',
                'avg_tx_in_usd', 'avg_tx_out_usd', 'median_tx_in_usd', 'median_tx_out_usd', 'max_tx_usd', 'min_tx_usd',
                # Statistical features
                'amount_variance', 'amount_skewness', 'amount_kurtosis', 'volume_std', 'volume_cv', 'flow_concentration',
                # Transaction counts
                'tx_in_count', 'tx_out_count', 'tx_total_count',
                # Temporal features
                'activity_days', 'activity_span_days', 'avg_daily_volume_usd', 'peak_hour', 'peak_day', 'regularity_score', 'burst_factor',
                # Flow characteristics
                'reciprocity_ratio', 'flow_diversity', 'counterparty_concentration',
                'velocity_score', 'structuring_score',
                # Behavioral pattern features (entropy/ratios)
                'hourly_entropy', 'daily_entropy', 'weekend_transaction_ratio', 'night_transaction_ratio', 'consistency_score',
                # Classification features
                # 'is_exchange_like', 'is_whale', 'is_mixer_like', 'is_contract_like', # Missing in values and DB
                'is_new_address', 'is_dormant_reactivated',
                # 'is_high_volume_trader', 'is_hub_address', 'is_retail_active', 'is_whale_inactive', 'is_retail_inactive', 'is_regular_user', # Missing in values and DB
                # Supporting metrics
                'unique_recipients_count', 'unique_senders_count',
                # GRAPH ANALYTICS COLUMNS (MANDATORY - matching schema exactly)
                'pagerank', 'betweenness', 'closeness', 'clustering_coefficient', 'kcore', 'community_id', 'centrality_score',
                # K-hop neighborhood features (matching schema)
                'khop1_count', 'khop2_count', 'khop3_count', 'khop1_volume_usd', 'khop2_volume_usd', 'khop3_volume_usd',
                # Advanced flow features (from schema)
                'flow_reciprocity_entropy', 'counterparty_stability', 'flow_burstiness', 'transaction_regularity', 'amount_predictability',
                # Risk and anomaly features (from schema)
                # 'behavioral_anomaly_score', 'graph_anomaly_score', 'neighborhood_anomaly_score', # Missing in DB
                # 'global_anomaly_score', # Missing in DB
                # 'outlier_transactions', 'suspicious_pattern_score', # Missing in DB
                # Temporal metadata (from schema)
                'first_activity_timestamp', 'last_activity_timestamp',
                # Asset diversity (schema)
                'unique_assets_in', 'unique_assets_out', 'dominant_asset_in', 'dominant_asset_out', 'asset_diversity_score',
                # Behavioral pattern arrays and peaks
                'hourly_activity', 'daily_activity', 'peak_activity_hour', 'peak_activity_day',
                # Additional flow/behavioral metrics
                'small_transaction_ratio', 'concentration_ratio',
                # Version
                '_version'
            ]
            
            # Insert batch using dynamic table name
            self.client.insert(
                self.features_table_name,  # Dynamic table name (e.g., graph_features_analytics_90d)
                batch_data,
                column_names=column_names
            )

    def get_features_count(self) -> int:

        query = f"SELECT count() FROM {self.features_table_name}"
        
        result = self.client.query(query)
        count = result.result_rows[0][0]
        return int(count)
    def get_all_features(
        self,
        window_days: int = None,
        processing_date: str = None,
        limit: int = 1_000_000,
        offset: int = 0
    ) -> List[Dict]:
        where_clauses = []
        params = {'limit': limit, 'offset': offset}
        
        if window_days is not None:
            where_clauses.append("window_days = %(window_days)s")
            params['window_days'] = window_days
            
        if processing_date is not None:
            from datetime import datetime
            date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
            where_clauses.append("processing_date = %(processing_date)s")
            params['processing_date'] = date_obj
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
        SELECT *
        FROM {self.features_table_name}
        WHERE {where_clause}
        ORDER BY total_volume_usd DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """
        
        result = self.client.query(query, parameters=params)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def get_window_features_count(
        self,
        window_days: int = None,
        processing_date: str = None
    ) -> int:
        where_clauses = []
        params = {}
        
        if window_days is not None:
            where_clauses.append("window_days = %(window_days)s")
            params['window_days'] = window_days
            
        if processing_date is not None:
            from datetime import datetime
            date_obj = datetime.strptime(processing_date, '%Y-%m-%d').date()
            where_clauses.append("processing_date = %(processing_date)s")
            params['processing_date'] = date_obj
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
        SELECT count()
        FROM {self.features_table_name}
        WHERE {where_clause}
        """
        
        result = self.client.query(query, parameters=params)
        return int(result.result_rows[0][0])

    def get_features_by_quality(self, min_total_volume: Decimal, limit: int = 1000) -> List[Dict]:
        """Get features by volume instead of quality score."""
        params = {
            "min_volume": str(min_total_volume),
            "limit": int(limit),
        }

        query = f"""
        SELECT *
        FROM {self.features_table_name}
        WHERE total_volume_usd >= %(min_volume)s
        ORDER BY total_volume_usd DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        rows = [row_to_dict(row, result.column_names) for row in result.result_rows]
        return rows

    def get_features_for_export(
        self,
        feature_subset: Optional[List[str]] = None,
        limit: int = 1_000_000
    ) -> List[Dict]:

        params = {
            "limit": int(limit),
        }
        
        query = f"""
        SELECT *
        FROM {self.features_table_name}
        ORDER BY total_volume_usd DESC
        LIMIT %(limit)s
        """
        
        result = self.client.query(query, parameters=params)
        
        if feature_subset:
            # Filter the dictionary keys after using row_to_dict
            rows = []
            for row in result.result_rows:
                full_dict = row_to_dict(row, result.column_names)
                # Ensure address is always included
                filtered_keys = ['address'] + [col for col in feature_subset if col != 'address' and col in full_dict]
                filtered_dict = {key: full_dict[key] for key in filtered_keys if key in full_dict}
                rows.append(filtered_dict)
        else:
            rows = [row_to_dict(row, result.column_names) for row in result.result_rows]

        return rows

    def get_feature_counts(self) -> Dict[str, int]:

        query = f"""
        SELECT
            count() as total_addresses,
            countIf(total_volume_usd >= 100000) as high_volume,
            countIf(total_volume_usd >= 10000 AND total_volume_usd < 100000) as medium_volume,
            countIf(total_volume_usd < 10000) as low_volume
        FROM {self.features_table_name}
        """
        
        result = self.client.query(query)
        row = result.result_rows[0]
        
        counts = {
            'total_addresses': int(row[0]),
            'high_quality': int(row[1]),
            'medium_quality': int(row[2]),
            'low_quality': int(row[3]),
            'high_outliers': int(row[4])
        }

        return counts

    # ------------- Comprehensive Address Data (replaces AddressPanelRepository) -------------

    def get_addresses_comprehensive_data(self) -> List[Dict]:

        query = f"""
        SELECT *
        FROM {self.features_table_name}
        ORDER BY total_volume_usd DESC
        """
        
        result = self.client.query(query)
        column_names = result.column_names
        
        if not result.result_rows:
            raise ValueError(f"No comprehensive data found")
            
        comprehensive_data = []
        for row in result.result_rows:
            comprehensive_data.append(row_to_dict(row, column_names))
            
        return comprehensive_data

    def update_graph_features_batch(self, feature_updates: Dict[str, Dict[str, Any]]) -> int:

        if not feature_updates:
            raise ValueError("update_graph_features_batch called with empty feature_updates - programming error")
        
        addresses_to_update = list(feature_updates.keys())
        batch_size = 1000
        
        logger.info(f"Updating graph features for {len(addresses_to_update)} addresses in {self.features_table_name}")
        
        # Process in batches to manage memory and performance
        for i in range(0, len(addresses_to_update), batch_size):
            batch_addresses = addresses_to_update[i:i + batch_size]
            
            # Fetch existing records for this batch
            placeholders = ', '.join([f'%({j})s' for j in range(len(batch_addresses))])
            params = {str(j): batch_addresses[j] for j in range(len(batch_addresses))}
            
            fetch_query = f"""
            SELECT * FROM {self.features_table_name}
            WHERE address IN ({placeholders})
            FINAL
            """
            
            try:
                result = self.client.query(fetch_query, parameters=params)
                existing_records = {row[0]: row for row in result.result_rows}  # address is first column
                column_names = result.column_names
                
                # Prepare batch data for insertion
                batch_data = []
                
                for address in batch_addresses:
                    if address in existing_records:
                        # Merge updates with existing record
                        existing_row = list(existing_records[address])
                        updated_row = self._merge_feature_updates(existing_row, column_names, feature_updates[address])
                    else:
                        # Create new record with only updated fields (others will be defaults/nulls)
                        updated_row = self._create_new_feature_record(address, column_names, feature_updates[address])
                    
                    # Set new version for ReplacingMergeTree
                    version_index = column_names.index('_version')
                    updated_row[version_index] = self._generate_version()
                    
                    batch_data.append(updated_row)
                
                # Insert batch with updated records
                if batch_data:
                    self.client.insert(
                        self.features_table_name,
                        batch_data,
                        column_names=column_names
                    )


            except Exception as e:
                logger.error(f"Failed to update graph features batch {i//batch_size + 1}: {str(e)}")
                raise
        
        logger.info(f"Successfully updated graph features")
    
    def _merge_feature_updates(self, existing_row: List, column_names: List[str], updates: Dict[str, Any]) -> List:

        updated_row = existing_row.copy()
        
        for field_name, value in updates.items():
            if field_name in column_names:
                field_index = column_names.index(field_name)
                
                # Apply proper type conversion based on field type
                if isinstance(value, (int, float)):
                    updated_row[field_index] = float(value) if field_name in [
                        'pagerank', 'betweenness', 'closeness', 'clustering_coefficient',
                 'flow_reciprocity_entropy', 'counterparty_stability',
                        'flow_burstiness', 'transaction_regularity', 'amount_predictability'
                    ] else int(value)
                elif isinstance(value, bool):
                    updated_row[field_index] = bool(value)
                else:
                    # Handle strings and other types
                    updated_row[field_index] = value
            else:
                logger.warning(f"Field '{field_name}' not found in schema, skipping update")
        
        return updated_row
    
    def _create_new_feature_record(self, address: str, column_names: List[str], updates: Dict[str, Any]) -> List:

        # Initialize with None/default values
        new_record = [None] * len(column_names)
        
        # Set required address field
        address_index = column_names.index('address')
        new_record[address_index] = address
        
        # Set default values for critical fields to avoid null issues
        defaults = {
            'degree_in': 0,
            'degree_out': 0,
            'degree_total': 0,
            'unique_counterparties': 0,
            'total_in_usd': '0',
            'total_out_usd': '0',
            'net_flow_usd': '0',
            'total_volume_usd': '0',
            'tx_in_count': 0,
            'tx_out_count': 0,
            'tx_total_count': 0
        }
        
        # Apply defaults
        for field_name, default_value in defaults.items():
            if field_name in column_names:
                field_index = column_names.index(field_name)
                new_record[field_index] = default_value
        
        # Apply updates using the same merge logic
        return self._merge_feature_updates(new_record, column_names, updates)

    def get_comprehensive_node_data(self, addresses: List[str]) -> List[Dict]:

        addresses_str = ', '.join(f"'{addr}'" for addr in addresses)

        query = f"""
        SELECT *
        FROM {self.features_table_name}
        WHERE address IN ({addresses_str})
        """

        result = self.client.query(query)

        for row in result.result_rows:
            yield row_to_dict(row, result.column_names)

    def get_feature_columns(self) -> List[str]:
        """
        Get list of available feature column names from the features table.
        This is used by RiskScorer to discover available features dynamically.
        """
        try:
            # Get column information from ClickHouse system tables
            query = f"""
            SELECT name
            FROM system.columns
            WHERE database = currentDatabase()
            AND table = '{self.features_table_name}'
            ORDER BY position
            """

            result = self.client.query(query)
            columns = [row[0] for row in result.result_rows]

            # Filter out metadata columns that aren't features
            feature_columns = []
            for col in columns:
                # Exclude metadata columns
                if col not in ['address', '_version']:
                    feature_columns.append(col)

            return feature_columns

        except Exception as e:
            logger.warning(f"Failed to get feature columns: {e}")
            # Return fallback list of common feature columns
            return self._get_fallback_feature_columns()

    def _get_fallback_feature_columns(self) -> List[str]:
        """Fallback feature columns when dynamic discovery fails."""
        return [
            # Core volume features
            'total_volume_usd', 'total_in_usd', 'total_out_usd', 'net_flow_usd',
            # Network features
            'degree_total', 'unique_counterparties', 'reciprocity_ratio',
            # Graph features
            'pagerank', 'betweenness', 'closeness', 'clustering_coefficient',
            # Temporal features
            'velocity_score', 'burst_factor', 'peak_activity_hour',
            # Behavioral features
            'structuring_score', 'concentration_ratio',
            # Classification features
            'is_exchange_like', 'is_whale', 'is_mixer_like'
        ]