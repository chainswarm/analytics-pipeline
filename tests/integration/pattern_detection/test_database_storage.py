"""
Integration tests for pattern database storage.

Tests that patterns are correctly stored in their specialized tables.
Requires ClickHouse to be running.

Run with:
  cd tests/integration && docker-compose up -d
  pytest tests/integration/pattern_detection/test_database_storage.py -v
"""

import pytest
import time
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.storage.constants import PatternTypes


class TestPatternDatabaseStorage:
    """Integration tests for pattern database storage."""
    
    def test_cycle_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test cycle patterns stored in analyzers_patterns_cycle table."""
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        patterns = [{
            'pattern_id': 'cycle_integration_001',
            'pattern_type': PatternTypes.CYCLE,
            'pattern_hash': 'hash_cycle_001',
            'addresses_involved': ['A', 'B', 'C'],
            'address_roles': ['participant', 'participant', 'participant'],
            'cycle_path': ['A', 'B', 'C'],
            'cycle_length': 3,
            'cycle_volume_usd': 33000,
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 3,
            'evidence_volume_usd': 33000,
            'detection_method': 'cycle_detection'
        }]
        
        repo.insert_deduplicated_patterns(patterns, window_days=test_data_context['window_days'], processing_date=test_data_context['processing_date'])
        result = test_clickhouse_client.query("SELECT * FROM analyzers_patterns_cycle WHERE pattern_id = 'cycle_integration_001'")
        
        assert len(result.result_rows) == 1
        assert 'cycle_path' in result.column_names
    
    def test_motif_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test motif patterns stored in analyzers_patterns_motif table."""
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        patterns = [{
            'pattern_id': 'motif_integration_001',
            'pattern_type': PatternTypes.MOTIF_FANIN,
            'pattern_hash': 'hash_motif_001',
            'addresses_involved': ['CENTER', 'S1', 'S2'],
            'address_roles': ['center', 'source', 'source'],
            'motif_type': 'fanin',
            'motif_center_address': 'CENTER',
            'motif_participant_count': 5,
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 2,
            'evidence_volume_usd': 20000,
            'detection_method': 'motif_detection'
        }]
        
        repo.insert_deduplicated_patterns(patterns, window_days=test_data_context['window_days'], processing_date=test_data_context['processing_date'])
        result = test_clickhouse_client.query("SELECT * FROM analyzers_patterns_motif WHERE pattern_id = 'motif_integration_001'")
        
        assert len(result.result_rows) == 1
        assert 'motif_type' in result.column_names
    
    def test_layering_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test layering patterns stored in analyzers_patterns_layering table."""
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        patterns = [{
            'pattern_id': 'layering_integration_001',
            'pattern_type': PatternTypes.LAYERING_PATH,
            'pattern_hash': 'hash_layering_001',
            'addresses_involved': ['A', 'B', 'C', 'D'],
            'address_roles': ['source', 'intermediary', 'intermediary', 'destination'],
            'layering_path': ['A', 'B', 'C', 'D'],
            'path_depth': 4,
            'path_volume_usd': 40000,
            'source_address': 'A',
            'destination_address': 'D',
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 3,
            'evidence_volume_usd': 40000,
            'detection_method': 'path_analysis'
        }]
        
        repo.insert_deduplicated_patterns(patterns, window_days=test_data_context['window_days'], processing_date=test_data_context['processing_date'])
        result = test_clickhouse_client.query("SELECT * FROM analyzers_patterns_layering WHERE pattern_id = 'layering_integration_001'")
        
        assert len(result.result_rows) == 1
        assert 'layering_path' in result.column_names
    
    def test_threshold_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test threshold patterns stored in analyzers_patterns_threshold table."""
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        patterns = [{
            'pattern_id': 'threshold_integration_001',
            'pattern_type': PatternTypes.THRESHOLD_EVASION,
            'pattern_hash': 'hash_threshold_001',
            'addresses_involved': ['EVADER'],
            'address_roles': ['primary_address'],
            'primary_address': 'EVADER',
            'threshold_value': 10000,
            'threshold_type': 'reporting',
            'transactions_near_threshold': 10,
            'avg_transaction_size': 9500,
            'max_transaction_size': 9900,
            'size_consistency': 0.95,
            'clustering_score': 0.85,
            'unique_days': 1,
            'avg_daily_transactions': 10,
            'temporal_spread_score': 0.5,
            'threshold_avoidance_score': 0.85,
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 10,
            'evidence_volume_usd': 95000,
            'detection_method': 'temporal_analysis'
        }]
        
        repo.insert_deduplicated_patterns(patterns, window_days=test_data_context['window_days'], processing_date=test_data_context['processing_date'])
        result = test_clickhouse_client.query("SELECT * FROM analyzers_patterns_threshold WHERE pattern_id = 'threshold_integration_001'")
        
        assert len(result.result_rows) == 1
        assert 'threshold_value' in result.column_names
    
    def test_proximity_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test proximity patterns stored in analyzers_patterns_proximity table."""
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        patterns = [{
            'pattern_id': 'proximity_integration_001',
            'pattern_type': PatternTypes.PROXIMITY_RISK,
            'pattern_hash': 'hash_proximity_001',
            'addresses_involved': ['RISK', 'SUSPECT'],
            'address_roles': ['risk_source', 'suspect'],
            'risk_source_address': 'RISK',
            'distance_to_risk': 2,
            'risk_propagation_score': 0.333,
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 5,
            'evidence_volume_usd': 50000,
            'detection_method': 'proximity_analysis'
        }]
        
        repo.insert_deduplicated_patterns(patterns, window_days=test_data_context['window_days'], processing_date=test_data_context['processing_date'])
        result = test_clickhouse_client.query("SELECT * FROM analyzers_patterns_proximity WHERE pattern_id = 'proximity_integration_001'")
        
        assert len(result.result_rows) == 1
        assert 'risk_source_address' in result.column_names
    
    def test_network_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test network patterns stored in analyzers_patterns_network table."""
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        patterns = [{
            'pattern_id': 'network_integration_001',
            'pattern_type': PatternTypes.SMURFING_NETWORK,
            'pattern_hash': 'hash_network_001',
            'addresses_involved': ['A', 'B', 'C', 'D', 'E'],
            'address_roles': ['hub', 'participant', 'participant', 'participant', 'participant'],
            'network_members': ['A', 'B', 'C', 'D', 'E'],
            'network_size': 5,
            'network_density': 0.65,
            'hub_addresses': ['A'],
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 8,
            'evidence_volume_usd': 80000,
            'detection_method': 'network_analysis'
        }]
        
        repo.insert_deduplicated_patterns(patterns, window_days=test_data_context['window_days'], processing_date=test_data_context['processing_date'])
        result = test_clickhouse_client.query("SELECT * FROM analyzers_patterns_network WHERE pattern_id = 'network_integration_001'")
        
        assert len(result.result_rows) == 1
        assert 'network_members' in result.column_names
    
    def test_burst_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test burst patterns stored in analyzers_patterns_burst table."""
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        patterns = [{
            'pattern_id': 'burst_integration_001',
            'pattern_type': PatternTypes.TEMPORAL_BURST,
            'pattern_hash': 'hash_burst_001',
            'addresses_involved': ['BURSTER'],
            'address_roles': ['burst_source'],
            'burst_address': 'BURSTER',
            'burst_start_timestamp': int(time.time()) - 7200,
            'burst_end_timestamp': int(time.time()) - 3600,
            'burst_duration_seconds': 3600,
            'burst_transaction_count': 100,
            'burst_volume_usd': 500000,
            'normal_tx_rate': 10.0,
            'burst_tx_rate': 100.0,
            'burst_intensity': 10.0,
            'z_score': 5.5,
            'hourly_distribution': [],
            'peak_hours': [10, 11],
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 100,
            'evidence_volume_usd': 500000,
            'detection_method': 'temporal_analysis'
        }]
        
        repo.insert_deduplicated_patterns(patterns, window_days=test_data_context['window_days'], processing_date=test_data_context['processing_date'])
        result = test_clickhouse_client.query("SELECT * FROM analyzers_patterns_burst WHERE pattern_id = 'burst_integration_001'")
        
        assert len(result.result_rows) == 1
        assert 'burst_address' in result.column_names