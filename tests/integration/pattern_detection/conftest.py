"""
Pytest configuration and fixtures for pattern detection integration tests.

This module provides:
- ClickHouse client fixtures (source and test databases)
- Schema setup/teardown
- Test data loading from production ClickHouse
- Common test utilities
"""

import pytest
import os
from datetime import datetime
from dotenv import load_dotenv
from clickhouse_connect.driver import Client
from packages.storage.repositories import ClientFactory, get_connection_params, MigrateSchema

# Test parameters
TEST_NETWORK = "torus"
TEST_PROCESSING_DATE = "2025-11-20"
TEST_WINDOW_DAYS = 300

@pytest.fixture(scope="session")
def load_env():
    """Load environment variables from .env file."""
    load_dotenv()
    
@pytest.fixture(scope="session")
def source_clickhouse_client(load_env):
    """
    Client connected to production/source ClickHouse database.
    Used to extract test data.
    """
    # Get connection params from .env (your existing data pipeline)
    connection_params = get_connection_params(TEST_NETWORK)
    factory = ClientFactory(connection_params)
    
    with factory.client_context() as client:
        yield client

@pytest.fixture(scope="session")
def test_clickhouse_client():
    """
    Client connected to test ClickHouse database.
    Used for running tests in isolation.
    """
    from clickhouse_connect import get_client
    
    # Test ClickHouse connection (from docker-compose)
    client = get_client(
        host='localhost',
        port=8323,
        username='test',
        password='test',
        database='test'
    )
    
    yield client
    client.close()

@pytest.fixture(scope="session")
def setup_test_schema(test_clickhouse_client):
    """Initialize test database schema."""
    # Initialize schemas in test database using MigrateSchema
    migrator = MigrateSchema(test_clickhouse_client)
    migrator.run_core_migrations()
    migrator.run_analyzer_migrations()
    yield
    # Cleanup after all tests (optional)

@pytest.fixture(scope="function")
def clean_pattern_tables(test_clickhouse_client):
    """Clean pattern tables before each test."""
    tables = [
        'analyzers_patterns_cycle',
        'analyzers_patterns_layering',
        'analyzers_patterns_network',
        'analyzers_patterns_proximity',
        'analyzers_patterns_motif'
    ]
    
    for table in tables:
        test_clickhouse_client.command(f"TRUNCATE TABLE IF EXISTS {table}")
    
    yield

@pytest.fixture(scope="session")
def test_data_context():
    """Provide test data context."""
    return {
        'network': TEST_NETWORK,
        'processing_date': TEST_PROCESSING_DATE,
        'window_days': TEST_WINDOW_DAYS
    }

@pytest.fixture(scope="session")
def load_test_transfers(source_clickhouse_client, test_clickhouse_client, setup_test_schema, test_data_context):
    """
    Load transfer data from source ClickHouse for the test time window.
    This is the base data needed for pattern detection.
    """
    # Calculate time window
    from packages.utils import calculate_time_window
    start_ts, end_ts = calculate_time_window(
        test_data_context['window_days'],
        test_data_context['processing_date']
    )
    
    # TODO: Extract transfers from source and load into test DB
    # For now, just a placeholder
    
    yield