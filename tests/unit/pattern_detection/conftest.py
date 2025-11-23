"""
Pytest configuration for pattern detection unit tests.

These fixtures provide test context without external dependencies.
All tests use mocks for repositories and synthetic graph data.
"""

import pytest

# Test parameters - same as integration tests for consistency
TEST_NETWORK = "torus"
TEST_PROCESSING_DATE = "2025-11-20"
TEST_WINDOW_DAYS = 300


@pytest.fixture(scope="session")
def test_data_context():
    """
    Provide test data context for unit tests.
    
    Returns context needed by analyzers but without actual data loading.
    """
    return {
        'network': TEST_NETWORK,
        'processing_date': TEST_PROCESSING_DATE,
        'window_days': TEST_WINDOW_DAYS
    }