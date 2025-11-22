# Pattern Detection Integration Tests

Integration tests for structural pattern detection algorithms.

## Overview

These tests verify that each pattern detection algorithm works correctly with real-world data:
- **Cycle Detection** - Circular transaction patterns
- **Layering Detection** - Multi-hop obfuscation paths
- **Network Detection** - Smurfing networks and SCCs
- **Proximity Detection** - Risk propagation analysis
- **Motif Detection** - Fan-in and fan-out patterns

## Test Infrastructure

### Docker Compose
- Runs isolated ClickHouse instance for tests
- Ports: 8323 (HTTP), 9300 (Native), 9309 (Cluster)
- Database: `test`, User: `test`, Password: `test`

### Test Data
- Network: `torus`
- Processing Date: `2025-11-20`
- Window: `300 days`
- Source: Main data pipeline ClickHouse (from .env)

## Running Tests

### Start Test ClickHouse
```bash
cd tests/integration/pattern_detection
docker-compose up -d
```

### Run All Tests
```bash
pytest tests/integration/pattern_detection/ -v
```

### Run Specific Pattern Tests
```bash
pytest tests/integration/pattern_detection/test_cycle_detection.py -v
pytest tests/integration/pattern_detection/test_layering_detection.py -v
pytest tests/integration/pattern_detection/test_network_detection.py -v
pytest tests/integration/pattern_detection/test_proximity_detection.py -v
pytest tests/integration/pattern_detection/test_motif_detection.py -v
```

### Stop Test ClickHouse
```bash
cd tests/integration/pattern_detection
docker-compose down -v
```

## Test Structure

Each test file follows this pattern:
1. **Setup**: Load test data, create analyzer instance
2. **Execute**: Run pattern detection
3. **Verify**: Check results in specialized tables
4. **Cleanup**: Clean pattern tables for next test

## Test Coverage

- ✅ Pattern detection logic
- ✅ Deduplication
- ✅ Correct table storage
- ✅ Property calculations (volume, length, etc.)
- ✅ Backward compatibility via view

## Fixtures

### Session-scoped Fixtures
- `load_env`: Loads environment variables from .env
- `source_clickhouse_client`: Client for production ClickHouse (data source)
- `test_clickhouse_client`: Client for test ClickHouse (isolated testing)
- `setup_test_schema`: Initializes test database schema
- `test_data_context`: Provides test parameters (network, dates, window)
- `load_test_transfers`: Loads transfer data for testing

### Function-scoped Fixtures
- `clean_pattern_tables`: Truncates pattern tables before each test
- `analyzer`: Creates StructuralPatternAnalyzer instance (in specific test classes)

## TODO

- [ ] Implement test data loading from source ClickHouse
- [ ] Add detailed assertions for each test
- [ ] Add edge case tests (empty graphs, single nodes, etc.)
- [ ] Add performance benchmarks
- [ ] Add data validation tests
- [ ] Test error handling and recovery
- [ ] Test pattern view backward compatibility