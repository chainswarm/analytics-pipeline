# Pattern Detection Unit Tests

Fast, isolated unit tests for pattern detection algorithms using mocks and synthetic data.

## Overview

These are **unit tests**, not integration tests. They:
- ✅ Use mock repositories (no database required)
- ✅ Generate synthetic graph data
- ✅ Run in <5 seconds total
- ✅ No external dependencies
- ✅ Can run on any machine without setup

## Running Tests

### Run All Unit Tests
```bash
pytest tests/unit/pattern_detection/ -v
```

### Run Specific Pattern
```bash
pytest tests/unit/pattern_detection/test_cycle_detection.py -v
pytest tests/unit/pattern_detection/test_motif_detection.py -v
```

## Test Coverage

All 7 pattern detection algorithms:
- ✅ Cycle Detection (3-64 node cycles)
- ✅ Motif Detection (fan-in/fan-out, 5-50 participants)
- ✅ Layering Detection (3-8 hop paths)
- ✅ Threshold Detection (multiple thresholds)
- ✅ Proximity Detection (1-6 hops from risk)
- ✅ Network Detection (SCCs 3-30 nodes, smurfing)
- ✅ Burst Detection (interface documented)

## What These Tests Validate

1. **Algorithm Logic** - Pattern detection algorithms work correctly
2. **Property Calculations** - Volumes, lengths, scores computed accurately
3. **Deduplication** - Pattern hashes are consistent
4. **Data Structures** - All required fields present
5. **Edge Cases** - Handling of noise, small graphs, etc.

## vs. Integration Tests

| Aspect | Unit Tests (Here) | Integration Tests |
|--------|------------------|-------------------|
| Speed | Very Fast (<5s) | Slower (DB queries) |
| Dependencies | None | ClickHouse required |
| Data | Synthetic graphs | Real transaction data |
| Purpose | Algorithm correctness | End-to-end validation |
| Run When | Every commit | Before deployment |

## CI/CD

These tests run automatically on every commit via GitHub Actions:
- No external services required
- Fast feedback loop
- Must pass before integration tests run

See [`.github/workflows/ci.yml`](../../../.github/workflows/ci.yml)