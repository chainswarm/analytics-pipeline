# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-11-23

### Added
- Comprehensive pattern detection test suite (76 tests total)
  - Cycle detection tests (3-64 node cycles, 100x noise robustness)
  - Motif detection tests (fan-in/fan-out, 5-50 participants)
  - Layering detection tests (3-8 hop paths)
  - Threshold detection tests (multiple threshold levels)
  - Proximity detection tests (1-6 hops from risk sources)
  - Network detection tests (SCC and smurfing networks, 3-30 nodes)
  - Burst detection tests (interface documentation)

- Test infrastructure reorganization
  - Separated unit tests (69 tests, <5s, no dependencies) from integration tests (7 tests, ~10s, requires ClickHouse)
  - Created proper conftest.py fixtures for both test types
  - Moved docker-compose.yml to tests/integration/ level

- GitHub Actions CI/CD pipelines
  - CI workflow: Runs tests on every push/PR
  - Release workflow: Manual trigger with versioning, creates GitHub releases
  - ClickHouse integration via service containers
  - Multi-platform Docker builds (amd64/arm64)
  - Auto-publish to GitHub Container Registry (ghcr.io)
  - Optional Docker Hub publishing

- Documentation
  - Test migration guide explaining unit vs integration tests
  - Feature analyzer test strategy (70% unit, 30% integration)
  - Comprehensive workflow documentation
  - Test READMEs for both unit and integration tests

### Changed
- Reorganized tests from tests/integration/pattern_detection/ to tests/unit/pattern_detection/
- Updated all test docstrings to reflect unit test nature
- Improved test assertions to handle real detector behavior (volume filtering, community detection)

### Fixed
- Layering detection tests now handle 90th percentile volume filtering
- Network detection tests handle greedy modularity community splits
- Proximity detection tests are schema-aware (no non-existent columns)
- All unit tests removed database dependencies

## Future Releases

### Planned for [1.1.0]
- Feature analyzer unit tests (~70 tests)
- Feature analyzer integration tests (~15 tests)
- End-to-end pipeline integration tests with real data
- Performance benchmarking suite

### Planned for [1.2.0]
- Burst detection full implementation with timestamp data
- Cross-pattern correlation analysis
- Real-time pattern detection monitoring