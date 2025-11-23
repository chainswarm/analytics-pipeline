# AddressFeatureAnalyzer Test Strategy

## Analysis of `packages/analyzers/features/address_feature_analyzer.py`

### Component Breakdown

The `AddressFeatureAnalyzer` has **mixed testability** - some parts are pure logic (unit-testable), others require databases (integration-testable).

## 🔍 Testing Classification

### ✅ **Unit Testable** (Pure Logic - No DB Required)

#### 1. Mathematical Calculations
```python
# These are pure functions - UNIT TESTS
_calculate_entropy(values: List[int]) -> float
_calculate_gini_coefficient(values: List[float]) -> float
_calculate_shannon_entropy(probabilities: List[float]) -> float
_safe_skewness(values: List[float]) -> float
_safe_kurtosis(values: List[float]) -> float
```

**Test Strategy**: 
- Mock input data (lists of numbers)
- Verify formulas with known inputs/outputs
- Edge cases: empty lists, single values, zeros

#### 2. Feature Computation (With Mock Data)
```python
# Can be unit tested with synthetic flows - UNIT TESTS
_compute_flow_features_cached(address, flows) -> Dict
_extract_directional_flow_features_cached(address, flows) -> Dict
_extract_intraday_features_from_aggregates(hourly_volumes, hourly_activity) -> Dict
_compute_temporal_features_from_aggregates(address, patterns, summaries) -> Dict
_get_base_features_cached(address, flows, patterns, summaries) -> Dict
```

**Test Strategy**:
- Create synthetic flow dictionaries
- No DB, just list of dicts
- Verify feature calculations
- Test edge cases (empty flows, zero volumes)

#### 3. Graph Operations (With Mock Graphs)
```python
# NetworkX operations - UNIT TESTS with synthetic graphs
_build_graph_from_flows_data(flows) -> nx.DiGraph
_extract_addresses_from_flows(flows) -> List[str]
_build_flows_index_from_graph(G) -> Dict
_add_node_attributes_to_graph(G) -> None
```

**Test Strategy**:
- Create small NetworkX graphs
- Test with 5-10 node synthetic graphs
- Verify graph structure, not DB queries

#### 4. Graph Algorithms (Wrapped NetworkX)
```python
# Pure graph algorithms - UNIT TESTS with mock graphs
_compute_pagerank(G) -> Dict[str, float]
_compute_betweenness_centrality(G) -> Dict[str, float]
_compute_closeness_centrality(G) -> Dict[str, float]
_compute_kcore(G) -> Dict[str, int]
_compute_clustering_coefficient(G) -> Dict[str, float]
_compute_khop_features(G, max_k=3) -> Dict
```

**Test Strategy**:
- Small synthetic graphs (5-10 nodes)
- Verify algorithm results match expected NetworkX behavior
- Test edge cases (disconnected graphs, single nodes)

### ⚠️ **Mixed Testability** (Can Be Either)

#### 5. Community Detection
```python
# Uses external library but can be unit tested - UNIT + INTEGRATION
_compute_community_detection(G) -> Dict[str, int]
```

**Unit Test**: With small synthetic graphs
**Integration Test**: With real large graphs from DB to verify performance

#### 6. Graph Analytics Orchestration
```python
# Combines all graph algorithms - MIXED
_compute_all_graph_algorithms(G) -> Dict[str, Dict]
```

**Unit Test**: Mock all individual algorithm calls
**Integration Test**: Run with real graph from DB

### ❌ **Integration Only** (Requires Database)

#### 7. Main Analysis Flow
```python
# Heavy DB dependencies - INTEGRATION TESTS ONLY
analyze_address_features(batch_size, chunk_size) -> None
```

**Why Integration**:
- Calls 10+ repository methods
- Requires real ClickHouse connection
- Loads actual transfer data
- Inserts features into DB
- Tests END-TO-END flow

**Dependencies**:
- TransferRepository (4 methods)
- TransferAggregationRepository (4 methods)
- MoneyFlowsRepository (1 method)
- FeatureRepository (1 method)

## 📊 **Recommended Test Structure**

```
tests/
├── unit/
│   └── analyzers/
│       └── features/
│           ├── test_calculations.py          # Math functions
│           ├── test_feature_computation.py   # With mock flows
│           ├── test_graph_operations.py      # With mock graphs
│           └── test_graph_algorithms.py      # NetworkX wrappers
│
└── integration/
    └── analyzers/
        └── features/
            ├── test_full_pipeline.py         # analyze_address_features()
            └── test_with_real_data.py        # Using real DB data
```

## 🎯 **Test Coverage Matrix**

| Component | Type | Dependencies | Test Location |
|-----------|------|--------------|---------------|
| Math functions | Unit | None | `tests/unit/` |
| Feature computation | Unit | Synthetic data | `tests/unit/` |
| Graph building | Unit | Synthetic flows | `tests/unit/` |
| Graph algorithms | Unit | Mock graphs | `tests/unit/` |
| Community detection | Both | Graph (real/mock) | Both |
| All graph analytics | Both | Full graph | Both |
| **Main pipeline** | Integration | **4 repositories + DB** | `tests/integration/` |

## 📝 **Implementation Plan**

### **Unit Tests** (~400 lines, fast)

```python
# tests/unit/analyzers/features/test_calculations.py
def test_calculate_gini_coefficient():
    """Test Gini with known distributions."""
    # Equal distribution -> Gini = 0
    assert analyzer._calculate_gini_coefficient([1, 1, 1, 1]) == 0.0
    
    # Perfect inequality -> Gini near 1
    result = analyzer._calculate_gini_coefficient([0, 0, 0, 100])
    assert result > 0.9

# tests/unit/analyzers/features/test_feature_computation.py  
def test_compute_flow_features_with_synthetic_flows():
    """Test flow features with known flow data."""
    flows = [
        {'from_address': 'A', 'to_address': 'B', 'amount_usd_sum': 10000, 'tx_count': 1},
        {'from_address': 'B', 'to_address': 'C', 'amount_usd_sum': 20000, 'tx_count': 1},
    ]
    
    features = analyzer._compute_flow_features_cached('B', flows)
    
    assert 'flow_concentration' in features
    assert features['flow_concentration'] >= 0
    assert features['flow_concentration'] <= 1

# tests/unit/analyzers/features/test_graph_algorithms.py
def test_pagerank_on_synthetic_graph():
    """Test PageRank calculation."""
    G = nx.DiGraph()
    G.add_edge('A', 'B', weight=1.0)
    G.add_edge('B', 'C', weight=1.0)
    G.add_edge('C', 'A', weight=1.0)
    
    scores = analyzer._compute_pagerank(G)
    
    # All nodes in cycle should have equal PageRank
    assert len(scores) == 3
    assert abs(scores['A'] - scores['B']) < 0.01
    assert abs(scores['B'] - scores['C']) < 0.01
```

### **Integration Tests** (~200 lines, requires DB)

```python
# tests/integration/analyzers/features/test_full_pipeline.py
def test_analyze_address_features_end_to_end(
    test_clickhouse_client,
    setup_test_schema
):
    """Test full feature analysis pipeline with real DB."""
    # Setup real repositories
    transfer_repo = TransferRepository(test_clickhouse_client)
    money_flows_repo = MoneyFlowsRepository(test_clickhouse_client)
    feature_repo = FeatureRepository(test_clickhouse_client)
    # ... etc
    
    # Load test data into DB
    # ...
    
    analyzer = AddressFeatureAnalyzer(...)
    
    # Run full pipeline
    analyzer.analyze_address_features()
    
    # Verify features were inserted
    result = feature_repo.get_features(...)
    assert len(result) > 0
```

## 🎯 **Test Count Estimate**

### Unit Tests (Fast, No DB)
- ✅ **Mathematical functions**: ~15 tests
- ✅ **Feature computations**: ~25 tests  
- ✅ **Graph operations**: ~10 tests
- ✅ **Graph algorithms**: ~20 tests
- **Total**: ~70 unit tests, <5s runtime

### Integration Tests (With ClickHouse)
- ✅ **Full pipeline**: ~5 tests
- ✅ **With real data**: ~5 tests
- ✅ **Error handling**: ~5 tests
- **Total**: ~15 integration tests, ~30s runtime

## 💡 **Key Insights**

### What Makes AddressFeatureAnalyzer Special

Unlike pattern detection (which we can fully unit test with mocks):

1. **Graph Algorithms Need Real Testing**
   - PageRank, community detection are complex
   - Need to verify with known graph structures
   - But can use small synthetic graphs (unit tests)

2. **Multiple Repository Dependencies**
   - 4 different repositories
   - 10+ database queries
   - Can't practically mock all of them (integration tests)

3. **Heavy Data Processing**
   - Chunks of 10,000 addresses
   - Batch inserts
   - Needs real DB for performance testing

## ✅ **Recommended Approach: 80/20 Split**

- **80% Unit Tests** (70 tests)
  - All math functions
  - All feature computations with synthetic data
  - All graph algorithms with small graphs
  - Fast, no dependencies
  
- **20% Integration Tests** (15 tests)
  - Full pipeline execution
  - Real data loading
  - Performance validation
  - Database storage

## 🚀 **Implementation Priority**

1. **High Priority - Unit Tests** (Now)
   - Math functions (critical, pure logic)
   - Feature computations (core algorithms)
   - Graph operations (foundational)

2. **Medium Priority - Integration Tests** (After DB tests working)
   - Full pipeline with test data
   - Storage verification
   - Error handling

3. **Low Priority - Performance Tests** (Future)
   - Large graph benchmarking
   - Memory profiling
   - Scalability testing

## 📂 **Proposed File Structure**

```
tests/
├── unit/
│   └── analyzers/
│       └── features/
│           ├── __init__.py
│           ├── conftest.py
│           ├── test_math_functions.py       # Pure calculations
│           ├── test_feature_computation.py  # With synthetic flows
│           ├── test_graph_building.py       # Graph construction
│           ├── test_graph_algorithms.py     # NetworkX wrappers
│           └── test_temporal_features.py    # Time-based features
│
└── integration/
    └── analyzers/
        └── features/
            ├── __init__.py
            ├── test_full_pipeline.py        # End-to-end with DB
            ├── test_batch_processing.py     # Chunking/batching
            └── test_data_quality.py         # Real data validation
```

## 🎓 **Summary**

**AddressFeatureAnalyzer is MIXED**:

- **70% Unit Testable** - Math, feature computation, graph algorithms
- **30% Integration Testable** - Full pipeline, DB queries, storage

**Best Strategy**:
1. Write comprehensive unit tests for all pure logic
2. Write focused integration tests for DB interactions
3. Use synthetic data whenever possible
4. Reserve real data for critical E2E validation

This gives you fast feedback (unit) + confidence (integration) without excessive test infrastructure.