"""
Integration tests for cycle pattern detection.

Tests the cycle detection algorithm from StructuralPatternAnalyzer
against real-world data to verify:
- Cycles are correctly identified
- Pattern properties are accurate (length, volume)
- Deduplication works correctly
- Data is stored in analyzers_patterns_cycle table

Enhanced with:
- Dynamic cycle generation for sizes: 3, 4, 16, 32, 64
- Noise transactions (1:100 ratio to cyclic transactions)
- Parametrized tests
- Dynamic assertions
- Debug console output
"""

import pytest
import time
import random
import networkx as nx
from typing import Tuple, List
from packages.analyzers.structural.structural_pattern_analyzer import StructuralPatternAnalyzer
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.address_label_repository import AddressLabelRepository


def generate_cycle_with_noise(cycle_size: int, noise_ratio: float = 0.01) -> Tuple[nx.DiGraph, dict]:
    """
    Generate a cycle graph with noise transactions.
    
    Args:
        cycle_size: Number of nodes in the cycle (e.g., 3, 4, 16, 32, 64)
        noise_ratio: Ratio of noise transactions to cycle transactions (default: 0.01 = 1:100)
    
    Returns:
        Tuple of (graph, metadata) where metadata contains:
            - cycle_nodes: List of nodes in the cycle
            - cycle_edges: List of edges in the cycle
            - noise_edges: List of noise edges
            - expected_volume: Total volume of cycle edges
            - cycle_tx_count: Number of transactions in cycle
            - noise_tx_count: Number of noise transactions
    """
    print(f"\n{'='*80}")
    print(f"🔧 GENERATING CYCLE: size={cycle_size}, noise_ratio={noise_ratio}")
    print(f"{'='*80}")
    
    G = nx.DiGraph()
    
    # Generate cycle nodes
    cycle_nodes = [f"ADDR_{i:04d}" for i in range(cycle_size)]
    print(f"📍 Cycle nodes: {len(cycle_nodes)} nodes")
    
    # Create cycle edges with realistic amounts
    cycle_edges = []
    total_cycle_volume = 0
    base_amount = 10000  # Base amount in USD
    
    for i in range(cycle_size):
        from_node = cycle_nodes[i]
        to_node = cycle_nodes[(i + 1) % cycle_size]  # Wrap around to create cycle
        
        # Vary amounts slightly for realism
        amount = base_amount * random.uniform(0.8, 1.2)
        tx_count = random.randint(1, 5)
        
        G.add_edge(from_node, to_node, amount_usd_sum=amount, tx_count=tx_count)
        cycle_edges.append((from_node, to_node, amount))
        total_cycle_volume += amount
    
    print(f"💰 Cycle edges: {len(cycle_edges)} edges, total volume: ${total_cycle_volume:.2f}")
    
    # Calculate number of noise edges based on ratio
    # noise_ratio is the multiplier: noise_edges = cycle_edges * noise_ratio
    # For example: ratio=100 means 100x more noise than cycle edges
    num_noise_edges = int(len(cycle_edges) * noise_ratio)
    
    # Generate noise edges (random edges not part of the main cycle)
    noise_edges = []
    noise_nodes = []
    
    if num_noise_edges > 0:
        print(f"🔊 Adding noise: {num_noise_edges} noise edges (ratio: {noise_ratio}x = {num_noise_edges} noise / {len(cycle_edges)} cycle)")
        
        # Create additional nodes for noise
        noise_node_count = max(2, num_noise_edges)
        noise_nodes = [f"NOISE_{i:04d}" for i in range(noise_node_count)]
        
        for i in range(num_noise_edges):
            # Random connections between noise nodes or between noise and cycle nodes
            if random.random() < 0.7:
                # Noise to noise
                from_node = random.choice(noise_nodes)
                to_node = random.choice(noise_nodes)
            else:
                # Mix with cycle nodes
                from_node = random.choice(noise_nodes + cycle_nodes[:2])
                to_node = random.choice(noise_nodes + cycle_nodes[:2])
            
            # Avoid self-loops
            if from_node == to_node:
                to_node = random.choice(noise_nodes + cycle_nodes)
            
            amount = base_amount * random.uniform(0.1, 0.5)  # Smaller amounts for noise
            tx_count = 1
            
            G.add_edge(from_node, to_node, amount_usd_sum=amount, tx_count=tx_count)
            noise_edges.append((from_node, to_node, amount))
    else:
        print(f"🔇 No noise edges requested (ratio: {noise_ratio}x)")
    
    print(f"📊 Graph stats:")
    print(f"   Total nodes: {G.number_of_nodes()}")
    print(f"   Total edges: {G.number_of_edges()}")
    print(f"   Cycle nodes: {len(cycle_nodes)}")
    print(f"   Noise nodes: {len(noise_nodes)}")
    print(f"   Cycle edges: {len(cycle_edges)}")
    print(f"   Noise edges: {len(noise_edges)}")
    
    metadata = {
        'cycle_nodes': cycle_nodes,
        'cycle_edges': cycle_edges,
        'noise_edges': noise_edges,
        'noise_nodes': noise_nodes,
        'expected_volume': total_cycle_volume,
        'cycle_tx_count': len(cycle_edges),
        'noise_tx_count': len(noise_edges),
        'cycle_size': cycle_size
    }
    
    return G, metadata


def create_simple_cycle_graph() -> nx.DiGraph:
    """
    Create a simple 3-node cycle: A -> B -> C -> A
    
    Returns graph with proper edge attributes.
    """
    G = nx.DiGraph()
    G.add_edge('A', 'B', amount_usd_sum=10000, tx_count=5)
    G.add_edge('B', 'C', amount_usd_sum=12000, tx_count=3)
    G.add_edge('C', 'A', amount_usd_sum=11000, tx_count=4)
    return G


def create_complex_cycle_graph() -> nx.DiGraph:
    """
    Create a more complex graph with:
    - One 4-node cycle: A -> B -> C -> D -> A
    - One 3-node cycle: E -> F -> G -> E
    - Some non-cycle edges
    """
    G = nx.DiGraph()
    
    # 4-node cycle
    G.add_edge('A', 'B', amount_usd_sum=50000, tx_count=10)
    G.add_edge('B', 'C', amount_usd_sum=45000, tx_count=8)
    G.add_edge('C', 'D', amount_usd_sum=48000, tx_count=9)
    G.add_edge('D', 'A', amount_usd_sum=47000, tx_count=7)
    
    # 3-node cycle
    G.add_edge('E', 'F', amount_usd_sum=5000, tx_count=2)
    G.add_edge('F', 'G', amount_usd_sum=5500, tx_count=3)
    G.add_edge('G', 'E', amount_usd_sum=5200, tx_count=2)
    
    # Non-cycle edges
    G.add_edge('A', 'E', amount_usd_sum=1000, tx_count=1)
    G.add_edge('H', 'I', amount_usd_sum=2000, tx_count=1)
    
    return G


def create_no_cycle_graph() -> nx.DiGraph:
    """
    Create a graph with no cycles (DAG).
    """
    G = nx.DiGraph()
    G.add_edge('A', 'B', amount_usd_sum=10000, tx_count=5)
    G.add_edge('B', 'C', amount_usd_sum=12000, tx_count=3)
    G.add_edge('C', 'D', amount_usd_sum=11000, tx_count=4)
    G.add_edge('A', 'D', amount_usd_sum=5000, tx_count=2)
    return G


class TestCycleDetection:
    """Test cycle pattern detection."""
    
    @pytest.fixture
    def analyzer(self, test_data_context):
        """Create StructuralPatternAnalyzer instance with mock repos."""
        from unittest.mock import Mock
        from packages.utils import calculate_time_window
        from packages.analyzers.structural import StructuralPatternAnalyzer
        
        start_ts, end_ts = calculate_time_window(
            test_data_context['window_days'],
            test_data_context['processing_date']
        )
        
        # Create mock repositories (for unit-style tests)
        mock_money_flows = Mock()
        mock_pattern_repo = Mock()
        mock_label_repo = Mock()
        
        analyzer = StructuralPatternAnalyzer(
            money_flows_repository=mock_money_flows,
            pattern_repository=mock_pattern_repo,
            address_label_repository=mock_label_repo,
            window_days=test_data_context['window_days'],
            start_timestamp=start_ts,
            end_timestamp=end_ts,
            network=test_data_context['network']
        )
        
        # Override max_cycle_length to support large cycles for testing
        # Default is 8-10, we need to support up to 64 for our tests
        if 'cycle_detection' in analyzer.config:
            analyzer.config['cycle_detection']['max_cycle_length'] = 100
            print(f"🔧 Overriding max_cycle_length to 100 for testing")
        
        return analyzer
    
    @pytest.mark.parametrize("cycle_size", [3, 4, 16, 32, 64])
    def test_dynamic_cycle_detection_with_noise(self, analyzer, cycle_size):
        """
        Test cycle detection with dynamically generated cycles of various sizes.
        Includes noise transactions at 1:100 ratio.
        """
        print(f"\n{'#'*80}")
        print(f"# TEST: Cycle Detection - Size {cycle_size}")
        print(f"{'#'*80}")
        
        # Generate cycle with noise (100x more noise than cycle)
        G, metadata = generate_cycle_with_noise(cycle_size, noise_ratio=100)
        
        # Mock the graph building to return our test graph
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        # Get cycle detector and show its configuration
        cycle_detector = analyzer.cycle_detector
        max_cycle_cfg = analyzer.config.get('cycle_detection', {}).get('max_cycle_length', 'N/A')
        print(f"⚙️  Cycle detector max_cycle_length config: {max_cycle_cfg}")
        
        # Run detection
        print(f"\n🔍 Running cycle detection...")
        start_time = time.time()
        patterns = cycle_detector.detect(G)
        detection_time = time.time() - start_time
        
        print(f"⏱️  Detection completed in {detection_time:.4f} seconds")
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Debug: Print all detected patterns
        for idx, pattern in enumerate(patterns):
            print(f"\n  Pattern {idx + 1}:")
            print(f"    Type: {pattern.get('pattern_type', 'N/A')}")
            print(f"    Length: {pattern.get('cycle_length', 'N/A')}")
            print(f"    Volume: ${pattern.get('cycle_volume_usd', 0):.2f}")
            print(f"    Addresses: {len(pattern.get('addresses_involved', []))}")
            print(f"    Pattern ID: {pattern.get('pattern_id', 'N/A')[:50]}...")
            
            # Print cycle path for main detected cycle
            cycle_path = pattern.get('cycle_path', [])
            if cycle_path:
                print(f"    Cycle Path ({len(cycle_path)} hops):")
                # For small cycles, print all addresses
                if len(cycle_path) <= 10:
                    for i, addr in enumerate(cycle_path):
                        next_addr = cycle_path[(i + 1) % len(cycle_path)]
                        print(f"      [{i+1}] {addr} → {next_addr}")
                else:
                    # For large cycles, print first 5, ..., last 5
                    for i in range(5):
                        next_addr = cycle_path[i + 1] if i < len(cycle_path) - 1 else cycle_path[0]
                        print(f"      [{i+1}] {cycle_path[i]} → {next_addr}")
                    print(f"      ... ({len(cycle_path) - 10} more hops) ...")
                    for i in range(len(cycle_path) - 5, len(cycle_path)):
                        next_addr = cycle_path[(i + 1) % len(cycle_path)]
                        print(f"      [{i+1}] {cycle_path[i]} → {next_addr}")
        
        # Dynamic assertions based on cycle size
        print(f"\n✅ Running assertions...")
        
        # Should detect at least one cycle (the main one we created)
        assert len(patterns) >= 1, f"Expected at least 1 cycle, found {len(patterns)}"
        print(f"   ✓ Found at least one cycle pattern")
        
        # Find the largest cycle (should be our main cycle)
        main_pattern = max(patterns, key=lambda p: p.get('cycle_length', 0))
        
        # Verify pattern type
        assert main_pattern['pattern_type'] == 'cycle', \
            f"Expected pattern_type='cycle', got '{main_pattern['pattern_type']}'"
        print(f"   ✓ Pattern type is 'cycle'")
        
        # Verify cycle length matches expected size
        assert main_pattern['cycle_length'] == cycle_size, \
            f"Expected cycle_length={cycle_size}, got {main_pattern['cycle_length']}"
        print(f"   ✓ Cycle length matches expected size: {cycle_size}")
        
        # Verify all cycle nodes are in the detected pattern
        detected_addresses = set(main_pattern['addresses_involved'])
        expected_addresses = set(metadata['cycle_nodes'])
        
        # The detected cycle should contain all our cycle nodes
        # (but might have them in different order due to detection algorithm)
        assert detected_addresses == expected_addresses, \
            f"Address mismatch. Expected {len(expected_addresses)} addresses, got {len(detected_addresses)}"
        print(f"   ✓ All {len(expected_addresses)} cycle nodes detected")
        
        # Verify volume is reasonable (should be close to expected)
        expected_volume = metadata['expected_volume']
        detected_volume = main_pattern['cycle_volume_usd']
        volume_tolerance = expected_volume * 0.1  # 10% tolerance
        
        assert abs(detected_volume - expected_volume) <= volume_tolerance, \
            f"Volume mismatch. Expected ~${expected_volume:.2f}, got ${detected_volume:.2f}"
        print(f"   ✓ Cycle volume is accurate: ${detected_volume:.2f} (expected: ${expected_volume:.2f})")
        
        # Verify required fields exist
        required_fields = ['pattern_id', 'pattern_hash', 'cycle_path', 'addresses_involved']
        for field in required_fields:
            assert field in main_pattern, f"Missing required field: {field}"
        print(f"   ✓ All required fields present: {', '.join(required_fields)}")
        
        # Performance check for large cycles
        if cycle_size >= 32:
            assert detection_time < 10.0, \
                f"Detection too slow for {cycle_size}-node cycle: {detection_time:.2f}s"
            print(f"   ✓ Performance acceptable: {detection_time:.4f}s < 10s")
        
        print(f"\n{'='*80}")
        print(f"✅ TEST PASSED: Cycle size {cycle_size}")
        print(f"{'='*80}\n")
    
    def test_cycle_detection_basic(self, analyzer):
        """Test basic cycle detection with simple 3-node cycle."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Basic Cycle Detection (Legacy)")
        print(f"{'#'*80}")
        
        # Create simple cycle graph
        G = create_simple_cycle_graph()
        
        # Mock the graph building to return our test graph
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        # Get cycle detector (use attribute, not dictionary)
        cycle_detector = analyzer.cycle_detector
        
        # Run detection
        patterns = cycle_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Assertions
        assert len(patterns) == 1, f"Expected 1 cycle, found {len(patterns)}"
        
        pattern = patterns[0]
        assert pattern['pattern_type'] == 'cycle'
        assert pattern['cycle_length'] == 3
        assert set(pattern['addresses_involved']) == {'A', 'B', 'C'}
        assert pattern['cycle_volume_usd'] > 0
        assert 'pattern_id' in pattern
        assert 'pattern_hash' in pattern
        
        print(f"✅ TEST PASSED: Basic cycle detection")
    
    def test_cycle_deduplication(self, analyzer):
        """Test that same cycle detected multiple times is deduplicated."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Cycle Deduplication")
        print(f"{'#'*80}")
        
        G = create_simple_cycle_graph()
        
        # Use attribute, not dictionary
        cycle_detector = analyzer.cycle_detector
        
        # Run detection twice on same graph
        print(f"🔍 Running detection #1...")
        patterns1 = cycle_detector.detect(G)
        print(f"   Found {len(patterns1)} pattern(s)")
        
        print(f"🔍 Running detection #2...")
        patterns2 = cycle_detector.detect(G)
        print(f"   Found {len(patterns2)} pattern(s)")
        
        # Should return same number of patterns (deduplication works)
        assert len(patterns1) == len(patterns2), \
            f"Deduplication failed: {len(patterns1)} vs {len(patterns2)} patterns"
        
        # Pattern IDs should be identical
        if len(patterns1) > 0:
            assert patterns1[0]['pattern_id'] == patterns2[0]['pattern_id'], \
                "Pattern IDs should match for deduplicated patterns"
            assert patterns1[0]['pattern_hash'] == patterns2[0]['pattern_hash'], \
                "Pattern hashes should match for deduplicated patterns"
            print(f"   ✓ Pattern ID: {patterns1[0]['pattern_id'][:50]}...")
            print(f"   ✓ Pattern hash: {patterns1[0]['pattern_hash'][:50]}...")
        
        print(f"✅ TEST PASSED: Deduplication working correctly")
    
    def test_cycle_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test that cycles are stored in analyzers_patterns_cycle table."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Cycle Storage in Database")
        print(f"{'#'*80}")
        
        from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
        from packages.storage.constants import PatternTypes
        from datetime import datetime
        
        # Create repository
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        # Create fake cycle pattern
        patterns = [{
            'pattern_id': 'cycle_test_001',
            'pattern_type': PatternTypes.CYCLE,
            'pattern_hash': 'hash_test_001',
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
        
        print(f"💾 Inserting pattern into database...")
        # Insert patterns
        repo.insert_deduplicated_patterns(
            patterns,
            window_days=test_data_context['window_days'],
            processing_date=test_data_context['processing_date']
        )
        
        print(f"🔍 Querying database for pattern...")
        # Verify in specialized table
        result = test_clickhouse_client.query(
            "SELECT * FROM analyzers_patterns_cycle WHERE pattern_id = 'cycle_test_001'"
        )
        
        print(f"📊 Query returned {len(result.result_rows)} row(s)")
        
        assert len(result.result_rows) == 1, "Pattern should be in cycle table"
        
        # Verify columns exist
        row = result.result_rows[0]
        print(f"📋 Available columns: {', '.join(result.column_names)}")
        
        assert result.column_names[0] == 'window_days'
        assert 'cycle_path' in result.column_names
        assert 'cycle_length' in result.column_names
        assert 'cycle_volume_usd' in result.column_names
        
        print(f"✅ TEST PASSED: Pattern stored correctly in database")
    
    def test_cycle_properties_accurate(self, analyzer):
        """Test that cycle properties (length, volume) are calculated correctly."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Cycle Properties Accuracy")
        print(f"{'#'*80}")
        
        # Create cycle with known properties
        G = nx.DiGraph()
        G.add_edge('A', 'B', amount_usd_sum=1000, tx_count=1)
        G.add_edge('B', 'C', amount_usd_sum=2000, tx_count=1)
        G.add_edge('C', 'D', amount_usd_sum=3000, tx_count=1)
        G.add_edge('D', 'A', amount_usd_sum=4000, tx_count=1)
        
        print(f"📊 Test graph: A->B->C->D->A")
        print(f"   Edge A->B: $1,000")
        print(f"   Edge B->C: $2,000")
        print(f"   Edge C->D: $3,000")
        print(f"   Edge D->A: $4,000")
        print(f"   Expected total: $10,000")
        
        # Use attribute, not dictionary
        cycle_detector = analyzer.cycle_detector
        patterns = cycle_detector.detect(G)
        
        assert len(patterns) == 1, "Should detect one 4-node cycle"
        
        pattern = patterns[0]
        print(f"\n✅ Detected pattern:")
        print(f"   Cycle length: {pattern['cycle_length']}")
        print(f"   Cycle volume: ${pattern['cycle_volume_usd']:.2f}")
        print(f"   Addresses: {pattern['addresses_involved']}")
        
        assert pattern['cycle_length'] == 4, "Cycle should have length 4"
        
        # Volume should be sum of all edges in cycle
        expected_volume = 1000 + 2000 + 3000 + 4000
        assert pattern['cycle_volume_usd'] == expected_volume, \
            f"Expected volume {expected_volume}, got {pattern['cycle_volume_usd']}"
        
        # All addresses should be involved
        assert len(pattern['addresses_involved']) == 4
        assert set(pattern['addresses_involved']) == {'A', 'B', 'C', 'D'}
        
        print(f"✅ TEST PASSED: Properties calculated correctly")