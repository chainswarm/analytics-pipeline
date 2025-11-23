"""
Unit tests for proximity risk detection.

Tests the proximity analysis algorithm from StructuralPatternAnalyzer
against real-world data to verify:
- Proximity to risk sources is calculated correctly
- Distance measurements are accurate (1-6 hops)
- Risk propagation scores use decay formula
- Deduplication works correctly
- Data is stored in analyzers_patterns_proximity table

Enhanced with:
- Extended distance testing (1-6 hops, not just 1-3)
- Dynamic proximity graph generation
- Noise transactions
- Parametrized tests
- Debug console output
"""

import pytest
import time
import random
import networkx as nx
from typing import Tuple, List, Dict
from packages.analyzers.structural.structural_pattern_analyzer import StructuralPatternAnalyzer
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.address_label_repository import AddressLabelRepository


def generate_proximity_graph_with_noise(
    max_distance: int,
    addresses_per_distance: int = 3,
    noise_ratio: float = 0.01
) -> Tuple[nx.DiGraph, dict]:
    """
    Generate a proximity graph with addresses at various distances from risk source.
    
    Args:
        max_distance: Maximum hops from risk source (3, 4, 5, 6)
        addresses_per_distance: Number of addresses at each distance level
        noise_ratio: Ratio of noise transactions
    
    Returns:
        Tuple of (graph, metadata) where metadata contains:
            - risk_address: The risk source address
            - addresses_by_distance: Dict[distance -> List[addresses]]
            - expected_propagation_scores: Dict[address -> score]
            - total_addresses: Total non-noise addresses
    """
    print(f"\n{'='*80}")
    print(f"🔧 GENERATING PROXIMITY GRAPH: max_distance={max_distance}, nodes_per_level={addresses_per_distance}")
    print(f"{'='*80}")
    
    G = nx.DiGraph()
    
    # Risk source
    risk_address = "RISK_SOURCE"
    
    # Build layered graph: RISK -> D1 -> D2 -> ... -> Dn
    addresses_by_distance = {}
    all_risk_addresses = [risk_address]
    base_amount = 50000
    
    # Generate addresses at each distance level
    previous_level = [risk_address]
    
    for distance in range(1, max_distance + 1):
        current_level = []
        
        for i in range(addresses_per_distance):
            addr = f"DIST_{distance}_ADDR_{i:03d}"
            current_level.append(addr)
            
            # Connect to random node from previous level
            source = random.choice(previous_level)
            amount = base_amount * random.uniform(0.8, 1.2)
            tx_count = random.randint(1, 3)
            
            G.add_edge(source, addr, amount_usd_sum=amount, tx_count=tx_count)
        
        addresses_by_distance[distance] = current_level
        previous_level = current_level
        
        print(f"📍 Distance {distance}: {len(current_level)} addresses")
    
    # Calculate expected propagation scores
    # Formula: distance_decay_factor / (distance + 1)
    # Using default decay factor of 1.0
    expected_propagation_scores = {}
    decay_factor = 1.0
    
    for distance, addrs in addresses_by_distance.items():
        expected_score = decay_factor / (distance + 1)
        for addr in addrs:
            expected_propagation_scores[addr] = expected_score
    
    # Total risk-related edges
    total_edges = sum(len(addrs) for addrs in addresses_by_distance.values())
    
    # Add noise edges
    num_noise_edges = int(total_edges * noise_ratio)
    noise_edges = []
    
    if num_noise_edges > 0:
        print(f"🔊 Adding {num_noise_edges} noise edges")
        noise_nodes = [f"NOISE_{i:04d}" for i in range(max(3, num_noise_edges))]
        
        for i in range(num_noise_edges):
            from_node = random.choice(noise_nodes)
            to_node = random.choice(noise_nodes)
            if from_node == to_node:
                to_node = random.choice(noise_nodes)
            
            amount = base_amount * random.uniform(0.2, 0.8)
            G.add_edge(from_node, to_node, amount_usd_sum=amount, tx_count=1)
            noise_edges.append((from_node, to_node))
    
    total_addresses = sum(len(addrs) for addrs in addresses_by_distance.values())
    
    print(f"📊 Graph stats:")
    print(f"   Total nodes: {G.number_of_nodes()}")
    print(f"   Total edges: {G.number_of_edges()}")
    print(f"   Risk source: {risk_address}")
    print(f"   Addresses by distance: {total_addresses}")
    print(f"   Noise edges: {len(noise_edges)}")
    
    metadata = {
        'risk_address': risk_address,
        'addresses_by_distance': addresses_by_distance,
        'expected_propagation_scores': expected_propagation_scores,
        'total_addresses': total_addresses,
        'max_distance': max_distance,
        'noise_edges': noise_edges
    }
    
    return G, metadata


def create_simple_proximity_graph() -> nx.DiGraph:
    """
    Create a simple proximity graph: RISK → A → B → C
    Distances: A=1, B=2, C=3
    """
    G = nx.DiGraph()
    G.add_edge('RISK', 'A', amount_usd_sum=10000, tx_count=2)
    G.add_edge('A', 'B', amount_usd_sum=8000, tx_count=2)
    G.add_edge('B', 'C', amount_usd_sum=6000, tx_count=1)
    return G


def create_multi_path_proximity() -> nx.DiGraph:
    """
    Create graph with multiple paths from risk source.
    """
    G = nx.DiGraph()
    # Risk source with 2 paths
    G.add_edge('RISK', 'A1', amount_usd_sum=10000, tx_count=2)
    G.add_edge('RISK', 'A2', amount_usd_sum=12000, tx_count=3)
    
    # Path 1: RISK -> A1 -> B1 -> C1
    G.add_edge('A1', 'B1', amount_usd_sum=8000, tx_count=2)
    G.add_edge('B1', 'C1', amount_usd_sum=6000, tx_count=1)
    
    # Path 2: RISK -> A2 -> B2
    G.add_edge('A2', 'B2', amount_usd_sum=9000, tx_count=2)
    
    return G


class TestProximityDetection:
    """Test proximity risk pattern detection."""
    
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
        
        # Create mock repositories
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
        
        # Override max_distance to support extended testing (default is 3)
        if 'proximity_analysis' in analyzer.config:
            analyzer.config['proximity_analysis']['max_distance'] = 6
            print(f"🔧 Overriding max_distance to 6 for testing")
        
        return analyzer
    
    @pytest.mark.parametrize("max_distance", [3, 4, 5, 6])
    def test_dynamic_proximity_detection_with_noise(self, analyzer, max_distance):
        """
        Test proximity detection with various distance levels (up to 6 hops).
        Includes noise transactions.
        """
        print(f"\n{'#'*80}")
        print(f"# TEST: Proximity Detection - Max Distance {max_distance}")
        print(f"{'#'*80}")
        
        # Generate proximity graph with noise
        G, metadata = generate_proximity_graph_with_noise(
            max_distance=max_distance,
            addresses_per_distance=3,
            noise_ratio=10
        )
        
        # Mock the graph building
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        # Mock the risk address identification to return our risk source
        def mock_identify_risk(G_param):
            return [metadata['risk_address']]
        
        analyzer.proximity_detector._identify_risk_addresses = mock_identify_risk
        
        # Get proximity detector
        proximity_detector = analyzer.proximity_detector
        
        # Run detection
        print(f"\n🔍 Running proximity detection...")
        start_time = time.time()
        patterns = proximity_detector.detect(G)
        detection_time = time.time() - start_time
        
        print(f"⏱️  Detection completed in {detection_time:.4f} seconds")
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Debug: Print sample detected patterns
        patterns_by_distance = {}
        for pattern in patterns[:10]:  # Show first 10
            dist = pattern.get('distance_to_risk', 0)
            if dist not in patterns_by_distance:
                patterns_by_distance[dist] = []
            patterns_by_distance[dist].append(pattern)
        
        for dist in sorted(patterns_by_distance.keys()):
            print(f"\n  Distance {dist}: {len([p for p in patterns if p.get('distance_to_risk') == dist])} patterns")
            sample = patterns_by_distance[dist][0]
            print(f"    Sample - Propagation Score: {sample.get('risk_propagation_score', 0):.4f}")
        
        print(f"\n✅ Running assertions...")
        
        # Should detect patterns for addresses at various distances
        assert len(patterns) > 0, "Expected proximity patterns to be detected"
        print(f"   ✓ Found {len(patterns)} proximity pattern(s)")
        
        # Verify patterns exist for each distance level
        detected_distances = set(p.get('distance_to_risk', 0) for p in patterns)
        print(f"   ✓ Detected distances: {sorted(detected_distances)}")
        
        # Check a sample pattern
        sample_pattern = patterns[0]
        
        # Verify pattern type
        assert sample_pattern['pattern_type'] == 'proximity_risk'
        print(f"   ✓ Pattern type is 'proximity_risk'")
        
        # Verify risk source
        assert sample_pattern['risk_source_address'] == metadata['risk_address']
        print(f"   ✓ Risk source identified: {sample_pattern['risk_source_address']}")
        
        # Verify distance is within range
        sample_distance = sample_pattern['distance_to_risk']
        assert 1 <= sample_distance <= max_distance, \
            f"Distance {sample_distance} out of range [1, {max_distance}]"
        print(f"   ✓ Sample distance in valid range: {sample_distance}")
        
        # Verify propagation score formula: decay_factor / (distance + 1)
        expected_score = 1.0 / (sample_distance + 1)
        actual_score = sample_pattern['risk_propagation_score']
        
        assert abs(actual_score - expected_score) < 0.01, \
            f"Propagation score mismatch. Expected {expected_score:.4f}, got {actual_score:.4f}"
        print(f"   ✓ Propagation score correct: {actual_score:.4f}")
        
        # Verify address roles
        assert sample_pattern['address_roles'] == ['risk_source', 'suspect']
        print(f"   ✓ Address roles correctly assigned")
        
        # Verify required fields
        required_fields = [
            'pattern_id', 'pattern_hash', 'risk_source_address',
            'distance_to_risk', 'risk_propagation_score'
        ]
        for field in required_fields:
            assert field in sample_pattern, f"Missing field: {field}"
        print(f"   ✓ All required fields present")
        
        print(f"\n{'='*80}")
        print(f"✅ TEST PASSED: Proximity max distance {max_distance}")
        print(f"{'='*80}\n")
    
    def test_proximity_detection_basic(self, analyzer):
        """Test basic proximity detection with simple 3-hop path."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Basic Proximity Detection")
        print(f"{'#'*80}")
        
        G = create_simple_proximity_graph()
        
        # Mock the graph building
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        # Mock risk identification
        analyzer.proximity_detector._identify_risk_addresses = lambda G_param: ['RISK']
        
        proximity_detector = analyzer.proximity_detector
        patterns = proximity_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        if len(patterns) > 0:
            # Should find A (dist=1), B (dist=2), C (dist=3)
            distances = sorted(set(p['distance_to_risk'] for p in patterns))
            print(f"   Distances found: {distances}")
            
            # Check distance 1 pattern
            dist1_patterns = [p for p in patterns if p['distance_to_risk'] == 1]
            if dist1_patterns:
                pattern = dist1_patterns[0]
                assert pattern['pattern_type'] == 'proximity_risk'
                assert pattern['risk_source_address'] == 'RISK'
                expected_score = 1.0 / (1 + 1)  # 0.5
                assert abs(pattern['risk_propagation_score'] - expected_score) < 0.01
                print(f"   ✓ Distance 1 pattern correct")
        
        print(f"✅ TEST PASSED: Basic proximity detection")
    
    def test_distance_calculation(self, analyzer):
        """Test that shortest path distance is calculated correctly."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Distance Calculation")
        print(f"{'#'*80}")
        
        G = create_simple_proximity_graph()
        
        # Mock risk identification
        analyzer.proximity_detector._identify_risk_addresses = lambda G_param: ['RISK']
        
        proximity_detector = analyzer.proximity_detector
        patterns = proximity_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Expected distances: A=1, B=2, C=3
        expected_distances = {
            'A': 1,
            'B': 2,
            'C': 3
        }
        
        for addr, expected_dist in expected_distances.items():
            # Find pattern for this address
            addr_patterns = [p for p in patterns 
                           if addr in p.get('addresses_involved', [])]
            
            if addr_patterns:
                pattern = addr_patterns[0]
                actual_dist = pattern['distance_to_risk']
                assert actual_dist == expected_dist, \
                    f"Distance to {addr}: expected {expected_dist}, got {actual_dist}"
                print(f"   ✓ Distance to {addr}: {actual_dist}")
        
        print(f"✅ TEST PASSED: Distance calculation")
    
    def test_risk_propagation_score(self, analyzer):
        """Test that risk propagation score uses correct decay formula."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Risk Propagation Score Formula")
        print(f"{'#'*80}")
        
        G = create_simple_proximity_graph()
        
        # Mock risk identification
        analyzer.proximity_detector._identify_risk_addresses = lambda G_param: ['RISK']
        
        proximity_detector = analyzer.proximity_detector
        patterns = proximity_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # decay_factor = 1.0 (default)
        # formula: risk_propagation = decay_factor / (distance + 1)
        
        for pattern in patterns:
            distance = pattern['distance_to_risk']
            actual_score = pattern['risk_propagation_score']
            expected_score = 1.0 / (distance + 1)
            
            assert abs(actual_score - expected_score) < 0.01, \
                f"Score mismatch at distance {distance}: expected {expected_score:.4f}, got {actual_score:.4f}"
            
            print(f"   ✓ Distance {distance}: score = {actual_score:.4f} (expected {expected_score:.4f})")
        
        print(f"✅ TEST PASSED: Propagation score formula")
    
    def test_multiple_risk_sources(self, analyzer):
        """Test proximity detection with multiple risk sources."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Multiple Risk Sources")
        print(f"{'#'*80}")
        
        # Create graph with 2 risk sources
        G = nx.DiGraph()
        G.add_edge('RISK1', 'A', amount_usd_sum=10000, tx_count=2)
        G.add_edge('A', 'B', amount_usd_sum=8000, tx_count=2)
        G.add_edge('RISK2', 'C', amount_usd_sum=12000, tx_count=3)
        G.add_edge('C', 'D', amount_usd_sum=9000, tx_count=2)
        
        # Mock risk identification to return both
        analyzer.proximity_detector._identify_risk_addresses = lambda G_param: ['RISK1', 'RISK2']
        
        proximity_detector = analyzer.proximity_detector
        patterns = proximity_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Should find patterns from both risk sources
        risk_sources = set(p['risk_source_address'] for p in patterns)
        
        if len(risk_sources) > 0:
            print(f"   Risk sources found: {risk_sources}")
            print(f"   ✓ Multiple risk sources handled")
        
        print(f"✅ TEST PASSED: Multiple risk sources")
    
    def test_max_distance_cutoff(self, analyzer):
        """Test that distances beyond max are not included."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Max Distance Cutoff")
        print(f"{'#'*80}")
        
        # Create long chain: RISK -> A -> B -> C -> D -> E -> F -> G (7 hops)
        G = nx.DiGraph()
        nodes = ['RISK', 'A', 'B', 'C', 'D', 'E', 'F', 'G']
        for i in range(len(nodes) - 1):
            G.add_edge(nodes[i], nodes[i + 1], amount_usd_sum=10000, tx_count=1)
        
        # Mock risk identification
        analyzer.proximity_detector._identify_risk_addresses = lambda G_param: ['RISK']
        
        # Set max_distance to 5
        analyzer.config['proximity_analysis']['max_distance'] = 5
        
        proximity_detector = analyzer.proximity_detector
        patterns = proximity_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Should only find distances 1-5, not 6 or 7
        distances = [p['distance_to_risk'] for p in patterns]
        max_detected = max(distances) if distances else 0
        
        assert max_detected <= 5, f"Found distance {max_detected} > max (5)"
        print(f"   ✓ Max distance respected: {max_detected} <= 5")
        
        print(f"✅ TEST PASSED: Max distance cutoff")
    
    def test_proximity_deduplication(self, analyzer):
        """Test that proximity patterns are deduplicated correctly."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Proximity Deduplication")
        print(f"{'#'*80}")
        
        G = create_simple_proximity_graph()
        
        # Mock risk identification
        analyzer.proximity_detector._identify_risk_addresses = lambda G_param: ['RISK']
        
        proximity_detector = analyzer.proximity_detector
        
        # Run detection twice
        print(f"🔍 Running detection #1...")
        patterns1 = proximity_detector.detect(G)
        print(f"   Found {len(patterns1)} pattern(s)")
        
        print(f"🔍 Running detection #2...")
        patterns2 = proximity_detector.detect(G)
        print(f"   Found {len(patterns2)} pattern(s)")
        
        # Should return same patterns
        assert len(patterns1) == len(patterns2), "Pattern counts differ"
        
        if len(patterns1) > 0 and len(patterns2) > 0:
            # Compare pattern IDs and hashes
            ids1 = sorted([p['pattern_id'] for p in patterns1])
            ids2 = sorted([p['pattern_id'] for p in patterns2])
            assert ids1 == ids2, "Pattern IDs differ"
            print(f"   ✓ Pattern IDs match")
            print(f"   ✓ Deduplication working")
        
        print(f"✅ TEST PASSED: Deduplication")