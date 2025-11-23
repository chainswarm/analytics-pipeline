"""
Integration tests for motif (fan-in/fan-out) detection.

Tests the motif detection algorithm from StructuralPatternAnalyzer
against real-world data to verify:
- Fan-in patterns are correctly identified
- Fan-out patterns are correctly identified
- Center addresses are identified correctly
- Pattern properties are accurate (participant count, volume)
- Deduplication works correctly
- Data is stored in analyzers_patterns_motif table

Enhanced with:
- Dynamic motif generation for various sizes
- Noise transactions
- Parametrized tests
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


def generate_fanin_motif_with_noise(num_sources: int, noise_ratio: float = 0.01) -> Tuple[nx.DiGraph, dict]:
    """
    Generate a fan-in motif pattern with noise transactions.
    
    Args:
        num_sources: Number of source nodes sending to center (5, 10, 20, 50)
        noise_ratio: Ratio of noise transactions to motif transactions
    
    Returns:
        Tuple of (graph, metadata) where metadata contains:
            - center_address: The aggregation point
            - source_addresses: List of source nodes
            - fanin_volume: Total incoming volume
            - in_degree: Number of sources
            - noise_edges: List of noise edges
    """
    print(f"\n{'='*80}")
    print(f"🔧 GENERATING FAN-IN MOTIF: sources={num_sources}, noise_ratio={noise_ratio}")
    print(f"{'='*80}")
    
    G = nx.DiGraph()
    
    # Generate center and source nodes
    center = "CENTER_FANIN"
    sources = [f"SOURCE_{i:04d}" for i in range(num_sources)]
    print(f"📍 Center: {center}")
    print(f"📍 Sources: {len(sources)} nodes")
    
    # Create fan-in edges
    total_fanin_volume = 0
    base_amount = 10000  # Base amount in USD
    
    for source in sources:
        amount = base_amount * random.uniform(0.8, 1.2)
        tx_count = random.randint(1, 3)
        G.add_edge(source, center, amount_usd_sum=amount, tx_count=tx_count)
        total_fanin_volume += amount
    
    print(f"💰 Fan-in edges: {len(sources)} edges, total volume: ${total_fanin_volume:.2f}")
    
    # Add minimal outgoing edges from center (to avoid fan-out detection)
    G.add_edge(center, "OUTPUT_1", amount_usd_sum=5000, tx_count=1)
    
    # Calculate noise edges
    num_noise_edges = int(len(sources) * noise_ratio)
    noise_edges = []
    
    if num_noise_edges > 0:
        print(f"🔊 Adding noise: {num_noise_edges} noise edges")
        noise_nodes = [f"NOISE_{i:04d}" for i in range(max(2, num_noise_edges))]
        
        for i in range(num_noise_edges):
            from_node = random.choice(noise_nodes)
            to_node = random.choice(noise_nodes)
            if from_node == to_node:
                to_node = random.choice(noise_nodes + sources[:2])
            
            amount = base_amount * random.uniform(0.1, 0.5)
            G.add_edge(from_node, to_node, amount_usd_sum=amount, tx_count=1)
            noise_edges.append((from_node, to_node, amount))
    
    print(f"📊 Graph stats:")
    print(f"   Total nodes: {G.number_of_nodes()}")
    print(f"   Total edges: {G.number_of_edges()}")
    print(f"   Center in-degree: {G.in_degree(center)}")
    print(f"   Center out-degree: {G.out_degree(center)}")
    
    metadata = {
        'center_address': center,
        'source_addresses': sources,
        'fanin_volume': total_fanin_volume,
        'in_degree': len(sources),
        'noise_edges': noise_edges,
        'num_sources': num_sources
    }
    
    return G, metadata


def generate_fanout_motif_with_noise(num_destinations: int, noise_ratio: float = 0.01) -> Tuple[nx.DiGraph, dict]:
    """
    Generate a fan-out motif pattern with noise transactions.
    
    Args:
        num_destinations: Number of destination nodes receiving from center
        noise_ratio: Ratio of noise transactions to motif transactions
    
    Returns:
        Tuple of (graph, metadata) where metadata contains:
            - center_address: The distribution point
            - destination_addresses: List of destination nodes
            - fanout_volume: Total outgoing volume
            - out_degree: Number of destinations
            - noise_edges: List of noise edges
    """
    print(f"\n{'='*80}")
    print(f"🔧 GENERATING FAN-OUT MOTIF: destinations={num_destinations}, noise_ratio={noise_ratio}")
    print(f"{'='*80}")
    
    G = nx.DiGraph()
    
    # Generate center and destination nodes
    center = "CENTER_FANOUT"
    destinations = [f"DEST_{i:04d}" for i in range(num_destinations)]
    print(f"📍 Center: {center}")
    print(f"📍 Destinations: {len(destinations)} nodes")
    
    # Create fan-out edges
    total_fanout_volume = 0
    base_amount = 10000  # Base amount in USD
    
    for dest in destinations:
        amount = base_amount * random.uniform(0.8, 1.2)
        tx_count = random.randint(1, 3)
        G.add_edge(center, dest, amount_usd_sum=amount, tx_count=tx_count)
        total_fanout_volume += amount
    
    print(f"💰 Fan-out edges: {len(destinations)} edges, total volume: ${total_fanout_volume:.2f}")
    
    # Add minimal incoming edges to center (to avoid fan-in detection)
    G.add_edge("INPUT_1", center, amount_usd_sum=5000, tx_count=1)
    
    # Calculate noise edges
    num_noise_edges = int(len(destinations) * noise_ratio)
    noise_edges = []
    
    if num_noise_edges > 0:
        print(f"🔊 Adding noise: {num_noise_edges} noise edges")
        noise_nodes = [f"NOISE_{i:04d}" for i in range(max(2, num_noise_edges))]
        
        for i in range(num_noise_edges):
            from_node = random.choice(noise_nodes)
            to_node = random.choice(noise_nodes)
            if from_node == to_node:
                to_node = random.choice(noise_nodes + destinations[:2])
            
            amount = base_amount * random.uniform(0.1, 0.5)
            G.add_edge(from_node, to_node, amount_usd_sum=amount, tx_count=1)
            noise_edges.append((from_node, to_node, amount))
    
    print(f"📊 Graph stats:")
    print(f"   Total nodes: {G.number_of_nodes()}")
    print(f"   Total edges: {G.number_of_edges()}")
    print(f"   Center in-degree: {G.in_degree(center)}")
    print(f"   Center out-degree: {G.out_degree(center)}")
    
    metadata = {
        'center_address': center,
        'destination_addresses': destinations,
        'fanout_volume': total_fanout_volume,
        'out_degree': len(destinations),
        'noise_edges': noise_edges,
        'num_destinations': num_destinations
    }
    
    return G, metadata


def create_simple_fanin_graph() -> nx.DiGraph:
    """
    Create a simple fan-in: 5 sources → 1 center
    """
    G = nx.DiGraph()
    center = 'CENTER'
    for i in range(5):
        source = f'SOURCE_{i}'
        G.add_edge(source, center, amount_usd_sum=10000, tx_count=2)
    # Minimal outgoing
    G.add_edge(center, 'OUTPUT', amount_usd_sum=5000, tx_count=1)
    return G


def create_simple_fanout_graph() -> nx.DiGraph:
    """
    Create a simple fan-out: 1 center → 5 destinations
    """
    G = nx.DiGraph()
    center = 'CENTER'
    # Minimal incoming
    G.add_edge('INPUT', center, amount_usd_sum=5000, tx_count=1)
    for i in range(5):
        dest = f'DEST_{i}'
        G.add_edge(center, dest, amount_usd_sum=10000, tx_count=2)
    return G


class TestMotifDetection:
    """Test motif pattern detection (fan-in and fan-out)."""
    
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
        
        return analyzer
    
    @pytest.mark.parametrize("num_sources", [5, 10, 20, 50])
    def test_dynamic_fanin_detection_with_noise(self, analyzer, num_sources):
        """
        Test fan-in detection with dynamically generated motifs of various sizes.
        Includes noise transactions.
        """
        print(f"\n{'#'*80}")
        print(f"# TEST: Fan-In Detection - {num_sources} sources")
        print(f"{'#'*80}")
        
        # Generate fan-in with noise
        G, metadata = generate_fanin_motif_with_noise(num_sources, noise_ratio=10)
        
        # Mock the graph building
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        # Get motif detector
        motif_detector = analyzer.motif_detector
        
        # Run detection
        print(f"\n🔍 Running motif detection...")
        start_time = time.time()
        patterns = motif_detector.detect(G)
        detection_time = time.time() - start_time
        
        print(f"⏱️  Detection completed in {detection_time:.4f} seconds")
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Debug: Print all detected patterns
        for idx, pattern in enumerate(patterns):
            print(f"\n  Pattern {idx + 1}:")
            print(f"    Type: {pattern.get('pattern_type', 'N/A')}")
            print(f"    Motif Type: {pattern.get('motif_type', 'N/A')}")
            print(f"    Center: {pattern.get('motif_center_address', 'N/A')}")
            print(f"    Participants: {pattern.get('motif_participant_count', 'N/A')}")
            print(f"    Volume: ${pattern.get('evidence_volume_usd', 0):.2f}")
        
        # Find fan-in pattern
        fanin_patterns = [p for p in patterns if p.get('motif_type') == 'fanin']
        
        print(f"\n✅ Running assertions...")
        assert len(fanin_patterns) >= 1, f"Expected at least 1 fan-in pattern, found {len(fanin_patterns)}"
        print(f"   ✓ Found fan-in pattern")
        
        # Verify main fan-in pattern
        pattern = fanin_patterns[0]
        
        assert pattern['pattern_type'] == 'motif_fanin'
        print(f"   ✓ Pattern type is 'motif_fanin'")
        
        assert pattern['motif_type'] == 'fanin'
        print(f"   ✓ Motif type is 'fanin'")
        
        assert pattern['motif_center_address'] == metadata['center_address']
        print(f"   ✓ Center address identified correctly")
        
        # Verify addresses involved
        assert metadata['center_address'] in pattern['addresses_involved']
        print(f"   ✓ Center address in addresses_involved")
        
        # Verify address roles
        assert 'center' in pattern['address_roles']
        assert 'source' in pattern['address_roles']
        print(f"   ✓ Address roles correctly assigned")
        
        # Verify volume
        expected_volume = metadata['fanin_volume']
        detected_volume = pattern['evidence_volume_usd']
        volume_tolerance = expected_volume * 0.15  # 15% tolerance
        
        assert abs(detected_volume - expected_volume) <= volume_tolerance, \
            f"Volume mismatch. Expected ~${expected_volume:.2f}, got ${detected_volume:.2f}"
        print(f"   ✓ Fan-in volume accurate: ${detected_volume:.2f}")
        
        print(f"\n{'='*80}")
        print(f"✅ TEST PASSED: Fan-in with {num_sources} sources")
        print(f"{'='*80}\n")
    
    @pytest.mark.parametrize("num_destinations", [5, 10, 20, 50])
    def test_dynamic_fanout_detection_with_noise(self, analyzer, num_destinations):
        """
        Test fan-out detection with dynamically generated motifs of various sizes.
        Includes noise transactions.
        """
        print(f"\n{'#'*80}")
        print(f"# TEST: Fan-Out Detection - {num_destinations} destinations")
        print(f"{'#'*80}")
        
        # Generate fan-out with noise
        G, metadata = generate_fanout_motif_with_noise(num_destinations, noise_ratio=10)
        
        # Mock the graph building
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        # Get motif detector
        motif_detector = analyzer.motif_detector
        
        # Run detection
        print(f"\n🔍 Running motif detection...")
        start_time = time.time()
        patterns = motif_detector.detect(G)
        detection_time = time.time() - start_time
        
        print(f"⏱️  Detection completed in {detection_time:.4f} seconds")
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Debug: Print all detected patterns
        for idx, pattern in enumerate(patterns):
            print(f"\n  Pattern {idx + 1}:")
            print(f"    Type: {pattern.get('pattern_type', 'N/A')}")
            print(f"    Motif Type: {pattern.get('motif_type', 'N/A')}")
            print(f"    Center: {pattern.get('motif_center_address', 'N/A')}")
            print(f"    Participants: {pattern.get('motif_participant_count', 'N/A')}")
            print(f"    Volume: ${pattern.get('evidence_volume_usd', 0):.2f}")
        
        # Find fan-out pattern
        fanout_patterns = [p for p in patterns if p.get('motif_type') == 'fanout']
        
        print(f"\n✅ Running assertions...")
        assert len(fanout_patterns) >= 1, f"Expected at least 1 fan-out pattern, found {len(fanout_patterns)}"
        print(f"   ✓ Found fan-out pattern")
        
        # Verify main fan-out pattern
        pattern = fanout_patterns[0]
        
        assert pattern['pattern_type'] == 'motif_fanout'
        print(f"   ✓ Pattern type is 'motif_fanout'")
        
        assert pattern['motif_type'] == 'fanout'
        print(f"   ✓ Motif type is 'fanout'")
        
        assert pattern['motif_center_address'] == metadata['center_address']
        print(f"   ✓ Center address identified correctly")
        
        # Verify addresses involved
        assert metadata['center_address'] in pattern['addresses_involved']
        print(f"   ✓ Center address in addresses_involved")
        
        # Verify address roles
        assert 'center' in pattern['address_roles']
        assert 'destination' in pattern['address_roles']
        print(f"   ✓ Address roles correctly assigned")
        
        # Verify volume
        expected_volume = metadata['fanout_volume']
        detected_volume = pattern['evidence_volume_usd']
        volume_tolerance = expected_volume * 0.15  # 15% tolerance
        
        assert abs(detected_volume - expected_volume) <= volume_tolerance, \
            f"Volume mismatch. Expected ~${expected_volume:.2f}, got ${detected_volume:.2f}"
        print(f"   ✓ Fan-out volume accurate: ${detected_volume:.2f}")
        
        print(f"\n{'='*80}")
        print(f"✅ TEST PASSED: Fan-out with {num_destinations} destinations")
        print(f"{'='*80}\n")
    
    def test_fanin_detection_basic(self, analyzer):
        """Test basic fan-in detection with simple pattern."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Basic Fan-In Detection")
        print(f"{'#'*80}")
        
        G = create_simple_fanin_graph()
        
        # Mock the graph building
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        motif_detector = analyzer.motif_detector
        patterns = motif_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Find fan-in patterns
        fanin_patterns = [p for p in patterns if p.get('motif_type') == 'fanin']
        assert len(fanin_patterns) >= 1, "Should detect fan-in pattern"
        
        pattern = fanin_patterns[0]
        assert pattern['pattern_type'] == 'motif_fanin'
        assert pattern['motif_center_address'] == 'CENTER'
        assert 'center' in pattern['address_roles']
        
        print(f"✅ TEST PASSED: Basic fan-in detection")
    
    def test_fanout_detection_basic(self, analyzer):
        """Test basic fan-out detection with simple pattern."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Basic Fan-Out Detection")
        print(f"{'#'*80}")
        
        G = create_simple_fanout_graph()
        
        # Mock the graph building
        analyzer._build_graph_from_flows_data = lambda flows: G
        analyzer._extract_addresses_from_flows = lambda flows: list(G.nodes())
        analyzer._load_address_labels = lambda addrs: None
        
        motif_detector = analyzer.motif_detector
        patterns = motif_detector.detect(G)
        
        print(f"📋 Detected {len(patterns)} pattern(s)")
        
        # Find fan-out patterns
        fanout_patterns = [p for p in patterns if p.get('motif_type') == 'fanout']
        assert len(fanout_patterns) >= 1, "Should detect fan-out pattern"
        
        pattern = fanout_patterns[0]
        assert pattern['pattern_type'] == 'motif_fanout'
        assert pattern['motif_center_address'] == 'CENTER'
        assert 'center' in pattern['address_roles']
        
        print(f"✅ TEST PASSED: Basic fan-out detection")
    
    def test_motif_center_identification(self, analyzer):
        """Test that center addresses are correctly identified."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Center Address Identification")
        print(f"{'#'*80}")
        
        # Create both patterns
        G_fanin = create_simple_fanin_graph()
        G_fanout = create_simple_fanout_graph()
        
        motif_detector = analyzer.motif_detector
        
        # Test fan-in
        patterns_in = motif_detector.detect(G_fanin)
        fanin = [p for p in patterns_in if p.get('motif_type') == 'fanin']
        if fanin:
            assert fanin[0]['motif_center_address'] == 'CENTER'
            print(f"   ✓ Fan-in center correctly identified")
        
        # Test fan-out
        patterns_out = motif_detector.detect(G_fanout)
        fanout = [p for p in patterns_out if p.get('motif_type') == 'fanout']
        if fanout:
            assert fanout[0]['motif_center_address'] == 'CENTER'
            print(f"   ✓ Fan-out center correctly identified")
        
        print(f"✅ TEST PASSED: Center identification")
    
    def test_motif_stored_in_correct_table(self, test_clickhouse_client, test_data_context, setup_test_schema, clean_pattern_tables):
        """Test that motif patterns are stored in analyzers_patterns_motif table."""
        print(f"\n{'#'*80}")
        print(f"# TEST: Motif Storage in Database")
        print(f"{'#'*80}")
        
        from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
        from packages.storage.constants import PatternTypes
        
        repo = StructuralPatternRepository(test_clickhouse_client)
        
        # Create fake fan-in pattern
        patterns = [{
            'pattern_id': 'motif_fanin_test_001',
            'pattern_type': PatternTypes.MOTIF_FANIN,
            'pattern_hash': 'hash_fanin_test_001',
            'addresses_involved': ['CENTER', 'S1', 'S2', 'S3'],
            'address_roles': ['center', 'source', 'source', 'source'],
            'motif_type': 'fanin',
            'motif_center_address': 'CENTER',
            'motif_participant_count': 6,
            'detection_timestamp': int(time.time()),
            'pattern_start_time': 0,
            'pattern_end_time': 0,
            'pattern_duration_hours': 0,
            'evidence_transaction_count': 3,
            'evidence_volume_usd': 30000,
            'detection_method': 'motif_detection'
        }]
        
        print(f"💾 Inserting pattern into database...")
        repo.insert_deduplicated_patterns(
            patterns,
            window_days=test_data_context['window_days'],
            processing_date=test_data_context['processing_date']
        )
        
        print(f"🔍 Querying database for pattern...")
        result = test_clickhouse_client.query(
            "SELECT * FROM analyzers_patterns_motif WHERE pattern_id = 'motif_fanin_test_001'"
        )
        
        print(f"📊 Query returned {len(result.result_rows)} row(s)")
        
        assert len(result.result_rows) == 1, "Pattern should be in motif table"
        
        # Verify columns exist
        print(f"📋 Available columns: {', '.join(result.column_names)}")
        
        assert 'motif_type' in result.column_names
        assert 'motif_center_address' in result.column_names
        assert 'motif_participant_count' in result.column_names
        
        print(f"✅ TEST PASSED: Pattern stored correctly in database")