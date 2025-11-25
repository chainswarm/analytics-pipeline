from typing import Dict, List
from datetime import datetime, timezone
import networkx as nx
import numpy as np
from loguru import logger

from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.storage.repositories.address_label_repository import AddressLabelRepository
from packages.analyzers.structural.structural_pattern_config_loader import load_structural_pattern_config
from packages.analyzers.structural.detectors import (
    CycleDetector,
    LayeringDetector,
    NetworkDetector,
    ProximityDetector,
    MotifDetector,
    BurstDetector,
    ThresholdDetector
)


class StructuralPatternAnalyzer:
    """
    Orchestrator for structural pattern analysis.
    Coordinates multiple specialized pattern detectors to identify suspicious patterns in transaction graphs.
    """

    def __init__(
        self,
        money_flows_repository: MoneyFlowsRepository,
        pattern_repository: StructuralPatternRepository,
        address_label_repository: AddressLabelRepository,
        window_days: int,
        start_timestamp: int,
        end_timestamp: int,
        network: str,
        config_path: str = None
    ):
        self.money_flows_repository = money_flows_repository
        self.pattern_repository = pattern_repository
        self.address_label_repository = address_label_repository
        self.window_days = window_days
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.network = network
        self._address_labels_cache = {}
        
        self.config = load_structural_pattern_config(config_path)
        logger.info(f"Loaded structural pattern configuration for network: {network}")
        
        # Initialize all pattern detectors
        self._init_detectors()

    def _init_detectors(self) -> None:
        """Initialize all pattern detector instances."""
        self.cycle_detector = CycleDetector(self.config, self._address_labels_cache, self.network)
        self.layering_detector = LayeringDetector(self.config, self._address_labels_cache, self.network)
        self.network_detector = NetworkDetector(self.config, self._address_labels_cache, self.network)
        self.proximity_detector = ProximityDetector(self.config, self._address_labels_cache, self.network)
        self.motif_detector = MotifDetector(self.config, self._address_labels_cache, self.network)
        self.burst_detector = BurstDetector(self.config, self._address_labels_cache, self.network)
        self.threshold_detector = ThresholdDetector(self.config, self._address_labels_cache, self.network)
        logger.info("Initialized all pattern detectors")

    def analyze_structural_patterns(self) -> None:
        """
        Main orchestration method for structural pattern analysis.
        Coordinates data loading, graph building, and pattern detection across all detectors.
        """
        logger.info("Starting structural pattern analysis - pure snapshot approach")
        
        logger.info(f"Querying windowed flows from transfers [{self.start_timestamp}, {self.end_timestamp})")
        windowed_flows = self.money_flows_repository.get_windowed_flows_from_transfers(
            start_timestamp_ms=self.start_timestamp,
            end_timestamp_ms=self.end_timestamp
        )
        
        if not windowed_flows:
            raise ValueError("No flows found in time window - indicates data pipeline failure or empty window")
        
        logger.info(f"Retrieved {len(windowed_flows)} windowed flows")
        
        addresses = self._extract_addresses_from_flows(windowed_flows)
        if not addresses:
            raise ValueError("No active addresses found in flows - indicates data issue")
        
        logger.info(f"Loading address labels for {len(addresses)} addresses")
        self._load_address_labels(addresses)

        logger.info(f"Building graph from {len(windowed_flows)} windowed flows for {len(addresses)} addresses")
        G = self._build_graph_from_flows_data(windowed_flows)
        if G.number_of_nodes() == 0:
            raise ValueError("Cannot analyze patterns without graph data")

        logger.info(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

        processing_date = datetime.fromtimestamp(self.end_timestamp / 1000, tz=timezone.utc).strftime('%Y-%m-%d')

        deduplicated_patterns = []

        logger.info("Detecting cycles with deduplication")
        cycle_patterns = self.cycle_detector.detect(G)
        deduplicated_patterns.extend(cycle_patterns)
        logger.info(f"Found {len(cycle_patterns)} unique cycle patterns")

        logger.info("Analyzing network patterns (SCC and smurfing) with deduplication")
        network_patterns = self.network_detector.detect(G)
        deduplicated_patterns.extend(network_patterns)
        logger.info(f"Found {len(network_patterns)} unique network-based patterns")

        logger.info("Detecting layering paths with deduplication")
        layering_patterns = self.layering_detector.detect(G)
        deduplicated_patterns.extend(layering_patterns)
        logger.info(f"Found {len(layering_patterns)} unique layering path patterns")

        logger.info("Analyzing proximity to risk with deduplication")
        proximity_patterns = self.proximity_detector.detect(G)
        deduplicated_patterns.extend(proximity_patterns)
        logger.info(f"Found {len(proximity_patterns)} unique proximity risk patterns")

        logger.info("Detecting structural motifs with deduplication")
        motif_patterns = self.motif_detector.detect(G)
        deduplicated_patterns.extend(motif_patterns)
        logger.info(f"Found {len(motif_patterns)} unique motif patterns")

        logger.info("Detecting temporal burst patterns with deduplication")
        burst_patterns = self.burst_detector.detect(G)
        deduplicated_patterns.extend(burst_patterns)
        logger.info(f"Found {len(burst_patterns)} unique burst patterns")

        logger.info("Detecting threshold evasion patterns with deduplication")
        threshold_patterns = self.threshold_detector.detect(G)
        deduplicated_patterns.extend(threshold_patterns)
        logger.info(f"Found {len(threshold_patterns)} unique threshold evasion patterns")

        if deduplicated_patterns:
            logger.info(f"Storing {len(deduplicated_patterns)} deduplicated structural patterns")
            
            self.pattern_repository.insert_deduplicated_patterns(
                deduplicated_patterns,
                window_days=self.window_days,
                processing_date=processing_date
            )
        else:
            logger.info("No structural patterns detected")

        logger.info("Structural pattern analysis completed")

    def _extract_addresses_from_flows(self, flows: List[Dict]) -> List[str]:
        """
        Extract unique addresses from flows data.
        
        Args:
            flows: List of flow dictionaries
            
        Returns:
            Sorted list of unique addresses
        """
        addresses_set = set()
        for flow in flows:
            addresses_set.add(flow['from_address'])
            addresses_set.add(flow['to_address'])
        return sorted(list(addresses_set))

    def _build_graph_from_flows_data(self, flows: List[Dict]) -> nx.DiGraph:
        """
        Build directed graph from windowed flows data.
        
        Args:
            flows: List of flow dictionaries
            
        Returns:
            NetworkX directed graph with weighted edges
            
        Raises:
            ValueError: If no flows provided
        """
        if not flows:
            raise ValueError("No flows provided - cannot build graph")

        G = nx.DiGraph()

        for flow in flows:
            usd_amount = float(flow['amount_usd_sum'])
            G.add_edge(
                flow['from_address'],
                flow['to_address'],
                weight=usd_amount,
                amount_usd_sum=usd_amount,
                tx_count=int(flow['tx_count']),
                log_weight=np.log1p(usd_amount)
            )

        logger.info(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges from {len(flows)} flows")
        return G

    def _load_address_labels(self, addresses: List[str]) -> None:
        """
        Load address labels from repository into cache.
        
        Args:
            addresses: List of addresses to load labels for
        """
        if self._address_labels_cache:
            return
        
        labels_list = self.address_label_repository.get_labels_for_addresses(
            self.network, addresses
        )
        
        for label in labels_list:
            address = label['address']
            if address not in self._address_labels_cache:
                self._address_labels_cache[address] = label
        
        logger.info(f"Loaded {len(self._address_labels_cache)} address labels from repository")