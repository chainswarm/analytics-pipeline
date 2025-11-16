import time
import json
from typing import Dict, List
from decimal import Decimal
from datetime import datetime, timedelta
import networkx as nx
import numpy as np
from loguru import logger

from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.storage.repositories.address_label_repository import AddressLabelRepository
from packages.storage.constants import PatternTypes, DetectionMethods, AddressTypes, TrustLevels
from packages.analyzers.structural.structural_pattern_config_loader import load_structural_pattern_config
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class StructuralPatternAnalyzer:

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

    def analyze_structural_patterns(self) -> None:
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
        
        trusted_addresses = self._get_trusted_addresses(addresses)
        fraudulent_addresses = self._get_fraudulent_addresses(addresses)
        logger.info(f"Found {len(trusted_addresses)} trusted and {len(fraudulent_addresses)} fraudulent addresses")

        logger.info(f"Building graph from {len(windowed_flows)} windowed flows for {len(addresses)} addresses")
        G = self._build_graph_from_flows_data(windowed_flows)
        if G.number_of_nodes() == 0:
            raise ValueError("Cannot analyze patterns without graph data")

        logger.info(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

        processing_date = datetime.fromtimestamp(self.end_timestamp / 1000).strftime('%Y-%m-%d')

        deduplicated_patterns = []

        logger.info("Detecting cycles with deduplication")
        cycle_patterns = self._detect_cycles_deduplicated(G)
        deduplicated_patterns.extend(cycle_patterns)
        logger.info(f"Found {len(cycle_patterns)} unique cycle patterns")

        logger.info("Analyzing strongly connected components with deduplication")
        scc_patterns = self._analyze_scc_deduplicated(G)
        deduplicated_patterns.extend(scc_patterns)
        logger.info(f"Found {len(scc_patterns)} unique SCC-based patterns")

        logger.info("Detecting layering paths with deduplication")
        layering_patterns = self._detect_layering_deduplicated(G)
        deduplicated_patterns.extend(layering_patterns)
        logger.info(f"Found {len(layering_patterns)} unique layering path patterns")

        logger.info("Analyzing proximity to risk with deduplication")
        proximity_patterns = self._analyze_proximity_deduplicated(G)
        deduplicated_patterns.extend(proximity_patterns)
        logger.info(f"Found {len(proximity_patterns)} unique proximity risk patterns")

        logger.info("Detecting smurfing networks with deduplication")
        smurfing_patterns = self._detect_smurfing_deduplicated(G)
        deduplicated_patterns.extend(smurfing_patterns)
        logger.info(f"Found {len(smurfing_patterns)} unique smurfing network patterns")

        logger.info("Detecting structural motifs with deduplication")
        motif_patterns = self._detect_motifs_deduplicated(G)
        deduplicated_patterns.extend(motif_patterns)
        logger.info(f"Found {len(motif_patterns)} unique motif patterns")

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
        """Extract unique addresses from flows data."""
        addresses_set = set()
        for flow in flows:
            addresses_set.add(flow['from_address'])
            addresses_set.add(flow['to_address'])
        return sorted(list(addresses_set))

    def _build_graph_from_flows_data(self, flows: List[Dict]) -> nx.DiGraph:
        """Build directed graph from windowed flows data."""
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

    def _is_trusted_address(self, address: str) -> bool:
        label_info = self._address_labels_cache.get(address)
        if not label_info:
            return False
            
        trust_level = label_info.get('trust_level')
        address_type = label_info.get('address_type')
        
        safe_trust_levels = [TrustLevels.VERIFIED, TrustLevels.OFFICIAL]
        safe_address_types = [
            AddressTypes.EXCHANGE,
            AddressTypes.INSTITUTIONAL,
            AddressTypes.STAKING,
            AddressTypes.VALIDATOR,
        ]
        
        return (trust_level in safe_trust_levels and
                address_type in safe_address_types)

    def _is_fraudulent_address(self, address: str) -> bool:
        label_info = self._address_labels_cache.get(address)
        if not label_info:
            return False
            
        trust_level = label_info.get('trust_level')
        address_type = label_info.get('address_type')
        
        fraudulent_address_types = [
            AddressTypes.MIXER,
            AddressTypes.SCAM,
            AddressTypes.DARK_MARKET,
            AddressTypes.SANCTIONED
        ]
        
        return (address_type in fraudulent_address_types or
                trust_level == TrustLevels.BLACKLISTED)

    def _get_trusted_addresses(self, addresses: List[str]) -> List[str]:
        """Get list of trusted addresses from the given addresses."""
        return [addr for addr in addresses if self._is_trusted_address(addr)]

    def _get_fraudulent_addresses(self, addresses: List[str]) -> List[str]:
        """Get list of fraudulent addresses from the given addresses."""
        return [addr for addr in addresses if self._is_fraudulent_address(addr)]

    def _adjust_severity_for_trust(self, base_severity: float, participants: List[str]) -> float:
        """Adjust pattern severity based on trust levels of participants."""
        if not participants:
            return base_severity
            
        # Get configuration
        severity_config = self.config["severity_adjustments"]
        trust_reduction_factor = severity_config["trust_reduction_factor"]
        fraud_increase_factor = severity_config["fraud_increase_factor"]
        
        trusted_count = sum(1 for addr in participants if self._is_trusted_address(addr))
        fraudulent_count = sum(1 for addr in participants if self._is_fraudulent_address(addr))
        
        adjusted_severity = base_severity
        
        # Reduce severity if trusted addresses involved
        if trusted_count > 0:
            trust_reduction = trust_reduction_factor * trusted_count / len(participants)
            adjusted_severity *= (1.0 - trust_reduction)
        
        # Increase severity if fraudulent addresses involved
        if fraudulent_count > 0:
            fraud_increase = fraud_increase_factor * fraudulent_count / len(participants)
            adjusted_severity *= (1.0 + fraud_increase)
        
        return min(adjusted_severity, 1.0)

    def _calculate_trust_risk_modifier(self, address: str) -> float:
        """Calculate risk modifier based on address trust level."""
        severity_config = self.config["severity_adjustments"]
        
        if self._is_trusted_address(address):
            return severity_config["trust_risk_modifier"]
        elif self._is_fraudulent_address(address):
            return severity_config["fraud_risk_modifier"]
        else:
            return severity_config["unknown_risk_modifier"]

    def _get_address_context(self, address: str) -> Dict:
        """Get trust and type context for address."""
        label_info = self._address_labels_cache.get(address, {})
        
        return {
            'trust_level': label_info.get('trust_level', TrustLevels.UNVERIFIED),
            'address_type': label_info.get('address_type', AddressTypes.UNKNOWN),
            'is_trusted': self._is_trusted_address(address),
            'is_fraudulent': self._is_fraudulent_address(address),
            'risk_modifier': self._calculate_trust_risk_modifier(address)
        }


    def _calculate_cycle_volume(self, G: nx.DiGraph, cycle: List[str]) -> float:
        """Calculate total volume in a cycle."""
        total_volume = 0.0
        for i in range(len(cycle)):
            from_addr = cycle[i]
            to_addr = cycle[(i + 1) % len(cycle)]
            if G.has_edge(from_addr, to_addr):
                total_volume += G[from_addr][to_addr]['amount_usd_sum']
        return total_volume

    def _calculate_cycle_severity(self, G: nx.DiGraph, cycle: List[str], volume: float) -> float:
        """Calculate severity score for a cycle based on length and volume."""
        cycle_config = self.config["cycle_detection"]
        
        length_factor = min(len(cycle) / 10.0, 1.0)  # Longer cycles are more suspicious
        volume_factor = min(volume / cycle_config["volume_threshold"], 1.0)  # Higher volume is more suspicious
        
        length_weight = cycle_config["length_severity_weight"]
        volume_weight = cycle_config["volume_severity_weight"]
        
        return (length_factor * length_weight + volume_factor * volume_weight)

    def _calculate_path_volume(self, G: nx.DiGraph, path: List[str]) -> float:
        """Calculate total volume along a path."""
        total_volume = 0.0
        for i in range(len(path) - 1):
            if G.has_edge(path[i], path[i + 1]):
                total_volume += G[path[i]][path[i + 1]]['amount_usd_sum']
        return total_volume

    def _calculate_path_density(self, G: nx.DiGraph, path: List[str]) -> float:
        """Calculate density of connections in a path subgraph."""
        if len(path) < 2:
            return 0.0
        subgraph = G.subgraph(path)
        return nx.density(subgraph)

    def _is_layering_pattern(self, G: nx.DiGraph, path: List[str], volume: float) -> bool:
        """Determine if a path exhibits layering characteristics."""
        path_config = self.config["path_analysis"]
        min_volume = path_config["layering_min_volume"]
        cv_threshold = path_config["layering_cv_threshold"]
        
        if len(path) < 3 or volume < min_volume:  # Minimum threshold
            return False
        
        # Check for volume consistency (layering often maintains similar volumes)
        volumes = []
        for i in range(len(path) - 1):
            if G.has_edge(path[i], path[i + 1]):
                volumes.append(G[path[i]][path[i + 1]]['amount_usd_sum'])
        
        if not volumes:
            return False
            
        # Calculate coefficient of variation
        mean_vol = np.mean(volumes)
        std_vol = np.std(volumes)
        cv = std_vol / max(mean_vol, 1.0)
        
        # Layering typically has low variation in volumes
        return cv < cv_threshold

    def _calculate_layering_severity(self, G: nx.DiGraph, path: List[str], volume: float) -> float:
        """Calculate severity score for layering pattern."""
        path_config = self.config["path_analysis"]
        
        length_factor = min((len(path) - 2) / 6.0, 1.0)  # More hops = more suspicious
        volume_factor = min(volume / path_config["layering_volume_threshold"], 1.0)
        
        length_weight = path_config["layering_length_weight"]
        volume_weight = path_config["layering_volume_weight"]
        
        return (length_factor * length_weight + volume_factor * volume_weight)

    def _identify_risk_addresses(self, G: nx.DiGraph) -> List[str]:
        """Identify potentially high-risk addresses based on graph metrics."""
        risk_addresses = []
        
        # Get configuration
        risk_config = self.config["risk_identification"]
        high_volume_threshold = risk_config["high_volume_threshold"]
        high_degree_threshold = risk_config["high_degree_threshold"]
        
        # Calculate volume and degree metrics
        for node in G.nodes():
            in_volume = sum(data['amount_usd_sum'] for _, _, data in G.in_edges(node, data=True))
            out_volume = sum(data['amount_usd_sum'] for _, _, data in G.out_edges(node, data=True))
            total_volume = in_volume + out_volume
            
            degree = G.degree(node)
            
            # Simple heuristic: high volume + high degree
            if total_volume > high_volume_threshold and degree > high_degree_threshold:
                risk_addresses.append(node)
                
        return risk_addresses

    def _is_smurfing_network(self, community_graph: nx.DiGraph) -> bool:
        """Determine if a community exhibits smurfing characteristics."""
        network_config = self.config["network_analysis"]
        min_size = network_config["min_community_size"]
        small_tx_threshold = network_config["small_transaction_threshold"]
        small_tx_ratio_threshold = network_config["small_transaction_ratio_threshold"]
        density_threshold = network_config["density_threshold"]
        
        if community_graph.number_of_nodes() < min_size:
            return False
            
        # Calculate average transaction size
        volumes = [data['amount_usd_sum'] for _, _, data in community_graph.edges(data=True)]
        if not volumes:
            return False
            
        avg_volume = np.mean(volumes)
        
        # Smurfing networks typically have many small transactions
        small_tx_ratio = sum(1 for v in volumes if v < small_tx_threshold) / len(volumes)
        
        # High density + small transactions = potential smurfing
        density = nx.density(community_graph)
        return small_tx_ratio > small_tx_ratio_threshold and density > density_threshold

    def _calculate_smurfing_severity(self, community_graph: nx.DiGraph) -> float:
        """Calculate severity score for smurfing network."""
        network_config = self.config["network_analysis"]
        
        density = nx.density(community_graph)
        size_factor = min(community_graph.number_of_nodes() / network_config["max_size_factor"], 1.0)
        density_factor = min(density / network_config["max_density_factor"], 1.0)
        
        size_weight = network_config["size_severity_weight"]
        density_weight = network_config["density_severity_weight"]
        
        return (size_factor * size_weight + density_factor * density_weight)

    def _identify_hubs_in_network(self, community_graph: nx.DiGraph) -> List[str]:
        if community_graph.number_of_nodes() < 3:
            return []
            
        degrees = [(node, community_graph.degree(node)) for node in community_graph.nodes()]
        degrees.sort(key=lambda x: x[1], reverse=True)
        
        num_hubs = max(1, len(degrees) // 5)
        return [node for node, _ in degrees[:num_hubs]]

    def _detect_cycles_deduplicated(self, G: nx.DiGraph) -> List[Dict]:
        patterns_by_id = {}
        cycle_config = self.config["cycle_detection"]
        max_cycle_length = cycle_config["max_cycle_length"]
        max_cycles_per_scc = cycle_config["max_cycles_per_scc"]
        min_cycle_length = cycle_config["min_cycle_length"]
        confidence_score = cycle_config["confidence_score"]
        risk_score_multiplier = cycle_config["risk_score_multiplier"]
        
        sccs = list(nx.strongly_connected_components(G))
        
        for scc in sccs:
            if len(scc) < 2:
                continue
                
            scc_graph = G.subgraph(scc).copy()
            cycles_found = 0
            
            try:
                for cycle in nx.simple_cycles(scc_graph):
                    if cycles_found >= max_cycles_per_scc:
                        break
                    
                    if len(cycle) < min_cycle_length or len(cycle) > max_cycle_length:
                        continue
                    
                    sorted_cycle = sorted(cycle)
                    pattern_hash = generate_pattern_hash(PatternTypes.CYCLE, sorted_cycle)
                    pattern_id = generate_pattern_id(PatternTypes.CYCLE, pattern_hash)
                    
                    if pattern_id in patterns_by_id:
                        continue
                    
                    cycle_volume = self._calculate_cycle_volume(G, cycle)
                    base_severity = self._calculate_cycle_severity(G, cycle, cycle_volume)
                    severity_score = self._adjust_severity_for_trust(base_severity, cycle)
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.CYCLE,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': sorted_cycle,
                        'address_roles': ['participant'] * len(sorted_cycle),
                        'severity_score': severity_score,
                        'confidence_score': confidence_score,
                        'risk_score': min(severity_score * risk_score_multiplier, 1.0),
                        'cycle_path': cycle,
                        'cycle_length': len(cycle),
                        'cycle_volume_usd': cycle_volume,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': len(cycle),
                        'evidence_volume_usd': cycle_volume,
                        'detection_method': DetectionMethods.CYCLE_DETECTION,
                        'anomaly_score': severity_score
                    }
                    
                    cycles_found += 1
                    
            except Exception as e:
                logger.warning(f"Cycle detection failed for SCC of size {len(scc)}: {e}")
                
        return list(patterns_by_id.values())

    def _analyze_scc_deduplicated(self, G: nx.DiGraph) -> List[Dict]:
        patterns_by_id = {}
        sccs = list(nx.strongly_connected_components(G))
        scc_config = self.config["scc_analysis"]
        
        scc_sizes = [len(scc) for scc in sccs]
        if not scc_sizes:
            return []
            
        mean_size = np.mean(scc_sizes)
        std_size = np.std(scc_sizes)
        
        for scc in sccs:
            scc_size = len(scc)
            
            if scc_size < scc_config["min_scc_size"]:
                continue
                
            if std_size > 0:
                size_z_score = abs(scc_size - mean_size) / std_size
                z_score_norm = scc_config["z_score_normalization"]
                base_anomaly_score = min(size_z_score / z_score_norm, 1.0)
            else:
                base_anomaly_score = 0.0
                
            anomaly_score = self._adjust_severity_for_trust(base_anomaly_score, list(scc))
                
            if anomaly_score < scc_config["anomaly_threshold"]:
                continue
                
            sorted_scc = sorted(list(scc))
            pattern_hash = generate_pattern_hash(PatternTypes.SMURFING_NETWORK, sorted_scc)
            pattern_id = generate_pattern_id('scc', pattern_hash)
            
            if pattern_id in patterns_by_id:
                continue
                
            scc_graph = G.subgraph(scc)
            total_volume = sum(data['amount_usd_sum'] for _, _, data in scc_graph.edges(data=True))
            edge_count = scc_graph.number_of_edges()
            density = nx.density(scc_graph)
            
            patterns_by_id[pattern_id] = {
                'pattern_id': pattern_id,
                'pattern_type': PatternTypes.SMURFING_NETWORK,
                'pattern_subtype': 'anomalous_scc',
                'pattern_hash': pattern_hash,
                'addresses_involved': sorted_scc,
                'address_roles': ['participant'] * len(sorted_scc),
                'severity_score': anomaly_score,
                'confidence_score': scc_config["confidence_score"],
                'risk_score': anomaly_score * scc_config["risk_score_multiplier"],
                'network_members': sorted_scc,
                'network_size': scc_size,
                'network_density': density,
                'detection_timestamp': int(time.time()),
                'evidence_transaction_count': edge_count,
                'evidence_volume_usd': total_volume,
                'detection_method': DetectionMethods.SCC_ANALYSIS,
                'anomaly_score': anomaly_score
            }
                
        return list(patterns_by_id.values())

    def _detect_layering_deduplicated(self, G: nx.DiGraph) -> List[Dict]:
        patterns_by_id = {}
        path_config = self.config["path_analysis"]
        
        min_path_length = path_config["min_path_length"]
        max_path_length = path_config["max_path_length"]
        max_paths_to_check = path_config["max_paths_to_check"]
        confidence_score = path_config["confidence_score"]
        risk_score_multiplier = path_config["risk_score_multiplier"]
        high_volume_percentile = path_config["high_volume_percentile"]
        max_source_nodes = path_config["max_source_nodes"]
        max_target_nodes = path_config["max_target_nodes"]
        
        node_volumes = {}
        for node in G.nodes():
            in_volume = sum(data['amount_usd_sum'] for _, _, data in G.in_edges(node, data=True))
            out_volume = sum(data['amount_usd_sum'] for _, _, data in G.out_edges(node, data=True))
            node_volumes[node] = in_volume + out_volume
            
        volume_threshold = np.percentile(list(node_volumes.values()), high_volume_percentile) if node_volumes else 0
        high_volume_nodes = [node for node, vol in node_volumes.items() if vol >= volume_threshold]
        
        if len(high_volume_nodes) < 2:
            return []
            
        paths_checked = 0
        
        for source in high_volume_nodes[:max_source_nodes]:
            if paths_checked >= max_paths_to_check:
                break
                
            for target in high_volume_nodes[:max_target_nodes]:
                if source == target or paths_checked >= max_paths_to_check:
                    continue
                    
                try:
                    paths = nx.all_simple_paths(G, source, target, cutoff=max_path_length)
                    
                    for path in paths:
                        paths_checked += 1
                        if paths_checked >= max_paths_to_check:
                            break
                            
                        if len(path) < min_path_length:
                            continue
                            
                        path_volume = self._calculate_path_volume(G, path)
                        
                        if self._is_layering_pattern(G, path, path_volume):
                            sorted_path = sorted(path)
                            pattern_hash = generate_pattern_hash(PatternTypes.LAYERING_PATH, sorted_path)
                            pattern_id = generate_pattern_id(PatternTypes.LAYERING_PATH, pattern_hash)
                            
                            if pattern_id in patterns_by_id:
                                continue
                            
                            base_severity = self._calculate_layering_severity(G, path, path_volume)
                            severity_score = self._adjust_severity_for_trust(base_severity, path)
                            
                            patterns_by_id[pattern_id] = {
                                'pattern_id': pattern_id,
                                'pattern_type': PatternTypes.LAYERING_PATH,
                                'pattern_hash': pattern_hash,
                                'addresses_involved': sorted_path,
                                'address_roles': ['source'] + ['intermediary'] * (len(path) - 2) + ['destination'],
                                'severity_score': severity_score,
                                'confidence_score': confidence_score,
                                'risk_score': severity_score * risk_score_multiplier,
                                'layering_path': path,
                                'path_depth': len(path),
                                'path_volume_usd': path_volume,
                                'source_address': source,
                                'destination_address': target,
                                'detection_timestamp': int(time.time()),
                                'evidence_transaction_count': len(path) - 1,
                                'evidence_volume_usd': path_volume,
                                'detection_method': DetectionMethods.PATH_ANALYSIS,
                                'anomaly_score': severity_score
                            }
                                
                except nx.NetworkXNoPath:
                    continue
                except Exception as e:
                    logger.warning(f"Path detection failed between {source} and {target}: {e}")
                    continue
                    
        return list(patterns_by_id.values())

    def _analyze_proximity_deduplicated(self, G: nx.DiGraph) -> List[Dict]:
        patterns_by_id = {}
        proximity_config = self.config["proximity_analysis"]
        
        max_distance = proximity_config["max_distance"]
        confidence_score = proximity_config["confidence_score"]
        base_severity = proximity_config["base_severity"]
        distance_decay_factor = proximity_config["distance_decay_factor"]
        
        all_addresses = list(G.nodes())
        fraudulent_addresses = self._get_fraudulent_addresses(all_addresses)
        
        if not fraudulent_addresses:
            risk_addresses = self._identify_risk_addresses(G)
        else:
            risk_addresses = fraudulent_addresses
        
        if not risk_addresses:
            return []
            
        for risk_addr in risk_addresses:
            try:
                distances = nx.single_source_shortest_path_length(
                    G.to_undirected(), risk_addr, cutoff=max_distance
                )
                
                for address, distance in distances.items():
                    if address == risk_addr or distance == 0:
                        continue
                        
                    pattern_hash = generate_pattern_hash(PatternTypes.PROXIMITY_RISK, [risk_addr, address])
                    pattern_id = generate_pattern_id(PatternTypes.PROXIMITY_RISK, pattern_hash)
                    
                    if pattern_id in patterns_by_id:
                        continue
                    
                    risk_propagation = distance_decay_factor / (distance + 1)
                    calculated_severity = risk_propagation * base_severity
                    severity_score = self._adjust_severity_for_trust(calculated_severity, [address])
                    
                    address_volume = sum(data['amount_usd_sum'] for _, _, data in G.in_edges(address, data=True))
                    address_volume += sum(data['amount_usd_sum'] for _, _, data in G.out_edges(address, data=True))
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.PROXIMITY_RISK,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': [risk_addr, address],
                        'address_roles': ['risk_source', 'suspect'],
                        'severity_score': severity_score,
                        'confidence_score': confidence_score,
                        'risk_score': severity_score,
                        'risk_source_address': risk_addr,
                        'distance_to_risk': distance,
                        'risk_propagation_score': risk_propagation,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': G.in_degree(address) + G.out_degree(address),
                        'evidence_volume_usd': address_volume,
                        'detection_method': DetectionMethods.PROXIMITY_ANALYSIS,
                        'anomaly_score': severity_score
                    }
                    
            except Exception as e:
                logger.warning(f"Proximity analysis failed for risk address {risk_addr}: {e}")
                
        return list(patterns_by_id.values())

    def _detect_smurfing_deduplicated(self, G: nx.DiGraph) -> List[Dict]:
        patterns_by_id = {}
        network_config = self.config["network_analysis"]
        
        min_community_size = network_config["min_community_size"]
        max_community_size = network_config["max_community_size"]
        confidence_score = network_config["confidence_score"]
        risk_score_multiplier = network_config["risk_score_multiplier"]
        
        try:
            G_undirected = G.to_undirected()
            communities = nx.community.greedy_modularity_communities(G_undirected, weight='weight')
            
            for community in communities:
                community_size = len(community)
                
                if community_size < min_community_size or community_size > max_community_size:
                    continue
                    
                community_graph = G.subgraph(community)
                
                if self._is_smurfing_network(community_graph):
                    sorted_community = sorted(list(community))
                    pattern_hash = generate_pattern_hash(PatternTypes.SMURFING_NETWORK, sorted_community)
                    pattern_id = generate_pattern_id(PatternTypes.SMURFING_NETWORK, pattern_hash)
                    
                    if pattern_id in patterns_by_id:
                        continue
                    
                    density = nx.density(community_graph)
                    total_volume = sum(data.get('amount_usd_sum', 0) for _, _, data in community_graph.edges(data=True))
                    
                    base_severity = self._calculate_smurfing_severity(community_graph)
                    severity_score = self._adjust_severity_for_trust(base_severity, list(community))
                    
                    hub_addresses = self._identify_hubs_in_network(community_graph)
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.SMURFING_NETWORK,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': sorted_community,
                        'address_roles': ['hub' if addr in hub_addresses else 'participant' for addr in sorted_community],
                        'severity_score': severity_score,
                        'confidence_score': confidence_score,
                        'risk_score': severity_score * risk_score_multiplier,
                        'network_members': sorted_community,
                        'network_size': community_size,
                        'network_density': density,
                        'hub_addresses': hub_addresses,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': community_graph.number_of_edges(),
                        'evidence_volume_usd': total_volume,
                        'detection_method': DetectionMethods.NETWORK_ANALYSIS,
                        'anomaly_score': severity_score
                    }
                        
        except Exception as e:
            logger.warning(f"Smurfing network detection failed: {e}")
            
        return list(patterns_by_id.values())

    def _detect_motifs_deduplicated(self, G: nx.DiGraph) -> List[Dict]:
        patterns_by_id = {}
        motif_config = self.config["motif_detection"]
        
        confidence_score = motif_config["confidence_score"]
        risk_score_multiplier = motif_config["risk_score_multiplier"]
        base_severity = motif_config["base_severity"]
        degree_percentile = motif_config["degree_percentile_threshold"]
        fanin_max_out_degree = motif_config["fanin_max_out_degree"]
        fanout_max_in_degree = motif_config["fanout_max_in_degree"]
        
        in_degrees = [G.in_degree(node) for node in G.nodes()]
        out_degrees = [G.out_degree(node) for node in G.nodes()]
        
        if not in_degrees or not out_degrees:
            return []
            
        in_degree_threshold = np.percentile(in_degrees, degree_percentile)
        out_degree_threshold = np.percentile(out_degrees, degree_percentile)
        
        for node in G.nodes():
            in_deg = G.in_degree(node)
            out_deg = G.out_degree(node)
            
            if in_deg >= in_degree_threshold and out_deg <= fanin_max_out_degree:
                in_neighbors = list(G.predecessors(node))
                all_addresses = [node] + in_neighbors
                pattern_hash = generate_pattern_hash(PatternTypes.MOTIF_FANIN, sorted(all_addresses))
                pattern_id = generate_pattern_id(PatternTypes.MOTIF_FANIN, pattern_hash)
                
                if pattern_id not in patterns_by_id:
                    calculated_severity = min(in_deg / max(in_degree_threshold, 1), 1.0) * base_severity
                    severity_score = self._adjust_severity_for_trust(calculated_severity, all_addresses)
                    
                    fanin_volume = sum(data['amount_usd_sum'] for _, _, data in G.in_edges(node, data=True))
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.MOTIF_FANIN,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': all_addresses,
                        'address_roles': ['center'] + ['source'] * len(in_neighbors),
                        'severity_score': severity_score,
                        'confidence_score': confidence_score,
                        'risk_score': severity_score * risk_score_multiplier,
                        'motif_type': 'fanin',
                        'motif_center_address': node,
                        'motif_participant_count': in_deg + out_deg,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': in_deg,
                        'evidence_volume_usd': fanin_volume,
                        'detection_method': DetectionMethods.MOTIF_DETECTION,
                        'anomaly_score': severity_score
                    }
                
            if out_deg >= out_degree_threshold and in_deg <= fanout_max_in_degree:
                out_neighbors = list(G.successors(node))
                all_addresses = [node] + out_neighbors
                pattern_hash = generate_pattern_hash(PatternTypes.MOTIF_FANOUT, sorted(all_addresses))
                pattern_id = generate_pattern_id(PatternTypes.MOTIF_FANOUT, pattern_hash)
                
                if pattern_id not in patterns_by_id:
                    calculated_severity = min(out_deg / max(out_degree_threshold, 1), 1.0) * base_severity
                    severity_score = self._adjust_severity_for_trust(calculated_severity, all_addresses)
                    
                    fanout_volume = sum(data['amount_usd_sum'] for _, _, data in G.out_edges(node, data=True))
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.MOTIF_FANOUT,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': all_addresses,
                        'address_roles': ['center'] + ['destination'] * len(out_neighbors),
                        'severity_score': severity_score,
                        'confidence_score': confidence_score,
                        'risk_score': severity_score * risk_score_multiplier,
                        'motif_type': 'fanout',
                        'motif_center_address': node,
                        'motif_participant_count': in_deg + out_deg,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': out_deg,
                        'evidence_volume_usd': fanout_volume,
                        'detection_method': DetectionMethods.MOTIF_DETECTION,
                        'anomaly_score': severity_score
                    }
                
        return list(patterns_by_id.values())