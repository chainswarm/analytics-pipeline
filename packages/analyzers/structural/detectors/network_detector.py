import time
from typing import Dict, List
import networkx as nx
import numpy as np
from loguru import logger

from packages.analyzers.structural.base_detector import BasePatternDetector
from packages.storage.constants import PatternTypes, DetectionMethods
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class NetworkDetector(BasePatternDetector):
    """
    Detector for network-based patterns including SCC analysis and smurfing networks.
    Identifies anomalous strongly connected components and community-based smurfing patterns.
    """

    def _validate_config(self) -> None:
        """Validate that network_analysis and scc_analysis configurations are present."""
        if "network_analysis" not in self.config:
            raise ValueError("Missing 'network_analysis' section in configuration")
        if "scc_analysis" not in self.config:
            raise ValueError("Missing 'scc_analysis' section in configuration")

    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect network patterns in the graph (SCC and smurfing).
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected network pattern dictionaries
        """
        patterns = []
        
        # Detect SCC-based patterns
        scc_patterns = self._analyze_scc(G)
        patterns.extend(scc_patterns)
        
        # Detect smurfing networks
        smurfing_patterns = self._detect_smurfing(G)
        patterns.extend(smurfing_patterns)
        
        return patterns

    def _analyze_scc(self, G: nx.DiGraph) -> List[Dict]:
        """
        Analyze strongly connected components for anomalous patterns.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected SCC-based pattern dictionaries
        """
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

    def _detect_smurfing(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect smurfing networks using community detection.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected smurfing pattern dictionaries
        """
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

    def _is_smurfing_network(self, community_graph: nx.DiGraph) -> bool:
        """
        Determine if a community exhibits smurfing characteristics.
        
        Args:
            community_graph: Subgraph representing the community
            
        Returns:
            True if community shows smurfing characteristics
        """
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
        """
        Calculate severity score for smurfing network.
        
        Args:
            community_graph: Subgraph representing the smurfing network
            
        Returns:
            Severity score between 0 and 1
        """
        network_config = self.config["network_analysis"]
        
        density = nx.density(community_graph)
        size_factor = min(community_graph.number_of_nodes() / network_config["max_size_factor"], 1.0)
        density_factor = min(density / network_config["max_density_factor"], 1.0)
        
        size_weight = network_config["size_severity_weight"]
        density_weight = network_config["density_severity_weight"]
        
        return (size_factor * size_weight + density_factor * density_weight)

    def _identify_hubs_in_network(self, community_graph: nx.DiGraph) -> List[str]:
        """
        Identify hub addresses in a network based on degree centrality.
        
        Args:
            community_graph: Subgraph representing the network
            
        Returns:
            List of hub addresses
        """
        if community_graph.number_of_nodes() < 3:
            return []
            
        degrees = [(node, community_graph.degree(node)) for node in community_graph.nodes()]
        degrees.sort(key=lambda x: x[1], reverse=True)
        
        num_hubs = max(1, len(degrees) // 5)
        return [node for node, _ in degrees[:num_hubs]]