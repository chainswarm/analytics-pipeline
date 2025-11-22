import time
from typing import Dict, List
import networkx as nx
from loguru import logger

from packages.analyzers.structural.base_detector import BasePatternDetector
from packages.storage.constants import PatternTypes, DetectionMethods
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class ProximityDetector(BasePatternDetector):
    """
    Detector for proximity-based risk patterns.
    Identifies addresses that are close to known risk addresses in the transaction graph.
    """

    def _validate_config(self) -> None:
        """Validate that proximity_analysis and risk_identification configurations are present."""
        if "proximity_analysis" not in self.config:
            raise ValueError("Missing 'proximity_analysis' section in configuration")
        if "risk_identification" not in self.config:
            raise ValueError("Missing 'risk_identification' section in configuration")
        if "severity_adjustments" not in self.config:
            raise ValueError("Missing 'severity_adjustments' section in configuration")

    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect proximity-based risk patterns in the graph.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected proximity pattern dictionaries
        """
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

    def _identify_risk_addresses(self, G: nx.DiGraph) -> List[str]:
        """
        Identify potentially high-risk addresses based on graph metrics.
        
        Args:
            G: Graph to analyze
            
        Returns:
            List of addresses identified as potentially risky
        """
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

    def _get_trusted_addresses(self, addresses: List[str]) -> List[str]:
        """
        Get list of trusted addresses from the given addresses.
        
        Args:
            addresses: List of addresses to filter
            
        Returns:
            List of trusted addresses
        """
        return [addr for addr in addresses if self._is_trusted_address(addr)]

    def _get_fraudulent_addresses(self, addresses: List[str]) -> List[str]:
        """
        Get list of fraudulent addresses from the given addresses.
        
        Args:
            addresses: List of addresses to filter
            
        Returns:
            List of fraudulent addresses
        """
        return [addr for addr in addresses if self._is_fraudulent_address(addr)]