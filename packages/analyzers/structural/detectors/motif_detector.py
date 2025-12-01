import time
from typing import Dict, List
import networkx as nx
import numpy as np
from loguru import logger

from packages.analyzers.structural.base_detector import BasePatternDetector
from chainswarm_core.constants.patterns import PatternTypes, DetectionMethods
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class MotifDetector(BasePatternDetector):
    """
    Detector for structural motifs in the transaction graph.
    Identifies fan-in and fan-out patterns that may indicate fund aggregation or distribution.
    """

    def _validate_config(self) -> None:
        """Validate that motif_detection configuration is present."""
        if "motif_detection" not in self.config:
            raise ValueError("Missing 'motif_detection' section in configuration")

    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect motif patterns in the graph (fan-in and fan-out).
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected motif pattern dictionaries
        """
        patterns_by_id = {}
        motif_config = self.config["motif_detection"]
        
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
            
            # Detect fan-in motif
            if in_deg >= in_degree_threshold and out_deg <= fanin_max_out_degree:
                in_neighbors = list(G.predecessors(node))
                all_addresses = [node] + in_neighbors
                pattern_hash = generate_pattern_hash(PatternTypes.MOTIF_FANIN, sorted(all_addresses))
                pattern_id = generate_pattern_id(PatternTypes.MOTIF_FANIN, pattern_hash)
                
                if pattern_id not in patterns_by_id:
                    
                    fanin_volume = sum(data['amount_usd_sum'] for _, _, data in G.in_edges(node, data=True))
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.MOTIF_FANIN,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': all_addresses,
                        'address_roles': ['center'] + ['source'] * len(in_neighbors),
                        'motif_type': 'fanin',
                        'motif_center_address': node,
                        'motif_participant_count': in_deg + out_deg,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': in_deg,
                        'evidence_volume_usd': fanin_volume,
                        'detection_method': DetectionMethods.MOTIF_DETECTION
                    }
            
            # Detect fan-out motif
            if out_deg >= out_degree_threshold and in_deg <= fanout_max_in_degree:
                out_neighbors = list(G.successors(node))
                all_addresses = [node] + out_neighbors
                pattern_hash = generate_pattern_hash(PatternTypes.MOTIF_FANOUT, sorted(all_addresses))
                pattern_id = generate_pattern_id(PatternTypes.MOTIF_FANOUT, pattern_hash)
                
                if pattern_id not in patterns_by_id:
                    
                    fanout_volume = sum(data['amount_usd_sum'] for _, _, data in G.out_edges(node, data=True))
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.MOTIF_FANOUT,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': all_addresses,
                        'address_roles': ['center'] + ['destination'] * len(out_neighbors),
                        'motif_type': 'fanout',
                        'motif_center_address': node,
                        'motif_participant_count': in_deg + out_deg,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': out_deg,
                        'evidence_volume_usd': fanout_volume,
                        'detection_method': DetectionMethods.MOTIF_DETECTION
                    }
                
        return list(patterns_by_id.values())