import time
from typing import Dict, List
import networkx as nx
from loguru import logger

from packages.analyzers.structural.base_detector import BasePatternDetector
from chainswarm_core.constants.patterns import PatternTypes, DetectionMethods
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class CycleDetector(BasePatternDetector):
    """
    Detector for circular transaction patterns.
    Identifies cycles in the transaction graph that may indicate fund circulation.
    """

    def _validate_config(self) -> None:
        """Validate that cycle_detection configuration is present."""
        if "cycle_detection" not in self.config:
            raise ValueError("Missing 'cycle_detection' section in configuration")

    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect cycle patterns in the graph.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected cycle pattern dictionaries
        """
        patterns_by_id = {}
        cycle_config = self.config["cycle_detection"]
        max_cycle_length = cycle_config["max_cycle_length"]
        max_cycles_per_scc = cycle_config["max_cycles_per_scc"]
        min_cycle_length = cycle_config["min_cycle_length"]
        
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
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.CYCLE,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': sorted_cycle,
                        'address_roles': ['participant'] * len(sorted_cycle),
                        'cycle_path': cycle,
                        'cycle_length': len(cycle),
                        'cycle_volume_usd': cycle_volume,
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': len(cycle),
                        'evidence_volume_usd': cycle_volume,
                        'detection_method': DetectionMethods.CYCLE_DETECTION
                    }
                    
                    cycles_found += 1
                    
            except Exception as e:
                logger.warning(f"Cycle detection failed for SCC of size {len(scc)}: {e}")
                
        return list(patterns_by_id.values())

    def _calculate_cycle_volume(self, G: nx.DiGraph, cycle: List[str]) -> float:
        """
        Calculate total volume in a cycle.
        
        Args:
            G: Graph containing the cycle
            cycle: List of addresses forming the cycle
            
        Returns:
            Total USD volume in the cycle
        """
        total_volume = 0.0
        for i in range(len(cycle)):
            from_addr = cycle[i]
            to_addr = cycle[(i + 1) % len(cycle)]
            if G.has_edge(from_addr, to_addr):
                total_volume += G[from_addr][to_addr]['amount_usd_sum']
        return total_volume