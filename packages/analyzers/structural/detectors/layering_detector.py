import time
from typing import Dict, List
import networkx as nx
import numpy as np
from loguru import logger

from packages.analyzers.structural.base_detector import BasePatternDetector
from packages.storage.constants import PatternTypes, DetectionMethods
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class LayeringDetector(BasePatternDetector):
    """
    Detector for layering patterns in transaction flows.
    Identifies paths with consistent volumes that may indicate fund layering.
    """

    def _validate_config(self) -> None:
        """Validate that path_analysis configuration is present."""
        if "path_analysis" not in self.config:
            raise ValueError("Missing 'path_analysis' section in configuration")

    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect layering patterns in the graph.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected layering pattern dictionaries
        """
        patterns_by_id = {}
        path_config = self.config["path_analysis"]
        
        min_path_length = path_config["min_path_length"]
        max_path_length = path_config["max_path_length"]
        max_paths_to_check = path_config["max_paths_to_check"]
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
                            
                            patterns_by_id[pattern_id] = {
                                'pattern_id': pattern_id,
                                'pattern_type': PatternTypes.LAYERING_PATH,
                                'pattern_hash': pattern_hash,
                                'addresses_involved': sorted_path,
                                'address_roles': ['source'] + ['intermediary'] * (len(path) - 2) + ['destination'],
                                'layering_path': path,
                                'path_depth': len(path),
                                'path_volume_usd': path_volume,
                                'source_address': source,
                                'destination_address': target,
                                'detection_timestamp': int(time.time()),
                                'evidence_transaction_count': len(path) - 1,
                                'evidence_volume_usd': path_volume,
                                'detection_method': DetectionMethods.PATH_ANALYSIS
                            }
                                
                except nx.NetworkXNoPath:
                    continue
                except Exception as e:
                    logger.warning(f"Path detection failed between {source} and {target}: {e}")
                    continue
                    
        return list(patterns_by_id.values())

    def _calculate_path_volume(self, G: nx.DiGraph, path: List[str]) -> float:
        """
        Calculate total volume along a path.
        
        Args:
            G: Graph containing the path
            path: List of addresses forming the path
            
        Returns:
            Total USD volume along the path
        """
        total_volume = 0.0
        for i in range(len(path) - 1):
            if G.has_edge(path[i], path[i + 1]):
                total_volume += G[path[i]][path[i + 1]]['amount_usd_sum']
        return total_volume

    def _calculate_path_density(self, G: nx.DiGraph, path: List[str]) -> float:
        """
        Calculate density of connections in a path subgraph.
        
        Args:
            G: Graph containing the path
            path: List of addresses forming the path
            
        Returns:
            Density value between 0 and 1
        """
        if len(path) < 2:
            return 0.0
        subgraph = G.subgraph(path)
        return nx.density(subgraph)

    def _is_layering_pattern(self, G: nx.DiGraph, path: List[str], volume: float) -> bool:
        """
        Determine if a path exhibits layering characteristics.
        
        Args:
            G: Graph containing the path
            path: List of addresses forming the path
            volume: Total volume in the path
            
        Returns:
            True if path shows layering characteristics
        """
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