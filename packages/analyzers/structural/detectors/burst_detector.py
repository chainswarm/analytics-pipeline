import time
from typing import Dict, List
import networkx as nx
from loguru import logger

from packages.analyzers.structural.base_detector import BasePatternDetector
from packages.storage.constants import PatternTypes, DetectionMethods
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class BurstDetector(BasePatternDetector):
    """
    Detector for temporal burst patterns.
    Identifies sudden spikes in transaction activity that may indicate suspicious behavior.
    
    NOTE: Current implementation requires timestamp data on graph edges.
    Returns empty list if timestamp data is not available.
    """

    def _validate_config(self) -> None:
        """Validate that burst_detection configuration is present."""
        if "burst_detection" not in self.config:
            raise ValueError("Missing 'burst_detection' section in configuration")
        if "severity_adjustments" not in self.config:
            raise ValueError("Missing 'severity_adjustments' section in configuration")

    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect temporal burst patterns in the graph.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected burst pattern dictionaries
            
        Note:
            Returns empty list if timestamp data is not available on edges.
        """
        # Check if graph has timestamp data
        if not self._has_timestamp_data(G):
            logger.warning(
                "BurstDetector: Graph edges do not contain timestamp data. "
                "Burst detection requires 'timestamps' attribute on edges. "
                "Returning empty pattern list."
            )
            return []
        
        patterns_by_id = {}
        burst_config = self.config["burst_detection"]
        
        # Get configuration values
        min_burst_intensity = burst_config.get("min_burst_intensity", 3.0)
        min_burst_transactions = burst_config.get("min_burst_transactions", 10)
        time_window_seconds = burst_config.get("time_window_seconds", 3600)  # 1 hour default
        z_score_threshold = burst_config.get("z_score_threshold", 2.0)
        confidence_score = burst_config.get("confidence_score", 0.75)
        risk_score_multiplier = burst_config.get("risk_score_multiplier", 0.8)
        
        # Analyze each node for burst patterns
        for node in G.nodes():
            burst_pattern = self._analyze_temporal_bursts(
                G, node, time_window_seconds, min_burst_intensity, 
                min_burst_transactions, z_score_threshold
            )
            
            if burst_pattern:
                pattern_hash = generate_pattern_hash(
                    PatternTypes.TEMPORAL_BURST,
                    [node, str(burst_pattern['burst_start_timestamp'])]
                )
                pattern_id = generate_pattern_id(PatternTypes.TEMPORAL_BURST, pattern_hash)
                
                if pattern_id in patterns_by_id:
                    continue
                
                base_severity = self._calculate_burst_severity(burst_pattern)
                severity_score = self._adjust_severity_for_trust(base_severity, [node])
                
                patterns_by_id[pattern_id] = {
                    'pattern_id': pattern_id,
                    'pattern_type': PatternTypes.TEMPORAL_BURST,
                    'pattern_hash': pattern_hash,
                    'addresses_involved': [node],
                    'address_roles': ['burst_source'],
                    'severity_score': severity_score,
                    'confidence_score': confidence_score,
                    'risk_score': min(severity_score * risk_score_multiplier, 1.0),
                    'burst_address': node,
                    'burst_start_timestamp': burst_pattern['burst_start_timestamp'],
                    'burst_end_timestamp': burst_pattern['burst_end_timestamp'],
                    'burst_duration_seconds': burst_pattern['burst_duration_seconds'],
                    'burst_transaction_count': burst_pattern['burst_transaction_count'],
                    'burst_volume_usd': burst_pattern['burst_volume_usd'],
                    'normal_tx_rate': burst_pattern['normal_tx_rate'],
                    'burst_tx_rate': burst_pattern['burst_tx_rate'],
                    'burst_intensity': burst_pattern['burst_intensity'],
                    'z_score': burst_pattern['z_score'],
                    'hourly_distribution': burst_pattern.get('hourly_distribution', []),
                    'peak_hours': burst_pattern.get('peak_hours', []),
                    'detection_timestamp': int(time.time()),
                    'evidence_transaction_count': burst_pattern['burst_transaction_count'],
                    'evidence_volume_usd': burst_pattern['burst_volume_usd'],
                    'detection_method': DetectionMethods.TEMPORAL_ANALYSIS,
                    'anomaly_score': severity_score
                }
        
        return list(patterns_by_id.values())

    def _has_timestamp_data(self, G: nx.DiGraph) -> bool:
        """
        Check if graph edges contain timestamp data.
        
        Args:
            G: Graph to check
            
        Returns:
            True if timestamp data is available
        """
        # Check a sample of edges for timestamp data
        for u, v, data in G.edges(data=True):
            # Look for timestamp arrays or similar attributes
            if 'timestamps' in data or 'timestamp' in data:
                return True
            # Only check first edge as indicator
            return False
        return False

    def _analyze_temporal_bursts(
        self,
        G: nx.DiGraph,
        node: str,
        time_window: int,
        min_intensity: float,
        min_transactions: int,
        z_threshold: float
    ) -> Dict:
        """
        Analyze a node for temporal burst patterns.
        
        Args:
            G: Graph containing the node
            node: Address to analyze
            time_window: Time window in seconds for burst detection
            min_intensity: Minimum burst intensity ratio
            min_transactions: Minimum number of transactions in burst
            z_threshold: Z-score threshold for burst detection
            
        Returns:
            Dictionary with burst details if detected, None otherwise
        """
        # Placeholder implementation - would need actual timestamp data
        # This is a skeleton that shows the expected return structure
        
        # In real implementation, this would:
        # 1. Collect all timestamps from edges involving this node
        # 2. Identify time windows with abnormally high activity
        # 3. Calculate statistical measures (z-score, intensity)
        # 4. Return burst details if thresholds exceeded
        
        return None

    def _calculate_burst_severity(self, burst_pattern: Dict) -> float:
        """
        Calculate severity score for a burst pattern.
        
        Args:
            burst_pattern: Dictionary containing burst metrics
            
        Returns:
            Severity score between 0 and 1
        """
        burst_config = self.config["burst_detection"]
        
        # Weight factors
        intensity_weight = burst_config.get("intensity_severity_weight", 0.4)
        volume_weight = burst_config.get("volume_severity_weight", 0.3)
        z_score_weight = burst_config.get("z_score_severity_weight", 0.3)
        
        # Normalize factors to 0-1 range
        intensity_factor = min(burst_pattern['burst_intensity'] / 10.0, 1.0)
        volume_factor = min(burst_pattern['burst_volume_usd'] / 100000.0, 1.0)  # $100k threshold
        z_score_factor = min(burst_pattern['z_score'] / 5.0, 1.0)  # z-score of 5 = max
        
        severity = (
            intensity_factor * intensity_weight +
            volume_factor * volume_weight +
            z_score_factor * z_score_weight
        )
        
        return min(severity, 1.0)