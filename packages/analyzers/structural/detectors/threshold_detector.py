import time
from typing import Dict, List, Set
import networkx as nx
import numpy as np
from loguru import logger

from packages.analyzers.structural.base_detector import BasePatternDetector
from packages.storage.constants import PatternTypes, DetectionMethods
from packages.utils.pattern_utils import generate_pattern_hash, generate_pattern_id


class ThresholdDetector(BasePatternDetector):
    """
    Detector for threshold evasion patterns.
    Identifies addresses that systematically avoid transaction reporting thresholds
    by structuring transactions to stay just below specified limits.
    """

    def _validate_config(self) -> None:
        """Validate that threshold_detection configuration is present."""
        if "threshold_detection" not in self.config:
            raise ValueError("Missing 'threshold_detection' section in configuration")
        if "severity_adjustments" not in self.config:
            raise ValueError("Missing 'severity_adjustments' section in configuration")

    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect threshold evasion patterns in the graph.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of detected threshold evasion pattern dictionaries
        """
        patterns_by_id = {}
        threshold_config = self.config["threshold_detection"]
        
        # Get thresholds from configuration
        thresholds = self._get_thresholds(threshold_config)
        
        # Configuration values
        min_transactions = threshold_config.get("min_transactions_near_threshold", 5)
        clustering_threshold = threshold_config.get("clustering_score_threshold", 0.7)
        consistency_threshold = threshold_config.get("size_consistency_threshold", 0.8)
        confidence_score = threshold_config.get("confidence_score", 0.80)
        risk_score_multiplier = threshold_config.get("risk_score_multiplier", 0.9)
        
        # Analyze each node for threshold evasion
        for node in G.nodes():
            for threshold_type, threshold_value in thresholds.items():
                evasion_pattern = self._analyze_threshold_evasion(
                    G, node, threshold_value, threshold_type,
                    min_transactions, clustering_threshold, consistency_threshold
                )
                
                if evasion_pattern:
                    pattern_hash = generate_pattern_hash(
                        PatternTypes.THRESHOLD_EVASION,
                        [node, threshold_type, str(threshold_value)]
                    )
                    pattern_id = generate_pattern_id(PatternTypes.THRESHOLD_EVASION, pattern_hash)
                    
                    if pattern_id in patterns_by_id:
                        continue
                    
                    base_severity = evasion_pattern['threshold_avoidance_score']
                    severity_score = self._adjust_severity_for_trust(base_severity, [node])
                    
                    patterns_by_id[pattern_id] = {
                        'pattern_id': pattern_id,
                        'pattern_type': PatternTypes.THRESHOLD_EVASION,
                        'pattern_hash': pattern_hash,
                        'addresses_involved': [node],
                        'address_roles': ['primary_address'],
                        'severity_score': severity_score,
                        'confidence_score': confidence_score,
                        'risk_score': min(severity_score * risk_score_multiplier, 1.0),
                        'primary_address': node,
                        'threshold_value': threshold_value,
                        'threshold_type': threshold_type,
                        'transactions_near_threshold': evasion_pattern['transactions_near_threshold'],
                        'avg_transaction_size': evasion_pattern['avg_transaction_size'],
                        'max_transaction_size': evasion_pattern['max_transaction_size'],
                        'size_consistency': evasion_pattern['size_consistency'],
                        'clustering_score': evasion_pattern['clustering_score'],
                        'unique_days': evasion_pattern['unique_days'],
                        'avg_daily_transactions': evasion_pattern['avg_daily_transactions'],
                        'temporal_spread_score': evasion_pattern['temporal_spread_score'],
                        'threshold_avoidance_score': evasion_pattern['threshold_avoidance_score'],
                        'detection_timestamp': int(time.time()),
                        'evidence_transaction_count': evasion_pattern['transactions_near_threshold'],
                        'evidence_volume_usd': evasion_pattern['avg_transaction_size'] * evasion_pattern['transactions_near_threshold'],
                        'detection_method': DetectionMethods.TEMPORAL_ANALYSIS,
                        'anomaly_score': severity_score
                    }
        
        return list(patterns_by_id.values())

    def _get_thresholds(self, config: Dict) -> Dict[str, float]:
        """
        Extract threshold values from configuration.
        
        Args:
            config: Threshold detection configuration
            
        Returns:
            Dictionary mapping threshold type to value
        """
        thresholds = {}
        
        # Standard reporting thresholds
        if "reporting_threshold_usd" in config:
            thresholds["reporting"] = config["reporting_threshold_usd"]
        
        # Custom thresholds
        if "custom_thresholds" in config:
            for threshold_name, threshold_value in config["custom_thresholds"].items():
                thresholds[threshold_name] = threshold_value
        
        # Default fallback
        if not thresholds:
            thresholds["default"] = 10000.0  # $10,000 default
        
        return thresholds

    def _analyze_threshold_evasion(
        self,
        G: nx.DiGraph,
        node: str,
        threshold: float,
        threshold_type: str,
        min_transactions: int,
        clustering_threshold: float,
        consistency_threshold: float
    ) -> Dict:
        """
        Analyze a node for threshold evasion patterns.
        
        Args:
            G: Graph containing the node
            node: Address to analyze
            threshold: Threshold value to check against
            threshold_type: Type of threshold being checked
            min_transactions: Minimum number of near-threshold transactions
            clustering_threshold: Minimum clustering score to detect pattern
            consistency_threshold: Minimum size consistency score
            
        Returns:
            Dictionary with evasion details if detected, None otherwise
        """
        # Collect transaction amounts from outgoing edges
        transaction_amounts = []
        for _, target, data in G.out_edges(node, data=True):
            amount = data.get('amount_usd_sum', 0)
            tx_count = data.get('tx_count', 1)
            
            # Estimate individual transaction sizes if aggregated
            if tx_count > 1:
                avg_amount = amount / tx_count
                transaction_amounts.extend([avg_amount] * tx_count)
            else:
                transaction_amounts.append(amount)
        
        if len(transaction_amounts) < min_transactions:
            return None
        
        # Define "near threshold" range (e.g., 80-99% of threshold)
        threshold_config = self.config["threshold_detection"]
        near_threshold_lower = threshold * threshold_config.get("near_threshold_lower_pct", 0.80)
        near_threshold_upper = threshold * threshold_config.get("near_threshold_upper_pct", 0.99)
        
        # Count transactions near the threshold
        near_threshold_txs = [
            amt for amt in transaction_amounts
            if near_threshold_lower <= amt <= near_threshold_upper
        ]
        
        if len(near_threshold_txs) < min_transactions:
            return None
        
        # Calculate clustering score (how concentrated are txs near threshold)
        all_amounts = np.array(transaction_amounts)
        near_amounts = np.array(near_threshold_txs)
        
        clustering_score = len(near_threshold_txs) / len(transaction_amounts)
        
        if clustering_score < clustering_threshold:
            return None
        
        # Calculate size consistency (how similar are the near-threshold amounts)
        if len(near_amounts) > 1:
            cv = np.std(near_amounts) / max(np.mean(near_amounts), 1.0)
            size_consistency = max(0, 1.0 - cv)  # Lower CV = higher consistency
        else:
            size_consistency = 1.0
        
        if size_consistency < consistency_threshold:
            return None
        
        # Calculate temporal spread (placeholder - would need timestamp data)
        unique_days = 1  # Placeholder
        avg_daily_transactions = len(near_threshold_txs)  # Placeholder
        temporal_spread_score = 0.5  # Placeholder
        
        # Calculate overall avoidance score
        threshold_avoidance_score = self._calculate_avoidance_score(
            clustering_score, size_consistency, temporal_spread_score
        )
        
        return {
            'transactions_near_threshold': len(near_threshold_txs),
            'avg_transaction_size': float(np.mean(near_amounts)),
            'max_transaction_size': float(np.max(near_amounts)),
            'size_consistency': size_consistency,
            'clustering_score': clustering_score,
            'unique_days': unique_days,
            'avg_daily_transactions': avg_daily_transactions,
            'temporal_spread_score': temporal_spread_score,
            'threshold_avoidance_score': threshold_avoidance_score
        }

    def _calculate_avoidance_score(
        self,
        clustering_score: float,
        consistency_score: float,
        temporal_score: float
    ) -> float:
        """
        Calculate overall threshold avoidance score.
        
        Args:
            clustering_score: How concentrated txs are near threshold
            consistency_score: How consistent the transaction sizes are
            temporal_score: How spread out the transactions are over time
            
        Returns:
            Avoidance score between 0 and 1
        """
        threshold_config = self.config["threshold_detection"]
        
        # Weight factors
        clustering_weight = threshold_config.get("clustering_severity_weight", 0.4)
        consistency_weight = threshold_config.get("consistency_severity_weight", 0.4)
        temporal_weight = threshold_config.get("temporal_severity_weight", 0.2)
        
        score = (
            clustering_score * clustering_weight +
            consistency_score * consistency_weight +
            temporal_score * temporal_weight
        )
        
        return min(score, 1.0)