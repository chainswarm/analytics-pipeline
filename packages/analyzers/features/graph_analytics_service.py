import time
import traceback
from decimal import Decimal
from typing import Dict, Any, List, Optional
from loguru import logger
from collections import defaultdict
import numpy as np

from packages.storage.repositories.graph_analytics_repository import GraphAnalyticsRepository
from packages.storage.config import AnalyticsConfig


class GraphAnalyticsService:
    """Service for graph algorithm computation and feature enhancement."""
    
    def __init__(
        self,
        graph_analytics_repository: GraphAnalyticsRepository,
        analytics_config: AnalyticsConfig,
        network: str,
        processing_date: str
    ):
        self.graph_analytics_repository = graph_analytics_repository
        self.analytics_config = analytics_config
        self.network = network
        self.processing_date = processing_date
        
        self.features_table = analytics_config.get_features_windowed_table_name(processing_date)
        
        logger.info(
            "Graph analytics service initialized",
            business_decision="initialize_analytics_service",
            reason="algorithm_computation_setup",
            extra={
                "network": network,
                "features_table": self.features_table
            }
        )
    
    def validate_algorithm_dependencies(self, flows_table: str) -> bool:
        """Validate that required dependencies exist before running algorithms."""
        return self.graph_analytics_repository.validate_feature_dependencies(
            self.features_table,
            flows_table
        )
    
    def get_comprehensive_node_data_for_algorithms(self, addresses: List[str]) -> List[Dict]:
        """Get comprehensive node data for algorithm processing."""
        return self.graph_analytics_repository.get_comprehensive_node_data(
            self.features_table,
            addresses
        )
    
    def update_algorithm_results(self, algorithm_results: Dict[str, Dict]) -> int:
        """Update feature tables with computed algorithm results."""
        feature_updates = self._build_feature_update_data(algorithm_results)
        
        return self.graph_analytics_repository.update_graph_features_batch(
            self.features_table,
            feature_updates
        )
    
    def _build_feature_update_data(self, algorithm_results: Dict[str, Dict]) -> Dict[str, Dict[str, Any]]:
        """Build update data structure from algorithm results."""
        update_data = defaultdict(dict)
        
        for algorithm, results in algorithm_results.items():
            if algorithm == 'communities':
                for address, community_id in results.items():
                    update_data[address]['community_id'] = community_id
                    
            elif algorithm == 'pagerank':
                for address, score in results.items():
                    update_data[address]['pagerank'] = score
                    
            elif algorithm == 'kcore':
                for address, kcore in results.items():
                    update_data[address]['kcore'] = kcore
                    
            elif algorithm == 'clustering':
                for address, clustering in results.items():
                    update_data[address]['clustering_coefficient'] = clustering
                    
            elif algorithm == 'betweenness':
                for address, betweenness in results.items():
                    update_data[address]['betweenness'] = betweenness
                    
            elif algorithm == 'khop':
                for address, khop_data in results.items():
                    update_data[address].update(khop_data)
        
        logger.info(
            "Feature update data built from algorithm results",
            business_decision="prepare_feature_updates",
            reason="algorithm_results_processed",
            extra={
                "addresses_to_update": len(update_data),
                "algorithms_processed": list(algorithm_results.keys())
            }
        )
        
        return update_data
    
    def calculate_risk_scores(self, node_data: List[Dict]) -> Dict[str, float]:
        """Calculate risk scores from comprehensive node data."""
        risk_scores = {}
        
        for data in node_data:
            address = data['address']
            risk_score = 0.0
            
            # High velocity increases risk
            velocity = data.get('velocity_score', 0.0)
            risk_score += velocity * 0.3
            
            # High structuring score increases risk
            structuring = data.get('structuring_score', 0.0)
            risk_score += structuring * 0.4
            
            # Very high volume can indicate risk
            volume = data.get('total_volume_usd', 0.0)
            if volume > 10000000:  # >10M USD
                risk_score += 0.2
                
            # Very high degree can indicate mixing/hub behavior
            degree = data.get('degree_total', 0)
            if degree > 1000:
                risk_score += 0.1
                
            risk_scores[address] = min(risk_score, 1.0)  # Cap at 1.0
        
        logger.info(
            "Risk scores calculated for algorithm enhancement",
            business_decision="enhance_risk_analysis",
            reason="comprehensive_risk_assessment",
            extra={
                "addresses_scored": len(risk_scores),
                "avg_risk_score": np.mean(list(risk_scores.values())) if risk_scores else 0.0
            }
        )
        
        return risk_scores