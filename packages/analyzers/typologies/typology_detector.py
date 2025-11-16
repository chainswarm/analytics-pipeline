import time
import json
import uuid
from typing import Any, Dict, List, Optional
from decimal import Decimal
from enum import Enum
from collections import defaultdict
from loguru import logger

from packages.analyzers.typologies.typology_config_loader import load_typology_config
from packages.storage.repositories.alerts_repository import AlertsRepository
from packages.storage.repositories.alert_cluster_repository import AlertClusterRepository
from packages.storage.repositories.feature_repository import FeatureRepository
from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.structural_pattern_repository import StructuralPatternRepository
from packages.utils.decorators import log_errors
from packages.storage.constants import AddressTypes, Severities, PatternTypes


class TypologyType(Enum):
    PEEL_CHAIN = "PEEL_CHAIN"
    STRUCTURING = "STRUCTURING"
    PING_PONG = "PING_PONG"
    HUB_ANOMALY = "HUB_ANOMALY"
    FRESH_TO_EXCHANGE = "FRESH_TO_EXCHANGE"
    RAPID_FANOUT = "RAPID_FANOUT"
    MIXING_BEHAVIOR = "MIXING_BEHAVIOR"
    INTRADAY_STRUCTURING = "INTRADAY_STRUCTURING"
    VELOCITY_ANOMALY = "VELOCITY_ANOMALY"
    TIME_BASED_PATTERN = "TIME_BASED_PATTERN"
    # Structural pattern typologies
    CYCLE_DETECTION = "CYCLE_DETECTION"
    LAYERING_PATH = "LAYERING_PATH"
    SMURFING_NETWORK = "SMURFING_NETWORK"
    PROXIMITY_RISK = "PROXIMITY_RISK"
    MOTIF_FANIN = "MOTIF_FANIN"
    MOTIF_FANOUT = "MOTIF_FANOUT"


class TypologyDetector:
    """Simplified typology detector using string-based operations."""
    
    def __init__(
        self,
        alerts_repository: AlertsRepository,
        alert_cluster_repository: AlertClusterRepository,
        feature_repository: FeatureRepository,
        money_flows_repository: MoneyFlowsRepository,
        structural_pattern_repository: StructuralPatternRepository,
        window_days: int,
        start_timestamp: int,
        end_timestamp: int,
        network: str,
        processing_date: str
    ):
        self.alerts_repository = alerts_repository
        self.alert_cluster_repository = alert_cluster_repository
        self.feature_repository = feature_repository
        self.money_flows_repository = money_flows_repository
        self.structural_pattern_repository = structural_pattern_repository

        self.window_days = window_days
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.network = network
        self.processing_date = processing_date
        self.typology_config = load_typology_config()
        
        clustering_config = self.typology_config.get('clustering', {})
        self.clustering_strategies = clustering_config.get('strategies', {})

    @log_errors
    def detect_typologies(self) -> None:
        """Main method to detect typologies and create alerts."""
        all_alerts = []
        comprehensive_data = self.feature_repository.get_addresses_comprehensive_data()
        batch_alerts = self._detect_batch_typologies(comprehensive_data)
        all_alerts.extend(batch_alerts)

        if all_alerts:
            valid_alerts = [alert for alert in all_alerts if alert]
            if valid_alerts:
                alert_dicts = [self._alert_to_dict(alert) for alert in valid_alerts]
                self.alerts_repository.insert_alerts(
                    alert_dicts,
                    window_days=self.window_days,
                    processing_date=self.processing_date
                )
                logger.info(f"Inserted {len(alert_dicts)} alerts")
                
                clusters = self._create_alert_clusters(alert_dicts)
                if clusters:
                    for cluster in clusters:
                        self.alert_cluster_repository.create_cluster(
                            cluster,
                            window_days=self.window_days,
                            processing_date=self.processing_date
                        )
                    logger.success(f"Created {len(clusters)} alert clusters")
                else:
                    logger.info("No clusters created (addresses have single alerts)")

    def _detect_batch_typologies(self, comprehensive_data: List[Dict]) -> List[Dict]:
        """Detect all typologies for batch of addresses."""
        batch_alerts = []
        
        for address_data in comprehensive_data:
            address = address_data['address']
            
            # Run all detection algorithms
            alerts = []
            
            # 1. Peel Chain Detection
            peel_alert = self._detect_peel_chain(address, address_data)
            if peel_alert:
                alerts.append(peel_alert)
            
            # 2. Structuring Detection
            structuring_alert = self._detect_structuring(address, address_data)
            if structuring_alert:
                alerts.append(structuring_alert)
            
            # 3. Ping-Pong Detection
            ping_pong_alert = self._detect_ping_pong(address, address_data)
            if ping_pong_alert:
                alerts.append(ping_pong_alert)
            
            # 4. Hub Anomaly Detection - DISABLED (requires ML classification: is_exchange_like)
            # hub_alert = self._detect_hub_anomaly(address, address_data)
            # if hub_alert:
            #     alerts.append(hub_alert)
            
            # 6. Rapid Fanout Detection
            fanout_alert = self._detect_rapid_fanout(address, address_data)
            if fanout_alert:
                alerts.append(fanout_alert)
            
            # 7. Mixing Behavior Detection - DISABLED (requires ML classification: is_mixer_like)
            # mixing_alert = self._detect_mixing_behavior(address, address_data)
            # if mixing_alert:
            #     alerts.append(mixing_alert)
            
            # 8. Velocity Anomaly Detection
            velocity_alert = self._detect_velocity_anomaly(address, address_data)
            if velocity_alert:
                alerts.append(velocity_alert)
            
            batch_alerts.extend(alerts)

        # BATCHED Fresh-to-Exchange Detection
        #TODO: this is not optimal implementation, it causes SELECT n + 1 isse: KW
        fresh_alerts = self._detect_fresh_to_exchange_in_batch(comprehensive_data)
        batch_alerts.extend(fresh_alerts)
        
        # Structural Pattern Detection
        structural_alerts = self._detect_structural_patterns()
        batch_alerts.extend(structural_alerts)
        
        return batch_alerts

    def _detect_peel_chain(self, address: str, data: Dict) -> Optional[Dict]:
        """Detect peel chain pattern."""
        degree_out = data['degree_total']
        volume_usd = data['total_volume_usd']
        burst_factor = data['burst_factor']

        # Convert potential Decimal types to float for mathematical operations
        degree_out = int(degree_out) if degree_out else 0
        volume_usd = float(volume_usd) if volume_usd else 0.0
        burst_factor = float(burst_factor) if burst_factor else 0.0

        # Check basic peel chain criteria
        if (degree_out >= self.typology_config['peel_chain']['min_recipients'] and
            volume_usd >= self.typology_config['peel_chain']['min_volume_usd'] and
            burst_factor < 0.3):  # Low burst indicates sequential pattern

            confidence = min(
                (degree_out / 20.0) * 0.4 +
                min(volume_usd / 50000.0, 1.0) * 0.3 +
                (1 - burst_factor) * 0.3,
                1.0
            )

            if confidence > 0.6:
                return self._create_alert(
                    address=address,
                    typology_type=TypologyType.PEEL_CHAIN,
                    confidence_score=confidence,
                    evidence={
                        'degree_out': degree_out,
                        'volume_usd': volume_usd,
                        'burst_factor': burst_factor,
                        'sequential_pattern_indicator': True
                    },
                    risk_indicators=['long_chain', 'sequential_transfers', 'low_branching'],
                    description=f"Peel chain detected: {degree_out} recipients, ${volume_usd:.2f}"
                )
        return None

    def _detect_structuring(self, address: str, data: Dict) -> Optional[Dict]:
        """Detect structuring pattern."""
        structuring_score = data['structuring_score']
        tx_count = data['tx_total_count']
        volume_usd = data['total_volume_usd']

        # Convert potential Decimal types to float for mathematical operations
        structuring_score = float(structuring_score) if structuring_score else 0.0
        tx_count = int(tx_count) if tx_count else 0
        volume_usd = float(volume_usd) if volume_usd else 0.0

        # Check structuring criteria
        if (structuring_score > self.typology_config['structuring']['min_score'] and
            tx_count >= self.typology_config['structuring']['min_transactions'] and
            volume_usd > 0):

            avg_tx_size = volume_usd / tx_count if tx_count > 0 else 0.0

            # Enhanced structuring detection
            if avg_tx_size < self.typology_config['structuring']['max_amount_usd']:
                confidence = min(
                    structuring_score * 0.5 +
                    min(tx_count / 50.0, 1.0) * 0.3 +
                    (1 - min(avg_tx_size / 10000.0, 1.0)) * 0.2,
                    1.0
                )

                if confidence > 0.6:
                    return self._create_alert(
                        address=address,
                        typology_type=TypologyType.STRUCTURING,
                        confidence_score=confidence,
                        evidence={
                            'structuring_score': structuring_score,
                            'transaction_count': tx_count,
                            'avg_transaction_size': avg_tx_size,
                            'volume_usd': volume_usd
                        },
                        risk_indicators=['small_transactions', 'below_threshold', 'frequent_activity'],
                        description=f"Structuring detected: {tx_count} transactions, avg ${avg_tx_size:.2f}"
                    )
        return None

    def _detect_ping_pong(self, address: str, data: Dict) -> Optional[Dict]:
        """Detect ping-pong pattern."""
        reciprocity_ratio = data['reciprocity_ratio']
        degree_total = data['degree_total']
        volume_usd = data['total_volume_usd']

        # Convert potential Decimal types to float for mathematical operations
        reciprocity_ratio = float(reciprocity_ratio) if reciprocity_ratio else 0.0
        degree_total = int(degree_total) if degree_total else 0
        volume_usd = float(volume_usd) if volume_usd else 0.0

        # High reciprocity with limited counterparties suggests ping-pong
        if (reciprocity_ratio > self.typology_config['ping_pong']['min_reciprocity'] and
            degree_total < self.typology_config['ping_pong']['max_counterparties'] and
            volume_usd >= self.typology_config['ping_pong']['min_volume']):

            confidence = min(
                reciprocity_ratio * 0.5 +
                (1 - degree_total / 20.0) * 0.3 +
                min(volume_usd / 10000.0, 1.0) * 0.2,
                1.0
            )

            if confidence > 0.7:
                return self._create_alert(
                    address=address,
                    typology_type=TypologyType.PING_PONG,
                    confidence_score=confidence,
                    evidence={
                        'reciprocity_ratio': reciprocity_ratio,
                        'degree_total': degree_total,
                        'volume_usd': volume_usd,
                        'bidirectional_pattern': True
                    },
                    risk_indicators=['high_reciprocity', 'limited_counterparties', 'bidirectional_flows'],
                    description=f"Ping-pong pattern: {reciprocity_ratio:.2f} reciprocity ratio"
                )
        return None

    def _detect_hub_anomaly(self, address: str, data: Dict) -> Optional[Dict]:
        """Detect hub anomaly pattern."""
        degree_total = data['degree_total']
        pagerank = data['pagerank']
        velocity_score = data['velocity_score']
        is_exchange_like = data['is_exchange_like']
        volume_usd = data['total_volume_usd']

        # Convert potential Decimal types to float for mathematical operations
        degree_total = int(degree_total) if degree_total else 0
        pagerank = float(pagerank) if pagerank else 0.0
        velocity_score = float(velocity_score) if velocity_score else 0.0
        volume_usd = float(volume_usd) if volume_usd else 0.0

        # High degree non-exchange with high centrality
        if (degree_total >= self.typology_config['hub_anomaly']['min_degree'] and
            not is_exchange_like and
            pagerank > self.typology_config['hub_anomaly']['min_pagerank']):

            confidence = min(
                (degree_total / 100.0) * 0.3 +
                min(pagerank * 10000, 1.0) * 0.3 +
                velocity_score * 0.2 +
                min(volume_usd / 100000.0, 1.0) * 0.2,
                1.0
            )

            if confidence > 0.7:
                return self._create_alert(
                    address=address,
                    typology_type=TypologyType.HUB_ANOMALY,
                    confidence_score=confidence,
                    evidence={
                        'degree_total': degree_total,
                        'pagerank': pagerank,
                        'velocity_score': velocity_score,
                        'volume_usd': volume_usd,
                        'is_exchange_like': is_exchange_like
                    },
                    risk_indicators=['high_degree', 'central_position', 'non_exchange', 'high_velocity'],
                    description=f"Hub anomaly: {degree_total} connections, PageRank {pagerank:.6f}"
                )
        return None

    def _detect_fresh_to_exchange_in_batch(self, comprehensive_data: List[Dict]) -> List[Dict]:
        """
        Detect fresh-to-exchange pattern using optimized repository method.
        """
        fresh_addresses = [
            d['address'] for d in comprehensive_data
            if d['is_new_address'] and float(d['total_out_usd']) >= self.typology_config['fresh_to_exchange']['min_volume_usd']
        ]

        if not fresh_addresses:
            return []

        flows = self.money_flows_repository.get_fresh_to_exchange_flows(
            fresh_addresses=fresh_addresses,
            network=self.network,
            start_ts=self.start_timestamp,
            end_ts=self.end_timestamp
        )
        
        if not flows:
            return []
        
        flows_by_source = defaultdict(list)
        for flow in flows:
            flows_by_source[flow['from_address']].append({
                'address': flow['to_address'],
                'type': 'CEX',
                'label': flow['label'],
                'volume_usd': float(flow['amount_usd_sum']) if flow['amount_usd_sum'] else 0.0
            })
        
        alerts = []
        for address, exchange_interactions in flows_by_source.items():
            total_exchange_volume = sum(item['volume_usd'] for item in exchange_interactions)
            if total_exchange_volume >= self.typology_config['fresh_to_exchange']['min_volume_usd']:
                alerts.append(self._create_alert(
                    address=address,
                    typology_type=TypologyType.FRESH_TO_EXCHANGE,
                    confidence_score=0.9,
                    evidence={'source_is_new': True, 'exchange_interactions': exchange_interactions, 'volume_usd': total_exchange_volume},
                    risk_indicators=['fresh_funds', 'immediate_exchange_transfer'],
                    description=f"New address sent ${total_exchange_volume:.2f} to {len(exchange_interactions)} known exchange(s)."
                ))
        
        return alerts

    def _detect_rapid_fanout(self, address: str, data: Dict) -> Optional[Dict]:
        """Detect rapid fanout pattern."""
        degree_out = data['degree_total']
        burst_factor = data['burst_factor']
        volume_usd = data['total_volume_usd']

        # Convert potential Decimal types to float for mathematical operations
        degree_out = int(degree_out) if degree_out else 0
        burst_factor = float(burst_factor) if burst_factor else 0.0
        volume_usd = float(volume_usd) if volume_usd else 0.0

        # High burst activity with many recipients
        if (degree_out >= self.typology_config['rapid_fanout']['min_recipients'] and
            burst_factor > self.typology_config['rapid_fanout']['min_burst_factor'] and
            volume_usd >= self.typology_config['rapid_fanout']['min_volume']):

            confidence = min(
                (degree_out / 50.0) * 0.4 +
                burst_factor * 0.4 +
                min(volume_usd / 25000.0, 1.0) * 0.2,
                1.0
            )

            if confidence > 0.6:
                return self._create_alert(
                    address=address,
                    typology_type=TypologyType.RAPID_FANOUT,
                    confidence_score=confidence,
                    evidence={
                        'degree_out': degree_out,
                        'burst_factor': burst_factor,
                        'volume_usd': volume_usd,
                        'rapid_distribution': True
                    },
                    risk_indicators=['many_recipients', 'burst_activity', 'rapid_distribution'],
                    description=f"Rapid fanout: {degree_out} recipients, burst factor {burst_factor:.2f}"
                )
        return None

    def _detect_mixing_behavior(self, address: str, data: Dict) -> Optional[Dict]:
        """Detect mixing behavior pattern."""
        is_mixer_like = data['is_mixer_like']
        degree_total = data['degree_total']
        volume_usd = data['total_volume_usd']
        velocity_score = data['velocity_score']

        # Convert potential Decimal types to float for mathematical operations
        degree_total = int(degree_total) if degree_total else 0
        volume_usd = float(volume_usd) if volume_usd else 0.0
        velocity_score = float(velocity_score) if velocity_score else 0.0

        # Check mixing behavior indicators
        if (is_mixer_like or
            (degree_total >= self.typology_config['mixing_behavior']['min_degree'] and
             velocity_score > self.typology_config['mixing_behavior']['min_velocity'])):

            confidence = 0.0
            if is_mixer_like:
                confidence += 0.5
            if degree_total >= 20:
                confidence += 0.3
            if velocity_score > 0.5:
                confidence += 0.2

            confidence = min(confidence, 1.0)

            if confidence > 0.6:
                return self._create_alert(
                    address=address,
                    typology_type=TypologyType.MIXING_BEHAVIOR,
                    confidence_score=confidence,
                    evidence={
                        'is_mixer_like': is_mixer_like,
                        'degree_total': degree_total,
                        'velocity_score': velocity_score,
                        'volume_usd': volume_usd
                    },
                    risk_indicators=['mixer_pattern', 'multiple_counterparties', 'high_velocity'],
                    description=f"Mixing behavior: mixer-like={is_mixer_like}, {degree_total} connections"
                )
        return None

    def _detect_velocity_anomaly(self, address: str, data: Dict) -> Optional[Dict]:
        """Detect velocity anomaly pattern."""
        velocity_score = data['velocity_score']
        volume_usd = data['total_volume_usd']
        tx_count = data['tx_total_count']
        burst_factor = data['burst_factor']

        # Convert potential Decimal types to float for mathematical operations
        velocity_score = float(velocity_score) if velocity_score else 0.0
        volume_usd = float(volume_usd) if volume_usd else 0.0
        tx_count = int(tx_count) if tx_count else 0
        burst_factor = float(burst_factor) if burst_factor else 0.0

        # High velocity with significant volume using windowed features
        if (velocity_score > self.typology_config['velocity_anomaly']['min_velocity'] and
            volume_usd >= self.typology_config['velocity_anomaly']['min_volume'] and
            burst_factor > 0.7):  # High burst indicates velocity anomaly

            daily_velocity = tx_count / max(self.window_days, 1)

            confidence = min(
                velocity_score * 0.4 +
                burst_factor * 0.3 +
                min(daily_velocity / 20.0, 1.0) * 0.3,
                1.0
            )

            if confidence > 0.7:
                return self._create_alert(
                    address=address,
                    typology_type=TypologyType.VELOCITY_ANOMALY,
                    confidence_score=confidence,
                    evidence={
                        'velocity_score': velocity_score,
                        'burst_factor': burst_factor,
                        'daily_velocity': daily_velocity,
                        'volume_usd': volume_usd
                    },
                    risk_indicators=['high_velocity', 'rapid_movement', 'burst_activity'],
                    description=f"Velocity anomaly: score {velocity_score:.2f}, burst {burst_factor:.2f}"
                )
        return None

    def _create_alert(
        self,
        address: str,
        typology_type: TypologyType,
        confidence_score: float,
        evidence: Dict[str, Any],
        risk_indicators: List[str],
        description: str,
        related_addresses: Optional[List[str]] = None
    ) -> Dict:
        """Create alert with simplified classification."""
        
        # Simple address type inference
        suspected_address_type = self._infer_simple_address_type(typology_type, evidence, risk_indicators)
        
        # Simple severity calculation
        severity = self._calculate_simple_severity(confidence_score)
        
        # Extract volume_usd from evidence (different fields for different typologies)
        # Check each field and use first non-None value, keep as Decimal for precision
        volume_usd = Decimal('0')
        for key in ['volume_usd', 'cycle_volume_usd', 'path_volume_usd', 'evidence_volume_usd']:
            if key in evidence and evidence[key] is not None:
                volume_usd = Decimal(str(evidence[key]))
                break
        
        # Create the core alert object
        alert = {
            'alert_id': str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{address}-{typology_type.value}-{self.processing_date}")),
            'address': address,
            'typology_type': typology_type,
            'confidence_score': Decimal(str(confidence_score)),
            'evidence': evidence,
            'risk_indicators': risk_indicators,
            'severity': severity,  # Simple string value
            'suspected_address_type': suspected_address_type,  # Simple string value
            'description': description,
            'related_addresses': related_addresses or [],
            'volume_usd': volume_usd
        }
        
        return alert

    def _infer_simple_address_type(self, typology_type: TypologyType, evidence: Dict, risk_indicators: List[str]) -> str:
        """Infer suspected address type using simple logic."""
        
        # Pattern-based address type inference
        if typology_type == TypologyType.MIXING_BEHAVIOR:
            # Disabled: is_mixer_like classification removed from analyzers
            return AddressTypes.UNKNOWN
                
        elif typology_type == TypologyType.FRESH_TO_EXCHANGE:
            # Fresh addresses sending to exchanges are likely wallets
            return AddressTypes.WALLET
                
        elif typology_type == TypologyType.HUB_ANOMALY:
            # High-degree non-exchange could be various types
            if evidence.get('volume_usd', 0) > 1000000:  # High volume hub
                return AddressTypes.INSTITUTIONAL
            else:
                return AddressTypes.UNKNOWN
                
        elif typology_type in [TypologyType.PEEL_CHAIN, TypologyType.RAPID_FANOUT]:
            # Distribution patterns could indicate various types
            if evidence.get('volume_usd', 0) > 500000:
                return AddressTypes.MERCHANT  # Could be merchant distribution
            else:
                return AddressTypes.WALLET
                
        elif typology_type == TypologyType.STRUCTURING:
            # Structuring could indicate evasion behavior
            if 'below_threshold' in risk_indicators:
                return AddressTypes.WALLET  # Likely individual evading reporting
            else:
                return AddressTypes.UNKNOWN
                
        elif typology_type in [TypologyType.PING_PONG, TypologyType.VELOCITY_ANOMALY]:
            # High-frequency bidirectional activity
            return AddressTypes.WALLET  # Likely hot wallet or active trader
            
        # Structural pattern typologies
        elif typology_type == TypologyType.CYCLE_DETECTION:
            # Cycle patterns often indicate money laundering
            return AddressTypes.UNKNOWN  # Could be various types
            
        elif typology_type == TypologyType.LAYERING_PATH:
            # Layering paths suggest obfuscation
            return AddressTypes.UNKNOWN  # Intermediate addresses
            
        elif typology_type == TypologyType.SMURFING_NETWORK:
            # Smurfing networks are coordinated
            return AddressTypes.WALLET  # Likely coordinated wallets
            
        elif typology_type == TypologyType.PROXIMITY_RISK:
            # Proximity to risk varies by context
            return AddressTypes.UNKNOWN  # Depends on risk source
            
        elif typology_type in [TypologyType.MOTIF_FANIN, TypologyType.MOTIF_FANOUT]:
            # Motifs could indicate various behaviors
            if evidence.get('motif_participant_count', 0) > 50:
                return AddressTypes.INSTITUTIONAL  # Large-scale operations
            else:
                return AddressTypes.WALLET  # Smaller operations
            
        else:
            return AddressTypes.UNKNOWN

    def _calculate_simple_severity(self, confidence_score: float) -> str:
        """Calculate severity using simple string mapping."""
        if confidence_score >= 0.9:
            return Severities.CRITICAL
        elif confidence_score >= 0.75:
            return Severities.HIGH
        elif confidence_score >= 0.6:
            return Severities.MEDIUM
        else:
            return Severities.LOW

    def _alert_to_dict(self, alert: Dict) -> Dict:
        """Format the alert object into a dictionary for database insertion."""
        
        # Format for database with simple string values
        return {
            'alert_id': alert['alert_id'],
            'network': self.network,
            'address': alert['address'],
            'typology_type': alert['typology_type'].value,
            'severity': alert['severity'],  # Simple string
            'suspected_address_type': alert['suspected_address_type'],  # Simple string
            'confidence_score': float(alert['confidence_score']),
            'description': alert['description'],
            'volume_usd': alert.get('volume_usd', Decimal('0')),
            'evidence_json': json.dumps(alert['evidence'], default=str),
            'risk_indicators': alert['risk_indicators'],
            'related_addresses': alert.get('related_addresses', []),
            '_version': int(time.time() * 1000)
        }

    def _detect_structural_patterns(self) -> List[Dict]:
        alerts = []

        patterns = self.structural_pattern_repository.get_high_risk_deduplicated_patterns(
            window_days=self.window_days,
            processing_date=self.processing_date,
            min_risk_score=0.5
        )

        for pattern in patterns:
            pattern_alerts = self._create_alert_from_structural_pattern(pattern)
            if pattern_alerts:
                alerts.extend(pattern_alerts)

        return alerts

    def _create_alert_from_structural_pattern(self, pattern: Dict) -> List[Dict]:
        pattern_type = pattern['pattern_type']
        addresses_involved = pattern['addresses_involved']
        
        if not pattern_type or not addresses_involved:
            return []
            
        # Map pattern types to typology types
        typology_mapping = {
            PatternTypes.CYCLE: TypologyType.CYCLE_DETECTION,
            PatternTypes.LAYERING_PATH: TypologyType.LAYERING_PATH,
            PatternTypes.SMURFING_NETWORK: TypologyType.SMURFING_NETWORK,
            PatternTypes.PROXIMITY_RISK: TypologyType.PROXIMITY_RISK,
            PatternTypes.MOTIF_FANIN: TypologyType.MOTIF_FANIN,
            PatternTypes.MOTIF_FANOUT: TypologyType.MOTIF_FANOUT,
        }
        
        typology_type = typology_mapping.get(pattern_type)
        if not typology_type:
            logger.warning(f"Unknown pattern type: {pattern_type}")
            return None
            
        # Extract pattern data
        severity_score = float(pattern['severity_score'])
        confidence_score = float(pattern['confidence_score'])
        risk_score = float(pattern['risk_score'])
        
        # Create evidence from pattern data
        evidence = {
            'pattern_id': pattern['pattern_id'],
            'pattern_type': pattern_type,
            'severity_score': severity_score,
            'risk_score': risk_score,
            'detection_method': pattern['detection_method']
        }
        
        # Add pattern-specific evidence
        if pattern_type == PatternTypes.CYCLE:
            evidence.update({
                'cycle_length': pattern['cycle_length'],
                'cycle_volume_usd': float(pattern['cycle_volume_usd']),
                'cycle_path': pattern['cycle_path']
            })
            risk_indicators = ['money_laundering_cycle', 'circular_flow', 'fund_obfuscation']
            description = f"Money laundering cycle detected: {pattern['cycle_length']} addresses, ${float(pattern['cycle_volume_usd']):,.2f}"
            
        elif pattern_type == PatternTypes.LAYERING_PATH:
            evidence.update({
                'path_depth': pattern['path_depth'],
                'path_volume_usd': float(pattern['path_volume_usd']),
                'source_address': pattern['source_address'],
                'destination_address': pattern['destination_address']
            })
            risk_indicators = ['layering_scheme', 'multi_hop_transfer', 'fund_obfuscation']
            description = f"Layering path detected: {pattern['path_depth']} hops, ${float(pattern['path_volume_usd']):,.2f}"
            
        elif pattern_type == PatternTypes.SMURFING_NETWORK:
            evidence.update({
                'network_size': pattern['network_size'],
                'network_density': float(pattern['network_density']),
                'hub_addresses': pattern['hub_addresses']
            })
            risk_indicators = ['smurfing_network', 'coordinated_transfers', 'threshold_evasion']
            description = f"Smurfing network detected: {pattern['network_size']} addresses, density {float(pattern['network_density']):.2f}"
            
        elif pattern_type == PatternTypes.PROXIMITY_RISK:
            evidence.update({
                'risk_source_address': pattern['risk_source_address'],
                'distance_to_risk': pattern['distance_to_risk'],
                'risk_propagation_score': float(pattern['risk_propagation_score'])
            })
            risk_indicators = ['proximity_to_risk', 'risk_contamination', 'guilt_by_association']
            distance = pattern['distance_to_risk']
            description = f"Risk proximity detected: {distance} hops from high-risk address"
            
        elif pattern_type in [PatternTypes.MOTIF_FANIN, PatternTypes.MOTIF_FANOUT]:
            evidence.update({
                'motif_type': pattern.get('motif_type'),
                'motif_center_address': pattern['motif_center_address'],
                'motif_participant_count': pattern['motif_participant_count']
            })
            motif_type = pattern['motif_type']
            if motif_type == 'fanin':
                risk_indicators = ['concentration_pattern', 'fund_collection', 'aggregation_behavior']
                description = f"Fund collection pattern detected: {pattern['motif_participant_count']} participants"
            else:
                risk_indicators = ['distribution_pattern', 'fund_dispersal', 'broadcasting_behavior']
                description = f"Fund distribution pattern detected: {pattern['motif_participant_count']} participants"
        else:
            risk_indicators = ['structural_anomaly']
            description = f"Structural pattern detected: {pattern_type}"
        
        # Add common evidence fields
        evidence.update({
            'evidence_transaction_count': pattern['evidence_transaction_count'],
            'evidence_volume_usd': float(pattern['evidence_volume_usd']),
            'anomaly_score': float(pattern['anomaly_score'])
        })
        
        alert_confidence = max(confidence_score, risk_score, severity_score)
        
        alerts = []
        for address in addresses_involved:
            related = [addr for addr in addresses_involved if addr != address]
            
            alert = self._create_alert(
                address=address,
                typology_type=typology_type,
                confidence_score=alert_confidence,
                evidence=evidence,
                risk_indicators=risk_indicators,
                description=description,
                related_addresses=related
            )
            if alert:
                alerts.append(alert)
        
        return alerts

    def _extract_related_addresses(self, pattern: Dict) -> List[str]:
        related = []
        
        cycle_path = pattern.get('cycle_path', [])
        if cycle_path:
            related.extend(cycle_path)
            
        layering_path = pattern.get('layering_path', [])
        if layering_path:
            related.extend(layering_path)
            
        network_members = pattern.get('network_members', [])
        if network_members:
            related.extend(network_members[:10])
            
        hub_addresses = pattern.get('hub_addresses', [])
        if hub_addresses:
            related.extend(hub_addresses)
            
        source_addr = pattern.get('source_address', '')
        if source_addr:
            related.append(source_addr)
            
        dest_addr = pattern.get('destination_address', '')
        if dest_addr:
            related.append(dest_addr)
            
        risk_source = pattern.get('risk_source_address', '')
        if risk_source:
            related.append(risk_source)
            
        motif_center = pattern.get('motif_center_address', '')
        if motif_center:
            related.append(motif_center)
            
        related = list(set(related))
            
        return related[:20]

    def _create_alert_clusters(self, alerts: List[Dict]) -> List[Dict]:
        """
        Create clusters from detected alerts using enabled strategies.
        """
        all_clusters = []
        
        if self.clustering_strategies.get('same_entity', {}).get('enabled', False):
            same_entity_clusters = self._cluster_by_same_entity(alerts)
            all_clusters.extend(same_entity_clusters)
            logger.info(f"Same-entity clustering: {len(same_entity_clusters)} clusters")
        
        return all_clusters

    def _cluster_by_same_entity(self, alerts: List[Dict]) -> List[Dict]:
        """
        Group alerts for the same address into clusters.
        """
        if not alerts:
            return []
        
        strategy_config = self.clustering_strategies.get('same_entity', {})
        min_alerts = strategy_config.get('min_alerts', 2)
        
        alerts_by_address = defaultdict(list)
        for alert in alerts:
            address = alert.get('address')
            if address:
                alerts_by_address[address].append(alert)
        
        clusters = []
        for address, address_alerts in alerts_by_address.items():
            if len(address_alerts) >= min_alerts:
                sorted_alerts = address_alerts
                
                # Use volume_usd from first alert (all alerts for same address have same total volume)
                # This avoids double-counting since all alerts reference the same address's activity
                total_volume = float(address_alerts[0].get('volume_usd', Decimal('0')))
                
                severity_order = {
                    Severities.LOW: 1,
                    Severities.MEDIUM: 2,
                    Severities.HIGH: 3,
                    Severities.CRITICAL: 4
                }
                max_severity = max(
                    address_alerts,
                    key=lambda a: severity_order.get(a.get('severity', Severities.LOW), 0)
                )['severity']
                
                confidences = [
                    float(a.get('confidence_score', 0.5))
                    for a in address_alerts
                ]
                avg_confidence = sum(confidences) / len(confidences)
                
                cluster = {
                    'cluster_id': f"cluster_same_entity_{address}_{self.processing_date}",
                    'cluster_type': 'same_entity',
                    'primary_alert_id': sorted_alerts[0]['alert_id'],
                    'related_alert_ids': [a['alert_id'] for a in address_alerts],
                    'addresses_involved': [address],
                    'total_alerts': len(address_alerts),
                    'total_volume_usd': total_volume,
                    'severity_max': max_severity,
                    'confidence_avg': avg_confidence
                }
                
                clusters.append(cluster)

        return clusters
