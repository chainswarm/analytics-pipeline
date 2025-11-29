from typing import Dict, List, Optional
from decimal import Decimal
import numpy as np
import networkx as nx
from chainswarm_core import terminate_event
from loguru import logger
from packages.storage.repositories.money_flows_repository import MoneyFlowsRepository
from packages.storage.repositories.transfer_aggregation_repository import TransferAggregationRepository
from packages.storage.repositories.feature_repository import FeatureRepository
from packages.storage.repositories.transfer_repository import TransferRepository
from cdlib import algorithms as cd_algorithms
from collections import defaultdict

class AddressFeatureAnalyzer:

    def __init__(
        self,
        transfer_repository: TransferRepository,
        transfer_aggregation_repository: TransferAggregationRepository,
        money_flows_repository: MoneyFlowsRepository,
        feature_repository: FeatureRepository,
        window_days: int,
        start_timestamp: int,
        end_timestamp: int,
        network: str
    ):
        self.transfer_repository = transfer_repository
        self.transfer_aggregation_repository = transfer_aggregation_repository
        self.money_flows_repository = money_flows_repository
        self.feature_repository = feature_repository
        self.window_days = window_days
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.network = network

    def analyze_address_features(self, batch_size: int = 1000, chunk_size: int = 10000) -> None:
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
        
        logger.info(f"Building graph from {len(windowed_flows)} windowed flows for {len(addresses)} addresses")
        G = self._build_graph_from_flows_data(windowed_flows)
        if G.number_of_nodes() == 0:
            raise ValueError("Cannot build features without global graph data")

        logger.info("Computing global graph analytics")
        graph_analytics = self._compute_all_graph_algorithms(G)

        logger.info("Indexing flows by address from global graph")
        flows_by_address = self._build_flows_index_from_graph(G)
        
        all_features_list = []
        for i in range(0, len(addresses), chunk_size):
            if terminate_event.is_set():
                break

            chunk = addresses[i:i + chunk_size]
            logger.info(f"Processing address features for chunk {i}-{i + len(chunk) - 1} of {len(addresses)}")

            patterns_map = self.transfer_aggregation_repository.get_bulk_address_temporal_patterns(
                addresses=chunk, start_timestamp_ms=self.start_timestamp, end_timestamp_ms=self.end_timestamp
            )
            summaries_map = self.transfer_aggregation_repository.get_bulk_address_temporal_summaries(
                addresses=chunk, start_timestamp_ms=self.start_timestamp, end_timestamp_ms=self.end_timestamp
            )
            amount_moments = self.transfer_repository.get_bulk_address_amount_moments(
                chunk, self.start_timestamp, self.end_timestamp
            )
            behavioral_counters = self.transfer_repository.get_bulk_address_behavioral_counters(
                chunk, self.start_timestamp, self.end_timestamp
            )
            hourly_volumes_map = self.transfer_repository.get_bulk_address_hourly_volumes(
                chunk, self.start_timestamp, self.end_timestamp
            )
            reciprocity_stats = self.transfer_aggregation_repository.get_bulk_address_reciprocity_stats(
                addresses=chunk, start_timestamp_ms=self.start_timestamp, end_timestamp_ms=self.end_timestamp
            )
            stability_map = self.transfer_aggregation_repository.get_bulk_address_counterparty_stability(
                addresses=chunk, start_timestamp_ms=self.start_timestamp, end_timestamp_ms=self.end_timestamp, buckets=4, top_k=10
            )
            interevent_stats = self.transfer_repository.get_bulk_address_interevent_stats(
                chunk, self.start_timestamp, self.end_timestamp
            )
            outlier_counts = self.transfer_repository.get_bulk_address_outlier_counts(
                chunk, self.start_timestamp, self.end_timestamp
            )
            
            chunk_features_list = []
            for address in chunk:
                if terminate_event.is_set(): break

                flows = flows_by_address[address]
                if not flows:
                    raise ValueError(f"No money flows found for address {address} - core data missing")

                base_features = self._get_base_features_cached(
                    address=address, flows=flows, patterns=patterns_map.get(address, {}), summaries=summaries_map.get(address, {})
                )
                stats_features = self._compute_statistical_features_from_moments(
                    address=address, moments=amount_moments.get(address)
                ) or self._empty_statistical_features()
                flow_features = self._compute_flow_features_cached(address=address, flows=flows) or {}
                behavioral_features = self._compute_behavioral_features_from_counters(
                    address=address, counters=behavioral_counters.get(address)
                ) or {'round_number_ratio': 0.0, 'unusual_timing_score': 0.0, 'structuring_score': 0.0}
                graph_features = graph_analytics.get(address, self._empty_graph_features())
                intraday_features = self._extract_intraday_features_from_aggregates(
                    hourly_volumes=hourly_volumes_map.get(address, [0.0]*24),
                    hourly_activity=patterns_map.get(address, {}).get('hourly_activity', [0]*24)
                ) or {}
                directional_flow_features = self._extract_directional_flow_features_cached(
                    address=address, flows=flows
                ) or {}

                adv_flow_recip_entropy = 0.0
                rstats = reciprocity_stats.get(address, {'total_volume': 0, 'reciprocal_volume': 0})
                total_vol, recip_vol = float(rstats.get('total_volume', 0)), float(rstats.get('reciprocal_volume', 0))
                if total_vol > 0.0:
                    p_rec = max(0.0, min(1.0, recip_vol / total_vol))
                    p_non = 1.0 - p_rec
                    import math
                    ent = 0.0
                    if p_rec > 0: ent -= p_rec * math.log2(p_rec)
                    if p_non > 0: ent -= p_non * math.log2(p_non)
                    adv_flow_recip_entropy = float(max(0.0, min(1.0, ent)))

                stability = float(stability_map.get(address, 0.0))
                istats = interevent_stats.get(address, {'mean_inter_s': 0.0, 'std_inter_s': 0.0, 'n': 0})
                mean_inter = float(istats.get('mean_inter_s') or 0.0)
                std_inter = float(istats.get('std_inter_s') or 0.0)
                n_inter = int(istats.get('n') or 0)
                burstiness = float(max(0.0, min(1.0, (std_inter - mean_inter) / (std_inter + mean_inter)))) if n_inter >= 2 and (mean_inter + std_inter) > 0.0 else 0.0
                
                transaction_regularity = float(base_features['regularity_score'])
                amount_predictability = float(max(0.0, 1.0 - min(1.0, float(stats_features['volume_cv']))))
                advanced_features = {
                    'flow_reciprocity_entropy': adv_flow_recip_entropy, 'counterparty_stability': stability,
                    'flow_burstiness': burstiness, 'transaction_regularity': transaction_regularity,
                    'amount_predictability': amount_predictability
                }
                
                all_features = { **base_features, **stats_features, **flow_features, **behavioral_features, **graph_features, **intraday_features, **directional_flow_features, **advanced_features }
                
                # Add missing anomaly scores (required by repository but not yet computed here)
                all_features.update({
                    'behavioral_anomaly_score': 0.0,
                    'graph_anomaly_score': 0.0,
                    'neighborhood_anomaly_score': 0.0,
                    'global_anomaly_score': 0.0,
                    'outlier_transactions': 0,
                    'suspicious_pattern_score': 0.0
                })

                patterns = patterns_map.get(address, {})
                summaries = summaries_map.get(address, {})
                
                total_vol = float(base_features.get('total_volume_usd', 0))
                tx_total = int(base_features.get('tx_total_count') or 0)
                degree_total = int(base_features.get('degree_total') or 0)
                
                all_features.update({
                    'unique_assets_in': 1, 'unique_assets_out': 1, 'dominant_asset_in': 'NATIVE', 'dominant_asset_out': 'NATIVE', 'asset_diversity_score': 0.0,
                    'hourly_activity': [int(x) for x in patterns.get('hourly_activity', [0]*24)],
                    'daily_activity': [int(x) for x in patterns.get('daily_activity', [0]*7)],
                    'peak_activity_hour': int(patterns.get('peak_activity_hour') or 0),
                    'peak_activity_day': int(patterns.get('peak_activity_day') or 0),
                    'small_transaction_ratio': float(behavioral_features.get('structuring_score') or 0.0),
                    'first_activity_timestamp': int(summaries.get('first_timestamp') or self.start_timestamp),
                    'last_activity_timestamp': int(summaries.get('last_timestamp') or self.end_timestamp),
                    'window_start_timestamp': self.start_timestamp, 'window_end_timestamp': self.end_timestamp
                })
                chunk_features_list.append(all_features)
            all_features_list.extend(chunk_features_list)

        if all_features_list:
            logger.info(f"Generated {len(all_features_list)} total feature sets")
            
            total_inserted = 0
            logger.info(f"Inserting {len(all_features_list)} feature sets into {self.feature_repository.features_table_name}")
            
            from datetime import datetime, timezone
            processing_date = datetime.fromtimestamp(self.end_timestamp / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
            
            for j in range(0, len(all_features_list), batch_size):
                self.feature_repository.insert_features(
                    all_features_list[j:j + batch_size],
                    window_days=self.window_days,
                    processing_date=processing_date
                )
                total_inserted += len(all_features_list[j:j + batch_size])

        logger.info(f"Address features inserted: {total_inserted}")

    def _extract_addresses_from_flows(self, flows: List[Dict]) -> List[str]:
        addresses_set = set()
        for flow in flows:
            addresses_set.add(flow['from_address'])
            addresses_set.add(flow['to_address'])
        return sorted(list(addresses_set))
    
    def _build_graph_from_flows_data(self, flows: List[Dict]) -> nx.DiGraph:
        G = nx.DiGraph()
        for flow in flows:
            G.add_edge(
                flow['from_address'], flow['to_address'],
                weight=float(flow['amount_usd_sum']),
                tx_count=int(flow['tx_count']),
                amount_usd_sum=float(flow['amount_usd_sum'])
            )
        logger.info(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        return G

    def _compute_all_graph_algorithms(self, G: nx.DiGraph) -> Dict[str, Dict]:
        self._add_node_attributes_to_graph(G)
        results = defaultdict(lambda: self._empty_graph_features())
        
        pagerank_scores = self._compute_pagerank(G)
        betweenness_scores = self._compute_betweenness_centrality(G)
        closeness_scores = self._compute_closeness_centrality(G)
        kcore_scores = self._compute_kcore(G)
        clustering_scores = self._compute_clustering_coefficient(G)
        communities = self._compute_community_detection(G)
        khop_features = self._compute_khop_features(G)
        
        for address in list(G.nodes()):
            pagerank_val = pagerank_scores.get(address, 0.0)
            betweenness_val = betweenness_scores.get(address, 0.0)
            clustering_val = clustering_scores.get(address, 0.0)
            results[address].update({
                'pagerank': pagerank_val,
                'betweenness': betweenness_val,
                'closeness': closeness_scores.get(address, 0.0),
                'clustering_coefficient': clustering_val,
                'kcore': kcore_scores.get(address, 0),
                'community_id': communities.get(address, -1),
                'centrality_score': (pagerank_val * 0.4) + (betweenness_val * 0.3) + (clustering_val * 0.3),
                'degree': G.degree(address),
                **khop_features[address]
            })
        logger.info(f"Graph analytics computed for {len(results)} addresses")
        return results
    
    def _compute_pagerank(self, G: nx.DiGraph) -> Dict[str, float]:
        return nx.pagerank(G, weight='weight', alpha=0.85)

    def _compute_community_detection(self, G: nx.DiGraph) -> Dict[str, int]:
        try:
            coms = cd_algorithms.leiden(G.to_undirected(), weights='weight')
            return {node: i for i, com in enumerate(coms.communities) for node in com}
        except Exception as e:
            # User requested this to raise a hard error instead of silent/warning failure
            raise ValueError(f"Community detection failed: {e}")
    
    def _compute_betweenness_centrality(self, G: nx.DiGraph) -> Dict[str, float]:
        return nx.betweenness_centrality(G, k=min(1000, G.number_of_nodes()-1) if G.number_of_nodes()>1 else None, weight='weight', normalized=True)

    def _compute_closeness_centrality(self, G: nx.DiGraph) -> Dict[str, float]:
        return nx.closeness_centrality(G, distance='weight')

    def _compute_kcore(self, G: nx.DiGraph) -> Dict[str, int]:
        return nx.core_number(G.to_undirected())
    
    def _compute_clustering_coefficient(self, G: nx.DiGraph) -> Dict[str, float]:
        return nx.clustering(G.to_undirected(), weight='weight')
    
    def _compute_khop_features(self, G: nx.DiGraph, max_k: int = 3) -> Dict[str, Dict[str, int]]:
        khop_results = defaultdict(dict)
        for node in G.nodes():
            for k in range(1, max_k + 1):
                neighbors = set(nx.single_source_shortest_path_length(G, node, cutoff=k).keys()) - {node}
                khop_results[node][f'khop{k}_count'] = len(neighbors)
                khop_results[node][f'khop{k}_volume_usd'] = sum(G.nodes[n].get('total_volume_usd', 0.0) for n in neighbors)
        return khop_results
    
    def _add_node_attributes_to_graph(self, G: nx.DiGraph) -> None:
        node_volumes = {}
        for node in G.nodes():
            in_volume = sum(data.get('amount_usd_sum', 0.0) for _, _, data in G.in_edges(node, data=True))
            out_volume = sum(data.get('amount_usd_sum', 0.0) for _, _, data in G.out_edges(node, data=True))
            node_volumes[node] = in_volume + out_volume
        nx.set_node_attributes(G, node_volumes, 'total_volume_usd')

    def _empty_graph_features(self) -> Dict[str, any]:
        return {
            'pagerank': 0.0, 'betweenness': 0.0, 'closeness': 0.0, 'clustering_coefficient': 0.0, 'kcore': 0, 'community_id': -1,
            'centrality_score': 0.0, 'degree': 0,
            **{f'khop{k}_{m}': v for k in range(1,4) for m,v in [('count',0),('volume_usd',Decimal('0'))]},
            'flow_reciprocity_entropy': 0.0, 'counterparty_stability': 0.0, 'flow_burstiness': 0.0,
            'transaction_regularity': 0.0, 'amount_predictability': 0.0
        }
    
    def _safe_skewness(self, values: List[float]) -> float:
        if len(values) < 3: return 0.0
        try: from scipy.stats import skew; return skew(values)
        except: return 0.0

    def _safe_kurtosis(self, values: List[float]) -> float:
        if len(values) < 4: return 0.0
        try: from scipy.stats import kurtosis; return kurtosis(values)
        except: return 0.0
            
    def _empty_statistical_features(self) -> Dict:
        return {'amount_variance': 0.0, 'amount_skewness': 0.0, 'amount_kurtosis': 0.0, 'volume_std': 0.0, 'volume_cv': 0.0}

    def _calculate_entropy(self, values: List[int]) -> float:
        total = sum(values)
        if total == 0: return 0.0
        return -sum((v/total) * np.log2(v/total) for v in values if v > 0)
    
    def _calculate_gini_coefficient(self, values: List[float]) -> float:
        if not values: return 0.0
        values = sorted([v for v in values if v > 0])
        n = len(values)
        if n <= 1: return 0.0
        return 1.0 - (2.0 * sum((i + 1) * v for i, v in enumerate(values))) / (n * sum(values))
    
    def _calculate_shannon_entropy(self, probabilities: List[float]) -> float:
        return -sum(p * np.log2(p) for p in probabilities if p > 0)

    def _build_flows_index_from_graph(self, G: nx.DiGraph) -> Dict[str, List[Dict]]:
        flows_by_address: Dict[str, List[Dict]] = defaultdict(list)
        for u, v, data in G.edges(data=True):
            flow = { 'from_address': u, 'to_address': v, 'amount_usd_sum': data.get('amount_usd_sum', 0.0), 'tx_count': int(data.get('tx_count', 0)) }
            flows_by_address[u].append(flow)
            flows_by_address[v].append(flow)
        return flows_by_address

    def _compute_temporal_features_from_aggregates(self, address: str, patterns: Dict, summaries: Dict) -> Dict:
        hourly, daily = patterns.get('hourly_activity', []), patterns.get('daily_activity', [])
        first_ts, last_ts = int(summaries.get('first_timestamp', 0)), int(summaries.get('last_timestamp', 0))
        total_txs, activity_days, total_vol = int(summaries.get('total_tx_count', 0)), int(summaries.get('distinct_activity_days', 0)), float(summaries.get('total_volume', 0.0))
        hour_entropy = self._calculate_entropy(hourly) if total_txs > 0 else 0.0
        regularity_score = 1.0 - (hour_entropy / 4.585) if hour_entropy > 0 else 1.0 # 4.585 is log2(24)
        return {
            'activity_days': activity_days, 'activity_span_days': max(1, (last_ts - first_ts) // 86400000),
            'avg_daily_volume_usd': Decimal(str(total_vol / max(activity_days, 1))),
            'peak_hour': int(np.argmax(hourly)) if hourly else 0, 'peak_day': int(np.argmax(daily)) if daily else 0,
            'regularity_score': regularity_score,
            'burst_factor': (max(hourly) / (total_txs / 24.0)) if total_txs > 0 else 0.0,
            'hourly_entropy': hour_entropy, 'daily_entropy': self._calculate_entropy(daily) if total_txs > 0 else 0.0,
            'weekend_transaction_ratio': summaries.get('weekend_tx_count', 0) / max(total_txs, 1),
            'night_transaction_ratio': summaries.get('night_tx_count', 0) / max(total_txs, 1),
            'consistency_score': regularity_score * (1.0 - (hour_entropy / 4.585)),
            'is_new_address': (first_ts // 1000) >= (self.start_timestamp // 1000), 'is_dormant_reactivated': False
        }

    def _get_base_features_cached(self, address: str, flows: List[Dict], patterns: Dict, summaries: Dict) -> Dict:
        temporal_features = self._compute_temporal_features_from_aggregates(address, patterns, summaries)
        total_in = sum(Decimal(str(f['amount_usd_sum'])) for f in flows if f['to_address'] == address)
        total_out = sum(Decimal(str(f['amount_usd_sum'])) for f in flows if f['from_address'] == address)
        tx_in = sum(f['tx_count'] for f in flows if f['to_address'] == address)
        tx_out = sum(f['tx_count'] for f in flows if f['from_address'] == address)
        recipients = {f['to_address'] for f in flows if f['from_address'] == address}
        senders = {f['from_address'] for f in flows if f['to_address'] == address}
        total_vol = total_in + total_out
        tx_total = tx_in + tx_out
        
        amounts_in = [float(f['amount_usd_sum']) for f in flows if f['to_address'] == address]
        amounts_out = [float(f['amount_usd_sum']) for f in flows if f['from_address'] == address]
        all_amounts = amounts_in + amounts_out
        
        median_in = float(np.median(amounts_in)) if amounts_in else 0.0
        median_out = float(np.median(amounts_out)) if amounts_out else 0.0
        max_tx = float(max(all_amounts)) if all_amounts else 0.0
        min_tx = float(min(all_amounts)) if all_amounts else 0.0
        return {
            'address': address, 'degree_in': tx_in, 'degree_out': tx_out, 'degree_total': len(senders | recipients),
            'unique_counterparties': len(senders | recipients), 'total_in_usd': total_in, 'total_out_usd': total_out,
            'net_flow_usd': total_in - total_out, 'total_volume_usd': total_vol,
            'avg_tx_in_usd': total_in / max(tx_in, 1), 'avg_tx_out_usd': total_out / max(tx_out, 1),
            'median_tx_in_usd': Decimal(str(median_in)), 'median_tx_out_usd': Decimal(str(median_out)),
            'max_tx_usd': Decimal(str(max_tx)), 'min_tx_usd': Decimal(str(min_tx)),
            'tx_in_count': tx_in, 'tx_out_count': tx_out, 'tx_total_count': tx_total, **temporal_features,
            'reciprocity_ratio': float(min(total_in, total_out) / max(total_vol, 1)),
            'velocity_score': float(min(tx_total / 100.0, 1.0)),
            'unique_recipients_count': len(recipients), 'unique_senders_count': len(senders)
        }

    def _compute_statistical_features_from_moments(self, address: str, moments: Optional[Dict]) -> Dict:
        if not moments: raise ValueError(f"Missing statistical moments for {address}")
        n, s1, s2 = int(moments['n']), float(moments['s1']), float(moments['s2'])
        if n < 2: return self._empty_statistical_features()
        mean = s1 / n; variance = (s2 - s1**2 / n) / (n - 1)
        return {
            'amount_variance': variance, 'volume_std': np.sqrt(variance), 'volume_cv': np.sqrt(variance) / max(mean, 1.0),
            'amount_skewness': 0.0, 'amount_kurtosis': 0.0 # simplified
        }

    def _compute_flow_features_cached(self, address: str, flows: List[Dict]) -> Dict:
        if not flows: return {'flow_concentration': 0.0, 'flow_diversity': 0.0, 'counterparty_concentration': 0.0, 'concentration_ratio': 0.0}
        flow_amounts = [float(f['amount_usd_sum']) for f in flows]
        total_volume = sum(flow_amounts)
        counterparty_volumes = defaultdict(float)
        for f in flows: counterparty_volumes[f['from_address'] if f['to_address'] == address else f['to_address']] += float(f['amount_usd_sum'])
        return {
            'flow_concentration': self._calculate_gini_coefficient(flow_amounts),
            'flow_diversity': self._calculate_shannon_entropy([a/total_volume for a in flow_amounts if a>0]) / (np.log(len(flow_amounts)) if len(flow_amounts)>1 else 1),
            'counterparty_concentration': self._calculate_gini_coefficient(list(counterparty_volumes.values())),
            'concentration_ratio': max(counterparty_volumes.values()) / total_volume if total_volume > 0 else 0.0
        }

    def _compute_behavioral_features_from_counters(self, address: str, counters: Optional[Dict]) -> Dict:
        if not counters: raise ValueError(f"Missing behavioral counters for {address}")
        total = int(counters.get('total_tx_pos_amount', 0))
        if total <= 0: return {'round_number_ratio': 0.0, 'unusual_timing_score': 0.0, 'structuring_score': 0.0}
        small_c, round_c, unusual_c = int(counters.get('small_amount_count',0)), int(counters.get('round_number_count',0)), int(counters.get('unusual_tx_count',0))
        struct_score = (small_c/total)*1.5 if (small_c/total) > 0.5 and small_c>=3 else small_c/total
        return {
            'round_number_ratio': round_c / total,
            'unusual_timing_score': unusual_c / total,
            'structuring_score': min(1.0, struct_score)
        }
    
    def _extract_intraday_features_from_aggregates(self, hourly_volumes: List[float], hourly_activity: List[int]) -> Dict:
        if len(hourly_volumes) != 24 or len(hourly_activity) != 24: raise ValueError("Invalid hourly aggregates")
        non_zero_volumes, total_txs = [v for v in hourly_volumes if v > 0], sum(hourly_activity)

        return {
            'hourly_volume_variance': float(np.var(non_zero_volumes)) if len(non_zero_volumes) > 1 else 0.0,
            'peak_volume_hour': int(np.argmax(hourly_volumes)),
            'intraday_volume_ratio': float(max(hourly_volumes) / (sum(non_zero_volumes) / 24.0)) if sum(non_zero_volumes) > 0 else 0.0,
            'hourly_transaction_entropy': float(self._calculate_shannon_entropy([c/total_txs for c in hourly_activity if c>0])) if total_txs > 0 else 0.0,
            'volume_concentration_score': float(self._calculate_gini_coefficient(non_zero_volumes))
        }

    def _extract_directional_flow_features_cached(self, address: str, flows: List[Dict]) -> Dict:
        if not flows: return {'in_out_ratio': 0.5, 'flow_asymmetry': 0.0, 'dominant_flow_direction': 'balanced', 'flow_direction_entropy': 0.0, 'counterparty_overlap_ratio': 0.0}
        total_in = sum(f['amount_usd_sum'] for f in flows if f['to_address'] == address)
        total_out = sum(f['amount_usd_sum'] for f in flows if f['from_address'] == address)
        senders, recipients = {f['from_address'] for f in flows if f['to_address'] == address}, {f['to_address'] for f in flows if f['from_address'] == address}
        total_vol = total_in + total_out
        return {
            'in_out_ratio': total_in / total_vol if total_vol > 0 else 0.5,
            'flow_asymmetry': abs(total_in - total_out) / total_vol if total_vol > 0 else 0.0,
            'dominant_flow_direction': 'incoming' if total_in > 1.5 * total_out else 'outgoing' if total_out > 1.5 * total_in else 'balanced',
            'flow_direction_entropy': self._calculate_shannon_entropy([len([f for f in flows if f['to_address'] == address])/len(flows), len([f for f in flows if f['from_address'] == address])/len(flows)]) if flows else 0.0,
            'counterparty_overlap_ratio': len(senders & recipients) / max(len(senders | recipients), 1)
        }
