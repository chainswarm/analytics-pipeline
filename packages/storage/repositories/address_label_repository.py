from typing import List, Dict
from chainswarm_core.db import BaseRepository, row_to_dict
from chainswarm_core import AddressTypes, TrustLevels
from chainswarm_core.constants.risk import get_address_type_risk_level


class AddressLabelRepository(BaseRepository):

    def __init__(self, client):
        super().__init__(client)

    def insert_labels(self, labels: List[Dict]):
        batch_size = 1000
        version = self._generate_version()

        for i in range(0, len(labels), batch_size):
            batch = labels[i:i + batch_size]

            batch_data = []
            for label in batch:
                # Calculate simple risk level
                address_type = label.get('address_type', AddressTypes.UNKNOWN)
                risk_level = get_address_type_risk_level(address_type)
                
                batch_data.append([
                    label['network'],
                    label['network_type'],
                    label['address'],
                    label['label'],
                    address_type,
                    label['trust_level'],
                    label['source'],
                    label['confidence_score'],
                    risk_level,
                    version,
                ])

            self.client.insert(
                'core_address_labels',
                batch_data,
                column_names=[
                    'network', 'network_type', 'address', 'label', 'address_type', 'trust_level',
                    'source', 'confidence_score', 'risk_level', '_version'
                ]
            )

    def get_exchange_labels_for_addresses(self, network: str, addresses: List[str]) -> List[Dict]:
        """Get exchange labels for addresses with efficient database filtering."""
        #TODO: to be removed as this query is not used in optimal way !
        if not addresses:
            return []

        placeholders = ', '.join([f'%(addr_{i})s' for i in range(len(addresses))])
        
        query = f"""
        SELECT *
        FROM core_address_labels
        WHERE network = %(network)s 
          AND address IN ({placeholders})
          AND address_type = %(exchange_type)s
          AND trust_level IN (%(verified)s, %(official)s)
        ORDER BY trust_level DESC, confidence_score DESC
        """
        
        parameters = {
            'network': network,
            'exchange_type': AddressTypes.EXCHANGE,
            'verified': TrustLevels.VERIFIED,
            'official': TrustLevels.OFFICIAL,
            **{f'addr_{i}': addr for i, addr in enumerate(addresses)}
        }
        
        result = self.client.query(query, parameters=parameters)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]

    def get_labels_for_addresses(self, network: str, addresses: List[str]) -> List[Dict]:
        """Get all labels for addresses."""
        if not addresses:
            return []

        placeholders = ', '.join([f'%(addr_{i})s' for i in range(len(addresses))])
        
        query = f"""
        SELECT *
        FROM core_address_labels
        WHERE network = %(network)s
          AND address IN ({placeholders})
        ORDER BY trust_level DESC, confidence_score DESC
        """
        
        parameters = {
            'network': network,
            **{f'addr_{i}': addr for i, addr in enumerate(addresses)}
        }
        
        result = self.client.query(query, parameters=parameters)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]
    
    def get_all_labels(self, network: str, limit: int = 10_000_000) -> List[Dict]:
        query = """
        SELECT *
        FROM core_address_labels
        WHERE network = %(network)s
        ORDER BY address, trust_level DESC, confidence_score DESC
        LIMIT %(limit)s
        """
        
        parameters = {
            'network': network,
            'limit': limit
        }
        
        result = self.client.query(query, parameters=parameters)
        return [row_to_dict(row, result.column_names) for row in result.result_rows]