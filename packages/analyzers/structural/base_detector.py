from abc import ABC, abstractmethod
from typing import Dict, List
import networkx as nx
from loguru import logger

from packages.storage.constants import AddressTypes, TrustLevels


class BasePatternDetector(ABC):
    """
    Abstract base class for pattern detectors.
    Contains shared functionality for all pattern detection algorithms.
    """

    def __init__(
        self,
        config: Dict,
        address_labels_cache: Dict,
        network: str = None
    ):
        """
        Initialize the detector with configuration and address labels.
        
        Args:
            config: Configuration dictionary for pattern detection
            address_labels_cache: Cache of address labels for trust/risk evaluation
            network: Network identifier for network-specific configuration overrides
        """
        self.config = config
        self._address_labels_cache = address_labels_cache
        self.network = network
        self._validate_config()
        logger.debug(f"Initialized {self.__class__.__name__} for network={network}")

    @abstractmethod
    def _validate_config(self) -> None:
        """
        Validate that required configuration sections are present.
        Each detector should implement this to check for its specific config requirements.
        
        Raises:
            ValueError: If required configuration is missing
        """
        pass

    @abstractmethod
    def detect(self, G: nx.DiGraph) -> List[Dict]:
        """
        Detect patterns in the graph.
        
        Args:
            G: NetworkX directed graph to analyze
            
        Returns:
            List of pattern dictionaries, each containing:
                - pattern_id: Unique identifier
                - pattern_type: Type of pattern detected
                - pattern_hash: Hash for deduplication
                - addresses_involved: List of involved addresses
                - address_roles: Role of each address in the pattern
                - severity_score: Pattern severity (0-1)
                - confidence_score: Detection confidence (0-1)
                - risk_score: Overall risk score
                - detection_timestamp: Unix timestamp of detection
                - evidence_transaction_count: Number of transactions
                - evidence_volume_usd: Total USD volume
                - detection_method: Method used for detection
                - anomaly_score: Anomaly indicator
                - (pattern-specific fields)
        """
        pass

    def _get_config_value(self, section: str, key: str, default=None):
        """
        Get configuration value with network-specific override support.
        
        Checks network_overrides first, then falls back to base config.
        
        Args:
            section: Configuration section name
            key: Configuration key within the section
            default: Default value if key not found
            
        Returns:
            Configuration value with network override if available
        """
        if section not in self.config:
            return default
        
        section_config = self.config[section]
        
        # Check for network-specific override
        if self.network and 'network_overrides' in section_config:
            network_overrides = section_config['network_overrides']
            if self.network in network_overrides:
                network_config = network_overrides[self.network]
                if key in network_config:
                    return network_config[key]
        
        # Fall back to base config
        return section_config.get(key, default)

    def _is_trusted_address(self, address: str) -> bool:
        """
        Check if an address is trusted based on its labels.
        
        Args:
            address: Address to check
            
        Returns:
            True if address is trusted, False otherwise
        """
        label_info = self._address_labels_cache.get(address)
        if not label_info:
            return False
            
        trust_level = label_info.get('trust_level')
        address_type = label_info.get('address_type')
        
        safe_trust_levels = [TrustLevels.VERIFIED, TrustLevels.OFFICIAL]
        safe_address_types = [
            AddressTypes.EXCHANGE,
            AddressTypes.INSTITUTIONAL,
            AddressTypes.STAKING,
            AddressTypes.VALIDATOR,
        ]
        
        return (trust_level in safe_trust_levels and
                address_type in safe_address_types)

    def _is_fraudulent_address(self, address: str) -> bool:
        """
        Check if an address is known to be fraudulent.
        
        Args:
            address: Address to check
            
        Returns:
            True if address is fraudulent, False otherwise
        """
        label_info = self._address_labels_cache.get(address)
        if not label_info:
            return False
            
        trust_level = label_info.get('trust_level')
        address_type = label_info.get('address_type')
        
        fraudulent_address_types = [
            AddressTypes.MIXER,
            AddressTypes.SCAM,
            AddressTypes.DARK_MARKET,
            AddressTypes.SANCTIONED
        ]
        
        return (address_type in fraudulent_address_types or
                trust_level == TrustLevels.BLACKLISTED)

    def _adjust_severity_for_trust(self, base_severity: float, participants: List[str]) -> float:
        """
        Adjust pattern severity based on trust levels of participants.
        
        Args:
            base_severity: Base severity score before adjustment
            participants: List of addresses involved in the pattern
            
        Returns:
            Adjusted severity score
        """
        if not participants:
            return base_severity
            
        # Get configuration
        severity_config = self.config["severity_adjustments"]
        trust_reduction_factor = severity_config["trust_reduction_factor"]
        fraud_increase_factor = severity_config["fraud_increase_factor"]
        
        trusted_count = sum(1 for addr in participants if self._is_trusted_address(addr))
        fraudulent_count = sum(1 for addr in participants if self._is_fraudulent_address(addr))
        
        adjusted_severity = base_severity
        
        # Reduce severity if trusted addresses involved
        if trusted_count > 0:
            trust_reduction = trust_reduction_factor * trusted_count / len(participants)
            adjusted_severity *= (1.0 - trust_reduction)
        
        # Increase severity if fraudulent addresses involved
        if fraudulent_count > 0:
            fraud_increase = fraud_increase_factor * fraudulent_count / len(participants)
            adjusted_severity *= (1.0 + fraud_increase)
        
        return min(adjusted_severity, 1.0)

    def _calculate_trust_risk_modifier(self, address: str) -> float:
        """
        Calculate risk modifier based on address trust level.
        
        Args:
            address: Address to evaluate
            
        Returns:
            Risk modifier value
        """
        severity_config = self.config["severity_adjustments"]
        
        if self._is_trusted_address(address):
            return severity_config["trust_risk_modifier"]
        elif self._is_fraudulent_address(address):
            return severity_config["fraud_risk_modifier"]
        else:
            return severity_config["unknown_risk_modifier"]

    def _get_address_context(self, address: str) -> Dict:
        """
        Get trust and type context for address.
        
        Args:
            address: Address to get context for
            
        Returns:
            Dictionary containing trust_level, address_type, is_trusted, 
            is_fraudulent, and risk_modifier
        """
        label_info = self._address_labels_cache.get(address, {})
        
        return {
            'trust_level': label_info.get('trust_level', TrustLevels.UNVERIFIED),
            'address_type': label_info.get('address_type', AddressTypes.UNKNOWN),
            'is_trusted': self._is_trusted_address(address),
            'is_fraudulent': self._is_fraudulent_address(address),
            'risk_modifier': self._calculate_trust_risk_modifier(address)
        }