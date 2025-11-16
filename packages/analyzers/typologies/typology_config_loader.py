import json
import requests
from pathlib import Path
from typing import Dict, Any
from loguru import logger


def load_typology_config(config_path: str = None) -> Dict[str, Any]:
    """
    Shared configuration loader for both TypologyDetector and RiskScorer.
    Loads typology configuration from JSON file with fallback to URL.

    Args:
        config_path: Optional custom path to config file. If None, uses default path.

    Returns:
        Dictionary containing typology configuration

    Raises:
        RuntimeError: If configuration cannot be loaded from file or URL
    """
    if config_path is None:
        # Default path relative to packages/analyzers/
        config_path = Path(__file__).parent.parent / 'typologies' / 'typology_detector_settings.json'

    config_path = Path(config_path)

    try:
        if not config_path.exists():
            logger.warning(f"Configuration file not found at {config_path}. Attempting to fetch from URL.")
            return _fetch_config_from_url(config_path)

        with open(config_path, 'r') as f:
            logger.info(f"Loading typology configuration from {config_path}")
            config_data = json.load(f)

        _validate_config(config_data)
        return config_data

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file {config_path}: {e}")
        return _fetch_config_from_url(config_path)
    except Exception as e:
        logger.error(f"Error loading configuration from {config_path}: {e}")
        return _fetch_config_from_url(config_path)


def _fetch_config_from_url(local_path: Path) -> Dict[str, Any]:
    """Fetch configuration from remote URL and save locally."""
    url = "https://raw.githubusercontent.com/chainswarm/data-pipeline/refs/heads/main/packages/analyzers/typologies/typology_detector_settings.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully fetched typology configuration from {url}")

        config_data = response.json()
        _validate_config(config_data)

        # Save the fetched config for future runs
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, 'w') as f:
            json.dump(config_data, f, indent=4)

        logger.info(f"Saved configuration to {local_path}")
        return config_data

    except (requests.RequestException, json.JSONDecodeError) as e:
        raise RuntimeError(f"Failed to fetch or parse configuration from {url}: {e}") from e


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration structure."""
    required_keys = ["peel_chain", "structuring", "ping_pong", "hub_anomaly",
                    "fresh_to_exchange", "rapid_fanout", "mixing_behavior"]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")

    # Validate each typology rule has required structure
    for rule_name, rule_config in config.items():
        if not isinstance(rule_config, dict):
            raise ValueError(f"Configuration for '{rule_name}' must be a dictionary")

        # Check for required threshold fields (basic validation)
        required_thresholds = [
            'min_recipients', 'min_volume_usd', 'max_branching_factor'  # peel_chain example
        ]

        # Note: Different rules have different threshold structures, so we do minimal validation
        if 'min_recipients' in rule_config and not isinstance(rule_config['min_recipients'], (int, float)):
            raise ValueError(f"min_recipients for '{rule_name}' must be a number")


def get_config_summary(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get summary information about the loaded configuration."""
    return {
        "typology_types": len(config),
        "available_rules": list(config.keys()),
        "config_source": "file_or_url"
    }