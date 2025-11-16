import json
import requests
from pathlib import Path
from typing import Dict, Any
from loguru import logger


def load_structural_pattern_config(config_path: str = None) -> Dict[str, Any]:
    """
    Configuration loader for StructuralPatternAnalyzer detection settings.
    Loads structural pattern configuration from JSON file with fallback to URL.

    Args:
        config_path: Optional custom path to config file. If None, uses default path.

    Returns:
        Dictionary containing structural pattern configuration

    Raises:
        RuntimeError: If configuration cannot be loaded from file or URL
    """
    if config_path is None:
        # Default path relative to packages/analyzers/
        config_path = Path(__file__).parent / 'structural_pattern_settings.json'

    config_path = Path(config_path)

    try:
        if not config_path.exists():
            logger.warning(f"Configuration file not found at {config_path}. Attempting to fetch from URL.")
            return _fetch_config_from_url(config_path)

        with open(config_path, 'r') as f:
            logger.info(f"Loading structural pattern configuration from {config_path}")
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
    url = "https://raw.githubusercontent.com/chainswarm/data-pipeline/refs/heads/main/packages/analyzers/structural/structural_pattern_settings.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully fetched structural pattern configuration from {url}")

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
    required_keys = ["cycle_detection", "path_analysis", "proximity_analysis", 
                     "network_analysis", "motif_detection", "severity_adjustments"]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")

    # Validate cycle detection parameters
    cycle_config = config.get("cycle_detection", {})
    required_cycle_keys = ["max_cycle_length", "max_cycles_per_scc", "confidence_score"]
    for key in required_cycle_keys:
        if key not in cycle_config:
            raise ValueError(f"Missing required cycle detection parameter: {key}")

    # Validate path analysis parameters
    path_config = config.get("path_analysis", {})
    required_path_keys = ["min_path_length", "max_path_length", "max_paths_to_check", "confidence_score"]
    for key in required_path_keys:
        if key not in path_config:
            raise ValueError(f"Missing required path analysis parameter: {key}")

    # Validate proximity analysis parameters
    proximity_config = config.get("proximity_analysis", {})
    required_proximity_keys = ["max_distance", "confidence_score"]
    for key in required_proximity_keys:
        if key not in proximity_config:
            raise ValueError(f"Missing required proximity analysis parameter: {key}")


def get_config_summary(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get summary information about the loaded configuration."""
    return {
        "config_sections": len(config),
        "available_sections": list(config.keys()),
        "detection_methods": [
            "cycle_detection", "path_analysis", "proximity_analysis", 
            "network_analysis", "motif_detection"
        ],
        "config_source": "file_or_url"
    }