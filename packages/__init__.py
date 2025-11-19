import signal
import sys
import time
import uuid
import os
import threading
from typing import Dict, Optional, Any
from dotenv import load_dotenv
from prometheus_client import (
    CollectorRegistry, Counter, Histogram, Gauge, Info,
    start_http_server, generate_latest
)
from loguru import logger
import socket

load_dotenv()

_correlation_context = threading.local()

def generate_correlation_id() -> str:
    """Generate a unique correlation ID for request tracing."""
    return f"req_{uuid.uuid4().hex[:12]}"


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID from thread-local storage."""
    return getattr(_correlation_context, 'correlation_id', None)


def set_correlation_id(correlation_id: str):
    """Set the correlation ID in thread-local storage."""
    _correlation_context.correlation_id = correlation_id


def setup_logger(service_name: str):
    """
    Setup simple logger with auto-detection of service name.

    Args:
        service_name: Optional service name. If not provided, auto-detects from file path.
    """

    def patch_record(record):
        record["extra"]["service"] = service_name
        correlation_id = get_correlation_id()
        if correlation_id:
            record["extra"]["correlation_id"] = correlation_id
        record["extra"]["timestamp"] = time.time()
        return True

    # Try to get logs directory from environment variable first
    logs_dir = os.environ.get('LOGS_DIR')

    if not logs_dir:
        # Get the absolute path to the project root directory
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        logs_dir = os.path.join(project_root, "logs")

    # Ensure the directory exists and is writable
    try:
        os.makedirs(logs_dir, exist_ok=True)

        # Test write access by creating and removing a temporary file
        import tempfile
        test_file = os.path.join(logs_dir, f'.write_test_{service_name}_{int(time.time())}')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)

    except (OSError, PermissionError) as e:
        # Fallback to a temp directory if the intended logs directory isn't accessible
        import tempfile
        fallback_logs_dir = os.path.join(tempfile.gettempdir(), 'analytics-pipeline-logs')

        try:
            os.makedirs(fallback_logs_dir, exist_ok=True)
            # Test the fallback directory too
            test_file = os.path.join(fallback_logs_dir, f'.write_test_{service_name}_{int(time.time())}')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)

            logs_dir = fallback_logs_dir
            print(f"Warning: Using fallback logs directory {logs_dir} due to permission error: {e}")

        except (OSError, PermissionError):
            # Last resort: use current directory
            logs_dir = os.getcwd()
            print(f"Warning: Using current directory for logs due to permission errors. Original error: {e}")

    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()

    logger.remove()

    # File logger with JSON serialization for Loki ingestion
    try:
        logger.add(
            os.path.join(logs_dir, f"{service_name}.log"),
            rotation="500 MB",
            level=log_level,
            filter=patch_record,
            serialize=True,
            format="{time} | {level} | {extra[service]} | {message} | {extra}"
        )
    except Exception as e:
        # If file logging fails completely, just proceed with console logging
        print(f"Warning: Could not set up file logging: {e}. Proceeding with console-only logging.")

    # Console logger with human-readable format
    console_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{extra[service]}</cyan> | {message} | <white>{extra}</white>"
    if get_correlation_id():
        console_format += " | <yellow>{extra[correlation_id]}</yellow>"

    logger.add(
        sys.stdout,
        format=console_format,
        level=log_level,
        filter=patch_record,
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )

    logger.info(f"Logger configured with level: {log_level}")

    return service_name

terminate_event = threading.Event()

def signal_handler(sig, frame):
    logger.info(
        "Shutdown signal received",
        extra={
            "signal": sig,
        }
    )
    terminate_event.set()
    time.sleep(2)

def shutdown_handler(signum, frame):
    logger.info("Shutdown signal received. Waiting for current processing to complete...")
    terminate_event.set()

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# Global metrics registry per service
_service_registries: Dict[str, CollectorRegistry] = {}
_metrics_servers: Dict[str, Any] = {}
_metrics_lock = threading.Lock()


class MetricsRegistry:
    """Centralized metrics registry for a service following logging conventions"""

    def __init__(self, service_name: str, port: Optional[int] = None):
        self.service_name = service_name
        self.registry = CollectorRegistry()
        self.port = port
        self.server = None
        self._common_labels = self._extract_labels_from_service_name(service_name)

        # Initialize common metrics
        self._init_common_metrics()

    def _extract_labels_from_service_name(self, service_name: str) -> Dict[str, str]:
        """Extract common labels from service name following logging conventions"""
        labels = {"service": service_name}

        # Parse service name patterns for analytics pipeline
        if 'analytics' in service_name:
            labels["component"] = "analytics"
        elif 'api' in service_name:
            labels["component"] = "api"

        return labels

    def _init_common_metrics(self):
        """Initialize common metrics available to all services"""
        # Service info
        self.service_info = Info(
            'service_info',
            'Service information',
            registry=self.registry
        )
        self.service_info.info({
            'service_name': self.service_name,
            'version': '1.0.0',
            **self._common_labels
        })

        # Service uptime
        self.service_start_time = Gauge(
            'service_start_time_seconds',
            'Service start time in Unix timestamp',
            registry=self.registry
        )
        self.service_start_time.set_to_current_time()

        # Common error counter
        self.errors_total = Counter(
            'service_errors_total',
            'Total number of errors by type',
            ['error_type', 'component'],
            registry=self.registry
        )

        # Health status
        self.health_status = Gauge(
            'service_health_status',
            'Service health status (1=healthy, 0=unhealthy)',
            registry=self.registry
        )
        self.health_status.set(1)  # Start as healthy

    def create_counter(self, name: str, description: str, labelnames: list = None) -> Counter:
        """Create a counter metric with common labels"""
        return Counter(
            name, description,
            labelnames or [],
            registry=self.registry
        )

    def create_histogram(self, name: str, description: str, labelnames: list = None,
                         buckets: tuple = None) -> Histogram:
        """Create a histogram metric with common labels"""
        kwargs = {
            'name': name,
            'documentation': description,
            'labelnames': labelnames or [],
            'registry': self.registry
        }
        if buckets:
            kwargs['buckets'] = buckets
        return Histogram(**kwargs)

    def create_gauge(self, name: str, description: str, labelnames: list = None) -> Gauge:
        """Create a gauge metric with common labels"""
        return Gauge(
            name, description,
            labelnames or [],
            registry=self.registry
        )

    def start_metrics_server(self, port: Optional[int] = None) -> bool:
        """Start HTTP server for metrics endpoint"""
        if self.server is not None:
            logger.warning(f"Metrics server already running for {self.service_name}")
            return True

        target_port = port or self.port or self._get_default_port()

        try:
            # Check if port is available
            if not self._is_port_available(target_port):
                logger.warning(f"Port {target_port} not available, trying next available port")
                target_port = self._find_available_port(target_port)

            self.server = start_http_server(target_port, registry=self.registry)
            self.port = target_port
            logger.info(f"Metrics server started for {self.service_name} on port {target_port}")
            logger.info(f"Metrics available at: http://localhost:{target_port}/metrics")
            return True

        except Exception as e:
            logger.error(f"Failed to start metrics server for {self.service_name}: {e}")
            return False

    def _get_default_port(self) -> int:
        """Get default port based on service name"""
        # Check generic METRICS_PORT environment variable
        env_port = os.getenv('METRICS_PORT')
        if env_port:
            try:
                return int(env_port)
            except ValueError:
                logger.warning(f"Invalid METRICS_PORT value: {env_port}, using default")

        # Base port mapping for analytics services
        port_mapping = {
            'analytics-initialize-analyzers': 9301,
            'analytics-detect-typologies': 9302,
            'analytics-detect-structural-patterns': 9303,
            'analytics-build-features': 9304,
            'analytics-api': 9300,
        }

        # Try to match service name to port
        for key, port in port_mapping.items():
            if key in self.service_name:
                return port

        # Default fallback
        return 9090

    def _is_port_available(self, port: int) -> bool:
        """Check if port is available"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return True
        except OSError:
            return False

    def _find_available_port(self, start_port: int) -> int:
        """Find next available port starting from start_port"""
        for port in range(start_port, start_port + 100):
            if self._is_port_available(port):
                return port
        raise RuntimeError(f"No available ports found starting from {start_port}")

    def get_metrics_text(self) -> str:
        """Get metrics in Prometheus text format"""
        return generate_latest(self.registry).decode('utf-8')

    def record_error(self, error_type: str, component: str = "unknown"):
        """Record an error occurrence"""
        self.errors_total.labels(error_type=error_type, component=component).inc()

    def set_health_status(self, healthy: bool):
        """Set service health status"""
        self.health_status.set(1 if healthy else 0)

def setup_metrics(service_name: str, port: Optional[int] = None, start_server: bool = True) -> MetricsRegistry:
    """
    Setup metrics for a service following the same pattern as setup_logger.

    Args:
        service_name: Name of the service (e.g., 'analytics-detect-typologies')
        port: Optional port for metrics server
        start_server: Whether to start HTTP server immediately

    Returns:
        MetricsRegistry: Configured metrics registry for the service
    """
    with _metrics_lock:
        if service_name in _service_registries:
            logger.debug(f"Metrics already setup for {service_name}")
            return _service_registries[service_name]

        # Create metrics registry
        metrics_registry = MetricsRegistry(service_name, port)
        _service_registries[service_name] = metrics_registry

        # Start metrics server if requested
        if start_server:
            success = metrics_registry.start_metrics_server()
            if success:
                _metrics_servers[service_name] = metrics_registry.server

        logger.info(f"Metrics setup completed for service: {service_name}")
        return metrics_registry

def get_metrics_registry(service_name: str) -> Optional[MetricsRegistry]:
    """Get existing metrics registry for a service"""
    return _service_registries.get(service_name)

def shutdown_metrics_servers():
    """Shutdown all metrics servers"""
    with _metrics_lock:
        for service_name, server in _metrics_servers.items():
            try:
                if hasattr(server, 'shutdown'):
                    server.shutdown()
                logger.info(f"Shutdown metrics server for {service_name}")
            except Exception as e:
                logger.error(f"Error shutting down metrics server for {service_name}: {e}")

        _metrics_servers.clear()


# Common metric buckets for different use cases
DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float('inf'))
SIZE_BUCKETS = (64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, float('inf'))
COUNT_BUCKETS = (1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, float('inf'))

from functools import wraps
import traceback
from typing import Optional


def manage_metrics(success_metric_name: str = "execution_success", failure_metric_name: str = "execution_failure"):
    """
    Decorator to manage metrics for a function, recording success on completion
    and failure on any exception.

    Args:
        success_metric_name (str): Name of the metric to record on successful execution.
        failure_metric_name (str): Name of the metric to record on failed execution.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Initialize metrics_registry as None
            metrics_registry = None
            correlation_id = get_correlation_id() or generate_correlation_id()
            set_correlation_id(correlation_id)  # Ensure correlation ID is set for logging

            try:
                # Try to find metrics_registry in args or kwargs
                if len(args) > 0 and isinstance(args[0], str):
                    # Assume first argument is service_name, get registry
                    metrics_registry = get_metrics_registry(args[0])
                elif 'service_name' in kwargs:
                    metrics_registry = get_metrics_registry(kwargs['service_name'])

                # If not found, try to access from local scope after function execution
                if metrics_registry is None:
                    local_vars = {}

                    def capture_locals(*args, **kwargs):
                        local_vars['metrics_registry'] = locals().get('metrics_registry')
                        return func(*args, **kwargs)

                    result = capture_locals(*args, **kwargs)
                    metrics_registry = local_vars.get('metrics_registry')
                else:
                    result = func(*args, **kwargs)

                # Record success metric if metrics_registry is available
                if metrics_registry is not None:
                    # Create success counter if it doesn't exist
                    if not hasattr(metrics_registry, success_metric_name):
                        setattr(
                            metrics_registry,
                            success_metric_name,
                            metrics_registry.create_counter(
                                name=success_metric_name,
                                description=f"Total number of successful {func.__name__} executions",
                                labelnames=["component"]
                            )
                        )
                    # Increment success counter
                    getattr(metrics_registry, success_metric_name).labels(component="main").inc()
                    metrics_registry.set_health_status(True)
                    logger.info(
                        f"Successfully recorded {success_metric_name} for {func.__name__}",
                        extra={"correlation_id": correlation_id}
                    )

                return result

            except Exception as e:
                # Record failure metric if metrics_registry is available
                if metrics_registry is not None:
                    metrics_registry.record_error(failure_metric_name, component="main")
                    metrics_registry.set_health_status(False)
                    logger.error(
                        f"Failed to execute {func.__name__}: {str(e)}",
                        extra={"correlation_id": correlation_id, "traceback": traceback.format_exc()}
                    )
                # Re-raise the exception to maintain original behavior
                raise

            finally:
                # Reset correlation ID to avoid leakage
                set_correlation_id(None)

        return wrapper

    return decorator