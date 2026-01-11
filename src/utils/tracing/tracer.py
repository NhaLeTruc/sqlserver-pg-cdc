"""
Tracer initialization and configuration for OpenTelemetry.

Provides setup functions for distributed tracing with OTLP exporters
and auto-instrumentation for common libraries.
"""

import logging
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

logger = logging.getLogger(__name__)

# Global tracer instance
_tracer: trace.Tracer | None = None
_is_initialized = False


def initialize_tracing(
    service_name: str = "sqlserver-pg-cdc",
    otlp_endpoint: str | None = None,
    console_export: bool = False,
    sampling_rate: float = 1.0
) -> trace.Tracer:
    """
    Initialize distributed tracing with OpenTelemetry.

    Args:
        service_name: Name of the service for identification
        otlp_endpoint: OTLP collector endpoint (e.g., "localhost:4317")
        console_export: If True, also export traces to console (debug)
        sampling_rate: Sampling rate 0.0-1.0 (1.0 = trace everything)

    Returns:
        Configured tracer instance

    Example:
        >>> tracer = initialize_tracing(
        ...     service_name="cdc-reconciliation",
        ...     otlp_endpoint="localhost:4317"
        ... )
    """
    global _tracer, _is_initialized

    if _is_initialized:
        logger.warning("Tracing already initialized, returning existing tracer")
        return _tracer

    # Create resource with service name
    resource = Resource(attributes={
        SERVICE_NAME: service_name
    })

    # Set up tracer provider
    provider = TracerProvider(resource=resource)

    # Configure exporters
    exporters = []

    # OTLP exporter (Jaeger, Tempo, etc.)
    if otlp_endpoint is None:
        otlp_endpoint = os.getenv("OTLP_ENDPOINT", "localhost:4317")

    if otlp_endpoint:
        try:
            otlp_exporter = OTLPSpanExporter(
                endpoint=otlp_endpoint,
                insecure=True  # Use insecure for local development
            )
            provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            exporters.append("OTLP")
            logger.info(f"OTLP exporter configured: {otlp_endpoint}")
        except Exception as e:
            logger.warning(f"Failed to configure OTLP exporter: {e}")

    # Console exporter for debugging
    if console_export or os.getenv("TRACE_CONSOLE", "").lower() == "true":
        console_exporter = ConsoleSpanExporter()
        provider.add_span_processor(BatchSpanProcessor(console_exporter))
        exporters.append("Console")
        logger.info("Console exporter configured")

    if not exporters:
        logger.warning("No trace exporters configured, tracing will be a no-op")

    # Set global tracer provider
    trace.set_tracer_provider(provider)

    # Get tracer instance
    _tracer = trace.get_tracer(service_name)
    _is_initialized = True

    logger.info(
        f"Tracing initialized: {service_name} "
        f"(exporters: {', '.join(exporters)}, sampling: {sampling_rate})"
    )

    return _tracer


def get_tracer() -> trace.Tracer:
    """
    Get the global tracer instance.

    Initializes tracing with defaults if not already initialized.

    Returns:
        Configured tracer instance
    """
    global _tracer

    if _tracer is None:
        logger.info("Tracer not initialized, initializing with defaults")
        _tracer = initialize_tracing()

    return _tracer


def shutdown_tracing(timeout: int = 30):
    """
    Shutdown tracing and flush pending spans.

    Should be called before application exit.

    Args:
        timeout: Timeout in seconds for flushing spans
    """
    global _is_initialized

    if _is_initialized:
        try:
            provider = trace.get_tracer_provider()
            if hasattr(provider, 'shutdown'):
                provider.shutdown()
            logger.info("Tracing shutdown complete")
        except Exception as e:
            logger.error(f"Error during tracing shutdown: {e}")
        finally:
            _is_initialized = False


# Auto-instrumentation helpers
def instrument_psycopg2():
    """
    Automatically instrument psycopg2 for database tracing.

    Call this once at application startup.
    """
    try:
        from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
        Psycopg2Instrumentor().instrument()
        logger.info("psycopg2 instrumentation enabled")
    except ImportError:
        logger.warning("opentelemetry-instrumentation-psycopg2 not installed")
    except Exception as e:
        logger.error(f"Failed to instrument psycopg2: {e}")


def instrument_requests():
    """
    Automatically instrument requests library for HTTP tracing.

    Call this once at application startup.
    """
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
        logger.info("requests instrumentation enabled")
    except ImportError:
        logger.warning("opentelemetry-instrumentation-requests not installed")
    except Exception as e:
        logger.error(f"Failed to instrument requests: {e}")


def setup_auto_instrumentation():
    """
    Setup all available auto-instrumentation.

    Convenience function to enable all supported auto-instrumentation.
    """
    logger.info("Setting up auto-instrumentation")

    # Database instrumentation
    instrument_psycopg2()

    # HTTP instrumentation
    instrument_requests()

    logger.info("Auto-instrumentation setup complete")
