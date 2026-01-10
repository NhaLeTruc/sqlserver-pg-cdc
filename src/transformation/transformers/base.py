"""
Base transformer class and common utilities.

Provides abstract base class for all transformers and shared metrics
for tracking transformation operations.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from prometheus_client import Counter, Histogram

logger = logging.getLogger(__name__)


# Metrics
TRANSFORMATIONS_APPLIED = Counter(
    "transformations_applied_total",
    "Total transformations applied",
    ["transformer_type", "field_pattern"],
)

TRANSFORMATION_TIME = Histogram(
    "transformation_seconds",
    "Time to apply transformations",
    ["transformer_type"],
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1],
)

TRANSFORMATION_ERRORS = Counter(
    "transformation_errors_total",
    "Transformation errors",
    ["transformer_type", "error_type"],
)


class Transformer(ABC):
    """Base class for data transformers."""

    @abstractmethod
    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        """
        Transform a single value.

        Args:
            value: Value to transform
            context: Transformation context (field_name, row, etc.)

        Returns:
            Transformed value
        """
        pass

    def get_type(self) -> str:
        """Get transformer type for metrics."""
        return self.__class__.__name__
