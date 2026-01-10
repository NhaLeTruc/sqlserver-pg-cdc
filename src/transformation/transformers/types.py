"""
Type conversion and conditional transformers.

Provides transformers for data type conversions and conditional
application of transformations based on business rules.
"""

import logging
import re
from typing import Any, Callable, Dict, List, Optional, Pattern

from opentelemetry import trace

from src.utils.tracing import trace_operation

from .base import (
    TRANSFORMATIONS_APPLIED,
    TRANSFORMATION_ERRORS,
    TRANSFORMATION_TIME,
    Transformer,
)

logger = logging.getLogger(__name__)


class TypeConversionTransformer(Transformer):
    """
    Convert data types.

    Useful for handling differences between source and target databases.
    """

    def __init__(self, target_type: type):
        """
        Initialize type conversion transformer.

        Args:
            target_type: Target Python type (str, int, float, bool, etc.)
        """
        self.target_type = target_type

    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        """Transform value by converting type."""
        with TRANSFORMATION_TIME.labels(transformer_type=self.get_type()).time():
            if value is None:
                return None

            try:
                converted = self.target_type(value)

                if type(value) != type(converted):
                    TRANSFORMATIONS_APPLIED.labels(
                        transformer_type=self.get_type(),
                        field_pattern=context.get("field_name", "unknown"),
                    ).inc()

                return converted

            except (ValueError, TypeError) as e:
                TRANSFORMATION_ERRORS.labels(
                    transformer_type=self.get_type(),
                    error_type=type(e).__name__,
                ).inc()
                logger.warning(f"Type conversion failed for {value}: {e}")
                return value


class ConditionalTransformer(Transformer):
    """
    Apply transformation conditionally based on predicate.

    Allows for business rule-based transformations.
    """

    def __init__(
        self,
        predicate: Callable[[Any, Dict[str, Any]], bool],
        transformer: Transformer,
        else_transformer: Optional[Transformer] = None,
    ):
        """
        Initialize conditional transformer.

        Args:
            predicate: Function that returns True if transformation should apply
            transformer: Transformer to apply if predicate is True
            else_transformer: Optional transformer to apply if predicate is False
        """
        self.predicate = predicate
        self.transformer = transformer
        self.else_transformer = else_transformer

    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        """Transform value conditionally."""
        with TRANSFORMATION_TIME.labels(transformer_type=self.get_type()).time():
            try:
                if self.predicate(value, context):
                    return self.transformer.transform(value, context)
                elif self.else_transformer:
                    return self.else_transformer.transform(value, context)
                else:
                    return value

            except Exception as e:
                TRANSFORMATION_ERRORS.labels(
                    transformer_type=self.get_type(),
                    error_type=type(e).__name__,
                ).inc()
                logger.warning(f"Conditional transformation failed: {e}")
                return value


class TransformationPipeline:
    """
    Chain multiple transformers for fields matching patterns.

    Allows building complex transformation workflows.
    """

    def __init__(self):
        """Initialize transformation pipeline."""
        self.field_transformers: Dict[str, List[Transformer]] = {}
        self.compiled_patterns: Dict[str, Pattern] = {}

    def add_transformer(
        self,
        field_pattern: str,
        transformer: Transformer,
        case_sensitive: bool = False,
    ) -> None:
        """
        Add transformer for fields matching pattern.

        Args:
            field_pattern: Regex pattern to match field names
            transformer: Transformer to apply
            case_sensitive: Whether pattern matching is case sensitive
        """
        if field_pattern not in self.field_transformers:
            self.field_transformers[field_pattern] = []

        self.field_transformers[field_pattern].append(transformer)

        # Compile regex pattern
        flags = 0 if case_sensitive else re.IGNORECASE
        self.compiled_patterns[field_pattern] = re.compile(field_pattern, flags)

        logger.debug(
            f"Added {transformer.get_type()} for pattern '{field_pattern}'"
        )

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform all fields in row.

        Args:
            row: Dictionary of field_name -> value

        Returns:
            Transformed row dictionary
        """
        with trace_operation(
            "transform_row",
            kind=trace.SpanKind.INTERNAL,
            field_count=len(row),
        ):
            transformed = row.copy()

            for field_name, value in row.items():
                # Check each pattern
                for pattern_str, transformers in self.field_transformers.items():
                    pattern = self.compiled_patterns[pattern_str]

                    if pattern.match(field_name):
                        # Apply all transformers for this pattern
                        for transformer in transformers:
                            value = transformer.transform(
                                value, {"field_name": field_name, "row": row}
                            )

                transformed[field_name] = value

            return transformed

    def transform_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform multiple rows.

        Args:
            rows: List of row dictionaries

        Returns:
            List of transformed row dictionaries
        """
        return [self.transform_row(row) for row in rows]

    def get_transformer_count(self) -> int:
        """Get total number of registered transformers."""
        return sum(len(transformers) for transformers in self.field_transformers.values())

    def get_patterns(self) -> List[str]:
        """Get list of registered field patterns."""
        return list(self.field_transformers.keys())
