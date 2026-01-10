"""
Data transformation framework for CDC pipeline.

Supports:
- PII masking (email, phone, SSN, etc.)
- Field-level hashing/encryption
- Data type conversions
- Business rule application
- Configurable transformation pipelines

Designed to be integrated with Kafka Connect SMTs or used standalone
in reconciliation processes.
"""

import hashlib
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Pattern

from opentelemetry import trace
from prometheus_client import Counter, Histogram

from src.utils.tracing import get_tracer, trace_operation

logger = logging.getLogger(__name__)
tracer = get_tracer()


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


class PIIMaskingTransformer(Transformer):
    """
    Mask PII data (email, phone, SSN, credit cards, etc.).

    Preserves format while obscuring sensitive data.
    """

    def __init__(
        self,
        mask_char: str = "*",
        preserve_format: bool = True,
        email_preserve_domain: bool = True,
    ):
        """
        Initialize PII masking transformer.

        Args:
            mask_char: Character to use for masking
            preserve_format: Whether to preserve original format
            email_preserve_domain: Keep email domain visible
        """
        self.mask_char = mask_char
        self.preserve_format = preserve_format
        self.email_preserve_domain = email_preserve_domain

    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        """Transform value by masking PII fields."""
        with TRANSFORMATION_TIME.labels(transformer_type=self.get_type()).time():
            if not isinstance(value, str):
                return value

            field_name = context.get("field_name", "").lower()

            try:
                if "email" in field_name:
                    result = self._mask_email(value)
                elif "phone" in field_name or "mobile" in field_name or "tel" in field_name:
                    result = self._mask_phone(value)
                elif "ssn" in field_name or "social" in field_name:
                    result = self._mask_ssn(value)
                elif "credit" in field_name or "card" in field_name or "cc" in field_name:
                    result = self._mask_credit_card(value)
                elif "ip" in field_name and "address" in field_name:
                    result = self._mask_ip_address(value)
                else:
                    result = value

                if result != value:
                    TRANSFORMATIONS_APPLIED.labels(
                        transformer_type=self.get_type(),
                        field_pattern=field_name,
                    ).inc()

                return result

            except Exception as e:
                TRANSFORMATION_ERRORS.labels(
                    transformer_type=self.get_type(),
                    error_type=type(e).__name__,
                ).inc()
                logger.warning(f"PII masking failed for {field_name}: {e}")
                return value

    def _mask_email(self, email: str) -> str:
        """
        Mask email address.

        Examples:
            user@example.com -> u***@example.com
            john.doe@company.com -> j*******@company.com
        """
        if "@" not in email:
            return email

        local, domain = email.split("@", 1)

        if len(local) <= 1:
            return email

        if self.email_preserve_domain:
            masked_local = local[0] + self.mask_char * (len(local) - 1)
            return f"{masked_local}@{domain}"
        else:
            masked_local = local[0] + self.mask_char * (len(local) - 1)
            masked_domain = self.mask_char * len(domain)
            return f"{masked_local}@{masked_domain}"

    def _mask_phone(self, phone: str) -> str:
        """
        Mask phone number keeping last 4 digits.

        Examples:
            (123) 456-7890 -> (***) ***-7890
            +1-555-123-4567 -> +*-***-***-4567
        """
        # Extract all digits
        digits = re.sub(r"\D", "", phone)

        if len(digits) < 4:
            return phone

        # Keep last 4 digits
        masked_digits = self.mask_char * (len(digits) - 4) + digits[-4:]

        if self.preserve_format:
            # Preserve original format by replacing digits in place
            result = phone
            digit_index = 0
            for i, char in enumerate(phone):
                if char.isdigit():
                    if digit_index < len(masked_digits):
                        result = result[:i] + masked_digits[digit_index] + result[i + 1 :]
                        digit_index += 1
            return result
        else:
            return masked_digits

    def _mask_ssn(self, ssn: str) -> str:
        """
        Mask Social Security Number keeping last 4 digits.

        Examples:
            123-45-6789 -> ***-**-6789
            123456789 -> *****6789
        """
        digits = re.sub(r"\D", "", ssn)

        if len(digits) != 9:
            # Not a valid SSN, mask entire value
            return self.mask_char * len(ssn)

        if self.preserve_format and "-" in ssn:
            return f"{self.mask_char * 3}-{self.mask_char * 2}-{digits[-4:]}"
        else:
            return self.mask_char * 5 + digits[-4:]

    def _mask_credit_card(self, card: str) -> str:
        """
        Mask credit card number keeping last 4 digits.

        Examples:
            4532-1234-5678-9010 -> ****-****-****-9010
            4532123456789010 -> ************9010
        """
        digits = re.sub(r"\D", "", card)

        if len(digits) < 13 or len(digits) > 19:
            # Not a valid credit card length
            return self.mask_char * len(card)

        # Keep last 4 digits
        masked_digits = self.mask_char * (len(digits) - 4) + digits[-4:]

        if self.preserve_format:
            result = card
            digit_index = 0
            for i, char in enumerate(card):
                if char.isdigit():
                    if digit_index < len(masked_digits):
                        result = result[:i] + masked_digits[digit_index] + result[i + 1 :]
                        digit_index += 1
            return result
        else:
            return masked_digits

    def _mask_ip_address(self, ip: str) -> str:
        """
        Mask IP address keeping first octet.

        Examples:
            192.168.1.100 -> 192.***.*.***
            10.0.0.1 -> 10.*.*.* """
        # IPv4 pattern
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", ip):
            parts = ip.split(".")
            return f"{parts[0]}.{self.mask_char * 3}.{self.mask_char}.{self.mask_char * 3}"

        # IPv6 or other - mask most of it
        return ip[:4] + self.mask_char * (len(ip) - 4)


class HashingTransformer(Transformer):
    """
    One-way hash transformation for PII.

    Useful for pseudonymization where you need consistent but irreversible
    transformation of identifiers.
    """

    def __init__(
        self,
        algorithm: str = "sha256",
        salt: str = "",
        truncate: Optional[int] = None,
    ):
        """
        Initialize hashing transformer.

        Args:
            algorithm: Hash algorithm (sha256, sha512, md5, etc.)
            salt: Salt to add to values before hashing
            truncate: Optional truncation length for hash output
        """
        self.algorithm = algorithm
        self.salt = salt
        self.truncate = truncate

    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        """Transform value by hashing."""
        with TRANSFORMATION_TIME.labels(transformer_type=self.get_type()).time():
            if value is None:
                return None

            try:
                data = f"{self.salt}{str(value)}".encode("utf-8")
                hasher = hashlib.new(self.algorithm)
                hasher.update(data)
                hash_value = hasher.hexdigest()

                if self.truncate:
                    hash_value = hash_value[: self.truncate]

                TRANSFORMATIONS_APPLIED.labels(
                    transformer_type=self.get_type(),
                    field_pattern=context.get("field_name", "unknown"),
                ).inc()

                return hash_value

            except Exception as e:
                TRANSFORMATION_ERRORS.labels(
                    transformer_type=self.get_type(),
                    error_type=type(e).__name__,
                ).inc()
                logger.warning(f"Hashing failed: {e}")
                return value


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


def create_pii_pipeline(salt: str = "default_salt") -> TransformationPipeline:
    """
    Create standard PII transformation pipeline.

    This is a pre-configured pipeline for common PII masking scenarios.

    Args:
        salt: Salt for hashing transformations

    Returns:
        Configured TransformationPipeline
    """
    pipeline = TransformationPipeline()

    # Mask PII fields
    masker = PIIMaskingTransformer()
    pipeline.add_transformer(r".*email.*", masker)
    pipeline.add_transformer(r".*phone.*", masker)
    pipeline.add_transformer(r".*mobile.*", masker)
    pipeline.add_transformer(r".*ssn.*", masker)
    pipeline.add_transformer(r".*social.*security.*", masker)
    pipeline.add_transformer(r".*credit.*card.*", masker)
    pipeline.add_transformer(r".*cc_number.*", masker)
    pipeline.add_transformer(r".*ip.*address.*", masker)

    # Hash sensitive IDs
    hasher = HashingTransformer(algorithm="sha256", salt=salt, truncate=16)
    pipeline.add_transformer(r".*customer_id.*", hasher)
    pipeline.add_transformer(r".*user_id.*", hasher)
    pipeline.add_transformer(r".*account_id.*", hasher)

    logger.info(
        f"Created PII pipeline with {pipeline.get_transformer_count()} transformers"
    )

    return pipeline


def create_gdpr_pipeline(salt: str = "gdpr_salt") -> TransformationPipeline:
    """
    Create GDPR-compliant transformation pipeline.

    Pseudonymizes personal data while maintaining data utility.

    Args:
        salt: Salt for hashing transformations

    Returns:
        Configured TransformationPipeline
    """
    pipeline = TransformationPipeline()

    # Pseudonymize identifiable information
    hasher = HashingTransformer(algorithm="sha256", salt=salt)

    pipeline.add_transformer(r".*email.*", hasher)
    pipeline.add_transformer(r".*name.*", hasher)
    pipeline.add_transformer(r".*address.*", hasher)
    pipeline.add_transformer(r".*phone.*", hasher)
    pipeline.add_transformer(r".*ip.*", hasher)

    logger.info(f"Created GDPR pipeline with {pipeline.get_transformer_count()} transformers")

    return pipeline
