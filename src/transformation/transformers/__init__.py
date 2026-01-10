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

from .base import Transformer
from .pii import HashingTransformer, PIIMaskingTransformer
from .rules import create_gdpr_pipeline, create_pii_pipeline
from .types import (
    ConditionalTransformer,
    TransformationPipeline,
    TypeConversionTransformer,
)

__all__ = [
    "Transformer",
    "PIIMaskingTransformer",
    "HashingTransformer",
    "TypeConversionTransformer",
    "ConditionalTransformer",
    "TransformationPipeline",
    "create_pii_pipeline",
    "create_gdpr_pipeline",
]
