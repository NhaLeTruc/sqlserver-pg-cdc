"""
Business rule transformers and pipeline factories.

Provides factory functions for creating pre-configured transformation
pipelines for common use cases (PII masking, GDPR compliance, etc.).
"""

import logging

from .pii import HashingTransformer, PIIMaskingTransformer
from .types import TransformationPipeline

logger = logging.getLogger(__name__)


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
