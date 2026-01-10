"""
Unit tests for data transformation framework.

Tests PII masking, hashing, type conversion, and transformation pipelines.
"""

from unittest.mock import Mock, patch

import pytest

from transformation.transformers import (
    ConditionalTransformer,
    HashingTransformer,
    PIIMaskingTransformer,
    TransformationPipeline,
    Transformer,
    TypeConversionTransformer,
    create_gdpr_pipeline,
    create_pii_pipeline,
)


class TestPIIMaskingTransformer:
    """Test PII masking transformer functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = PIIMaskingTransformer()

    def test_mask_email_basic(self):
        """Test basic email masking."""
        result = self.transformer.transform(
            "user@example.com", {"field_name": "email"}
        )
        assert result == "u***@example.com"

    def test_mask_email_long_local_part(self):
        """Test email masking with long local part."""
        result = self.transformer.transform(
            "john.doe@company.com", {"field_name": "user_email"}
        )
        assert result == "j*******@company.com"
        assert "@company.com" in result

    def test_mask_email_single_char_local(self):
        """Test email masking with single character local part."""
        result = self.transformer.transform("a@test.com", {"field_name": "email"})
        # Should not mask single character
        assert result == "a@test.com"

    def test_mask_email_no_at_sign(self):
        """Test email masking with invalid email."""
        result = self.transformer.transform("notanemail", {"field_name": "email"})
        assert result == "notanemail"

    def test_mask_email_without_domain_preservation(self):
        """Test email masking without domain preservation."""
        transformer = PIIMaskingTransformer(email_preserve_domain=False)
        result = transformer.transform("user@example.com", {"field_name": "email"})
        assert result.startswith("u***@")
        assert result != "u***@example.com"

    def test_mask_phone_basic(self):
        """Test phone number masking."""
        result = self.transformer.transform(
            "(123) 456-7890", {"field_name": "phone"}
        )
        assert result == "(***) ***-7890"
        assert result.endswith("7890")

    def test_mask_phone_international(self):
        """Test international phone masking."""
        result = self.transformer.transform(
            "+1-555-123-4567", {"field_name": "mobile"}
        )
        assert result.endswith("4567")
        assert "+" in result
        assert "-" in result

    def test_mask_phone_digits_only(self):
        """Test phone masking with digits only."""
        result = self.transformer.transform("1234567890", {"field_name": "tel"})
        assert result.endswith("7890")

    def test_mask_phone_too_short(self):
        """Test phone masking with too few digits."""
        result = self.transformer.transform("123", {"field_name": "phone"})
        # Should not mask if less than 4 digits
        assert result == "123"

    def test_mask_phone_without_format_preservation(self):
        """Test phone masking without format preservation."""
        transformer = PIIMaskingTransformer(preserve_format=False)
        result = transformer.transform("(123) 456-7890", {"field_name": "phone"})
        # Should return masked digits only
        assert result == "******7890"

    def test_mask_ssn_basic(self):
        """Test SSN masking."""
        result = self.transformer.transform("123-45-6789", {"field_name": "ssn"})
        assert result == "***-**-6789"
        assert result.endswith("6789")

    def test_mask_ssn_no_dashes(self):
        """Test SSN masking without dashes."""
        result = self.transformer.transform("123456789", {"field_name": "social"})
        assert result == "*****6789"

    def test_mask_ssn_invalid_length(self):
        """Test SSN masking with invalid length."""
        result = self.transformer.transform("12345", {"field_name": "ssn"})
        # Should mask entire value if not valid SSN
        assert result == "*****"

    def test_mask_ssn_without_format_preservation(self):
        """Test SSN masking without format preservation."""
        transformer = PIIMaskingTransformer(preserve_format=False)
        result = transformer.transform("123-45-6789", {"field_name": "ssn"})
        assert result == "*****6789"

    def test_mask_credit_card_basic(self):
        """Test credit card masking."""
        result = self.transformer.transform(
            "4532-1234-5678-9010", {"field_name": "credit_card"}
        )
        assert result == "****-****-****-9010"
        assert result.endswith("9010")

    def test_mask_credit_card_no_dashes(self):
        """Test credit card masking without dashes."""
        result = self.transformer.transform(
            "4532123456789010", {"field_name": "cc_number"}
        )
        assert result.endswith("9010")
        assert len(result) == 16

    def test_mask_credit_card_invalid_length(self):
        """Test credit card masking with invalid length."""
        result = self.transformer.transform("123456", {"field_name": "card"})
        # Should mask entire value if not valid card length
        assert all(c == "*" for c in result)

    def test_mask_ip_address_ipv4(self):
        """Test IPv4 address masking."""
        result = self.transformer.transform(
            "192.168.1.100", {"field_name": "ip_address"}
        )
        assert result.startswith("192.")
        assert result == "192.***.*.***"

    def test_mask_ip_address_ipv6(self):
        """Test IPv6 address masking."""
        result = self.transformer.transform(
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            {"field_name": "ip_address"},
        )
        # Should keep first 4 characters
        assert result.startswith("2001")
        assert len(result) == len("2001:0db8:85a3:0000:0000:8a2e:0370:7334")

    def test_no_masking_for_unrecognized_field(self):
        """Test that unrecognized fields are not masked."""
        result = self.transformer.transform("sensitive data", {"field_name": "other"})
        assert result == "sensitive data"

    def test_non_string_value_passthrough(self):
        """Test that non-string values pass through unchanged."""
        result = self.transformer.transform(12345, {"field_name": "email"})
        assert result == 12345

    def test_custom_mask_char(self):
        """Test custom masking character."""
        transformer = PIIMaskingTransformer(mask_char="X")
        result = transformer.transform("user@example.com", {"field_name": "email"})
        assert result == "uXXX@example.com"

    def test_exception_handling(self):
        """Test that exceptions are caught and original value returned."""
        with patch.object(
            PIIMaskingTransformer, "_mask_email", side_effect=Exception("Test error")
        ):
            transformer = PIIMaskingTransformer()
            result = transformer.transform("user@example.com", {"field_name": "email"})
            # Should return original value on error
            assert result == "user@example.com"


class TestHashingTransformer:
    """Test hashing transformer functionality."""

    def test_basic_hashing(self):
        """Test basic SHA256 hashing."""
        transformer = HashingTransformer()
        result = transformer.transform("test@example.com", {"field_name": "email"})

        assert isinstance(result, str)
        assert len(result) == 64  # SHA256 hex digest length

    def test_hashing_with_salt(self):
        """Test hashing with salt."""
        transformer1 = HashingTransformer(salt="salt1")
        transformer2 = HashingTransformer(salt="salt2")

        value = "test@example.com"
        result1 = transformer1.transform(value, {"field_name": "email"})
        result2 = transformer2.transform(value, {"field_name": "email"})

        # Different salts should produce different hashes
        assert result1 != result2

    def test_hashing_with_truncation(self):
        """Test hashing with truncation."""
        transformer = HashingTransformer(truncate=16)
        result = transformer.transform("test@example.com", {"field_name": "email"})

        assert len(result) == 16

    def test_hashing_different_algorithms(self):
        """Test different hashing algorithms."""
        transformer_sha256 = HashingTransformer(algorithm="sha256")
        transformer_sha512 = HashingTransformer(algorithm="sha512")
        transformer_md5 = HashingTransformer(algorithm="md5")

        value = "test@example.com"
        result_sha256 = transformer_sha256.transform(value, {"field_name": "email"})
        result_sha512 = transformer_sha512.transform(value, {"field_name": "email"})
        result_md5 = transformer_md5.transform(value, {"field_name": "email"})

        assert len(result_sha256) == 64  # SHA256
        assert len(result_sha512) == 128  # SHA512
        assert len(result_md5) == 32  # MD5

    def test_hashing_deterministic(self):
        """Test that hashing is deterministic."""
        transformer = HashingTransformer(salt="test_salt")
        value = "test@example.com"

        result1 = transformer.transform(value, {"field_name": "email"})
        result2 = transformer.transform(value, {"field_name": "email"})

        assert result1 == result2

    def test_hashing_different_values(self):
        """Test that different values produce different hashes."""
        transformer = HashingTransformer()

        result1 = transformer.transform("value1", {"field_name": "email"})
        result2 = transformer.transform("value2", {"field_name": "email"})

        assert result1 != result2

    def test_hashing_none_value(self):
        """Test hashing None value."""
        transformer = HashingTransformer()
        result = transformer.transform(None, {"field_name": "email"})

        assert result is None

    def test_hashing_numeric_value(self):
        """Test hashing numeric value."""
        transformer = HashingTransformer()
        result = transformer.transform(12345, {"field_name": "id"})

        assert isinstance(result, str)
        assert len(result) == 64

    def test_hashing_exception_handling(self):
        """Test exception handling in hashing."""
        transformer = HashingTransformer(algorithm="invalid_algorithm")
        result = transformer.transform("test", {"field_name": "email"})

        # Should return original value on error
        assert result == "test"


class TestTypeConversionTransformer:
    """Test type conversion transformer functionality."""

    def test_string_to_int(self):
        """Test converting string to int."""
        transformer = TypeConversionTransformer(target_type=int)
        result = transformer.transform("123", {"field_name": "age"})

        assert result == 123
        assert isinstance(result, int)

    def test_int_to_string(self):
        """Test converting int to string."""
        transformer = TypeConversionTransformer(target_type=str)
        result = transformer.transform(123, {"field_name": "id"})

        assert result == "123"
        assert isinstance(result, str)

    def test_string_to_float(self):
        """Test converting string to float."""
        transformer = TypeConversionTransformer(target_type=float)
        result = transformer.transform("123.45", {"field_name": "price"})

        assert result == 123.45
        assert isinstance(result, float)

    def test_string_to_bool(self):
        """Test converting string to bool."""
        transformer = TypeConversionTransformer(target_type=bool)
        result = transformer.transform("True", {"field_name": "is_active"})

        assert result is True
        assert isinstance(result, bool)

    def test_conversion_none_value(self):
        """Test that None values pass through."""
        transformer = TypeConversionTransformer(target_type=int)
        result = transformer.transform(None, {"field_name": "age"})

        assert result is None

    def test_conversion_error_handling(self):
        """Test error handling for invalid conversions."""
        transformer = TypeConversionTransformer(target_type=int)
        result = transformer.transform("not_a_number", {"field_name": "age"})

        # Should return original value on error
        assert result == "not_a_number"

    def test_no_conversion_needed(self):
        """Test when value is already target type."""
        transformer = TypeConversionTransformer(target_type=int)
        result = transformer.transform(123, {"field_name": "age"})

        assert result == 123
        assert isinstance(result, int)


class TestConditionalTransformer:
    """Test conditional transformer functionality."""

    def test_predicate_true(self):
        """Test transformation when predicate is True."""

        def is_positive(value, context):
            return isinstance(value, (int, float)) and value > 0

        inner_transformer = TypeConversionTransformer(target_type=str)
        transformer = ConditionalTransformer(
            predicate=is_positive, transformer=inner_transformer
        )

        result = transformer.transform(123, {"field_name": "value"})
        assert result == "123"
        assert isinstance(result, str)

    def test_predicate_false(self):
        """Test no transformation when predicate is False."""

        def is_positive(value, context):
            return isinstance(value, (int, float)) and value > 0

        inner_transformer = TypeConversionTransformer(target_type=str)
        transformer = ConditionalTransformer(
            predicate=is_positive, transformer=inner_transformer
        )

        result = transformer.transform(-123, {"field_name": "value"})
        # Should return original value
        assert result == -123
        assert isinstance(result, int)

    def test_else_transformer(self):
        """Test else transformer when predicate is False."""

        def is_positive(value, context):
            return isinstance(value, (int, float)) and value > 0

        true_transformer = TypeConversionTransformer(target_type=str)
        false_transformer = HashingTransformer()
        transformer = ConditionalTransformer(
            predicate=is_positive,
            transformer=true_transformer,
            else_transformer=false_transformer,
        )

        # Positive value - use true_transformer
        result1 = transformer.transform(123, {"field_name": "value"})
        assert result1 == "123"

        # Negative value - use false_transformer
        result2 = transformer.transform(-123, {"field_name": "value"})
        assert isinstance(result2, str)
        assert len(result2) == 64  # SHA256 hash

    def test_context_based_predicate(self):
        """Test predicate that uses context."""

        def is_email_field(value, context):
            return "email" in context.get("field_name", "").lower()

        masker = PIIMaskingTransformer()
        transformer = ConditionalTransformer(predicate=is_email_field, transformer=masker)

        # Email field - should mask
        result1 = transformer.transform("user@example.com", {"field_name": "user_email"})
        assert result1 == "u***@example.com"

        # Non-email field - should pass through
        result2 = transformer.transform("user@example.com", {"field_name": "name"})
        assert result2 == "user@example.com"

    def test_exception_handling(self):
        """Test exception handling in conditional transformer."""

        def failing_predicate(value, context):
            raise ValueError("Test error")

        inner_transformer = TypeConversionTransformer(target_type=str)
        transformer = ConditionalTransformer(
            predicate=failing_predicate, transformer=inner_transformer
        )

        result = transformer.transform(123, {"field_name": "value"})
        # Should return original value on error
        assert result == 123


class TestTransformationPipeline:
    """Test transformation pipeline functionality."""

    def test_add_transformer(self):
        """Test adding transformer to pipeline."""
        pipeline = TransformationPipeline()
        transformer = PIIMaskingTransformer()

        pipeline.add_transformer(".*email.*", transformer)

        assert len(pipeline.get_patterns()) == 1
        assert ".*email.*" in pipeline.get_patterns()

    def test_multiple_transformers_same_pattern(self):
        """Test multiple transformers for same pattern."""
        pipeline = TransformationPipeline()
        transformer1 = PIIMaskingTransformer()
        transformer2 = HashingTransformer()

        pipeline.add_transformer(".*email.*", transformer1)
        pipeline.add_transformer(".*email.*", transformer2)

        assert pipeline.get_transformer_count() == 2

    def test_transform_row_single_field(self):
        """Test transforming single field in row."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()
        pipeline.add_transformer("email", masker)

        row = {"email": "user@example.com", "name": "John Doe"}
        result = pipeline.transform_row(row)

        assert result["email"] == "u***@example.com"
        assert result["name"] == "John Doe"

    def test_transform_row_multiple_fields(self):
        """Test transforming multiple fields in row."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()
        pipeline.add_transformer("email", masker)
        pipeline.add_transformer("phone", masker)

        row = {
            "email": "user@example.com",
            "phone": "(123) 456-7890",
            "name": "John Doe",
        }
        result = pipeline.transform_row(row)

        assert result["email"] == "u***@example.com"
        assert result["phone"] == "(***) ***-7890"
        assert result["name"] == "John Doe"

    def test_transform_row_pattern_matching(self):
        """Test pattern matching for field names."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()
        pipeline.add_transformer(".*email.*", masker)

        row = {
            "user_email": "user@example.com",
            "contact_email": "contact@example.com",
            "name": "John Doe",
        }
        result = pipeline.transform_row(row)

        assert result["user_email"] == "u***@example.com"
        assert result["contact_email"] == "c******@example.com"
        assert result["name"] == "John Doe"

    def test_transform_row_case_insensitive(self):
        """Test case-insensitive pattern matching."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()
        pipeline.add_transformer("EMAIL", masker, case_sensitive=False)

        row = {"email": "user@example.com", "EMAIL": "upper@example.com"}
        result = pipeline.transform_row(row)

        assert result["email"] == "u***@example.com"

    def test_transform_row_chained_transformers(self):
        """Test chaining multiple transformers."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()
        hasher = HashingTransformer()
        pipeline.add_transformer("email", masker)
        pipeline.add_transformer("email", hasher)

        row = {"email": "user@example.com"}
        result = pipeline.transform_row(row)

        # Should be hashed (second transformer)
        assert isinstance(result["email"], str)
        assert len(result["email"]) == 64

    def test_transform_rows(self):
        """Test transforming multiple rows."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()
        pipeline.add_transformer("email", masker)

        rows = [
            {"email": "user1@example.com", "name": "User 1"},
            {"email": "user2@example.com", "name": "User 2"},
        ]
        results = pipeline.transform_rows(rows)

        assert len(results) == 2
        assert results[0]["email"] == "u****@example.com"
        assert results[1]["email"] == "u****@example.com"

    def test_get_transformer_count(self):
        """Test getting total transformer count."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()
        hasher = HashingTransformer()

        pipeline.add_transformer("email", masker)
        pipeline.add_transformer("phone", masker)
        pipeline.add_transformer("ssn", hasher)

        assert pipeline.get_transformer_count() == 3

    def test_get_patterns(self):
        """Test getting list of patterns."""
        pipeline = TransformationPipeline()
        masker = PIIMaskingTransformer()

        pipeline.add_transformer("email", masker)
        pipeline.add_transformer("phone", masker)

        patterns = pipeline.get_patterns()
        assert len(patterns) == 2
        assert "email" in patterns
        assert "phone" in patterns

    def test_empty_pipeline(self):
        """Test pipeline with no transformers."""
        pipeline = TransformationPipeline()

        row = {"email": "user@example.com"}
        result = pipeline.transform_row(row)

        # Should return unchanged
        assert result == row


class TestCreatePIIPipeline:
    """Test pre-configured PII pipeline."""

    def test_pii_pipeline_email(self):
        """Test PII pipeline masks email."""
        pipeline = create_pii_pipeline()

        row = {"user_email": "user@example.com"}
        result = pipeline.transform_row(row)

        assert result["user_email"] == "u***@example.com"

    def test_pii_pipeline_phone(self):
        """Test PII pipeline masks phone."""
        pipeline = create_pii_pipeline()

        row = {"phone_number": "(123) 456-7890"}
        result = pipeline.transform_row(row)

        assert result["phone_number"] == "(***) ***-7890"

    def test_pii_pipeline_ssn(self):
        """Test PII pipeline masks SSN."""
        pipeline = create_pii_pipeline()

        row = {"ssn": "123-45-6789"}
        result = pipeline.transform_row(row)

        assert result["ssn"] == "***-**-6789"

    def test_pii_pipeline_credit_card(self):
        """Test PII pipeline masks credit card."""
        pipeline = create_pii_pipeline()

        row = {"credit_card_number": "4532-1234-5678-9010"}
        result = pipeline.transform_row(row)

        assert result["credit_card_number"] == "****-****-****-9010"

    def test_pii_pipeline_ip_address(self):
        """Test PII pipeline masks IP address."""
        pipeline = create_pii_pipeline()

        row = {"ip_address": "192.168.1.100"}
        result = pipeline.transform_row(row)

        assert result["ip_address"] == "192.***.*.***"

    def test_pii_pipeline_customer_id_hashing(self):
        """Test PII pipeline hashes customer ID."""
        pipeline = create_pii_pipeline(salt="test_salt")

        row = {"customer_id": "CUST12345"}
        result = pipeline.transform_row(row)

        # Should be hashed (16 characters truncated)
        assert isinstance(result["customer_id"], str)
        assert len(result["customer_id"]) == 16
        assert result["customer_id"] != "CUST12345"

    def test_pii_pipeline_user_id_hashing(self):
        """Test PII pipeline hashes user ID."""
        pipeline = create_pii_pipeline(salt="test_salt")

        row = {"user_id": "USER12345"}
        result = pipeline.transform_row(row)

        # Should be hashed
        assert isinstance(result["user_id"], str)
        assert len(result["user_id"]) == 16

    def test_pii_pipeline_mixed_fields(self):
        """Test PII pipeline with mixed sensitive and non-sensitive fields."""
        pipeline = create_pii_pipeline()

        row = {
            "email": "user@example.com",
            "phone": "(123) 456-7890",
            "name": "John Doe",
            "address": "123 Main St",
        }
        result = pipeline.transform_row(row)

        # Sensitive fields masked
        assert result["email"] == "u***@example.com"
        assert result["phone"] == "(***) ***-7890"
        # Non-sensitive fields unchanged
        assert result["name"] == "John Doe"
        assert result["address"] == "123 Main St"


class TestCreateGDPRPipeline:
    """Test pre-configured GDPR pipeline."""

    def test_gdpr_pipeline_email_hashing(self):
        """Test GDPR pipeline hashes email."""
        pipeline = create_gdpr_pipeline(salt="gdpr_salt")

        row = {"email": "user@example.com"}
        result = pipeline.transform_row(row)

        # Should be hashed (full SHA256)
        assert isinstance(result["email"], str)
        assert len(result["email"]) == 64
        assert result["email"] != "user@example.com"

    def test_gdpr_pipeline_name_hashing(self):
        """Test GDPR pipeline hashes name."""
        pipeline = create_gdpr_pipeline(salt="gdpr_salt")

        row = {"full_name": "John Doe"}
        result = pipeline.transform_row(row)

        # Should be hashed
        assert isinstance(result["full_name"], str)
        assert len(result["full_name"]) == 64

    def test_gdpr_pipeline_address_hashing(self):
        """Test GDPR pipeline hashes address."""
        pipeline = create_gdpr_pipeline(salt="gdpr_salt")

        row = {"home_address": "123 Main St"}
        result = pipeline.transform_row(row)

        # Should be hashed
        assert isinstance(result["home_address"], str)
        assert len(result["home_address"]) == 64

    def test_gdpr_pipeline_phone_hashing(self):
        """Test GDPR pipeline hashes phone."""
        pipeline = create_gdpr_pipeline(salt="gdpr_salt")

        row = {"phone_number": "(123) 456-7890"}
        result = pipeline.transform_row(row)

        # Should be hashed
        assert isinstance(result["phone_number"], str)
        assert len(result["phone_number"]) == 64

    def test_gdpr_pipeline_ip_hashing(self):
        """Test GDPR pipeline hashes IP address."""
        pipeline = create_gdpr_pipeline(salt="gdpr_salt")

        row = {"ip_address": "192.168.1.100"}
        result = pipeline.transform_row(row)

        # Should be hashed
        assert isinstance(result["ip_address"], str)
        assert len(result["ip_address"]) == 64

    def test_gdpr_pipeline_deterministic(self):
        """Test GDPR pipeline produces deterministic hashes."""
        pipeline = create_gdpr_pipeline(salt="test_salt")

        row = {"email": "user@example.com"}
        result1 = pipeline.transform_row(row)
        result2 = pipeline.transform_row(row)

        assert result1["email"] == result2["email"]

    def test_gdpr_pipeline_different_salts(self):
        """Test GDPR pipeline with different salts produces different hashes."""
        pipeline1 = create_gdpr_pipeline(salt="salt1")
        pipeline2 = create_gdpr_pipeline(salt="salt2")

        row = {"email": "user@example.com"}
        result1 = pipeline1.transform_row(row)
        result2 = pipeline2.transform_row(row)

        assert result1["email"] != result2["email"]

    def test_gdpr_pipeline_non_pii_fields(self):
        """Test GDPR pipeline leaves non-PII fields unchanged."""
        pipeline = create_gdpr_pipeline()

        row = {
            "email": "user@example.com",
            "order_id": "ORD12345",
            "total_amount": 99.99,
        }
        result = pipeline.transform_row(row)

        # PII hashed
        assert len(result["email"]) == 64
        # Non-PII unchanged
        assert result["order_id"] == "ORD12345"
        assert result["total_amount"] == 99.99


class TestTransformerMetrics:
    """Test that transformers track metrics."""

    def test_pii_masking_metrics(self):
        """Test PII masking transformer increments metrics."""
        from prometheus_client import REGISTRY

        transformer = PIIMaskingTransformer()

        # Get initial metric value
        before = REGISTRY.get_sample_value(
            "transformations_applied_total",
            {"transformer_type": "PIIMaskingTransformer", "field_pattern": "email"},
        ) or 0

        # Apply transformation
        transformer.transform("user@example.com", {"field_name": "email"})

        # Check metric incremented
        after = REGISTRY.get_sample_value(
            "transformations_applied_total",
            {"transformer_type": "PIIMaskingTransformer", "field_pattern": "email"},
        ) or 0

        assert after > before

    def test_hashing_metrics(self):
        """Test hashing transformer increments metrics."""
        from prometheus_client import REGISTRY

        transformer = HashingTransformer()

        # Get initial metric value
        before = REGISTRY.get_sample_value(
            "transformations_applied_total",
            {"transformer_type": "HashingTransformer", "field_pattern": "email"},
        ) or 0

        # Apply transformation
        transformer.transform("user@example.com", {"field_name": "email"})

        # Check metric incremented
        after = REGISTRY.get_sample_value(
            "transformations_applied_total",
            {"transformer_type": "HashingTransformer", "field_pattern": "email"},
        ) or 0

        assert after > before

    def test_error_metrics(self):
        """Test error metrics are tracked."""
        from prometheus_client import REGISTRY

        transformer = HashingTransformer(algorithm="invalid")

        # Get initial metric value
        before = REGISTRY.get_sample_value(
            "transformation_errors_total",
            {"transformer_type": "HashingTransformer", "error_type": "ValueError"},
        ) or 0

        # Trigger error
        transformer.transform("test", {"field_name": "email"})

        # Check metric incremented
        after = REGISTRY.get_sample_value(
            "transformation_errors_total",
            {"transformer_type": "HashingTransformer", "error_type": "ValueError"},
        ) or 0

        assert after > before
