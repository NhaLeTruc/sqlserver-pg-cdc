"""
PII masking and hashing transformers.

Provides transformers for masking personally identifiable information
(email, phone, SSN, credit cards) and one-way hashing for pseudonymization.
"""

import hashlib
import json
import logging
import re
import secrets
from typing import Any

from .base import (
    TRANSFORMATION_ERRORS,
    TRANSFORMATION_TIME,
    TRANSFORMATIONS_APPLIED,
    Transformer,
)

logger = logging.getLogger(__name__)


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

    def transform(self, value: Any, context: dict[str, Any]) -> Any:
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
                # SEC-7: Don't log field names to avoid leaking PII field information
                logger.warning(f"PII masking failed: {type(e).__name__}")
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

        # BUG-4: Validate email format - reject malformed emails like user@@example.com
        if not local or not domain or domain.startswith("@") or "@" in domain:
            logger.debug("Invalid email format detected")
            return self.mask_char * len(email)

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
            # BUG-3: Log invalid credit card length for data quality monitoring
            logger.debug(f"Invalid credit card length: {len(digits)} digits")
            return self.mask_char * len(card)

        # BUG-3: Log Luhn validation failures for data quality monitoring
        # (but still mask the card to protect PII)
        if not self._luhn_check(digits):
            logger.debug("Credit card failed Luhn checksum validation")

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
            10.0.0.1 -> 10.*.*.*
        """
        # IPv4 pattern
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", ip):
            parts = ip.split(".")
            return f"{parts[0]}.{self.mask_char * 3}.{self.mask_char}.{self.mask_char * 3}"

        # IPv6 or other - mask most of it
        return ip[:4] + self.mask_char * (len(ip) - 4)

    def _luhn_check(self, card_number: str) -> bool:
        """
        Validate credit card number using Luhn algorithm.

        Args:
            card_number: String of digits only

        Returns:
            True if valid, False otherwise
        """
        digits = [int(d) for d in card_number]
        # Double every second digit from right, subtract 9 if > 9
        for i in range(len(digits) - 2, -1, -2):
            digits[i] *= 2
            if digits[i] > 9:
                digits[i] -= 9
        return sum(digits) % 10 == 0


class HashingTransformer(Transformer):
    """
    One-way hash transformation for PII.

    Useful for pseudonymization where you need consistent but irreversible
    transformation of identifiers.
    """

    # SEC-2: Only allow cryptographically secure hash algorithms
    ALLOWED_ALGORITHMS = frozenset({"sha256", "sha384", "sha512", "blake2b", "blake2s"})
    MIN_SALT_LENGTH = 8

    def __init__(
        self,
        algorithm: str = "sha256",
        salt: str | None = None,
        truncate: int | None = None,
    ):
        """
        Initialize hashing transformer.

        Args:
            algorithm: Hash algorithm (sha256, sha384, sha512, blake2b, blake2s)
            salt: Salt to add to values before hashing. If None, a random salt is generated.
            truncate: Optional truncation length for hash output

        Raises:
            ValueError: If algorithm is insecure or salt is too short
        """
        # SEC-2: Reject weak/insecure hash algorithms
        if algorithm.lower() not in self.ALLOWED_ALGORITHMS:
            raise ValueError(
                f"Insecure hash algorithm: {algorithm}. "
                f"Allowed algorithms: {', '.join(sorted(self.ALLOWED_ALGORITHMS))}"
            )

        # SEC-3: Require strong salts
        if salt is None:
            salt = secrets.token_hex(16)
            logger.warning(
                "No salt provided to HashingTransformer. Generated random salt. "
                "For consistent hashing across runs, provide an explicit salt."
            )
        elif len(salt) < self.MIN_SALT_LENGTH:
            raise ValueError(
                f"Salt must be at least {self.MIN_SALT_LENGTH} characters long"
            )

        self.algorithm = algorithm.lower()
        self.salt = salt
        self.truncate = truncate

    def transform(self, value: Any, context: dict[str, Any]) -> Any:
        """Transform value by hashing."""
        with TRANSFORMATION_TIME.labels(transformer_type=self.get_type()).time():
            if value is None:
                return None

            try:
                # BUG-5: Handle complex types properly for consistent hashing
                if isinstance(value, float):
                    str_value = repr(value)  # Preserve precision
                elif isinstance(value, (dict, list)):
                    str_value = json.dumps(value, sort_keys=True)
                else:
                    str_value = str(value)

                data = f"{self.salt}{str_value}".encode()
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
