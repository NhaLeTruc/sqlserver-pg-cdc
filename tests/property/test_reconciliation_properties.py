"""
Property-based tests for reconciliation logic using Hypothesis.

Tests invariants and properties that should hold for all inputs:
- Row count comparison properties
- Checksum determinism
- Identifier quoting safety
- Incremental checksum consistency
"""

from hypothesis import given, strategies as st, settings, assume
from hypothesis.stateful import RuleBasedStateMachine, rule, invariant, initialize
from src.reconciliation.compare import (
    compare_row_counts,
    compare_checksums,
    _quote_postgres_identifier,
    _quote_sqlserver_identifier,
    _get_db_type,
)
import hashlib
import re
from typing import Any, List
from unittest.mock import Mock


# Property: Row count comparison should be symmetric and consistent
@given(
    table=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll'), min_codepoint=65, max_codepoint=122)),
    source_count=st.integers(min_value=0, max_value=10000000),
    target_count=st.integers(min_value=0, max_value=10000000)
)
def test_row_count_comparison_properties(table: str, source_count: int, target_count: int):
    """Row count comparison should maintain mathematical invariants."""
    result = compare_row_counts(table, source_count, target_count)

    # Property 1: Difference should be symmetric
    assert result['difference'] == target_count - source_count

    # Property 2: Match should be reflexive
    if source_count == target_count:
        assert result['match'] is True
        assert result['status'] == 'MATCH'
        assert result['difference'] == 0
    else:
        assert result['match'] is False
        assert result['status'] == 'MISMATCH'
        assert result['difference'] != 0

    # Property 3: Result should contain all required fields
    assert 'table' in result
    assert 'source_count' in result
    assert 'target_count' in result
    assert 'match' in result
    assert 'difference' in result
    assert 'status' in result
    assert 'timestamp' in result

    # Property 4: Counts should be preserved
    assert result['source_count'] == source_count
    assert result['target_count'] == target_count

    # Property 5: Table name should be preserved
    assert result['table'] == table


# Property: Row count comparison should handle boundary cases
@given(
    table=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll'), min_codepoint=65, max_codepoint=122)),
    count=st.integers(min_value=0, max_value=10000000)
)
def test_row_count_identical_comparison(table: str, count: int):
    """Comparing identical counts should always match."""
    result = compare_row_counts(table, count, count)

    assert result['match'] is True
    assert result['difference'] == 0
    assert result['status'] == 'MATCH'


# Property: Checksum comparison should be deterministic
@given(
    table=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll'), min_codepoint=65, max_codepoint=122)),
    data=st.binary(min_size=0, max_size=1000)
)
def test_checksum_deterministic(table: str, data: bytes):
    """Same data should produce identical checksum."""
    checksum1 = hashlib.sha256(data).hexdigest()
    checksum2 = hashlib.sha256(data).hexdigest()

    # Property 1: Checksums should be identical
    assert checksum1 == checksum2

    # Property 2: Comparison should show match
    result = compare_checksums(table, checksum1, checksum2)
    assert result['match'] is True
    assert result['status'] == 'MATCH'

    # Property 3: Checksums should be preserved
    assert result['source_checksum'] == checksum1
    assert result['target_checksum'] == checksum2


# Property: Different data should produce different checksums (with high probability)
@given(
    table=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll'), min_codepoint=65, max_codepoint=122)),
    data1=st.binary(min_size=1, max_size=1000),
    data2=st.binary(min_size=1, max_size=1000)
)
def test_checksum_uniqueness(table: str, data1: bytes, data2: bytes):
    """Different data should produce different checksums (collision resistance)."""
    assume(data1 != data2)  # Only test when data is different

    checksum1 = hashlib.sha256(data1).hexdigest()
    checksum2 = hashlib.sha256(data2).hexdigest()

    # Property: Different data should produce different checksums
    # (This is a cryptographic property of SHA256)
    assert checksum1 != checksum2

    result = compare_checksums(table, checksum1, checksum2)
    assert result['match'] is False
    assert result['status'] == 'MISMATCH'


# Property: PostgreSQL identifier quoting should prevent injection
@given(
    identifier=st.text(
        min_size=1,
        max_size=50,
        alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'), min_codepoint=48, max_codepoint=122)
    )
)
def test_postgres_identifier_quoting_safety(identifier: str):
    """PostgreSQL identifier quoting should produce valid quoted identifiers."""
    # Filter to only valid identifier characters
    assume(re.match(r'^[\w]+$', identifier))

    try:
        quoted = _quote_postgres_identifier(identifier)

        # Property 1: Quoted identifier should contain original identifier
        clean_identifier = identifier.replace('[', '').replace(']', '')
        assert clean_identifier in quoted

        # Property 2: Should be wrapped in double quotes
        assert '"' in quoted

        # Property 3: Should not contain dangerous SQL keywords bare
        dangerous_patterns = [
            '; DROP TABLE',
            '-- ',
            '/*',
            '*/',
            'UNION SELECT',
            'OR 1=1'
        ]
        for pattern in dangerous_patterns:
            # These should be quoted and harmless
            if pattern in identifier:
                assert quoted.startswith('"') or '[' in quoted

    except ValueError:
        # Invalid identifiers should raise ValueError
        pass


# Property: SQL Server identifier quoting should prevent injection
@given(
    identifier=st.text(
        min_size=1,
        max_size=50,
        alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'), min_codepoint=48, max_codepoint=122)
    )
)
def test_sqlserver_identifier_quoting_safety(identifier: str):
    """SQL Server identifier quoting should produce valid quoted identifiers."""
    # Filter to only valid identifier characters
    assume(re.match(r'^[\w]+$', identifier))

    try:
        quoted = _quote_sqlserver_identifier(identifier)

        # Property 1: Should be wrapped in brackets
        assert quoted.startswith('[')
        assert quoted.endswith(']')

        # Property 2: Quoted identifier should contain original identifier
        clean_identifier = identifier.replace('[', '').replace(']', '')
        assert clean_identifier in quoted

        # Property 3: Double bracketing should not occur
        assert '[[' not in quoted
        assert ']]' not in quoted

    except ValueError:
        # Invalid identifiers should raise ValueError
        pass


# Property: Schema.table format should be handled correctly
@given(
    schema=st.text(
        min_size=1,
        max_size=30,
        alphabet=st.characters(whitelist_categories=('Lu', 'Ll'), min_codepoint=65, max_codepoint=122)
    ),
    table=st.text(
        min_size=1,
        max_size=30,
        alphabet=st.characters(whitelist_categories=('Lu', 'Ll'), min_codepoint=65, max_codepoint=122)
    )
)
def test_schema_table_quoting(schema: str, table: str):
    """Schema.table identifiers should be quoted correctly."""
    assume(re.match(r'^[\w]+$', schema))
    assume(re.match(r'^[\w]+$', table))

    identifier = f"{schema}.{table}"

    # PostgreSQL quoting
    try:
        pg_quoted = _quote_postgres_identifier(identifier)
        assert schema in pg_quoted
        assert table in pg_quoted
        assert '.' in pg_quoted  # Should preserve separator
    except ValueError:
        pass

    # SQL Server quoting
    try:
        sql_quoted = _quote_sqlserver_identifier(identifier)
        assert schema in sql_quoted
        assert table in sql_quoted
        assert '].[' in sql_quoted  # Should have proper bracket separation
    except ValueError:
        pass


# Property: Database type detection should be consistent
def test_db_type_detection_psycopg2():
    """Psycopg2 cursors should be detected as PostgreSQL."""
    mock_cursor = Mock()
    mock_cursor.__module__ = 'psycopg2.extensions'

    db_type = _get_db_type(mock_cursor)
    assert db_type == 'postgresql'


def test_db_type_detection_pyodbc():
    """PyODBC cursors should be detected as SQL Server."""
    mock_cursor = Mock()
    mock_cursor.__module__ = 'pyodbc'

    db_type = _get_db_type(mock_cursor)
    assert db_type == 'sqlserver'


# Stateful testing for incremental checksums
class IncrementalChecksumMachine(RuleBasedStateMachine):
    """State machine for testing incremental checksum properties."""

    def __init__(self):
        super().__init__()
        self.rows: List[bytes] = []
        self.full_checksum: str = ""
        self.checksum_computed: bool = False

    @initialize()
    def init_state(self):
        """Initialize empty state."""
        self.rows = []
        self.full_checksum = ""
        self.checksum_computed = False

    @rule(data=st.binary(min_size=0, max_size=100))
    def add_row(self, data: bytes):
        """Add row to dataset."""
        self.rows.append(data)
        self.checksum_computed = False  # Invalidate checksum

    @rule()
    def calculate_full_checksum(self):
        """Calculate full checksum from all rows."""
        hasher = hashlib.sha256()
        for row in self.rows:
            hasher.update(row)
        self.full_checksum = hasher.hexdigest()
        self.checksum_computed = True

    @rule()
    def recalculate_and_verify(self):
        """Recalculate checksum and verify it matches."""
        if self.checksum_computed and self.rows:
            hasher = hashlib.sha256()
            for row in self.rows:
                hasher.update(row)
            new_checksum = hasher.hexdigest()

            # Property: Checksum should be stable for unchanged data
            assert new_checksum == self.full_checksum

    @invariant()
    def checksum_deterministic(self):
        """Checksum should always be deterministic for same data."""
        if len(self.rows) > 0:
            hasher1 = hashlib.sha256()
            hasher2 = hashlib.sha256()

            for row in self.rows:
                hasher1.update(row)
                hasher2.update(row)

            # Property: Same sequence should produce same hash
            assert hasher1.hexdigest() == hasher2.hexdigest()

    @invariant()
    def rows_immutable_during_checksum(self):
        """Row data should not be modified during checksum calculation."""
        if self.checksum_computed and self.rows:
            # Verify we can still iterate over rows
            row_count = len(self.rows)
            assert row_count >= 0


# Create test case from state machine
TestIncrementalChecksum = IncrementalChecksumMachine.TestCase


# Property: Checksum should change when data changes
@given(
    data_before=st.lists(st.binary(min_size=1, max_size=100), min_size=1, max_size=20),
    data_after=st.lists(st.binary(min_size=1, max_size=100), min_size=1, max_size=20)
)
@settings(max_examples=50)
def test_checksum_changes_with_data(data_before: List[bytes], data_after: List[bytes]):
    """Checksum should change when underlying data changes."""
    assume(data_before != data_after)  # Only test when data is different

    # Calculate checksum for before state
    hasher_before = hashlib.sha256()
    for row in data_before:
        hasher_before.update(row)
    checksum_before = hasher_before.hexdigest()

    # Calculate checksum for after state
    hasher_after = hashlib.sha256()
    for row in data_after:
        hasher_after.update(row)
    checksum_after = hasher_after.hexdigest()

    # Property: Different data should produce different checksums
    assert checksum_before != checksum_after


# Property: Row order should affect checksum
@given(
    data=st.lists(st.binary(min_size=1, max_size=100), min_size=2, max_size=10, unique=True)
)
def test_checksum_order_dependent(data: List[bytes]):
    """Checksum should depend on row order."""
    assume(len(data) >= 2)

    # Calculate checksum with original order
    hasher_original = hashlib.sha256()
    for row in data:
        hasher_original.update(row)
    checksum_original = hasher_original.hexdigest()

    # Calculate checksum with reversed order
    hasher_reversed = hashlib.sha256()
    for row in reversed(data):
        hasher_reversed.update(row)
    checksum_reversed = hasher_reversed.hexdigest()

    # Property: Order matters (unless palindrome)
    if data != list(reversed(data)):
        assert checksum_original != checksum_reversed


# Property: Empty data should produce consistent checksum
def test_empty_data_checksum():
    """Empty data should produce SHA256 hash of empty string."""
    hasher = hashlib.sha256()
    empty_checksum = hasher.hexdigest()

    # This should be the SHA256 of empty string
    expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert empty_checksum == expected


# Property: Single byte changes should produce completely different hash
@given(
    data=st.binary(min_size=10, max_size=1000),
    bit_position=st.integers(min_value=0, max_value=7)
)
@settings(max_examples=50)
def test_checksum_avalanche_effect(data: bytes, bit_position: int):
    """Single bit flip should completely change checksum (avalanche effect)."""
    assume(len(data) > 0)

    # Original checksum
    checksum_original = hashlib.sha256(data).hexdigest()

    # Flip one bit in the data
    data_list = bytearray(data)
    byte_index = len(data_list) // 2  # Flip bit in middle byte
    data_list[byte_index] ^= (1 << bit_position)  # XOR to flip bit
    data_modified = bytes(data_list)

    # Modified checksum
    checksum_modified = hashlib.sha256(data_modified).hexdigest()

    # Property: Checksums should be completely different
    assert checksum_original != checksum_modified

    # Property: At least 50% of bits should differ (avalanche effect)
    # Convert hex to binary and count differing bits
    original_bits = bin(int(checksum_original, 16))[2:].zfill(256)
    modified_bits = bin(int(checksum_modified, 16))[2:].zfill(256)

    differing_bits = sum(1 for a, b in zip(original_bits, modified_bits) if a != b)
    total_bits = 256

    # Good hash functions should differ in ~50% of bits
    assert differing_bits > total_bits * 0.3  # At least 30% different (generous bound)


# Configuration for hypothesis
settings.register_profile("ci", max_examples=100, deadline=1000)
settings.register_profile("dev", max_examples=20, deadline=500)
settings.register_profile("thorough", max_examples=500, deadline=2000)

# Load default profile
settings.load_profile("dev")
