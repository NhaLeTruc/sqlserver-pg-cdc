"""
Property-based tests for data integrity and validation.

Tests properties related to:
- NULL handling in checksums
- Type conversions
- Boundary conditions
- Data corruption detection
"""

from hypothesis import given, strategies as st, assume, settings
from typing import Any, List, Optional, Tuple
import hashlib


# Strategy for database-like values (common SQL types)
db_value = st.one_of(
    st.none(),
    st.integers(min_value=-(2**31), max_value=2**31 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=100),
    st.binary(max_size=100),
    st.booleans(),
    st.dates(),
    st.datetimes(),
    st.decimals(allow_nan=False, allow_infinity=False, places=2)
)


def row_to_string(row: Tuple[Any, ...]) -> str:
    """Convert row to string representation (simulating checksum calculation)."""
    return "|".join(str(val) if val is not None else "NULL" for val in row)


def calculate_row_checksum(rows: List[Tuple[Any, ...]]) -> str:
    """Calculate checksum for list of rows."""
    hasher = hashlib.sha256()
    for row in rows:
        row_str = row_to_string(row)
        hasher.update(row_str.encode('utf-8'))
    return hasher.hexdigest()


# Property: NULL values should be handled consistently
@given(
    non_null_value=st.integers(min_value=0, max_value=1000),
    position=st.integers(min_value=0, max_value=4)
)
def test_null_handling_consistency(non_null_value: int, position: int):
    """NULL values should be represented consistently in checksums."""
    # Create row with NULL at specific position
    row1 = [None if i == position else non_null_value for i in range(5)]
    row2 = [None if i == position else non_null_value for i in range(5)]

    # Property: Same NULL pattern should produce same string
    assert row_to_string(tuple(row1)) == row_to_string(tuple(row2))

    # Property: NULL should be represented as "NULL" string
    assert "NULL" in row_to_string(tuple(row1))


# Property: Row order should be preserved in checksum
@given(
    rows=st.lists(
        st.tuples(db_value, db_value, db_value),
        min_size=2,
        max_size=20
    )
)
@settings(max_examples=50)
def test_row_order_preservation(rows: List[Tuple[Any, ...]]):
    """Checksums should preserve row order."""
    checksum1 = calculate_row_checksum(rows)
    checksum2 = calculate_row_checksum(list(reversed(rows)))

    # Property: Different order should produce different checksum
    # (unless rows are palindromic)
    if rows != list(reversed(rows)):
        assert checksum1 != checksum2


# Property: Duplicate rows should affect checksum
@given(
    base_row=st.tuples(db_value, db_value, db_value),
    count=st.integers(min_value=1, max_value=10)
)
def test_duplicate_row_detection(base_row: Tuple[Any, ...], count: int):
    """Duplicate rows should produce different checksum than single row."""
    single_row = [base_row]
    multiple_rows = [base_row] * count

    checksum_single = calculate_row_checksum(single_row)
    checksum_multiple = calculate_row_checksum(multiple_rows)

    # Property: Multiple copies should differ from single copy
    if count > 1:
        assert checksum_single != checksum_multiple


# Property: Empty rows should be handled
def test_empty_row_list():
    """Empty row list should produce consistent checksum."""
    checksum1 = calculate_row_checksum([])
    checksum2 = calculate_row_checksum([])

    # Property: Empty lists should produce same checksum
    assert checksum1 == checksum2

    # Property: Should be SHA256 of empty data
    expected = hashlib.sha256().hexdigest()
    assert checksum1 == expected


# Property: Row with all NULLs should be valid
@given(column_count=st.integers(min_value=1, max_value=20))
def test_all_null_row(column_count: int):
    """Row with all NULL values should be handled correctly."""
    null_row = tuple([None] * column_count)
    row_str = row_to_string(null_row)

    # Property: Should contain only NULL separators
    expected_nulls = column_count
    actual_nulls = row_str.count("NULL")
    assert actual_nulls == expected_nulls

    # Property: String should have separators
    if column_count > 1:
        assert "|" in row_str


# Property: Type conversions should be deterministic
@given(
    int_value=st.integers(min_value=-1000, max_value=1000),
    float_value=st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    str_value=st.text(min_size=0, max_size=50),
    bool_value=st.booleans()
)
def test_type_conversion_determinism(
    int_value: int,
    float_value: float,
    str_value: str,
    bool_value: bool
):
    """Type conversions to string should be deterministic."""
    row = (int_value, float_value, str_value, bool_value)

    # Convert multiple times
    str1 = row_to_string(row)
    str2 = row_to_string(row)

    # Property: Conversion should be deterministic
    assert str1 == str2


# Property: Special characters should be escaped/handled safely
@given(
    special_string=st.text(
        alphabet=st.characters(
            blacklist_categories=('Cs',),  # Exclude surrogates
            blacklist_characters=('\x00',)  # Exclude null bytes
        ),
        min_size=0,
        max_size=100
    )
)
def test_special_character_handling(special_string: str):
    """Special characters should not break checksum calculation."""
    row = (special_string,)

    try:
        row_str = row_to_string(row)
        checksum = hashlib.sha256(row_str.encode('utf-8')).hexdigest()

        # Property: Should produce valid hex checksum
        assert len(checksum) == 64
        assert all(c in '0123456789abcdef' for c in checksum)

    except UnicodeEncodeError:
        # Some characters may not be encodable to UTF-8
        # This is expected behavior
        pass


# Property: Row count should match input
@given(
    rows=st.lists(
        st.tuples(db_value, db_value),
        min_size=0,
        max_size=100
    )
)
def test_row_count_preservation(rows: List[Tuple[Any, ...]]):
    """Number of rows should be preserved in processing."""
    # Simulate processing
    processed_count = 0
    hasher = hashlib.sha256()

    for row in rows:
        row_str = row_to_string(row)
        hasher.update(row_str.encode('utf-8'))
        processed_count += 1

    # Property: Count should match input
    assert processed_count == len(rows)


# Property: Single column changes should change checksum
@given(
    base_value=st.integers(min_value=0, max_value=1000),
    modified_value=st.integers(min_value=0, max_value=1000),
    column_index=st.integers(min_value=0, max_value=4)
)
def test_single_column_modification_detection(
    base_value: int,
    modified_value: int,
    column_index: int
):
    """Changing a single column should change the checksum."""
    assume(base_value != modified_value)

    # Create base row
    base_row = [(base_value,) * 5]

    # Create modified row
    modified_row_tuple = list((base_value,) * 5)
    modified_row_tuple[column_index] = modified_value
    modified_row = [tuple(modified_row_tuple)]

    checksum_base = calculate_row_checksum(base_row)
    checksum_modified = calculate_row_checksum(modified_row)

    # Property: Modification should change checksum
    assert checksum_base != checksum_modified


# Property: Adding a row should change checksum
@given(
    existing_rows=st.lists(
        st.tuples(db_value, db_value),
        min_size=1,
        max_size=20
    ),
    new_row=st.tuples(db_value, db_value)
)
def test_row_addition_detection(
    existing_rows: List[Tuple[Any, ...]],
    new_row: Tuple[Any, ...]
):
    """Adding a row should change the checksum."""
    checksum_before = calculate_row_checksum(existing_rows)

    existing_rows_copy = existing_rows.copy()
    existing_rows_copy.append(new_row)
    checksum_after = calculate_row_checksum(existing_rows_copy)

    # Property: Adding row should change checksum
    assert checksum_before != checksum_after


# Property: Removing a row should change checksum
@given(
    rows=st.lists(
        st.tuples(db_value, db_value),
        min_size=2,
        max_size=20
    ),
    index_to_remove=st.integers(min_value=0)
)
def test_row_removal_detection(
    rows: List[Tuple[Any, ...]],
    index_to_remove: int
):
    """Removing a row should change the checksum."""
    assume(len(rows) >= 2)
    assume(index_to_remove < len(rows))

    checksum_before = calculate_row_checksum(rows)

    rows_copy = rows.copy()
    rows_copy.pop(index_to_remove)
    checksum_after = calculate_row_checksum(rows_copy)

    # Property: Removing row should change checksum
    assert checksum_before != checksum_after


# Property: Numeric precision should be handled
@given(
    float_val=st.floats(
        min_value=-1000000.0,
        max_value=1000000.0,
        allow_nan=False,
        allow_infinity=False
    )
)
def test_numeric_precision_handling(float_val: float):
    """Numeric values should be converted consistently."""
    row1 = (float_val,)
    row2 = (float_val,)

    str1 = row_to_string(row1)
    str2 = row_to_string(row2)

    # Property: Same value should produce same string
    assert str1 == str2


# Property: Boolean values should be consistent
@given(bool_val=st.booleans())
def test_boolean_representation(bool_val: bool):
    """Boolean values should have consistent string representation."""
    row = (bool_val,)
    row_str = row_to_string(row)

    # Property: Should contain "True" or "False"
    assert "True" in row_str or "False" in row_str


# Property: Whitespace should be preserved
@given(
    text_with_whitespace=st.text(
        alphabet=st.characters(whitelist_categories=('Zs', 'Lu', 'Ll')),
        min_size=1,
        max_size=50
    )
)
def test_whitespace_preservation(text_with_whitespace: str):
    """Whitespace in text should be preserved in checksum."""
    row = (text_with_whitespace,)
    row_str = row_to_string(row)

    # Property: Original text should be in row string
    assert text_with_whitespace in row_str


# Property: Column count should be consistent
@given(
    column_count=st.integers(min_value=1, max_value=50),
    value=db_value
)
def test_column_count_consistency(column_count: int, value: Any):
    """Number of separators should match column count - 1."""
    # Skip test if value contains the separator character
    # (current implementation doesn't escape separators)
    assume("|" not in str(value))

    row = tuple([value] * column_count)
    row_str = row_to_string(row)

    # Property: Should have column_count - 1 separators
    separator_count = row_str.count("|")
    assert separator_count == column_count - 1


# Property: Checksums should have fixed length
@given(
    rows=st.lists(
        st.tuples(db_value, db_value, db_value),
        min_size=0,
        max_size=100
    )
)
def test_checksum_length_fixed(rows: List[Tuple[Any, ...]]):
    """SHA256 checksums should always be 64 hex characters."""
    checksum = calculate_row_checksum(rows)

    # Property: SHA256 hex digest is always 64 characters
    assert len(checksum) == 64

    # Property: Should only contain hex digits
    assert all(c in '0123456789abcdef' for c in checksum)


# Configure settings
settings.register_profile("data_integrity", max_examples=100, deadline=1000)
settings.load_profile("data_integrity")
