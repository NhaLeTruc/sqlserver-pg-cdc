"""
Performance test configuration.

Automatically triggers test environment reset before performance tests.
"""

import pytest


@pytest.fixture(scope="module", autouse=True)
def performance_test_setup(clean_test_environment):
    """
    Automatically reset test environment before performance tests.

    This ensures performance measurements are not affected by stale data
    from previous test runs.

    The clean_test_environment fixture:
    - Truncates all tables
    - Clears Kafka topics
    - Resets connector offsets (unless QUICK_RESET=1)

    Environment variables:
    - SKIP_RESET=1: Skip reset (not recommended for performance tests)
    - QUICK_RESET=1: Quick reset without connector restart
    """
    # The fixture does all the work
    yield