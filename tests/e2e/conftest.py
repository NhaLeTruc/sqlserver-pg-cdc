"""
E2E test configuration.

Automatically triggers test environment reset before e2e tests.
"""

import pytest


@pytest.fixture(scope="module", autouse=True)
def e2e_test_setup(clean_test_environment):
    """
    Automatically reset test environment before e2e tests.

    This ensures e2e tests run with a clean, predictable state.

    The clean_test_environment fixture:
    - Truncates all tables
    - Clears Kafka topics
    - Resets connector offsets (unless QUICK_RESET=1)

    Environment variables:
    - SKIP_RESET=1: Skip reset (not recommended for e2e tests)
    - QUICK_RESET=1: Quick reset without connector restart
    """
    # The fixture does all the work
    yield