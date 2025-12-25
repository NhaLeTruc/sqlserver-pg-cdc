"""
Performance test configuration.

The clean_test_environment fixture is available for manual use but no longer
automatically triggered. To use it, explicitly add it as a parameter to your test:

    def test_performance(clean_test_environment):
        # test code here
"""

import pytest


# Disabled: Auto-cleanup integration removed
# To use clean_test_environment, explicitly add it as a test parameter
#
# @pytest.fixture(scope="module", autouse=True)
# def performance_test_setup(clean_test_environment):
#     """
#     Automatically reset test environment before performance tests.
#
#     This ensures performance measurements are not affected by stale data
#     from previous test runs.
#
#     The clean_test_environment fixture:
#     - Truncates all tables
#     - Clears Kafka topics
#     - Resets connector offsets (unless QUICK_RESET=1)
#
#     Environment variables:
#     - SKIP_RESET=1: Skip reset (not recommended for performance tests)
#     - QUICK_RESET=1: Quick reset without connector restart
#     """
#     # The fixture does all the work
#     yield