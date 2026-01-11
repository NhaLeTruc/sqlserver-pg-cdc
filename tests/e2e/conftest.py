"""
E2E test configuration.

The clean_test_environment fixture is available for manual use but no longer
automatically triggered. To use it, explicitly add it as a parameter to your test:

    def test_something(clean_test_environment):
        # test code here
"""


# Disabled: Auto-cleanup integration removed
# To use clean_test_environment, explicitly add it as a test parameter
#
# @pytest.fixture(scope="module", autouse=True)
# def e2e_test_setup(clean_test_environment):
#     """
#     Automatically reset test environment before e2e tests.
#
#     This ensures e2e tests run with a clean, predictable state.
#
#     The clean_test_environment fixture:
#     - Truncates all tables
#     - Clears Kafka topics
#     - Resets connector offsets (unless QUICK_RESET=1)
#
#     Environment variables:
#     - SKIP_RESET=1: Skip reset (not recommended for e2e tests)
#     - QUICK_RESET=1: Quick reset without connector restart
#     """
#     # The fixture does all the work
#     yield
