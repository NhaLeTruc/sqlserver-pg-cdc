#!/usr/bin/env python3
"""
Reset test environment to clean state.

This module provides programmatic access to test environment reset functionality,
allowing tests to reset the environment before execution.

Usage:
    python reset_test_environment.py
    python reset_test_environment.py --quick

    # From Python code:
    from scripts.python.reset_test_environment import reset_environment
    reset_environment(quick=False)
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """Get the project root directory."""
    # This script is in scripts/python/, so go up two levels
    return Path(__file__).parent.parent.parent


def reset_environment(quick: bool = False, verbose: bool = False) -> bool:
    """
    Reset the test environment to a clean state.

    Args:
        quick: If True, skip connector restart (faster but less thorough)
        verbose: If True, show detailed output

    Returns:
        True if reset was successful, False otherwise

    Raises:
        FileNotFoundError: If reset script is not found
        subprocess.CalledProcessError: If reset script fails
    """
    project_root = get_project_root()
    reset_script = project_root / "scripts" / "bash" / "reset-test-environment.sh"

    if not reset_script.exists():
        raise FileNotFoundError(f"Reset script not found: {reset_script}")

    # Build command
    cmd = [str(reset_script)]
    if quick:
        cmd.append("--quick")

    # Set up environment (inherit from parent process)
    env = os.environ.copy()

    try:
        logger.info("Starting test environment reset...")

        # Run the reset script
        result = subprocess.run(
            cmd,
            cwd=project_root,
            env=env,
            capture_output=not verbose,
            text=True,
            check=True
        )

        if verbose and result.stdout:
            print(result.stdout)

        logger.info("Test environment reset completed successfully")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Reset script failed with exit code {e.returncode}")
        if e.stdout:
            logger.error(f"STDOUT: {e.stdout}")
        if e.stderr:
            logger.error(f"STDERR: {e.stderr}")
        return False

    except Exception as e:
        logger.error(f"Unexpected error during reset: {e}")
        return False


def main():
    """Command-line interface for test environment reset."""
    parser = argparse.ArgumentParser(
        description='Reset test environment to clean state',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full reset (truncate tables, clear Kafka, redeploy connectors)
  python reset_test_environment.py

  # Quick reset (skip connector restart)
  python reset_test_environment.py --quick

  # Verbose output
  python reset_test_environment.py --verbose
        """
    )

    parser.add_argument(
        '--quick',
        action='store_true',
        help='Quick mode: skip connector restart (faster but less thorough)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed output from reset script'
    )

    args = parser.parse_args()

    # Run reset
    success = reset_environment(quick=args.quick, verbose=args.verbose)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
