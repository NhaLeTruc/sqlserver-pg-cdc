"""
Command-line argument parser configuration.

This module sets up the argument parser for the reconcile CLI tool,
defining all commands and their options.
"""

import argparse


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the argument parser for the CLI.

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        description="Data reconciliation tool for SQL Server to PostgreSQL CDC pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run one-time reconciliation for specific tables
  reconcile run --tables customers,orders,products --output report.json

  # Use Vault for credentials
  reconcile run --use-vault --tables customers --validate-checksums

  # Perform row-level reconciliation with repair script generation
  reconcile run --tables customers --row-level --generate-repair --pk-columns customer_id

  # Row-level reconciliation with composite primary key
  reconcile run --tables user_orgs --row-level --pk-columns user_id,org_id --generate-repair

  # Parallel reconciliation (3-5x faster for multiple tables)
  reconcile run --tables customers,orders,products,users --parallel --parallel-workers 4

  # Parallel reconciliation with row-level analysis
  reconcile run --tables-file tables.txt --parallel --parallel-workers 8 --row-level

  # Schedule periodic reconciliation every 6 hours
  reconcile schedule --cron "0 */6 * * *" --tables-file tables.txt

  # Schedule with interval (in seconds)
  reconcile schedule --interval 3600 --tables customers

  # Generate console report from previous run
  reconcile report --input report.json --format console
        """
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # ========== Run command ==========
    run_parser = subparsers.add_parser('run', help='Run one-time reconciliation')
    run_parser.add_argument(
        '--tables',
        help='Comma-separated list of tables to reconcile'
    )
    run_parser.add_argument(
        '--tables-file',
        help='File containing list of tables (one per line)'
    )
    run_parser.add_argument(
        '--validate-checksums',
        action='store_true',
        help='Validate data checksums (slower but more thorough)'
    )
    run_parser.add_argument(
        '--output',
        help='Output file path for report'
    )
    run_parser.add_argument(
        '--format',
        choices=['console', 'json', 'csv'],
        default='console',
        help='Output format (default: console)'
    )
    run_parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue with remaining tables if one fails'
    )
    run_parser.add_argument(
        '--row-level',
        action='store_true',
        help='Perform row-level reconciliation (detailed comparison)'
    )
    run_parser.add_argument(
        '--pk-columns',
        default='id',
        help='Comma-separated list of primary key columns (default: id)'
    )
    run_parser.add_argument(
        '--row-level-chunk-size',
        type=int,
        default=1000,
        help='Chunk size for row-level reconciliation (default: 1000)'
    )
    run_parser.add_argument(
        '--generate-repair',
        action='store_true',
        help='Generate SQL repair scripts for row-level discrepancies'
    )
    run_parser.add_argument(
        '--output-dir',
        help='Output directory for repair scripts (default: current directory)'
    )
    run_parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel table reconciliation (3-5x faster for multiple tables)'
    )
    run_parser.add_argument(
        '--parallel-workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    run_parser.add_argument(
        '--parallel-timeout',
        type=int,
        default=3600,
        help='Timeout per table in seconds for parallel mode (default: 3600)'
    )
    run_parser.add_argument(
        '--use-vault',
        action='store_true',
        help='Fetch credentials from HashiCorp Vault'
    )
    # Source database options
    run_parser.add_argument('--source-server', help='SQL Server host')
    run_parser.add_argument('--source-database', help='SQL Server database name')
    run_parser.add_argument('--source-user', help='SQL Server username')
    run_parser.add_argument('--source-password', help='SQL Server password')
    # Target database options
    run_parser.add_argument('--target-host', help='PostgreSQL host')
    run_parser.add_argument('--target-port', help='PostgreSQL port')
    run_parser.add_argument('--target-database', help='PostgreSQL database name')
    run_parser.add_argument('--target-user', help='PostgreSQL username')
    run_parser.add_argument('--target-password', help='PostgreSQL password')

    # ========== Schedule command ==========
    schedule_parser = subparsers.add_parser('schedule', help='Schedule periodic reconciliation')
    schedule_parser.add_argument(
        '--tables',
        help='Comma-separated list of tables to reconcile'
    )
    schedule_parser.add_argument(
        '--tables-file',
        help='File containing list of tables (one per line)'
    )
    schedule_parser.add_argument(
        '--validate-checksums',
        action='store_true',
        help='Validate data checksums (slower but more thorough)'
    )
    schedule_parser.add_argument(
        '--cron',
        help='Cron expression (e.g., "0 */6 * * *" for every 6 hours)'
    )
    schedule_parser.add_argument(
        '--interval',
        type=int,
        default=3600,
        help='Interval in seconds (default: 3600 = 1 hour)'
    )
    schedule_parser.add_argument(
        '--output-dir',
        help='Directory to save reconciliation reports'
    )
    schedule_parser.add_argument(
        '--use-vault',
        action='store_true',
        help='Fetch credentials from HashiCorp Vault'
    )
    # Source database options
    schedule_parser.add_argument('--source-server', help='SQL Server host')
    schedule_parser.add_argument('--source-database', help='SQL Server database name')
    schedule_parser.add_argument('--source-user', help='SQL Server username')
    schedule_parser.add_argument('--source-password', help='SQL Server password')
    # Target database options
    schedule_parser.add_argument('--target-host', help='PostgreSQL host')
    schedule_parser.add_argument('--target-port', help='PostgreSQL port')
    schedule_parser.add_argument('--target-database', help='PostgreSQL database name')
    schedule_parser.add_argument('--target-user', help='PostgreSQL username')
    schedule_parser.add_argument('--target-password', help='PostgreSQL password')

    # ========== Report command ==========
    report_parser = subparsers.add_parser('report', help='Generate report from previous run')
    report_parser.add_argument(
        '--input',
        required=True,
        help='Input JSON report file'
    )
    report_parser.add_argument(
        '--format',
        choices=['console', 'json', 'csv'],
        default='console',
        help='Output format (default: console)'
    )
    report_parser.add_argument(
        '--output',
        help='Output file path (required for json and csv formats)'
    )

    return parser
