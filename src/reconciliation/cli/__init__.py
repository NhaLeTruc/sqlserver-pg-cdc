"""
Command-line interface for data reconciliation.

This module provides a CLI for running reconciliation jobs between
SQL Server source and PostgreSQL target databases.

Available commands:
- run: Execute one-time reconciliation
- schedule: Set up periodic reconciliation jobs
- report: Generate reports from previous runs
"""

import sys

from src.utils.vault_client import VaultClient

from .commands import cmd_report, cmd_run, cmd_schedule
from .credentials import get_credentials_from_vault_or_env, setup_logging
from .parser import create_parser


def main() -> None:
    """Main entry point for the reconcile CLI"""
    parser = create_parser()
    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    # Execute command
    if args.command == 'run':
        if not args.tables and not args.tables_file:
            parser.error("Either --tables or --tables-file is required")
        cmd_run(args)
    elif args.command == 'schedule':
        if not args.tables and not args.tables_file:
            parser.error("Either --tables or --tables-file is required")
        cmd_schedule(args)
    elif args.command == 'report':
        cmd_report(args)
    else:
        parser.print_help()
        sys.exit(1)


__all__ = [
    'main',
    'setup_logging',
    'get_credentials_from_vault_or_env',
    'cmd_run',
    'cmd_schedule',
    'cmd_report',
    'create_parser',
]


if __name__ == '__main__':
    main()
