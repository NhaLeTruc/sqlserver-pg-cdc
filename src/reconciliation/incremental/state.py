"""
Checksum state management for incremental reconciliation.

This module provides the IncrementalChecksumTracker class for managing
checksum state across reconciliation runs.
"""

import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

from opentelemetry import trace
from prometheus_client import Counter

from src.utils.tracing import trace_operation

logger = logging.getLogger(__name__)


# Metrics
CHECKSUM_STATE_OPERATIONS = Counter(
    "checksum_state_operations_total",
    "Checksum state file operations",
    ["operation"],  # load, save
)


class IncrementalChecksumTracker:
    """
    Tracks checksum state for incremental updates.

    Stores last checksum calculation timestamp, checksum value,
    and row count for each table to enable delta processing.
    """

    def __init__(self, state_dir: str = "./reconciliation_state"):
        """
        Initialize checksum tracker.

        Args:
            state_dir: Directory to store state files
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized checksum tracker with state dir: {self.state_dir}")

    def get_last_checksum_timestamp(self, table: str) -> datetime | None:
        """
        Get timestamp of last checksum calculation.

        Args:
            table: Table name

        Returns:
            Timestamp of last checksum, or None if never calculated
        """
        with trace_operation(
            "get_last_checksum_timestamp",
            kind=trace.SpanKind.INTERNAL,
            table=table,
        ):
            state_file = self._get_state_file(table)

            if not state_file.exists():
                logger.debug(f"No previous checksum state for table {table}")
                return None

            try:
                with open(state_file) as f:
                    state = json.load(f)

                last_run = datetime.fromisoformat(state["last_run"])
                logger.debug(f"Last checksum for {table}: {last_run.isoformat()}")

                CHECKSUM_STATE_OPERATIONS.labels(operation="load").inc()
                return last_run

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Failed to load checksum state for {table}: {e}")
                return None

    def get_last_checksum(self, table: str) -> str | None:
        """
        Get last calculated checksum value.

        Args:
            table: Table name

        Returns:
            Last checksum value, or None if never calculated
        """
        state_file = self._get_state_file(table)

        if not state_file.exists():
            return None

        try:
            with open(state_file) as f:
                state = json.load(f)
            return state.get("checksum")
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to load checksum for {table}: {e}")
            return None

    def save_checksum_state(
        self,
        table: str,
        checksum: str,
        row_count: int,
        timestamp: datetime | None = None,
        mode: str = "full",
    ) -> None:
        """
        Save checksum state for table.

        Args:
            table: Table name
            checksum: Calculated checksum
            row_count: Number of rows processed
            timestamp: Timestamp of calculation (defaults to now)
            mode: Calculation mode ('full' or 'incremental')
        """
        with trace_operation(
            "save_checksum_state",
            kind=trace.SpanKind.INTERNAL,
            table=table,
            mode=mode,
        ):
            if timestamp is None:
                timestamp = datetime.now(UTC)

            state_file = self._get_state_file(table)

            state = {
                "table": table,
                "checksum": checksum,
                "row_count": row_count,
                "last_run": timestamp.isoformat(),
                "mode": mode,
            }

            try:
                with open(state_file, "w") as f:
                    json.dump(state, f, indent=2)

                logger.info(
                    f"Saved checksum state for {table}: "
                    f"{row_count} rows, mode={mode}"
                )

                CHECKSUM_STATE_OPERATIONS.labels(operation="save").inc()

            except Exception as e:
                logger.error(f"Failed to save checksum state for {table}: {e}")
                raise

    def clear_state(self, table: str) -> None:
        """
        Clear saved state for a table.

        Args:
            table: Table name
        """
        state_file = self._get_state_file(table)

        if state_file.exists():
            state_file.unlink()
            logger.info(f"Cleared checksum state for table {table}")

    def list_tracked_tables(self) -> list[str]:
        """
        List all tables with saved checksum state.

        Returns:
            List of table names
        """
        tables = []

        for state_file in self.state_dir.glob("*_checksum_state.json"):
            table_name = state_file.stem.replace("_checksum_state", "")
            tables.append(table_name)

        return sorted(tables)

    def _get_state_file(self, table: str) -> Path:
        """Get state file path for table."""
        # BUG-12: Sanitize table name for filesystem - handle all OS special chars
        safe_table_name = re.sub(r'[/\\:*?"<>|]', '_', table)
        return self.state_dir / f"{safe_table_name}_checksum_state.json"
