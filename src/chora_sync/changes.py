"""
Change tracking for sync.

Tracks changes to entities for syncing between databases.
"""

import sqlite3
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional
from pathlib import Path

from .clock import VectorClock


class ChangeType(Enum):
    """Type of change to an entity."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class Change:
    """
    A single change to an entity.

    Records what changed, when, and the causal ordering (vector clock).
    """
    entity_id: str
    change_type: ChangeType
    table_name: str
    column_name: Optional[str]
    value: Optional[str]
    site_id: str
    db_version: int
    clock: VectorClock
    timestamp: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "entity_id": self.entity_id,
            "change_type": self.change_type.value,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "value": self.value,
            "site_id": self.site_id,
            "db_version": self.db_version,
            "clock": self.clock.to_dict(),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Change":
        """Create from dictionary."""
        return cls(
            entity_id=d["entity_id"],
            change_type=ChangeType(d["change_type"]),
            table_name=d["table_name"],
            column_name=d.get("column_name"),
            value=d.get("value"),
            site_id=d["site_id"],
            db_version=d["db_version"],
            clock=VectorClock.from_dict(d["clock"]),
            timestamp=datetime.fromisoformat(d["timestamp"]),
        )


class ChangeTracker:
    """
    Tracks changes to entities for sync.

    Uses a separate table to record all changes with vector clocks.
    Changes can then be exchanged between databases for sync.
    """

    def __init__(self, db_path: str, site_id: str):
        """
        Initialize change tracker.

        Args:
            db_path: Path to SQLite database
            site_id: Unique identifier for this site/database
        """
        self.db_path = Path(db_path).expanduser()
        self.site_id = site_id
        self._current_clock = VectorClock()
        self._init_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self) -> None:
        """Initialize change tracking tables."""
        conn = self._get_connection()
        try:
            conn.executescript("""
                -- Change log table
                CREATE TABLE IF NOT EXISTS sync_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    change_type TEXT NOT NULL CHECK (change_type IN ('insert', 'update', 'delete')),
                    table_name TEXT NOT NULL,
                    column_name TEXT,
                    value TEXT,
                    site_id TEXT NOT NULL,
                    db_version INTEGER NOT NULL,
                    clock_json TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_sync_changes_version ON sync_changes(db_version);
                CREATE INDEX IF NOT EXISTS idx_sync_changes_entity ON sync_changes(entity_id);

                -- Site metadata table
                CREATE TABLE IF NOT EXISTS sync_sites (
                    site_id TEXT PRIMARY KEY,
                    last_seen_version INTEGER DEFAULT 0,
                    last_sync TEXT
                );

                -- Local clock state
                CREATE TABLE IF NOT EXISTS sync_clock (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    clock_json TEXT NOT NULL
                );
            """)

            # Load or initialize clock
            row = conn.execute("SELECT clock_json FROM sync_clock WHERE id = 1").fetchone()
            if row:
                self._current_clock = VectorClock.from_json(row["clock_json"])
            else:
                self._current_clock = VectorClock()
                conn.execute(
                    "INSERT INTO sync_clock (id, clock_json) VALUES (1, ?)",
                    (self._current_clock.to_json(),)
                )

            conn.commit()
        finally:
            conn.close()

    def record_change(
        self,
        entity_id: str,
        change_type: ChangeType,
        table_name: str = "entities",
        column_name: Optional[str] = None,
        value: Optional[str] = None,
    ) -> Change:
        """
        Record a change to an entity.

        Args:
            entity_id: ID of the changed entity
            change_type: Type of change
            table_name: Table that was changed
            column_name: Column that was changed (for updates)
            value: New value (JSON string)

        Returns:
            The recorded Change
        """
        # Increment clock for this site
        self._current_clock = self._current_clock.increment(self.site_id)

        conn = self._get_connection()
        try:
            # Get current db version
            row = conn.execute("SELECT COALESCE(MAX(db_version), 0) as v FROM sync_changes").fetchone()
            db_version = row["v"] + 1

            timestamp = datetime.utcnow()

            change = Change(
                entity_id=entity_id,
                change_type=change_type,
                table_name=table_name,
                column_name=column_name,
                value=value,
                site_id=self.site_id,
                db_version=db_version,
                clock=self._current_clock,
                timestamp=timestamp,
            )

            # Insert change record
            conn.execute(
                """
                INSERT INTO sync_changes
                (entity_id, change_type, table_name, column_name, value, site_id, db_version, clock_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    change.entity_id,
                    change.change_type.value,
                    change.table_name,
                    change.column_name,
                    change.value,
                    change.site_id,
                    change.db_version,
                    change.clock.to_json(),
                    change.timestamp.isoformat(),
                ),
            )

            # Update local clock state
            conn.execute(
                "UPDATE sync_clock SET clock_json = ? WHERE id = 1",
                (self._current_clock.to_json(),)
            )

            conn.commit()
            return change
        finally:
            conn.close()

    def get_changes_since(self, since_version: int = 0) -> List[Change]:
        """
        Get all changes since a version.

        Args:
            since_version: Get changes after this version

        Returns:
            List of changes
        """
        conn = self._get_connection()
        try:
            rows = conn.execute(
                """
                SELECT * FROM sync_changes
                WHERE db_version > ?
                ORDER BY db_version ASC
                """,
                (since_version,),
            ).fetchall()

            return [
                Change(
                    entity_id=row["entity_id"],
                    change_type=ChangeType(row["change_type"]),
                    table_name=row["table_name"],
                    column_name=row["column_name"],
                    value=row["value"],
                    site_id=row["site_id"],
                    db_version=row["db_version"],
                    clock=VectorClock.from_json(row["clock_json"]),
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                )
                for row in rows
            ]
        finally:
            conn.close()

    def get_current_version(self) -> int:
        """Get the current database version."""
        conn = self._get_connection()
        try:
            row = conn.execute("SELECT COALESCE(MAX(db_version), 0) as v FROM sync_changes").fetchone()
            return row["v"]
        finally:
            conn.close()

    def get_current_clock(self) -> VectorClock:
        """Get the current vector clock."""
        return self._current_clock

    def apply_remote_change(self, change: Change) -> bool:
        """
        Apply a change from a remote site.

        Uses vector clock comparison to determine if change should be applied.

        Args:
            change: Change from remote site

        Returns:
            True if change was applied, False if it was a duplicate or old
        """
        conn = self._get_connection()
        try:
            # Check if we've already seen this change
            existing = conn.execute(
                """
                SELECT clock_json FROM sync_changes
                WHERE entity_id = ? AND site_id = ? AND db_version = ?
                """,
                (change.entity_id, change.site_id, change.db_version),
            ).fetchone()

            if existing:
                return False  # Already have this change

            # Merge clocks
            self._current_clock = self._current_clock.merge(change.clock)

            # Record the remote change
            conn.execute(
                """
                INSERT INTO sync_changes
                (entity_id, change_type, table_name, column_name, value, site_id, db_version, clock_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    change.entity_id,
                    change.change_type.value,
                    change.table_name,
                    change.column_name,
                    change.value,
                    change.site_id,
                    change.db_version,
                    change.clock.to_json(),
                    change.timestamp.isoformat(),
                ),
            )

            # Update local clock
            conn.execute(
                "UPDATE sync_clock SET clock_json = ? WHERE id = 1",
                (self._current_clock.to_json(),)
            )

            conn.commit()
            return True
        finally:
            conn.close()

    def update_site_version(self, site_id: str, version: int) -> None:
        """
        Update the last seen version for a remote site.

        Args:
            site_id: Remote site ID
            version: Last version we synced from that site
        """
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO sync_sites (site_id, last_seen_version, last_sync)
                VALUES (?, ?, ?)
                ON CONFLICT(site_id) DO UPDATE SET
                    last_seen_version = excluded.last_seen_version,
                    last_sync = excluded.last_sync
                """,
                (site_id, version, datetime.utcnow().isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    def get_site_version(self, site_id: str) -> int:
        """
        Get the last seen version for a remote site.

        Args:
            site_id: Remote site ID

        Returns:
            Last version we synced from that site, or 0
        """
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT last_seen_version FROM sync_sites WHERE site_id = ?",
                (site_id,),
            ).fetchone()
            return row["last_seen_version"] if row else 0
        finally:
            conn.close()
