"""
CR-SQLite extension loader.

CR-SQLite is a SQLite extension that adds CRDT support for conflict-free sync.
"""

import sqlite3
import platform
import os
from pathlib import Path
from typing import Optional


class CRSQLiteNotAvailable(Exception):
    """CR-SQLite extension is not available."""
    pass


def get_extension_path() -> Optional[Path]:
    """
    Get the path to the cr-sqlite extension for the current platform.

    Returns:
        Path to extension file, or None if not found
    """
    # Determine platform-specific extension name
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "darwin":
        ext_name = "crsqlite.dylib"
        arch = "arm64" if machine == "arm64" else "x86_64"
    elif system == "linux":
        ext_name = "crsqlite.so"
        arch = "arm64" if machine in ("aarch64", "arm64") else "x86_64"
    elif system == "windows":
        ext_name = "crsqlite.dll"
        arch = "x86_64"  # Windows primarily x86_64
    else:
        return None

    # Check common locations
    search_paths = [
        # Local package extensions directory
        Path(__file__).parent / "extensions" / ext_name,
        # User-level installation
        Path.home() / ".chora" / "extensions" / ext_name,
        # System-level
        Path("/usr/local/lib") / ext_name,
        Path("/usr/lib") / ext_name,
        # Environment variable
        Path(os.environ.get("CRSQLITE_PATH", "")) if os.environ.get("CRSQLITE_PATH") else None,
    ]

    for path in search_paths:
        if path and path.exists():
            return path

    return None


def is_crsqlite_available() -> bool:
    """
    Check if cr-sqlite extension is available.

    Returns:
        True if extension can be loaded
    """
    return get_extension_path() is not None


def load_crsqlite(conn: sqlite3.Connection, extension_path: Optional[Path] = None) -> None:
    """
    Load the cr-sqlite extension into a connection.

    Args:
        conn: SQLite connection to load extension into
        extension_path: Optional explicit path to extension

    Raises:
        CRSQLiteNotAvailable: If extension cannot be loaded
    """
    path = extension_path or get_extension_path()

    if path is None:
        raise CRSQLiteNotAvailable(
            "CR-SQLite extension not found. Install it:\n"
            "  macOS: brew install nickstenning/tap/crsqlite\n"
            "  Or download from: https://github.com/vlcn-io/cr-sqlite/releases\n"
            "  And set CRSQLITE_PATH environment variable"
        )

    # Enable extension loading
    conn.enable_load_extension(True)

    try:
        conn.load_extension(str(path.with_suffix("")))  # SQLite wants path without extension
    except sqlite3.OperationalError as e:
        raise CRSQLiteNotAvailable(f"Failed to load CR-SQLite from {path}: {e}")


def init_crsqlite_tables(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Initialize CRDT tracking for a table.

    This converts a regular table to a CRDT-enabled table that can be
    synced conflict-free between databases.

    Args:
        conn: SQLite connection with cr-sqlite loaded
        table_name: Name of table to enable CRDT sync for
    """
    # Enable CRDT on the table
    # cr-sqlite uses crsql_as_crr() to mark tables for CRDT tracking
    conn.execute(f"SELECT crsql_as_crr('{table_name}')")


def get_site_id(conn: sqlite3.Connection) -> bytes:
    """
    Get the unique site ID for this database.

    Each database has a unique site ID that identifies it in the sync network.

    Args:
        conn: SQLite connection with cr-sqlite loaded

    Returns:
        Site ID as bytes
    """
    row = conn.execute("SELECT crsql_site_id()").fetchone()
    return row[0]


def get_db_version(conn: sqlite3.Connection) -> int:
    """
    Get the current database version (for sync).

    Args:
        conn: SQLite connection with cr-sqlite loaded

    Returns:
        Current version number
    """
    row = conn.execute("SELECT crsql_db_version()").fetchone()
    return row[0]


def get_changes_since(conn: sqlite3.Connection, since_version: int) -> list:
    """
    Get all changes since a version.

    Args:
        conn: SQLite connection with cr-sqlite loaded
        since_version: Get changes after this version

    Returns:
        List of change tuples
    """
    return conn.execute(
        "SELECT * FROM crsql_changes WHERE db_version > ?",
        (since_version,)
    ).fetchall()


def apply_changes(conn: sqlite3.Connection, changes: list) -> None:
    """
    Apply changes from another database.

    Args:
        conn: SQLite connection with cr-sqlite loaded
        changes: List of change tuples from get_changes_since()
    """
    for change in changes:
        conn.execute(
            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            change
        )
