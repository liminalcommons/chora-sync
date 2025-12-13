"""
chora-sync: CR-SQLite based sync for chora workspaces.

Provides conflict-free sync between SQLite databases using CRDTs.
"""

from .extension import load_crsqlite, is_crsqlite_available, CRSQLiteNotAvailable
from .clock import VectorClock
from .changes import ChangeTracker, Change, ChangeType
from .merge import DatabaseMerger, MergeResult

__version__ = "0.1.0"
__all__ = [
    "load_crsqlite",
    "is_crsqlite_available",
    "CRSQLiteNotAvailable",
    "VectorClock",
    "ChangeTracker",
    "Change",
    "ChangeType",
    "DatabaseMerger",
    "MergeResult",
]
