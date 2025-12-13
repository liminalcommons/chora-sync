"""
Database merge operations for sync.

Merges changes between two databases using vector clocks for ordering.
"""

from dataclasses import dataclass
from typing import List, Tuple, Optional
from pathlib import Path

from .clock import VectorClock
from .changes import ChangeTracker, Change, ChangeType


@dataclass
class MergeResult:
    """Result of a database merge operation."""
    changes_sent: int
    changes_received: int
    conflicts_resolved: int
    errors: List[str]

    @property
    def success(self) -> bool:
        """True if merge completed without errors."""
        return len(self.errors) == 0


class DatabaseMerger:
    """
    Merges changes between two databases.

    Uses vector clocks to:
    1. Determine which changes need to be exchanged
    2. Order concurrent changes deterministically
    3. Apply changes without conflicts
    """

    def __init__(self, local_tracker: ChangeTracker):
        """
        Initialize merger with local change tracker.

        Args:
            local_tracker: ChangeTracker for the local database
        """
        self.local = local_tracker

    def get_changes_for_remote(self, remote_site_id: str) -> Tuple[List[Change], int]:
        """
        Get changes to send to a remote site.

        Args:
            remote_site_id: ID of the remote site

        Returns:
            Tuple of (changes to send, current local version)
        """
        # Get the last version the remote site has seen
        last_remote_version = self.local.get_site_version(remote_site_id)

        # Get all changes since then
        changes = self.local.get_changes_since(last_remote_version)

        # Filter out changes that originated from the remote site
        changes_to_send = [c for c in changes if c.site_id != remote_site_id]

        return changes_to_send, self.local.get_current_version()

    def apply_remote_changes(
        self,
        changes: List[Change],
        remote_site_id: str,
        remote_version: int,
    ) -> MergeResult:
        """
        Apply changes received from a remote site.

        Args:
            changes: Changes from remote site
            remote_site_id: ID of the remote site
            remote_version: Current version at remote site

        Returns:
            MergeResult with statistics
        """
        applied = 0
        conflicts = 0
        errors = []

        for change in changes:
            try:
                if self.local.apply_remote_change(change):
                    applied += 1
            except Exception as e:
                errors.append(f"Error applying change {change.entity_id}: {e}")

        # Update the last seen version for this remote
        self.local.update_site_version(remote_site_id, remote_version)

        return MergeResult(
            changes_sent=0,
            changes_received=applied,
            conflicts_resolved=conflicts,
            errors=errors,
        )

    def sync_with(self, remote_tracker: ChangeTracker) -> MergeResult:
        """
        Bidirectional sync with another database.

        Args:
            remote_tracker: ChangeTracker for the remote database

        Returns:
            MergeResult with statistics
        """
        remote_site_id = remote_tracker.site_id
        local_site_id = self.local.site_id

        # Get changes to send to remote
        changes_to_send, local_version = self.get_changes_for_remote(remote_site_id)

        # Get changes from remote
        last_remote_version = self.local.get_site_version(remote_site_id)
        remote_changes = remote_tracker.get_changes_since(last_remote_version)
        remote_changes = [c for c in remote_changes if c.site_id != local_site_id]
        remote_current_version = remote_tracker.get_current_version()

        # Apply remote changes locally
        local_result = self.apply_remote_changes(
            remote_changes, remote_site_id, remote_current_version
        )

        # Apply local changes to remote
        remote_merger = DatabaseMerger(remote_tracker)
        remote_result = remote_merger.apply_remote_changes(
            changes_to_send, local_site_id, local_version
        )

        # Update remote's view of local version
        remote_tracker.update_site_version(local_site_id, local_version)

        # Combine results
        all_errors = local_result.errors + [f"Remote: {e}" for e in remote_result.errors]

        return MergeResult(
            changes_sent=len(changes_to_send),
            changes_received=local_result.changes_received,
            conflicts_resolved=local_result.conflicts_resolved + remote_result.conflicts_resolved,
            errors=all_errors,
        )


def merge_databases(db_path_a: str, db_path_b: str) -> MergeResult:
    """
    Convenience function to merge two databases.

    Creates trackers and performs bidirectional sync.

    Args:
        db_path_a: Path to first database
        db_path_b: Path to second database

    Returns:
        MergeResult with statistics
    """
    import uuid

    # Create trackers with unique site IDs
    # In practice, site IDs would be persistent
    tracker_a = ChangeTracker(db_path_a, f"site-{Path(db_path_a).stem}")
    tracker_b = ChangeTracker(db_path_b, f"site-{Path(db_path_b).stem}")

    merger = DatabaseMerger(tracker_a)
    return merger.sync_with(tracker_b)
