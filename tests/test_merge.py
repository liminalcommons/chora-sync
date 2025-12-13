"""
Tests for database merge operations.
"""

import pytest
import tempfile
import os
from datetime import datetime

from chora_sync.merge import DatabaseMerger, MergeResult, merge_databases
from chora_sync.changes import ChangeTracker, Change, ChangeType
from chora_sync.clock import VectorClock


@pytest.fixture
def temp_db_a():
    """Create first temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def temp_db_b():
    """Create second temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def tracker_a(temp_db_a):
    """Create tracker for site A."""
    return ChangeTracker(temp_db_a, "site-a")


@pytest.fixture
def tracker_b(temp_db_b):
    """Create tracker for site B."""
    return ChangeTracker(temp_db_b, "site-b")


@pytest.fixture
def merger(tracker_a):
    """Create a merger for site A."""
    return DatabaseMerger(tracker_a)


class TestMergeResult:
    """Tests for MergeResult dataclass."""

    def test_success_with_no_errors(self):
        """success is True when no errors."""
        result = MergeResult(
            changes_sent=5,
            changes_received=3,
            conflicts_resolved=0,
            errors=[],
        )
        assert result.success is True

    def test_success_false_with_errors(self):
        """success is False when there are errors."""
        result = MergeResult(
            changes_sent=5,
            changes_received=3,
            conflicts_resolved=0,
            errors=["Error 1", "Error 2"],
        )
        assert result.success is False

    def test_attributes(self):
        """All attributes are accessible."""
        result = MergeResult(
            changes_sent=10,
            changes_received=20,
            conflicts_resolved=2,
            errors=["error"],
        )
        assert result.changes_sent == 10
        assert result.changes_received == 20
        assert result.conflicts_resolved == 2
        assert result.errors == ["error"]


class TestDatabaseMergerGetChangesForRemote:
    """Tests for get_changes_for_remote method."""

    def test_returns_all_changes_for_new_remote(self, merger, tracker_a):
        """Returns all changes for a remote that hasn't synced."""
        tracker_a.record_change("entity-1", ChangeType.INSERT)
        tracker_a.record_change("entity-2", ChangeType.INSERT)
        tracker_a.record_change("entity-3", ChangeType.INSERT)

        changes, version = merger.get_changes_for_remote("site-b")
        assert len(changes) == 3
        assert version == 3

    def test_returns_only_new_changes(self, merger, tracker_a):
        """Returns only changes since last sync."""
        # Record some initial changes
        tracker_a.record_change("entity-1", ChangeType.INSERT)
        tracker_a.record_change("entity-2", ChangeType.INSERT)

        # Mark site-b as having seen version 2
        tracker_a.update_site_version("site-b", 2)

        # Record more changes
        tracker_a.record_change("entity-3", ChangeType.INSERT)

        changes, version = merger.get_changes_for_remote("site-b")
        assert len(changes) == 1
        assert changes[0].entity_id == "entity-3"
        assert version == 3

    def test_filters_out_remote_origin_changes(self, merger, tracker_a):
        """Filters out changes that originated from the remote site."""
        # Local change
        tracker_a.record_change("local-entity", ChangeType.INSERT)

        # Apply a change from remote (site-b)
        remote_change = Change(
            entity_id="remote-entity",
            change_type=ChangeType.INSERT,
            table_name="entities",
            column_name=None,
            value=None,
            site_id="site-b",
            db_version=1,
            clock=VectorClock(counters={"site-b": 1}),
            timestamp=datetime.utcnow(),
        )
        tracker_a.apply_remote_change(remote_change)

        changes, _ = merger.get_changes_for_remote("site-b")

        # Should only return the local change, not the one from site-b
        assert len(changes) == 1
        assert changes[0].entity_id == "local-entity"

    def test_empty_when_no_changes(self, merger):
        """Returns empty list when no changes."""
        changes, version = merger.get_changes_for_remote("site-b")
        assert changes == []
        assert version == 0


class TestDatabaseMergerApplyRemoteChanges:
    """Tests for apply_remote_changes method."""

    def test_applies_changes_successfully(self, merger, tracker_a):
        """Successfully applies remote changes."""
        remote_changes = [
            Change(
                entity_id="entity-1",
                change_type=ChangeType.INSERT,
                table_name="entities",
                column_name=None,
                value='{"name": "Entity 1"}',
                site_id="site-b",
                db_version=1,
                clock=VectorClock(counters={"site-b": 1}),
                timestamp=datetime.utcnow(),
            ),
            Change(
                entity_id="entity-2",
                change_type=ChangeType.INSERT,
                table_name="entities",
                column_name=None,
                value='{"name": "Entity 2"}',
                site_id="site-b",
                db_version=2,
                clock=VectorClock(counters={"site-b": 2}),
                timestamp=datetime.utcnow(),
            ),
        ]

        result = merger.apply_remote_changes(remote_changes, "site-b", 2)

        assert result.success
        assert result.changes_received == 2
        assert result.errors == []

    def test_updates_remote_version(self, merger, tracker_a):
        """Updates the tracked version for the remote site."""
        changes = [
            Change(
                entity_id="entity-1",
                change_type=ChangeType.INSERT,
                table_name="entities",
                column_name=None,
                value=None,
                site_id="site-b",
                db_version=5,
                clock=VectorClock(counters={"site-b": 5}),
                timestamp=datetime.utcnow(),
            ),
        ]

        merger.apply_remote_changes(changes, "site-b", 10)

        assert tracker_a.get_site_version("site-b") == 10

    def test_skips_duplicate_changes(self, merger, tracker_a):
        """Duplicate changes are skipped (not counted as received)."""
        change = Change(
            entity_id="entity-1",
            change_type=ChangeType.INSERT,
            table_name="entities",
            column_name=None,
            value=None,
            site_id="site-b",
            db_version=1,
            clock=VectorClock(counters={"site-b": 1}),
            timestamp=datetime.utcnow(),
        )

        # Apply once
        result1 = merger.apply_remote_changes([change], "site-b", 1)
        assert result1.changes_received == 1

        # Apply again (duplicate)
        result2 = merger.apply_remote_changes([change], "site-b", 1)
        assert result2.changes_received == 0

    def test_empty_changes_list(self, merger):
        """Handles empty changes list."""
        result = merger.apply_remote_changes([], "site-b", 5)
        assert result.success
        assert result.changes_received == 0


class TestDatabaseMergerSyncWith:
    """Tests for bidirectional sync."""

    def test_syncs_changes_both_directions(self, tracker_a, tracker_b):
        """Changes flow in both directions."""
        # Create changes on both sides
        tracker_a.record_change("entity-from-a", ChangeType.INSERT)
        tracker_b.record_change("entity-from-b", ChangeType.INSERT)

        merger = DatabaseMerger(tracker_a)
        result = merger.sync_with(tracker_b)

        assert result.success
        assert result.changes_sent == 1
        assert result.changes_received == 1

    def test_sync_updates_clocks(self, tracker_a, tracker_b):
        """Sync updates vector clocks on both sides."""
        tracker_a.record_change("entity-a", ChangeType.INSERT)
        tracker_b.record_change("entity-b", ChangeType.INSERT)

        merger = DatabaseMerger(tracker_a)
        merger.sync_with(tracker_b)

        # Both trackers should have merged clocks
        clock_a = tracker_a.get_current_clock()
        clock_b = tracker_b.get_current_clock()

        assert clock_a.get("site-b") == 1
        assert clock_b.get("site-a") == 1

    def test_sync_tracks_site_versions(self, tracker_a, tracker_b):
        """Sync updates site version tracking."""
        tracker_a.record_change("entity-a", ChangeType.INSERT)
        tracker_b.record_change("entity-b", ChangeType.INSERT)

        merger = DatabaseMerger(tracker_a)
        merger.sync_with(tracker_b)

        # Each knows the other's version
        assert tracker_a.get_site_version("site-b") == 1
        assert tracker_b.get_site_version("site-a") == 1

    def test_incremental_sync(self, tracker_a, tracker_b):
        """Subsequent syncs only exchange new changes."""
        # Initial changes
        tracker_a.record_change("entity-a1", ChangeType.INSERT)
        tracker_b.record_change("entity-b1", ChangeType.INSERT)

        merger = DatabaseMerger(tracker_a)
        result1 = merger.sync_with(tracker_b)
        assert result1.changes_sent == 1
        assert result1.changes_received == 1

        # New changes after first sync
        tracker_a.record_change("entity-a2", ChangeType.INSERT)
        tracker_b.record_change("entity-b2", ChangeType.INSERT)

        result2 = merger.sync_with(tracker_b)
        assert result2.changes_sent == 1  # Only new change
        assert result2.changes_received == 1  # Only new change

    def test_no_changes_to_sync(self, tracker_a, tracker_b):
        """Handles case with no changes on either side."""
        merger = DatabaseMerger(tracker_a)
        result = merger.sync_with(tracker_b)

        assert result.success
        assert result.changes_sent == 0
        assert result.changes_received == 0

    def test_one_sided_sync(self, tracker_a, tracker_b):
        """Handles case with changes only on one side."""
        tracker_a.record_change("entity-a1", ChangeType.INSERT)
        tracker_a.record_change("entity-a2", ChangeType.INSERT)

        merger = DatabaseMerger(tracker_a)
        result = merger.sync_with(tracker_b)

        assert result.changes_sent == 2
        assert result.changes_received == 0


class TestMergeDatabasesConvenience:
    """Tests for merge_databases convenience function."""

    def test_merges_two_databases(self, temp_db_a, temp_db_b):
        """Convenience function merges two databases."""
        # Set up changes in each database
        tracker_a = ChangeTracker(temp_db_a, "site-a")
        tracker_b = ChangeTracker(temp_db_b, "site-b")

        tracker_a.record_change("entity-from-a", ChangeType.INSERT)
        tracker_b.record_change("entity-from-b", ChangeType.INSERT)

        # Use convenience function (note: it creates its own trackers)
        result = merge_databases(temp_db_a, temp_db_b)

        # Should complete successfully
        assert result.success

    def test_returns_merge_result(self, temp_db_a, temp_db_b):
        """Returns a MergeResult object."""
        result = merge_databases(temp_db_a, temp_db_b)
        assert isinstance(result, MergeResult)

    def test_empty_databases(self, temp_db_a, temp_db_b):
        """Handles empty databases."""
        result = merge_databases(temp_db_a, temp_db_b)
        assert result.success
        assert result.changes_sent == 0
        assert result.changes_received == 0


class TestSyncScenarios:
    """Real-world sync scenarios."""

    def test_multiple_entities_different_types(self, tracker_a, tracker_b):
        """Syncs multiple entities of different change types."""
        # Site A: various operations
        tracker_a.record_change("entity-1", ChangeType.INSERT, value='{"data": "1"}')
        tracker_a.record_change("entity-2", ChangeType.INSERT, value='{"data": "2"}')
        tracker_a.record_change("entity-1", ChangeType.UPDATE, column_name="data", value='{"data": "1-updated"}')

        # Site B: different operations
        tracker_b.record_change("entity-3", ChangeType.INSERT, value='{"data": "3"}')
        tracker_b.record_change("entity-3", ChangeType.DELETE)

        merger = DatabaseMerger(tracker_a)
        result = merger.sync_with(tracker_b)

        assert result.success
        assert result.changes_sent == 3
        assert result.changes_received == 2

    def test_sync_chain_three_sites(self, temp_db_a, temp_db_b):
        """Simulates sync chain: A -> B -> C."""
        fd, temp_db_c = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        try:
            tracker_a = ChangeTracker(temp_db_a, "site-a")
            tracker_b = ChangeTracker(temp_db_b, "site-b")
            tracker_c = ChangeTracker(temp_db_c, "site-c")

            # A creates entity
            tracker_a.record_change("entity-from-a", ChangeType.INSERT)

            # A syncs with B
            merger_a = DatabaseMerger(tracker_a)
            merger_a.sync_with(tracker_b)

            # B syncs with C (entity from A should propagate)
            merger_b = DatabaseMerger(tracker_b)
            result = merger_b.sync_with(tracker_c)

            assert result.changes_sent == 1  # Entity from A via B

            # Verify C has the change
            changes_at_c = tracker_c.get_changes_since(0)
            assert len(changes_at_c) == 1
            assert changes_at_c[0].entity_id == "entity-from-a"

        finally:
            if os.path.exists(temp_db_c):
                os.remove(temp_db_c)

    def test_idempotent_sync(self, tracker_a, tracker_b):
        """Multiple syncs converge to idempotent state."""
        tracker_a.record_change("entity-1", ChangeType.INSERT)

        merger = DatabaseMerger(tracker_a)

        # First sync - sends entity to remote
        result1 = merger.sync_with(tracker_b)
        assert result1.changes_sent == 1

        # Second sync - version tracking converges
        # (first sync updates remote's view of local, second updates local's view)
        result2 = merger.sync_with(tracker_b)
        # May still send due to version tracking lag, but nothing new received
        assert result2.changes_received == 0

        # Third sync - fully converged, no changes either direction
        result3 = merger.sync_with(tracker_b)
        assert result3.changes_sent == 0
        assert result3.changes_received == 0

        # Fourth sync - stays stable
        result4 = merger.sync_with(tracker_b)
        assert result4.changes_sent == 0
        assert result4.changes_received == 0
