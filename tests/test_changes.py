"""
Tests for change tracking functionality.
"""

import pytest
import tempfile
import os
from datetime import datetime
from pathlib import Path

from chora_sync.changes import ChangeTracker, Change, ChangeType
from chora_sync.clock import VectorClock


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def tracker(temp_db):
    """Create a ChangeTracker with temporary database."""
    return ChangeTracker(temp_db, "test-site")


class TestChangeDataclass:
    """Tests for the Change dataclass."""

    def test_create_change(self):
        """Create a Change object."""
        change = Change(
            entity_id="entity-1",
            change_type=ChangeType.INSERT,
            table_name="entities",
            column_name=None,
            value='{"name": "test"}',
            site_id="site-a",
            db_version=1,
            clock=VectorClock(counters={"site-a": 1}),
            timestamp=datetime.utcnow(),
        )
        assert change.entity_id == "entity-1"
        assert change.change_type == ChangeType.INSERT

    def test_change_to_dict(self):
        """Convert Change to dictionary."""
        now = datetime.utcnow()
        change = Change(
            entity_id="entity-1",
            change_type=ChangeType.UPDATE,
            table_name="entities",
            column_name="name",
            value='"new-name"',
            site_id="site-a",
            db_version=2,
            clock=VectorClock(counters={"site-a": 2}),
            timestamp=now,
        )
        d = change.to_dict()
        assert d["entity_id"] == "entity-1"
        assert d["change_type"] == "update"
        assert d["table_name"] == "entities"
        assert d["column_name"] == "name"
        assert d["value"] == '"new-name"'
        assert d["clock"] == {"site-a": 2}
        assert d["timestamp"] == now.isoformat()

    def test_change_from_dict(self):
        """Create Change from dictionary."""
        d = {
            "entity_id": "entity-1",
            "change_type": "delete",
            "table_name": "entities",
            "column_name": None,
            "value": None,
            "site_id": "site-a",
            "db_version": 3,
            "clock": {"site-a": 3},
            "timestamp": "2024-01-15T10:30:00",
        }
        change = Change.from_dict(d)
        assert change.entity_id == "entity-1"
        assert change.change_type == ChangeType.DELETE
        assert change.db_version == 3
        assert change.clock.get("site-a") == 3

    def test_change_roundtrip(self):
        """Change serialization round-trips correctly."""
        original = Change(
            entity_id="entity-1",
            change_type=ChangeType.INSERT,
            table_name="entities",
            column_name="data",
            value='{"key": "value"}',
            site_id="site-a",
            db_version=1,
            clock=VectorClock(counters={"site-a": 1, "site-b": 2}),
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
        )
        restored = Change.from_dict(original.to_dict())
        assert restored.entity_id == original.entity_id
        assert restored.change_type == original.change_type
        assert restored.clock == original.clock
        assert restored.timestamp == original.timestamp


class TestChangeType:
    """Tests for ChangeType enum."""

    def test_change_types(self):
        """All change types have correct values."""
        assert ChangeType.INSERT.value == "insert"
        assert ChangeType.UPDATE.value == "update"
        assert ChangeType.DELETE.value == "delete"

    def test_change_type_from_string(self):
        """Create ChangeType from string value."""
        assert ChangeType("insert") == ChangeType.INSERT
        assert ChangeType("update") == ChangeType.UPDATE
        assert ChangeType("delete") == ChangeType.DELETE


class TestChangeTrackerInit:
    """Tests for ChangeTracker initialization."""

    def test_init_creates_tables(self, tracker):
        """Initialization creates required tables."""
        import sqlite3
        conn = sqlite3.connect(tracker.db_path)
        cursor = conn.cursor()

        # Check tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        assert "sync_changes" in tables
        assert "sync_sites" in tables
        assert "sync_clock" in tables
        conn.close()

    def test_init_starts_with_empty_clock(self, tracker):
        """New tracker starts with empty vector clock."""
        clock = tracker.get_current_clock()
        assert clock.counters == {}

    def test_init_starts_at_version_zero(self, tracker):
        """New tracker starts at version 0."""
        assert tracker.get_current_version() == 0


class TestChangeTrackerRecordChange:
    """Tests for recording changes."""

    def test_record_insert(self, tracker):
        """Record an insert change."""
        change = tracker.record_change(
            entity_id="entity-1",
            change_type=ChangeType.INSERT,
            value='{"name": "test"}',
        )
        assert change.entity_id == "entity-1"
        assert change.change_type == ChangeType.INSERT
        assert change.site_id == "test-site"
        assert change.db_version == 1

    def test_record_update(self, tracker):
        """Record an update change."""
        change = tracker.record_change(
            entity_id="entity-1",
            change_type=ChangeType.UPDATE,
            column_name="name",
            value='"new-name"',
        )
        assert change.change_type == ChangeType.UPDATE
        assert change.column_name == "name"

    def test_record_delete(self, tracker):
        """Record a delete change."""
        change = tracker.record_change(
            entity_id="entity-1",
            change_type=ChangeType.DELETE,
        )
        assert change.change_type == ChangeType.DELETE
        assert change.value is None

    def test_recording_increments_clock(self, tracker):
        """Recording a change increments the vector clock."""
        initial_clock = tracker.get_current_clock()
        assert initial_clock.get("test-site") == 0

        tracker.record_change("entity-1", ChangeType.INSERT)

        new_clock = tracker.get_current_clock()
        assert new_clock.get("test-site") == 1

    def test_recording_increments_version(self, tracker):
        """Recording a change increments the database version."""
        assert tracker.get_current_version() == 0

        tracker.record_change("entity-1", ChangeType.INSERT)
        assert tracker.get_current_version() == 1

        tracker.record_change("entity-2", ChangeType.INSERT)
        assert tracker.get_current_version() == 2

    def test_multiple_records(self, tracker):
        """Record multiple changes."""
        for i in range(5):
            tracker.record_change(f"entity-{i}", ChangeType.INSERT)

        assert tracker.get_current_version() == 5
        assert tracker.get_current_clock().get("test-site") == 5


class TestChangeTrackerGetChanges:
    """Tests for retrieving changes."""

    def test_get_changes_empty(self, tracker):
        """Get changes from empty database."""
        changes = tracker.get_changes_since(0)
        assert changes == []

    def test_get_all_changes(self, tracker):
        """Get all changes since version 0."""
        tracker.record_change("entity-1", ChangeType.INSERT)
        tracker.record_change("entity-2", ChangeType.INSERT)
        tracker.record_change("entity-1", ChangeType.UPDATE, column_name="name")

        changes = tracker.get_changes_since(0)
        assert len(changes) == 3
        assert changes[0].entity_id == "entity-1"
        assert changes[1].entity_id == "entity-2"
        assert changes[2].change_type == ChangeType.UPDATE

    def test_get_changes_since_version(self, tracker):
        """Get changes after a specific version."""
        tracker.record_change("entity-1", ChangeType.INSERT)
        tracker.record_change("entity-2", ChangeType.INSERT)
        tracker.record_change("entity-3", ChangeType.INSERT)

        changes = tracker.get_changes_since(2)
        assert len(changes) == 1
        assert changes[0].entity_id == "entity-3"

    def test_get_changes_preserves_clock(self, tracker):
        """Retrieved changes have correct vector clocks."""
        tracker.record_change("entity-1", ChangeType.INSERT)
        tracker.record_change("entity-2", ChangeType.INSERT)

        changes = tracker.get_changes_since(0)
        assert changes[0].clock.get("test-site") == 1
        assert changes[1].clock.get("test-site") == 2


class TestChangeTrackerApplyRemote:
    """Tests for applying remote changes."""

    def test_apply_remote_change(self, tracker):
        """Apply a change from another site."""
        remote_change = Change(
            entity_id="entity-1",
            change_type=ChangeType.INSERT,
            table_name="entities",
            column_name=None,
            value='{"name": "remote"}',
            site_id="remote-site",
            db_version=1,
            clock=VectorClock(counters={"remote-site": 1}),
            timestamp=datetime.utcnow(),
        )

        result = tracker.apply_remote_change(remote_change)
        assert result is True

        # Clock should be merged
        clock = tracker.get_current_clock()
        assert clock.get("remote-site") == 1

    def test_apply_duplicate_change(self, tracker):
        """Applying duplicate change returns False."""
        remote_change = Change(
            entity_id="entity-1",
            change_type=ChangeType.INSERT,
            table_name="entities",
            column_name=None,
            value='{"name": "remote"}',
            site_id="remote-site",
            db_version=1,
            clock=VectorClock(counters={"remote-site": 1}),
            timestamp=datetime.utcnow(),
        )

        # Apply twice
        result1 = tracker.apply_remote_change(remote_change)
        result2 = tracker.apply_remote_change(remote_change)

        assert result1 is True
        assert result2 is False

    def test_apply_merges_clocks(self, tracker):
        """Applying remote change merges vector clocks."""
        # Create local change
        tracker.record_change("local-entity", ChangeType.INSERT)

        # Apply remote change
        remote_change = Change(
            entity_id="remote-entity",
            change_type=ChangeType.INSERT,
            table_name="entities",
            column_name=None,
            value=None,
            site_id="remote-site",
            db_version=5,
            clock=VectorClock(counters={"remote-site": 5}),
            timestamp=datetime.utcnow(),
        )
        tracker.apply_remote_change(remote_change)

        # Clock should have both sites
        clock = tracker.get_current_clock()
        assert clock.get("test-site") == 1
        assert clock.get("remote-site") == 5


class TestChangeTrackerSiteTracking:
    """Tests for site version tracking."""

    def test_initial_site_version(self, tracker):
        """Unknown site returns version 0."""
        version = tracker.get_site_version("unknown-site")
        assert version == 0

    def test_update_site_version(self, tracker):
        """Update and retrieve site version."""
        tracker.update_site_version("remote-site", 10)
        version = tracker.get_site_version("remote-site")
        assert version == 10

    def test_update_site_version_multiple(self, tracker):
        """Update site version multiple times."""
        tracker.update_site_version("remote-site", 5)
        tracker.update_site_version("remote-site", 10)
        version = tracker.get_site_version("remote-site")
        assert version == 10

    def test_multiple_sites(self, tracker):
        """Track multiple remote sites."""
        tracker.update_site_version("site-a", 5)
        tracker.update_site_version("site-b", 10)
        tracker.update_site_version("site-c", 15)

        assert tracker.get_site_version("site-a") == 5
        assert tracker.get_site_version("site-b") == 10
        assert tracker.get_site_version("site-c") == 15


class TestChangeTrackerPersistence:
    """Tests for persistence across tracker instances."""

    def test_clock_persists(self, temp_db):
        """Clock state persists across tracker instances."""
        # First tracker
        tracker1 = ChangeTracker(temp_db, "test-site")
        tracker1.record_change("entity-1", ChangeType.INSERT)
        tracker1.record_change("entity-2", ChangeType.INSERT)

        # New tracker on same database
        tracker2 = ChangeTracker(temp_db, "test-site")
        clock = tracker2.get_current_clock()
        assert clock.get("test-site") == 2

    def test_changes_persist(self, temp_db):
        """Changes persist across tracker instances."""
        # First tracker
        tracker1 = ChangeTracker(temp_db, "test-site")
        tracker1.record_change("entity-1", ChangeType.INSERT)

        # New tracker
        tracker2 = ChangeTracker(temp_db, "test-site")
        changes = tracker2.get_changes_since(0)
        assert len(changes) == 1
        assert changes[0].entity_id == "entity-1"

    def test_version_persists(self, temp_db):
        """Version persists across tracker instances."""
        tracker1 = ChangeTracker(temp_db, "test-site")
        tracker1.record_change("entity-1", ChangeType.INSERT)
        tracker1.record_change("entity-2", ChangeType.INSERT)

        tracker2 = ChangeTracker(temp_db, "test-site")
        assert tracker2.get_current_version() == 2
