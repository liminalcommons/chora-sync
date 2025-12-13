# chora-sync

**CRDT-based sync for chora workspaces.**

Enables conflict-free synchronization between SQLite databases using vector clocks and CR-SQLite.

## Status

Phase 2 - Sync prep. This package provides the foundation for Phase 3 multiplayer sync.

## Features

- **Vector Clocks**: Track causality between changes across databases
- **Change Tracking**: Record all entity changes with ordering information
- **Database Merging**: Bidirectional sync without conflicts
- **CR-SQLite Support**: Optional native CRDT support via cr-sqlite extension

## Quick Start

```python
from chora_sync import ChangeTracker, DatabaseMerger, ChangeType

# Create trackers for two databases
tracker_a = ChangeTracker("/path/to/db_a.db", site_id="site-a")
tracker_b = ChangeTracker("/path/to/db_b.db", site_id="site-b")

# Record changes
tracker_a.record_change("feature-voice", ChangeType.INSERT, value='{"name": "Voice"}')
tracker_b.record_change("feature-canvas", ChangeType.INSERT, value='{"name": "Canvas"}')

# Sync databases
merger = DatabaseMerger(tracker_a)
result = merger.sync_with(tracker_b)

print(f"Sent: {result.changes_sent}, Received: {result.changes_received}")
```

## Vector Clocks

Vector clocks track "happened-before" relationships:

```python
from chora_sync import VectorClock

clock_a = VectorClock()
clock_a = clock_a.increment("site-a")  # {site-a: 1}
clock_a = clock_a.increment("site-a")  # {site-a: 2}

clock_b = VectorClock()
clock_b = clock_b.increment("site-b")  # {site-b: 1}

# Compare clocks
print(clock_a < clock_b)           # False
print(clock_a.is_concurrent(clock_b))  # True (neither happened-before)

# Merge clocks
merged = clock_a.merge(clock_b)    # {site-a: 2, site-b: 1}
```

## CR-SQLite (Optional)

For native CRDT support, install the cr-sqlite extension:

```bash
# macOS
brew install nickstenning/tap/crsqlite

# Or download from releases
# https://github.com/vlcn-io/cr-sqlite/releases
```

Then enable it:

```python
from chora_sync import load_crsqlite, is_crsqlite_available
import sqlite3

if is_crsqlite_available():
    conn = sqlite3.connect("my.db")
    load_crsqlite(conn)

    # Enable CRDT on a table
    conn.execute("SELECT crsql_as_crr('entities')")
```

## Architecture

```
┌─────────────────┐          ┌─────────────────┐
│  Database A     │          │  Database B     │
│  (site-a)       │          │  (site-b)       │
│                 │          │                 │
│  ┌───────────┐  │          │  ┌───────────┐  │
│  │ entities  │  │          │  │ entities  │  │
│  └───────────┘  │          │  └───────────┘  │
│                 │          │                 │
│  ┌───────────┐  │◀────────▶│  ┌───────────┐  │
│  │ sync_     │  │  Changes │  │ sync_     │  │
│  │ changes   │  │  + Clock │  │ changes   │  │
│  └───────────┘  │          │  └───────────┘  │
└─────────────────┘          └─────────────────┘
```

## Components

| Module | Purpose |
|--------|---------|
| `extension.py` | CR-SQLite extension loader |
| `clock.py` | Vector clock implementation |
| `changes.py` | Change tracking and recording |
| `merge.py` | Database merge operations |

## Phase 3 Roadmap

- WebSocket-based real-time sync
- Sync server for hub-and-spoke topology
- Encrypted sync (age encryption)
- Conflict resolution UI

## License

MIT
