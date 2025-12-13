"""
Tests for vector clock implementation.
"""

import pytest
import json

from chora_sync.clock import VectorClock


class TestVectorClockBasics:
    """Basic VectorClock functionality tests."""

    def test_empty_clock(self):
        """Empty clock has no counters."""
        clock = VectorClock()
        assert clock.counters == {}
        assert clock.get("site-a") == 0

    def test_increment_creates_counter(self):
        """Incrementing creates a counter for the site."""
        clock = VectorClock()
        new_clock = clock.increment("site-a")
        assert new_clock.get("site-a") == 1
        # Original unchanged (immutable)
        assert clock.get("site-a") == 0

    def test_increment_is_immutable(self):
        """Incrementing returns new clock, doesn't modify original."""
        clock = VectorClock(counters={"site-a": 5})
        new_clock = clock.increment("site-a")
        assert new_clock.get("site-a") == 6
        assert clock.get("site-a") == 5

    def test_multiple_increments(self):
        """Multiple increments accumulate."""
        clock = VectorClock()
        clock = clock.increment("site-a")
        clock = clock.increment("site-a")
        clock = clock.increment("site-a")
        assert clock.get("site-a") == 3

    def test_increments_different_sites(self):
        """Different sites have independent counters."""
        clock = VectorClock()
        clock = clock.increment("site-a")
        clock = clock.increment("site-b")
        clock = clock.increment("site-a")
        assert clock.get("site-a") == 2
        assert clock.get("site-b") == 1


class TestVectorClockMerge:
    """Tests for clock merge operations."""

    def test_merge_empty_clocks(self):
        """Merging empty clocks produces empty clock."""
        clock1 = VectorClock()
        clock2 = VectorClock()
        merged = clock1.merge(clock2)
        assert merged.counters == {}

    def test_merge_with_empty(self):
        """Merging with empty clock preserves values."""
        clock1 = VectorClock(counters={"site-a": 3})
        clock2 = VectorClock()
        merged = clock1.merge(clock2)
        assert merged.get("site-a") == 3

    def test_merge_takes_max(self):
        """Merge takes maximum of each counter."""
        clock1 = VectorClock(counters={"site-a": 3, "site-b": 5})
        clock2 = VectorClock(counters={"site-a": 7, "site-b": 2})
        merged = clock1.merge(clock2)
        assert merged.get("site-a") == 7
        assert merged.get("site-b") == 5

    def test_merge_combines_sites(self):
        """Merge includes all sites from both clocks."""
        clock1 = VectorClock(counters={"site-a": 3})
        clock2 = VectorClock(counters={"site-b": 5})
        merged = clock1.merge(clock2)
        assert merged.get("site-a") == 3
        assert merged.get("site-b") == 5

    def test_merge_is_commutative(self):
        """a.merge(b) == b.merge(a)."""
        clock1 = VectorClock(counters={"site-a": 3, "site-b": 5})
        clock2 = VectorClock(counters={"site-a": 7, "site-c": 2})
        merged1 = clock1.merge(clock2)
        merged2 = clock2.merge(clock1)
        assert merged1 == merged2

    def test_merge_is_associative(self):
        """(a.merge(b)).merge(c) == a.merge(b.merge(c))."""
        clock1 = VectorClock(counters={"site-a": 1})
        clock2 = VectorClock(counters={"site-b": 2})
        clock3 = VectorClock(counters={"site-c": 3})
        merged1 = clock1.merge(clock2).merge(clock3)
        merged2 = clock1.merge(clock2.merge(clock3))
        assert merged1 == merged2


class TestVectorClockCompare:
    """Tests for clock comparison operations."""

    def test_equal_empty_clocks(self):
        """Empty clocks are equal."""
        clock1 = VectorClock()
        clock2 = VectorClock()
        assert clock1.compare(clock2) == 0
        assert clock1 == clock2

    def test_equal_clocks(self):
        """Clocks with same counters are equal."""
        clock1 = VectorClock(counters={"site-a": 3, "site-b": 5})
        clock2 = VectorClock(counters={"site-a": 3, "site-b": 5})
        assert clock1.compare(clock2) == 0
        assert clock1 == clock2

    def test_happened_before(self):
        """clock1 < clock2 when all counters <= and at least one <."""
        clock1 = VectorClock(counters={"site-a": 3})
        clock2 = VectorClock(counters={"site-a": 5})
        assert clock1.compare(clock2) == -1
        assert clock1 < clock2
        assert clock1 <= clock2
        assert not clock1 > clock2
        assert not clock1 >= clock2

    def test_happened_after(self):
        """clock1 > clock2 when all counters >= and at least one >."""
        clock1 = VectorClock(counters={"site-a": 5})
        clock2 = VectorClock(counters={"site-a": 3})
        assert clock1.compare(clock2) == 1
        assert clock1 > clock2
        assert clock1 >= clock2
        assert not clock1 < clock2
        assert not clock1 <= clock2

    def test_concurrent(self):
        """Clocks are concurrent when neither happened-before."""
        clock1 = VectorClock(counters={"site-a": 3, "site-b": 1})
        clock2 = VectorClock(counters={"site-a": 1, "site-b": 3})
        assert clock1.compare(clock2) is None
        assert clock1.is_concurrent(clock2)
        # Note: can't use < or > with concurrent clocks meaningfully
        assert not clock1 < clock2
        assert not clock1 > clock2

    def test_missing_site_treated_as_zero(self):
        """Missing sites are treated as 0."""
        clock1 = VectorClock(counters={"site-a": 3})
        clock2 = VectorClock(counters={"site-a": 3, "site-b": 1})
        assert clock1.compare(clock2) == -1
        assert clock1 < clock2


class TestVectorClockSerialization:
    """Tests for clock serialization."""

    def test_to_json(self):
        """Serialize to JSON string."""
        clock = VectorClock(counters={"site-a": 3, "site-b": 5})
        json_str = clock.to_json()
        data = json.loads(json_str)
        assert data == {"site-a": 3, "site-b": 5}

    def test_from_json(self):
        """Deserialize from JSON string."""
        json_str = '{"site-a": 3, "site-b": 5}'
        clock = VectorClock.from_json(json_str)
        assert clock.get("site-a") == 3
        assert clock.get("site-b") == 5

    def test_roundtrip_json(self):
        """JSON serialization round-trips correctly."""
        original = VectorClock(counters={"site-a": 3, "site-b": 5})
        restored = VectorClock.from_json(original.to_json())
        assert original == restored

    def test_to_dict(self):
        """Convert to dictionary."""
        clock = VectorClock(counters={"site-a": 3})
        d = clock.to_dict()
        assert d == {"site-a": 3}
        # Should be a copy
        d["site-a"] = 100
        assert clock.get("site-a") == 3

    def test_from_dict(self):
        """Create from dictionary."""
        d = {"site-a": 3, "site-b": 5}
        clock = VectorClock.from_dict(d)
        assert clock.get("site-a") == 3
        # Should be a copy
        d["site-a"] = 100
        assert clock.get("site-a") == 3


class TestVectorClockEdgeCases:
    """Edge case tests."""

    def test_empty_site_id(self):
        """Empty string site ID is valid."""
        clock = VectorClock().increment("")
        assert clock.get("") == 1

    def test_unicode_site_id(self):
        """Unicode site IDs work."""
        clock = VectorClock().increment("站点-α")
        assert clock.get("站点-α") == 1

    def test_large_counter_values(self):
        """Large counter values handled correctly."""
        clock = VectorClock(counters={"site-a": 10**18})
        assert clock.get("site-a") == 10**18

    def test_many_sites(self):
        """Many sites handled correctly."""
        counters = {f"site-{i}": i for i in range(100)}
        clock = VectorClock(counters=counters)
        for i in range(100):
            assert clock.get(f"site-{i}") == i
