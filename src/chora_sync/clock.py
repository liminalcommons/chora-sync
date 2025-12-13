"""
Vector clock implementation for sync ordering.

Vector clocks track causality between events in distributed systems,
allowing us to determine if events happened-before, happened-after,
or are concurrent.
"""

import json
from dataclasses import dataclass, field
from typing import Dict, Optional
from copy import deepcopy


@dataclass
class VectorClock:
    """
    A vector clock for tracking causality.

    Each site in the sync network has its own counter in the clock.
    Comparing clocks lets us determine ordering:
    - clock1 < clock2: clock1 happened-before clock2
    - clock1 > clock2: clock2 happened-before clock1
    - clock1 || clock2: concurrent (neither happened-before)
    """

    counters: Dict[str, int] = field(default_factory=dict)

    def increment(self, site_id: str) -> "VectorClock":
        """
        Increment the clock for a site (after local event).

        Args:
            site_id: The site ID that generated an event

        Returns:
            New VectorClock with incremented counter
        """
        new_counters = deepcopy(self.counters)
        new_counters[site_id] = new_counters.get(site_id, 0) + 1
        return VectorClock(counters=new_counters)

    def merge(self, other: "VectorClock") -> "VectorClock":
        """
        Merge with another clock (take max of all counters).

        Args:
            other: Another VectorClock to merge with

        Returns:
            New VectorClock with merged counters
        """
        all_sites = set(self.counters.keys()) | set(other.counters.keys())
        new_counters = {
            site: max(self.counters.get(site, 0), other.counters.get(site, 0))
            for site in all_sites
        }
        return VectorClock(counters=new_counters)

    def compare(self, other: "VectorClock") -> Optional[int]:
        """
        Compare two vector clocks.

        Args:
            other: Another VectorClock to compare

        Returns:
            -1 if self happened-before other
             1 if other happened-before self
             0 if equal
             None if concurrent (no causal ordering)
        """
        all_sites = set(self.counters.keys()) | set(other.counters.keys())

        less_or_equal = True
        greater_or_equal = True

        for site in all_sites:
            self_val = self.counters.get(site, 0)
            other_val = other.counters.get(site, 0)

            if self_val > other_val:
                less_or_equal = False
            if self_val < other_val:
                greater_or_equal = False

        if less_or_equal and greater_or_equal:
            return 0  # Equal
        elif less_or_equal:
            return -1  # self happened-before other
        elif greater_or_equal:
            return 1  # other happened-before self
        else:
            return None  # Concurrent

    def __lt__(self, other: "VectorClock") -> bool:
        """Return True if self happened-before other."""
        return self.compare(other) == -1

    def __gt__(self, other: "VectorClock") -> bool:
        """Return True if other happened-before self."""
        return self.compare(other) == 1

    def __eq__(self, other: object) -> bool:
        """Return True if clocks are equal."""
        if not isinstance(other, VectorClock):
            return False
        return self.compare(other) == 0

    def __le__(self, other: "VectorClock") -> bool:
        """Return True if self happened-before-or-equal other."""
        cmp = self.compare(other)
        return cmp in (-1, 0)

    def __ge__(self, other: "VectorClock") -> bool:
        """Return True if other happened-before-or-equal self."""
        cmp = self.compare(other)
        return cmp in (1, 0)

    def is_concurrent(self, other: "VectorClock") -> bool:
        """Return True if clocks are concurrent (neither happened-before)."""
        return self.compare(other) is None

    def get(self, site_id: str) -> int:
        """Get counter for a site."""
        return self.counters.get(site_id, 0)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.counters)

    @classmethod
    def from_json(cls, json_str: str) -> "VectorClock":
        """Deserialize from JSON string."""
        return cls(counters=json.loads(json_str))

    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary."""
        return deepcopy(self.counters)

    @classmethod
    def from_dict(cls, d: Dict[str, int]) -> "VectorClock":
        """Create from dictionary."""
        return cls(counters=deepcopy(d))
