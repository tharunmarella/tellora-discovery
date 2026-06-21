"""Size-capped TTL cache for always-on worker processes."""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Generic, TypeVar

T = TypeVar("T")


class TTLLRUCache(Generic[T]):
    """LRU cache with per-entry TTL and a hard size cap."""

    def __init__(self, maxsize: int = 5000, ttl: float = 3600.0) -> None:
        self.maxsize = maxsize
        self.ttl = ttl
        self._data: OrderedDict[str, tuple[float, T]] = OrderedDict()

    def get(self, key: str) -> T | None:
        item = self._data.get(key)
        if item is None:
            return None
        ts, val = item
        if time.time() - ts >= self.ttl:
            del self._data[key]
            return None
        self._data.move_to_end(key)
        return val

    def set(self, key: str, val: T) -> None:
        if key in self._data:
            del self._data[key]
        self._data[key] = (time.time(), val)
        while len(self._data) > self.maxsize:
            self._data.popitem(last=False)

    def delete(self, key: str) -> None:
        self._data.pop(key, None)
