"""Unit tests for hc_cache.py — TTL, stale fallback, version invalidation."""
import os
from unittest.mock import Mock

import pytest

from hc_cache import DiskCache


class FakeClock:
    def __init__(self, start=0.0):
        self.t = start

    def now(self):
        return self.t


def make_cache(tmp_path, version="2026.0.2", ttl=100, clock=None):
    clock = clock or FakeClock()
    path = os.path.join(str(tmp_path), "cache.json")
    return DiskCache(path, version, logger=Mock(), ttl=ttl, now=clock.now), clock


def test_fresh_value_served_without_reloading(tmp_path):
    cache, _ = make_cache(tmp_path)
    loader = Mock(return_value={"programs": [1, 2]})
    assert cache.get("progs", loader) == {"programs": [1, 2]}
    assert cache.get("progs", loader) == {"programs": [1, 2]}
    loader.assert_called_once()          # second get hit the cache


def test_value_reloaded_after_ttl(tmp_path):
    cache, clock = make_cache(tmp_path, ttl=100)
    loader = Mock(side_effect=[{"v": 1}, {"v": 2}])
    assert cache.get("k", loader) == {"v": 1}
    clock.t = 150                        # past the TTL
    assert cache.get("k", loader) == {"v": 2}
    assert loader.call_count == 2


def test_stale_fallback_when_loader_fails(tmp_path):
    cache, clock = make_cache(tmp_path, ttl=100)
    good = Mock(return_value={"v": "cached"})
    assert cache.get("k", good) == {"v": "cached"}
    clock.t = 200                        # expired

    def boom():
        raise RuntimeError("cloud outage")

    assert cache.get("k", boom) == {"v": "cached"}   # stale value returned
    cache._logger.warning.assert_called()            # pylint: disable=protected-access


def test_loader_failure_without_cache_raises(tmp_path):
    cache, _ = make_cache(tmp_path)

    def boom():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        cache.get("missing", boom)


def test_version_change_invalidates_cache(tmp_path):
    path = os.path.join(str(tmp_path), "cache.json")
    first = DiskCache(path, "2026.0.1", logger=Mock(), ttl=1000)
    first.set("k", {"v": 1})

    # A new plugin version pointed at the same file starts empty.
    second = DiskCache(path, "2026.0.2", logger=Mock(), ttl=1000)
    loader = Mock(return_value={"v": 2})
    assert second.get("k", loader) == {"v": 2}
    loader.assert_called_once()


def test_persisted_value_survives_reopen(tmp_path):
    path = os.path.join(str(tmp_path), "cache.json")
    first = DiskCache(path, "2026.0.2", logger=Mock(), ttl=1000)
    first.set("k", {"v": 42})
    second = DiskCache(path, "2026.0.2", logger=Mock(), ttl=1000)
    loader = Mock(return_value={"v": 0})
    assert second.get("k", loader) == {"v": 42}
    loader.assert_not_called()


def test_invalidate_removes_entry(tmp_path):
    cache, _ = make_cache(tmp_path)
    cache.set("k", {"v": 1})
    cache.invalidate("k")
    loader = Mock(return_value={"v": 2})
    assert cache.get("k", loader) == {"v": 2}
    loader.assert_called_once()
