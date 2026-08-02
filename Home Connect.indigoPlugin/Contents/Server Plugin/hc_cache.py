"""JSON disk cache with a 24 h TTL and expired-value fallback.

Program definitions / capability lists are expensive against the 1000 req/day
budget (PRD §3.5), so they are cached to disk. ``get(key, loader)`` returns a
fresh value if one is cached, otherwise calls ``loader``; if the loader raises
it falls back to the stale value (with a warning) rather than failing. The whole
cache is invalidated when the plugin version changes. Stdlib only; never imports
``indigo``.
"""
import json
import logging
import os
import tempfile
import threading
import time

DEFAULT_TTL = 24 * 60 * 60  # 24 hours


class DiskCache:
    """A small JSON-file cache keyed by string, with per-entry timestamps."""

    def __init__(self, path, plugin_version, logger=None, ttl=DEFAULT_TTL, now=time.time):
        self._path = path
        self._version = str(plugin_version)
        self._logger = logger or logging.getLogger("hc_cache")
        self._ttl = ttl
        self._now = now
        self._lock = threading.RLock()
        self._entries = {}
        self._load()

    def _load(self):
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                stored = json.load(handle)
        except (OSError, ValueError):
            stored = None
        if not isinstance(stored, dict) or stored.get("version") != self._version:
            # Missing, unreadable, or a different plugin version -> start clean.
            self._entries = {}
            return
        entries = stored.get("entries")
        self._entries = entries if isinstance(entries, dict) else {}

    def _save(self):
        payload = {"version": self._version, "entries": self._entries}
        directory = os.path.dirname(self._path) or "."
        try:
            os.makedirs(directory, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            os.replace(tmp, self._path)
        except OSError as exc:
            self._logger.warning("Home Connect cache save failed: %s", exc)

    def get(self, key, loader):
        """Return a cached fresh value, else ``loader()``; on loader failure,
        fall back to the stale cached value if one exists."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and (self._now() - entry.get("ts", 0)) < self._ttl:
                return entry.get("value")
            try:
                value = loader()
            except Exception as exc:  # pylint: disable=broad-except
                if entry is not None:
                    self._logger.warning("Home Connect cache: loader for '%s' failed (%s); "
                                         "using stale value", key, exc)
                    return entry.get("value")
                raise
            self._entries[key] = {"ts": self._now(), "value": value}
            self._save()
            return value

    def set(self, key, value):
        with self._lock:
            self._entries[key] = {"ts": self._now(), "value": value}
            self._save()

    def invalidate(self, key):
        with self._lock:
            if key in self._entries:
                del self._entries[key]
                self._save()

    def clear(self):
        with self._lock:
            self._entries = {}
            self._save()
