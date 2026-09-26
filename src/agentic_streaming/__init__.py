"""Agentic Streaming Analytics Framework Package.

Autonomous anomaly detection and agentic orchestration for streaming cloud metrics.
"""

__version__ = "0.1.0"
import sys

# Windows compatibility patch for kafka-python / selectors
if sys.platform == "win32":
    import selectors

    if hasattr(selectors, "SelectSelector") and not getattr(selectors.SelectSelector, "_is_patched_for_windows", False):
        def _safe_select_unregister(self, fileobj):
            try:
                key = super(selectors.SelectSelector, self).unregister(fileobj)
            except (KeyError, ValueError):
                return None
            if key is not None:
                self._readers.discard(key.fd)
                self._writers.discard(key.fd)
            return key

        selectors.SelectSelector.unregister = _safe_select_unregister
        selectors.SelectSelector._is_patched_for_windows = True
