"""FINDINGS_2 regressions: harness seam routing, Go exhaustion docs, capacity PK, meta redaction, capped next.

Deterministic only. No live model CLIs.
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import core, store  # noqa: E402


class TestClearCapacityPK(unittest.TestCase):
    def test_clear_removes_all_windows_and_orphan_pool_model(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "cap1", {"g": 1}, str(ws), "p1", planner_t3_thread="planner-t3")
        core.record_capacity(sd, "grok-4.6-go", "exhausted", {"class": "GoUsageLimitError"})
        rows = [r for r in core.list_capacity(sd) if r["route"] == "grok-4.6-go"]
        self.assertEqual({r["window"] for r in rows}, {"limit"})
        # Simulate an orphan row sharing pool/model but with a stale route label
        # (PK overwrite left the old label behind in a hypothetical sharer).
        con = store.connect(sd)
        try:
            con.execute(
                "INSERT OR IGNORE INTO capacity(route, state, evidence_json, reset_at,"
                " updated_at, pool, model, window) VALUES(?,?,?,?,?,?,?,?)",
                ("stale-label", "exhausted", "{}", None, core._utcnow(),
                 "go", "opencode-go/grok-4.6", "5h"))
        finally:
            con.close()
        core.record_capacity(sd, "muse-spark-xhigh-free", "degraded",
                             {"source": "test"}, reset_at=core.degraded_until())
        core.clear_capacity(sd, "grok-4.6-go")
        self.assertNotIn("grok-4.6-go", core.exhausted_routes(sd))
        remaining = [dict(r) for r in core.list_capacity(sd)]
        self.assertFalse([r for r in remaining if r["model"] == "opencode-go/grok-4.6"])
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(sd))


if __name__ == "__main__":
    unittest.main()
