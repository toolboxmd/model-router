"""Deterministic regressions for FINDINGS_1 (pre-review on HEAD)."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import controller, core, policy, store  # noqa: E402


class FindingsMajor(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name="ws"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def _mark_running(self, rid):
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id=?", (rid,))
        finally:
            con.close()

    def _insert_turn(self, rid, route, seq=0):
        # A worker turn is recorded as its T3 child thread slot.
        job = core.get_job(self.sd, rid)
        st = json.loads(job.get("controller_state") or "{}")
        st.setdefault("t3_threads", {})[f"impl_{seq}"] = {
            "thread_id": f"sub.planner-t3.{rid}{seq}", "route": route}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id=?",
                        (json.dumps(st, sort_keys=True), rid))
        finally:
            con.close()

    def test_cap_fallback_skips_exhausted_degraded_one_turn_and_lane(self):
        # qwen (30 USD, cap 1) is full via j2; deepseek is the next
        # cap-only successor in the default lane.
        core.submit(self.sd, "j1", {"g": 1}, self.ws("a"), "p", lane="default",
                    route="qwen3.8-flash-go", planner_t3_thread="planner-t3")
        core.submit(self.sd, "j2", {"g": 1}, self.ws("b"), "p", lane="default",
                    route="qwen3.8-flash-go", planner_t3_thread="planner-t3")
        for rid in ("j1", "j2"):
            self._mark_running(rid)
        self.assertTrue(core.route_concurrency_full(self.sd, "qwen3.8-flash-go"))
        # Exhausted successor is skipped to hy3-go.
        core.record_capacity(self.sd, "deepseek-v4.1-flash-go", "exhausted", {"source": "test"})
        move = controller._preflight_move(self.sd, "j1", "qwen3.8-flash-go")
        self.assertIsNotNone(move)
        self.assertEqual(move["route"], "hy3-go")
        self.assertIn("concurrent", move["reason"])
        # Degraded successor is skipped the same way.
        core.clear_capacity(self.sd, "deepseek-v4.1-flash-go")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route=?, lane=? WHERE request_id=?",
                        ("qwen3.8-flash-go", "implementation_default", "j1"))
        finally:
            con.close()
        core.record_capacity(self.sd, "deepseek-v4.1-flash-go", "degraded", {"source": "test"})
        move = controller._preflight_move(self.sd, "j1", "qwen3.8-flash-go")
        self.assertEqual(move["route"], "hy3-go")
        # One-turn successor already used is skipped the same way.
        core.clear_capacity(self.sd, "deepseek-v4.1-flash-go")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route=?, lane=? WHERE request_id=?",
                        ("qwen3.8-flash-go", "implementation_default", "j1"))
        finally:
            con.close()
        self._insert_turn("j1", "deepseek-v4.1-flash-go", seq=0)
        move = controller._preflight_move(self.sd, "j1", "qwen3.8-flash-go")
        self.assertEqual(move["route"], "hy3-go")
        # In-transaction switch fallback also skips an exhausted successor.
        core.submit(self.sd, "k1", {"g": 1}, self.ws("k1"), "p", lane="default",
                    route="muse-spark-xhigh-free", planner_t3_thread="planner-t3")
        self._mark_running("k1")
        core.record_capacity(self.sd, "deepseek-v4.1-flash-go", "exhausted", {"source": "test"})
        # k1 targets qwen (full); the atomic fallback must not land on the
        # exhausted deepseek rung.
        res = controller._switch_route(self.sd, "k1", "qwen3.8-flash-go", "test",
                                       {"source": "test"})
        self.assertEqual(res["route"], "hy3-go")
        core.clear_capacity(self.sd, "deepseek-v4.1-flash-go")
        # Lane membership: a hard-only route never resolves inside the
        # default lane; the fallback returns None instead of jumping lanes.
        self.assertIsNone(core.next_capable_route(
            self.sd, "glm-5.3-go", "implementation_default", exclude="k1"))
        # Cap is still enforced: a full capped successor is skipped.
        core.submit(self.sd, "j3", {"g": 1}, self.ws("c"), "p", lane="default",
                    route="deepseek-v4.1-flash-go", planner_t3_thread="planner-t3")
        self._mark_running("j3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route=?, lane=? WHERE request_id=?",
                        ("qwen3.8-flash-go", "implementation_default", "j1"))
            con.execute("DELETE FROM invocations WHERE request_id='j1'")
        finally:
            con.close()
        move = controller._preflight_move(self.sd, "j1", "qwen3.8-flash-go")
        self.assertEqual(move["route"], "hy3-go")


class FindingsMinorDeepseekNote(unittest.TestCase):
    def test_tier_note_renders_dollars_not_date(self):
        note = policy.ROUTES["deepseek-v4.1-flash-go"]["note"]
        self.assertEqual(note, "$15 USD tier from 2026-09-20")
        self.assertNotIn("$2026", note)
        self.assertEqual(policy.monthly_limit_usd("deepseek-v4.1-flash-go"), 15)


class FindingsMinorBoolCap(unittest.TestCase):
    def test_bool_cap_rejected_and_not_masked(self):
        orig = dict(policy.ROUTES["qwen3.8-flash-go"])
        try:
            policy.ROUTES["qwen3.8-flash-go"]["max_concurrent"] = True
            problems = policy.validate_policy()
            self.assertTrue(any("qwen3.8-flash-go" in p and "max_concurrent must be 1" in p
                                for p in problems), problems)
            self.assertIsNone(policy.route_max_concurrent("qwen3.8-flash-go"))
        finally:
            policy.ROUTES["qwen3.8-flash-go"].clear()
            policy.ROUTES["qwen3.8-flash-go"].update(orig)
        self.assertEqual(policy.validate_policy(), [])


if __name__ == "__main__":
    unittest.main()
