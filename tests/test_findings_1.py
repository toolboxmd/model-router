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
        root = store.ensure_state_dir(self.sd)
        stdout = root / "outputs" / f"{rid}-{seq}.stdout"
        stderr = root / "outputs" / f"{rid}-{seq}.stderr"
        store.secure_write_text(stdout, "")
        store.secure_write_text(stderr, "")
        con = store.connect(self.sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                "stdout_path,stderr_path,started_at,state,meta_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (f"{rid}-{seq}", rid, "opencode_control", "[]", self.ws("a"), "tok",
                 str(stdout), str(stderr), core._utcnow(), "completed",
                 json.dumps({"route": route, "seq": seq})))
        finally:
            con.close()

    def test_cap_fallback_skips_exhausted_degraded_one_turn_and_lane(self):
        # qwen (30 USD, cap 1) is full via j2; deepseek is the next
        # cap-only successor in the default lane.
        core.submit(self.sd, "j1", {"g": 1}, self.ws("a"), "p", lane="default",
                    route="qwen3.8-flash-go")
        core.submit(self.sd, "j2", {"g": 1}, self.ws("b"), "p", lane="default",
                    route="qwen3.8-flash-go")
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
                    route="muse-spark-xhigh-free")
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
                    route="deepseek-v4.1-flash-go")
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


class FindingsMinorPermissions(unittest.TestCase):
    def test_missing_route_is_read_only_and_default_requires_resolved_set(self):
        from runner import adapters
        for route in (None, "bogus-route"):
            rules = {r["permission"]: r["action"] for r in policy.session_permissions(route)}
            self.assertEqual(rules["external_directory"], "deny")
            self.assertEqual(rules["webfetch"], "deny")
            self.assertEqual(rules["question"], "deny")
            self.assertEqual(rules["task"], "deny")
        full = {r["permission"]: r["action"]
                for r in policy.session_permissions("glm-5.3-flash-go")}
        self.assertEqual(full["external_directory"], "allow")
        seen = {}

        def fake_request(method, path, body):
            base = path.split("?", 1)[0]
            if method == "POST" and base == "/session":
                seen["perm"] = list(body.get("permission") or [])
                return {"id": "ses_000001", "directory": "/tmp"}
            return {}

        client = adapters.OpenCodeClient("http://127.0.0.1:9", "pwd", directory="/tmp",
                                         request_func=fake_request)
        got = client.create_session(title="t")
        self.assertTrue(got["id"].startswith("ses_"))
        dflt = {r["permission"]: r["action"] for r in seen["perm"]}
        self.assertEqual(dflt["external_directory"], "deny")
        self.assertEqual(dflt["question"], "deny")


if __name__ == "__main__":
    unittest.main()
