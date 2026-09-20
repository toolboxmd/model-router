"""FINDINGS_2 regressions: harness seam routing, Go exhaustion docs, capacity PK, meta redaction, capped next.

Deterministic only. No live model CLIs.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, harnesses, policy, store  # noqa: E402


class TestHarnessRegistryRouting(unittest.TestCase):
    def test_route_uses_owned_server(self):
        self.assertFalse(harnesses.route_uses_owned_server("luna/max"))
        self.assertTrue(harnesses.route_uses_owned_server("luna-go/max"))
        self.assertTrue(harnesses.route_uses_owned_server("muse-spark-xhigh-free"))
        self.assertTrue(harnesses.route_uses_owned_server("grok-4.6-go"))
        self.assertFalse(harnesses.route_uses_owned_server("fable-5.1/max"))
        self.assertFalse(harnesses.route_uses_owned_server("bogus-route"))
        # Registry is the source: owned_server flag decides.
        self.assertTrue(harnesses.harness_named("opencode").owned_server)
        self.assertFalse(harnesses.harness_named("codex").owned_server)
        self.assertFalse(harnesses.harness_named("claude").owned_server)

    def test_controller_no_direct_harness_string_compare(self):
        src = (ROOT / "runner" / "controller.py").read_text()
        self.assertNotIn('["harness"] == "opencode"', src)
        self.assertNotIn(".get(\"harness\") == \"opencode\"", src)
        self.assertIn("route_uses_owned_server", src)

    def _submit(self, base, rid="hr1"):
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir(exist_ok=True)
        core.submit(sd, rid, {"goal": "t"}, str(ws), "planner-1")
        return sd

    def _opencode_success(self, session="ses_route_001"):
        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            body = {"opencode_session_id": session, "ok": True, "rc": 0,
                    "assistant_text": json.dumps({"action": "completion",
                                                  "output": "PLANNED", "artifact": ""}),
                    "finish": "stop",
                    "actual_model": {"providerID": "opencode-go", "modelID": "gpt-5.6-luna"}}
            return 0, "RUNNER_RESULT " + json.dumps(body) + "\n", ""
        return run

    def test_dispatch_fallback_uses_registry(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = self._submit(base)

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            if kind == "codex_dispatch":
                return 1, "", "codex failed to start"
            return self._opencode_success()(cmd, cwd, timeout, kind, meta)

        res = controller.dispatch(sd, "hr1", run_cmd=run)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        job = core.get_job(sd, "hr1")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("dispatch_route"), "luna-go/max")

    def test_resume_luna_uses_registry_for_opencode_route(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = self._submit(base, rid="hr2")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='ses_route_001', status='running'"
                        " WHERE request_id='hr2'")
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='hr2'",
                        (json.dumps({"dispatch_route": "luna-go/max", "seq": 1}),))
        finally:
            con.close()
        res = controller.resume_luna(sd, "hr2", "followup",
                                     run_cmd=self._opencode_success("ses_route_001"))
        self.assertEqual(res["action"], "resumed")
        self.assertEqual(res.get("route"), "luna-go/max")


class TestGoExhaustionDocs(unittest.TestCase):
    def test_only_grok_go_has_next_pool(self):
        self.assertEqual(policy.next_pool_route("grok-4.6-go"), "grok-4.6-xai")
        self.assertEqual(policy.next_pool_route("muse-spark-xhigh-free"), "muse-spark-xhigh-go")
        for route in ("glm-5.3-go", "qwen3.8-flash-go", "minimax-m3-go",
                      "kimi-k3-go", "deepseek-v4-pro-go", "muse-spark-xhigh-go"):
            self.assertIsNone(policy.next_pool_route(route), route)
        # Without a next pool the lane moves to the next family (or ends).
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free"), "glm-5.3-go")
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free", lane="hard"), "kimi-k3-go")

    def test_skill_and_policy_state_declared_pools_only(self):
        rendered = policy.render_skill_table()
        on_disk = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        self.assertEqual(on_disk, rendered)
        self.assertNotIn("for Go routes, that means the xAI pool", rendered)
        self.assertIn("where the route declares one", rendered)
        self.assertIn("free Muse to Go Muse, Go Grok to xAI Grok", rendered)
        self.assertIn("without a next pool it moves to the next model family", rendered)
        # Overload cap wording retains the required cap phrase and clarifies abort.
        self.assertIn("`next` capped at 20 seconds", rendered)
        self.assertIn("aborts instead of waiting", rendered)
        runner_doc = (ROOT / "RUNNER.md").read_text()
        self.assertIn("have no `next_pool`", runner_doc)
        self.assertIn("aborts instead of waiting", runner_doc)


class TestClearCapacityPK(unittest.TestCase):
    def test_clear_removes_all_windows_and_orphan_pool_model(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "cap1", {"g": 1}, str(ws), "p1")
        core.record_capacity(sd, "grok-4.6-go", "exhausted", {"class": "GoUsageLimitError"})
        rows = [r for r in core.list_capacity(sd) if r["route"] == "grok-4.6-go"]
        self.assertEqual({r["window"] for r in rows}, {"5h", "weekly", "monthly"})
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


class TestMetaRedaction(unittest.TestCase):
    def test_redact_meta_masks_free_text_without_truncation(self):
        secret = "hunter2-secret-xyz"
        long_prompt = ("TASK password=" + secret + " ") + ("x" * 8000)
        redacted = core._redact_meta({"prompt": long_prompt, "route": "muse-spark-xhigh-free",
                                      "OPENCODE_SERVER_PASSWORD": "should-vanish"})
        blob = json.dumps(redacted)
        self.assertNotIn(secret, blob)
        self.assertIn("<redacted>", blob)
        self.assertEqual(redacted["OPENCODE_SERVER_PASSWORD"], "<redacted>")
        # No truncation: the supervisor re-reads this prompt.
        self.assertGreater(len(redacted["prompt"]), 8000)
        self.assertIn("route", redacted)

    def test_redact_meta_keeps_structure(self):
        out = core._redact_meta({"prompt": "Bearer abcdefgh12345678 here",
                                 "nested": {"msg": "api_key=supersecretvalue"}})
        blob = json.dumps(out)
        self.assertNotIn("abcdefgh12345678", blob)
        self.assertNotIn("supersecretvalue", blob)


class TestCappedNext(unittest.TestCase):
    def test_capped_retry_next(self):
        h = harnesses.harness_named("opencode")
        raw, capped = h.capped_retry_next({"next": 999})
        self.assertEqual(raw, 999.0)
        self.assertEqual(capped, 20.0)
        raw, capped = h.capped_retry_next({"next": 5})
        self.assertEqual((raw, capped), (5.0, 5.0))
        raw, capped = h.capped_retry_next({})
        self.assertEqual((raw, capped), (0.0, 0.0))

    def test_docs_state_cap_as_abort(self):
        runner_doc = (ROOT / "RUNNER.md").read_text()
        skill = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        self.assertIn("`next` capped at 20 seconds", runner_doc)
        self.assertIn("`next` capped at 20 seconds", skill)


if __name__ == "__main__":
    unittest.main()
