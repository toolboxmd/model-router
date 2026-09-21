"""Issue #46: Codex usage-limit message is exhaustion with its reset time.

Deterministic only. No live model CLIs. Uses CLI-shaped Codex JSON event
streams through injected run_cmd stubs plus throwaway state dirs/homes,
covering: usage-limit message and UsageLimitExceeded / RateLimitExceeded
error items with resets_at classify exhausted with the carried reset; the
dispatch marks the Codex pool exhausted and falls back to Luna on OpenCode
Go in the same step with the route reason recorded; later jobs skip Codex
in preflight until the reset; unrecognized dispatch failures block with
the last provider message; the rate-limit probe reads
account/rateLimits/read through the app-server and details the raw shape
when no rate record arrives; a public-path drill moves Codex to Luna Go
and completes.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, harnesses, policy  # noqa: E402

LIVE_USAGE_MESSAGE = (
    "You've hit your usage limit. Visit "
    "https://chatgpt.com/codex/settings/usage to purchase more credits "
    "or try again at Sep 22nd, 2026 9:51 AM."
)
LIVE_RESET_ISO = "2026-09-22T09:51:00+00:00"


def fresh_state(testcase, request_id="e46-1"):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    testcase.addCleanup(tmp.cleanup)
    base = Path(tmp.name)
    sd = str(base / "state")
    ws = base / "ws"
    ws.mkdir()
    core.submit(sd, request_id, {"goal": "exh46"}, str(ws), "planner-sess")
    return sd, base, ws


def isolate_homes(testcase):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    testcase.addCleanup(tmp.cleanup)
    base = Path(tmp.name)
    homes = {}
    for key, dirname in (("MODEL_ROUTER_OPENCODE_HOME", "oc"),
                         ("MODEL_ROUTER_CODEX_HOME", "cx"),
                         ("MODEL_ROUTER_CLAUDE_HOME", "cl"),
                         ("MODEL_ROUTER_GROK_HOME", "gr"),
                         ("MODEL_ROUTER_AGENTS_HOME", "ag")):
        d = base / dirname
        d.mkdir(parents=True, exist_ok=True)
        homes[key] = str(d)
    oc = Path(homes["MODEL_ROUTER_OPENCODE_HOME"])
    (oc / "skills" / "operations").mkdir(parents=True, exist_ok=True)
    (oc / "skills" / "project-direction").mkdir(parents=True, exist_ok=True)
    (oc / "skills" / "operations" / "SKILL.md").write_text("# operations\n")
    (oc / "skills" / "project-direction" / "SKILL.md").write_text("# pd\n")
    (oc / "plugins").mkdir(parents=True, exist_ok=True)
    (oc / "plugins" / "agentsmd-project-direction.js").write_text("// p\n")
    (oc / "opencode.json").write_text(json.dumps({"mcp": {}}))
    (oc / "AGENTS.md").write_text("# AGENTS.md\n")
    saved = {k: os.environ.get(k) for k in homes}
    saved_codex = os.environ.get("CODEX_HOME")
    saved_grok = os.environ.get("GROK_HOME")
    os.environ.update(homes)
    os.environ.pop("CODEX_HOME", None)
    os.environ.pop("GROK_HOME", None)

    def _restore():
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        if saved_codex is None:
            os.environ.pop("CODEX_HOME", None)
        else:
            os.environ["CODEX_HOME"] = saved_codex
        if saved_grok is None:
            os.environ.pop("GROK_HOME", None)
        else:
            os.environ["GROK_HOME"] = saved_grok
    testcase.addCleanup(_restore)
    return base, homes


def codex_stream(*objs):
    return "\n".join(json.dumps(o) for o in objs) + "\n"


def usage_limit_dispatch(thread="thr-exh-001"):
    return codex_stream(
        {"type": "thread.started", "thread_id": thread},
        {"type": "item.completed",
         "item": {"type": "agent_message", "text": LIVE_USAGE_MESSAGE}},
    )


def opencode_completion(session="ses_exh_001"):
    def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
        body = {"opencode_session_id": session, "ok": True, "rc": 0,
                "assistant_text": json.dumps({"action": "completion",
                                              "output": "DONE", "artifact": ""}),
                "finish": "stop",
                "actual_model": {"providerID": "opencode-go",
                                 "modelID": "gpt-5.6-luna"}}
        return 0, "RUNNER_RESULT " + json.dumps(body) + "\n", ""
    return run


def combined_run(codex_out, codex_rc=1, session="ses_exh_001"):
    oc = opencode_completion(session)

    def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
        if kind == "codex_dispatch":
            return codex_rc, codex_out, ""
        return oc(cmd, cwd, timeout, kind, meta)
    return run


class TestUsageLimitClassification(unittest.TestCase):
    def test_message_with_reset_time_is_exhausted(self):
        h = harnesses.harness_named("codex")
        out = usage_limit_dispatch()
        evidence = h.usage_limit_evidence(out, "", None)
        self.assertIsInstance(evidence, dict)
        self.assertEqual(evidence["message"], LIVE_USAGE_MESSAGE)
        signal, found = h.dispatch_limit_signal(out, "", None, 1)
        self.assertEqual(signal, "exhausted")
        self.assertEqual(found["message"], LIVE_USAGE_MESSAGE)
        self.assertEqual(policy.parse_provider_reset(found), LIVE_RESET_ISO)

    def test_error_item_usagelimit_with_resets_at(self):
        h = harnesses.harness_named("codex")
        out = codex_stream(
            {"type": "thread.started", "thread_id": "thr-exh-002"},
            {"type": "item.completed",
             "item": {"type": "error", "code": "UsageLimitExceeded",
                      "message": "quota hit",
                      "resets_at": "2026-09-22T09:51:00Z"}},
        )
        self.assertEqual(policy.classify_signal({"name": "UsageLimitExceeded"}),
                         "exhausted")
        signal, evidence = h.dispatch_limit_signal(out, "", None, 1)
        self.assertEqual(signal, "exhausted")
        self.assertEqual(evidence["code"], "UsageLimitExceeded")
        self.assertEqual(policy.parse_provider_reset(evidence),
                         "2026-09-22T09:51:00+00:00")

    def test_error_item_ratelimit_with_resets_at(self):
        h = harnesses.harness_named("codex")
        out = codex_stream(
            {"type": "thread.started", "thread_id": "thr-exh-003"},
            {"type": "item.completed",
             "item": {"type": "error", "code": "RateLimitExceeded",
                      "message": "slow down",
                      "resets_at": "2026-09-22T09:51:00Z"}},
        )
        signal, evidence = h.dispatch_limit_signal(out, "", None, 1)
        self.assertEqual(signal, "exhausted")
        self.assertEqual(evidence["code"], "RateLimitExceeded")
        self.assertEqual(policy.parse_provider_reset(evidence), LIVE_RESET_ISO)

    def test_bare_mention_is_not_a_limit(self):
        h = harnesses.harness_named("codex")
        out = codex_stream(
            {"type": "thread.started", "thread_id": "thr-exh-004"},
            {"type": "item.completed",
             "item": {"type": "agent_message",
                      "text": "we discussed the usage limit policy"}},
        )
        self.assertIsNone(h.usage_limit_evidence(out, "", None))
        signal, evidence = h.dispatch_limit_signal(out, "", None, 1)
        self.assertIsNone(signal)
        self.assertIsNone(evidence)

    def test_fake_codex_cli_usage_limit_stream(self):
        """A fake `codex` executable emitting the live refusal shape."""
        import subprocess
        import tempfile
        from tests.fakes import VERSION_GUARD, write_fake
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        bindir = Path(tmp.name) / "bin"
        bindir.mkdir()
        body = (
            "import json, sys\n"
            "print(json.dumps("
            '{"type": "thread.started", "thread_id": "thr-fake-001"}))\n'
            "print(json.dumps({"
            '"type": "item.completed", "item": {"type": "agent_message", '
            '"text": ' + json.dumps(LIVE_USAGE_MESSAGE) + "}}))\n"
            "sys.exit(1)\n"
        )
        write_fake(bindir, "codex", body, sys.executable)
        proc = subprocess.run(
            [str(bindir / "codex"), "exec", "--json", "--model", "x"],
            capture_output=True, text=True, timeout=30,
            stdin=subprocess.DEVNULL)
        self.assertEqual(proc.returncode, 1)
        h = harnesses.harness_named("codex")
        signal, evidence = h.dispatch_limit_signal(
            proc.stdout, proc.stderr, None, proc.returncode)
        self.assertEqual(signal, "exhausted")
        self.assertEqual(evidence["message"], LIVE_USAGE_MESSAGE)
        self.assertEqual(policy.parse_provider_reset(evidence), LIVE_RESET_ISO)

    def test_human_reset_parsing(self):
        import time
        now = 1789900000.0
        self.assertEqual(
            policy._human_reset_in_text(
                "or try again at Sep 22nd, 2026 9:51 AM.", now),
            LIVE_RESET_ISO)
        self.assertIsNone(policy._human_reset_in_text("no date here", now))
        self.assertIsNone(policy._human_reset_in_text(
            "Sep 22nd, 2026 9:51 AM", now + 10 * 86400))
        _ = time.time


class TestDispatchExhaustionFallback(unittest.TestCase):
    def test_usage_limit_falls_back_and_marks_capacity(self):
        isolate_homes(self)
        sd, _base, _ws = fresh_state(self, "e46-d1")
        calls = []

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            calls.append(kind)
            return combined_run(usage_limit_dispatch())(
                cmd, cwd, timeout, kind, meta)

        res = controller.dispatch(sd, "e46-d1", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        # Codex ran exactly once: no retry of the exhausted pool.
        self.assertEqual(calls.count("codex_dispatch"), 1)
        job = core.get_job(sd, "e46-d1")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("dispatch_route"), "luna-go/max")
        self.assertEqual(st.get("route_reason"), "dispatch_exhausted")
        self.assertIn("luna/max", core.exhausted_routes(sd))
        marks = [r for r in core.list_capacity(sd)
                 if r["route"] == "luna/max" and r["state"] == "exhausted"]
        self.assertTrue(marks)
        self.assertEqual(marks[0]["reset_at"], LIVE_RESET_ISO)
        self.assertEqual(marks[0]["reset_source"], "provider")
        self.assertIn(LIVE_USAGE_MESSAGE[:60],
                      marks[0]["evidence_json"])

    def test_error_item_without_thread_also_falls_back(self):
        isolate_homes(self)
        sd, _base, _ws = fresh_state(self, "e46-d2")
        out = codex_stream(
            {"type": "item.completed",
             "item": {"type": "error", "code": "UsageLimitExceeded",
                      "message": "quota hit",
                      "resets_at": "2026-09-22T09:51:00Z"}},
        )
        res = controller.dispatch(sd, "e46-d2",
                                  run_cmd=combined_run(out),
                                  probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        self.assertIn("luna/max", core.exhausted_routes(sd))
        job = core.get_job(sd, "e46-d2")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("route_reason"), "dispatch_exhausted")

    def test_preflight_skips_codex_on_shared_ledger(self):
        isolate_homes(self)
        sd, _base, ws = fresh_state(self, "e46-d5")
        res = controller.dispatch(
            sd, "e46-d5",
            run_cmd=combined_run(usage_limit_dispatch()),
            probe=lambda *a: None)
        self.assertEqual(res.get("route"), "luna-go/max")
        done = controller.step(sd, "e46-d5",
                               run_cmd=combined_run(usage_limit_dispatch()))
        self.assertEqual(done["action"], "completed")
        core.submit(sd, "e46-d6", {"goal": "exh46"}, str(ws), "planner-sess")
        seen = []

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            seen.append(kind)
            if kind == "codex_dispatch":
                raise AssertionError("Codex must be skipped in preflight")
            return opencode_completion("ses_exh_006")(
                cmd, cwd, timeout, kind, meta)

        res2 = controller.dispatch(sd, "e46-d6", run_cmd=run,
                                   probe=lambda *a: None)
        self.assertEqual(res2["action"], "dispatched")
        self.assertEqual(res2.get("route"), "luna-go/max")
        self.assertNotIn("codex_dispatch", seen)
        job = core.get_job(sd, "e46-d6")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("route_reason"), "preflight_exhausted")

    def test_unrecognized_failure_keeps_provider_message(self):
        isolate_homes(self)
        sd, _base, _ws = fresh_state(self, "e46-d7")
        out = (codex_stream(
            {"type": "thread.started", "thread_id": "thr-exh-007"},
            {"type": "item.completed",
             "item": {"type": "agent_message",
                      "text": "codex backend exploded: kaput-123"}}))

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            return 1, out, "backend exploded: kaput-123\n"

        res = controller.dispatch(sd, "e46-d7", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["reason"], "codex_dispatch_failed")
        job = core.get_job(sd, "e46-d7")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("kaput-123", job["block_reason"])
        self.assertNotIn("turn not completed", job["block_reason"])


class TestCodexProbeAppServer(unittest.TestCase):
    def _payload(self):
        return {"rateLimits": {
            "limitId": "codex",
            "primary": {"usedPercent": 12.0, "windowDurationMins": 300,
                        "resetsAt": "2026-09-21T03:00:00Z"},
            "secondary": {"usedPercent": 61.0, "windowDurationMins": 10080,
                          "resetsAt": "2026-09-22T09:51:00Z"},
            "planType": "plus"},
            "rateLimitsByLimitId": {}}

    def test_app_server_payload_stores_windows(self):
        isolate_homes(self)
        sd, _base, _ws = fresh_state(self, "e46-p1")
        out = controller._default_codex_probe(
            sd, "e46-p1", "luna/max", query_fn=self._payload)
        self.assertEqual(out["action"], "probe-ok")
        self.assertEqual(out["windows"], ["5h", "weekly"])
        readings = core.list_readings(sd, pool="codex")
        by_window = {r["window"]: r for r in readings}
        self.assertEqual(by_window["5h"]["used"], 12.0)
        self.assertEqual(by_window["weekly"]["used"], 61.0)
        self.assertEqual(by_window["5h"]["source"], "provider_reported")
        self.assertEqual(by_window["5h"]["reset_at"],
                         "2026-09-21T03:00:00+00:00")
        self.assertEqual(by_window["weekly"]["reset_at"], LIVE_RESET_ISO)

    def test_result_wrapper_unwraps(self):
        h = harnesses.harness_named("codex")
        import datetime
        observed = datetime.datetime.now(datetime.timezone.utc).isoformat()
        readings = h.probe_rate_limits({"id": 1, "result": self._payload()},
                                       observed)
        self.assertEqual({r["window"] for r in readings}, {"5h", "weekly"})

    def test_no_rate_record_details_shape(self):
        isolate_homes(self)
        sd, _base, _ws = fresh_state(self, "e46-p2")
        payload = {"planType": "pro", "credits": {"balance": "0"}}
        out = controller._default_codex_probe(
            sd, "e46-p2", "luna/max", query_fn=lambda: payload)
        self.assertEqual(out["action"], "probe-unknown")
        readings = core.list_readings(sd, pool="codex")
        unknown = [r for r in readings if r["used"] is None]
        self.assertTrue(unknown)
        detail = json.loads(unknown[0]["detail_json"] or "{}")
        self.assertIn("no rate record", json.dumps(detail))
        shape = detail.get("shape") or []
        self.assertIn("planType", shape)
        self.assertIn("credits", shape)
        self.assertNotIn("0", json.dumps(shape))

    def test_query_failure_records_cause(self):
        isolate_homes(self)
        sd, _base, _ws = fresh_state(self, "e46-p3")

        def boom():
            raise RuntimeError("app-server exploded")
        out = controller._default_codex_probe(sd, "e46-p3", "luna/max",
                                              query_fn=boom)
        self.assertEqual(out["action"], "probe-unknown")
        readings = core.list_readings(sd, pool="codex")
        unknown = [r for r in readings if r["used"] is None]
        self.assertTrue(unknown)
        detail = json.loads(unknown[0]["detail_json"] or "{}")
        self.assertIn("RuntimeError", json.dumps(detail))


class TestPublicDrill(unittest.TestCase):
    def test_codex_to_luna_go_and_complete(self):
        """Public path: submit, dispatch (Codex limit -> Luna Go), step."""
        isolate_homes(self)
        sd, _base, _ws = fresh_state(self, "e46-pub-1")
        run = combined_run(usage_limit_dispatch("thr-pub-001"),
                           session="ses_pub_001")
        res = controller.dispatch(sd, "e46-pub-1", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        self.assertIn("luna/max", core.exhausted_routes(sd))
        done = controller.step(sd, "e46-pub-1", run_cmd=run)
        self.assertEqual(done["action"], "completed")
        job = core.get_job(sd, "e46-pub-1")
        self.assertEqual(job["status"], "succeeded")
        self.assertEqual(job["adapter"], "opencode")
        result = core.result_view(sd, "e46-pub-1")
        self.assertIn("capacity", result)
        self.assertTrue([r for r in result["capacity"]
                         if r["route"] == "luna/max"
                         and r["state"] == "exhausted"])


if __name__ == "__main__":
    unittest.main()
