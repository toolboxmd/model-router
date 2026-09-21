"""Issue #44: Codex kit carries the login, 401 is an auth failure, probe before dispatch.

Deterministic only. No live model CLIs. Uses fake Codex CLI text shaped
like the live 2026-09-21 rehearsal (stderr 401 Unauthorized, stdout
missing bearer authentication) and throwaway state dirs/homes.
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
from runner import kits as kitmod  # noqa: E402

LIVE_STDOUT = "Missing bearer or basic authentication in header\n"
LIVE_STDERR = "failed to connect to websocket: HTTP error: 401 Unauthorized\n"

SECRET = "sekret-auth-44-xyz"


def fresh_state(testcase, request_id="a44-1"):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    testcase.addCleanup(tmp.cleanup)
    base = Path(tmp.name)
    sd = str(base / "state")
    ws = base / "ws"
    ws.mkdir()
    core.submit(sd, request_id, {"goal": "auth44"}, str(ws), "planner-sess")
    return sd, base, ws


def isolate_homes(testcase, with_codex_auth=True, with_grok_auth=True):
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
    # Minimal installed kit deps so materialization never raises.
    oc = Path(homes["MODEL_ROUTER_OPENCODE_HOME"])
    (oc / "skills" / "operations").mkdir(parents=True, exist_ok=True)
    (oc / "skills" / "project-direction").mkdir(parents=True, exist_ok=True)
    (oc / "skills" / "operations" / "SKILL.md").write_text("# operations\n")
    (oc / "skills" / "project-direction" / "SKILL.md").write_text("# pd\n")
    (oc / "plugins").mkdir(parents=True, exist_ok=True)
    (oc / "plugins" / "agentsmd-project-direction.js").write_text("// p\n")
    (oc / "opencode.json").write_text(json.dumps({"mcp": {}}))
    (oc / "AGENTS.md").write_text("# AGENTS.md\n")
    if with_codex_auth:
        (Path(homes["MODEL_ROUTER_CODEX_HOME"]) / "auth.json").write_text(
            json.dumps({"token": SECRET}))
    if with_grok_auth:
        (Path(homes["MODEL_ROUTER_GROK_HOME"]) / "auth.json").write_text(
            json.dumps({"token": SECRET}))
    saved = {k: os.environ.get(k) for k in homes}
    saved_codex = os.environ.get("CODEX_HOME")
    saved_grok = os.environ.get("GROK_HOME")
    os.environ.update(homes)
    # Direct CODEX_HOME/GROK_HOME must not leak the real login in.
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


def codex_success(cmd, cwd=None, timeout=None, **kw):
    out = "\n".join([
        json.dumps({"type": "thread.started", "thread_id": "thr-auth-001"}),
        json.dumps({"type": "item.completed",
                    "item": {"type": "agent_message",
                             "text": json.dumps({"action": "completion",
                                                 "output": "DONE",
                                                 "artifact": ""})}}),
        json.dumps({"type": "turn.completed", "usage": {}}),
    ]) + "\n"
    return 0, out, ""


def codex_401_no_thread(cmd, cwd=None, timeout=None, **kw):
    return 1, LIVE_STDOUT, LIVE_STDERR


def codex_401_with_thread(cmd, cwd=None, timeout=None, **kw):
    out = (json.dumps({"type": "thread.started",
                       "thread_id": "thr-auth-002"}) + "\n" + LIVE_STDOUT)
    return 1, out, LIVE_STDERR


def codex_generic_fail_with_thread(cmd, cwd=None, timeout=None, **kw):
    out = (json.dumps({"type": "thread.started",
                       "thread_id": "thr-generic-001"}) + "\ncodex failed\n")
    return 1, out, "boom\n"


class TestKitAuthLink(unittest.TestCase):
    def test_codex_kit_links_auth_json(self):
        base, homes = isolate_homes(self, with_codex_auth=True)
        dest = base / "codex-kit"
        kitmod.materialize_codex_kit("dispatcher", dest)
        target = dest / "auth.json"
        self.assertTrue(target.is_symlink(), "login is shared by link, never copied")
        self.assertEqual(Path(os.path.realpath(target)),
                         Path(os.path.realpath(
                             Path(homes["MODEL_ROUTER_CODEX_HOME"]) / "auth.json")))
        stored = json.loads((dest / "kit.json").read_text())
        self.assertIn("auth.json", stored["auth"]["linked"])
        self.assertEqual(stored["auth"]["missing"], [])
        # No secret values in kit.json or the ledger view.
        self.assertNotIn(SECRET, (dest / "kit.json").read_text())
        contents = kitmod.kit_contents_for_ledger(dest)
        self.assertEqual(contents["auth"]["linked"], ["auth.json"])
        self.assertNotIn(SECRET, json.dumps(contents))

    def test_codex_kit_honors_home_overrides(self):
        base, homes = isolate_homes(self, with_codex_auth=True)
        override = base / "override-cx"
        override.mkdir()
        (override / "auth.json").write_text(json.dumps({"token": "override-token"}))
        saved_model = os.environ.get("MODEL_ROUTER_CODEX_HOME")
        saved_direct = os.environ.get("CODEX_HOME")
        os.environ["MODEL_ROUTER_CODEX_HOME"] = str(override)
        os.environ["CODEX_HOME"] = homes["MODEL_ROUTER_CODEX_HOME"]
        try:
            dest = base / "codex-override"
            kitmod.materialize_codex_kit("dispatcher", dest)
            self.assertEqual(Path(os.path.realpath(dest / "auth.json")),
                             Path(os.path.realpath(override / "auth.json")))
        finally:
            if saved_model is None:
                os.environ.pop("MODEL_ROUTER_CODEX_HOME", None)
            else:
                os.environ["MODEL_ROUTER_CODEX_HOME"] = saved_model
            if saved_direct is None:
                os.environ.pop("CODEX_HOME", None)
            else:
                os.environ["CODEX_HOME"] = saved_direct

    def test_codex_kit_missing_auth_records_gap(self):
        base, _homes = isolate_homes(self, with_codex_auth=False)
        dest = base / "codex-missing"
        kitmod.materialize_codex_kit("dispatcher", dest)
        self.assertFalse((dest / "auth.json").exists())
        stored = json.loads((dest / "kit.json").read_text())
        self.assertEqual(stored["auth"]["linked"], [])
        self.assertIn("auth.json", stored["auth"]["missing"])

    def test_grok_kit_links_config_login_when_present(self):
        base, homes = isolate_homes(self, with_grok_auth=True)
        dest = base / "grok-kit"
        kitmod.materialize_grok_kit("recovery", dest)
        target = dest / "auth.json"
        self.assertTrue(target.is_symlink())
        self.assertEqual(Path(os.path.realpath(target)),
                         Path(os.path.realpath(
                             Path(homes["MODEL_ROUTER_GROK_HOME"]) / "auth.json")))
        stored = json.loads((dest / "kit.json").read_text())
        self.assertIn("auth.json", stored["auth"]["linked"])
        self.assertNotIn(SECRET, (dest / "kit.json").read_text())

    def test_grok_kit_missing_login_records_gap(self):
        base, _homes = isolate_homes(self, with_grok_auth=False)
        dest = base / "grok-missing"
        kitmod.materialize_grok_kit("recovery", dest)
        self.assertFalse((dest / "auth.json").exists())
        stored = json.loads((dest / "kit.json").read_text())
        self.assertIn("auth.json", stored["auth"]["missing"])

    def test_spawn_spec_materializes_codex_auth_link(self):
        base, homes = isolate_homes(self, with_codex_auth=True)
        sd = str(base / "state")
        inv = {"request_id": "r-auth", "invocation_id": "i-auth-1",
               "stdout_path": str(base / "state" / "outputs" / "r-auth.i-auth-1.stdout"),
               "meta_json": json.dumps({"route": "luna/max", "stage": "dispatch"})}
        (base / "state" / "outputs").mkdir(parents=True, exist_ok=True)
        env, _pwd = harnesses.harness_named("codex").spawn_spec(
            inv, {"PATH": "x"})
        kit_dir = Path(env["CODEX_HOME"])
        self.assertTrue((kit_dir / "auth.json").is_symlink())
        self.assertEqual(Path(os.path.realpath(kit_dir / "auth.json")),
                         Path(os.path.realpath(
                             Path(homes["MODEL_ROUTER_CODEX_HOME"]) / "auth.json")))
        contents = kitmod.kit_contents_for_ledger(kit_dir)
        self.assertIsNotNone(contents)
        self.assertEqual(contents["kit"], "dispatcher")
        self.assertIn("auth.json", contents["auth"]["linked"])
        self.assertNotIn(SECRET, json.dumps(contents))

    def test_kit_contents_listed_without_secrets(self):
        base, _homes = isolate_homes(self, with_codex_auth=True)
        sd = str(base / "state")
        dest = kitmod.kit_dir_for(sd, "r-ledger", "i-ledger-1", "dispatcher")
        kitmod.materialize_codex_kit("dispatcher", dest)
        contents = kitmod.kit_contents_for_ledger(dest)
        kit = policy.kit_for_role("dispatcher")
        self.assertEqual(contents["skills"], kit["skills"])
        self.assertEqual(contents["plugins"], kit["plugins"])
        self.assertEqual(contents["mcp"], kit["mcp"])
        self.assertIn("auth.json", contents["auth"]["linked"])
        blob = json.dumps(contents, sort_keys=True)
        self.assertNotIn(SECRET, blob)
        self.assertNotIn("token", blob.lower().replace("auth", ""))

    def test_ledger_lists_kit_contents_without_secrets(self):
        from runner import store
        base, _homes = isolate_homes(self, with_codex_auth=True)
        sd, _b, ws = fresh_state(self, "a44-ledger-1")
        inv_id = "ledgerinv001"
        kit_dir = kitmod.kit_dir_for(sd, "a44-ledger-1", inv_id, "dispatcher")
        kitmod.materialize_codex_kit("dispatcher", kit_dir)
        kit = policy.kit_for_role("dispatcher")
        con = store.connect(sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json,"
                " workspace, owner_token, stdout_path, stderr_path, started_at,"
                " state, stage, requested_route, kit, kit_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (inv_id, "a44-ledger-1", "codex_dispatch", "[]", str(ws),
                 "tok", str(kit_dir / "out"), str(kit_dir / "err"),
                 "2026-09-21T00:00:00+00:00", "completed", "dispatch",
                 "luna/max", "dispatcher", policy.kit_hash(kit)))
        finally:
            con.close()
        measures = core.invocation_measurements(sd, "a44-ledger-1")
        self.assertEqual(len(measures), 1)
        m = measures[0]
        self.assertIsNotNone(m["kit_contents"])
        self.assertEqual(m["kit_contents"]["kit"], "dispatcher")
        self.assertIn("auth.json", m["kit_contents"]["auth"]["linked"])
        self.assertIn("auth.json", m["kit_auth"]["linked"])
        self.assertNotIn(SECRET, json.dumps(m["kit_contents"], sort_keys=True))
        self.assertNotIn(SECRET, json.dumps(m, sort_keys=True))


class TestCodexAuthClassification(unittest.TestCase):
    def test_auth_text_markers(self):
        h = harnesses.harness_named("codex")
        reason = h.auth_failure_reason(LIVE_STDOUT, LIVE_STDERR)
        self.assertIsInstance(reason, str)
        self.assertTrue(reason)
        self.assertLessEqual(len(reason), 120)
        self.assertIsNone(h.auth_failure_reason("turn completed\n", ""))
        self.assertIsNone(h.auth_failure_reason("", "codex failed to start\n"))

    def test_bare_401_substring_is_not_auth(self):
        h = harnesses.harness_named("codex")
        # Counts, ports, and ids containing 401 as a substring are not
        # auth failures: only a standalone 401 token counts.
        self.assertIsNone(h.auth_failure_reason("processed 4010 items\n", ""))
        self.assertIsNone(h.auth_failure_reason("", "listening on port 14012\n"))
        self.assertIsNone(h.auth_failure_reason("exit 4010\n", "count 4012\n"))
        self.assertIsNone(
            h.classify_signal({"message": "processed 4010 items"}))
        # A standalone 401 still counts even without the "unauthorized"
        # word, and the live shape still classifies.
        self.assertIsNotNone(h.auth_failure_reason("", "HTTP error: 401\n"))
        self.assertEqual(
            h.classify_signal({"message": "HTTP error: 401"}), "hard")

    def test_policy_hard_covers_401(self):
        self.assertEqual(
            policy.classify_signal({"name": "APIError",
                                    "data": {"statusCode": 401,
                                             "responseBody": "Unauthorized"}}),
            "hard")
        self.assertEqual(
            policy.classify_signal({"type": "retry",
                                    "action": {"reason": "auth"}}),
            "hard")
        h = harnesses.harness_named("codex")
        self.assertEqual(h.classify_signal({"message": LIVE_STDERR}), "hard")
        self.assertIsNone(h.classify_signal({"message": "codex failed to start"}))


class TestDispatchAuthBlock(unittest.TestCase):
    def test_401_without_thread_blocks_auth_no_fallback(self):
        isolate_homes(self, with_codex_auth=True)
        sd, _base, _ws = fresh_state(self, "a44-auth-1")
        calls = []

        def run(cmd, cwd=None, timeout=None, **kw):
            calls.append((list(cmd), dict(kw)))
            return codex_401_no_thread(cmd, cwd, timeout, **kw)

        res = controller.dispatch(sd, "a44-auth-1", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["reason"], "codex_auth_failed")
        job = core.get_job(sd, "a44-auth-1")
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"].startswith("codex_auth_failed: "))
        # Auth never falls back to another route: exactly one child ran.
        self.assertEqual(len(calls), 1)
        self.assertIn("codex", calls[0][0][0])
        view = core.status_view(sd, "a44-auth-1")
        self.assertTrue(view["job"]["block_reason"].startswith("codex_auth_failed: "))
        self.assertIn("codex_auth_failed", view["job"]["block_reason"])

    def test_401_with_thread_blocks_auth_with_task(self):
        isolate_homes(self, with_codex_auth=True)
        sd, _base, _ws = fresh_state(self, "a44-auth-2")

        def run(cmd, cwd=None, timeout=None, **kw):
            return codex_401_with_thread(cmd, cwd, timeout, **kw)

        res = controller.dispatch(sd, "a44-auth-2", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["reason"], "codex_auth_failed")
        job = core.get_job(sd, "a44-auth-2")
        self.assertTrue(job["block_reason"].startswith("codex_auth_failed: "))
        self.assertTrue(job["codex_task_id"].startswith("thr-auth-"))

    def test_generic_failure_keeps_old_reason(self):
        isolate_homes(self, with_codex_auth=True)
        sd, _base, _ws = fresh_state(self, "a44-auth-3")

        def run(cmd, cwd=None, timeout=None, **kw):
            return codex_generic_fail_with_thread(cmd, cwd, timeout, **kw)

        res = controller.dispatch(sd, "a44-auth-3", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["reason"], "codex_dispatch_failed")
        job = core.get_job(sd, "a44-auth-3")
        self.assertTrue(job["block_reason"].startswith("codex_dispatch_failed"))
        self.assertNotIn("codex_auth_failed", job["block_reason"])

    def test_4010_count_is_not_auth(self):
        isolate_homes(self, with_codex_auth=True)
        sd, _base, _ws = fresh_state(self, "a44-auth-5")

        def run(cmd, cwd=None, timeout=None, **kw):
            return 1, "processed 4010 items\n", "listening on port 14012\n"

        res = controller.dispatch(sd, "a44-auth-5", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["reason"], "codex_dispatch_failed")
        job = core.get_job(sd, "a44-auth-5")
        self.assertNotIn("codex_auth_failed", job["block_reason"])

    def test_401_missing_home_suffix(self):
        _base, _homes = isolate_homes(self, with_codex_auth=False)
        sd, _b, _ws = fresh_state(self, "a44-auth-6")

        def run(cmd, cwd=None, timeout=None, **kw):
            return codex_401_no_thread(cmd, cwd, timeout, **kw)

        res = controller.dispatch(sd, "a44-auth-6", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["reason"], "codex_auth_failed")
        job = core.get_job(sd, "a44-auth-6")
        self.assertIn("(auth.json missing in CODEX_HOME)", job["block_reason"])

    def test_resume_401_blocks_auth(self):
        isolate_homes(self, with_codex_auth=True)
        sd, _base, _ws = fresh_state(self, "a44-auth-7")
        controller._save_codex_task(sd, "a44-auth-7", "thr-auth-resume-1")

        def run(cmd, cwd=None, timeout=None, **kw):
            return codex_401_no_thread(cmd, cwd, timeout, **kw)

        res = controller.resume_luna(sd, "a44-auth-7", "followup ctx",
                                     run_cmd=run)
        self.assertEqual(res["reason"], "codex_auth_failed")
        job = core.get_job(sd, "a44-auth-7")
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"].startswith("codex_auth_failed: "))


class TestProbeBeforeDispatch(unittest.TestCase):
    def test_default_probe_records_unknown_when_due(self):
        isolate_homes(self, with_codex_auth=False)
        sd, _base, _ws = fresh_state(self, "a44-probe-1")
        self.assertEqual(core.list_readings(sd, pool="codex"), [])
        out = controller._default_codex_probe(sd, "a44-probe-1", "luna/max")
        self.assertEqual(out["action"], "probe-unknown")
        readings = core.list_readings(sd, pool="codex")
        self.assertTrue(readings)
        by_window = {r["window"]: r for r in readings}
        self.assertIn("5h", by_window)
        self.assertIsNone(by_window["5h"]["used"])
        self.assertEqual(by_window["5h"]["source"], "provider_reported")
        detail = json.dumps(by_window["5h"].get("detail") or {})
        # The reason travels in the reading detail, never a secret.
        self.assertTrue(detail)
        self.assertNotIn(SECRET, detail)

    def test_default_probe_skipped_when_fresh(self):
        import datetime
        isolate_homes(self, with_codex_auth=False)
        sd, _base, _ws = fresh_state(self, "a44-probe-2")
        now = datetime.datetime.now(datetime.timezone.utc)
        observed = now.isoformat()
        spec = policy.ROUTES["luna/max"]
        core.record_reading(sd, spec["pool"], spec["model"], "5h",
                            10.0, 100.0, None, observed_at=observed,
                            source="provider_reported")
        core.record_reading(sd, spec["pool"], spec["model"], "weekly",
                            10.0, 100.0, None, observed_at=observed,
                            source="provider_reported")
        out = controller._default_codex_probe(sd, "a44-probe-2", "luna/max")
        self.assertEqual(out["action"], "probe-skipped")

    def test_dispatch_runs_probe_and_never_blocks_on_failure(self):
        isolate_homes(self, with_codex_auth=False)
        sd, _base, _ws = fresh_state(self, "a44-probe-3")
        calls = []

        def flaky_probe(state_dir, request_id, route):
            calls.append(route)
            raise RuntimeError("probe exploded")

        res = controller.dispatch(sd, "a44-probe-3", run_cmd=codex_success,
                                  probe=flaky_probe)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(calls, [policy.stage_routes("dispatch")[0]])
        readings = core.list_readings(sd, pool="codex")
        unknown = [r for r in readings if r["used"] is None]
        self.assertTrue(unknown, "probe failure records unknown")
        self.assertTrue(all(r["source"] == "provider_reported" for r in unknown))

    def test_dispatch_default_probe_runs_when_due(self):
        isolate_homes(self, with_codex_auth=False)
        sd, _base, _ws = fresh_state(self, "a44-probe-4")
        res = controller.dispatch(sd, "a44-probe-4", run_cmd=codex_success)
        self.assertEqual(res["action"], "dispatched")
        readings = core.list_readings(sd, pool="codex")
        self.assertTrue(any(r["used"] is None for r in readings),
                        "default probe ran when due and recorded unknown")

    def test_default_probe_ok_via_newest_rollout(self):
        base, homes = isolate_homes(self, with_codex_auth=False)
        sd, _b, _ws = fresh_state(self, "a44-probe-5")
        roll_dir = Path(homes["MODEL_ROUTER_CODEX_HOME"]) / "rollouts"
        roll_dir.mkdir(parents=True, exist_ok=True)
        rollout = roll_dir / "sess-001.jsonl"
        rollout.write_text("\n".join(json.dumps(o) for o in [
            {"type": "event", "rate_limits": {"primary": {
                "used_percent": 12.5, "window_minutes": 300,
                "resets_at": "2026-09-21T03:00:00+00:00",
                "plan_type": "plus"}}},
        ]) + "\n")
        out = controller._default_codex_probe(sd, "a44-probe-5", "luna/max")
        self.assertEqual(out["action"], "probe-ok")
        readings = core.list_readings(sd, pool="codex")
        self.assertTrue(readings)
        by_window = {r["window"]: r for r in readings}
        self.assertIn("5h", by_window)
        self.assertEqual(by_window["5h"]["used"], 12.5)
        self.assertEqual(by_window["5h"]["source"], "provider_reported")
        _ = base


if __name__ == "__main__":
    unittest.main()
