"""Issue #73: silent Codex reasoning is not killed as stalled; auth reads error evidence only.

Deterministic only. No live model CLIs. Covers the 2026-09-23 live report
(job ao-router-1, runner v0.26.1): Luna at max effort reasoned silently for
181 seconds with no stream events, the 180-second detector ended the turn
(rc 4), and the failure was then classified hard/auth because a successful
``command_execution`` item quoted the runner's own auth-pattern text.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, harnesses, policy  # noqa: E402

PY = sys.executable

LIVE_LIKE_STDOUT = "Missing bearer or basic authentication in header\n"
LIVE_LIKE_STDERR = "failed to connect to websocket: HTTP error: 401 Unauthorized\n"

THREAD_STARTED = {"type": "thread.started", "thread_id": "thr-73-001"}
# The live shape: a successful command result whose output quotes the
# runner's own auth-pattern text. Never error evidence.
AUTH_QUOTING_COMMAND = {
    "type": "item.completed",
    "item": {"type": "command_execution",
             "command": "sed -n '1,340p' runner/auth.py",
             "output": ("probe redacted: Missing Bearer <redacted> Basic <redacted> "
                        "header, service answered HTTP 401")},
}
STALL_ERR = ("runner: stalled: no harness output for 181s "
             "(window 300s, 48 lines seen, longest silence 180.9s)\n")


def _stdout(*objs) -> str:
    return "\n".join(json.dumps(o) for o in objs) + "\n"


def _submit(testcase, rid="i73-1"):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    testcase.addCleanup(tmp.cleanup)
    base = Path(tmp.name)
    sd = str(base / "state")
    ws = base / "ws"
    ws.mkdir()
    core.submit(sd, rid, {"goal": "issue73"}, str(ws), "planner-sess")
    return sd, base


def _opencode_success(session="ses_issue73_001"):
    def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
        body = {"opencode_session_id": session, "ok": True, "rc": 0,
                "assistant_text": json.dumps({"action": "completion",
                                              "output": "PLANNED", "artifact": ""}),
                "finish": "stop",
                "actual_model": {"providerID": "opencode-go", "modelID": "gpt-5.6-luna"}}
        return 0, "RUNNER_RESULT " + json.dumps(body) + "\n", ""
    return run


def _last_error(sd, rid):
    return json.loads(core.get_job(sd, rid)["last_error_json"] or "{}")


class TestCodexAuthEvidenceBoundary(unittest.TestCase):
    def test_stderr_transport_text_is_auth(self):
        h = harnesses.harness_named("codex")
        self.assertIsNotNone(h.auth_failure_reason("", LIVE_LIKE_STDERR))
        self.assertIsNotNone(h.auth_failure_reason("", "Missing bearer or basic "
                                                      "authentication in header\n"))

    def test_turn_failed_error_fields_are_auth(self):
        h = harnesses.harness_named("codex")
        out = _stdout({"type": "turn.failed",
                       "error": {"message": "failed to connect to websocket: "
                                            "HTTP error: 401 Unauthorized"}})
        self.assertIsNotNone(h.auth_failure_reason(out, ""))
        out = _stdout({"type": "error",
                       "message": "Missing bearer or basic authentication in header"})
        self.assertIsNotNone(h.auth_failure_reason(out, ""))
        out = _stdout({"type": "turn.failed",
                       "error": {"code": 401, "message": "unauthorized"}})
        self.assertIsNotNone(h.auth_failure_reason(out, ""))

    def test_last_agent_message_of_failed_turn_is_auth(self):
        h = harnesses.harness_named("codex")
        out = _stdout(THREAD_STARTED,
                      {"type": "item.completed",
                       "item": {"type": "agent_message", "id": "m1",
                                "text": "dispatch note: Missing bearer or basic "
                                        "authentication in header, log in again"}})
        self.assertIsNotNone(h.auth_failure_reason(out, ""))

    def test_command_execution_output_is_never_auth(self):
        # The live regression: a successful command result quotes both the
        # auth phrases and a 401, on a turn with no completion event and a
        # stall marker on stderr. None of that is error evidence.
        h = harnesses.harness_named("codex")
        out = _stdout(THREAD_STARTED, AUTH_QUOTING_COMMAND)
        self.assertIsNone(h.auth_failure_reason(out, ""))
        self.assertIsNone(h.auth_failure_reason(out, STALL_ERR))
        # A bare command-output count is not auth either.
        out = _stdout(THREAD_STARTED,
                      {"type": "item.completed",
                       "item": {"type": "command_execution", "command": "wc",
                                "output": "processed 4010 items, port 14012"}})
        self.assertIsNone(h.auth_failure_reason(out, ""))

    def test_turn_failed_with_unrelated_error_ignores_command_output(self):
        h = harnesses.harness_named("codex")
        out = _stdout(THREAD_STARTED, AUTH_QUOTING_COMMAND,
                      {"type": "turn.failed", "error": {"message": "codex failed"}})
        self.assertIsNone(h.auth_failure_reason(out, ""))

    def test_earlier_agent_message_is_not_auth(self):
        h = harnesses.harness_named("codex")
        out = _stdout(THREAD_STARTED,
                      {"type": "item.completed",
                       "item": {"type": "agent_message", "id": "m1",
                                "text": "saw Missing bearer text in a file, ignoring"}},
                      {"type": "item.completed",
                       "item": {"type": "agent_message", "id": "m2",
                                "text": "working on the dispatch"}})
        self.assertIsNone(h.auth_failure_reason(out, ""))

    def test_completed_turn_agent_message_is_not_auth(self):
        h = harnesses.harness_named("codex")
        out = _stdout(THREAD_STARTED,
                      {"type": "item.completed",
                       "item": {"type": "agent_message", "id": "m1",
                                "text": "Missing bearer or basic authentication "
                                        "in header"}},
                      {"type": "turn.completed", "usage": {}})
        self.assertIsNone(h.auth_failure_reason(out, ""))

    def test_ambiguous_failure_is_not_auth(self):
        h = harnesses.harness_named("codex")
        self.assertIsNone(h.auth_failure_reason("codex failed\n", "boom\n"))
        self.assertIsNone(h.auth_failure_reason("", ""))
        # Counts, ports, and ids containing 401 as a substring never count.
        self.assertIsNone(h.auth_failure_reason("processed 4010 items\n", ""))
        self.assertIsNone(h.auth_failure_reason("", "listening on port 14012\n"))

    def test_structured_401_still_classifies_hard(self):
        h = harnesses.harness_named("codex")
        self.assertEqual(h.classify_signal({"message": "HTTP error: 401"}), "hard")
        self.assertEqual(h.classify_signal({"message": LIVE_LIKE_STDERR}), "hard")
        self.assertIsNone(h.classify_signal({"message": "codex failed to start"}))


class TestStallWindowPolicy(unittest.TestCase):
    def test_codex_window_clears_the_observed_silent_interval(self):
        self.assertEqual(policy.STALL_SILENCE_SECS_BY_HARNESS["codex"], 300)
        self.assertGreater(policy.STALL_SILENCE_SECS_BY_HARNESS["codex"], 181)
        self.assertEqual(harnesses.harness_named("codex").stall_window_secs({}), 300.0)

    def test_opencode_window_stays_unchanged(self):
        self.assertEqual(policy.STALL_SILENCE_SECS, 180)
        self.assertEqual(policy.STALL_SILENCE_SECS_BY_HARNESS["opencode"], 180)
        self.assertEqual(policy.STALL_SILENCE_SECS_BY_HARNESS["opencode"],
                         policy.STALL_SILENCE_SECS)
        for name in ("opencode", "claude", "grok"):
            self.assertEqual(harnesses.harness_named(name).stall_window_secs({}),
                             180.0, name)

    def test_overrides_still_win(self):
        h = harnesses.harness_named("codex")
        self.assertEqual(h.stall_window_secs({"stall_secs": 3}), 3.0)
        prev = os.environ.get("RUNNER_STALL_SECS")
        os.environ["RUNNER_STALL_SECS"] = "7"
        try:
            self.assertEqual(h.stall_window_secs({}), 7.0)
        finally:
            if prev is None:
                os.environ.pop("RUNNER_STALL_SECS", None)
            else:
                os.environ["RUNNER_STALL_SECS"] = prev

    def test_evidence_recorded_and_policy_valid(self):
        self.assertIn("#73", policy.STALL_CODEX_EVIDENCE)
        self.assertIn("181", policy.STALL_CODEX_EVIDENCE)
        self.assertEqual(policy.validate_policy(), [])
        skill = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        for name in ("300", "180", "silence window", "#73"):
            self.assertIn(name, skill, name)


class TestEffectiveStallWindowBound(unittest.TestCase):
    """The effective stall window passes through unclamped (#88).

    There is no per-turn elapsed deadline since #88, so the legacy
    ``timeout_secs`` slot is accepted for compatibility and ignored:
    configured and override windows are never clamped below it. Stall
    (genuine stream silence) is the only time-based end for active turns.
    """

    def test_policy_defaults_pass_through(self):
        self.assertEqual(
            harnesses.harness_named("codex").effective_stall_window_secs({}, 1800), 300.0)
        self.assertEqual(
            harnesses.harness_named("opencode").effective_stall_window_secs({}, 1800), 180.0)
        self.assertEqual(
            harnesses.harness_named("claude").effective_stall_window_secs({}, 900), 180.0)
        # A missing timeout (new rows store NULL) gives the same windows.
        self.assertEqual(
            harnesses.harness_named("codex").effective_stall_window_secs({}, None), 300.0)
        self.assertEqual(
            harnesses.harness_named("opencode").effective_stall_window_secs({}, None), 180.0)

    def test_small_override_passes_through(self):
        h = harnesses.harness_named("codex")
        self.assertEqual(h.effective_stall_window_secs({"stall_secs": 3}, 30), 3.0)

    def test_window_equal_to_former_timeout_passes_through(self):
        # #88 replaced the clamp: a window equal to a legacy timeout value
        # is used as-is instead of being forced strictly below it.
        h = harnesses.harness_named("codex")
        self.assertEqual(h.effective_stall_window_secs({"stall_secs": 8}, 8), 8.0)

    def test_window_above_former_timeout_passes_through(self):
        # #88 replaced the clamp: a window above a legacy timeout value is
        # used as-is; the active turn is never killed for its age.
        h = harnesses.harness_named("codex")
        self.assertEqual(h.effective_stall_window_secs({"stall_secs": 600}, 8), 600.0)

    def test_env_override_above_former_timeout_passes_through(self):
        h = harnesses.harness_named("codex")
        prev = os.environ.get("RUNNER_STALL_SECS")
        os.environ["RUNNER_STALL_SECS"] = "600"
        try:
            window = h.effective_stall_window_secs({}, 8)
        finally:
            if prev is None:
                os.environ.pop("RUNNER_STALL_SECS", None)
            else:
                os.environ["RUNNER_STALL_SECS"] = prev
        self.assertEqual(window, 600.0)

    def test_missing_or_invalid_timeout_leaves_the_window(self):
        h = harnesses.harness_named("codex")
        for bad in (None, 0, -5, "junk"):
            self.assertEqual(
                h.effective_stall_window_secs({"stall_secs": 600}, bad), 600.0)

    def test_tiny_former_timeout_leaves_the_window(self):
        # #88: even a tiny legacy timeout no longer shrinks the window.
        h = harnesses.harness_named("codex")
        self.assertEqual(h.effective_stall_window_secs({"stall_secs": 600}, 1), 600.0)


class TestDispatchStallPath(unittest.TestCase):
    def _stall_run(self, out, session="ses_issue73_001"):
        ok = _opencode_success(session)

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            if kind == "codex_dispatch":
                return 4, out, STALL_ERR
            if kind == "codex_resume":
                return 4, out, STALL_ERR
            return ok(cmd, cwd, timeout, kind, meta)
        return run

    def test_stalled_dispatch_with_thread_moves_laterally_not_auth(self):
        sd, _base = _submit(self, "i73-d1")
        out = _stdout(THREAD_STARTED, AUTH_QUOTING_COMMAND)
        res = controller.dispatch(sd, "i73-d1", run_cmd=self._stall_run(out),
                                  probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        job = core.get_job(sd, "i73-d1")
        self.assertNotEqual(job["status"], "blocked")
        self.assertNotIn("codex_auth_failed", job["block_reason"] or "")
        self.assertEqual(_last_error(sd, "i73-d1").get("signal"), "stalled")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("dispatch_route_reason"), "dispatch_stalled")
        self.assertNotIn("route_reason", st)

    def test_stalled_dispatch_without_thread_moves_laterally(self):
        sd, _base = _submit(self, "i73-d2")
        res = controller.dispatch(sd, "i73-d2",
                                  run_cmd=self._stall_run(""),
                                  probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        job = core.get_job(sd, "i73-d2")
        self.assertNotIn("codex_auth_failed", job["block_reason"] or "")
        self.assertEqual(_last_error(sd, "i73-d2").get("signal"), "stalled")

    def test_resume_stall_blocks_stalled_not_auth(self):
        sd, _base = _submit(self, "i73-r1")
        controller._save_codex_task(sd, "i73-r1", "thr-73-resume-1")
        out = _stdout({"type": "thread.started", "thread_id": "thr-73-resume-1"},
                      AUTH_QUOTING_COMMAND)
        res = controller.resume_luna(sd, "i73-r1", "followup ctx",
                                     run_cmd=self._stall_run(out))
        self.assertEqual(res["reason"], "codex_resume_stalled")
        job = core.get_job(sd, "i73-r1")
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"].startswith("codex_resume_stalled"))
        self.assertNotIn("codex_auth_failed", job["block_reason"])
        self.assertEqual(_last_error(sd, "i73-r1").get("signal"), "stalled")

    def test_genuine_auth_without_stall_marker_still_blocks_auth(self):
        sd, _base = _submit(self, "i73-a1")

        def run(cmd, cwd=None, timeout=None, **kw):
            return 1, LIVE_LIKE_STDOUT, LIVE_LIKE_STDERR

        res = controller.dispatch(sd, "i73-a1", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["reason"], "codex_auth_failed")


class TestSupervisorStallRecordsStalled(unittest.TestCase):
    def test_cli_silence_with_auth_pattern_output_is_stalled_never_hard(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        script = bindir / "codex"
        script.write_text(
            "#!" + PY + "\n"
            "import json, sys, time\n"
            "print(json.dumps({'type': 'thread.started', "
            "'thread_id': 'stall-thread'}), flush=True)\n"
            "print(json.dumps({'type': 'item.completed', 'item': "
            "{'type': 'command_execution', 'command': 'sed -n 1,340p x', "
            "'output': 'Missing Bearer <redacted> Basic <redacted> 401'}}), "
            "flush=True)\n"
            "sys.stdout.flush()\n"
            "time.sleep(20)\n")
        script.chmod(0o700)
        core.submit(sd, "cl73", {"g": 1}, str(ws), "p")
        from runner import store as _store
        con = _store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='cl73'")
        finally:
            con.close()
        started = time.monotonic()
        rc, out, err = core._durable_run(
            sd, "cl73", "tok", "codex_dispatch", [str(script)],
            cwd=str(ws), timeout=30,
            meta={"stage": "dispatch", "route": "luna/max",
                  "reason": "test", "stall_secs": 1})
        elapsed = time.monotonic() - started
        for inv in core._list_invocations(sd, "cl73"):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass
        self.assertEqual(rc, 4)
        self.assertLess(elapsed, 15.0)
        inv = core._list_invocations(sd, "cl73")[-1]
        result = json.loads(inv["result_json"] or "{}")
        self.assertEqual(result.get("signal"), "stalled")
        self.assertEqual(result["signal_evidence"]["source"], "stream_silence")
        self.assertEqual(inv["terminal_class"], "stalled")
        self.assertIn("stalled", err)
        # The same stream through auth evidence stays unknown: the
        # command-output auth text never classifies.
        h = harnesses.harness_named("codex")
        self.assertIsNone(h.auth_failure_reason(out, err))


class TestNoElapsedDeadlineBoundary(unittest.TestCase):
    """No elapsed deadline ends an active turn (#88).

    The legacy ``timeout`` argument to the durable run is recorded but
    never enforced: a silent turn still ends as stalled on the policy
    silence window, while an active turn with stream activity runs past
    the former per-turn cap to completion instead of dying with rc 124.
    """

    def _run_script(self, rid, body, timeout, stall_secs):
        from runner import store as _store
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        script = base / "bin-codex" / "codex"
        script.parent.mkdir(exist_ok=True)
        script.write_text("#!" + PY + "\n" + body)
        script.chmod(0o700)
        core.submit(sd, rid, {"g": 1}, str(ws), "p")
        con = _store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id=?", (rid,))
        finally:
            con.close()
        started = time.monotonic()
        rc, out, err = core._durable_run(
            sd, rid, "tok", "codex_dispatch", [str(script)],
            cwd=str(ws), timeout=timeout,
            meta={"stage": "dispatch", "route": "luna/max",
                  "reason": "test", "stall_secs": stall_secs})
        elapsed = time.monotonic() - started
        for inv in core._list_invocations(sd, rid):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass
        inv = core._list_invocations(sd, rid)[-1]
        return rc, out, err, elapsed, json.loads(inv["result_json"] or "{}"), inv

    def test_silent_turn_ends_stalled_on_the_window(self):
        # A legacy timeout value no longer bounds the turn: the silent
        # script still ends as stalled, on the configured window.
        rc, _out, err, _elapsed, result, inv = self._run_script(
            "i73-b1",
            "import time\ntime.sleep(60)\n",
            timeout=30, stall_secs=2)
        self.assertEqual(rc, 4)
        self.assertEqual(result.get("signal"), "stalled")
        self.assertEqual(result["signal_evidence"]["window_secs"], 2.0)
        self.assertGreaterEqual(result["signal_evidence"]["silence_secs"], 2.0)
        self.assertIsNotNone(inv.get("elapsed_secs"))
        self.assertIn("stalled", err)

    def test_silent_after_output_ends_stalled_on_the_window(self):
        rc, _out, err, _elapsed, result, inv = self._run_script(
            "i73-b2",
            "import json, sys, time\n"
            "print(json.dumps({'type': 'thread.started', "
            "'thread_id': 'stall-thread'}), flush=True)\n"
            "print(json.dumps({'type': 'item.completed', 'item': "
            "{'type': 'command_execution', 'command': 'sed -n 1,340p x', "
            "'output': 'quiet'}}), flush=True)\n"
            "sys.stdout.flush()\n"
            "time.sleep(60)\n",
            timeout=30, stall_secs=2)
        self.assertEqual(rc, 4)
        self.assertEqual(result.get("signal"), "stalled")
        self.assertEqual(result["signal_evidence"]["window_secs"], 2.0)
        # A live-like stream that goes silent after its first output ends
        # on the window however old the turn is allowed to grow.
        self.assertIsNotNone(inv.get("elapsed_secs"))
        self.assertIn("stalled", err)

    def test_active_turn_runs_past_the_former_timeout(self):
        # The #88 regression: an active turn (steady stream activity, no
        # silence) used to die with rc 124 at the per-turn cap. With the
        # legacy timeout recorded but unenforced, it runs ~7s of activity
        # past a 3s legacy value to completion.
        rc, _out, _err, elapsed, result, inv = self._run_script(
            "i73-b3",
            "import json, sys, time\n"
            "for i in range(24):\n"
            "    print(json.dumps({'type': 'item.completed', 'i': i}), flush=True)\n"
            "    time.sleep(0.3)\n",
            timeout=3, stall_secs=30)
        self.assertEqual(rc, 0)
        self.assertIsNone(result.get("signal"))
        self.assertGreater(elapsed, 3.0)
        self.assertIsNotNone(inv.get("elapsed_secs"))
        self.assertGreater(inv["elapsed_secs"], 3.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
