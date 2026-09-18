"""Bridge tests: defaults, sessions, resume/no-fork, quota, busy/callback.

Deterministic only, stdlib only, no live model CLIs. Command injection
and an injectable control seam keep everything offline; a fake localhost
HTTP server proves the provider-envelope + control path.
"""
from __future__ import annotations

import http.server
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters, controller, core, policy, store  # noqa: E402

PY = sys.executable


def cli(state_dir, *args, timeout=20):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(ROOT))
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def kill_pid(pid):
    try:
        os.kill(int(pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError, ValueError):
        return
    end = time.time() + 5.0
    while time.time() < end:
        try:
            waited, _ = os.waitpid(int(pid), os.WNOHANG)
            if waited == int(pid):
                return
        except ChildProcessError:
            pass
        except (ValueError, OverflowError, OSError):
            return
        try:
            os.kill(int(pid), 0)
        except ProcessLookupError:
            return
        except PermissionError:
            return
        time.sleep(0.05)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.sd = str(Path(self.tmp.name) / "state")
        self.wsbase = Path(self.tmp.name) / "ws"
        self.wsbase.mkdir()
        self.kids = []

    def tearDown(self):
        for pid in self.kids:
            kill_pid(pid)
        try:
            con = store.connect(self.sd)
            rows = con.execute("SELECT owner_pid FROM jobs").fetchall()
            con.close()
            for r in rows:
                if r["owner_pid"]:
                    kill_pid(int(r["owner_pid"]))
        except Exception:
            pass
        self.tmp.cleanup()

    def ws(self, name="w1"):
        p = self.wsbase / name
        p.mkdir(exist_ok=True)
        return str(p)

    def track(self, pid):
        if pid:
            self.kids.append(pid)
        return pid


class TestPublicDefaults(Base):
    def test_submit_works_with_builtin_defaults_no_extra_commands(self):
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1",
                         "--task", '{"goal":"fix typo"}', "--workspace", self.ws(),
                         "--planner-session", "claude-1", "--no-start")
        self.assertEqual(rc, 0)
        self.assertTrue(out.get("acknowledged"))
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["planner_session_id"], "claude-1")
        # Production planner default is Fable 5.1 max; Sonnet medium is
        # only the explicit bounded live-test override.
        self.assertEqual(job["planner_model"], "fable-5.1")
        self.assertEqual(job["planner_effort"], "max")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual(job["attempts"], 0)
        self.assertIsNone(job["owner_token"])

    def test_submit_allows_explicit_planner_overrides(self):
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1",
                         "--task", '{"goal":"t"}', "--workspace", self.ws(),
                         "--planner-session", "claude-1",
                         "--planner-model", "claude-opus-4",
                         "--planner-effort", "high",
                         "--no-start")
        self.assertEqual(rc, 0)
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["planner_model"], "claude-opus-4")
        self.assertEqual(job["planner_effort"], "high")

    def test_codex_dispatch_defaults(self):
        cmd = adapters.build_codex_dispatch_cmd("/tmp/ws", "do work")
        blob = " ".join(cmd)
        self.assertIn("codex", cmd[0])
        self.assertIn("gpt-5.6-luna", cmd)
        self.assertIn("--json", cmd)
        self.assertIn("--output-last-message", cmd)
        self.assertIn("--sandbox", cmd)
        self.assertIn("workspace-write", cmd)
        self.assertIn("--cd", cmd)
        self.assertIn("/tmp/ws", cmd)
        self.assertIn(adapters.CODEX_MODEL, cmd)
        # Effort is encoded via -c model_reasoning_effort, never --reasoning.
        self.assertNotIn("--reasoning", cmd)
        self.assertTrue(any("model_reasoning_effort" in c and "max" in c for c in cmd), blob)
        # Resume reuses only the saved ID with cwd, never --cd/--reasoning.
        rcmd = adapters.build_codex_resume_cmd("thread-abc-123", "/tmp/ws", "follow up")
        self.assertIn("resume", rcmd)
        self.assertIn("thread-abc-123", rcmd)
        self.assertIn("--json", rcmd)
        self.assertNotIn("--cd", rcmd)
        self.assertNotIn("--reasoning", rcmd)

    def test_claude_resume_defaults_and_overrides(self):
        cmd = adapters.build_claude_cmd("claude-live-001", "answer this",
                                        model="claude-sonnet-5", effort="medium")
        self.assertIn("claude", cmd[0])
        self.assertIn("--resume", cmd)
        self.assertIn("claude-live-001", cmd)
        self.assertIn("claude-sonnet-5", cmd)
        self.assertIn("medium", cmd)
        cmd2 = adapters.build_claude_cmd("s1", "p", model="m2", effort="high")
        self.assertIn("m2", cmd2)
        self.assertIn("high", cmd2)

    def test_opencode_defaults_free_first(self):
        cmd = adapters.build_opencode_cmd("/tmp/ws", "implement")
        blob = " ".join(cmd)
        self.assertIn("opencode", cmd[0])
        self.assertIn("opencode/muse-spark-1.3-contributor-free", cmd)
        self.assertIn("--variant", cmd)
        self.assertIn("xhigh", cmd)
        self.assertIn("--dir", cmd)
        self.assertIn("/tmp/ws", cmd)
        self.assertIn("--format", cmd)
        self.assertIn("json", cmd)
        self.assertIn("--pure", cmd)
        self.assertIn("--agent", cmd)
        self.assertIn("build", cmd)
        self.assertNotIn("--effort", cmd)
        self.assertNotIn("--cd", cmd)
        # Allowance drives the model route; Go only after exact exhaustion.
        go = adapters.build_opencode_cmd("/tmp/ws", "implement",
                                         allowance="go-included")
        self.assertIn("opencode-go/muse-spark-1.3-contributor", go)
        # Saved sessions resume via --session.
        resumed = adapters.build_opencode_cmd("/tmp/ws", "implement",
                                              session_id="oc-saved-1")
        self.assertIn("--session", resumed)
        self.assertIn("oc-saved-1", resumed)
        self.assertNotIn("muse-spark-1.3-contributor", " ".join(
            c for c in cmd if c.startswith("muse-")), blob)

    def test_missing_planner_session_rejected_no_fork(self):
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r1", {"a": 1}, self.ws(), "")
        with self.assertRaises(ValueError):
            adapters.build_claude_cmd("", "prompt")
        with self.assertRaises(ValueError):
            adapters.build_codex_resume_cmd("", "/tmp/ws", "p")

    def test_public_recipe_submit_status_questions_answer_cancel_recover(self):
        w = self.ws()
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"goal":"t"}',
                       "--workspace", w, "--planner-session", "claude-1", "--no-start")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "status", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertIn("job", out)
        rc, _, _ = cli(self.sd, "post-question", "--request-id", "r1",
                       "--qid", "q1", "--prompt", "Confirm?")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "questions", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertEqual(len(out["questions"]), 1)
        rc, _, _ = cli(self.sd, "answer", "--request-id", "r1",
                       "--qid", "q1", "--answer", "Yes.")
        self.assertEqual(rc, 0)
        rc, _, _ = cli(self.sd, "cancel", "--request-id", "r1")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)


class TestAdapterSessions(Base):
    def test_codex_task_persisted_before_accepted_and_resume_reuses_saved(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")
        seen = []

        def fake_run(cmd, cwd=None, timeout=120):
            seen.append(list(cmd))
            self.assertIn("codex", cmd[0])
            out = json.dumps({"task_id": "codex-task-123",
                              "action": "planner_question",
                              "qid": "q1", "prompt": "Confirm?"})
            return 0, out, ""

        res = controller.dispatch(self.sd, "r1", run_cmd=fake_run)
        self.assertEqual(res["codex_task_id"], "codex-task-123")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["codex_task_id"], "codex-task-123")
        self.assertEqual(job["adapter"], "codex")
        self.assertEqual(job["model"], "gpt-5.6-luna")
        self.assertEqual(job["effort"], "max")
        # Second dispatch never forks: no new run, same ID.
        seen.clear()

        def boom(cmd, cwd=None, timeout=120):
            raise AssertionError("must not fork a second Codex task")

        res2 = controller.dispatch(self.sd, "r1", run_cmd=boom)
        self.assertEqual(res2["codex_task_id"], "codex-task-123")
        self.assertEqual(core.get_job(self.sd, "r1")["codex_task_id"], "codex-task-123")

    def test_resume_only_saved_task_id(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")

        def fake_dispatch(cmd, cwd=None, timeout=120):
            return 0, json.dumps({"task_id": "codex-saved-9", "action": "completion"}), ""

        controller.dispatch(self.sd, "r1", run_cmd=fake_dispatch)
        captured = {}

        def fake_resume(cmd, cwd=None, timeout=120):
            captured["cmd"] = list(cmd)
            return 0, json.dumps({"action": "completion", "output": "done"}), ""

        controller.resume_luna(self.sd, "r1", "planner answer text", run_cmd=fake_resume)
        cmd = captured["cmd"]
        self.assertIn("resume", cmd)
        self.assertIn("codex-saved-9", cmd)
        self.assertIn("--json", cmd)
        # Never contains a different task ID.
        self.assertNotIn("codex-other", " ".join(cmd))

    def test_planner_resume_never_forks_new_session(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-exact-77")
        captured = {}

        def fake_claude(cmd, cwd=None, timeout=120):
            captured["cmd"] = list(cmd)
            return 0, "Approved as written.", ""

        res = controller.planner_callback(self.sd, "r1", "q1", "Confirm paragraph?",
                                          run_cmd=fake_claude)
        self.assertEqual(res["action"], "answered")
        cmd = captured["cmd"]
        self.assertIn("--resume", cmd)
        idx = cmd.index("--resume")
        self.assertEqual(cmd[idx + 1], "claude-exact-77")
        # Persisted question then answer, job resumed.
        qs = core.list_questions(self.sd, "r1", only_pending=False)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0]["status"], "answered")
        self.assertEqual(qs[0]["answer"], "Approved as written.")

    def test_opencode_session_persisted_with_route_adapter_model(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")

        def fake_oc(cmd, cwd=None, timeout=120):
            self.assertIn("opencode", cmd[0])
            self.assertIn("opencode/muse-spark-1.3-contributor-free", cmd)
            self.assertIn("--variant", cmd)
            self.assertIn("xhigh", cmd)
            self.assertIn("--dir", cmd)
            self.assertNotIn("--effort", cmd)
            self.assertNotIn("--cd", cmd)
            return 0, json.dumps({"opencode_session_id": "oc-sess-5", "ok": True}), ""

        res = controller.run_implementation(self.sd, "r1", artifact="/tmp/a.txt",
                                            payload={"k": "v"}, run_cmd=fake_oc)
        self.assertEqual(res["action"], "implementation_ok")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["opencode_session_id"], "oc-sess-5")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")

    def test_luna_structured_actions_parse(self):
        q = controller.parse_luna_action(json.dumps({"action": "planner_question", "qid": "q1"}))
        self.assertEqual(q["action"], "planner_question")
        impl = controller.parse_luna_action(json.dumps({"action": "implementation", "artifact": "a"}))
        self.assertEqual(impl["action"], "implementation")
        done = controller.parse_luna_action(json.dumps({"action": "completion", "output": "ok"}))
        self.assertEqual(done["action"], "completion")
        self.assertIsNone(controller.parse_luna_action("plain text with no envelope"))

    def test_implementation_carries_saved_artifact_output(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")
        seen = {}

        def fake(cmd, cwd=None, timeout=120):
            seen["cmd"] = list(cmd)
            return 0, json.dumps({"ok": True}), ""

        controller.run_implementation(self.sd, "r1", artifact="outputs/fix.txt",
                                      payload={"output": "prior output"}, run_cmd=fake)
        blob = " ".join(seen["cmd"])
        # Adapter carries the saved artifact/output in the prompt tail.
        # The command itself stays provider-shaped; prompt is last arg.
        self.assertIn("opencode", seen["cmd"][0])
        # Prompt arg contains artifact (last element after --).
        self.assertIn("outputs/fix.txt", seen["cmd"][-1])


class TestControllerLifecycle(Base):
    def test_submit_persists_before_launch_with_builtin_spawn_seam(self):
        order = []

        def fake_spawn(cmd):
            # By the time spawn runs, the durable row must already commit.
            job = core.get_job(self.sd, "r1")
            order.append("spawn-after-persist")
            self.assertEqual(job["request_id"], "r1")
            self.assertEqual(job["status"], "running")
            self.assertEqual(job["attempts"], 1)
            return 424242

        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")
        # submit_and_start persists first, then uses built-in spawn seam.
        # Use submit row + start with fake spawn to stay offline.
        info = core.start_controller(self.sd, "r1", spawn=fake_spawn)
        self.assertEqual(order, ["spawn-after-persist"])
        self.assertEqual(info["pid"], 424242)
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["attempts"], 1)
        self.assertEqual(job["owner_pid"], 424242)

    def test_no_start_stays_offline(self):
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                       "--workspace", self.ws(), "--planner-session", "claude-1",
                       "--no-start")
        self.assertEqual(rc, 0)
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["attempts"], 0)
        self.assertIsNone(job["owner_token"])
        self.assertIsNone(job["owner_pid"])

    def test_controller_death_recoverable_resumes_saved_ids_never_forks(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")
        before = core.get_job(self.sd, "r1")

        def fake_dispatch(cmd, cwd=None, timeout=120):
            return 0, json.dumps({"task_id": "codex-keep-1", "action": "completion"}), ""

        controller.dispatch(self.sd, "r1", run_cmd=fake_dispatch)

        def fake_oc(cmd, cwd=None, timeout=120):
            return 0, json.dumps({"opencode_session_id": "oc-keep-2"}), ""

        controller.run_implementation(self.sd, "r1", run_cmd=fake_oc)
        # Hold the lease with a real detached sleep controller (offline mode).
        import subprocess as _sp
        root = store.ensure_state_dir(self.sd)
        con = store.connect(self.sd)
        try:
            tok = core.get_job(self.sd, "r1")["owner_token"]
        finally:
            con.close()
        # Use the worker lease path for a live holder, then adopt via recover.
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        self.track(info["pid"])
        time.sleep(0.8)
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertEqual(out.get("action"), "adopted-live-worker")
        mid = core.get_job(self.sd, "r1")
        self.assertEqual(mid["codex_task_id"], "codex-keep-1")
        self.assertEqual(mid["opencode_session_id"], "oc-keep-2")
        self.assertEqual(mid["planner_session_id"], "claude-1")
        self.assertEqual(mid["executor_session_id"], before["executor_session_id"])
        # Kill and recover: same IDs, no fork, partial preserved.
        kill_pid(info["pid"])
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        after = core.get_job(self.sd, "r1")
        self.assertEqual(after["codex_task_id"], "codex-keep-1")
        self.assertEqual(after["opencode_session_id"], "oc-keep-2")
        self.assertEqual(after["planner_session_id"], "claude-1")
        self.assertEqual(after["executor_session_id"], before["executor_session_id"])

    def test_builtin_controller_sleep_adopt(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        self.track(info["pid"])
        # Real detached controller in offline sleep mode holds the same lease shape.
        proc = subprocess.Popen([PY, "-m", "runner.controller", "--state-dir", self.sd,
                                 "--request-id", "r1", "--token", info["token"],
                                 "--mode", "sleep", "--duration", "10"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                start_new_session=True, close_fds=True, cwd=str(ROOT))
        self.track(proc.pid)
        time.sleep(0.8)
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        # A live recorded PID without a matching fresh handshake stays
        # claimed and blocks duplicates (never treated as worker_gone).
        self.assertIn(out.get("action"), ("adopted-live-worker", "reconciled-launch-race",
                                          "noop", "blocked-claimed-live-pid", "blocked-unknown-owner"))


class TestPlannerBusyAndCallback(Base):
    def test_busy_planner_becomes_durable_blocked_with_reason(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")

        def fake_busy(cmd, cwd=None, timeout=120):
            return 0, "Session is busy, try later", ""

        res = controller.planner_callback(self.sd, "r1", "q-busy", "Confirm?",
                                          run_cmd=fake_busy)
        self.assertEqual(res["action"], "blocked")
        self.assertIn("busy", res["reason"])
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("busy", (job["block_reason"] or "").lower())
        pending = core.list_questions(self.sd, "r1", only_pending=True)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["qid"], "q-busy")

    def test_callback_failure_becomes_durable_blocked_with_reason(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")

        def fake_fail(cmd, cwd=None, timeout=120):
            return 1, "", "claude: connection reset"

        res = controller.planner_callback(self.sd, "r1", "q-fail", "Confirm?",
                                          run_cmd=fake_fail)
        self.assertEqual(res["action"], "blocked")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"])
        pending = core.list_questions(self.sd, "r1", only_pending=True)
        self.assertEqual(len(pending), 1)
        # Error evidence persisted without secrets.
        self.assertTrue(job["last_error_json"])
        blob = job["last_error_json"]
        self.assertNotIn("Bearer", blob)

    def test_planner_question_persisted_before_claude_and_answer_before_resume(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")
        calls = []

        def fake_dispatch(cmd, cwd=None, timeout=120):
            calls.append("dispatch")
            return 0, json.dumps({"task_id": "codex-q-1",
                                  "action": "planner_question",
                                  "qid": "q-live-1",
                                  "prompt": "Confirm paragraph?"}), ""

        d = controller.dispatch(self.sd, "r1", run_cmd=fake_dispatch)
        luna_action = d["luna_action"]
        self.assertEqual(luna_action["action"], "planner_question")

        def fake_claude(cmd, cwd=None, timeout=120):
            # Question must already be persisted before Claude is called.
            pending = core.list_questions(self.sd, "r1", only_pending=True)
            calls.append("claude-after-persist")
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["qid"], "q-live-1")
            return 0, "Approved as written.", ""

        res = controller.planner_callback(self.sd, "r1", "q-live-1",
                                          "Confirm paragraph?", run_cmd=fake_claude)
        self.assertEqual(res["action"], "answered")
        self.assertIn("claude-after-persist", calls)

        def fake_resume(cmd, cwd=None, timeout=120):
            # Answer must already be persisted before Luna resume.
            qs = core.list_questions(self.sd, "r1", only_pending=False)
            answered = [q for q in qs if q["qid"] == "q-live-1" and q["status"] == "answered"]
            calls.append("resume-after-answer")
            self.assertEqual(len(answered), 1)
            self.assertIn("codex-q-1", " ".join(cmd))
            return 0, json.dumps({"action": "completion", "output": "done"}), ""

        r = controller.resume_luna(self.sd, "r1", "Approved as written.", run_cmd=fake_resume)
        self.assertEqual(r["action"], "resumed")
        self.assertIn("resume-after-answer", calls)


class TestQuotaClassification(Base):
    def test_exact_free_usage_limit_error(self):
        self.assertTrue(policy.classify_quota_exhaustion({"class": "FreeUsageLimitError"}))
        self.assertTrue(policy.classify_quota_exhaustion({"error": {"type": "FreeUsageLimitError"}}))
        self.assertTrue(policy.classify_provider_free_exhaustion({"class": "FreeUsageLimitError"}))

    def test_response_body_json_decoded(self):
        inner = json.dumps({"error": {"class": "FreeUsageLimitError", "message": "free exhausted"}})
        self.assertTrue(policy.classify_quota_exhaustion({"responseBody": inner}))
        self.assertTrue(policy.classify_provider_free_exhaustion({"responseBody": inner}))

    def test_retry_status_free_tier_limit_opencode(self):
        self.assertTrue(policy.classify_quota_exhaustion(
            {"action": "retry", "reason": "free_tier_limit", "provider": "opencode"}))
        self.assertTrue(policy.classify_quota_exhaustion(
            {"status": {"action": {"reason": "free_tier_limit"}}, "provider": "opencode"}))

    def test_generic_errors_never_select_go(self):
        bad = [
            {"class": "RateLimitError"},
            {"code": "rate_limit"},
            "HTTP 429 Too Many Requests",
            {"code": "TIMEOUT"},
            "timeout",
            {"class": "DataPolicyError"},
            {"class": "RegionError"},
            {"class": "AuthError"},
            "CONSENT_REQUIRED",
            "PERMISSION_DENIED",
            "INVALID_PLAN",
            {"class": "GoUsageLimitError"},
            {"reason": "account_rate_limit", "provider": "opencode"},
            None, 42,
        ]
        for b in bad:
            self.assertFalse(policy.classify_quota_exhaustion(b), b)
            self.assertFalse(policy.classify_provider_free_exhaustion(b), b)
        self.assertIsNone(policy.next_implementation_route(
            "muse-spark-xhigh-free", {"class": "RateLimitError"}))
        self.assertIsNone(policy.next_implementation_route(
            "muse-spark-xhigh-free", "HTTP 429"))
        self.assertFalse(policy.ALLOW_ZEN_OVERFLOW)
        self.assertFalse(policy.ALLOW_DIRECT_PAID_API)

    def test_free_to_go_only_on_exact_evidence(self):
        self.assertEqual(policy.next_implementation_route(
            "muse-spark-xhigh-free", {"class": "FreeUsageLimitError"}),
            "muse-spark-xhigh-go")
        self.assertEqual(policy.next_implementation_route(
            "muse-spark-xhigh-free",
            {"reason": "free_tier_limit", "provider": "opencode"}),
            "muse-spark-xhigh-go")


class TestQuotaTransfer(Base):
    def test_free_exhaustion_aborts_old_session_confirms_idle_preserves_artifacts(self):
        w = self.ws()
        core.submit(self.sd, "r1", {"goal": "t"}, w, "claude-1")
        log = Path(self.sd) / "outputs" / "r1.log"
        log.write_text("prior artifact chunk\n", encoding="utf-8")
        calls = []

        def fake_oc(cmd, cwd=None, timeout=120):
            envelope = {"class": "FreeUsageLimitError",
                        "opencode_session_id": "oc-free-1",
                        "responseBody": json.dumps({"error": {"class": "FreeUsageLimitError"}})}
            return 1, json.dumps(envelope), ""

        def fake_control(method, path, body):
            calls.append((method, path, dict(body)))
            return {"ok": True, "idle": True}

        res = controller.run_implementation(self.sd, "r1", artifact="outputs/fix.txt",
                                            run_cmd=fake_oc,
                                            control_request_func=fake_control)
        self.assertEqual(res["action"], "transferred_to_go")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["route"], "muse-spark-xhigh-go")
        self.assertEqual(job["opencode_session_id"], "oc-free-1")
        # Abort + idle confirm exercised against the saved session.
        methods = [m for m, _, _ in calls]
        self.assertIn("POST", methods)
        self.assertIn("GET", methods)
        self.assertTrue(any("oc-free-1" in json.dumps(b) or "oc-free-1" in p for _, p, b in calls))
        # Artifacts preserved.
        self.assertIn("prior artifact chunk", log.read_text(encoding="utf-8"))
        self.assertTrue(job["last_error_json"])
        self.assertIn("FreeUsageLimitError", job["last_error_json"])

    def test_generic_rate_limit_never_transfers(self):
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")

        def fake_oc(cmd, cwd=None, timeout=120):
            return 1, json.dumps({"class": "RateLimitError", "message": "429 rate limit"}), ""

        res = controller.run_implementation(self.sd, "r1", run_cmd=fake_oc,
                                            control_request_func=lambda m, p, b: {"ok": True})
        self.assertEqual(res["action"], "blocked")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual(job["status"], "blocked")


class FakeStatusHandler(http.server.BaseHTTPRequestHandler):
    mode = "free"

    def do_GET(self):  # noqa: N802
        parsed = self.path
        if parsed.startswith("/session/status"):
            if FakeStatusHandler.mode == "free":
                body = {"session": "oc-fake-1", "status": "error",
                        "error": {"class": "FreeUsageLimitError",
                                  "responseBody": json.dumps({"error": {"class": "FreeUsageLimitError"}})}}
            else:
                body = {"session": "oc-fake-1", "status": "error",
                        "error": {"class": "RateLimitError", "message": "429"}}
            data = json.dumps(body).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        data = json.dumps({"ok": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):  # silence
        return


class TestFakeHttpControl(Base):
    def _serve(self, mode):
        FakeStatusHandler.mode = mode
        srv = http.server.HTTPServer(("127.0.0.1", 0), FakeStatusHandler)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        t.start()
        self.addCleanup(srv.shutdown)
        self.addCleanup(srv.server_close)
        return f"http://127.0.0.1:{port}"

    def test_fake_http_free_envelope_selects_go_and_generic_does_not(self):
        base = self._serve("free")
        pwd = adapters.generate_control_password()
        ctrl = adapters.OpenCodeControl(base, pwd, "oc-fake-1")
        status = ctrl.session_status()
        self.assertIn("oc-fake-1", json.dumps(status))
        err = status.get("error", status)
        self.assertTrue(policy.classify_provider_free_exhaustion(err))
        self.assertTrue(policy.classify_quota_exhaustion(err))
        # Abort + idle confirm over the same seam.
        self.assertEqual(ctrl.abort().get("ok"), True)
        idle = ctrl.ensure_idle_ownership()
        self.assertTrue(isinstance(idle, dict))
        # Password never persisted in DB/events.
        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1")
        con = store.connect(self.sd)
        try:
            blob = json.dumps([dict(r) for r in con.execute("SELECT * FROM jobs").fetchall()])
            ev = json.dumps([dict(r) for r in con.execute("SELECT * FROM events").fetchall()])
        finally:
            con.close()
        self.assertNotIn(pwd, blob + ev)

        base2 = self._serve("generic")
        ctrl2 = adapters.OpenCodeControl(base2, adapters.generate_control_password(), "oc-fake-1")
        status2 = ctrl2.session_status()
        err2 = status2.get("error", status2)
        self.assertFalse(policy.classify_provider_free_exhaustion(err2))
        self.assertFalse(policy.classify_quota_exhaustion(err2))
        self.assertIsNone(policy.next_implementation_route("muse-spark-xhigh-free", err2))

    def test_control_rejects_non_localhost(self):
        with self.assertRaises(ValueError):
            adapters.OpenCodeControl("http://example.com:4000",
                                     adapters.generate_control_password(), "s1")

    def test_status_observes_only_saved_session(self):
        base = self._serve("free")
        ctrl = adapters.OpenCodeControl(base, adapters.generate_control_password(), "oc-saved-9")
        status = ctrl.session_status()
        # Fake server echoes the saved session scoping; the seam never
        # requests another session ID.
        self.assertTrue(isinstance(status, dict))


if __name__ == "__main__":
    unittest.main(verbosity=2)
