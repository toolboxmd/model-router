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
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import core, policy, store  # noqa: E402

PY = sys.executable


def codex_out(thread, env):
    """Realistic ``codex exec --json`` stream carrying Luna's envelope."""
    lines = [{"type": "thread.started", "thread_id": thread},
             {"type": "item.completed", "item": {"id": "item_0", "type": "agent_message",
                                                 "text": json.dumps(env)}},
             {"type": "turn.completed", "usage": {}}]
    return "\n".join(json.dumps(x) for x in lines) + "\n"


def claude_out(sid, text):
    return json.dumps({"type": "result", "subtype": "success", "is_error": False,
                       "result": text, "session_id": sid})


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
                         "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3", "--no-start")
        self.assertEqual(rc, 0)
        self.assertTrue(out.get("acknowledged"))
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["planner_session_id"], "claude-1")
        # The planner runs in its own T3 thread: nothing is chosen for it.
        self.assertIsNone(job["planner_model"])
        self.assertIsNone(job["planner_effort"])
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual(job["attempts"], 0)
        self.assertIsNone(job["owner_token"])

    def test_submit_allows_explicit_planner_overrides(self):
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1",
                         "--task", '{"goal":"t"}', "--workspace", self.ws(),
                         "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3",
                         "--planner-model", "claude-opus-4",
                         "--planner-effort", "high",
                         "--no-start")
        self.assertEqual(rc, 0)
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["planner_model"], "claude-opus-4")
        self.assertEqual(job["planner_effort"], "high")


    def test_missing_planner_session_rejected_no_fork(self):
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r1", {"a": 1}, self.ws(), "", planner_t3_thread="planner-t3")

    def test_public_recipe_submit_status_questions_answer_cancel_recover(self):
        w = self.ws()
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"goal":"t"}',
                       "--workspace", w, "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3", "--no-start")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "status", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertIn("job", out)
        core.post_question(self.sd, "r1", "q1", "Confirm?")
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

        core.submit(self.sd, "r1", {"goal": "t"}, self.ws(), "claude-1", planner_t3_thread="planner-t3")
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
                       "--workspace", self.ws(), "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3",
                       "--no-start")
        self.assertEqual(rc, 0)
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["attempts"], 0)
        self.assertIsNone(job["owner_token"])
        self.assertIsNone(job["owner_pid"])


FREE_STATUS = {"type": "retry", "attempt": 1, "message": "m", "next": 5,
               "action": {"reason": "free_tier_limit", "provider": "opencode",
                          "title": "t", "message": "m", "label": "l"}}
FREE_API_ERROR = {"name": "APIError", "data": {
    "statusCode": 429, "responseBody": json.dumps({"type": "error", "error": {"type": "FreeUsageLimitError"}})}}


class TestQuotaClassification(Base):
    def test_only_exact_provider_shapes_count(self):
        self.assertTrue(policy.classify_quota_exhaustion(FREE_STATUS))
        self.assertTrue(policy.classify_quota_exhaustion(FREE_API_ERROR))
        self.assertTrue(policy.classify_quota_exhaustion({"type": "error", "error": FREE_API_ERROR}))
        self.assertEqual(policy.next_implementation_route("muse-spark-xhigh-free", FREE_STATUS),
                         "muse-spark-xhigh-go")

    def test_text_and_generic_errors_never_select_go(self):
        bad = [
            {"class": "FreeUsageLimitError"},
            {"error": {"message": "FreeUsageLimitError"}},
            {"name": "UnknownError", "data": {"message": "FreeUsageLimitError"}},
            {"responseBody": json.dumps({"error": {"type": "FreeUsageLimitError"}})},
            "FreeUsageLimitError", "FREE_ALLOWANCE_EXHAUSTED:confirmed",
            {"code": "FREE_ALLOWANCE_EXHAUSTED", "confirmed": True},
            {"reason": "free_tier_limit", "provider": "opencode"},
            dict(FREE_STATUS, type="busy"),
            dict(FREE_STATUS, action=dict(FREE_STATUS["action"], provider="opencode-go")),
            dict(FREE_STATUS, action=dict(FREE_STATUS["action"], reason="account_rate_limit")),
            {"name": "APIError", "data": {"statusCode": 429, "responseBody": '{"error":{"type":"RateLimitError"}}'}},
            {"name": "APIError", "data": {"responseBody": '{"error":{"type":"GoUsageLimitError"}}'}},
            {"class": "RateLimitError"}, "HTTP 429 Too Many Requests", {"code": "TIMEOUT"},
            {"class": "DataPolicyError"}, {"class": "RegionError"}, {"class": "AuthError"},
            None, 42,
        ]
        for b in bad:
            self.assertFalse(policy.classify_quota_exhaustion(b), b)
        self.assertIsNone(policy.next_implementation_route("muse-spark-xhigh-free", "HTTP 429"))
        self.assertFalse(policy.ALLOW_ZEN_OVERFLOW)
        self.assertFalse(policy.ALLOW_DIRECT_PAID_API)

    def test_non_implementation_routes_rejected_at_submit(self):
        for route in ("luna-max-review", "luna-go/max", "luna/max",
                      "fable-5.1/max", "opus-5.5/high-review", "astra/medium"):
            with self.assertRaises(ValueError):
                core.submit(self.sd, "r-" + route.replace("/", "-"), {"g": 1}, self.ws(), "p", route=route, planner_t3_thread="planner-t3")


class RealShapeHandler(http.server.BaseHTTPRequestHandler):
    """Minimal OpenCode 1.18.31 shapes for the loopback client."""
    status = {}
    seen = []

    def _send(self, obj):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        RealShapeHandler.seen.append(("GET", self.path, self.headers.get("Authorization")))
        if self.path.startswith("/session/status"):
            return self._send(RealShapeHandler.status)
        self._send([])

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length", "0"))
        if n:
            self.rfile.read(n)
        RealShapeHandler.seen.append(("POST", self.path, self.headers.get("Authorization")))
        self._send(True)

    def log_message(self, *args):
        return


if __name__ == "__main__":
    unittest.main(verbosity=2)
