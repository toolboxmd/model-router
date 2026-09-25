"""Deterministic unit/integration tests. Stdlib only, no live model CLIs.

Every ownership-sensitive test uses real detached fake processes
through the public CLI or the harness fakes. No test calls a live model
CLI.
"""
from __future__ import annotations

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

from runner import controller, core, policy, store  # noqa: E402

PY = sys.executable


def cli(state_dir, *args, timeout=20):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(ROOT))
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(path: Path, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


def wait_pid_dead(pid, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            waited, _ = os.waitpid(int(pid), os.WNOHANG)
            if waited == int(pid):
                return True
        except ChildProcessError:
            pass
        except (ValueError, OverflowError, OSError):
            return True
        try:
            os.kill(int(pid), 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        time.sleep(0.05)
    return False


def kill_pid(pid):
    try:
        os.kill(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        return
    wait_pid_dead(pid, 5.0)


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
        # Kill any workers left in this state dir.
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


class TestSubmit(Base):
    def test_persists_before_ack(self):
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1",
                         "--task", '{"goal":"t"}', "--workspace", self.ws(),
                         "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3")
        self.assertEqual(rc, 0)
        self.assertTrue(out.get("acknowledged"))
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["policy_id"], policy.POLICY_ID)
        self.assertEqual(job["planner_session_id"], "claude-1")
        self.assertTrue(job["executor_session_id"].startswith("exec-"))
        self.assertTrue(job["output_path"].endswith("r1.log"))
        self.assertEqual(job["status"], "pending")
        self.assertTrue(Path(job["output_path"]).exists())
        con = store.connect(self.sd)
        try:
            ev = con.execute("SELECT kind FROM events WHERE request_id='r1'").fetchall()
        finally:
            con.close()
        self.assertIn("submitted", [r["kind"] for r in ev])

    def test_idempotent_same_payload(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3")
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                         "--workspace", self.ws(), "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3")
        self.assertEqual(rc, 0)
        con = store.connect(self.sd)
        try:
            n = con.execute("SELECT COUNT(*) n FROM jobs WHERE request_id='r1'").fetchone()["n"]
        finally:
            con.close()
        self.assertEqual(n, 1)

    def test_conflicting_reuse_rejected(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws("w1"), "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3")
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":2}',
                         "--workspace", self.ws("w1"), "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3")
        self.assertNotEqual(rc, 0)

    def test_workspace_conflict(self):
        w = self.ws()
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                       "--workspace", w, "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3")
        self.assertEqual(rc, 0)
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r2", "--task", '{"a":1}',
                       "--workspace", w, "--planner-session", "claude-2", "--planner-t3-thread", "planner-t3")
        self.assertNotEqual(rc, 0)
        # After terminal, the workspace is free.
        cli(self.sd, "cancel", "--request-id", "r1")
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r2", "--task", '{"a":1}',
                       "--workspace", w, "--planner-session", "claude-2", "--planner-t3-thread", "planner-t3")
        self.assertEqual(rc, 0)

    def test_concurrent_duplicate_submissions(self):
        w = self.ws()
        task = '{"goal":"concurrent"}'
        procs = []
        for _ in range(8):
            procs.append(subprocess.Popen(
                [PY, "-m", "runner", "--state-dir", self.sd, "submit",
                 "--request-id", "race", "--task", task,
                 "--workspace", w, "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=str(ROOT)))
        outs = [p.communicate(timeout=20) for p in procs]
        codes = [p.returncode for p in procs]
        self.assertTrue(all(c == 0 for c in codes), codes)
        con = store.connect(self.sd)
        try:
            n = con.execute("SELECT COUNT(*) n FROM jobs WHERE request_id='race'").fetchone()["n"]
        finally:
            con.close()
        self.assertEqual(n, 1)

    def test_missing_session_rejected(self):
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r1", {"a": 1}, self.ws(), "", planner_t3_thread="planner-t3")
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                       "--workspace", self.ws(), "--planner-session", "claude-1", "--planner-t3-thread", "planner-t3",
                       "--route", "nope/0")
        self.assertNotEqual(rc, 0)

    def test_unsupported_route(self):
        with self.assertRaises(ValueError):
            policy.validate_route("bogus/9")
        self.assertFalse(policy.is_supported("bogus/9"))
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", 'x',
                       "--workspace", self.ws(), "--planner-session", "s", "--planner-t3-thread", "planner-t3",
                       "--route", "bogus/9")
        self.assertNotEqual(rc, 0)


class TestPolicy(Base):
    def test_recovery_order_bounded(self):
        # One escalation: Grok 4.6 on Go, native Grok Build on the xAI pool,
        # then OpenCode's xAI provider, then the planner. Pool moves inside
        # recovery are not second escalations.
        self.assertEqual(policy.next_recovery_route(None), "grok-4.6-go")
        self.assertEqual(policy.next_recovery_route("muse-spark-xhigh-free"), "grok-4.6-go")
        self.assertEqual(policy.next_recovery_route("grok-4.6-go"), "grok-4.6-build")
        self.assertEqual(policy.next_recovery_route("grok-4.6-build"), "grok-4.6-xai")
        self.assertIsNone(policy.next_recovery_route("grok-4.6-xai"))
        self.assertIsNone(policy.next_recovery_route("opus-5.5/high-review"))

    def test_envelope(self):
        a = policy.make_action("implementation", "muse-spark-xhigh-free", {"t": 1})
        self.assertEqual(a["policy"], policy.POLICY_ID)
        r = policy.make_result("completion", True, "done")
        self.assertTrue(r["ok"])
        with self.assertRaises(ValueError):
            policy.make_action("nope", "muse-spark-xhigh-free")
        with self.assertRaises(ValueError):
            policy.make_action("implementation", "bogus/0")


class TestHygiene(Base):
    def test_perms_wal_redaction(self):
        core.submit(self.sd, "r1", {"secret_token": "abc"}, self.ws(), "p", planner_t3_thread="planner-t3")
        root = Path(self.sd)
        controller._advertise(self.sd, "r1", "tok-secret")
        self.assertEqual(oct(root.stat().st_mode & 0o777), "0o700")
        self.assertEqual(oct((root / "jobs.db").stat().st_mode & 0o777), "0o600")
        self.assertEqual(oct((root / "workers" / "r1.json").stat().st_mode & 0o777), "0o600")
        view = core.status_view(self.sd, "r1")
        self.assertNotIn("tok-secret", json.dumps(view))
        self.assertNotIn("task_json", view["job"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
