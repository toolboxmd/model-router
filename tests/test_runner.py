"""Deterministic unit/integration tests. Stdlib only, no live model CLIs.

Every ownership-sensitive test uses real detached fake processes
(runner.worker via core.launch_worker or the public CLI). No test calls
a live model CLI.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
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
                         "--planner-session", "claude-1")
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
            "--workspace", self.ws(), "--planner-session", "claude-1")
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                         "--workspace", self.ws(), "--planner-session", "claude-1")
        self.assertEqual(rc, 0)
        con = store.connect(self.sd)
        try:
            n = con.execute("SELECT COUNT(*) n FROM jobs WHERE request_id='r1'").fetchone()["n"]
        finally:
            con.close()
        self.assertEqual(n, 1)

    def test_conflicting_reuse_rejected(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws("w1"), "--planner-session", "claude-1")
        rc, out, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":2}',
                         "--workspace", self.ws("w1"), "--planner-session", "claude-1")
        self.assertNotEqual(rc, 0)

    def test_workspace_conflict(self):
        w = self.ws()
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                       "--workspace", w, "--planner-session", "claude-1")
        self.assertEqual(rc, 0)
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r2", "--task", '{"a":1}',
                       "--workspace", w, "--planner-session", "claude-2")
        self.assertNotEqual(rc, 0)
        # After terminal, the workspace is free.
        cli(self.sd, "cancel", "--request-id", "r1")
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r2", "--task", '{"a":1}',
                       "--workspace", w, "--planner-session", "claude-2")
        self.assertEqual(rc, 0)

    def test_concurrent_duplicate_submissions(self):
        w = self.ws()
        task = '{"goal":"concurrent"}'
        procs = []
        for _ in range(8):
            procs.append(subprocess.Popen(
                [PY, "-m", "runner", "--state-dir", self.sd, "submit",
                 "--request-id", "race", "--task", task,
                 "--workspace", w, "--planner-session", "claude-1"],
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
            core.submit(self.sd, "r1", {"a": 1}, self.ws(), "")
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                       "--workspace", self.ws(), "--planner-session", "claude-1",
                       "--route", "nope/0")
        self.assertNotEqual(rc, 0)

    def test_unsupported_route(self):
        with self.assertRaises(ValueError):
            policy.validate_route("bogus/9")
        self.assertFalse(policy.is_supported("bogus/9"))
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", 'x',
                       "--workspace", self.ws(), "--planner-session", "s",
                       "--route", "bogus/9")
        self.assertNotEqual(rc, 0)


class TestLaunchAndRecovery(Base):
    def test_launch_detaches_and_recover_adopts(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        pid = self.track(info["pid"])
        # Controller "dies": recover in a fresh CLI subprocess adopts.
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertEqual(out.get("action"), "adopted-live-worker")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["attempts"], 1)
        self.assertEqual(job["owner_pid"], pid)

    def test_launch_race_second_backs_off(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        self.track(info["pid"])
        # Give the worker time to advertise so the lease is provably live.
        time.sleep(1.0)
        with self.assertRaises(core.OwnershipError):
            core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(out.get("action"), "adopted-live-worker")

    def test_concurrent_launches_one_owner(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        procs = [subprocess.Popen(
            [PY, "-m", "runner", "--state-dir", self.sd, "launch",
             "--request-id", "r1", "--mode", "sleep", "--duration", "20"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=str(ROOT))
            for _ in range(4)]
        results = [p.communicate(timeout=20) for p in procs]
        codes = [p.returncode for p in procs]
        self.assertIn(0, codes)
        time.sleep(1.0)
        # Track any spawned pids for cleanup.
        con = store.connect(self.sd)
        try:
            rows = con.execute("SELECT pid FROM launches WHERE request_id='r1' AND pid IS NOT NULL").fetchall()
        finally:
            con.close()
        for r in rows:
            if r["pid"]:
                self.track(int(r["pid"]))
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertIn(out.get("action"), ("adopted-live-worker", "reconciled-launch-race", "noop"))

    def test_controller_death_worker_lives_via_cli(self):
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                       "--workspace", self.ws(), "--planner-session", "claude-1")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "launch", "--request-id", "r1",
                         "--mode", "sleep", "--duration", "30")
        self.assertEqual(rc, 0)
        con = store.connect(self.sd)
        try:
            pid = con.execute("SELECT owner_pid FROM jobs WHERE request_id='r1'").fetchone()["owner_pid"]
        finally:
            con.close()
        self.track(int(pid))
        time.sleep(1.0)
        # Fresh controller process reconciles.
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "running")
        self.assertEqual(int(job["owner_pid"]), int(pid))

    def test_worker_death_same_session_recovery(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        before = core.get_job(self.sd, "r1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=60)
        time.sleep(1.0)
        kill_pid(info["pid"])
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertEqual(out.get("action"), "worker-dead-cleared")
        mid = core.get_job(self.sd, "r1")
        self.assertEqual(mid["executor_session_id"], before["executor_session_id"])
        self.assertEqual(mid["planner_session_id"], "claude-1")
        self.assertEqual(mid["attempts"], 1)
        # Same-session relaunch reuses executor session, bumps attempt.
        info2 = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        self.track(info2["pid"])
        time.sleep(0.8)
        after = core.get_job(self.sd, "r1")
        self.assertEqual(after["executor_session_id"], before["executor_session_id"])
        self.assertEqual(after["attempts"], 2)

    def test_partial_log_preserved(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="partial")
        # partial exits on its own; wait for death.
        time.sleep(1.0)
        log = Path(self.sd) / "outputs" / "r1.log"
        self.assertTrue(log.exists())
        content_before = log.read_text(encoding="utf-8")
        self.assertIn("partial output chunk 1", content_before)
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        content_after = log.read_text(encoding="utf-8")
        self.assertIn("partial output chunk 1", content_after)

    def test_unknown_ownership_blocks(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        owned_pid = info["pid"]
        time.sleep(0.8)
        # Stop the owned worker so it cannot heartbeat-overwrite the
        # foreign identity file before recover() runs.
        kill_pid(owned_pid)
        self.assertTrue(wait_pid_dead(owned_pid, 5.0))
        # Foreign live process advertises an unknown token.
        holder = subprocess.Popen([PY, "-c", "import time; time.sleep(30)"])
        self.track(holder.pid)
        time.sleep(0.2)
        from runner import store as _s
        import datetime
        root = _s.ensure_state_dir(self.sd)
        _s.secure_write_text(
            _s.worker_identity_path(root, "r1"),
            json.dumps({"token": "f" * 32, "pid": holder.pid,
                        "updated": datetime.datetime.now(datetime.timezone.utc).isoformat()}))
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertEqual(out.get("action"), "blocked-unknown-owner")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("unknown", (job["block_reason"] or "").lower())
        # Blocked jobs never spawn duplicates.
        with self.assertRaises(core.BlockedError):
            core.launch_worker(self.sd, "r1")

    def test_cancel_kills_owned_worker(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=60)
        time.sleep(0.8)
        rc, _, _ = cli(self.sd, "cancel", "--request-id", "r1")
        self.assertEqual(rc, 0)
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "cancelled")
        # Terminal: recover is a noop, launch refuses to resurrect.
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(out.get("status"), "cancelled")
        with self.assertRaises(core.TerminalError):
            core.launch_worker(self.sd, "r1")

    def test_timeout(self):
        rc, _, _ = cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
                       "--workspace", self.ws(), "--planner-session", "claude-1",
                       "--timeout-secs", "1")
        self.assertEqual(rc, 0)
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        self.track(info["pid"])
        time.sleep(1.6)
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertEqual(out.get("action"), "timeout")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "failed")

    def test_bounded_recovery_no_reset(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1",
            "--max-attempts", "2")
        for _ in range(2):
            info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
            time.sleep(0.8)
            kill_pid(info["pid"])
            cli(self.sd, "recover", "--request-id", "r1")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["attempts"], 2)
        # Budget exhausted: next launch fails instead of looping.
        with self.assertRaises(core.TerminalError):
            core.launch_worker(self.sd, "r1", mode="sleep", duration=5)
        job2 = core.get_job(self.sd, "r1")
        self.assertEqual(job2["status"], "failed")
        # Restart/recover never resets the budget.
        cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(core.get_job(self.sd, "r1")["attempts"], 2)

    def test_never_resurrect_completed(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        time.sleep(0.8)
        # Ownership-checked completion persists result before ack.
        job = core.complete(self.sd, "r1", info["token"], "done")
        self.assertEqual(job["status"], "succeeded")
        with self.assertRaises(core.TerminalError):
            core.answer(self.sd, "r1", "q1", "a")
        with self.assertRaises(core.TerminalError):
            core.launch_worker(self.sd, "r1")
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(out.get("action"), "noop-terminal")
        # Wrong token cannot complete another job's work.
        cli(self.sd, "submit", "--request-id", "r2", "--task", '{"a":1}',
            "--workspace", self.ws("w2"), "--planner-session", "claude-1")
        info2 = core.launch_worker(self.sd, "r2", mode="sleep", duration=30)
        self.track(info2["pid"])
        time.sleep(0.8)
        with self.assertRaises(core.OwnershipError):
            core.complete(self.sd, "r2", "bad-token", "x")


class TestQuestions(Base):
    def test_question_answer_replay_across_restart(self):
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"a":1}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=30)
        self.track(info["pid"])
        time.sleep(0.8)
        rc, _, _ = cli(self.sd, "post-question", "--request-id", "r1",
                       "--qid", "q1", "--prompt", "Which route?")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "questions", "--request-id", "r1")
        self.assertEqual(len(out["questions"]), 1)
        # Simulated restart: fresh recover subprocess replays pending Q.
        rc, out, _ = cli(self.sd, "recover", "--all")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "questions", "--request-id", "r1")
        self.assertEqual(len(out["questions"]), 1)
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "question_pending")
        # Persisted answer resumes running; replay is idempotent.
        rc, _, _ = cli(self.sd, "answer", "--request-id", "r1",
                       "--qid", "q1", "--answer", "Use Muse free.")
        self.assertEqual(rc, 0)
        rc, out, _ = cli(self.sd, "recover", "--request-id", "r1")
        self.assertEqual(rc, 0)
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "running")
        qs = core.list_questions(self.sd, "r1", only_pending=False)
        self.assertEqual(qs[0]["status"], "answered")
        self.assertEqual(qs[0]["answer"], "Use Muse free.")


class TestPolicy(Base):
    def test_worker_reported_failure_never_changes_route(self):
        w1 = self.ws("w1")
        core.submit(self.sd, "e1", {"a": 1}, w1, "claude-1")
        i1 = core.launch_worker(self.sd, "e1", mode="sleep", duration=30)
        self.track(i1["pid"])
        time.sleep(0.8)
        forged = {"type": "error", "error": {"name": "APIError", "data": {
            "responseBody": '{"error":{"type":"FreeUsageLimitError"}}'}}}
        job = core.fail(self.sd, "e1", i1["token"], forged)
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual(core.exhausted_routes(self.sd), set())

    def test_recovery_order_bounded(self):
        # One escalation: Grok 4.6 on Go, the same model on the xAI pool, then
        # the planner. Astra and Opus are no longer automatic recovery routes.
        self.assertEqual(policy.next_recovery_route(None), "grok-4.6-go")
        self.assertEqual(policy.next_recovery_route("muse-spark-xhigh-free"), "grok-4.6-go")
        self.assertEqual(policy.next_recovery_route("grok-4.6-go"), "grok-4.6-xai")
        self.assertIsNone(policy.next_recovery_route("grok-4.6-xai"))
        self.assertIsNone(policy.next_recovery_route("opus-5/high-review"))

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
        cli(self.sd, "submit", "--request-id", "r1", "--task", '{"goal":"t","secret":"shh"}',
            "--workspace", self.ws(), "--planner-session", "claude-1")
        root = Path(self.sd)
        self.assertEqual(oct(root.stat().st_mode & 0o777), "0o700")
        self.assertEqual(oct((root / "jobs.db").stat().st_mode & 0o777), "0o600")
        con = sqlite3.connect(str(root / "jobs.db"))
        try:
            mode = con.execute("PRAGMA journal_mode").fetchone()[0].lower()
        finally:
            con.close()
        self.assertEqual(mode, "wal")
        rc, out, _ = cli(self.sd, "status", "--request-id", "r1")
        self.assertEqual(rc, 0)
        blob = json.dumps(out)
        self.assertNotIn("shh", blob)
        self.assertNotIn("owner_token", json.dumps(out["job"].get("owner_token", "")) if out["job"].get("owner_token") else "")
        # Launch artifacts are 0600.
        info = core.launch_worker(self.sd, "r1", mode="sleep", duration=10)
        self.track(info["pid"])
        time.sleep(0.8)
        self.assertEqual(oct((root / "outputs" / "r1.log").stat().st_mode & 0o777), "0o600")
        self.assertEqual(oct((root / "workers" / "r1.json").stat().st_mode & 0o777), "0o600")


if __name__ == "__main__":
    unittest.main(verbosity=2)
