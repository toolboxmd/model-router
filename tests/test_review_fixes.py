"""Regressions for the independent Luna max and Opus 5 high review findings."""
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

from runner import controller, core, store  # noqa: E402

PY = sys.executable
FREE_STATUS = {"type": "retry", "attempt": 1, "message": "m", "next": 1,
               "action": {"reason": "free_tier_limit", "provider": "opencode",
                          "title": "t", "message": "m", "label": "l"}}


def cli(sd, *args, env=None):
    p = subprocess.run([PY, "-m", "runner", "--state-dir", sd, *args], capture_output=True,
                       text=True, timeout=30, cwd=str(ROOT), env=env)
    try:
        return p.returncode, json.loads(p.stdout or "{}")
    except ValueError:
        return p.returncode, {"raw": p.stdout}


def sql(sd, q, args=()):
    con = store.connect(sd)
    try:
        con.execute(q, args)
    finally:
        con.close()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.sd = str(self.base / "state")
        self.procs = []
        self.addCleanup(self._kill)

    def _kill(self):
        for p in self.procs:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except Exception:
                pass

    @staticmethod
    def _restore_env(saved):
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def ws(self, name="w"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def sleeper(self):
        p = subprocess.Popen(["sleep", "30"], start_new_session=True)
        self.procs.append(p)
        return p


class TestProcessIdentity(Base):
    def test_reused_pid_is_neither_owner_nor_signalled(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        other = self.sleeper()
        sql(self.sd, "UPDATE jobs SET owner_token='t', owner_pid=?, owner_start=? WHERE request_id='r1'",
            (other.pid, "Thu Jan  1 00:00:00 1970"))
        job = core.get_job(self.sd, "r1")
        self.assertFalse(core._owner_alive(job))
        core._signal_pid(other.pid, "Thu Jan  1 00:00:00 1970", signal.SIGTERM)
        core._signal_group(other.pid, other.pid, "Thu Jan  1 00:00:00 1970", signal.SIGTERM)
        time.sleep(0.2)
        self.assertIsNone(other.poll(), "an unrelated process must not be signalled")
        rc, out = cli(self.sd, "cancel", "--request-id", "r1")
        self.assertEqual(rc, 0)
        time.sleep(0.2)
        self.assertIsNone(other.poll())
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "cancelled")


class TestLease(Base):
    def test_stale_controller_cannot_complete(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        sql(self.sd, "UPDATE jobs SET owner_token='new' WHERE request_id='r1'")
        with self.assertRaises(core.LeaseLostError):
            controller._complete_job(self.sd, "r1", "old", "done")
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "succeeded")

    def test_recover_adopts_advertised_controller_without_ack(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        holder = self.sleeper()
        from runner.core import _process_start_identity as process_start_identity
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=NULL, status='running' WHERE request_id='r1'")
        sql(self.sd, "INSERT INTO launches(request_id,attempt_no,start_token,state,created_at)"
                     " VALUES('r1',1,'tok','attempting','2000-01-01T00:00:00+00:00')")
        # Heartbeat is old: a controller refreshes it only between steps.
        store.secure_write_text(Path(self.sd) / "workers" / "r1.json", json.dumps(
            {"token": "tok", "pid": holder.pid, "updated": "2000-01-01T00:00:00+00:00",
             "start": process_start_identity(holder.pid)}))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "adopted-live-worker", out)
        job = core.get_job(self.sd, "r1")
        self.assertEqual((job["owner_token"], job["owner_pid"]), ("tok", holder.pid))
        rows = core.status_view(self.sd, "r1")["launches"]
        self.assertEqual([r["state"] for r in rows], ["acknowledged"])

    def test_recent_unacknowledged_launch_is_left_alone(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=NULL, status='running',"
                     " controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')) WHERE request_id='r1'")
        sql(self.sd, "INSERT INTO launches(request_id,attempt_no,start_token,state,created_at)"
                     " VALUES('r1',1,'tok','attempting',?)", (core._utcnow(),))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "launch-in-progress")
        self.assertEqual(core.get_job(self.sd, "r1")["owner_token"], "tok")

    def test_status_redacts_lease_token(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        sql(self.sd, "UPDATE jobs SET owner_token='secret-lease' WHERE request_id='r1'")
        rc, out = cli(self.sd, "status", "--request-id", "r1")
        self.assertNotIn("secret-lease", json.dumps(out))


class TestWorkspaceClaims(Base):
    def test_cancelling_symlinked_and_nested_workspaces_stay_claimed(self):
        ws = self.ws("real")
        core.submit(self.sd, "r1", {"g": 1}, ws, "p", planner_t3_thread="planner-t3")
        link = self.base / "link"
        link.symlink_to(ws)
        with self.assertRaises(core.WorkspaceConflictError):
            core.submit(self.sd, "r2", {"g": 2}, str(link), "p", planner_t3_thread="planner-t3")
        nested = Path(ws) / "sub"
        nested.mkdir()
        with self.assertRaises(core.WorkspaceConflictError):
            core.submit(self.sd, "r3", {"g": 3}, str(nested), "p", planner_t3_thread="planner-t3")
        sql(self.sd, "UPDATE jobs SET status='cancelling', cancel_requested=1 WHERE request_id='r1'")
        with self.assertRaises(core.WorkspaceConflictError):
            core.submit(self.sd, "r4", {"g": 4}, ws, "p", planner_t3_thread="planner-t3")
        with self.assertRaises(core.TerminalError):
            core.start_controller(self.sd, "r1", spawn=lambda cmd: 1)

    def test_changed_execution_settings_conflict(self):
        ws = self.ws()
        core.submit(self.sd, "r1", {"g": 1}, ws, "p", max_attempts=3, planner_t3_thread="planner-t3")
        with self.assertRaises(core.ConflictError):
            core.submit(self.sd, "r1", {"g": 1}, ws, "p", max_attempts=5, planner_t3_thread="planner-t3")
        with self.assertRaises(core.ConflictError):
            core.submit(self.sd, "r1", {"g": 1}, ws, "p", timeout_secs=60, planner_t3_thread="planner-t3")


class TestCapacityMemory(Base):
    def test_memory_switch_keeps_original_evidence_and_operator_can_clear(self):
        # The job starts on Muse free (which takes every new job); capacity
        # recorded afterwards moves it at preflight, keeping the evidence.
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        self.assertEqual(core.get_job(self.sd, "r1")["route"], "muse-spark-xhigh-free")
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "exhausted", FREE_STATUS)
        res = controller.run_implementation(self.sd, "r1")
        self.assertEqual(res["action"], "transferred_to_go")
        rows = core.list_capacity(self.sd)
        self.assertEqual(len(rows), 1)
        self.assertIn("free_tier_limit", rows[0]["evidence_json"])
        rc, out = cli(self.sd, "capacity")
        self.assertEqual(out["capacity"][0]["route"], "muse-spark-xhigh-free")
        rc, out = cli(self.sd, "capacity", "--clear", "muse-spark-xhigh-free")
        self.assertEqual(rc, 0)
        self.assertEqual(core.exhausted_routes(self.sd), set())


class TestRedaction(Base):
    def test_nested_secrets_are_redacted(self):
        got = store.redact_for_log({"prompt": "p", "env": {"OPENCODE_SERVER_PASSWORD": "x", "PATH": "/b"},
                                    "list": [{"api_key": "k"}]})
        self.assertEqual(got, {"prompt": "p", "env": {"OPENCODE_SERVER_PASSWORD": "<redacted>", "PATH": "/b"},
                               "list": [{"api_key": "<redacted>"}]})


if __name__ == "__main__":
    unittest.main(verbosity=2)


def _wait(fn, secs=15.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if fn():
            return True
        time.sleep(0.05)
    return False


class TestRoundSeven(Base):
    """Regressions for the sixth Opus 5 high review."""


    def test_starting_controller_holding_the_lock_is_not_blocked(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        holder = self.sleeper()
        from runner.core import _process_start_identity as process_start_identity
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=?, owner_start=?, status='running',"
                     " controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')) WHERE request_id='r1'", (holder.pid, process_start_identity(holder.pid)))
        fd = core.take_controller_lock(self.sd, "r1")
        try:
            out = core.recover_one(self.sd, "r1")
        finally:
            os.close(fd)
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "blocked", out)


class TestRoundEight(Base):
    """Regressions for the seventh Luna max and Opus 5 high reviews."""

    def test_recently_acknowledged_launch_is_not_blocked_before_its_handshake(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        holder = self.sleeper()
        from runner.core import _process_start_identity as process_start_identity
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=?, owner_start=?, status='running'"
                     " WHERE request_id='r1'", (holder.pid, process_start_identity(holder.pid)))
        sql(self.sd, "INSERT INTO launches(request_id,attempt_no,start_token,pid,state,created_at,ack_at)"
                     " VALUES('r1',1,'tok',?,'acknowledged',?,?)", (holder.pid, core._utcnow(), core._utcnow()))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "launch-in-progress")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "running")

    def test_lock_race_loser_releases_only_its_own_lease(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        code = ("import sys,time; sys.path.insert(0, %r); from runner import core; "
                "fd=core.take_controller_lock(%r, 'r1'); print('locked', flush=True); time.sleep(30)"
                % (str(ROOT), self.sd))
        holder = subprocess.Popen([PY, "-c", code], stdout=subprocess.PIPE, text=True,
                                  start_new_session=True)
        self.procs.append(holder)
        self.assertEqual(holder.stdout.readline().strip(), "locked")
        for token, owner, expect_event in (("stale", "current", False), ("current", "current", True)):
            sql(self.sd, "UPDATE jobs SET owner_token=? WHERE request_id='r1'", (owner,))
            env = dict(os.environ, DURABLE_RUNNER_TOKEN=token)
            subprocess.run([PY, "-m", "runner.controller", "--state-dir", self.sd, "--request-id", "r1"],
                           cwd=str(ROOT), env=env, timeout=30)
            job = core.get_job(self.sd, "r1")
            self.assertEqual(job["owner_token"], None if expect_event else "current")
            kinds = [e["kind"] for e in core.status_view(self.sd, "r1")["recent_events"]]
            self.assertEqual("controller_exited" in kinds, expect_event)


