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
from tests.fakes import FAKE_OPENCODE, write_fake  # noqa: E402

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
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
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
    def test_stale_controller_cannot_spawn_or_complete(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='new' WHERE request_id='r1'")
        run = core.make_durable_run_cmd(self.sd, "r1", "old")
        with self.assertRaises(core.LeaseLostError):
            run(["true"], None, 5, kind="codex_dispatch")
        self.assertEqual(core._list_invocations(self.sd, "r1"), [])
        with self.assertRaises(core.LeaseLostError):
            controller._complete_job(self.sd, "r1", "old", "done")
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "succeeded")

    def test_recover_adopts_advertised_controller_without_ack(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        holder = self.sleeper()
        from runner.supervisor import process_start_identity
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
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=NULL, status='running',"
                     " codex_task_id='th' WHERE request_id='r1'")
        sql(self.sd, "INSERT INTO launches(request_id,attempt_no,start_token,state,created_at)"
                     " VALUES('r1',1,'tok','attempting',?)", (core._utcnow(),))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "launch-in-progress")
        self.assertEqual(core.get_job(self.sd, "r1")["owner_token"], "tok")

    def test_status_redacts_lease_token(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='secret-lease' WHERE request_id='r1'")
        rc, out = cli(self.sd, "status", "--request-id", "r1")
        self.assertNotIn("secret-lease", json.dumps(out))


class TestLunaIdentity(Base):
    def test_resume_reporting_another_thread_never_replaces_saved_task(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET codex_task_id='saved-thread', status='running',"
                     " controller_state=? WHERE request_id='r1'",
            (json.dumps({"seq": 1, "last_action": {"action": "completion", "output": "x"}}),))

        def fake(cmd, cwd=None, timeout=None, **kw):
            # A completed turn on another thread: the thread comparison
            # applies only after the exit code and turn.completed checks.
            return 0, "\n".join((
                json.dumps({"type": "thread.started", "thread_id": "forked"}),
                json.dumps({"type": "turn.completed"}),
            )) + "\n", ""

        res = controller.resume_luna(self.sd, "r1", "hello", run_cmd=fake)
        self.assertEqual(res["reason"], "luna_task_mismatch")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["codex_task_id"], "saved-thread")
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "blocked-sticky")
        self.assertIsNone(core.get_job(self.sd, "r1")["owner_pid"])


class TestWorkspaceClaims(Base):
    def test_cancelling_symlinked_and_nested_workspaces_stay_claimed(self):
        ws = self.ws("real")
        core.submit(self.sd, "r1", {"g": 1}, ws, "p")
        link = self.base / "link"
        link.symlink_to(ws)
        with self.assertRaises(core.WorkspaceConflictError):
            core.submit(self.sd, "r2", {"g": 2}, str(link), "p")
        nested = Path(ws) / "sub"
        nested.mkdir()
        with self.assertRaises(core.WorkspaceConflictError):
            core.submit(self.sd, "r3", {"g": 3}, str(nested), "p")
        sql(self.sd, "UPDATE jobs SET status='cancelling', cancel_requested=1 WHERE request_id='r1'")
        with self.assertRaises(core.WorkspaceConflictError):
            core.submit(self.sd, "r4", {"g": 4}, ws, "p")
        with self.assertRaises(core.TerminalError):
            core.start_controller(self.sd, "r1", spawn=lambda cmd: 1)

    def test_changed_execution_settings_conflict(self):
        ws = self.ws()
        core.submit(self.sd, "r1", {"g": 1}, ws, "p", max_attempts=3)
        with self.assertRaises(core.ConflictError):
            core.submit(self.sd, "r1", {"g": 1}, ws, "p", max_attempts=5)
        with self.assertRaises(core.ConflictError):
            core.submit(self.sd, "r1", {"g": 1}, ws, "p", timeout_secs=60)


class TestConsumption(Base):
    def _codex_invocation(self, rc, envelope, completed=True):
        root = store.ensure_state_dir(self.sd)
        out = root / "outputs" / "x.stdout"
        lines = [{"type": "thread.started", "thread_id": "th"},
                 {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(envelope)}}]
        if completed:
            lines.append({"type": "turn.completed"})
        store.secure_write_text(out, "\n".join(json.dumps(x) for x in lines) + "\n")
        store.secure_write_text(root / "outputs" / "x.stderr", "")
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "pid,pgid,stdout_path,stderr_path,started_at,state,rc) VALUES"
                     "('inv1','r1','codex_resume','[]','/w','t',999999,999999,?,?,?,?,?)",
            (str(out), str(root / "outputs" / "x.stderr"), core._utcnow(),
             "completed" if rc == 0 else "failed", rc))

    def test_failed_turn_action_is_not_applied(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        self._codex_invocation(1, {"action": "completion", "output": "done"})
        core.consume_finished_invocations(self.sd, "r1")
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "succeeded")

    def test_cancellation_wins_over_a_consumed_completion(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        self._codex_invocation(0, {"action": "completion", "output": "done"})
        sql(self.sd, "UPDATE jobs SET cancel_requested=1, status='cancelling' WHERE request_id='r1'")
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "cancelled")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "cancelled")


class TestCapacityMemory(Base):
    def test_memory_switch_keeps_original_evidence_and_operator_can_clear(self):
        # The job starts on Muse free (which takes every new job); capacity
        # recorded afterwards moves it at preflight, keeping the evidence.
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        self.assertEqual(core.get_job(self.sd, "r1")["route"], "muse-spark-xhigh-free")
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "exhausted", FREE_STATUS)
        res = controller.run_implementation(self.sd, "r1", run_cmd=lambda *a, **k: (0, "", ""))
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


class TestRoundThree(Base):
    """Regressions for the second Luna max and Opus 5 high reviews."""

    def _insert_inv(self, inv_id, owner="tok", kind="codex_dispatch", pid=None, start=None,
                    started_at=None, key=None, meta=None, state="running"):
        root = store.ensure_state_dir(self.sd)
        out = root / "outputs" / f"{inv_id}.stdout"
        store.secure_write_text(out, "")
        store.secure_write_text(root / "outputs" / f"{inv_id}.stderr", "")
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "pid,pgid,process_start,stdout_path,stderr_path,started_at,state,timeout_secs,"
                     "action_key,meta_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (inv_id, "r1", kind, json.dumps(["true"]), self.ws(), owner, pid, pid, start, str(out),
             str(root / "outputs" / f"{inv_id}.stderr"), started_at or core._utcnow(), state, 5,
             key, json.dumps(meta or {})))

    def test_supervisor_never_spawns_after_cancel_or_lease_loss(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        marker = self.base / "spawned"
        for inv_id, setup in (("i-cancel", "UPDATE jobs SET owner_token='tok', cancel_requested=1 WHERE request_id='r1'"),
                              ("i-lease", "UPDATE jobs SET owner_token='other', cancel_requested=0 WHERE request_id='r1'")):
            sql(self.sd, setup)
            self._insert_inv(inv_id)
            sql(self.sd, "UPDATE invocations SET cmd_json=? WHERE invocation_id=?",
                (json.dumps(["touch", str(marker)]), inv_id))
            rc = subprocess.run([PY, "-m", "runner.supervisor", "--state-dir", self.sd,
                                 "--request-id", "r1", "--invocation-id", inv_id],
                                cwd=str(ROOT), timeout=30).returncode
            self.assertEqual(rc, 3)
            inv = [i for i in core._list_invocations(self.sd, "r1") if i["invocation_id"] == inv_id][0]
            self.assertEqual((inv["state"], inv["pid"]), ("abandoned", None))
        self.assertFalse(marker.exists())

    def test_row_never_claimed_by_a_supervisor_is_abandoned_not_blocking(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        self._insert_inv("i-old", started_at="2000-01-01T00:00:00+00:00")
        self._insert_inv("i-new")
        own = {i["invocation_id"]: core._invocation_ownership(i) for i in core._list_invocations(self.sd, "r1")}
        self.assertEqual(own, {"i-old": "never_started", "i-new": "unresolved"})
        self.assertEqual(core._abandon_never_started(self.sd, "r1"), 1)

    def test_unknown_owner_identity_blocks_and_is_never_signalled(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        other = self.sleeper()
        sql(self.sd, "UPDATE jobs SET owner_token='t', owner_pid=?, owner_start=NULL, status='running',"
                     " codex_task_id='th' WHERE request_id='r1'", (other.pid,))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "blocked-claimed-live-pid")
        self.assertFalse(core._signal_pid(other.pid, None, signal.SIGTERM))
        time.sleep(0.2)
        self.assertIsNone(other.poll())

    def test_stale_controller_writes_are_refused(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='new' WHERE request_id='r1'")
        controller._LEASE["token"] = "old"
        self.addCleanup(controller._LEASE.__setitem__, "token", None)
        with self.assertRaises(core.LeaseLostError):
            controller._persist_envelope(self.sd, "r1", {"action": "completion"}, "resumed")
        with self.assertRaises(core.LeaseLostError):
            controller._mark_blocked(self.sd, "r1", "x")
        with self.assertRaises(core.LeaseLostError):
            core.post_question(self.sd, "r1", "q1", "?", lease_token="old")
        job = core.get_job(self.sd, "r1")
        self.assertIsNone(job["controller_state"])
        self.assertNotEqual(job["status"], "blocked")

    def test_live_turn_without_completion_event_is_not_accepted(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        env = {"action": "completion", "output": "x"}

        def fake(cmd, cwd=None, timeout=None, **kw):
            return 0, "\n".join(json.dumps(x) for x in (
                {"type": "thread.started", "thread_id": "th"},
                {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(env)}})), ""

        res = controller.dispatch(self.sd, "r1", run_cmd=fake,
                                    probe=lambda *a: None)
        self.assertEqual(res["action"], "blocked")
        self.assertEqual(core.get_job(self.sd, "r1")["codex_task_id"], "th")

    def test_status_shows_no_model_text(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET controller_state=?, last_error_json=? WHERE request_id='r1'",
            (json.dumps({"seq": 2, "phase": "resumed", "last_action_name": "completion",
                         "last_action": {"action": "completion", "output": "MODEL-TEXT"},
                         "implementation_output": "WORKER-TEXT"}),
             json.dumps({"source": "codex_resume", "rc": 1, "stderr": "STDERR-TEXT"})))
        rc, out = cli(self.sd, "status", "--request-id", "r1")
        blob = json.dumps(out)
        for text in ("MODEL-TEXT", "WORKER-TEXT", "STDERR-TEXT"):
            self.assertNotIn(text, blob)
        self.assertEqual(out["job"]["controller_state"]["seq"], 2)

    def test_case_variant_path_is_the_same_workspace(self):
        real = self.base / "CaseDir"
        real.mkdir()
        variant = self.base / "casedir"
        if not variant.exists():
            self.skipTest("case-sensitive filesystem")
        core.submit(self.sd, "r1", {"g": 1}, str(real), "p")
        with self.assertRaises(core.WorkspaceConflictError):
            core.submit(self.sd, "r2", {"g": 2}, str(variant), "p")

    def test_cancellation_takes_precedence_over_timeout(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", timeout_secs=1)
        sql(self.sd, "UPDATE jobs SET cancel_requested=1, status='cancelling',"
                     " created_at='2000-01-01T00:00:00+00:00' WHERE request_id='r1'")
        self.assertEqual(core.recover_one(self.sd, "r1")["action"], "cancelled")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "cancelled")

    def test_public_answer_during_callback_wins_without_blocking(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")

        def fake_claude(cmd, cwd=None, timeout=None, **kw):
            core.answer(self.sd, "r1", "q1", "Human answer.")
            return 0, json.dumps({"type": "result", "subtype": "success", "is_error": False,
                                  "result": "Planner answer.", "session_id": "p"}), ""

        res = controller.planner_callback(self.sd, "r1", "q1", "Which?", run_cmd=fake_claude)
        self.assertEqual(res["action"], "answered")
        q = core.list_questions(self.sd, "r1", only_pending=False)[0]
        self.assertEqual(q["answer"], "Human answer.")
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "blocked")

    def test_other_live_invocation_is_waited_on_not_run_beside(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok' WHERE request_id='r1'")
        short = subprocess.Popen(["sleep", "1.5"], start_new_session=True)
        self.procs.append(short)
        from runner.supervisor import process_start_identity
        self._insert_inv("i-other", pid=short.pid, start=process_start_identity(short.pid), key="k-other")
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        t0 = time.monotonic()
        rc, out, err = run(["true"], None, 10, kind="codex_dispatch")
        self.assertGreater(time.monotonic() - t0, 1.0, "must wait for the other child")
        self.assertEqual(rc, 0)
        other = [i for i in core._list_invocations(self.sd, "r1") if i["invocation_id"] == "i-other"][0]
        self.assertNotIn(other["state"], core.LIVE_INVOCATION_STATES)


class TestRoundFour(Base):
    """Regressions for the third Luna max and Opus 5 high reviews."""

    def test_cancel_before_supervisor_claim_closes_the_row(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok' WHERE request_id='r1'")
        root = store.ensure_state_dir(self.sd)
        for name in ("i1.stdout", "i1.stderr"):
            store.secure_write_text(root / "outputs" / name, "")
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "stdout_path,stderr_path,started_at,state) VALUES('i1','r1','codex_dispatch',?,?,"
                     "'tok',?,?,?,'running')",
            (json.dumps(["sleep", "30"]), self.ws(), str(root / "outputs" / "i1.stdout"),
             str(root / "outputs" / "i1.stderr"), core._utcnow()))
        # cancel marks the row cancelling before the supervisor gets to claim it
        sql(self.sd, "UPDATE jobs SET cancel_requested=1, status='cancelling' WHERE request_id='r1'")
        sql(self.sd, "UPDATE invocations SET state='cancelling' WHERE invocation_id='i1'")
        rc = subprocess.run([PY, "-m", "runner.supervisor", "--state-dir", self.sd, "--request-id", "r1",
                             "--invocation-id", "i1"], cwd=str(ROOT), timeout=30).returncode
        self.assertEqual(rc, 3)
        inv = core._list_invocations(self.sd, "r1")[0]
        self.assertEqual((inv["state"], inv["rc"], inv["pid"]), ("abandoned", 125, None))
        self.assertEqual(core.recover_one(self.sd, "r1")["action"], "cancelled")

    def test_termination_during_a_run_stops_the_child_and_records_it(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok' WHERE request_id='r1'")
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        import threading
        box = {}
        t = threading.Thread(target=lambda: box.update(res=run(["sleep", "30"], None, 60,
                                                               kind="codex_dispatch")))
        t.start()
        self.assertTrue(_wait(lambda: any(i.get("pid") for i in core._list_invocations(self.sd, "r1"))))
        inv = core._list_invocations(self.sd, "r1")[0]
        os.kill(int(inv["supervisor_pid"]), signal.SIGTERM)
        t.join(20)
        inv = core._list_invocations(self.sd, "r1")[0]
        self.assertEqual(inv["state"], "failed")
        self.assertFalse(core._is_pgid_alive(inv["pgid"]))
        self.assertNotEqual(box["res"][0], 0)

    def test_unknown_identity_of_the_lease_holder_is_not_adopted(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        holder = self.sleeper()
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=NULL, status='running' WHERE request_id='r1'")
        store.secure_write_text(Path(self.sd) / "workers" / "r1.json", json.dumps(
            {"token": "tok", "pid": holder.pid, "updated": core._utcnow(), "start": None}))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "blocked-unknown-owner")
        self.assertIsNone(core.get_job(self.sd, "r1")["owner_pid"])

    def test_timeout_origin_is_kept_after_a_pending_drain(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", timeout_secs=1)
        sql(self.sd, "UPDATE jobs SET cancel_requested=2, status='blocked',"
                     " block_reason='timeout_pending: live child process group remains' WHERE request_id='r1'")
        core.recover_one(self.sd, "r1")
        job = core.get_job(self.sd, "r1")
        self.assertEqual((job["status"], job["error_class"]), ("failed", "timeout"))

    def test_status_summarizes_result_and_result_command_prints_it(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET status='succeeded', result_json=? WHERE request_id='r1'",
            (json.dumps({"ok": True, "output": json.dumps({"output": "REPORT-TEXT"})}),))
        rc, out = cli(self.sd, "status", "--request-id", "r1")
        self.assertNotIn("REPORT-TEXT", json.dumps(out))
        self.assertTrue(out["job"]["result_json"]["ok"])
        rc, out = cli(self.sd, "result", "--request-id", "r1")
        self.assertEqual(out["result"]["output"]["output"], "REPORT-TEXT")

    def test_missing_workspace_is_rejected(self):
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r1", {"g": 1}, str(self.base / "missing"), "p")

    def test_go_route_records_the_go_model_at_submit(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", route="muse-spark-xhigh-go")
        self.assertEqual(core.get_job(self.sd, "r1")["model"], "opencode-go/muse-spark-1.3-contributor")


def _wait(fn, secs=15.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if fn():
            return True
        time.sleep(0.05)
    return False


class TestRoundFive(Base):
    """Regressions for the fourth Luna max and Opus 5 high reviews."""

    def test_free_controller_lock_proves_an_unacknowledged_launch_is_gone(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=NULL, status='running',"
                     " codex_task_id='th' WHERE request_id='r1'")
        sql(self.sd, "INSERT INTO launches(request_id,attempt_no,start_token,state,created_at)"
                     " VALUES('r1',1,'tok','attempting','2000-01-01T00:00:00+00:00')")
        fd = core.take_controller_lock(self.sd, "r1")
        self.assertIsNotNone(fd)
        try:
            self.assertTrue(core.controller_lock_held(self.sd, "r1"))
            out = core.recover_one(self.sd, "r1")
            # This process holds the lock, as a live but unacknowledged
            # controller would: recover must leave it alone.
            self.assertEqual(out["action"], "launch-in-progress")
            self.assertIsNone(core.take_controller_lock(self.sd, "r1", wait=0.1))
        finally:
            os.close(fd)
        self.assertFalse(core.controller_lock_held(self.sd, "r1"))

    def test_lock_held_by_another_process_is_seen(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        code = ("import sys,time; sys.path.insert(0, %r); from runner import core; "
                "fd=core.take_controller_lock(%r, 'r1'); print('locked', flush=True); time.sleep(30)"
                % (str(ROOT), self.sd))
        holder = subprocess.Popen([PY, "-c", code], stdout=subprocess.PIPE, text=True,
                                  start_new_session=True)
        self.procs.append(holder)
        self.assertEqual(holder.stdout.readline().strip(), "locked")
        self.assertTrue(core.controller_lock_held(self.sd, "r1"))
        os.killpg(holder.pid, signal.SIGKILL)
        holder.wait(5)
        self.assertFalse(core.controller_lock_held(self.sd, "r1"))

    def test_cancel_written_last_wins_and_mirror_matches(self):
        for intent, status in ((1, "cancelled"), (2, "failed")):
            sd = str(self.base / f"s{intent}")
            core.submit(sd, "r1", {"g": 1}, self.ws(f"w{intent}"), "p")
            sql(sd, "UPDATE jobs SET cancel_requested=?, status='cancelling' WHERE request_id='r1'", (intent,))
            core._finalize_stopped(sd, "r1", True)
            job = core.get_job(sd, "r1")
            self.assertEqual(job["status"], status)
            mirror = json.loads((Path(sd) / "outputs" / "r1.result.json").read_text())
            self.assertEqual(mirror["status"], status)
            core._finalize_stopped(sd, "r1", False)  # a late pending drain never un-terminates
            self.assertEqual(core.get_job(sd, "r1")["status"], status)

    def test_uncommitted_child_is_found_through_the_side_record(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        child = self.sleeper()
        from runner.supervisor import child_record_path, process_start_identity
        root = store.ensure_state_dir(self.sd)
        out = root / "outputs" / "i1.stdout"
        store.secure_write_text(out, "")
        store.secure_write_text(Path(child_record_path(str(out))), json.dumps(
            {"pid": child.pid, "pgid": child.pid, "start": process_start_identity(child.pid)}))
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "supervisor_pid,stdout_path,stderr_path,started_at,state) VALUES"
                     "('i1','r1','codex_dispatch','[]',?,'tok',999999,?,?,?,'running')",
            (self.ws(), str(out), str(out), core._utcnow()))
        inv = core._list_invocations(self.sd, "r1")[0]
        self.assertEqual(core._invocation_ownership(inv), "live")
        core._terminate_invocations(self.sd, "r1", None, signal.SIGTERM)
        child.wait(5)
        self.assertIsNotNone(child.poll())

    def test_identical_resubmission_survives_a_removed_workspace(self):
        ws = Path(self.ws("gone"))
        core.submit(self.sd, "r1", {"g": 1}, str(ws), "p")
        ws.rmdir()
        job = core.submit(self.sd, "r1", {"g": 1}, str(ws), "p")
        self.assertEqual(job["request_id"], "r1")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r2", {"g": 2}, str(ws), "p")

    def test_unclaimed_row_under_a_live_controller_is_normal_startup(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        holder = self.sleeper()
        from runner.supervisor import process_start_identity
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=?, owner_start=?, status='running'"
                     " WHERE request_id='r1'", (holder.pid, process_start_identity(holder.pid)))
        root = store.ensure_state_dir(self.sd)
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "stdout_path,stderr_path,started_at,state) VALUES"
                     "('i1','r1','codex_dispatch','[]',?,'tok',?,?,?,'running')",
            (self.ws(), str(root / "x"), str(root / "x"), core._utcnow()))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "invocation-starting")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "running")


class TestRoundSix(Base):
    """Regressions for the fifth Luna max and Opus 5 high reviews."""

    def test_superseded_controller_exits_without_advertising(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='current' WHERE request_id='r1'")
        env = dict(os.environ, DURABLE_RUNNER_TOKEN="stale")
        rc = subprocess.run([PY, "-m", "runner.controller", "--state-dir", self.sd, "--request-id", "r1"],
                            cwd=str(ROOT), env=env, timeout=30).returncode
        self.assertEqual(rc, 0)
        self.assertFalse((Path(self.sd) / "workers" / "r1.json").exists())
        self.assertEqual(core.get_job(self.sd, "r1")["owner_token"], "current")

    def _claimed_row(self, started_at):
        root = store.ensure_state_dir(self.sd)
        store.secure_write_text(root / "outputs" / "i1.stdout", "")
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "supervisor_pid,supervisor_start,stdout_path,stderr_path,started_at,state) VALUES"
                     "('i1','r1','codex_dispatch','[]',?,'tok',999999,'x',?,?,?,'running')",
            (self.ws(), str(root / "outputs" / "i1.stdout"), str(root / "outputs" / "i1.stdout"), started_at))

    def test_dead_claiming_supervisor_without_child_record_never_started(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        self._claimed_row("2000-01-01T00:00:00+00:00")
        inv = core._list_invocations(self.sd, "r1")[0]
        self.assertEqual(core._invocation_ownership(inv), "never_started")
        fd = core.take_controller_lock(self.sd, "r1")
        try:
            self.assertEqual(core._abandon_never_started(self.sd, "r1"), 0, "a lock holder may still start it")
        finally:
            os.close(fd)
        self.assertEqual(core._abandon_never_started(self.sd, "r1"), 1)
        self.assertEqual(core._list_invocations(self.sd, "r1")[0]["state"], "abandoned")

    def test_recent_claimed_row_with_dead_supervisor_stays_unresolved(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        self._claimed_row(core._utcnow())
        inv = core._list_invocations(self.sd, "r1")[0]
        self.assertEqual(core._invocation_ownership(inv), "unresolved")

    def test_child_writes_side_record_and_spawn_failures_close_the_row(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok' WHERE request_id='r1'")
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        rc, out, err = run(["sh", "-c", "echo $$", "a\x00b"], None, 10, kind="codex_dispatch")
        self.assertEqual(rc, 0)
        inv = core._list_invocations(self.sd, "r1")[0]
        from runner.supervisor import child_record_path
        rec = json.loads(Path(child_record_path(inv["stdout_path"])).read_text())
        self.assertEqual(rec["pid"], inv["pid"])
        self.assertEqual(out.strip(), str(inv["pid"]))
        rc, out, err = run(["true"], str(self.base / "no-such-dir"), 10, kind="codex_resume")
        self.assertEqual(rc, 127)
        bad = [i for i in core._list_invocations(self.sd, "r1") if i["kind"] == "codex_resume"][0]
        self.assertEqual(bad["state"], "failed")
        self.assertIn("spawn failed", bad["result_json"])

    def test_lease_checked_questions_refuse_a_cancelled_job(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', cancel_requested=1, status='cancelling' WHERE request_id='r1'")
        with self.assertRaises(core.LeaseLostError):
            core.post_question(self.sd, "r1", "q1", "?", lease_token="tok")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "cancelling")


class TestRoundSeven(Base):
    """Regressions for the sixth Opus 5 high review."""

    def test_relative_state_dir_with_another_workspace(self):
        ws = self.ws("repo")
        other = self.base / "elsewhere"
        other.mkdir()
        env = dict(os.environ)
        rc = subprocess.run([PY, "-c", (
            "import os,sys; sys.path.insert(0, %r); os.chdir(%r)\n"
            "from runner import core, store\n"
            "core.submit('rel-state', 'r1', {'g': 1}, %r, 'p')\n"
            "con = store.connect('rel-state'); con.execute(\"UPDATE jobs SET owner_token='tok'\"); con.close()\n"
            "rc, out, err = core.make_durable_run_cmd('rel-state', 'r1', 'tok')(['sh', '-c', 'echo hi'], None, 10, kind='codex_dispatch')\n"
            "print(rc, out.strip())\n") % (str(ROOT), str(other), ws)],
            capture_output=True, text=True, timeout=60, env=env)
        self.assertEqual(rc.stdout.strip(), "0 hi", rc.stderr[-2000:])
        self.assertTrue(any((other / "rel-state" / "outputs").glob("*.child.json")))
        self.assertFalse(any(Path(ws).rglob("*.child.json")))

    def test_starting_controller_holding_the_lock_is_not_blocked(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        holder = self.sleeper()
        from runner.supervisor import process_start_identity
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=?, owner_start=?, status='running',"
                     " codex_task_id='th' WHERE request_id='r1'", (holder.pid, process_start_identity(holder.pid)))
        fd = core.take_controller_lock(self.sd, "r1")
        try:
            out = core.recover_one(self.sd, "r1")
        finally:
            os.close(fd)
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "blocked", out)


class TestRoundEight(Base):
    """Regressions for the seventh Luna max and Opus 5 high reviews."""

    def test_recently_acknowledged_launch_is_not_blocked_before_its_handshake(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        holder = self.sleeper()
        from runner.supervisor import process_start_identity
        sql(self.sd, "UPDATE jobs SET owner_token='tok', owner_pid=?, owner_start=?, status='running'"
                     " WHERE request_id='r1'", (holder.pid, process_start_identity(holder.pid)))
        sql(self.sd, "INSERT INTO launches(request_id,attempt_no,start_token,pid,state,created_at,ack_at)"
                     " VALUES('r1',1,'tok',?,'acknowledged',?,?)", (holder.pid, core._utcnow(), core._utcnow()))
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "launch-in-progress")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "running")

    def test_lock_race_loser_releases_only_its_own_lease(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
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

    def test_collected_dispatch_is_reused_after_the_controller_dies(self):
        # Hermetic probe: the step below runs the default pre-dispatch
        # probe, which must not consult the operator's live Codex account.
        cx_home = self.base / "cx-home"
        cx_home.mkdir(exist_ok=True)
        saved = {k: os.environ.get(k) for k in ("MODEL_ROUTER_CODEX_HOME", "CODEX_HOME")}
        os.environ["MODEL_ROUTER_CODEX_HOME"] = str(cx_home)
        os.environ.pop("CODEX_HOME", None)
        self.addCleanup(self._restore_env, saved)
        ws = self.ws()
        core.submit(self.sd, "r1", {"g": 1}, ws, "p")
        job = core.get_job(self.sd, "r1")
        from runner import adapters
        last = controller._last_message_path(self.sd, "r1", "codex-dispatch-last")
        cmd = adapters.build_codex_dispatch_cmd(job["workspace"], controller._full_luna_prompt(job["task_json"]),
                                                last_message_path=last)
        env = {"action": "completion", "output": "DONE"}
        root = store.ensure_state_dir(self.sd)
        out = root / "outputs" / "d1.stdout"
        store.secure_write_text(out, "\n".join(json.dumps(x) for x in (
            {"type": "thread.started", "thread_id": "th-1"},
            {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(env)}},
            {"type": "turn.completed"})) + "\n")
        store.secure_write_text(root / "outputs" / "d1.stderr", "")
        # Finished and collected by a controller that died before saving
        # the action; the supervisor had already saved the thread ID.
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "pid,pgid,stdout_path,stderr_path,started_at,state,rc,consumed_at,action_key) VALUES"
                     "('d1','r1','codex_dispatch',?,?,'old',999999,999999,?,?,?,'completed',0,?,?)",
            (json.dumps(cmd), job["workspace"], str(out), str(root / "outputs" / "d1.stderr"),
             core._utcnow(), core._utcnow(), core._action_key("codex_dispatch", cmd, None)))
        sql(self.sd, "UPDATE jobs SET codex_task_id='th-1', owner_token='tok', status='running' WHERE request_id='r1'")
        res = controller.step(self.sd, "r1", run_cmd=core.make_durable_run_cmd(self.sd, "r1", "tok"))
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res["luna_action"]["action"], "completion")
        self.assertEqual(len(core._list_invocations(self.sd, "r1")), 1, "no second dispatch")
        self.assertEqual(os.stat(last).st_mode & 0o777, 0o600)


class TestRoundNine(Base):
    """Regressions for the eighth Luna max review."""

    def test_recover_resumes_a_collected_dispatch_without_a_saved_thread(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        root = store.ensure_state_dir(self.sd)
        store.secure_write_text(root / "outputs" / "d1.stdout", "")
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "pid,pgid,stdout_path,stderr_path,started_at,state,rc,consumed_at) VALUES"
                     "('d1','r1','codex_dispatch','[]',?,'old',999999,999999,?,?,?,'completed',0,?)",
            (self.ws(), str(root / "outputs" / "d1.stdout"), str(root / "outputs" / "d1.stdout"),
             core._utcnow(), core._utcnow()))
        sql(self.sd, "UPDATE jobs SET owner_token='old', owner_pid=999999, status='running', attempts=1"
                     " WHERE request_id='r1'")
        spawned = []
        orig = core.start_controller
        core.start_controller = lambda sd, rid, **kw: spawned.append(rid) or {"pid": 1}
        self.addCleanup(setattr, core, "start_controller", orig)
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "resumed-controller")
        self.assertEqual(spawned, ["r1"])

    def test_existing_last_message_file_is_made_private(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        path = Path(controller._last_message_path(self.sd, "r1", "x"))
        os.chmod(path, 0o644)
        controller._last_message_path(self.sd, "r1", "x")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)


class TestRoundTen(Base):
    """Regressions for the ninth Luna max review."""

    def _consumable(self, kind, rc, thread, completed=True):
        root = store.ensure_state_dir(self.sd)
        lines = []
        if thread:
            lines.append({"type": "thread.started", "thread_id": thread})
        lines.append({"type": "item.completed", "item": {"type": "agent_message",
                                                          "text": json.dumps({"action": "completion", "output": "X"})}})
        if completed:
            lines.append({"type": "turn.completed"})
        store.secure_write_text(root / "outputs" / "c1.stdout", "\n".join(json.dumps(x) for x in lines) + "\n")
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "pid,pgid,stdout_path,stderr_path,started_at,state,rc) VALUES"
                     "('c1','r1',?,'[]',?,'old',999999,999999,?,?,?,?,?)",
            (kind, self.ws(), str(root / "outputs" / "c1.stdout"), str(root / "outputs" / "c1.stdout"),
             core._utcnow(), "completed" if rc == 0 else "failed", rc))

    def test_consumed_resume_from_another_thread_blocks_and_is_not_applied(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET codex_task_id='saved', status='running' WHERE request_id='r1'")
        self._consumable("codex_resume", 0, "forked")
        core.consume_finished_invocations(self.sd, "r1")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("luna_task_mismatch", job["block_reason"])
        self.assertIsNone(job["controller_state"])

    def test_consumed_resume_without_thread_is_not_applied(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET codex_task_id='saved', status='running' WHERE request_id='r1'")
        self._consumable("codex_resume", 0, None)
        core.consume_finished_invocations(self.sd, "r1")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "blocked")

    def test_consumed_failed_dispatch_blocks_instead_of_stranding(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='old', owner_pid=999999, status='running', attempts=1"
                     " WHERE request_id='r1'")
        self._consumable("codex_dispatch", 1, None, completed=False)
        out = core.recover_one(self.sd, "r1")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "blocked", out)
        self.assertIn("codex_dispatch_failed", job["block_reason"])


class TestRoundEleven(Base):
    """Regressions for the tenth Opus 5 high review."""

    def _dead_callback(self, rc, out=""):
        root = store.ensure_state_dir(self.sd)
        store.secure_write_text(root / "outputs" / "cb.stdout", out)
        sql(self.sd, "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                     "pid,pgid,stdout_path,stderr_path,started_at,state,rc,meta_json) VALUES"
                     "('cb','r1','claude_callback','[]',?,'old',999999,999999,?,?,?,?,?,?)",
            (self.ws(), str(root / "outputs" / "cb.stdout"), str(root / "outputs" / "cb.stdout"),
             core._utcnow(), "running" if rc is None else "failed", rc, json.dumps({"qid": "q1"})))

    def _job_with_question(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET codex_task_id='th', owner_token='old', owner_pid=999999, attempts=1,"
                     " controller_state=? WHERE request_id='r1'",
            (json.dumps({"seq": 1, "last_action": {"action": "planner_question", "qid": "q1", "prompt": "?"}}),))
        core.post_question(self.sd, "r1", "q1", "?")

    def test_answer_then_recover_after_a_dead_callback_resumes(self):
        self._job_with_question()
        self._dead_callback(None)
        core.answer(self.sd, "r1", "q1", "Descending.")
        spawned = []
        orig = core.start_controller
        core.start_controller = lambda sd, rid, **kw: spawned.append(rid) or {"pid": 1}
        self.addCleanup(setattr, core, "start_controller", orig)
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "resumed-controller", out)
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "blocked")

    def test_failed_callback_with_open_question_blocks_with_its_reason(self):
        self._job_with_question()
        self._dead_callback(1)
        core.consume_finished_invocations(self.sd, "r1")
        job = core.get_job(self.sd, "r1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("planner_callback_failed", job["block_reason"])

    def test_live_callback_failure_after_public_answer_uses_the_answer(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")

        def fake(cmd, cwd=None, timeout=None, **kw):
            core.answer(self.sd, "r1", "q1", "Human answer.")
            return 1, "", "api error"

        res = controller.planner_callback(self.sd, "r1", "q1", "?", run_cmd=fake)
        self.assertEqual(res["action"], "answered")
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "blocked")

    def test_live_resume_without_thread_blocks(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET codex_task_id='saved' WHERE request_id='r1'")

        def fake(cmd, cwd=None, timeout=None, **kw):
            return 0, json.dumps({"type": "turn.completed"}), ""

        res = controller.resume_luna(self.sd, "r1", "x", run_cmd=fake)
        self.assertEqual(res["reason"], "luna_task_mismatch")
