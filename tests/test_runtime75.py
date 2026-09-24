"""Compatible installed-runtime recovery after a removed plugin runtime.

Issue 75: a recoverable failure caused by a removed plugin runtime
continues through the currently installed compatible Model Router
runtime, preserving the job and its work. Recovery onto the installed
runtime replaces permanent per-job runtime copies for this rare event.

Every test uses disposable package copies and isolated state; the real
installed plugin cache is never moved or deleted and no real agent
process is used. A fixture proves the recovery path, never universal
upgrade compatibility.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, runtime, store  # noqa: E402
from runner.supervisor import process_start_identity  # noqa: E402

PY = sys.executable
DISPATCH_META = {"stage": "dispatch", "route": "luna/max", "reason": "initial"}
RESUME_META = {"stage": "dispatch", "route": "luna/max", "reason": "resume"}


def sql(sd, q, args=()):
    con = store.connect(sd)
    try:
        con.execute(q, args)
    finally:
        con.close()


def events(sd, request_id):
    con = store.connect(sd)
    try:
        return [dict(r) for r in con.execute(
            "SELECT kind, payload_json FROM events WHERE request_id=? ORDER BY id",
            (request_id,)).fetchall()]
    finally:
        con.close()


def payloads(sd, request_id, kind):
    return [json.loads(e["payload_json"]) for e in events(sd, request_id)
            if e["kind"] == kind]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.sd = str(self.base / "state")
        self.procs = []
        self.addCleanup(self._kill)
        self._saved_pkg_roots = (core._PKG_ROOT, runtime.PKG_ROOT)
        self.addCleanup(self._restore_pkg_roots)

    def _kill(self):
        for p in self.procs:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except Exception:
                pass

    def _restore_pkg_roots(self):
        core._PKG_ROOT, runtime.PKG_ROOT = self._saved_pkg_roots

    def ws(self, name="w"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def sleeper(self):
        p = subprocess.Popen(["sleep", "30"], start_new_session=True)
        self.procs.append(p)
        return p

    def disposable_runtime_copy(self, name="old-plugin-cache"):
        """A disposable stand-in for an installed plugin runtime copy."""
        dest = self.base / name / "model-router"
        shutil.copytree(ROOT, dest,
                        ignore=shutil.ignore_patterns(".git", "__pycache__", "*.pyc"))
        return dest

    def point_runtimes_at(self, root):
        core._PKG_ROOT = str(root)
        runtime.PKG_ROOT = str(root)

    def remove_runtime(self, root):
        """Simulate a plugin update removing the old runtime path."""
        shutil.rmtree(str(root), ignore_errors=True)

    def install_fake_codex(self):
        """A disposable `codex` that exits 0, for post-repair turns."""
        bindir = self.base / "fakebin"
        bindir.mkdir(exist_ok=True)
        codex = bindir / "codex"
        codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        codex.chmod(0o700)
        saved = os.environ.get("PATH", "")
        os.environ["PATH"] = str(bindir) + os.pathsep + saved
        self.addCleanup(lambda: os.environ.__setitem__("PATH", saved))


class NeverStartedSpawnBranch(Base):
    """The exact never-started supervisor spawn-error branch, with evidence."""

    def test_removed_runtime_is_diagnosed_retryable_and_not_memoized(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='r1'")
        old = self.disposable_runtime_copy()
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        cmd = ["codex", "exec", "--json", "drill"]
        rc, out, err = run(cmd, self.ws(), 10, kind="codex_dispatch", meta=dict(DISPATCH_META))
        self.assertEqual(rc, 127)
        self.assertIn("runtime_missing", err)
        self.assertIn("recover", err)
        self.assertIn(str(old), err)
        rows = core._list_invocations(self.sd, "r1")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((row["state"], row["rc"]), ("failed", 127))
        result = json.loads(row["result_json"])
        self.assertTrue(result.get("never_started"))
        self.assertTrue(result.get("runtime_missing"))
        self.assertIn(str(old), result.get("error", ""))
        spawn_ev = payloads(self.sd, "r1", "supervisor_spawn_failed")
        self.assertEqual(len(spawn_ev), 1)
        self.assertEqual(spawn_ev[0]["next_action"], "recover")
        # The failed action is not memoized: the same action retries fresh.
        key = core._action_key("codex_dispatch", cmd, DISPATCH_META)
        self.assertIsNone(core._prior_action_result(self.sd, "r1", key))
        # Repair means the installed runtime: point back and remake the
        # action exactly once. A disposable fake `codex` lets the
        # post-repair turn complete without any real agent process.
        self._restore_pkg_roots()
        self.install_fake_codex()
        sql(self.sd, "UPDATE jobs SET owner_token='tok2' WHERE request_id='r1'")
        run2 = core.make_durable_run_cmd(self.sd, "r1", "tok2")
        rc, out, err = run2(cmd, self.ws(), 30, kind="codex_dispatch", meta=dict(DISPATCH_META))
        self.assertEqual(rc, 0)
        rows = [i for i in core._list_invocations(self.sd, "r1")
                if i.get("action_key") == key]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["state"], "failed")
        self.assertTrue(json.loads(rows[0]["result_json"]).get("runtime_missing"))
        self.assertEqual((rows[1]["state"], rows[1]["rc"]), ("completed", 0))
        self.assertFalse(runtime.is_runtime_missing_row(rows[1]))

    def test_non_runtime_spawn_failure_keeps_sticky_semantics(self):
        # A missing job workspace is also provably never started, but the
        # installed runtime cannot repair it: recovery keeps the sticky
        # block instead of restarting.
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='r1'")
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        cmd = ["true"]
        rc, _, _ = run(cmd, str(self.base / "no-such-dir"), 10,
                       kind="codex_dispatch", meta=dict(DISPATCH_META))
        self.assertEqual(rc, 127)
        row = core._list_invocations(self.sd, "r1")[0]
        result = json.loads(row["result_json"])
        self.assertTrue(result.get("never_started"))
        self.assertFalse(result.get("runtime_missing", False))
        sql(self.sd, "UPDATE jobs SET owner_token=NULL, owner_pid=NULL, owner_start=NULL,"
                     " status='running' WHERE request_id='r1'")
        real_start = core.start_controller
        core.start_controller = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no new execution on an unrepairable spawn failure"))
        try:
            out = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(out["action"], "blocked-failed-dispatch")
        self.assertTrue(core.get_job(self.sd, "r1")["block_reason"].startswith(
            "codex_dispatch_failed rc=127"))


class ResumeRecoveryLoop(Base):
    """The T3 loop: resume fails on the removed runtime, recover continues."""

    def _stub_start(self, calls):
        def fake_start(state_dir, request_id):
            calls.append(request_id)
            con = store.connect(state_dir)
            try:
                con.execute("BEGIN IMMEDIATE")
                con.execute(
                    "INSERT INTO launches(request_id,attempt_no,start_token,state,created_at)"
                    " VALUES(?,?,?,'acknowledged',?)",
                    (request_id, 2, "tok-new", core._utcnow()))
                con.execute(
                    "UPDATE jobs SET owner_token=?, owner_pid=?, owner_start=?, updated_at=?"
                    " WHERE request_id=?",
                    ("tok-new", 999999999, "dead", core._utcnow(), request_id))
                con.execute("COMMIT")
            finally:
                con.close()
            return {"request_id": request_id, "pid": 999999999}
        return fake_start

    def test_resume_blocks_recoverably_then_recover_transfers_execution(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        before = core.get_job(self.sd, "r1")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running',"
                     " codex_task_id='thr-old', opencode_session_id='ses-old'"
                     " WHERE request_id='r1'")
        old = self.disposable_runtime_copy()
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        out = controller.resume_luna(self.sd, "r1", "follow up", run_cmd=run)
        self.assertEqual(out, {"action": "blocked", "reason": "runtime_missing"})
        job = core.get_job(self.sd, "r1")
        self.assertTrue(job["block_reason"].startswith("runtime_missing:"))
        self.assertIn("recover", job["block_reason"])
        self.assertIn(str(old), job["block_reason"])
        # The old controller is gone; recover from the installed runtime.
        self._restore_pkg_roots()
        sql(self.sd, "UPDATE jobs SET owner_token=NULL, owner_pid=NULL, owner_start=NULL"
                     " WHERE request_id='r1'")
        calls = []
        real_start = core.start_controller
        core.start_controller = self._stub_start(calls)
        try:
            rec = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(rec["action"], "resumed-controller")
        self.assertEqual(calls, ["r1"])
        after = core.get_job(self.sd, "r1")
        for field in ("task_json", "task_hash", "workspace", "route", "policy_id",
                      "planner_session_id"):
            self.assertEqual(after[field], before[field])
        self.assertEqual((after["codex_task_id"], after["opencode_session_id"]),
                         ("thr-old", "ses-old"))
        assessed = payloads(self.sd, "r1", "runtime_assessed")
        self.assertTrue(assessed and assessed[-1]["compatible"])
        recovered = payloads(self.sd, "r1", "runtime_recovered")
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]["old_runtime"]["root"], str(old))
        self.assertEqual(recovered[0]["new_runtime"]["root"], runtime.PKG_ROOT)
        self.assertTrue(recovered[0]["preserved"]["codex_task_id"])
        self.assertEqual(recovered[0]["preserved"]["route"], after["route"])
        # The never-started resume row is retained as history, not deleted.
        resume_rows = [i for i in core._list_invocations(self.sd, "r1")
                       if i.get("kind") == "codex_resume"]
        self.assertEqual(len(resume_rows), 1)
        self.assertTrue(runtime.is_runtime_missing_row(resume_rows[0]))
        # The restarted controller remakes the same action exactly once, and
        # the successful result is then memoized instead of rerun.
        self.install_fake_codex()
        run2 = core.make_durable_run_cmd(self.sd, "r1", "tok-new")
        resume_cmd = json.loads(resume_rows[0]["cmd_json"])
        key = core._action_key("codex_resume", resume_cmd, RESUME_META)
        rc, _, _ = run2(resume_cmd, self.ws(), 60,
                         kind="codex_resume", meta=dict(RESUME_META))
        self.assertEqual(rc, 0)
        same = [i for i in core._list_invocations(self.sd, "r1")
                if i.get("action_key") == key]
        self.assertEqual(len(same), 2)
        self.assertEqual((same[0]["state"], same[1]["state"]), ("failed", "completed"))
        self.assertEqual(same[1]["rc"], 0)
        again = run2(resume_cmd, self.ws(), 60,
                     kind="codex_resume", meta=dict(RESUME_META))
        self.assertEqual(again[0], 0)
        self.assertEqual(len([i for i in core._list_invocations(self.sd, "r1")
                              if i.get("action_key") == key]), 2)

    def test_question_pending_job_replays_without_a_second_writer(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET status='question_pending', codex_task_id='thr-q'"
                     " WHERE request_id='r1'")
        sql(self.sd, "INSERT INTO questions(request_id,qid,prompt,status,created_at)"
                     " VALUES('r1','q1','Proceed?','pending',?)", (core._utcnow(),))
        old = self.disposable_runtime_copy()
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        self._restore_pkg_roots()
        calls = []
        real_start = core.start_controller
        core.start_controller = self._stub_start(calls)
        try:
            rec = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(rec["action"], "resumed-controller")
        self.assertEqual(calls, ["r1"])
        con = store.connect(self.sd)
        try:
            q = con.execute("SELECT status FROM questions WHERE request_id='r1' AND qid='q1'"
                            ).fetchone()
        finally:
            con.close()
        self.assertEqual(q["status"], "pending")


class HealthyChildAndUnknownOwner(Base):
    def _live_row(self, rid, kind, supervisor, child, meta_extra=None):
        root = store.ensure_state_dir(self.sd)
        out_p = root / "outputs" / "live.stdout"
        err_p = root / "outputs" / "live.stderr"
        store.secure_write_text(out_p, "")
        store.secure_write_text(err_p, "")
        meta = {"stage": "dispatch", "route": "luna/max"}
        if meta_extra:
            meta.update(meta_extra)
        key = core._action_key(kind, ["codex", "exec", "live"], meta)
        sql(self.sd,
            "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
            "pid,pgid,process_start,supervisor_pid,supervisor_start,stdout_path,stderr_path,"
            "started_at,state,timeout_secs,meta_json,action_key,stage,requested_route,"
            "policy_version,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("live1", rid, kind, json.dumps(["codex", "exec", "live"]), self.ws(), "tok",
             child.pid, os.getpgid(child.pid), process_start_identity(child.pid),
             supervisor.pid, process_start_identity(supervisor.pid),
             str(out_p), str(err_p), core._utcnow(), "running", 600,
             json.dumps(meta), key, "planning", None, "2.7.0", 2))

    def test_healthy_child_survives_a_runtime_change(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET status='running' WHERE request_id='r1'")
        old = self.disposable_runtime_copy()
        old_root = str(old)
        self.remove_runtime(old)
        child, supervisor = self.sleeper(), self.sleeper()
        # A surviving planner callback: live, but not a dispatch turn, so
        # recovery adopts it without starting any new execution.
        self._live_row("r1", "codex_callback", supervisor, child,
                       meta_extra={"runtime": {"root": old_root, "version": "0.29.1",
                                              "policy_id": "durable-runner-policy-v2",
                                              "policy_version": "2.7.0", "schema_version": 2}})
        real_start = core.start_controller
        core.start_controller = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("healthy work must not be replaced"))
        try:
            out = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(out["action"], "adopted-live-invocation")
        self.assertIsNone(child.poll(), "the surviving child must not be signalled")
        self.assertIsNone(supervisor.poll())
        assessed = payloads(self.sd, "r1", "runtime_assessed")
        self.assertTrue(assessed and assessed[-1]["compatible"])

    def test_unknown_ownership_still_blocks_without_a_duplicate(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='r1'")
        holder = self.sleeper()
        store.secure_write_text(
            Path(self.sd) / "workers" / "r1.json",
            json.dumps({"token": "foreign", "pid": holder.pid,
                        "updated": core._utcnow(),
                        "start": process_start_identity(holder.pid)}))
        real_start = core.start_controller
        core.start_controller = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("unknown ownership must never start a second writer"))
        try:
            out = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(out["action"], "blocked-unknown-owner")
        self.assertTrue(core.get_job(self.sd, "r1")["block_reason"].startswith(
            "unknown worker ownership"))


class IncompatibleReplacement(Base):
    def _recover_blocked(self, rid):
        real_start = core.start_controller
        core.start_controller = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("incompatible state must not start execution"))
        try:
            return core.recover_one(self.sd, rid)
        finally:
            core.start_controller = real_start

    def test_foreign_policy_is_retained_with_its_reason(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p",
                    policy_id="other-policy-v9")
        before = core.get_job(self.sd, "r1")
        out = self._recover_blocked("r1")
        self.assertEqual(out["action"], "blocked-incompatible-runtime")
        job = core.get_job(self.sd, "r1")
        self.assertTrue(job["block_reason"].startswith("runtime_incompatible:"))
        self.assertIn("policy_incompatible", job["block_reason"])
        self.assertEqual((job["route"], job["task_json"], job["policy_id"]),
                         (before["route"], before["task_json"], "other-policy-v9"))

    def test_unreadable_controller_state_is_retained_with_its_reason(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        before = core.get_job(self.sd, "r1")
        sql(self.sd, "UPDATE jobs SET controller_state='not-json{{' WHERE request_id='r1'")
        out = self._recover_blocked("r1")
        self.assertEqual(out["action"], "blocked-incompatible-runtime")
        job = core.get_job(self.sd, "r1")
        self.assertIn("controller_state_unreadable", job["block_reason"])
        self.assertEqual((job["route"], job["task_json"]), (before["route"], before["task_json"]))

    def test_newer_schema_is_retained_with_its_reason(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        before = core.get_job(self.sd, "r1")
        root = store.ensure_state_dir(self.sd)
        store.secure_write_text(root / "outputs" / "f1.stdout", "")
        store.secure_write_text(root / "outputs" / "f1.stderr", "")
        # A non-dispatch row: recovery consumes it without effect, then the
        # compatibility decision keeps the specific reason.
        sql(self.sd,
            "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
            "stdout_path,stderr_path,started_at,state,rc,meta_json,schema_version) VALUES"
            "('f1','r1','codex_callback','[]',?,'tok',?,?,?,'completed',0,'{}',999)",
            (self.ws(), str(root / "outputs" / "f1.stdout"),
             str(root / "outputs" / "f1.stderr"), core._utcnow()))
        out = self._recover_blocked("r1")
        self.assertEqual(out["action"], "blocked-incompatible-runtime")
        job = core.get_job(self.sd, "r1")
        self.assertIn("schema_incompatible", job["block_reason"])
        self.assertEqual((job["route"], job["task_json"]), (before["route"], before["task_json"]))
        rows = core._list_invocations(self.sd, "r1")
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
