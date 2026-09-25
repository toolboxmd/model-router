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

from runner import codex_native, controller, core, policy, runtime, store  # noqa: E402
from runner.supervisor import process_start_identity  # noqa: E402
from tests.fakes import FAKE_CODEX_NATIVE_APP, FAKE_GROK, FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable
DISPATCH_META = {"stage": "dispatch", "route": "luna/max", "reason": "initial"}
RESUME_META = {"stage": "dispatch", "route": "luna/max", "reason": "resume"}

FIXED_THREAD = "thr-75-fixed"

# Dispatcher turns run on the native app-server branch (#86); the exec
# body below stays for the planner-callback path. Native turns answer
# from FAKE_NATIVE_PLAN (implement_then_complete: first turn
# implementation, later turns completion).
_FAKE_CODEX_HEAD = r'''
import json, os, sys
from pathlib import Path
st = Path(os.environ.get("FAKE_STATE", ""))
if str(st):
    st.mkdir(parents=True, exist_ok=True)
    argv = sys.argv[1:]
    with open(st / "codex75.log", "a") as f:
        f.write(json.dumps(argv) + "\n")
else:
    argv = sys.argv[1:]
'''

# A disposable `codex` shaped like the real CLI contract: thread.started,
# one agent_message, turn.completed, plus the last-message file. Dispatch
# answers an implementation envelope, resume the same thread with a
# completion envelope. No model is called.
FAKE_CODEX_75 = _FAKE_CODEX_HEAD + FAKE_CODEX_NATIVE_APP + r'''
import json, os, sys
from pathlib import Path
tid = "thr-75-fixed"
if argv[:2] == ["exec", "resume"]:
    env = {"action": "completion", "output": "RESUMED_75", "artifact": ""}
    assert tid in argv, argv
else:
    env = {"action": "implementation", "artifact": "fix.txt",
           "payload": {"instructions": "write fix.txt"}}
if "--output-last-message" in argv:
    lp = Path(argv[argv.index("--output-last-message") + 1])
    lp.parent.mkdir(parents=True, exist_ok=True)
    lp.write_text(json.dumps(env))
for obj in ({"type": "thread.started", "thread_id": tid},
            {"type": "item.completed",
             "item": {"type": "agent_message", "text": json.dumps(env)}},
            {"type": "turn.completed", "usage": {}}):
    print(json.dumps(obj), flush=True)
'''


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
        # Stop only the job-owned native server recorded in durable
        # state; ownership-checked, never a pattern kill.
        try:
            codex_native.stop_server(self.sd, "r1", "test-cleanup")
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

    def install_fake_harness_clis(self):
        """Disposable `codex`/`opencode`/`grok` shaped like the real CLIs.

        Used with the real durable run_cmd (real supervisors), so the
        missing-then-repaired path runs through the public controller
        turns with no real agent process. Dispatcher turns run on the
        native app-server branch (#86): the fake server answers from
        FAKE_NATIVE_PLAN on the fixed thread.
        """
        bindir = self.base / "fakebin"
        bindir.mkdir(exist_ok=True)
        fake_state = self.base / "fakestate"
        fake_state.mkdir(exist_ok=True)
        write_fake(bindir, "codex", FAKE_CODEX_75, PY)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        write_fake(bindir, "grok", FAKE_GROK, PY)
        saved_path = os.environ.get("PATH", "")
        saved_state = os.environ.get("FAKE_STATE")
        saved_thread = os.environ.get("FAKE_NATIVE_THREAD")
        saved_plan = os.environ.get("FAKE_NATIVE_PLAN")
        saved_output = os.environ.get("FAKE_NATIVE_OUTPUT")
        os.environ["PATH"] = str(bindir) + os.pathsep + saved_path
        os.environ["FAKE_STATE"] = str(fake_state)
        os.environ["FAKE_NATIVE_THREAD"] = FIXED_THREAD
        os.environ["FAKE_NATIVE_PLAN"] = "implement_then_complete"
        os.environ["FAKE_NATIVE_OUTPUT"] = "RESUMED_75"
        self.addCleanup(lambda: os.environ.__setitem__("PATH", saved_path))
        if saved_state is None:
            self.addCleanup(lambda: os.environ.pop("FAKE_STATE", None))
        else:
            self.addCleanup(lambda: os.environ.__setitem__("FAKE_STATE", saved_state))
        if saved_thread is None:
            self.addCleanup(lambda: os.environ.pop("FAKE_NATIVE_THREAD", None))
        else:
            self.addCleanup(lambda: os.environ.__setitem__("FAKE_NATIVE_THREAD", saved_thread))
        if saved_plan is None:
            self.addCleanup(lambda: os.environ.pop("FAKE_NATIVE_PLAN", None))
        else:
            self.addCleanup(lambda: os.environ.__setitem__("FAKE_NATIVE_PLAN", saved_plan))
        if saved_output is None:
            self.addCleanup(lambda: os.environ.pop("FAKE_NATIVE_OUTPUT", None))
        else:
            self.addCleanup(lambda: os.environ.__setitem__("FAKE_NATIVE_OUTPUT", saved_output))

    def rows_for_key(self, rid, key):
        return [i for i in core._list_invocations(self.sd, rid)
                if i.get("action_key") == key and i.get("state") != "abandoned"]

    def key_of(self, row, kind, meta):
        return core._action_key(kind, json.loads(row["cmd_json"]), meta)


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
        # the successful result is then memoized instead of rerun. The
        # remake uses the stored native turn meta (prompt/model travel in
        # meta on the native seam) against the fake app-server.
        self.install_fake_harness_clis()
        os.environ["FAKE_NATIVE_THREAD"] = "thr-old"
        os.environ["FAKE_NATIVE_PLAN"] = "completion"
        os.environ["FAKE_NATIVE_OUTPUT"] = "RESUMED_75"
        run2 = core.make_durable_run_cmd(self.sd, "r1", "tok-new")
        resume_cmd = json.loads(resume_rows[0]["cmd_json"])
        resume_meta = json.loads(resume_rows[0]["meta_json"])
        key = core._action_key("codex_resume", resume_cmd, resume_meta)
        rc, _, _ = run2(resume_cmd, self.ws(), 60,
                         kind="codex_resume", meta=dict(resume_meta))
        self.assertEqual(rc, 0)
        same = [i for i in core._list_invocations(self.sd, "r1")
                if i.get("action_key") == key]
        self.assertEqual(len(same), 2)
        self.assertEqual((same[0]["state"], same[1]["state"]), ("failed", "completed"))
        self.assertEqual(same[1]["rc"], 0)
        again = run2(resume_cmd, self.ws(), 60,
                     kind="codex_resume", meta=dict(resume_meta))
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


class DispatchTwiceMissingThenRepaired(Base):
    """Finding 1: a repaired dispatch retry is not reblocked by its stale row.

    Drives the real public ``controller.dispatch`` twice through real
    supervisors: first with the disposable old runtime removed (the
    spawn provably never starts), then on the installed runtime with a
    working fake `codex`. Asserts preserved identities, one logical
    action, no duplicate writer, no reblock after success, truthful
    provenance, and no false recovery event.
    """

    def _stub_start(self, calls):
        def fake_start(state_dir, request_id):
            calls.append(request_id)
            return {"request_id": request_id, "pid": 999999999}
        return fake_start

    def test_dispatch_twice_missing_then_repaired(self):
        self.assertEqual(policy.stage_routes("dispatch")[0], "luna/max")
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        before = core.get_job(self.sd, "r1")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='r1'")
        old = self.disposable_runtime_copy()
        old_root = str(old)
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        out = controller.dispatch(self.sd, "r1", run_cmd=run)
        self.assertEqual(out, {"action": "blocked", "reason": "runtime_missing"})
        job = core.get_job(self.sd, "r1")
        self.assertTrue(job["block_reason"].startswith("runtime_missing:"))
        self.assertIn("recover", job["block_reason"])
        self.assertIn(old_root, job["block_reason"])
        rows = core._list_invocations(self.sd, "r1")
        self.assertEqual(len(rows), 1)
        # Native seam (#86): prompt/model travel in invocation meta, so
        # the logical action key is recomputed from the stored meta.
        key = self.key_of(rows[0], "codex_dispatch",
                          json.loads(rows[0]["meta_json"]))
        failed_payload = json.loads(rows[0]["result_json"])
        self.assertTrue(failed_payload.get("never_started"))
        self.assertTrue(failed_payload.get("runtime_missing"))
        # Provenance names the real old version even though the old path
        # is already gone: the version was read at import, not at call.
        self.assertEqual(failed_payload["runtime"]["root"], old_root)
        self.assertEqual(failed_payload["runtime"]["version"], runtime.IMPORT_VERSION)
        self.assertNotEqual(failed_payload["runtime"]["version"], "unknown")
        # Repair: the installed runtime with a working fake `codex`.
        self._restore_pkg_roots()
        self.install_fake_harness_clis()
        sql(self.sd, "UPDATE jobs SET owner_token='tok2' WHERE request_id='r1'")
        run2 = core.make_durable_run_cmd(self.sd, "r1", "tok2")
        out2 = controller.dispatch(self.sd, "r1", run_cmd=run2)
        self.assertEqual(out2["action"], "dispatched")
        self.assertEqual(out2["codex_task_id"], FIXED_THREAD)
        self.assertEqual(out2["luna_action"]["action"], "implementation")
        same = self.rows_for_key("r1", key)
        self.assertEqual(len(same), 2, "one logical action: failed row plus its remake")
        self.assertEqual((same[0]["state"], same[1]["state"]), ("failed", "completed"))
        self.assertTrue(runtime.is_runtime_missing_row(same[0]))
        self.assertFalse(runtime.is_runtime_missing_row(same[1]))
        after = core.get_job(self.sd, "r1")
        for field in ("task_json", "task_hash", "workspace", "route", "policy_id",
                      "planner_session_id"):
            self.assertEqual(after[field], before[field])
        # The stale row no longer decides: recovery sees no missing failure.
        self.assertFalse(core._has_runtime_missing_failure(self.sd, "r1"))
        self.assertIsNone(core.runtime_missing_for_action(
            self.sd, "r1", "codex_dispatch",
            json.loads(same[1]["cmd_json"]),
            json.loads(same[1]["meta_json"])))
        # A third dispatch reuses the completed attempt: no duplicate
        # writer, no reblock, no new row. (The controller reports
        # already-dispatched once the task and its action are saved.)
        out3 = controller.dispatch(self.sd, "r1", run_cmd=run2)
        self.assertIn(out3["action"], ("dispatched", "already-dispatched"))
        self.assertEqual(len(self.rows_for_key("r1", key)), 2)
        # Recovery after the repaired success records no false recovery:
        # the controller restarts without a runtime_recovered event.
        calls = []
        real_start = core.start_controller
        core.start_controller = self._stub_start(calls)
        try:
            rec = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(rec["action"], "resumed-controller")
        self.assertEqual(calls, ["r1"])
        # The repaired job continues instead of reblocking: the stale
        # row never wins again.
        self.assertNotEqual(rec["status"], "blocked")
        self.assertEqual(payloads(self.sd, "r1", "runtime_recovered"), [])


class ResumeTwiceMissingThenRepaired(Base):
    """Finding 1 on the resume path: ``resume_luna`` twice, then recover."""

    def _stub_start(self, calls):
        def fake_start(state_dir, request_id):
            calls.append(request_id)
            con = store.connect(state_dir)
            try:
                con.execute("BEGIN IMMEDIATE")
                row = con.execute(
                    "SELECT COALESCE(MAX(attempt_no), 0) AS n FROM launches WHERE request_id=?",
                    (request_id,)).fetchone()
                attempt = int(row["n"]) + 1
                token = "tok-new-%d" % attempt
                con.execute(
                    "INSERT INTO launches(request_id,attempt_no,start_token,state,created_at)"
                    " VALUES(?,?,?,'acknowledged',?)",
                    (request_id, attempt, token, core._utcnow()))
                con.execute(
                    "UPDATE jobs SET owner_token=?, owner_pid=?, owner_start=?, updated_at=?"
                    " WHERE request_id=?",
                    (token, 999999999, "dead", core._utcnow(), request_id))
                con.execute("COMMIT")
            finally:
                con.close()
            return {"request_id": request_id, "pid": 999999999}
        return fake_start

    def test_resume_twice_missing_then_repaired(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        before = core.get_job(self.sd, "r1")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running',"
                     " codex_task_id=%r WHERE request_id='r1'" % FIXED_THREAD)
        old = self.disposable_runtime_copy()
        old_root = str(old)
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        out = controller.resume_luna(self.sd, "r1", "follow up", run_cmd=run)
        self.assertEqual(out, {"action": "blocked", "reason": "runtime_missing"})
        self.assertIn(old_root, core.get_job(self.sd, "r1")["block_reason"])
        rows = core._list_invocations(self.sd, "r1")
        self.assertEqual(len(rows), 1)
        # Native seam (#86): the logical action key carries the stored
        # prompt/model meta.
        key = self.key_of(rows[0], "codex_resume",
                          json.loads(rows[0]["meta_json"]))
        # Repair on the installed runtime with a working fake `codex`
        # that resumes the same thread. The repaired resume answers the
        # completion envelope on its first native turn.
        self._restore_pkg_roots()
        self.install_fake_harness_clis()
        os.environ["FAKE_NATIVE_PLAN"] = "completion"
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
        tok = core.get_job(self.sd, "r1")["owner_token"]
        run2 = core.make_durable_run_cmd(self.sd, "r1", tok)
        out2 = controller.resume_luna(self.sd, "r1", "follow up", run_cmd=run2)
        self.assertEqual(out2["action"], "resumed")
        self.assertEqual(out2["luna_action"]["action"], "completion")
        same = self.rows_for_key("r1", key)
        self.assertEqual(len(same), 2, "one logical action: failed row plus its remake")
        self.assertEqual((same[0]["state"], same[1]["state"]), ("failed", "completed"))
        after = core.get_job(self.sd, "r1")
        for field in ("task_json", "task_hash", "workspace", "route", "policy_id",
                      "planner_session_id", "codex_task_id"):
            self.assertEqual(after[field], before[field] if field != "codex_task_id"
                             else FIXED_THREAD)
        recovered = payloads(self.sd, "r1", "runtime_recovered")
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]["old_runtime"]["root"], old_root)
        self.assertEqual(recovered[0]["old_runtime"]["version"], runtime.IMPORT_VERSION)
        self.assertNotEqual(recovered[0]["old_runtime"]["version"], "unknown")
        self.assertEqual(recovered[0]["new_runtime"]["root"], runtime.PKG_ROOT)
        # A third resume reuses the completed attempt: no duplicate, no
        # reblock, no new row.
        out3 = controller.resume_luna(self.sd, "r1", "follow up", run_cmd=run2)
        self.assertEqual(out3["action"], "resumed")
        self.assertEqual(len(self.rows_for_key("r1", key)), 2)
        self.assertFalse(core._has_runtime_missing_failure(self.sd, "r1"))
        # A later recovery restarts without a second recovery event.
        calls2 = []
        core.start_controller = self._stub_start(calls2)
        try:
            rec2 = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(rec2["action"], "resumed-controller")
        self.assertEqual(len(payloads(self.sd, "r1", "runtime_recovered")), 1)


class WorkerTurnsGateMissingRuntime(Base):
    """Finding 2: OpenCode and Grok worker turns gate the missing runtime.

    A never-started spawn failure names its cause and blocks
    recoverably before capacity handling, the escalation ladder, or a
    sticky implementation failure. The explicit route is unchanged and
    the unstarted turn leaves no turn report. After repair the same
    public turn succeeds exactly once and is then reused.
    """

    def test_opencode_turn_missing_then_repaired(self):
        route = "muse-spark-xhigh-free"
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        before = core.get_job(self.sd, "r1")
        self.assertEqual(before["route"], route)
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='r1'")
        old = self.disposable_runtime_copy()
        old_root = str(old)
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        job = core.get_job(self.sd, "r1")
        out = controller._run_opencode_turn(
            self.sd, "r1", job, job["workspace"], route, "do work",
            0, "initial", run)
        self.assertEqual(out, {"action": "blocked", "reason": "runtime_missing"})
        job = core.get_job(self.sd, "r1")
        self.assertTrue(job["block_reason"].startswith("runtime_missing:"))
        self.assertIn(old_root, job["block_reason"])
        self.assertIn("recover", job["block_reason"])
        # No silent route move, no capacity mark, no turn report for a
        # turn that never started.
        self.assertEqual(job["route"], route)
        self.assertEqual(core.exhausted_routes(self.sd) | core.degraded_routes(self.sd), set())
        self.assertIsNone(core.latest_turn_report(self.sd, "r1"))
        rows = [i for i in core._list_invocations(self.sd, "r1")
                if i.get("kind") == "opencode_control" and i.get("state") != "abandoned"]
        self.assertEqual(len(rows), 1)
        self.assertTrue(runtime.is_runtime_missing_row(rows[0]))
        # Repair on the installed runtime with a working fake server.
        self._restore_pkg_roots()
        self.install_fake_harness_clis()
        sql(self.sd, "UPDATE jobs SET owner_token='tok2', status='running',"
                     " block_reason=NULL WHERE request_id='r1'")
        run2 = core.make_durable_run_cmd(self.sd, "r1", "tok2")
        job2 = core.get_job(self.sd, "r1")
        out2 = controller._run_opencode_turn(
            self.sd, "r1", job2, job2["workspace"], route, "do work",
            0, "initial", run2)
        self.assertEqual(out2["action"], "implementation_ok")
        key = self.key_of(rows[0], "opencode_control",
                          json.loads(rows[0]["meta_json"]))
        same = self.rows_for_key("r1", key)
        self.assertEqual(len(same), 2)
        self.assertEqual((same[0]["state"], same[1]["state"]), ("failed", "completed"))
        self.assertEqual(core.get_job(self.sd, "r1")["route"], route)
        # The repaired turn is then reused: no duplicate, no reblock.
        job3 = core.get_job(self.sd, "r1")
        out3 = controller._run_opencode_turn(
            self.sd, "r1", job3, job3["workspace"], route, "do work",
            0, "initial", run2)
        self.assertEqual(out3["action"], "implementation_ok")
        self.assertEqual(len(self.rows_for_key("r1", key)), 2)

    def test_grok_turn_missing_then_repaired(self):
        route = "grok-4.6-build"
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", route=route)
        self.assertEqual(core.get_job(self.sd, "r1")["route"], route)
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='r1'")
        old = self.disposable_runtime_copy()
        old_root = str(old)
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        job = core.get_job(self.sd, "r1")
        out = controller._run_grok_turn(
            self.sd, "r1", job, job["workspace"], route, "do work",
            0, "initial", run)
        self.assertEqual(out, {"action": "blocked", "reason": "runtime_missing"})
        job = core.get_job(self.sd, "r1")
        self.assertIn(old_root, job["block_reason"])
        self.assertIn("recover", job["block_reason"])
        self.assertEqual(job["route"], route)
        self.assertEqual(core.exhausted_routes(self.sd) | core.degraded_routes(self.sd), set())
        self.assertIsNone(core.latest_turn_report(self.sd, "r1"))
        rows = [i for i in core._list_invocations(self.sd, "r1")
                if i.get("kind") == "grok_control" and i.get("state") != "abandoned"]
        self.assertEqual(len(rows), 1)
        self.assertTrue(runtime.is_runtime_missing_row(rows[0]))
        self._restore_pkg_roots()
        self.install_fake_harness_clis()
        sql(self.sd, "UPDATE jobs SET owner_token='tok2', status='running',"
                     " block_reason=NULL WHERE request_id='r1'")
        run2 = core.make_durable_run_cmd(self.sd, "r1", "tok2")
        job2 = core.get_job(self.sd, "r1")
        out2 = controller._run_grok_turn(
            self.sd, "r1", job2, job2["workspace"], route, "do work",
            0, "initial", run2)
        self.assertEqual(out2["action"], "implementation_ok")
        key = self.key_of(rows[0], "grok_control",
                          json.loads(rows[0]["meta_json"]))
        same = self.rows_for_key("r1", key)
        self.assertEqual(len(same), 2)
        self.assertEqual((same[0]["state"], same[1]["state"]), ("failed", "completed"))
        self.assertEqual(core.get_job(self.sd, "r1")["route"], route)
        job3 = core.get_job(self.sd, "r1")
        out3 = controller._run_grok_turn(
            self.sd, "r1", job3, job3["workspace"], route, "do work",
            0, "initial", run2)
        self.assertEqual(out3["action"], "implementation_ok")
        self.assertEqual(len(self.rows_for_key("r1", key)), 2)


class PlannerCallbackGatesMissingRuntime(Base):
    """Finding 2 on the callback path: the question stays pending."""

    def test_codex_callback_missing_blocks_recoverably(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p",
                    planner_harness="codex")
        sql(self.sd, "UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='r1'")
        old = self.disposable_runtime_copy()
        old_root = str(old)
        self.point_runtimes_at(old)
        self.remove_runtime(old)
        run = core.make_durable_run_cmd(self.sd, "r1", "tok")
        out = controller.planner_callback(self.sd, "r1", "q1", "Proceed?", run_cmd=run)
        self.assertEqual(out["action"], "blocked")
        self.assertEqual(out["reason"], "runtime_missing")
        job = core.get_job(self.sd, "r1")
        self.assertTrue(job["block_reason"].startswith("runtime_missing:"))
        self.assertIn(old_root, job["block_reason"])
        self.assertIn("recover", job["block_reason"])
        pending = core.list_questions(self.sd, "r1", only_pending=True)
        self.assertEqual([q["qid"] for q in pending], ["q1"])
        self._restore_pkg_roots()


class CompatibilityAdoptsHealthyWork(Base):
    """Finding 3: the compatibility check never stops healthy owned work.

    An incompatible replacement still blocks with its specific reason
    when no controller or child is alive, but a live child or an
    advertised healthy controller is adopted first.
    """

    def _live_row(self, rid, kind, supervisor, child):
        root = store.ensure_state_dir(self.sd)
        out_p = root / "outputs" / "live2.stdout"
        err_p = root / "outputs" / "live2.stderr"
        store.secure_write_text(out_p, "")
        store.secure_write_text(err_p, "")
        meta = {"stage": "dispatch", "route": "luna/max"}
        key = core._action_key(kind, ["codex", "exec", "live"], meta)
        sql(self.sd,
            "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
            "pid,pgid,process_start,supervisor_pid,supervisor_start,stdout_path,stderr_path,"
            "started_at,state,timeout_secs,meta_json,action_key,stage,requested_route,"
            "policy_version,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("live2", rid, kind, json.dumps(["codex", "exec", "live"]), self.ws(), "tok",
             child.pid, os.getpgid(child.pid), process_start_identity(child.pid),
             supervisor.pid, process_start_identity(supervisor.pid),
             str(out_p), str(err_p), core._utcnow(), "running", 600,
             json.dumps(meta), key, "planning", None, "2.7.0", 2))

    def test_incompatible_state_adopts_a_live_child(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p",
                    policy_id="other-policy-v9")
        sql(self.sd, "UPDATE jobs SET status='running' WHERE request_id='r1'")
        child, supervisor = self.sleeper(), self.sleeper()
        self._live_row("r1", "codex_callback", supervisor, child)
        real_start = core.start_controller
        core.start_controller = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("a live child must be adopted, never replaced"))
        try:
            out = core.recover_one(self.sd, "r1")
        finally:
            core.start_controller = real_start
        self.assertEqual(out["action"], "adopted-live-invocation")
        self.assertIsNone(child.poll(), "the surviving child must not be signalled")
        self.assertIsNone(supervisor.poll())

    def test_terminal_jobs_are_not_assessed(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p")
        sql(self.sd, "UPDATE jobs SET status='succeeded' WHERE request_id='r1'")
        before = [e["kind"] for e in events(self.sd, "r1")]
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "noop-terminal")
        after = [e["kind"] for e in events(self.sd, "r1")]
        self.assertNotIn("runtime_assessed", after,
                         "recover --all must not assess finished jobs")
        self.assertNotIn("runtime_recovered", after)


if __name__ == "__main__":
    unittest.main()
