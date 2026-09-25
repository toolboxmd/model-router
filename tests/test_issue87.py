"""Issue 87: dispatcher-owned recovery, truthful evidence, and acceptance.

Failing-before regressions for the architecture-audit refinements on top of
merged #75 (runtime recovery) and #88 (no elapsed kill):

- failure taxonomy (timeout, stall, provider, infrastructure,
  implementation, verification; cancellation separate; unknown stays
  unknown) on durable turn reports;
- dispatch fallback reason kept separate from the worker route reason;
- failure-report writing separated from expensive proof (stalled,
  exhausted, crashed, or otherwise incomplete turns skip the suite
  truthfully; proof timeout is classified apart from rc127);
- the whole owned proof process tree is stopped before a later writer;
- completion binds required proof to the current candidate, verifies the
  PR is open in the intended repository at the intended commit (draft
  only when explicitly authorized), preserves one PR identity, and
  requires named acceptance evidence;
- recovery decisions link the failed attempt to the next attempt with
  timestamps, and exhaustion carries a concrete decision (never a request
  for the planner to implement);
- a planner-directed eligible route is assigned through the dispatcher;
  ineligible names are rejected with evidence, never silently swapped;
- the three small #75 review caveats: one recover for a
  runtime-missing blocked question, the compatibility gate on the
  live-child replacement path, and late evidence on terminal jobs.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import adapters, controller, core, store  # noqa: E402
from tests.fakes import use_fake_t3  # noqa: E402
from tests.fakes import isolate_t3_env  # noqa: E402


def setUpModule():
    isolate_t3_env()


PY = sys.executable


def sql(sd, stmt, args=()):
    con = store.connect(sd)
    try:
        con.execute(stmt, args)
        con.commit()
    finally:
        con.close()


def new_dirs(prefix="r87-"):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    base = Path(tmp.name)
    sd = str(base / "state")
    ws = base / "ws"
    ws.mkdir()
    return tmp, sd, ws


def set_controller_state(sd, rid, **fields):
    con = store.connect(sd)
    try:
        cur = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (rid,)).fetchone()
        st = json.loads(cur["controller_state"] or "{}") if cur and cur["controller_state"] else {}
        st.update(fields)
        con.execute("UPDATE jobs SET controller_state=? WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), rid))
        con.commit()
    finally:
        con.close()


def mark_turns(sd, rid, routes):
    """Record worker turns on ``routes`` as the job's T3 thread slots."""
    con = store.connect(sd)
    try:
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (rid,)).fetchone()
        st = json.loads(row["controller_state"] or "{}")
        threads = st.setdefault("t3_threads", {})
        for i, route in enumerate(routes):
            threads[f"impl_used{i}"] = {"thread_id": f"sub.p.{rid}{i}", "route": route}
        con.execute("UPDATE jobs SET controller_state=? WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), rid))
        con.commit()
    finally:
        con.close()


def stub_pr_verifier(result):
    def _verify(workspace, pr_url):
        return dict(result)
    return _verify


class FailureTaxonomy(unittest.TestCase):
    def test_failure_class_mapping(self):
        self.assertEqual(core.failure_class_for(signal="stalled"), "stall")
        for sig in ("exhausted", "overloaded", "context"):
            self.assertEqual(core.failure_class_for(signal=sig), "provider", sig)
        for sig in ("hard",):
            self.assertEqual(core.failure_class_for(signal=sig), "infrastructure", sig)
        self.assertEqual(core.failure_class_for(runtime_missing=True), "infrastructure")
        self.assertEqual(core.failure_class_for(proof_class="timeout"), "timeout")
        self.assertEqual(core.failure_class_for(rc=124), "timeout")
        self.assertEqual(core.failure_class_for(proof_class="failed"), "verification")
        self.assertEqual(core.failure_class_for(proof_class="not_found"), "verification")
        self.assertEqual(core.failure_class_for(rc=1), "implementation")
        self.assertEqual(core.failure_class_for(cancelled=True), "cancelled")
        self.assertEqual(core.failure_class_for(), "unknown")
        self.assertEqual(core.failure_class_for(signal=None, rc=None), "unknown")
        self.assertIn("unknown", core.FAILURE_CLASSES)


class DispatchWorkerReasonSeparation(unittest.TestCase):
    def test_dispatch_reason_kept_apart_from_worker_reason(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "sep", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='sep'")
        controller._record_dispatch_reason(sd, "sep", "luna/max", "luna-go/max",
                                           "dispatch_exhausted")
        st = controller._load_controller_state(core.get_job(sd, "sep"))
        self.assertEqual(st.get("dispatch_route_reason"), "dispatch_exhausted")
        # The shared key is left for worker moves: the dispatch cause
        # never leaks into the first worker invocation, which keeps
        # reason initial.
        self.assertNotIn("route_reason", st)
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='sep'"
                             " AND kind='route_switched' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertEqual(json.loads(ev["payload_json"])["scope"], "dispatch")
        # A later worker move records the worker key but preserves the
        # dispatch key, so an Observer never reads a worker attempt as
        # dispatch_stalled again.
        controller._switch_route(sd, "sep", "muse-spark-xhigh-go", "pool_move",
                                 {"source": "test"})
        st = controller._load_controller_state(core.get_job(sd, "sep"))
        self.assertEqual(st.get("route_reason"), "pool_move")
        self.assertEqual(st.get("dispatch_route_reason"), "dispatch_exhausted")
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='sep'"
                             " AND kind='route_switched' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertEqual(json.loads(ev["payload_json"])["scope"], "worker")

    def test_first_worker_invocation_reason_is_initial_after_fallback(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "sep2", {"g": 1, "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='sep2'")
        set_controller_state(sd, "sep2", seq=1)
        controller._record_dispatch_reason(sd, "sep2", "luna/max", "luna-go/max",
                                           "dispatch_fallback rc=1")
        job = core.get_job(sd, "sep2")
        st = controller._load_controller_state(job)
        meta_reason = st.get("route_reason") or "initial"
        self.assertEqual(meta_reason, "initial")
        self.assertEqual(st.get("dispatch_route_reason"), "dispatch_fallback rc=1")


class ProofSeparation(unittest.TestCase):
    def test_capacity_turn_skips_proof_truthfully(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "skip", {"g": 1, "proof": "false"}, str(ws), "p", planner_t3_thread="planner-t3")
        job = core.get_job(sd, "skip")
        full = {"assistant_text": "t", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None,
                "signal": "exhausted", "signal_evidence": {"source": "test"}}
        report = controller._write_turn_report(
            sd, "skip", job, 1, "muse-spark-xhigh-free", full, "ses",
            status="exhausted", run_proof=False,
            proof_skipped_reason="exhausted turn: proof deferred to the dispatcher")
        self.assertEqual(report["status"], "exhausted")
        self.assertIsNone(report["proof_exit_code"])
        self.assertEqual(report["proof_class"], "skipped")
        self.assertIn("deferred", report["proof_skipped"])
        self.assertIn("deferred", Path(report["proof_log"]).read_text())
        self.assertEqual(report["failure_class"], "provider")
        self.assertIsNone(report["proof_started_at"])
        # A skipped proof is not a passed proof: completion must refuse it
        # as incomplete instead of succeeding without evidence.
        reason = core.incomplete_proof_reason(
            job["task_json"], report, str(ws))
        self.assertIsNotNone(reason)
        self.assertIn("incomplete proof", reason)

    def test_stalled_turn_report_marks_stall_class(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "stallrep", {"g": 1, "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        job = core.get_job(sd, "stallrep")
        full = {"assistant_text": "t", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None,
                "signal": "stalled",
                "signal_evidence": {"source": "stream_silence"}}
        report = controller._write_turn_report(
            sd, "stallrep", job, 1, "muse-spark-xhigh-free", full, "ses",
            status="stalled", run_proof=False,
            proof_skipped_reason="stalled turn: proof deferred to the dispatcher")
        self.assertEqual(report["failure_class"], "stall")
        self.assertEqual(report["proof_class"], "skipped")

    def test_proof_timeout_classified_apart_from_not_found(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        rc, _out, cls, started, ended = controller._run_proof_command(
            str(ws), "sleep 30", timeout=1)
        self.assertEqual(rc, 124)
        self.assertEqual(cls, "timeout")
        self.assertTrue(started and ended and ended >= started)
        rc, _out, cls, _s, _e = controller._run_proof_command(
            str(ws), "definitely-missing-binary-xyz-87 true", timeout=30)
        self.assertEqual(rc, 127)
        self.assertEqual(cls, "not_found")
        rc, _out, cls, _s, _e = controller._run_proof_command(
            str(ws), "exit 3", timeout=30)
        self.assertEqual((rc, cls), (3, "failed"))
        rc, _out, cls, _s, _e = controller._run_proof_command(
            str(ws), "true", timeout=30)
        self.assertEqual((rc, cls), (0, "pass"))

    def test_proof_timeout_kills_the_whole_tree(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        pidfile = ws / "survivor.pid"
        cmd = ("sleep 120 & echo $! > survivor.pid; "
               "sleep 120 & wait")
        rc, _out, cls, _s, _e = controller._run_proof_command(
            str(ws), cmd, timeout=2)
        self.assertEqual((rc, cls), (124, "timeout"))
        survivor = int(pidfile.read_text().strip())
        time.sleep(0.5)
        with self.assertRaises(ProcessLookupError):
            os.kill(survivor, 0)

    def test_completion_refusal_names_proof_timeout(self):
        reason = core.completion_refusal_reason(
            {"status": "failed", "proof_exit_code": 124, "proof_class": "timeout"})
        self.assertTrue(reason.startswith("completion_refused:"))
        self.assertIn("timed out", reason)
        self.assertNotIn("not found", reason)

    def test_executed_proof_records_verification_row(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "vrow", {"g": 1, "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        job = core.get_job(sd, "vrow")
        full = {"assistant_text": "t", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(
            sd, "vrow", job, 1, "muse-spark-xhigh-free", full, "ses")
        self.assertEqual(report["proof_class"], "pass")
        con = store.connect(sd)
        try:
            rows = con.execute("SELECT kind, stage, rc, reason, started_at,"
                               " ended_at, elapsed_secs, terminal_class"
                               " FROM invocations WHERE request_id='vrow'"
                               " AND kind='proof'").fetchall()
        finally:
            con.close()
        self.assertEqual(len(rows), 1)
        row = dict(rows[0])
        self.assertEqual(row["stage"], "verification")
        self.assertEqual((row["rc"], row["reason"]), (0, "pass"))
        self.assertTrue(row["started_at"] and row["ended_at"])
        self.assertEqual(row["terminal_class"], "completed")

    def test_skipped_proof_records_no_attempt_row(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "vskip", {"g": 1, "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        job = core.get_job(sd, "vskip")
        full = {"assistant_text": "t", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None,
                "signal": "exhausted", "signal_evidence": {"source": "test"}}
        controller._write_turn_report(
            sd, "vskip", job, 1, "muse-spark-xhigh-free", full, "ses",
            status="exhausted", run_proof=False,
            proof_skipped_reason="exhausted turn: proof deferred")
        con = store.connect(sd)
        try:
            rows = con.execute("SELECT * FROM invocations WHERE request_id='vskip'"
                               " AND kind='proof'").fetchall()
        finally:
            con.close()
        self.assertEqual(rows, [])


class CompletionBinding(unittest.TestCase):
    def _git_ws(self, base, name, with_origin=False):
        ws = base / name
        ws.mkdir()
        subprocess.run(["git", "init"], cwd=str(ws), check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "test@example.test"],
                       cwd=str(ws), check=True, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Test"],
                       cwd=str(ws), check=True, capture_output=True)
        (ws / "a.txt").write_text("one\n")
        subprocess.run(["git", "add", "-A"], cwd=str(ws), check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", "one"], cwd=str(ws),
                       check=True, capture_output=True)
        if with_origin:
            subprocess.run(["git", "remote", "add", "origin",
                            "https://github.com/example/repo.git"],
                           cwd=str(ws), check=True, capture_output=True)
        return ws

    def _passing_report(self, sd, rid, ws, head=True):
        job = core.get_job(sd, rid)
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(
            sd, rid, job, 1, "muse-spark-xhigh-free", full, "ses")
        self.assertEqual(report["status"], "ok")
        if head:
            self.assertTrue(report.get("head_commit"))
        return report

    def test_stale_proof_refuses_completion(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws")
        core.submit(sd, "stale", {"goal": "x", "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        self._passing_report(sd, "stale", ws)
        (ws / "b.txt").write_text("two\n")
        subprocess.run(["git", "add", "-A"], cwd=str(ws), check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", "two"], cwd=str(ws),
                       check=True, capture_output=True)
        job = core.get_job(sd, "stale")
        report = core.latest_turn_report(sd, "stale")
        reason = core.incomplete_proof_reason(job["task_json"], report, str(ws))
        self.assertIsNotNone(reason)
        self.assertIn("stale proof", reason)

    def test_fresh_proof_passes_stale_check(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws")
        core.submit(sd, "fresh", {"goal": "x", "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        job = core.get_job(sd, "fresh")
        report = self._passing_report(sd, "fresh", ws)
        self.assertIsNone(core.incomplete_proof_reason(job["task_json"], report, str(ws)))

    def test_acceptance_evidence_required_when_task_names_it(self):
        task = {"goal": "x", "proof": "true",
                "acceptance": "real product interaction with the fleet pane"}
        self.assertIsNotNone(core.incomplete_acceptance_reason(
            json.dumps(task), {"action": "completion", "output": "done"}))
        self.assertIsNone(core.incomplete_acceptance_reason(
            json.dumps(task), {"action": "completion", "output": "done",
                               "acceptance_evidence": "drove the fleet pane; see proof log"}))
        self.assertIsNone(core.incomplete_acceptance_reason(
            json.dumps({"goal": "x"}), {"action": "completion", "output": "done"}))

    def test_duplicate_pr_refused_and_identity_preserved(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "dup", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        self.assertIsNone(core.duplicate_pr_reason(
            None, {"action": "completion", "pr_url": "https://example.test/pr/1"}))
        core.record_known_pr(sd, "dup", "https://example.test/pr/1")
        self.assertEqual(core.known_pr_url(sd, "dup"), "https://example.test/pr/1")
        # First identity wins: re-recording the same URL keeps it.
        core.record_known_pr(sd, "dup", "https://example.test/pr/1")
        self.assertEqual(core.known_pr_url(sd, "dup"), "https://example.test/pr/1")
        reason = core.duplicate_pr_reason(
            core.known_pr_url(sd, "dup"),
            {"action": "completion", "pr_url": "https://example.test/pr/2"})
        self.assertIsNotNone(reason)
        self.assertIn("duplicate PR", reason)
        self.assertIsNone(core.duplicate_pr_reason(
            core.known_pr_url(sd, "dup"),
            {"action": "completion", "pr_url": "https://example.test/pr/1"}))

    def test_live_pr_checks_through_stub(self):
        task_json = json.dumps({"goal": "x"})
        ok = {"ok": True, "state": "OPEN", "is_draft": False,
              "head_sha": "abc123", "repo": "example/repo"}
        self.assertIsNone(core.verify_pr_for_completion(
            "/tmp", "https://github.com/example/repo/pull/1", "abc123",
            task_json, verifier=stub_pr_verifier(ok)))
        closed = dict(ok, state="CLOSED")
        reason = core.verify_pr_for_completion(
            "/tmp", "https://github.com/example/repo/pull/1", "abc123",
            task_json, verifier=stub_pr_verifier(closed))
        self.assertIn("pr_not_open", reason)
        draft = dict(ok, is_draft=True)
        reason = core.verify_pr_for_completion(
            "/tmp", "https://github.com/example/repo/pull/1", "abc123",
            task_json, verifier=stub_pr_verifier(draft))
        self.assertIn("draft", reason)
        allowed = json.dumps({"goal": "x", "draft_pr_allowed": True})
        self.assertIsNone(core.verify_pr_for_completion(
            "/tmp", "https://github.com/example/repo/pull/1", "abc123",
            allowed, verifier=stub_pr_verifier(draft)))
        moved = dict(ok, head_sha="def456")
        reason = core.verify_pr_for_completion(
            "/tmp", "https://github.com/example/repo/pull/1", "abc123",
            task_json, verifier=stub_pr_verifier(moved))
        self.assertIn("pr_head_mismatch", reason)
        missing = {"ok": False, "unknown": True, "reason": "gh not installed"}
        reason = core.verify_pr_for_completion(
            "/tmp", "https://github.com/example/repo/pull/1", "abc123",
            task_json, verifier=stub_pr_verifier(missing))
        self.assertIn("pr_unverified", reason)

    def test_live_pr_wrong_repo_refused(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws", with_origin=True)
        task_json = json.dumps({"goal": "x"})
        wrong_repo = {"ok": True, "state": "OPEN", "is_draft": False,
                      "head_sha": core._workspace_head(str(ws)),
                      "repo": "other/project"}
        reason = core.verify_pr_for_completion(
            str(ws), "https://github.com/other/project/pull/1",
            core._workspace_head(str(ws)),
            task_json, verifier=stub_pr_verifier(wrong_repo))
        self.assertIn("pr_wrong_repo", reason)
        # gh shapes without a repo field fall back to the resolved PR URL.
        url_only = {"ok": True, "state": "OPEN", "is_draft": False,
                    "head_sha": core._workspace_head(str(ws)),
                    "repo": None,
                    "url": "https://github.com/other/project/pull/1"}
        reason = core.verify_pr_for_completion(
            str(ws), "https://github.com/other/project/pull/1",
            core._workspace_head(str(ws)),
            task_json, verifier=stub_pr_verifier(url_only))
        self.assertIn("pr_wrong_repo", reason)
        self.assertEqual(core._repo_from_pr_url(
            "https://github.com/toolboxmd/model-router/pull/100"),
            "toolboxmd/model-router")

    def test_step_completion_applies_new_gates(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws", with_origin=True)
        core.submit(sd, "gate-a", {"goal": "x", "proof": "true",
                                   "acceptance": "real interaction"},
                    str(ws), "p", planner_t3_thread="planner-t3")
        self._passing_report(sd, "gate-a", ws)
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='gate-a'")
        use_fake_t3(self, sd, "gate-a", default={"action": "completion",
                                                 "output": "STILL DONE"})
        saved = core.PR_VERIFIER
        core.PR_VERIFIER = stub_pr_verifier(
            {"ok": True, "state": "OPEN", "is_draft": False,
             "head_sha": core._workspace_head(str(ws)), "repo": "example/repo"})
        try:
            # Missing acceptance evidence refuses even with bound proof and
            # a verified PR.
            set_controller_state(
                sd, "gate-a", seq=1,
                last_action={"action": "completion", "output": "DONE",
                             "pr_url": "https://github.com/example/repo/pull/7"},
                last_action_name="completion")
            first = controller.step(sd, "gate-a")
            self.assertEqual(first["action"], "completion-refused-resumed")
            st = controller._load_controller_state(core.get_job(sd, "gate-a"))
            self.assertIn("acceptance", st.get("completion_refused_reason") or "")
        finally:
            core.PR_VERIFIER = saved

    def test_step_completion_refuses_unverified_pr_without_poisoning(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws", with_origin=True)
        core.submit(sd, "gate-b", {"goal": "x", "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        self._passing_report(sd, "gate-b", ws)
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='gate-b'")
        use_fake_t3(self, sd, "gate-b", default={"action": "completion",
                                                 "output": "STILL DONE"})
        saved = core.PR_VERIFIER
        core.PR_VERIFIER = stub_pr_verifier(
            {"ok": True, "state": "CLOSED", "is_draft": False,
             "head_sha": core._workspace_head(str(ws)), "repo": "example/repo"})
        try:
            # The PR is closed, so completion refuses; the unverified URL
            # is never preserved as the job's identity.
            set_controller_state(
                sd, "gate-b", seq=1,
                last_action={"action": "completion", "output": "DONE",
                             "pr_url": "https://github.com/example/repo/pull/7"},
                last_action_name="completion")
            first = controller.step(sd, "gate-b")
            self.assertEqual(first["action"], "completion-refused-resumed")
            st = controller._load_controller_state(core.get_job(sd, "gate-b"))
            self.assertIn("pr_not_open", st.get("completion_refused_reason") or "")
            self.assertIsNone(core.known_pr_url(sd, "gate-b"))
        finally:
            core.PR_VERIFIER = saved
        # A corrected URL verifies and completes: the invalid first URL
        # never poisons the job into a false duplicate.
        core.PR_VERIFIER = stub_pr_verifier(
            {"ok": True, "state": "OPEN", "is_draft": False,
             "head_sha": core._workspace_head(str(ws)), "repo": "example/repo"})
        try:
            set_controller_state(
                sd, "gate-b", seq=2,
                last_action={"action": "completion", "output": "DONE",
                             "pr_url": "https://github.com/example/repo/pull/8"},
                last_action_name="completion")
            done = controller.step(sd, "gate-b")
            self.assertEqual(done["action"], "completed")
            self.assertEqual(core.known_pr_url(sd, "gate-b"),
                             "https://github.com/example/repo/pull/8")
        finally:
            core.PR_VERIFIER = saved

    def test_completion_result_carries_acceptance_and_candidate(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws")
        core.submit(sd, "gate-c", {"goal": "x", "proof": "true",
                                   "acceptance": "real interaction"},
                    str(ws), "p", planner_t3_thread="planner-t3")
        self._passing_report(sd, "gate-c", ws)
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='gate-c'")
        use_fake_t3(self, sd, "gate-c", default={"action": "completion",
                                                 "output": "STILL DONE"})
        set_controller_state(
            sd, "gate-c", seq=1,
            last_action={"action": "completion", "output": "DONE",
                         "acceptance_evidence": "drove the real interaction"},
            last_action_name="completion")
        done = controller.step(sd, "gate-c")
        self.assertEqual(done["action"], "completed")
        job = core.get_job(sd, "gate-c")
        result = json.loads(job["result_json"])
        self.assertEqual(result["acceptance_evidence"],
                         "drove the real interaction")
        self.assertEqual(result["head_commit"], core._workspace_head(str(ws)))


class ExhaustionContent(unittest.TestCase):
    def test_recovery_exhausted_asks_the_planner_not_blocks(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "exh", {"g": 1, "observer_task_id": "model-router-87"},
                    str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='exh'")
        # Every recovery rung already ran once in this job.
        mark_turns(sd, "exh", ("grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"))
        set_controller_state(sd, "exh", ladder={"failures": 3, "rung": "recovery",
                                                "escalated": True})
        ended = controller._apply_ladder(sd, "exh")
        self.assertEqual(ended["action"], "recovery-exhausted-question")
        self.assertEqual(ended["qid"], "recovery-decision")
        # The job and Observer identities are preserved: the logical job
        # continues through the planner's answer, never cancel/resubmit.
        job = core.get_job(sd, "exh")
        self.assertEqual(job["status"], "question_pending")
        self.assertEqual(json.loads(job["task_json"]).get("observer_task_id"),
                         "model-router-87")
        pending = core.list_questions(sd, "exh")
        self.assertEqual([q["qid"] for q in pending], ["recovery-decision"])
        prompt = pending[0]["prompt"]
        for needle in ("decision required", "evidence", "attempted",
                       "eligible dispatcher routes", "recommendation"):
            self.assertIn(needle, prompt)
        self.assertNotIn("implement it yourself", prompt)
        st = controller._load_controller_state(job)
        self.assertEqual(st.get("recovery_question_qid"), "recovery-decision")
        self.assertEqual(st.get("last_action", {}).get("action"),
                         "planner_question")

    def test_exhaustion_answer_permits_exactly_one_directed_attempt(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "exh2", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='exh2'")
        # Every recovery rung already ran once in this job.
        mark_turns(sd, "exh2", ("grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"))
        set_controller_state(sd, "exh2", ladder={"failures": 3, "rung": "recovery",
                                                 "escalated": True})
        ended = controller._apply_ladder(sd, "exh2")
        self.assertEqual(ended["action"], "recovery-exhausted-question")
        # The planner answers with direction (an eligible route), never
        # with implementation.
        core.answer(sd, "exh2", "recovery-decision",
                    "use glm-5.3-flash-go with a narrower scope")
        set_controller_state(sd, "exh2", seq=7,
                             last_action={"action": "planner_question",
                                          "qid": "recovery-decision",
                                          "prompt": core.list_questions(
                                              sd, "exh2", only_pending=False)[0]["prompt"]},
                             last_action_name="planner_question")

        def fake_resume(state_dir, request_id, prompt, run_cmd=None, label=""):
            env = {"action": "implementation", "artifact": "",
                   "directed_route": "glm-5.3-flash-go"}
            return {"action": "resumed", "luna_action": env}

        real_resume = controller.resume_luna
        controller.resume_luna = fake_resume
        try:
            answered = controller._handle_question_action(
                sd, "exh2",
                {"action": "planner_question", "qid": "recovery-decision",
                 "prompt": core.list_questions(
                     sd, "exh2", only_pending=False)[0]["prompt"]})
        finally:
            controller.resume_luna = real_resume
        self.assertEqual(answered["action"], "question-answered-resumed")
        st = controller._load_controller_state(core.get_job(sd, "exh2"))
        self.assertTrue(st.get("planner_recovery_authorized"))
        # The authorized directed attempt runs once on the directed route:
        # the authorization stays in flight until a genuine worker result,
        # so a fake genuine implementation is used here (the stub run_cmd
        # alone would block without a worker result and must not consume).
        set_controller_state(sd, "exh2", seq=8,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        real_impl = controller.run_implementation
        real_resume2 = controller.resume_luna

        def fake_impl(state_dir, request_id, artifact=None, payload=None,
                      run_cmd=None):
            return {"action": "implementation_ok",
                    "report": {"seq": 8, "proof_exit_code": 0,
                               "proof_class": "pass"},
                    "session": "s"}

        def fake_resume2(state_dir, request_id, prompt, run_cmd=None,
                         label=""):
            return {"action": "resumed", "luna_action": {"action": "completion"}}

        controller.run_implementation = fake_impl
        controller.resume_luna = fake_resume2
        try:
            out = controller._handle_implementation_action(
                sd, "exh2",
                {"action": "implementation",
                 "directed_route": "glm-5.3-flash-go"})
        finally:
            controller.run_implementation = real_impl
            controller.resume_luna = real_resume2
        self.assertEqual(out["action"], "implementation-resumed")
        st = controller._load_controller_state(core.get_job(sd, "exh2"))
        self.assertTrue(st.get("planner_recovery_used"))
        self.assertFalse(st.get("planner_recovery_in_flight"))
        self.assertEqual(core.get_job(sd, "exh2")["route"],
                         "glm-5.3-flash-go")
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='exh2'"
                             " AND kind='route_switched' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertEqual(json.loads(ev["payload_json"])["reason"],
                         "planner_directed")
        # No implicit loop: the authorization is single-use.
        self.assertFalse(controller._consume_planner_authorized_attempt(
            sd, "exh2", {"action": "implementation",
                         "directed_route": "glm-5.3-flash-go"}))

    def test_used_authorization_ends_terminally_without_another_question(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "exh3", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='exh3'")
        mark_turns(sd, "exh3", ("grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"))
        set_controller_state(sd, "exh3", ladder={"failures": 3, "rung": "recovery",
                                                 "escalated": True},
                             planner_recovery_authorized=True,
                             planner_recovery_used=True)
        ended = controller._apply_ladder(sd, "exh3")
        self.assertEqual((ended["action"], ended["reason"]),
                         ("failed", "escalation_exhausted"))
        self.assertEqual(core.list_questions(sd, "exh3"), [],
                         "no second planner question after the used attempt")

    def test_approach_only_answer_consumes_one_attempt_without_repeat_question(self):
        # A planner answer with only an approach (no directed_route) reserves
        # exactly one dispatcher-owned attempt on the current route and
        # never re-posts the recovery question or loops. The reservation
        # stays in flight through capacity moves and is consumed only when
        # the authorized attempt reaches a genuine worker result: the old
        # helper marked used before the turn ran, so a preflight move lost
        # the authorization without ever running (see the faithful
        # go-to-build regression below).
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "approach", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='approach'")
        before_route = core.get_job(sd, "approach")["route"]
        set_controller_state(sd, "approach",
                             ladder={"failures": 3, "rung": "recovery", "escalated": True},
                             planner_recovery_authorized=True,
                             seq=9,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        consumed = controller._consume_planner_authorized_attempt(
            sd, "approach", {"action": "implementation"})
        self.assertTrue(consumed, "approach-only answer reserves the authorization")
        self.assertEqual(core.get_job(sd, "approach")["route"], before_route)
        st = controller._load_controller_state(core.get_job(sd, "approach"))
        self.assertTrue(st.get("planner_recovery_in_flight"))
        self.assertFalse(st.get("planner_recovery_used"),
                         "in flight through capacity moves, consumed only on a genuine result")
        con = store.connect(sd)
        try:
            dec = con.execute("SELECT payload_json FROM events WHERE request_id='approach'"
                              " AND kind='recovery_decision' ORDER BY id DESC LIMIT 1").fetchone()
            qs = con.execute("SELECT COUNT(*) AS n FROM questions WHERE request_id='approach'"
                             " AND qid='recovery-decision'").fetchone()["n"]
        finally:
            con.close()
        self.assertIsNotNone(dec)
        payload = json.loads(dec["payload_json"])
        self.assertEqual(payload["rung"], "recovery_directed")
        self.assertEqual(payload["target"], before_route)
        self.assertEqual(payload["reason"], "planner_directed")
        self.assertEqual(qs, 0, "no repeated planner question for an approach answer")
        # Continuing the same authorized attempt after a capacity move
        # reuses the reservation without a second decision or question.
        continued = controller._consume_planner_authorized_attempt(
            sd, "approach", {"action": "implementation"})
        self.assertTrue(continued, "same authorized attempt continues in flight")
        con = store.connect(sd)
        try:
            ndec = con.execute("SELECT COUNT(*) AS n FROM events WHERE request_id='approach'"
                               " AND kind='recovery_decision'").fetchone()["n"]
        finally:
            con.close()
        self.assertEqual(ndec, 1, "no second recovery decision for the same attempt")
        # After the genuine result the single use is consumed: no loop.
        set_controller_state(sd, "approach", planner_recovery_used=True,
                             planner_recovery_in_flight=False)
        self.assertFalse(controller._consume_planner_authorized_attempt(
            sd, "approach", {"action": "implementation"}),
            "single use only, no implicit loop")

    def test_escalation_exhausted_error_carries_decision(self):
        # Normal failures at 4 reach the planner before any authorized
        # directed attempt is used; only after that one attempt is used
        # does the job end terminally with the same concrete decision.
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "esc", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='esc'")
        set_controller_state(sd, "esc", ladder={"failures": 4, "rung": "recovery",
                                                "escalated": True})
        first = controller._apply_ladder(sd, "esc")
        self.assertEqual((first["action"], first["reason"]),
                         ("recovery-exhausted-question", "recovery_exhausted"))
        self.assertEqual(core.get_job(sd, "esc")["status"], "question_pending")
        # After the single authorized attempt is used, the same failures
        # end terminally with the decision payload for the planner.
        set_controller_state(sd, "esc", ladder={"failures": 4, "rung": "recovery_directed",
                                                "escalated": True},
                             planner_recovery_authorized=True,
                             planner_recovery_used=True,
                             recovery_question_qid="recovery-decision")
        sql(sd, "UPDATE jobs SET status='running' WHERE request_id='esc'")
        ended = controller._apply_ladder(sd, "esc")
        self.assertEqual((ended["action"], ended["reason"]), ("failed", "escalation_exhausted"))
        err = json.loads(core.get_job(sd, "esc")["result_json"])["error"]
        self.assertEqual(err["code"], "ESCALATION_EXHAUSTED")
        self.assertIn("decision_required", err)
        self.assertIn("evidence", err)
        self.assertIn("attempted", err)
        self.assertIn("recommendation", err)

    def test_normal_failures_four_questions_planner_before_authorized_used(self):
        # The usual path is one Grok escalation that fails: failures reach
        # 4 with no authorized attempt used yet. The planner must be woken
        # through the existing question path, preserving the job.
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "norm4", {"g": 1, "observer_task_id": "model-router-87"},
                    str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='norm4'")
        set_controller_state(sd, "norm4", ladder={"failures": 4, "rung": "recovery",
                                                  "escalated": True})
        ended = controller._apply_ladder(sd, "norm4")
        self.assertEqual(ended["action"], "recovery-exhausted-question")
        self.assertEqual(ended["qid"], "recovery-decision")
        job = core.get_job(sd, "norm4")
        self.assertEqual(job["status"], "question_pending")
        pending = core.list_questions(sd, "norm4")
        self.assertEqual([q["qid"] for q in pending], ["recovery-decision"])
        self.assertIn("decision required", pending[0]["prompt"])

    def test_recovery_decision_events_link_attempts(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "dec", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, ("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), opencode_session_id='s',"
                 " status='running' WHERE request_id='dec'"))
        set_controller_state(sd, "dec", seq=3,
                             ladder={"failures": 1, "counted_seq": 3})
        controller._apply_ladder(sd, "dec")
        con = store.connect(sd)
        try:
            rows = con.execute("SELECT payload_json FROM events WHERE request_id='dec'"
                               " AND kind='recovery_decision' ORDER BY id").fetchall()
        finally:
            con.close()
        self.assertEqual(len(rows), 1)
        payload = json.loads(rows[0]["payload_json"])
        self.assertEqual(payload["rung"], "correction")
        self.assertEqual(payload["failed_seq"], 3)
        # The next seq is unknown at decision time: questions, refusals,
        # and completions may consume seq numbers first, never a guess.
        self.assertIsNone(payload["next_attempt_seq"])
        self.assertTrue(payload.get("decided_at"))
        # The actual next attempt links when it starts.
        set_controller_state(sd, "dec", seq=5)
        controller._link_recovery_attempt(sd, "dec", 5, "muse-spark-xhigh-free")
        con = store.connect(sd)
        try:
            link = con.execute("SELECT payload_json FROM events WHERE request_id='dec'"
                               " AND kind='recovery_next_attempt' ORDER BY id").fetchall()
        finally:
            con.close()
        self.assertEqual(len(link), 1)
        linked = json.loads(link[0]["payload_json"])
        self.assertEqual((linked["failed_seq"], linked["next_seq"]), (3, 5))
        # And its outcome is preserved when recorded.
        controller._record_turn_outcome(
            sd, "dec", {"action": "implementation_ok",
                        "report": {"seq": 5, "proof_exit_code": 0}})
        con = store.connect(sd)
        try:
            res = con.execute("SELECT payload_json FROM events WHERE request_id='dec'"
                              " AND kind='recovery_attempt_result' ORDER BY id").fetchall()
        finally:
            con.close()
        self.assertEqual(len(res), 1)
        self.assertEqual(json.loads(res[0]["payload_json"])["outcome"], "ok")


class PlannerAuthorizedInflight(unittest.TestCase):
    """Failing-before regression for the planner-authorized consumption fix.

    Old code marked planner_recovery_used before run_implementation, so an
    approach-only answer on grok-4.6-go (already used, one turn per job)
    lost its authorization on the preflight move to grok-4.6-build without
    ever running, and the next step ended exhausted. This test drives the
    actual controller seam with a deterministic worker fixture and would
    fail on that code (used True after the first step, zero worker runs,
    terminal exhaustion next).
    """

    def test_approach_only_survives_preflight_and_runs_once_on_build(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        rid = "auth-inflight-001"
        core.submit(sd, rid,
                    {"goal": "inflight drill", "proof": "true",
                     "observer_task_id": "model-router-87"},
                    str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running',"
            " route='grok-4.6-go' WHERE request_id=?", (rid,))
        # grok-4.6-go already ran its one turn in this job.
        mark_turns(sd, rid, ("grok-4.6-go",))
        set_controller_state(sd, rid, seq=5,
                             ladder={"failures": 4, "rung": "recovery",
                                     "escalated": True, "counted_seq": 4},
                             planner_recovery_authorized=True,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        envelope = {"action": "implementation"}
        fake = use_fake_t3(self, sd, rid,
                           default="IMPLEMENTED by fake grok worker")

        def worker_threads():
            return [c for c in fake.commands if c.get("type") == "thread.create"
                    and c.get("threadId", "").rsplit(".", 1)[0] != "sub.planner-t3"]
        real_resume = controller.resume_luna
        controller.resume_luna = lambda *a, **k: {
            "action": "resumed", "luna_action": {"action": "completion"}}
        self.addCleanup(setattr, controller, "resume_luna", real_resume)
        # Step one: approach-only answer hits the preflight one-turn move.
        # No worker runs yet, so the authorization must stay in flight.
        first = controller._handle_implementation_action(
            sd, rid, dict(envelope))
        self.assertIn(first.get("action"), ("route_switched", "transferred_to_go",
                                            "stalled_retry"))
        self.assertEqual(core.get_job(sd, rid)["route"], "grok-4.6-build")
        st = controller._load_controller_state(core.get_job(sd, rid))
        self.assertTrue(st.get("planner_recovery_in_flight"))
        self.assertFalse(st.get("planner_recovery_used"),
                         "preflight must not consume the authorized attempt")
        self.assertEqual(worker_threads(), [],
                         "preflight starts no worker thread")
        self.assertEqual(core.list_questions(sd, rid), [],
                         "no repeated planner question while in flight")
        con = store.connect(sd)
        try:
            decisions = con.execute(
                "SELECT payload_json FROM events WHERE request_id=? AND kind='recovery_decision'"
                " ORDER BY id", (rid,)).fetchall()
            links = con.execute(
                "SELECT payload_json FROM events WHERE request_id=? AND kind='recovery_next_attempt'"
                " ORDER BY id", (rid,)).fetchall()
        finally:
            con.close()
        self.assertEqual(len(decisions), 1)
        dec = json.loads(decisions[0]["payload_json"])
        self.assertEqual((dec["rung"], dec["reason"]),
                         ("recovery_directed", "planner_directed"))
        self.assertEqual(dec["target"], "grok-4.6-go")
        self.assertEqual(dec["failed_seq"], 4)
        self.assertEqual(links, [], "no next-attempt link before the actual worker starts")
        # Step two: the same authorized attempt continues and runs once on
        # the post-move route, with no second question or decision.
        second = controller._handle_implementation_action(
            sd, rid, dict(envelope))
        self.assertEqual(second.get("action"), "implementation-resumed")
        self.assertEqual(len(worker_threads()), 1)
        threads = controller._t3_threads_map(core.get_job(sd, rid))
        self.assertEqual(threads["impl_5"]["route"], "grok-4.6-build")
        self.assertEqual(core.get_job(sd, rid)["route"], "grok-4.6-build")
        st = controller._load_controller_state(core.get_job(sd, rid))
        self.assertTrue(st.get("planner_recovery_used"),
                        "consumed only after the genuine worker result")
        self.assertFalse(st.get("planner_recovery_in_flight"))
        self.assertEqual(core.list_questions(sd, rid), [],
                         "no repeated planner question for the same attempt")
        con = store.connect(sd)
        try:
            decisions = con.execute(
                "SELECT payload_json FROM events WHERE request_id=? AND kind='recovery_decision'"
                " ORDER BY id", (rid,)).fetchall()
            links = con.execute(
                "SELECT payload_json FROM events WHERE request_id=? AND kind='recovery_next_attempt'"
                " ORDER BY id", (rid,)).fetchall()
            results = con.execute(
                "SELECT payload_json FROM events WHERE request_id=? AND kind='recovery_attempt_result'"
                " ORDER BY id", (rid,)).fetchall()
            switches = con.execute(
                "SELECT payload_json FROM events WHERE request_id=? AND kind='route_switched'"
                " ORDER BY id", (rid,)).fetchall()
        finally:
            con.close()
        self.assertEqual(len(decisions), 1, "no second recovery decision")
        self.assertEqual(len(links), 1)
        link = json.loads(links[0]["payload_json"])
        self.assertEqual(link["failed_seq"], 4)
        self.assertEqual(link["next_seq"], 5)
        self.assertEqual(link["route"], "grok-4.6-build")
        self.assertEqual(len(results), 1)
        self.assertEqual(json.loads(results[0]["payload_json"])["outcome"], "ok")
        reasons = [json.loads(r["payload_json"])["reason"] for r in switches]
        self.assertIn("preflight_one_turn", reasons)
        self.assertNotIn("planner_directed", reasons,
                         "approach-only uses no directed switch; the decision carries it")
        # Identities and provenance stay truthful.
        job = core.get_job(sd, rid)
        self.assertEqual(job["request_id"], rid)
        self.assertEqual(json.loads(job["task_json"]).get("observer_task_id"),
                         "model-router-87")
        # Single use: a further consume is refused, and exhaustion after
        # the used attempt ends terminally with evidence, never a new ask.
        self.assertFalse(controller._consume_planner_authorized_attempt(
            sd, rid, dict(envelope)))
        set_controller_state(sd, rid, ladder={"failures": 5,
                                              "rung": "recovery_directed",
                                              "escalated": True,
                                              "counted_seq": 5})
        sql(sd, "UPDATE jobs SET status='running' WHERE request_id=?", (rid,))
        ended = controller._apply_ladder(sd, rid)
        self.assertEqual((ended["action"], ended["reason"]),
                         ("failed", "escalation_exhausted"))


class PlannerDirectedRoute(unittest.TestCase):
    def _impl_job(self, rid):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, rid, {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id=?", (rid,))
        return tmp, sd, ws

    def test_eligible_planner_route_is_assigned(self):
        _tmp, sd, _ws = self._impl_job("pdr-ok")
        set_controller_state(sd, "pdr-ok", seq=1,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        out = controller._handle_implementation_action(
            sd, "pdr-ok",
            {"action": "implementation", "directed_route": "muse-spark-xhigh-go"})
        self.assertEqual(core.get_job(sd, "pdr-ok")["route"], "muse-spark-xhigh-go")
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='pdr-ok'"
                             " AND kind='route_switched' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertEqual(json.loads(ev["payload_json"])["reason"], "planner_directed")

    def test_ordinary_envelope_route_never_undoes_the_ladder(self):
        # At two failures the ladder owns the move to the correction
        # route even when the envelope repeats the old default: the
        # ordinary route field is inert and no planner_directed event
        # credits the planner with a direction it never gave.
        _tmp, sd, _ws = self._impl_job("pdr-ladder")
        set_controller_state(sd, "pdr-ladder", seq=2,
                             ladder={"failures": 2, "rung": "correction"},
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        controller._handle_implementation_action(
            sd, "pdr-ladder",
            {"action": "implementation", "route": "muse-spark-xhigh-free"})
        self.assertEqual(core.get_job(sd, "pdr-ladder")["route"],
                         "kimi-k2.7-code-go")
        con = store.connect(sd)
        try:
            rows = con.execute("SELECT payload_json FROM events WHERE request_id='pdr-ladder'"
                               " AND kind='route_switched' ORDER BY id").fetchall()
        finally:
            con.close()
        reasons = [json.loads(r["payload_json"])["reason"] for r in rows]
        self.assertIn("correction", reasons)
        self.assertNotIn("planner_directed", reasons)

    def test_correction_stage_route_assignable_from_any_lane(self):
        _tmp, sd, _ws = self._impl_job("pdr-corr")
        set_controller_state(sd, "pdr-corr", seq=1,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        controller._handle_implementation_action(
            sd, "pdr-corr",
            {"action": "implementation",
             "directed_route": "kimi-k2.7-code-go"})
        self.assertEqual(core.get_job(sd, "pdr-corr")["route"],
                         "kimi-k2.7-code-go")

    def test_ineligible_planner_route_is_rejected_without_substitution(self):
        _tmp, sd, _ws = self._impl_job("pdr-no")
        before = core.get_job(sd, "pdr-no")["route"]
        set_controller_state(sd, "pdr-no", seq=1,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        out = controller._handle_implementation_action(
            sd, "pdr-no",
            {"action": "implementation", "directed_route": "no-such-route"})
        self.assertEqual(core.get_job(sd, "pdr-no")["route"], before)
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='pdr-no'"
                             " AND kind='planner_route_rejected' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertIsNotNone(ev)
        self.assertIn("no-such-route", json.loads(ev["payload_json"])["requested"])

    def test_planner_harness_rung_stays_with_planner(self):
        # Planner-chosen rungs run in the planner session, never as a
        # dispatcher-assigned worker turn: the dispatcher rejects them
        # with the boundary instead of substituting a model silently.
        _tmp, sd, _ws = self._impl_job("pdr-rung")
        before = core.get_job(sd, "pdr-rung")["route"]
        set_controller_state(sd, "pdr-rung", seq=1,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        controller._handle_implementation_action(
            sd, "pdr-rung",
            {"action": "implementation", "directed_route": "astra/medium"})
        self.assertEqual(core.get_job(sd, "pdr-rung")["route"], before)
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='pdr-rung'"
                             " AND kind='planner_route_rejected' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertIsNotNone(ev)
        self.assertIn("astra/medium", json.loads(ev["payload_json"])["requested"])


def _dead_group():
    """A real process group proven dead: spawn detached, reap, verify."""
    proc = subprocess.Popen(["sleep", "0.05"], start_new_session=True)
    pgid = os.getpgid(proc.pid)
    proc.wait()
    time.sleep(0.2)
    assert not core._is_pgid_alive(pgid), "fixture group must be dead"
    return proc.pid, pgid


class TerminalClassCancelIntent(unittest.TestCase):
    def test_rc143_cancelled_only_with_explicit_job_intent(self):
        stop = {"ok": False, "rc": 143, "error": "terminated by cancellation"}
        self.assertEqual(
            core.terminal_class_for(143, stop, cancel_requested=True),
            "cancelled")
        self.assertEqual(
            core.terminal_class_for(143, stop), "infrastructure",
            "a stop returned to the dispatcher without job cancellation "
            "intent is infrastructure, never cancelled by exit code alone")
        self.assertEqual(
            core.terminal_class_for(143, None), "unknown",
            "a stop with no evidence and no intent stays unknown")


    def test_class_boundaries_preserved(self):
        self.assertEqual(
            core.terminal_class_for(
                124, {"error": "opencode serve did not emit a localhost URL"}),
            "infrastructure",
            "startup rc124 needs its supervisor marker")
        self.assertEqual(core.terminal_class_for(124, None), "timeout",
                         "an actual proof rc124 stays timeout")
        self.assertEqual(core.failure_class_for(signal="context"), "provider",
                         "context pressure classifies provider")


class HardErrorSkipsProof(unittest.TestCase):
    def test_failed_turn_skips_suite_truthfully(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "hard", {"g": 1, "proof": "true"}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running'"
                " WHERE request_id='hard'")
        job = core.get_job(sd, "hard")
        res = controller._finish_worker_turn(
            sd, "hard", job, "muse-spark-xhigh-free", 1, None,
            {"ok": False, "rc": 1, "signal": "hard",
             "signal_evidence": {"name": "DataPolicyError"},
             "error": "consent denied"},
            "sub.planner-t3.w1", 1, source=controller.T3_TURN_KIND)
        self.assertEqual(res["action"], "implementation_failed")
        report = res["report"]
        self.assertEqual(report["proof_class"], "skipped")
        self.assertIsNone(report["proof_exit_code"])
        self.assertEqual(report["failure_class"], "infrastructure")
        # A skipped proof is not a passed proof: completion refuses it.
        self.assertIsNotNone(core.incomplete_proof_reason(
            job["task_json"], report, str(ws)))


class ProofTreeOwnership(unittest.TestCase):
    def test_normal_exit_reaps_leftover_group(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        pidfile = ws / "survivor.pid"
        rc, _out, cls, _s, _e = controller._run_proof_command(
            str(ws), "sleep 30 >/dev/null 2>&1 & echo $! > survivor.pid; exit 0",
            timeout=10, state_dir=sd, request_id="proof-left")
        self.assertEqual((rc, cls), (0, "pass"))
        survivor = int(pidfile.read_text().strip())
        time.sleep(0.5)
        with self.assertRaises(ProcessLookupError):
            os.kill(survivor, 0)

    def test_pipe_held_child_is_not_a_timeout(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        pidfile = ws / "survivor.pid"
        rc, _out, cls, _s, _e = controller._run_proof_command(
            str(ws), "sleep 30 & echo $! > survivor.pid; exit 0",
            timeout=5, state_dir=sd, request_id="proof-pipe")
        # The main shell exited 0 while a background child held the
        # pipe: the attempt classifies by its actual exit, never 124.
        self.assertEqual((rc, cls), (0, "pass"))
        survivor = int(pidfile.read_text().strip())
        time.sleep(0.5)
        with self.assertRaises(ProcessLookupError):
            os.kill(survivor, 0)

    def test_recover_blocks_on_live_proof_group(self):
        # An isolated proof group blocks recovery while alive and clears
        # once the group is dead: the block is real ownership, not a
        # row in the test's own group.
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "proofown", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
        self.addCleanup(lambda: (proc.kill(), proc.wait()))
        core.record_proof_owner(sd, "proofown", proc.pid,
                                os.getpgid(proc.pid))
        rec = core.recover_one(sd, "proofown")
        self.assertEqual(rec["action"], "blocked-unresolved-proof")
        self.assertTrue(core.proof_owner_alive(sd, "proofown"))
        proc.kill()
        proc.wait()
        time.sleep(0.3)
        self.assertFalse(core.proof_owner_alive(sd, "proofown"))
        rec2 = core.recover_one(sd, "proofown")
        self.assertNotEqual(rec2["action"], "blocked-unresolved-proof")

    def test_proof_owner_records_start_identity(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "proofid", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
        self.addCleanup(lambda: (proc.kill(), proc.wait()))
        core.record_proof_owner(sd, "proofid", proc.pid,
                                os.getpgid(proc.pid))
        raw = core.proof_owner_path(sd, "proofid").read_text()
        rec = json.loads(raw)
        self.assertEqual(rec["pid"], proc.pid)
        self.assertTrue(rec.get("pid_start"), "start identity guards PID reuse")
        self.assertTrue(core.proof_owner_alive(sd, "proofid"))
        proc.kill()
        proc.wait()
        time.sleep(0.3)
        self.assertFalse(core.proof_owner_alive(sd, "proofid"))

    def test_public_cancel_drains_owned_proof_group(self):
        # Public cancel drains the actual owned proof group (with start
        # identity and PID reuse protection) and retains the workspace
        # claim only until ownership is confirmed dead.
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "cancelproof", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
        pgid = os.getpgid(proc.pid)
        pid = proc.pid
        self.addCleanup(lambda: (proc.poll() is None and (proc.kill(), proc.wait())))
        core.record_proof_owner(sd, "cancelproof", pid, pgid)
        self.assertTrue(core.proof_owner_alive(sd, "cancelproof"))
        job = core.cancel(sd, "cancelproof")
        self.assertEqual(job["status"], "cancelled")
        self.assertFalse(core.proof_owner_alive(sd, "cancelproof"))
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        try:
            os.killpg(pgid, 0)
            alive = True
        except ProcessLookupError:
            alive = False
        except PermissionError:
            alive = True
        self.assertFalse(alive, "owned proof group is confirmed dead")

    def test_ambiguous_proof_owner_retains_workspace_claim(self):
        # A proof record without group identities is unproven ownership,
        # never safe death: public cancel retains the workspace claim
        # instead of finalizing cancellation behind a possible owner.
        from runner import store as _store
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "ambigproof", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        path = core.proof_owner_path(sd, "ambigproof")
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _store.secure_write_text(path, json.dumps(
            {"pid": None, "pgid": None, "pid_start": None,
             "started_at": core._utcnow()}, sort_keys=True))
        self.assertTrue(core.proof_owner_alive(sd, "ambigproof"))
        job = core.cancel(sd, "ambigproof")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("pending", job["block_reason"])

    def test_unreadable_proof_owner_retains_workspace_claim(self):
        # A proof record that cannot be parsed proves nothing either:
        # cancel blocks with the claim retained until ownership resolves.
        from runner import store as _store
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "badproof", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        path = core.proof_owner_path(sd, "badproof")
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _store.secure_write_text(path, "{not json")
        self.assertTrue(core.proof_owner_alive(sd, "badproof"))
        job = core.cancel(sd, "badproof")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("pending", job["block_reason"])


class EligibleDirectedRoutes(unittest.TestCase):
    def test_lists_assignable_routes_and_rejects_rungs(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "elig", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        eligible = controller.eligible_directed_routes(sd, "elig")
        self.assertIn("muse-spark-xhigh-go", eligible)
        self.assertIn("kimi-k2.7-code-go", eligible)
        self.assertIn("grok-4.6-go", eligible)
        self.assertNotIn("astra/medium", eligible)
        self.assertNotIn("opus-5.5/high", eligible)
        self.assertNotIn("no-such-route", eligible)


class FailedCallbackWakeup(unittest.TestCase):
    def test_answer_then_recover_resumes_controller(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "cbk", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        sql(sd, ("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='blocked',"
                 " block_reason='planner_callback_failed rc=1: boom'"
                 " WHERE request_id='cbk'"))
        core.post_question(sd, "cbk", "q1", "which approach?")
        core.answer(sd, "cbk", "q1", "take the simple path")
        job = core.get_job(sd, "cbk")
        self.assertEqual(job["status"], "running")
        calls = []
        real_start = core.start_controller
        core.start_controller = lambda s, r: calls.append(r) or {"pid": 999}
        try:
            rec = core.recover_one(sd, "cbk")
        finally:
            core.start_controller = real_start
        self.assertEqual(rec["action"], "resumed-controller", rec)
        self.assertEqual(calls, ["cbk"])


class ProtocolWording(unittest.TestCase):
    def test_no_concrete_route_in_implementation_example(self):
        # The implementation envelope example carries no route field, so
        # a repeated default can never read as planner direction; an
        # explicit direction travels only as directed_route.
        self.assertNotIn('"route":"muse-spark-xhigh-free"',
                         adapters.LUNA_ACTION_PROTOCOL)
        self.assertIn("directed_route", adapters.LUNA_ACTION_PROTOCOL)

    def test_no_remote_means_complete_without_pr(self):
        # Workspaces without an origin push remote cannot open a PR: the
        # dispatcher completes without one instead of waking the planner
        # for a gate the runner waives.
        self.assertIn("no origin push remote", adapters.LUNA_ACTION_PROTOCOL)


if __name__ == "__main__":
    unittest.main()
