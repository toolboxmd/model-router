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
from runner.supervisor import process_start_identity  # noqa: E402
from tests.fakes import FAKE_OPENCODE, write_fake  # noqa: E402

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
        core.submit(sd, "sep", {"g": 1}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='sep'")
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
        core.submit(sd, "sep2", {"g": 1, "proof": "true"}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='sep2'")
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
        core.submit(sd, "skip", {"g": 1, "proof": "false"}, str(ws), "p")
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
        core.submit(sd, "stallrep", {"g": 1, "proof": "true"}, str(ws), "p")
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
        core.submit(sd, "vrow", {"g": 1, "proof": "true"}, str(ws), "p")
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
        core.submit(sd, "vskip", {"g": 1, "proof": "true"}, str(ws), "p")
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
        core.submit(sd, "stale", {"goal": "x", "proof": "true"}, str(ws), "p")
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
        core.submit(sd, "fresh", {"goal": "x", "proof": "true"}, str(ws), "p")
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
        core.submit(sd, "dup", {"g": 1}, str(ws), "p")
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

    def _refusing_resume(self, output="STILL DONE"):
        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            env = {"action": "completion", "output": output, "artifact": ""}
            lines = [
                json.dumps({"type": "thread.started", "thread_id": "thr"}),
                json.dumps({"type": "item.completed",
                            "item": {"type": "agent_message",
                                     "text": json.dumps(env)}}),
                json.dumps({"type": "turn.completed", "usage": {}}),
            ]
            return 0, "\n".join(lines) + "\n", ""
        return run

    def test_step_completion_applies_new_gates(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws", with_origin=True)
        core.submit(sd, "gate-a", {"goal": "x", "proof": "true",
                                   "acceptance": "real interaction"},
                    str(ws), "p")
        self._passing_report(sd, "gate-a", ws)
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='gate-a'")
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
            first = controller.step(sd, "gate-a", run_cmd=self._refusing_resume())
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
        core.submit(sd, "gate-b", {"goal": "x", "proof": "true"}, str(ws), "p")
        self._passing_report(sd, "gate-b", ws)
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='gate-b'")
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
            first = controller.step(sd, "gate-b", run_cmd=self._refusing_resume())
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
            done = controller.step(sd, "gate-b", run_cmd=self._refusing_resume())
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
                    str(ws), "p")
        self._passing_report(sd, "gate-c", ws)
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='gate-c'")
        set_controller_state(
            sd, "gate-c", seq=1,
            last_action={"action": "completion", "output": "DONE",
                         "acceptance_evidence": "drove the real interaction"},
            last_action_name="completion")
        done = controller.step(sd, "gate-c", run_cmd=self._refusing_resume())
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
                    str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='exh'")
        # Every recovery rung already ran once in this job.
        con = store.connect(sd)
        try:
            for i, route in enumerate(("grok-4.6-go", "grok-4.6-build", "grok-4.6-xai")):
                con.execute(
                    "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                    "owner_token,state,rc,stage,requested_route,policy_version,meta_json,"
                    "action_key,started_at,stdout_path,stderr_path) VALUES"
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"used{i}", "exh", "grok_control", "[]", str(ws), "tok",
                     "completed", 0, "implementation", route, "x",
                     json.dumps({"route": route}), f"key{i}", core._utcnow(),
                     "/dev/null", "/dev/null"))
            con.commit()
        finally:
            con.close()
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
        core.submit(sd, "exh2", {"g": 1}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='exh2'")
        # Every recovery rung already ran once in this job.
        con = store.connect(sd)
        try:
            for i, route in enumerate(("grok-4.6-go", "grok-4.6-build", "grok-4.6-xai")):
                con.execute(
                    "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                    "owner_token,state,rc,stage,requested_route,policy_version,meta_json,"
                    "action_key,started_at,stdout_path,stderr_path) VALUES"
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"x2used{i}", "exh2", "grok_control", "[]", str(ws), "tok",
                     "completed", 0, "implementation", route, "x",
                     json.dumps({"route": route}), f"x2key{i}", core._utcnow(),
                     "/dev/null", "/dev/null"))
            con.commit()
        finally:
            con.close()
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
                     sd, "exh2", only_pending=False)[0]["prompt"]},
                run_cmd=lambda *a, **k: (0, "", ""))
        finally:
            controller.resume_luna = real_resume
        self.assertEqual(answered["action"], "question-answered-resumed")
        st = controller._load_controller_state(core.get_job(sd, "exh2"))
        self.assertTrue(st.get("planner_recovery_authorized"))
        # The authorized directed attempt runs once on the directed route.
        set_controller_state(sd, "exh2", seq=8,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        controller._handle_implementation_action(
            sd, "exh2",
            {"action": "implementation",
             "directed_route": "glm-5.3-flash-go"},
            run_cmd=lambda *a, **k: (0, "", ""))
        st = controller._load_controller_state(core.get_job(sd, "exh2"))
        self.assertTrue(st.get("planner_recovery_used"))
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
        core.submit(sd, "exh3", {"g": 1}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='exh3'")
        con = store.connect(sd)
        try:
            for i, route in enumerate(("grok-4.6-go", "grok-4.6-build", "grok-4.6-xai")):
                con.execute(
                    "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                    "owner_token,state,rc,stage,requested_route,policy_version,meta_json,"
                    "action_key,started_at,stdout_path,stderr_path) VALUES"
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"u{i}", "exh3", "grok_control", "[]", str(ws), "tok",
                     "completed", 0, "implementation", route, "x",
                     json.dumps({"route": route}), f"k{i}", core._utcnow(),
                     "/dev/null", "/dev/null"))
            con.commit()
        finally:
            con.close()
        set_controller_state(sd, "exh3", ladder={"failures": 3, "rung": "recovery",
                                                 "escalated": True},
                             planner_recovery_authorized=True,
                             planner_recovery_used=True)
        ended = controller._apply_ladder(sd, "exh3")
        self.assertEqual((ended["action"], ended["reason"]),
                         ("failed", "escalation_exhausted"))
        self.assertEqual(core.list_questions(sd, "exh3"), [],
                         "no second planner question after the used attempt")

    def test_escalation_exhausted_error_carries_decision(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "esc", {"g": 1}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='esc'")
        set_controller_state(sd, "esc", ladder={"failures": 4, "rung": "recovery",
                                                "escalated": True})
        ended = controller._apply_ladder(sd, "esc")
        self.assertEqual((ended["action"], ended["reason"]), ("failed", "escalation_exhausted"))
        err = json.loads(core.get_job(sd, "esc")["result_json"])["error"]
        self.assertEqual(err["code"], "ESCALATION_EXHAUSTED")
        self.assertIn("decision_required", err)
        self.assertIn("evidence", err)
        self.assertIn("attempted", err)
        self.assertIn("recommendation", err)

    def test_recovery_decision_events_link_attempts(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "dec", {"g": 1}, str(ws), "p")
        sql(sd, ("UPDATE jobs SET codex_task_id='thr', opencode_session_id='s',"
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


class PlannerDirectedRoute(unittest.TestCase):
    def _impl_job(self, rid):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, rid, {"g": 1}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id=?", (rid,))
        return tmp, sd, ws

    def test_eligible_planner_route_is_assigned(self):
        _tmp, sd, _ws = self._impl_job("pdr-ok")
        set_controller_state(sd, "pdr-ok", seq=1,
                             last_action={"action": "implementation"},
                             last_action_name="implementation")
        out = controller._handle_implementation_action(
            sd, "pdr-ok",
            {"action": "implementation", "directed_route": "muse-spark-xhigh-go"},
            run_cmd=lambda *a, **k: (0, "", ""))
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
            {"action": "implementation", "route": "muse-spark-xhigh-free"},
            run_cmd=lambda *a, **k: (0, "", ""))
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
             "directed_route": "kimi-k2.7-code-go"},
            run_cmd=lambda *a, **k: (0, "", ""))
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
            {"action": "implementation", "directed_route": "no-such-route"},
            run_cmd=lambda *a, **k: (0, "", ""))
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
            {"action": "implementation", "directed_route": "astra/medium"},
            run_cmd=lambda *a, **k: (0, "", ""))
        self.assertEqual(core.get_job(sd, "pdr-rung")["route"], before)
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='pdr-rung'"
                             " AND kind='planner_route_rejected' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertIsNotNone(ev)
        self.assertIn("astra/medium", json.loads(ev["payload_json"])["requested"])


class Runtime75Caveats(unittest.TestCase):
    def test_blocked_runtime_missing_question_recovers_in_one_call(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "qrm", {"g": 1}, str(ws), "p")
        sql(sd, ("UPDATE jobs SET codex_task_id='thr', status='blocked',"
                 " block_reason='runtime_missing: old runtime path /gone is gone; run recover"
                 " on the installed runtime (do not restore cache directories)'"
                 " WHERE request_id='qrm'"))
        core.post_question(sd, "qrm", "q1", "which approach?")
        calls = []
        real_start = core.start_controller
        core.start_controller = lambda s, r: calls.append(r) or {"pid": 999}
        try:
            rec = core.recover_one(sd, "qrm")
        finally:
            core.start_controller = real_start
        self.assertEqual(rec["action"], "resumed-controller", rec)
        self.assertEqual(calls, ["qrm"])

    def test_live_child_replacement_gated_on_incompatible_runtime(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "gate75", {"g": 1}, str(ws), "p")
        proc = subprocess.Popen(["sleep", "60"])
        self.addCleanup(lambda: (proc.kill(), proc.wait()))
        pgid = os.getpgid(proc.pid)
        start = process_start_identity(proc.pid)
        self_ident = process_start_identity(os.getpid())
        root = store.ensure_state_dir(sd)
        out_path = root / "outputs" / "live.stdout"
        err_path = root / "outputs" / "live.stderr"
        store.secure_write_text(out_path, "")
        store.secure_write_text(err_path, "")
        sql(sd, ("UPDATE jobs SET codex_task_id='thr', status='running',"
                 " owner_token=NULL, owner_pid=NULL WHERE request_id='gate75'"))
        con = store.connect(sd)
        try:
            # Live owned-server child, but a stored schema newer than this
            # runtime's: the installed runtime cannot take this state over.
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                "owner_token,pid,pgid,process_start,supervisor_pid,supervisor_start,"
                "stdout_path,stderr_path,started_at,state,rc,stage,requested_route,"
                "policy_version,meta_json,action_key) VALUES"
                "('live1','gate75','opencode_control','[]',"
                "?,?,?,?,?,?,?,?,?,?,"
                "'running',NULL,'implementation','muse-spark-xhigh-free','x','{}','key1')",
                (str(ws), "tok", proc.pid, pgid, start, os.getpid(), self_ident,
                 str(out_path), str(err_path), core._utcnow()))
            con.commit()
        finally:
            con.close()
        invs = core._list_invocations(sd, "gate75")
        self.assertEqual(core._invocation_ownership(invs[0]), "live")
        sql(sd, "UPDATE invocations SET schema_version=999 WHERE invocation_id='live1'")
        calls = []
        real_start = core.start_controller
        core.start_controller = lambda s, r: calls.append(r) or {"pid": 999}
        try:
            rec = core.recover_one(sd, "gate75")
        finally:
            core.start_controller = real_start
        self.assertEqual(rec["action"], "adopted-live-invocation", rec)
        self.assertEqual(calls, [], "incompatible state must not start a replacement controller")
        self.assertIn("runtime_incompatible", rec.get("resume_error") or "")

    def test_terminal_late_rows_are_consumed_for_measurements(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "late", {"g": 1}, str(ws), "p")
        root = store.ensure_state_dir(sd)
        store.secure_write_text(root / "outputs" / "late1.stdout", "")
        store.secure_write_text(root / "outputs" / "late1.stderr", "")
        sql(sd, "UPDATE jobs SET status='succeeded' WHERE request_id='late'")
        con = store.connect(sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                "owner_token,pid,pgid,stdout_path,stderr_path,started_at,state,rc,stage,"
                "requested_route,policy_version,meta_json,action_key) VALUES"
                "('late1','late','grok_control','[]',"
                "?,?,999998,999999,?,?,?,"
                "'completed',0,'implementation','grok-4.6-build','x','{}','key9')",
                (str(ws), "tok",
                 str(root / "outputs" / "late1.stdout"),
                 str(root / "outputs" / "late1.stderr"), core._utcnow()))
            con.commit()
        finally:
            con.close()
        rec = core.recover_one(sd, "late")
        self.assertEqual(rec["action"], "noop-terminal")
        self.assertEqual(rec["status"], "succeeded")
        invs = core._list_invocations(sd, "late")
        self.assertTrue(invs[0]["consumed_at"], "late evidence is recorded, never resurrected")
        self.assertTrue(invs[0]["terminal_class"], "late rows carry a classified outcome")
        self.assertEqual(core.get_job(sd, "late")["status"], "succeeded")


def _finished_worker_row(sd, rid, ws, kind="opencode_control", rc=1):
    con = store.connect(sd)
    try:
        con.execute(
            "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
            "owner_token,stdout_path,stderr_path,started_at,ended_at,state,rc,"
            "consumed_at,stage,requested_route,policy_version,meta_json,action_key)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"fin-{rid}-{kind}", rid, kind, "[]", str(ws), "",
             "/dev/null", "/dev/null", core._utcnow(), core._utcnow(),
             "failed", rc, core._utcnow(), "implementation",
             "muse-spark-xhigh-free", "x",
             json.dumps({"route": "muse-spark-xhigh-free", "seq": 1}), "k1"))
        con.commit()
    finally:
        con.close()


def _supervisor_run(rc, result):
    def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
        return rc, "RUNNER_RESULT " + json.dumps(result) + "\n", ""
    return run


class ConfirmedStopRecovery(unittest.TestCase):
    def _running_job(self, rid):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, rid, {"g": 1, "proof": "true"}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running'"
                " WHERE request_id=?", (rid,))
        return tmp, sd, ws

    def _turn(self, sd, rid, ws, run):
        job = core.get_job(sd, rid)
        return controller._run_opencode_turn(
            sd, rid, job, str(ws), "muse-spark-xhigh-free", "prompt",
            1, "initial", run, artifact=None, attempt=0)

    def test_rc124_startup_failure_returns_evidence(self):
        _tmp, sd, ws = self._running_job("stop124")
        _finished_worker_row(sd, "stop124", ws)
        res = self._turn(sd, "stop124", ws, _supervisor_run(
            124, {"ok": False, "rc": 124,
                  "error": "opencode serve did not emit a localhost URL"}))
        self.assertEqual(res["action"], "implementation_failed")
        self.assertEqual(res["report"]["failure_class"], "infrastructure")
        self.assertNotEqual(res["report"]["failure_class"], "timeout")
        self.assertEqual(core.get_job(sd, "stop124")["status"], "running")

    def test_rc143_confirmed_stop_returns_evidence(self):
        _tmp, sd, ws = self._running_job("stop143")
        _finished_worker_row(sd, "stop143", ws)
        res = self._turn(sd, "stop143", ws, _supervisor_run(
            143, {"ok": False, "rc": 143,
                  "error": "terminated by cancellation"}))
        self.assertEqual(res["action"], "implementation_failed")
        self.assertEqual(res["report"]["failure_class"], "infrastructure")
        self.assertEqual(core.get_job(sd, "stop143")["status"], "running")

    def test_rc143_grok_confirmed_stop_returns_evidence(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "stopg", {"g": 1, "proof": "true"}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running'"
                " WHERE request_id='stopg'")
        _finished_worker_row(sd, "stopg", ws, kind="grok_control", rc=143)
        job = core.get_job(sd, "stopg")
        res = controller._run_grok_turn(
            sd, "stopg", job, str(ws), "grok-4.6-build", "prompt",
            1, "initial",
            _supervisor_run(143, {"ok": False, "rc": 143,
                                  "error": "terminated by cancellation"}),
            artifact=None, attempt=0)
        self.assertEqual(res["action"], "implementation_failed")
        self.assertEqual(core.get_job(sd, "stopg")["status"], "running")

    def test_missing_result_stays_blocked(self):
        _tmp, sd, ws = self._running_job("stop-missing")
        _finished_worker_row(sd, "stop-missing", ws)
        res = self._turn(sd, "stop-missing", ws,
                         lambda *a, **k: (1, "", ""))
        self.assertEqual(res["action"], "blocked")
        self.assertEqual(core.get_job(sd, "stop-missing")["status"], "blocked")

    def test_rc125_supervisor_loss_stays_blocked(self):
        _tmp, sd, ws = self._running_job("stop125")
        _finished_worker_row(sd, "stop125", ws)
        res = self._turn(sd, "stop125", ws, _supervisor_run(
            125, {"ok": False, "rc": 125, "error": "supervisor gone"}))
        self.assertEqual(res["action"], "blocked")

    def test_unresolved_ownership_stays_blocked(self):
        _tmp, sd, ws = self._running_job("stop-unres")
        con = store.connect(sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                "owner_token,stdout_path,stderr_path,started_at,state,stage,"
                "requested_route,policy_version,meta_json,action_key)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("unres1", "stop-unres", "opencode_control", "[]", str(ws), "",
                 "/dev/null", "/dev/null", core._utcnow(), "running",
                 "implementation", "muse-spark-xhigh-free", "x", "{}", "k9"))
            con.commit()
        finally:
            con.close()
        res = self._turn(sd, "stop-unres", ws, _supervisor_run(
            143, {"ok": False, "rc": 143, "error": "terminated"}))
        self.assertEqual(res["action"], "blocked")
        self.assertEqual(core.get_job(sd, "stop-unres")["status"], "blocked")


class HardErrorSkipsProof(unittest.TestCase):
    def test_failed_turn_skips_suite_truthfully(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "hard", {"g": 1, "proof": "true"}, str(ws), "p")
        sql(sd, "UPDATE jobs SET codex_task_id='thr', status='running'"
                " WHERE request_id='hard'")
        job = core.get_job(sd, "hard")
        res = controller._run_opencode_turn(
            sd, "hard", job, str(ws), "muse-spark-xhigh-free", "prompt",
            1, "initial",
            _supervisor_run(1, {"ok": False, "rc": 1, "signal": "hard",
                                "signal_evidence": {"name": "DataPolicyError"},
                                "error": "consent denied"}),
            artifact=None, attempt=0)
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
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "proofown", {"g": 1}, str(ws), "p")
        proc = subprocess.Popen(["sleep", "60"])
        self.addCleanup(lambda: (proc.kill(), proc.wait()))
        core.record_proof_owner(sd, "proofown", proc.pid,
                                os.getpgid(proc.pid))
        try:
            rec = core.recover_one(sd, "proofown")
        finally:
            pass
        self.assertEqual(rec["action"], "blocked-unresolved-proof")
        self.assertTrue(core.proof_owner_alive(sd, "proofown"))
        proc.kill()
        proc.wait()
        core.clear_proof_owner(sd, "proofown")
        self.assertFalse(core.proof_owner_alive(sd, "proofown"))


class EligibleDirectedRoutes(unittest.TestCase):
    def test_lists_assignable_routes_and_rejects_rungs(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "elig", {"g": 1}, str(ws), "p")
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
        core.submit(sd, "cbk", {"g": 1}, str(ws), "p")
        sql(sd, ("UPDATE jobs SET codex_task_id='thr', status='blocked',"
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


class RoutedTrace(unittest.TestCase):
    """Ordinary routed task: failure reaches the dispatcher with evidence,
    the dispatcher assigns correction, and the job continues verified with
    no planner implementation. Runs on deterministic fixture harnesses
    only; no model is called and no subscription is spent."""

    def _setup(self, task):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        fakestate = base / "fakestate"
        fakestate.mkdir()
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_OC_MODE",
                                                "FAKE_OC_PLAN", "FAKE_OC_WRITE")}
        self.addCleanup(lambda: [os.environ.pop(k, None) if v is None
                                 else os.environ.__setitem__(k, v)
                                 for k, v in saved.items()])
        os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
        os.environ["FAKE_STATE"] = str(fakestate)
        os.environ["FAKE_OC_PLAN"] = "implement_twice_then_complete"
        os.environ.pop("FAKE_OC_WRITE", None)
        write_fake(bindir, "codex",
                   "import sys\nsys.exit(1)\n", PY)
        core.submit(sd, "trace87", dict(task), str(ws), "planner-session-87",
                    planner_harness="claude")
        return tmp, sd, ws, bindir

    def _kill_groups(self, sd):
        for inv in core._list_invocations(sd, "trace87"):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass

    def test_failure_then_dispatcher_recovery_then_verified_continuation(self):
        task = {"goal": "write fix.txt", "proof": "test -f fix.txt",
                "observer_task_id": "model-router-87"}
        tmp, sd, ws, _bindir = self._setup(task)
        self.addCleanup(lambda: self._kill_groups(sd))
        run = core.make_durable_run_cmd(sd, "trace87", "tok-trace")
        sql(sd, "UPDATE jobs SET owner_token='tok-trace' WHERE request_id='trace87'")

        def step():
            return controller.step(sd, "trace87", run_cmd=run, token="tok-trace")

        # Dispatch falls back to Luna on OpenCode Go (fake Codex fails).
        first = step()
        self.assertEqual(first["action"], "dispatched")
        st = controller._load_controller_state(core.get_job(sd, "trace87"))
        self.assertEqual(st.get("dispatch_route"), "luna-go/max")
        self.assertEqual(st.get("dispatch_route_reason"), "dispatch_fallback rc=1")

        # First worker turn writes nothing: the task proof fails, the turn
        # fails for verification, and the evidence returns to Luna.
        second = step()
        self.assertEqual(second["action"], "implementation-resumed")
        job = core.get_job(sd, "trace87")
        rep1 = core.latest_turn_report(sd, "trace87")
        self.assertEqual(rep1["status"], "failed")
        self.assertEqual(rep1["proof_exit_code"], 1)
        self.assertEqual(rep1["failure_class"], "verification")
        self.assertEqual(rep1["proof_class"], "failed")
        self.assertEqual(controller._ladder(job)["failures"], 1)
        self.assertEqual(controller._ladder(job)["rung"], "correction")

        # The dispatcher assigns correction on the same logical job: the
        # fake worker now writes the file, proof passes, and the report
        # binds that proof to the candidate commit.
        os.environ["FAKE_OC_WRITE"] = "fix.txt"
        third = step()
        self.assertEqual(third["action"], "implementation-resumed")
        self.assertEqual(third["luna_action"]["action"], "completion")
        rep2 = core.latest_turn_report(sd, "trace87")
        self.assertEqual(rep2["status"], "ok")
        self.assertEqual(rep2["proof_exit_code"], 0)
        self.assertTrue((ws / "fix.txt").exists())

        # Completion carries no PR (fixture workspace has no origin
        # remote) and succeeds with the bound proof; identities persist.
        done = step()
        self.assertEqual(done["action"], "completed")
        job = core.get_job(sd, "trace87")
        self.assertEqual(job["status"], "succeeded")
        stored_task = json.loads(job["task_json"])
        self.assertEqual(stored_task.get("observer_task_id"), "model-router-87")
        questions = core.list_questions(sd, "trace87", only_pending=False)
        self.assertEqual(questions, [], "no planner implementation was ever requested")
        invs = core._list_invocations(sd, "trace87")
        self.assertFalse([i for i in invs if i.get("state") in ("running", "cancelling")])
        con = store.connect(sd)
        try:
            decisions = [json.loads(r["payload_json"]) for r in con.execute(
                "SELECT payload_json FROM events WHERE request_id='trace87'"
                " AND kind='recovery_decision' ORDER BY id").fetchall()]
            links = [json.loads(r["payload_json"]) for r in con.execute(
                "SELECT payload_json FROM events WHERE request_id='trace87'"
                " AND kind='recovery_next_attempt' ORDER BY id").fetchall()]
            results = [json.loads(r["payload_json"]) for r in con.execute(
                "SELECT payload_json FROM events WHERE request_id='trace87'"
                " AND kind='recovery_attempt_result' ORDER BY id").fetchall()]
        finally:
            con.close()
        self.assertTrue(decisions, "recovery decision links the failed attempt")
        self.assertEqual(decisions[0]["failed_seq"], rep1["seq"])
        self.assertIsNone(decisions[0]["next_attempt_seq"],
                          "the decision never guesses the next seq")
        self.assertTrue(decisions[0].get("failed_at") and decisions[0].get("decided_at"))
        self.assertTrue(links, "the actual next attempt links when it starts")
        self.assertEqual((links[0]["failed_seq"], links[0]["next_seq"]),
                         (rep1["seq"], rep2["seq"]))
        self.assertTrue(results, "the attempt outcome is preserved")
        self.assertEqual(results[0]["outcome"], "ok")
        # Every executed proof is an observable verification row with
        # stage, timestamps, and class for the Observer importer.
        con = store.connect(sd)
        try:
            proofs = con.execute(
                "SELECT kind, stage, rc, reason, started_at, ended_at, elapsed_secs,"
                " terminal_class FROM invocations WHERE request_id='trace87'"
                " AND kind='proof' ORDER BY id").fetchall()
        finally:
            con.close()
        self.assertEqual([(dict(p)["rc"], dict(p)["reason"]) for p in proofs],
                         [(1, "failed"), (0, "pass")])
        for p in proofs:
            row = dict(p)
            self.assertEqual((row["kind"], row["stage"]), ("proof", "verification"))
            self.assertTrue(row["started_at"] and row["ended_at"])
            self.assertIn(row["terminal_class"], ("failed", "completed"))


if __name__ == "__main__":
    unittest.main()
