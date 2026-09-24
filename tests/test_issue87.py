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
        # Back-compat: the shared key still carries the dispatch reason until
        # a worker move overwrites it.
        self.assertEqual(st.get("route_reason"), "dispatch_exhausted")
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='sep'"
                             " AND kind='route_switched' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertEqual(json.loads(ev["payload_json"])["scope"], "dispatch")
        # A later worker move overwrites the worker key but preserves the
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

    def test_step_completion_preserves_pr_identity_through_refusal(self):
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
            # The PR is closed, so completion refuses; the claimed identity
            # is still preserved for the correction path to update.
            set_controller_state(
                sd, "gate-b", seq=1,
                last_action={"action": "completion", "output": "DONE",
                             "pr_url": "https://github.com/example/repo/pull/7"},
                last_action_name="completion")
            first = controller.step(sd, "gate-b", run_cmd=self._refusing_resume())
            self.assertEqual(first["action"], "completion-refused-resumed")
            st = controller._load_controller_state(core.get_job(sd, "gate-b"))
            self.assertIn("pr_not_open", st.get("completion_refused_reason") or "")
            self.assertEqual(core.known_pr_url(sd, "gate-b"),
                             "https://github.com/example/repo/pull/7")
        finally:
            core.PR_VERIFIER = saved


class ExhaustionContent(unittest.TestCase):
    def test_recovery_exhausted_carries_concrete_decision(self):
        tmp, sd, ws = new_dirs()
        self.addCleanup(tmp.cleanup)
        core.submit(sd, "exh", {"g": 1}, str(ws), "p")
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
        self.assertEqual(ended["action"], "blocked")
        job = core.get_job(sd, "exh")
        reason = job["block_reason"] or ""
        self.assertTrue(reason.startswith("recovery_exhausted:"))
        self.assertIn("decision required", reason)
        self.assertIn("evidence", reason)
        self.assertIn("attempted", reason)
        self.assertIn("recommendation", reason)
        self.assertNotIn("implement it yourself", reason)

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
        self.assertEqual(payload["next_attempt_seq"], 4)
        self.assertTrue(payload.get("decided_at"))


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
                             last_action={"action": "implementation",
                                          "route": "muse-spark-xhigh-go"},
                             last_action_name="implementation")
        out = controller._handle_implementation_action(
            sd, "pdr-ok",
            {"action": "implementation", "route": "muse-spark-xhigh-go"},
            run_cmd=lambda *a, **k: (0, "", ""))
        self.assertEqual(core.get_job(sd, "pdr-ok")["route"], "muse-spark-xhigh-go")
        con = store.connect(sd)
        try:
            ev = con.execute("SELECT payload_json FROM events WHERE request_id='pdr-ok'"
                             " AND kind='route_switched' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            con.close()
        self.assertEqual(json.loads(ev["payload_json"])["reason"], "planner_directed")

    def test_ineligible_planner_route_is_rejected_without_substitution(self):
        _tmp, sd, _ws = self._impl_job("pdr-no")
        before = core.get_job(sd, "pdr-no")["route"]
        set_controller_state(sd, "pdr-no", seq=1,
                             last_action={"action": "implementation",
                                          "route": "no-such-route"},
                             last_action_name="implementation")
        out = controller._handle_implementation_action(
            sd, "pdr-no",
            {"action": "implementation", "route": "no-such-route"},
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
                             last_action={"action": "implementation",
                                          "route": "astra/max"},
                             last_action_name="implementation")
        controller._handle_implementation_action(
            sd, "pdr-rung",
            {"action": "implementation", "route": "astra/max"},
            run_cmd=lambda *a, **k: (0, "", ""))
        self.assertEqual(core.get_job(sd, "pdr-rung")["route"], before)


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
        finally:
            con.close()
        self.assertTrue(decisions, "recovery decision links the failed attempt")
        self.assertEqual(decisions[0]["failed_seq"], rep1["seq"])
        self.assertEqual(decisions[0]["next_attempt_seq"], rep2["seq"])
        self.assertTrue(decisions[0].get("failed_at") and decisions[0].get("decided_at"))


if __name__ == "__main__":
    unittest.main()
