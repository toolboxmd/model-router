"""Escalation ladder, step budgets, and the #8 review minors."""
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


def cli(state_dir, *args, env=None, timeout=30):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(ROOT), env=env)
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(fn, secs=12.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if fn():
            return True
        time.sleep(0.2)
    return False


def kill_pid(pid):
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        return


def _cleanup_job(sd, request_id):
    try:
        job = core.get_job(sd, request_id)
    except Exception:
        return
    if job.get("owner_pid"):
        kill_pid(job["owner_pid"])
    try:
        invs = core._list_invocations(sd, request_id)
    except Exception:
        return
    for inv in invs:
        for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
            if pg:
                try:
                    os.killpg(int(pg), signal.SIGKILL)
                except Exception:
                    pass


def _set_state(sd, request_id, **fields):
    con = store.connect(sd)
    try:
        cur = con.execute("SELECT controller_state FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        st = json.loads(cur["controller_state"] or "{}") if cur and cur["controller_state"] else {}
        st.update(fields)
        con.execute("UPDATE jobs SET controller_state=?, status='running' WHERE request_id=?",
                    (json.dumps(st), request_id))
    finally:
        con.close()


class Ladder(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        ws = self.base / "ws"
        ws.mkdir()
        core.submit(self.sd, "j", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), opencode_session_id='ses_old', status='running'"
                        " WHERE request_id='j'")
        finally:
            con.close()

    def job(self):
        return core.get_job(self.sd, "j")

    def test_rungs_follow_failures_and_end_at_the_planner(self):
        self.assertIsNone(controller._apply_ladder(self.sd, "j"))
        self.assertEqual(self.job()["route"], "muse-spark-xhigh-free")
        _set_state(self.sd, "j", ladder={"failures": 1})
        self.assertIsNone(controller._apply_ladder(self.sd, "j"))
        j = self.job()
        self.assertEqual((j["route"], j["opencode_session_id"]), ("muse-spark-xhigh-free", "ses_old"))
        self.assertEqual(controller._ladder(j)["rung"], "correction")
        _set_state(self.sd, "j", ladder={"failures": 2, "rung": "correction"})
        self.assertIsNone(controller._apply_ladder(self.sd, "j"))
        j = self.job()
        self.assertEqual((j["route"], j["opencode_session_id"]), ("kimi-k2.7-code-go", None))
        self.assertEqual(controller._ladder(j)["rung"], "correction_fresh")
        _set_state(self.sd, "j", ladder={"failures": 3, "rung": "correction_fresh"})
        self.assertIsNone(controller._apply_ladder(self.sd, "j"))
        j = self.job()
        self.assertEqual(j["route"], "grok-4.6-go")
        self.assertTrue(controller._ladder(j)["escalated"])
        events = [json.loads(r["payload_json"]) for r in self._events("route_switched")]
        self.assertEqual([e["reason"] for e in events], ["correction", "escalation"])
        # Normal failures at 4 reach the planner through the existing
        # question path before any authorized directed attempt is used.
        _set_state(self.sd, "j", ladder={"failures": 4, "rung": "recovery", "escalated": True})
        asked = controller._apply_ladder(self.sd, "j")
        self.assertEqual((asked["action"], asked["reason"]),
                         ("recovery-exhausted-question", "recovery_exhausted"))
        j = self.job()
        self.assertEqual(j["status"], "question_pending")
        self.assertEqual([q["qid"] for q in core.list_questions(self.sd, "j")],
                         ["recovery-decision"])
        # Only after the single authorized attempt is used does the same
        # exhaustion end terminally with its evidence for the planner.
        _set_state(self.sd, "j", ladder={"failures": 4, "rung": "recovery_directed",
                                         "escalated": True},
                   planner_recovery_authorized=True, planner_recovery_used=True,
                   recovery_question_qid="recovery-decision")
        ended = controller._apply_ladder(self.sd, "j")
        self.assertEqual((ended["action"], ended["reason"]), ("failed", "escalation_exhausted"))
        j = self.job()
        self.assertEqual((j["status"], j["error_class"]), ("failed", "escalation_exhausted"))
        self.assertEqual(json.loads(j["result_json"])["error"]["code"], "ESCALATION_EXHAUSTED")
        self.assertEqual(core.result_view(self.sd, "j")["result"]["error"]["failures"], 4)

    def test_failed_proof_counts_as_a_failure_and_success_does_not_reset(self):
        controller._record_turn_outcome(self.sd, "j", {"action": "implementation_ok",
                                                       "report": {"proof_exit_code": 1}})
        self.assertEqual(controller._ladder(self.job())["failures"], 1)
        controller._record_turn_outcome(self.sd, "j", {"action": "implementation_failed", "report": {}})
        self.assertEqual(controller._ladder(self.job())["failures"], 2)
        controller._record_turn_outcome(self.sd, "j", {"action": "implementation_ok",
                                                       "report": {"proof_exit_code": 0}})
        self.assertEqual(controller._ladder(self.job())["failures"], 2)

    def _events(self, kind):
        con = store.connect(self.sd)
        try:
            return con.execute("SELECT payload_json FROM events WHERE request_id='j' AND kind=? ORDER BY id",
                               (kind,)).fetchall()
        finally:
            con.close()


class Budgets(unittest.TestCase):
    def test_launch_budget_is_recover_owned_and_job_budget_is_not(self):
        self.assertTrue(any("controller_step_budget_exhausted".startswith(p) or p.startswith("controller_step_budget")
                            for p in core.RECOVER_OWNED_BLOCKS))
        self.assertFalse(any(p.startswith("job_step_budget") for p in core.RECOVER_OWNED_BLOCKS))
        self.assertGreater(core.MAX_JOB_STEPS, controller.MAX_LOOP_STEPS)

    def test_step_counter_persists_across_launches(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "b", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        for i in range(1, 4):
            self.assertEqual(controller._count_step(sd, "b"), i)
        st = controller._load_controller_state(core.get_job(sd, "b"))
        self.assertEqual(st["steps_total"], 3)


class ReviewMinors(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        ws = self.base / "ws"
        ws.mkdir()
        core.submit(self.sd, "m", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id='m'")
        finally:
            con.close()

    def test_reused_qid_with_another_prompt_blocks_and_clears(self):
        core.post_question(self.sd, "m", "q1", "first prompt")
        core.answer(self.sd, "m", "q1", "yes")
        res = controller._handle_question_action(self.sd, "m", {"qid": "q1", "prompt": "another prompt"})
        self.assertEqual(res["reason"], "question_conflict")
        job = core.get_job(self.sd, "m")
        self.assertTrue(job["block_reason"].startswith("planner_question_conflict"))
        core.clear_question(self.sd, "m", "q1")
        job = core.get_job(self.sd, "m")
        self.assertEqual((job["status"], job["block_reason"]), ("running", None))
        self.assertEqual(core.list_questions(self.sd, "m", only_pending=False), [])
        with self.assertRaises(core.NotFoundError):
            core.clear_question(self.sd, "m", "q1")


    def test_docs_state_the_callback_and_resume_rules(self):
        text = (ROOT / "RUNNER.md").read_text()
        self.assertIn("stored public answer wins", text)
        self.assertIn("questions --clear", text)
        self.assertIn("ESCALATION_EXHAUSTED", text)


class LadderIdempotency(unittest.TestCase):
    """The failure count survives controller death without double counting."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        ws = self.base / "ws"
        ws.mkdir()
        core.submit(self.sd, "j", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), opencode_session_id='ses', status='running'"
                        " WHERE request_id='j'")
        finally:
            con.close()

    def test_reused_implementation_invocation_counts_once_per_seq(self):
        _set_state(self.sd, "j", seq=1)
        controller._record_turn_outcome(self.sd, "j", {"action": "implementation_failed", "report": {}})
        self.assertEqual(controller._ladder(core.get_job(self.sd, "j"))["failures"], 1)
        # A recovered controller reuses the same seq-1 invocation: no second count.
        controller._record_turn_outcome(self.sd, "j", {"action": "implementation_failed", "report": {}})
        self.assertEqual(controller._ladder(core.get_job(self.sd, "j"))["failures"], 1)
        # The next dispatcher turn is a new seq and counts again.
        _set_state(self.sd, "j", seq=2)
        controller._record_turn_outcome(self.sd, "j", {"action": "implementation_failed", "report": {}})
        self.assertEqual(controller._ladder(core.get_job(self.sd, "j"))["failures"], 2)

    def test_recovery_failure_asks_planner_without_resuming_luna(self):
        # The recovery turn just failed (failures 3 -> 4): the evidence
        # returns to the planner through the question path, never a Luna
        # resume and never a terminal fail before the authorized attempt.
        _set_state(self.sd, "j", seq=3,
                   ladder={"failures": 3, "rung": "recovery", "escalated": True, "counted_seq": 2},
                   last_action={"action": "implementation", "artifact": None, "payload": {}},
                   last_action_name="implementation")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route='grok-4.6-go' WHERE request_id='j'")
        finally:
            con.close()
        orig_impl = controller.run_implementation
        orig_resume = controller.resume_luna
        controller.run_implementation = lambda *a, **k: {"action": "implementation_failed",
                                                         "report": {"proof_exit_code": None},
                                                         "session": "s"}
        controller.resume_luna = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("an exhausted escalation must never resume Luna"))
        self.addCleanup(setattr, controller, "run_implementation", orig_impl)
        self.addCleanup(setattr, controller, "resume_luna", orig_resume)
        res = controller._handle_implementation_action(
            self.sd, "j", {"action": "implementation", "artifact": None, "payload": {}})
        self.assertEqual((res["action"], res["reason"]),
                         ("recovery-exhausted-question", "recovery_exhausted"))
        job = core.get_job(self.sd, "j")
        self.assertEqual(job["status"], "question_pending")
        self.assertEqual([q["qid"] for q in core.list_questions(self.sd, "j")],
                         ["recovery-decision"])


class ProofStatus(unittest.TestCase):
    def test_failed_proof_marks_the_report_failed(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "p", {"goal": "x", "proof": "false"}, str(ws), "pl", planner_t3_thread="planner-t3")
        job = core.get_job(sd, "p")
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(sd, "p", job, 1, "muse-spark-xhigh-free",
                                               full, "ses")
        self.assertEqual(report["proof_exit_code"], 1)
        self.assertEqual(report["status"], "failed")
        on_disk = json.loads(Path(report["report_path"]).read_text())
        self.assertEqual(on_disk["status"], "failed")


class StepBudgetDefaults(unittest.TestCase):
    def test_default_launches_cover_the_job_step_budget(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        job = core.submit(sd, "d", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
        self.assertGreater(int(job["max_attempts"]) * controller.MAX_LOOP_STEPS, core.MAX_JOB_STEPS)


if __name__ == "__main__":
    unittest.main()
