"""Escalation ladder, step budgets, and the #8 review minors."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import controller, core, policy, store  # noqa: E402


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
        core.submit(self.sd, "j", {"g": 1}, str(ws), "p")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='thr', opencode_session_id='ses_old', status='running'"
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
        _set_state(self.sd, "j", ladder={"failures": 4, "rung": "recovery", "escalated": True})
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
        core.submit(sd, "b", {"g": 1}, str(ws), "p")
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
        core.submit(self.sd, "m", {"g": 1}, str(ws), "p")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='m'")
        finally:
            con.close()

    def test_reused_qid_with_another_prompt_blocks_and_clears(self):
        core.post_question(self.sd, "m", "q1", "first prompt")
        core.answer(self.sd, "m", "q1", "yes")
        res = controller._handle_question_action(self.sd, "m", {"qid": "q1", "prompt": "another prompt"},
                                                 run_cmd=lambda *a, **k: (0, "", ""))
        self.assertEqual(res["reason"], "question_conflict")
        job = core.get_job(self.sd, "m")
        self.assertTrue(job["block_reason"].startswith("planner_question_conflict"))
        core.clear_question(self.sd, "m", "q1")
        job = core.get_job(self.sd, "m")
        self.assertEqual((job["status"], job["block_reason"]), ("running", None))
        self.assertEqual(core.list_questions(self.sd, "m", only_pending=False), [])
        with self.assertRaises(core.NotFoundError):
            core.clear_question(self.sd, "m", "q1")

    def test_failed_resume_without_thread_is_a_failed_turn_not_a_mismatch(self):
        res = controller.resume_luna(self.sd, "m", "ctx", run_cmd=lambda *a, **k: (1, "", "boom"))
        self.assertEqual(res["reason"], "codex_resume_failed")
        job = core.get_job(self.sd, "m")
        self.assertTrue(job["block_reason"].startswith("codex_resume_failed rc=1"))

    def test_failed_first_dispatch_left_by_a_dead_controller_is_blocked_by_recover(self):
        core.submit(self.sd, "d", {"g": 1}, str(self.base / "ws"), "p") if False else None
        ws2 = self.base / "ws2"
        ws2.mkdir()
        core.submit(self.sd, "d", {"g": 1}, str(ws2), "p")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='d'")
            con.execute("INSERT INTO invocations(invocation_id, request_id, kind, cmd_json, workspace, owner_token,"
                        " stdout_path, stderr_path, started_at, state, rc, ended_at, consumed_at)"
                        " VALUES('inv1','d','codex_dispatch','[]',?,'tok','/dev/null','/dev/null',?,'failed',127,?,?)",
                        (str(ws2), core._utcnow(), core._utcnow(), core._utcnow()))
        finally:
            con.close()
        res = core.recover_one(self.sd, "d")
        self.assertEqual(res["action"], "blocked-failed-dispatch")
        job = core.get_job(self.sd, "d")
        self.assertTrue(job["block_reason"].startswith("codex_dispatch_failed rc=127"))

    def test_docs_state_the_callback_and_resume_rules(self):
        text = (ROOT / "RUNNER.md").read_text()
        self.assertIn("stored public answer wins", text)
        self.assertIn("questions --clear", text)
        self.assertIn("ESCALATION_EXHAUSTED", text)


if __name__ == "__main__":
    unittest.main()
