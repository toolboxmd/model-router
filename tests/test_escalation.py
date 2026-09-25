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
from runner import controller, core, policy, store  # noqa: E402
from tests.fakes import FAKE_CLAUDE, FAKE_OPENCODE, VERSION_GUARD, write_fake  # noqa: E402
from runner.core import _is_pid_alive  # noqa: E402

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


class LadderIdempotency(unittest.TestCase):
    """The failure count survives controller death without double counting."""

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
            con.execute("UPDATE jobs SET codex_task_id='thr', opencode_session_id='ses', status='running'"
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
            self.sd, "j", {"action": "implementation", "artifact": None, "payload": {}},
            run_cmd=lambda *a, **k: (0, "", ""))
        self.assertEqual((res["action"], res["reason"]),
                         ("recovery-exhausted-question", "recovery_exhausted"))
        job = core.get_job(self.sd, "j")
        self.assertEqual(job["status"], "question_pending")
        self.assertEqual([q["qid"] for q in core.list_questions(self.sd, "j")],
                         ["recovery-decision"])


class ResumeOrdering(unittest.TestCase):
    def test_zero_exit_without_turn_completed_is_failed_not_mismatch(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "m", {"g": 1}, str(ws), "p")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='m'")
        finally:
            con.close()
        out = json.dumps({"type": "thread.started", "thread_id": "other-thread"}) + "\n"
        res = controller.resume_luna(sd, "m", "ctx", run_cmd=lambda *a, **k: (0, out, ""))
        self.assertEqual(res["reason"], "codex_resume_failed")
        self.assertTrue(core.get_job(sd, "m")["block_reason"].startswith("codex_resume_failed rc=0"))


class ProofStatus(unittest.TestCase):
    def test_failed_proof_marks_the_report_failed(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "p", {"goal": "x", "proof": "false"}, str(ws), "pl")
        job = core.get_job(sd, "p")
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(sd, "p", job, 1, "muse-spark-xhigh-free",
                                               full, "ses")
        self.assertEqual(report["proof_exit_code"], 1)
        self.assertEqual(report["status"], "failed")
        on_disk = json.loads(Path(report["report_path"]).read_text())
        self.assertEqual(on_disk["status"], "failed")


class RecoveryQuestionConflict(unittest.TestCase):
    def test_consumed_question_with_another_prompt_blocks_with_conflict(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "c", {"g": 1}, str(ws), "p")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='saved', status='running' WHERE request_id='c'")
        finally:
            con.close()
        core.post_question(sd, "c", "q1", "first prompt")
        root = store.ensure_state_dir(sd)
        env = {"action": "planner_question", "qid": "q1", "prompt": "second prompt"}
        lines = [json.dumps({"type": "thread.started", "thread_id": "saved"}),
                 json.dumps({"type": "item.completed",
                             "item": {"type": "agent_message", "text": json.dumps(env)}}),
                 json.dumps({"type": "turn.completed"})]
        store.secure_write_text(root / "outputs" / "c1.stdout", "\n".join(lines) + "\n")
        store.secure_write_text(root / "outputs" / "c1.stderr", "")
        con = store.connect(sd)
        try:
            con.execute("INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                        "pid,pgid,stdout_path,stderr_path,started_at,state,rc) VALUES"
                        "('c1','c','codex_resume','[]',?,'old',999999,999999,?,?,?,?,?)",
                        (str(ws), str(root / "outputs" / "c1.stdout"),
                         str(root / "outputs" / "c1.stderr"), core._utcnow(),
                         "completed", 0))
        finally:
            con.close()
        applied = core.consume_finished_invocations(sd, "c")
        self.assertTrue(any(a.get("action") == "blocked" for a in applied), applied)
        job = core.get_job(sd, "c")
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"].startswith("planner_question_conflict"), job["block_reason"])
        # The stored prompt is untouched; the operator clears it the documented way.
        qs = core.list_questions(sd, "c", only_pending=False)
        self.assertEqual([(q["qid"], q["prompt"]) for q in qs], [("q1", "first prompt")])


class StepBudgetDefaults(unittest.TestCase):
    def test_default_launches_cover_the_job_step_budget(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        job = core.submit(sd, "d", {"g": 1}, str(ws), "p")
        self.assertGreater(int(job["max_attempts"]) * controller.MAX_LOOP_STEPS, core.MAX_JOB_STEPS)


FAKE_LUNA_ALWAYS_IMPL = r"""
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
with open(st / "codex.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
tid = "escalation-thread-001"
env = {"thread_id": tid, "action": "implementation", "artifact": "fix.txt",
       "payload": {"instructions": "write fix.txt"}}
lp = argv[argv.index("--output-last-message") + 1]
Path(lp).write_text(json.dumps(env))
for obj in ({"type": "thread.started", "thread_id": tid},
            {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(env)}},
            {"type": "turn.completed", "usage": {}}):
    print(json.dumps(obj), flush=True)
"""

FAKE_LUNA_COUNT_IMPL = r"""
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
with open(st / "codex.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
n_f = st / "codex_n"
n = int(n_f.read_text()) + 1 if n_f.exists() else 1
n_f.write_text(str(n))
tid = "budget-thread-001"
if n <= 20:
    env = {"thread_id": tid, "action": "implementation", "artifact": "fix.txt",
           "payload": {"instructions": "write fix.txt"}}
else:
    env = {"thread_id": tid, "action": "completion", "output": "BUDGET_DRILL_DONE", "artifact": ""}
lp = argv[argv.index("--output-last-message") + 1]
Path(lp).write_text(json.dumps(env))
for obj in ({"type": "thread.started", "thread_id": tid},
            {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(env)}},
            {"type": "turn.completed", "usage": {}}):
    print(json.dumps(obj), flush=True)
"""

FAKE_LUNA_CONFLICT = r"""
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
with open(st / "codex.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
n_f = st / "codex_n"
n = int(n_f.read_text()) + 1 if n_f.exists() else 1
n_f.write_text(str(n))
tid = "conflict-thread-001"
if n <= 1:
    env = {"thread_id": tid, "action": "planner_question", "qid": "q1", "prompt": "First?"}
elif n == 2:
    env = {"thread_id": tid, "action": "planner_question", "qid": "q1", "prompt": "Second?"}
else:
    env = {"thread_id": tid, "action": "completion", "output": "QUESTION_DRILL_DONE", "artifact": ""}
lp = argv[argv.index("--output-last-message") + 1]
Path(lp).write_text(json.dumps(env))
for obj in ({"type": "thread.started", "thread_id": tid},
            {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(env)}},
            {"type": "turn.completed", "usage": {}}):
    print(json.dumps(obj), flush=True)
"""


def _cli_env(base, codex_body, oc_mode="ok", extra_env=None):
    bindir = base / "bin"
    bindir.mkdir()
    fs = base / "fakestate"
    fs.mkdir()
    write_fake(bindir, "codex", codex_body, PY)
    write_fake(bindir, "claude", FAKE_CLAUDE, PY)
    write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
    env = dict(os.environ)
    env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""),
               FAKE_STATE=str(fs), FAKE_OC_MODE=oc_mode, FAKE_OC_DELAY="0.1",
               FAKE_OC_WRITE="fix.txt", PYTHONDONTWRITEBYTECODE="1")
    env.update(extra_env or {})
    return env, fs


class TestPublicEscalationDrill(unittest.TestCase):
    """Escalation through the public CLI: one escalation, then evidence to the planner."""

    def test_cli_escalation_asks_planner_once_with_evidence(self):
        # Four hard failures through the public CLI: one escalation, then
        # the evidence returns to the planner as a concrete decision
        # through the question path (never a terminal fail before the
        # single authorized directed attempt is used).
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        env, fs = _cli_env(base, FAKE_LUNA_ALWAYS_IMPL, oc_mode="hard_error",
                           extra_env={"FAKE_OC_MODE_GO": "hard_error"})
        rid = "esc-cli-001"
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"escalation drill"}',
                           "--workspace", str(ws), "--planner-session", "p-esc",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: _cleanup_job(sd, rid))
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "question_pending", 120),
                        core.get_job(sd, rid))
        job = core.get_job(sd, rid)
        self.assertEqual(job["route"], "grok-4.6-go")
        pending = core.list_questions(sd, rid)
        self.assertEqual([q["qid"] for q in pending], ["recovery-decision"])
        prompt = pending[0]["prompt"]
        for needle in ("decision required", "evidence", "attempted",
                       "eligible dispatcher routes", "recommendation"):
            self.assertIn(needle, prompt)
        ladder = controller._ladder(job)
        self.assertTrue(ladder["escalated"])
        self.assertEqual(ladder["failures"], 4)
        con = store.connect(sd)
        try:
            reasons = [json.loads(r["payload_json"])["reason"] for r in con.execute(
                "SELECT payload_json FROM events WHERE request_id=? AND kind='route_switched'"
                " ORDER BY id", (rid,)).fetchall()]
        finally:
            con.close()
        self.assertEqual(reasons, ["correction", "escalation"])
        codex_calls = [json.loads(line) for line in (fs / "codex.log").read_text().splitlines()]
        dispatches = [c for c in codex_calls if len(c) > 1 and c[0] == "exec" and c[1] != "resume"]
        resumes = [c for c in codex_calls if len(c) > 1 and c[0] == "exec" and c[1] == "resume"]
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(len(resumes), 3, "no resume after the exhausted recovery turn")
        prompts = [json.loads(line) for line in (fs / "opencode-requests.jsonl").read_text().splitlines()
                   if "prompt_async" in line]
        self.assertEqual(len(prompts), 4)
        self.assertEqual([p["body"]["model"]["modelID"] for p in prompts],
                         ["muse-spark-1.3-contributor-free", "muse-spark-1.3-contributor-free",
                          "kimi-k2.7-code", "grok-4.6"])
        reports = sorted((store.job_dir_for(store.ensure_state_dir(sd), rid)).glob("turn-*/report.json"))
        self.assertEqual(len(reports), 4)
        for rep in reports:
            self.assertEqual(json.loads(rep.read_text())["status"], "failed")


class TestPublicStepBudgetDrill(unittest.TestCase):
    """A launch blocks after 12 steps; recover continues the same job past it."""

    def test_cli_step_budget_recovers_then_succeeds(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        env, fs = _cli_env(base, FAKE_LUNA_COUNT_IMPL)
        rid = "budget-cli-001"
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"step budget drill"}',
                           "--workspace", str(ws), "--planner-session", "p-budget",
                           "--max-attempts", "5", "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: _cleanup_job(sd, rid))
        self.assertTrue(wait_for(lambda: (lambda j: j["status"] == "blocked"
                                          and (j["block_reason"] or "").startswith(
                                              "controller_step_budget_exhausted"))(
            core.get_job(sd, rid)), 90), core.get_job(sd, rid))
        job = core.get_job(sd, rid)
        self.assertEqual(controller._load_controller_state(job).get("steps_total"), 12)
        # The controller delivers the Issue #94 terminal report before it
        # releases the lease; recover only after release so it resumes the
        # job instead of adopting the still-exiting controller.
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid).get("owner_token") is None,
                                 90),
                        core.get_job(sd, rid))
        rc, rec, err = cli(sd, "recover", "--request-id", rid, env=env)
        self.assertEqual(rc, 0, err)
        self.assertEqual(rec.get("action"), "resumed-controller", rec)
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 90),
                        core.get_job(sd, rid))
        job = core.get_job(sd, rid)
        self.assertGreater(controller._load_controller_state(job).get("steps_total"), 12)
        self.assertIn("BUDGET_DRILL_DONE", job.get("result_json") or "")


class TestPublicQuestionDrill(unittest.TestCase):
    """A reused qid blocks with a conflict; questions --clear releases it publicly."""

    def test_cli_conflict_clears_via_questions_clear(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        env, fs = _cli_env(base, FAKE_LUNA_CONFLICT)
        rid = "conflict-cli-001"
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"question conflict drill"}',
                           "--workspace", str(ws), "--planner-session", "p-conflict",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: _cleanup_job(sd, rid))
        self.assertTrue(wait_for(lambda: (lambda j: j["status"] == "blocked"
                                          and (j["block_reason"] or "").startswith(
                                              "planner_question_conflict"))(
            core.get_job(sd, rid)), 60), core.get_job(sd, rid))
        rc, qs, err = cli(sd, "questions", "--request-id", rid, "--all", env=env)
        self.assertEqual(rc, 0, err)
        self.assertEqual([(q["qid"], q["status"]) for q in qs["questions"]], [("q1", "answered")])
        rc, cleared, err = cli(sd, "questions", "--request-id", rid, "--clear", "q1", env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(cleared.get("cleared"))
        self.assertEqual(core.get_job(sd, rid)["status"], "running")
        # The blocked controller delivers its end-of-job report (#94) before
        # releasing the job; start only once it has let go, or the launch
        # races it as a duplicate.
        self.assertTrue(wait_for(lambda: not core.get_job(sd, rid).get("owner_token"), 60),
                        core.get_job(sd, rid))
        rc, out, err = cli(sd, "start", "--request-id", rid, env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 60),
                        core.get_job(sd, rid))
        job = core.get_job(sd, rid)
        self.assertIn("QUESTION_DRILL_DONE", job.get("result_json") or "")
        rc, qs, err = cli(sd, "questions", "--request-id", rid, "--all", env=env)
        self.assertEqual(rc, 0, err)
        self.assertEqual([(q["qid"], q["prompt"], q["status"]) for q in qs["questions"]],
                         [("q1", "Second?", "answered")])


class TestPublicSpawnFailureDrills(unittest.TestCase):
    def test_cli_deleted_workspace_blocks_spawn_failure(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        env, fs = _cli_env(base, FAKE_LUNA_ALWAYS_IMPL)
        rid = "spawn-cli-001"
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"spawn failure drill"}',
                           "--workspace", str(ws), "--planner-session", "p-spawn",
                           "--no-start", env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: _cleanup_job(sd, rid))
        import shutil
        shutil.rmtree(ws)
        rc, out, err = cli(sd, "start", "--request-id", rid, env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "blocked", 60),
                        core.get_job(sd, rid))
        reason = core.get_job(sd, rid)["block_reason"] or ""
        # The Codex spawn fails, the OpenCode Luna fallback cannot start
        # without the workspace either: the job blocks with the reason.
        self.assertTrue(reason.startswith("codex_dispatch_failed")
                        or reason.startswith("dispatch_failed rc=127"), reason)

    def test_cli_dead_controller_failed_dispatch_blocked_by_recover(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        env, fs = _cli_env(base, FAKE_LUNA_ALWAYS_IMPL)
        rid = "spawn-cli-002"
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"dead controller drill"}',
                           "--workspace", str(ws), "--planner-session", "p-dead",
                           "--no-start", env=env)
        self.assertEqual(rc, 0, err)
        # An actual supervisor spawn failure through the durable machinery
        # (child cwd does not exist), then the controller dies before it can
        # block: the job sits running with no owner and a failed dispatch.
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok', status='running' WHERE request_id=?", (rid,))
        finally:
            con.close()
        run = core.make_durable_run_cmd(sd, rid, "tok")
        rc, out, err = run(["true"], "/nonexistent-model-router-dir", 10, kind="codex_dispatch")
        self.assertEqual(rc, 127, (out, err))
        rows = [i for i in core._list_invocations(sd, rid) if i["kind"] == "codex_dispatch"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state"], "failed")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token=NULL, owner_pid=NULL, owner_start=NULL,"
                        " status='running' WHERE request_id=?", (rid,))
        finally:
            con.close()
        rc, rec, err = cli(sd, "recover", "--request-id", rid, env=env)
        self.assertEqual(rc, 0, err)
        self.assertEqual(rec.get("action"), "blocked-failed-dispatch", rec)
        job = core.get_job(sd, rid)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"].startswith("codex_dispatch_failed rc=127"), job)


if __name__ == "__main__":
    unittest.main()
