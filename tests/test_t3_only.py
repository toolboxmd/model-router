"""Issue #110/#116: behavior kept when T3 became the only execution path.

Stdlib only, no live T3. These tests carry the regression proof that the
deleted direct-path suites held for behavior that remains: terminal
reports (once per terminal state, never changing the job), capacity
moves that still leave a report, the escalation ladder reaching the
planner through its T3 thread exactly once, report redaction and diffs,
and capacity memory shared across jobs.
"""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, policy, store  # noqa: E402
from tests.fakes import use_fake_t3  # noqa: E402

PLANNER = "planner-t3"


class Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def submit(self, rid, task=None, **kw):
        kw.setdefault("handoff_summary", f"Summary for {rid}.")
        return core.submit(self.sd, rid, task or {"goal": "t"}, self.ws(rid),
                           f"session-{rid}", planner_t3_thread=PLANNER, **kw)

    def report_record(self, rid):
        return controller._terminal_report_record(core.get_job(self.sd, rid))

    def planner_posts(self, fake):
        return [text for tid, text in fake.posts if tid == PLANNER]


class TerminalReports(Base):
    def _drive(self, rid, status):
        if status == "succeeded":
            controller._complete_job(self.sd, rid, None, "done", None,
                                     "https://example.test/pr/x")
        elif status == "blocked":
            controller._mark_blocked(self.sd, rid, f"{rid} blocked: needs judgment")
        elif status == "failed":
            controller._fail_offline(self.sd, rid, {"code": "ESCALATION_EXHAUSTED"})
        elif status == "cancelled":
            core.cancel(self.sd, rid)

    def test_every_terminal_state_reports_once_without_changing_it(self):
        for status in ("succeeded", "blocked", "failed", "cancelled"):
            with self.subTest(status=status):
                rid = f"t-{status}"
                self.submit(rid)
                fake = use_fake_t3(self, self.sd, rid)
                self._drive(rid, status)
                before = core.get_job(self.sd, rid)
                controller.deliver_terminal_report(self.sd, rid)
                again = controller.deliver_terminal_report(self.sd, rid)
                self.assertEqual(again["action"], "already-reported")
                posts = self.planner_posts(fake)
                self.assertEqual(len(posts), 1, posts)
                self.assertIn(rid, posts[0])
                self.assertIn(status, posts[0])
                self.assertIn("Summary for", posts[0])
                after = core.get_job(self.sd, rid)
                self.assertEqual((after["status"], after["result_json"], after["block_reason"]),
                                 (before["status"], before["result_json"], before["block_reason"]))
                rec = self.report_record(rid)
                self.assertEqual((rec.get("state"), rec.get("status")), ("delivered", status))

    def test_blocked_and_failed_carry_the_reason(self):
        self.submit("b1")
        fake = use_fake_t3(self, self.sd, "b1")
        controller._mark_blocked(self.sd, "b1", "b1 blocked: waiting on planner")
        controller.deliver_terminal_report(self.sd, "b1")
        self.assertIn("waiting on planner", self.planner_posts(fake)[0])
        self.submit("f1")
        fake = use_fake_t3(self, self.sd, "f1")
        controller._fail_offline(self.sd, "f1", {"code": "ESCALATION_EXHAUSTED"})
        controller.deliver_terminal_report(self.sd, "f1")
        post = self.planner_posts(fake)[0]
        self.assertIn("failed", post)
        self.assertIn("escalation_exhausted", post)

    def test_record_failure_is_not_delivery(self):
        self.submit("rf1")
        use_fake_t3(self, self.sd, "rf1")
        controller._mark_blocked(self.sd, "rf1", "rf1 blocked")
        before = core.get_job(self.sd, "rf1")
        with mock.patch.object(controller, "_save_terminal_report_record",
                               side_effect=OSError("ledger unwritable")):
            res = controller.deliver_terminal_report(self.sd, "rf1")
        self.assertEqual(res["action"], "report-error")
        self.assertNotEqual(self.report_record("rf1").get("state"), "delivered")
        after = core.get_job(self.sd, "rf1")
        self.assertEqual((after["status"], after["result_json"]),
                         (before["status"], before["result_json"]))
        self.assertIn("record_failed", core.status_view(self.sd, "rf1").get("output_tail", ""))

    def test_second_blocked_event_with_same_reason_reports_again(self):
        self.submit("e1")
        fake = use_fake_t3(self, self.sd, "e1")
        controller._mark_blocked(self.sd, "e1", "e1 blocked: same reason")
        self.assertEqual(controller.deliver_terminal_report(self.sd, "e1")["action"], "reported")
        first_rec = self.report_record("e1")
        # Recovered to running: the old blocked record clears without a post.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running', block_reason=NULL WHERE request_id='e1'")
            con.commit()
        finally:
            con.close()
        self.assertEqual(controller.deliver_terminal_report(self.sd, "e1")["action"],
                         "noop-not-terminal")
        self.assertEqual(self.report_record("e1"), {})
        controller._mark_blocked(self.sd, "e1", "e1 blocked: same reason")
        self.assertEqual(controller.deliver_terminal_report(self.sd, "e1")["action"], "reported")
        self.assertEqual(len(self.planner_posts(fake)), 2)
        self.assertNotEqual(self.report_record("e1").get("delivered_for"),
                            first_rec.get("delivered_for"))

    def test_step_on_a_terminal_job_reports_once_without_changing_it(self):
        self.submit("cr2")
        fake = use_fake_t3(self, self.sd, "cr2")
        controller._fail_offline(self.sd, "cr2", {"code": "ESCALATION_EXHAUSTED"})
        before = core.get_job(self.sd, "cr2")
        self.assertEqual(controller.step(self.sd, "cr2")["action"], "noop-terminal")
        controller.step(self.sd, "cr2")
        self.assertEqual(len(self.planner_posts(fake)), 1)
        after = core.get_job(self.sd, "cr2")
        self.assertEqual((after["status"], after["result_json"]),
                         (before["status"], before["result_json"]))

    def test_cancel_then_recover_reports_once(self):
        self.submit("cr1")
        fake = use_fake_t3(self, self.sd, "cr1")
        core.cancel(self.sd, "cr1")
        self.assertEqual(core.get_job(self.sd, "cr1")["status"], "cancelled")
        self.assertEqual(controller.deliver_terminal_report(self.sd, "cr1")["action"], "reported")
        out = core.recover_one(self.sd, "cr1")
        self.assertEqual(out["status"], "cancelled")
        self.assertEqual(controller.deliver_terminal_report(self.sd, "cr1")["action"],
                         "already-reported")
        self.assertEqual(len(self.planner_posts(fake)), 1)


class CapacityTurns(Base):
    """A capacity signal on a T3 worker turn leaves a report and moves."""

    def _turn(self, rid, route, full):
        job = core.get_job(self.sd, rid)
        return controller._finish_worker_turn(self.sd, rid, job, route, 1, None,
                                              full, f"sub.{PLANNER}.w1", 1,
                                              source=controller.T3_TURN_KIND)

    def test_exhaustion_moves_the_same_model_to_the_next_pool(self):
        self.submit("x1", {"goal": "t", "proof": "true"}, route="muse-spark-xhigh-free")
        res = self._turn("x1", "muse-spark-xhigh-free", {
            "ok": False, "rc": 1, "signal": "exhausted", "quota": True,
            "signal_evidence": {"message": "Free usage exceeded"},
            "error": "Free usage exceeded", "idle_confirmed": True})
        self.assertEqual(core.get_job(self.sd, "x1")["route"], "muse-spark-xhigh-go")
        report = res["report"]
        self.assertEqual(report["status"], "exhausted")
        self.assertEqual(report["proof_class"], "skipped")
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))

    def test_overload_moves_to_the_next_family_and_rests_the_route(self):
        self.submit("o1", route="muse-spark-xhigh-go")
        res = self._turn("o1", "muse-spark-xhigh-go", {
            "ok": False, "rc": 1, "signal": "overloaded",
            "signal_evidence": {"message": "overloaded_error"},
            "error": "overloaded_error", "idle_confirmed": True})
        self.assertEqual(core.get_job(self.sd, "o1")["route"], "glm-5.3-flash-go")
        self.assertEqual(res["report"]["status"], "overloaded")
        self.assertIn("muse-spark-xhigh-go", core.degraded_routes(self.sd))

    def test_unconfirmed_idle_blocks_instead_of_a_second_writer(self):
        self.submit("u1", route="muse-spark-xhigh-go")
        res = self._turn("u1", "muse-spark-xhigh-go", {
            "ok": False, "rc": 1, "signal": "overloaded",
            "signal_evidence": {"message": "overloaded_error"},
            "error": "overloaded_error", "idle_confirmed": False})
        self.assertEqual(res["reason"], "route_transfer_abort_failed")
        self.assertEqual(core.get_job(self.sd, "u1")["route"], "muse-spark-xhigh-go")

    def test_exhausted_route_is_skipped_by_later_jobs(self):
        self.submit("c1")
        free_status = {"type": "retry", "attempt": 1, "message": "m", "next": 1,
                       "action": {"reason": "free_tier_limit", "provider": "opencode",
                                  "title": "t", "message": "m", "label": "l"}}
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "exhausted", free_status)
        self.assertEqual(core.select_implementation_route(
            self.sd, "muse-spark-xhigh-free", free_status),
            ("muse-spark-xhigh-go", None))
        core.recover_one(self.sd, "c1")
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        job = self.submit("c2")
        self.assertNotEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual(policy.next_capacity_route("glm-5.1-go", set()), (None, None))


def _continuing():
    import inspect
    src = inspect.getsource(controller.run_controller_process)
    return src[src.index("continuing = ("):src.index(")", src.index("continuing = ("))]


class EscalationThroughT3(Base):
    """Failing worker turns climb the ladder and reach the planner once."""

    def test_failures_end_in_one_planner_question(self):
        self.submit("esc", {"goal": "t", "proof": "false"}, lane="small")
        fake = use_fake_t3(self, self.sd, "esc",
                           default={"action": "implementation", "artifact": "a1"})
        # The first dispatch starts from a fresh job: drop the saved
        # dispatcher thread so step() really dispatches over T3.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state='{}', status='running'"
                        " WHERE request_id='esc'")
            con.commit()
        finally:
            con.close()
        # The planner answers the recovery decision in its thread.
        fake.planner_replies = ["Try a narrower scope."]
        actions = []
        for _ in range(20):
            res = controller.step(self.sd, "esc")
            actions.append(res.get("action"))
            if res.get("action") == "recovery-exhausted-question":
                # The controller loop continues into the next step, which
                # asks the planner in its thread; no manual recover.
                self.assertIn('"recovery-exhausted-question"', _continuing())
                controller.step(self.sd, "esc")
                break
        job = core.get_job(self.sd, "esc")
        self.assertTrue(controller._ladder(job)["escalated"], actions)
        reports = list(store.job_dir_for(store.ensure_state_dir(self.sd), "esc")
                       .glob("turn-*/report.json"))
        self.assertEqual(len(reports), 4)
        answered = core.list_questions(self.sd, "esc", only_pending=False)
        self.assertEqual([(q["qid"], q["status"]) for q in answered],
                         [("recovery-decision", "answered")])
        questions = [p for p in self.planner_posts(fake) if "recovery-decision" in p]
        self.assertEqual(len(questions), 1)
        self.assertIn("HANDOFF SUMMARY", questions[0])
        threads = controller._t3_threads_map(job)
        self.assertIn("dispatch", threads)
        self.assertTrue(any(k.startswith("impl_") for k in threads))


class ReportContents(Base):
    def test_worker_text_is_full_and_redacted(self):
        self.submit("r1")
        job = core.get_job(self.sd, "r1")
        long_text = "X" * 20000 + " api_key=SECRET123 Bearer abcdefgh12345678"
        full = {"assistant_text": long_text, "usage": None, "native_ids": {},
                "actual_model": None}
        report = controller._write_turn_report(self.sd, "r1", job, 9,
                                               "muse-spark-xhigh-free", full, "t-1")
        body = Path(report["worker_text"]).read_text()
        self.assertNotIn("SECRET123", body)
        self.assertNotIn("abcdefgh12345678", body)
        self.assertGreater(len(body), 8000)
        self.assertNotIn("SECRET123", report["worker_summary"])
        turn_dir = Path(report["report_path"]).parent
        for name in ("report.json", "proof.log", "diff.patch", "worker.txt"):
            self.assertEqual((turn_dir / name).stat().st_mode & 0o777, 0o600, name)

    def test_git_diff_includes_staged_and_untracked(self):
        ws = Path(self.ws("g"))
        env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x",
                   GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@x")
        subprocess.run(["git", "init", "-q", str(ws)], check=True, env=env)
        (ws / "tracked.txt").write_text("base\n")
        subprocess.run(["git", "-C", str(ws), "add", "tracked.txt"], check=True, env=env)
        subprocess.run(["git", "-C", str(ws), "commit", "-q", "-m", "base"], check=True, env=env)
        (ws / "tracked.txt").write_text("unstaged\n")
        (ws / "staged.txt").write_text("staged\n")
        subprocess.run(["git", "-C", str(ws), "add", "staged.txt"], check=True, env=env)
        (ws / "new.txt").write_text("untracked-body\n")
        files, diff, note = controller._git_changes(str(ws))
        self.assertIsNone(note)
        for name in ("tracked.txt", "staged.txt", "new.txt"):
            self.assertIn(name, files)
        for text in ("unstaged", "staged", "untracked-body"):
            self.assertIn(text, diff)


class SubmitRequiresT3(Base):
    def test_submit_without_a_planner_thread_is_refused(self):
        with self.assertRaises(ValueError) as ctx:
            core.submit(self.sd, "n1", {"goal": "t"}, self.ws("n1"), "s")
        self.assertIn("--planner-t3-thread", str(ctx.exception))

    def test_legacy_modules_are_gone(self):
        for name in ("harnesses", "supervisor", "kits", "direction"):
            self.assertFalse((ROOT / "runner" / f"{name}.py").exists(), name)
        self.assertFalse((ROOT / "skills").exists())


if __name__ == "__main__":
    unittest.main()
