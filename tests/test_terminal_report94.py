"""Issue #94: one end-of-job report per terminal state.

Stdlib only, no live CLIs. The runner wakes the saved planner session
through the existing callback path when a job reaches succeeded, blocked,
failed, or cancelled: the report carries the request id, terminal status,
PR URL or reason, and the handoff summary. Delivery runs after the
terminal persist, never changes the job's status or result, retries a
busy planner within a bounded window, and is recorded on the job (visible
in ``status``) so recovery and restarted controllers send no duplicate.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters, controller, core, harnesses, store  # noqa: E402


def claude_ok(session, answer="Report received."):
    return json.dumps({"type": "result", "subtype": "success", "is_error": False,
                       "result": answer, "session_id": session,
                       "uuid": "fake-result-uuid", "duration_ms": 12,
                       "num_turns": 1, "total_cost_usd": 0.0,
                       "usage": {"input_tokens": 10, "output_tokens": 3}})


def codex_ok(thread, answer="Report received."):
    return "\n".join(json.dumps(o) for o in [
        {"type": "thread.started", "thread_id": thread},
        {"type": "item.completed",
         "item": {"type": "agent_message", "text": answer}},
        {"type": "turn.completed", "usage": {}},
    ])


def opencode_ok(session, answer="Report received."):
    return json.dumps({"part": {"type": "text", "text": answer},
                       "sessionID": session})


def grok_ok(session="ses_fresh_000001", answer="Report received."):
    return json.dumps({"text": answer, "stopReason": "end_turn",
                       "sessionId": session, "num_turns": 1,
                       "model": "grok-4.6"})


class Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name="ws"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def submit(self, rid, harness="claude", session=None, **kw):
        session = session or f"planner-{rid}"
        kw.setdefault("handoff_summary", f"Summary for {rid}.")
        if harness != "claude":
            kw["planner_harness"] = harness
        return core.submit(self.sd, rid, {"goal": "t"}, self.ws(rid),
                           session, **kw)

    def report_record(self, rid):
        st = json.loads(core.get_job(self.sd, rid).get("controller_state") or "{}")
        return st.get("terminal_report") or {}

    def child_kinds(self, rid):
        return [c["kind"] for c in
                core._list_invocations(self.sd, rid)] + [
                r["kind"] for r in self._child_rows(rid)]

    def _child_rows(self, rid):
        con = store.connect(self.sd)
        try:
            return [dict(r) for r in con.execute(
                "SELECT kind FROM child_calls WHERE request_id=?", (rid,)).fetchall()]
        finally:
            con.close()


class SucceededClaude(Base):
    def test_complete_persists_before_callback_and_reports_ready_to_merge(self):
        self.submit("s1", session="plan-s1")
        seen = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            job = core.get_job(self.sd, "s1")
            seen["status_at_callback"] = job["status"]
            seen["result_at_callback"] = job.get("result_json")
            seen["prompt"] = cmd[-1]
            seen["kind"] = kw.get("kind")
            return 0, claude_ok("plan-s1"), ""

        res = controller._complete_job(
            self.sd, "s1", None, "done", None,
            "https://example.test/pr/1", run_cmd=fake)
        self.assertEqual(res["action"], "completed")
        # The terminal persist precedes the callback: the fake observed it.
        self.assertEqual(seen["status_at_callback"], "succeeded")
        self.assertIn("https://example.test/pr/1", seen["result_at_callback"])
        # Report content: request id, status, ready to merge, PR URL, summary.
        prompt = seen["prompt"]
        for needle in ("s1", "succeeded", "ready to merge",
                       "https://example.test/pr/1", "Summary for s1."):
            self.assertIn(needle, prompt)
        self.assertEqual(seen["kind"], "claude_callback")
        # Immutable outcome: status and result untouched by delivery.
        job = core.get_job(self.sd, "s1")
        self.assertEqual(job["status"], "succeeded")
        self.assertIn("https://example.test/pr/1", job["result_json"])
        # Recorded on the job and visible in status.
        rec = self.report_record("s1")
        self.assertEqual(rec.get("state"), "delivered")
        self.assertEqual(rec.get("status"), "succeeded")
        self.assertEqual(rec.get("attempts"), 1)
        view = core.status_view(self.sd, "s1")
        self.assertEqual(view["job"]["controller_state"]["terminal_report"]["state"],
                         "delivered")
        kinds = [r["kind"] for r in self._child_rows("s1")]
        self.assertEqual(kinds.count("claude_callback"), 1)
        # The report is not a question: no question row exists.
        self.assertEqual(core.list_questions(self.sd, "s1", only_pending=False), [])

    def test_redelivery_is_exactly_once(self):
        self.submit("s2", session="plan-s2")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(list(cmd))
            return 0, claude_ok("plan-s2"), ""

        controller._complete_job(self.sd, "s2", None, "done", None,
                                 "https://example.test/pr/2", run_cmd=fake)
        self.assertEqual(len(calls), 1)
        again = controller.deliver_terminal_report(self.sd, "s2", run_cmd=fake)
        self.assertEqual(again["action"], "already-reported")
        self.assertEqual(len(calls), 1)
        # A restarted controller (fresh run_cmd closure) still sends nothing.
        def fake2(cmd, cwd=None, timeout=120, **kw):
            calls.append(list(cmd))
            return 0, claude_ok("plan-s2"), ""
        third = controller.deliver_terminal_report(self.sd, "s2", run_cmd=fake2)
        self.assertEqual(third["action"], "already-reported")
        self.assertEqual(len(calls), 1)


class AllTerminalStatuses(Base):
    def _drive(self, rid, status):
        if status == "succeeded":
            controller._complete_job(self.sd, rid, None, "done", None,
                                     "https://example.test/pr/x",
                                     run_cmd=lambda *a, **k: (0, claude_ok(f"planner-{rid}"), ""))
        elif status == "blocked":
            controller._mark_blocked(self.sd, rid, f"{rid} blocked: needs judgment")
        elif status == "failed":
            controller._fail_offline(self.sd, rid, {"code": "ESCALATION_EXHAUSTED"})
        elif status == "cancelled":
            core.cancel(self.sd, rid)
        else:
            raise AssertionError(status)

    def test_every_terminal_state_reports_once_without_changing_it(self):
        for status in ("succeeded", "blocked", "failed", "cancelled"):
            with self.subTest(status=status):
                rid = f"t-{status}"
                self.submit(rid)
                self._drive(rid, status)
                before = core.get_job(self.sd, rid)
                calls = []

                def fake(cmd, cwd=None, timeout=120, **kw):
                    calls.append(cmd[-1])
                    return 0, claude_ok(f"planner-{rid}"), ""

                # Succeeded already reported inside _complete_job; the rest
                # report here. Either way exactly one callback total.
                res = controller.deliver_terminal_report(self.sd, rid, run_cmd=fake)
                total = len(calls) + (1 if status == "succeeded" else 0)
                self.assertEqual(total, 1, res)
                if status != "succeeded":
                    self.assertEqual(res["action"], "reported")
                    prompt = calls[0]
                    self.assertIn(rid, prompt)
                    self.assertIn(status, prompt)
                    self.assertIn("Summary for", prompt)
                after = core.get_job(self.sd, rid)
                self.assertEqual(after["status"], before["status"])
                self.assertEqual(after["result_json"], before["result_json"])
                self.assertEqual(after["block_reason"], before["block_reason"])
                rec = self.report_record(rid)
                self.assertEqual(rec.get("state"), "delivered")
                self.assertEqual(rec.get("status"), status)

    def test_blocked_and_failed_carry_the_reason(self):
        self.submit("b1")
        controller._mark_blocked(self.sd, "b1", "b1 blocked: waiting on planner")
        seen = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            seen["prompt"] = cmd[-1]
            return 0, claude_ok("planner-b1"), ""

        controller.deliver_terminal_report(self.sd, "b1", run_cmd=fake)
        self.assertIn("waiting on planner", seen["prompt"])
        self.assertIn("blocked", seen["prompt"])

        self.submit("f1")
        controller._fail_offline(self.sd, "f1", {"code": "ESCALATION_EXHAUSTED"})
        seen2 = {}

        def fake2(cmd, cwd=None, timeout=120, **kw):
            seen2["prompt"] = cmd[-1]
            return 0, claude_ok("planner-f1"), ""

        controller.deliver_terminal_report(self.sd, "f1", run_cmd=fake2)
        self.assertIn("failed", seen2["prompt"])
        self.assertIn("escalation_exhausted", seen2["prompt"])


class PerHarnessCallbacks(Base):
    def test_codex_report_resumes_saved_thread(self):
        self.submit("c1", harness="codex", session="thr-c1")
        seen = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            seen["cmd"] = list(cmd)
            seen["kind"] = kw.get("kind")
            seen["prompt"] = cmd[-1]
            return 0, codex_ok("thr-c1"), ""

        controller._mark_blocked(self.sd, "c1", "c1 blocked")
        res = controller.deliver_terminal_report(self.sd, "c1", run_cmd=fake)
        self.assertEqual(res["action"], "reported")
        self.assertEqual(seen["cmd"][:4], ["codex", "exec", "resume", "thr-c1"])
        self.assertEqual(seen["kind"], "codex_callback")
        self.assertIn("c1 blocked", seen["prompt"])
        self.assertIn("Summary for c1.", seen["prompt"])

    def test_opencode_report_resumes_saved_session(self):
        self.submit("o1", harness="opencode", session="ses-o1")
        seen = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            seen["cmd"] = list(cmd)
            seen["kind"] = kw.get("kind")
            seen["prompt"] = cmd[-1]
            return 0, opencode_ok("ses-o1"), ""

        controller._mark_blocked(self.sd, "o1", "o1 blocked")
        res = controller.deliver_terminal_report(self.sd, "o1", run_cmd=fake)
        self.assertEqual(res["action"], "reported")
        self.assertEqual(seen["cmd"][:2], ["opencode", "run"])
        self.assertIn("ses-o1", seen["cmd"])
        self.assertEqual(seen["kind"], "opencode_callback")
        self.assertIn("o1 blocked", seen["prompt"])

    def test_grok_report_uses_fresh_session_from_summary(self):
        self.submit("g1", harness="grok", session="ses-grok-1")
        seen = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            seen["cmd"] = list(cmd)
            seen["kind"] = kw.get("kind")
            return 0, grok_ok("ses_fresh_9"), ""

        controller._mark_blocked(self.sd, "g1", "g1 blocked")
        res = controller.deliver_terminal_report(self.sd, "g1", run_cmd=fake)
        self.assertEqual(res["action"], "reported")
        # Fresh session: no --resume of the saved planner session.
        self.assertNotIn("--resume", seen["cmd"])
        self.assertEqual(seen["kind"], "grok_callback")
        self.assertEqual(self.report_record("g1")["state"], "delivered")

    def test_mismatched_resume_is_not_delivery(self):
        self.submit("m1", harness="codex", session="thr-m1")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(1)
            return 0, codex_ok("thr-other"), ""

        controller._mark_blocked(self.sd, "m1", "m1 blocked")
        res = controller.deliver_terminal_report(self.sd, "m1", run_cmd=fake)
        self.assertEqual(res["action"], "report-failed")
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.report_record("m1")["state"], "failed")
        self.assertEqual(core.get_job(self.sd, "m1")["status"], "blocked")


class BusyPlanner(Base):
    def test_busy_retry_exhaustion_sends_nothing_and_records(self):
        self.submit("w1", session="plan-w1")
        controller._mark_blocked(self.sd, "w1", "w1 blocked")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(1)
            return 0, claude_ok("plan-w1"), ""

        with mock.patch.object(adapters, "planner_session_in_use",
                               return_value=[424242]):
            res = controller.deliver_terminal_report(
                self.sd, "w1", run_cmd=fake, sleep_fn=lambda s: None)
        self.assertEqual(res["action"], "report-exhausted")
        self.assertEqual(res["attempts"], controller.TERMINAL_REPORT_MAX_ATTEMPTS)
        self.assertEqual(calls, [])
        rec = self.report_record("w1")
        self.assertEqual(rec["state"], "failed")
        self.assertEqual(rec["attempts"], controller.TERMINAL_REPORT_MAX_ATTEMPTS)
        self.assertIn("planner_busy", rec["last_reason"])
        # Visible in status; the job itself is unchanged.
        view = core.status_view(self.sd, "w1")
        self.assertEqual(view["job"]["controller_state"]["terminal_report"]["state"],
                         "failed")
        self.assertEqual(core.get_job(self.sd, "w1")["status"], "blocked")

    def test_busy_then_idle_delivers_on_retry(self):
        self.submit("w2", session="plan-w2")
        controller._mark_blocked(self.sd, "w2", "w2 blocked")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(cmd[-1])
            return 0, claude_ok("plan-w2"), ""

        busy = [True]

        def in_use(sid, exclude_pids=()):
            return [424243] if busy[0] else []

        def sleep_fn(s):
            busy[0] = False

        with mock.patch.object(adapters, "planner_session_in_use", in_use):
            with mock.patch.object(adapters, "wait_planner_quiet",
                                   return_value=True):
                res = controller.deliver_terminal_report(
                    self.sd, "w2", run_cmd=fake, sleep_fn=sleep_fn)
        self.assertEqual(res["action"], "reported")
        self.assertEqual(res["attempts"], 2)
        self.assertEqual(len(calls), 1)
        self.assertIn("w2", calls[0])


class StepWrapperExactlyOnce(Base):
    def test_blocked_step_reports_once_through_public_step(self):
        self.submit("p1", session="plan-p1")
        controller._mark_blocked(self.sd, "p1", "p1 blocked: needs judgment")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(cmd[-1])
            return 0, claude_ok("plan-p1"), ""

        first = controller.step(self.sd, "p1", run_cmd=fake)
        self.assertEqual(first["action"], "blocked")
        self.assertEqual(len(calls), 1)
        self.assertIn("p1", calls[0])
        second = controller.step(self.sd, "p1", run_cmd=fake)
        self.assertEqual(second["action"], "blocked")
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.report_record("p1")["state"], "delivered")


class BusyMatchFix(unittest.TestCase):
    def _ps(self, lines):
        out = mock.Mock()
        out.stdout = "\n".join(lines)
        return out

    def test_joined_session_id_and_resume_tokens_match(self):
        sid = "sess-abc-123"
        lines = [
            "1001 claude --resume sess-abc-123 -p hello",
            "1002 claude --session-id=sess-abc-123",
            "1003 claude --resume=sess-abc-123 --output-format json",
            "1004 codex exec resume sess-abc-123",
            "1005 claude -p unrelated",
        ]
        with mock.patch("runner.adapters.subprocess.run",
                         return_value=self._ps(lines)):
            found = adapters.planner_session_in_use(sid)
        self.assertEqual(sorted(found), [1001, 1002, 1003])

    def test_separate_token_still_matches_and_exclusions_hold(self):
        sid = "sess-xyz"
        lines = ["2001 claude --resume sess-xyz -p hi"]
        with mock.patch("runner.adapters.subprocess.run",
                         return_value=self._ps(lines)):
            self.assertEqual(adapters.planner_session_in_use(sid), [2001])
            self.assertEqual(adapters.planner_session_in_use(sid, exclude_pids=(2001,)), [])
            self.assertEqual(adapters.planner_session_in_use("other"), [])


class MissingPlannerSession(Base):
    def test_no_session_skips_without_changing_job(self):
        self.submit("n1", session="plan-n1")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET planner_session_id='' WHERE request_id='n1'")
            con.commit()
        finally:
            con.close()
        controller._mark_blocked(self.sd, "n1", "n1 blocked")
        before = core.get_job(self.sd, "n1")

        def fake(cmd, cwd=None, timeout=120, **kw):  # pragma: no cover
            raise AssertionError("no callback without a session")

        res = controller.deliver_terminal_report(self.sd, "n1", run_cmd=fake)
        self.assertEqual(res["action"], "report-skipped")
        after = core.get_job(self.sd, "n1")
        self.assertEqual(after["status"], before["status"])
        self.assertEqual(self.report_record("n1")["state"], "skipped")


if __name__ == "__main__":
    unittest.main()
