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

    def test_grok_report_resumes_saved_session_read_only(self):
        # Since #96 the Grok planner resumes its saved session read-only,
        # so the report reaches the planner's own session.
        self.submit("g1", harness="grok", session="ses-grok-1")
        seen = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            seen["cmd"] = list(cmd)
            seen["kind"] = kw.get("kind")
            return 0, grok_ok("ses-grok-1"), ""

        controller._mark_blocked(self.sd, "g1", "g1 blocked")
        with mock.patch.object(adapters, "planner_process_in_use", return_value=[]):
            res = controller.deliver_terminal_report(self.sd, "g1", run_cmd=fake)
        self.assertEqual(res["action"], "reported")
        i = seen["cmd"].index("--resume")
        self.assertEqual(seen["cmd"][i + 1], "ses-grok-1")
        self.assertIn("--permission-mode", seen["cmd"])
        self.assertEqual(seen["kind"], "grok_callback")
        self.assertEqual(self.report_record("g1")["state"], "delivered")

    def test_grok_report_from_another_session_is_not_delivery(self):
        self.submit("g2", harness="grok", session="ses-grok-2")

        def fake(cmd, cwd=None, timeout=120, **kw):
            return 0, grok_ok("ses_other"), ""

        controller._mark_blocked(self.sd, "g2", "g2 blocked")
        with mock.patch.object(adapters, "planner_process_in_use", return_value=[]):
            res = controller.deliver_terminal_report(self.sd, "g2", run_cmd=fake)
        self.assertNotEqual(res["action"], "reported")
        self.assertNotEqual(self.report_record("g2").get("state"), "delivered")

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

        with mock.patch.object(adapters, "planner_process_in_use",
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

        def in_use(binary, session, exclude_pids=()):
            assert binary == "claude"
            assert session == "plan-w2"
            return [424243] if busy[0] else []

        def sleep_fn(s):
            busy[0] = False

        with mock.patch.object(adapters, "planner_process_in_use", in_use):
            with mock.patch.object(adapters, "wait_planner_quiet",
                                   return_value=True):
                res = controller.deliver_terminal_report(
                    self.sd, "w2", run_cmd=fake, sleep_fn=sleep_fn)
        self.assertEqual(res["action"], "reported")
        self.assertEqual(res["attempts"], 2)
        self.assertEqual(len(calls), 1)
        self.assertIn("w2", calls[0])


class PerHarnessBusy(Base):
    def test_codex_and_opencode_busy_retry_within_window(self):
        for harness, session in (("codex", "thr-busy"), ("opencode", "ses-busy")):
            with self.subTest(harness=harness):
                rid = f"busy-{harness}"
                self.submit(rid, harness=harness, session=session)
                controller._mark_blocked(self.sd, rid, f"{rid} blocked")
                calls = []

                def fake(cmd, cwd=None, timeout=120, **kw):
                    calls.append(1)
                    raise AssertionError("no send while the planner is busy")

                seen = {}

                def in_use(binary, sid, exclude_pids=()):
                    seen["binary"] = binary
                    seen["sid"] = sid
                    return [424244]

                with mock.patch.object(adapters, "planner_process_in_use", in_use):
                    res = controller.deliver_terminal_report(
                        self.sd, rid, run_cmd=fake, sleep_fn=lambda s: None)
                self.assertEqual(res["action"], "report-exhausted")
                self.assertEqual(res["attempts"],
                                 controller.TERMINAL_REPORT_MAX_ATTEMPTS)
                self.assertEqual(calls, [])
                # The busy check names the harness binary and session.
                self.assertEqual(seen["sid"], session)
                self.assertIn(seen["binary"], ("codex", "opencode"))
                rec = self.report_record(rid)
                self.assertEqual(rec["state"], "failed")
                self.assertIn("planner_busy", rec["last_reason"])
                self.assertEqual(core.get_job(self.sd, rid)["status"], "blocked")

    def test_grok_waits_while_saved_session_is_busy(self):
        # The Grok report resumes the saved session (#96), so a live Grok
        # process holding it gates the send like every other harness.
        self.submit("busy-grok", harness="grok", session="ses-grok-busy")
        controller._mark_blocked(self.sd, "busy-grok", "busy-grok blocked")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(list(cmd))
            return 0, grok_ok("ses-grok-busy"), ""

        with mock.patch.object(adapters, "planner_process_in_use",
                               return_value=[424245]):
            res = controller.deliver_terminal_report(
                self.sd, "busy-grok", run_cmd=fake,
                sleep_fn=lambda s: None)
        self.assertNotEqual(res["action"], "reported")
        self.assertEqual(calls, [])
        self.assertIn("planner_busy",
                      self.report_record("busy-grok")["last_reason"])


class CrashWindowAdoption(Base):
    """A send without a record must be adopted, never duplicated or lost.

    Each test inserts a finished terminal-report invocation row with the
    exact stable action key delivery would use, but no controller_state
    record: the crash between spawn and record. Delivery must adopt the
    row's actual output with zero new sends.
    """

    def _key_for(self, rid, harness, session, status="blocked"):
        job = core.get_job(self.sd, rid)
        reason = controller._terminal_report_reason(job)
        pr_url = controller._terminal_report_pr_url(job)
        text = controller._terminal_report_text(
            rid, status, pr_url, reason, job.get("handoff_summary"))
        kind, cmd = controller._terminal_report_invocation(
            harness, session, text, job["workspace"], job)
        event_id = controller._terminal_event_id(self.sd, rid)
        meta = controller._terminal_report_meta(text, event_id)
        return kind, cmd, meta, harnesses.harness_for(kind).action_key(
            kind, cmd, meta)

    def _insert_row(self, rid, kind, cmd, meta, key, session, out,
                    rc=0, state="completed"):
        from runner import core as _core
        root = store.ensure_state_dir(self.sd)
        inv_id = f"inv-{rid}-crash"
        stdout_path, stderr_path = _core._invocation_output_paths(
            root, rid, inv_id)
        store.secure_write_text(stdout_path, out)
        store.secure_write_text(stderr_path, "")
        con = store.connect(self.sd)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json,"
                " workspace, owner_token, stdout_path, stderr_path, started_at,"
                " state, rc, ended_at, meta_json, action_key, session_id,"
                " session_kind) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (inv_id, rid, kind, json.dumps(cmd),
                 core.get_job(self.sd, rid)["workspace"], "tok",
                 str(stdout_path), str(stderr_path), core._utcnow(),
                 state, rc, core._utcnow(), json.dumps(meta), key,
                 session, "planner_session_id"))
            con.execute("COMMIT")
        finally:
            con.close()

    def test_crash_after_send_adopts_without_resending(self):
        self.submit("z1", session="plan-z1")
        controller._mark_blocked(self.sd, "z1", "z1 blocked: needs judgment")
        kind, cmd, meta, key = self._key_for("z1", "claude", "plan-z1")
        self._insert_row("z1", kind, cmd, meta, key, "plan-z1",
                         claude_ok("plan-z1", "Got it, will merge."))
        self.assertEqual(self.report_record("z1"), {})
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):  # pragma: no cover
            calls.append(1)
            raise AssertionError("adopted output must not resend")

        res = controller.deliver_terminal_report(self.sd, "z1", run_cmd=fake)
        self.assertEqual(res["action"], "reported")
        self.assertEqual(calls, [])
        rec = self.report_record("z1")
        self.assertEqual(rec["state"], "delivered")
        self.assertEqual(rec["status"], "blocked")
        job = core.get_job(self.sd, "z1")
        self.assertEqual(job["status"], "blocked")
        # A later trigger still sends nothing.
        again = controller.deliver_terminal_report(self.sd, "z1", run_cmd=fake)
        self.assertEqual(again["action"], "already-reported")
        self.assertEqual(calls, [])

    def test_crash_after_failed_send_records_failure_without_resending(self):
        self.submit("z2", session="plan-z2")
        controller._mark_blocked(self.sd, "z2", "z2 blocked")
        kind, cmd, meta, key = self._key_for("z2", "claude", "plan-z2")
        self._insert_row("z2", kind, cmd, meta, key, "plan-z2",
                         "planner exploded", rc=1, state="failed")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):  # pragma: no cover
            calls.append(1)
            raise AssertionError("adopted output must not resend")

        res = controller.deliver_terminal_report(self.sd, "z2", run_cmd=fake)
        self.assertEqual(res["action"], "report-failed")
        self.assertEqual(calls, [])
        self.assertEqual(self.report_record("z2")["state"], "failed")

    def test_live_send_defers_without_duplicating(self):
        import os as _os
        self.submit("z3", session="plan-z3")
        controller._mark_blocked(self.sd, "z3", "z3 blocked")
        kind, cmd, meta, key = self._key_for("z3", "claude", "plan-z3")
        # A running row owned by a live supervisor: the send is in flight
        # elsewhere, so delivery defers instead of duplicating it.
        root = store.ensure_state_dir(self.sd)
        stdout_path, stderr_path = core._invocation_output_paths(
            root, "z3", "inv-z3-live")
        store.secure_write_text(stdout_path, "")
        store.secure_write_text(stderr_path, "")
        con = store.connect(self.sd)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json,"
                " workspace, owner_token, stdout_path, stderr_path, started_at,"
                " state, supervisor_pid, meta_json, action_key) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("inv-z3-live", "z3", kind, json.dumps(cmd),
                 core.get_job(self.sd, "z3")["workspace"], "tok",
                 str(stdout_path), str(stderr_path), core._utcnow(),
                 "running", _os.getpid(), json.dumps(meta), key))
            con.execute("COMMIT")
        finally:
            con.close()
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):  # pragma: no cover
            calls.append(1)
            raise AssertionError("a live send must not duplicate")

        res = controller.deliver_terminal_report(self.sd, "z3", run_cmd=fake,
                                                 sleep_fn=lambda s: None)
        self.assertEqual(res["action"], "report-deferred")
        self.assertEqual(calls, [])
        rec = self.report_record("z3")
        self.assertEqual(rec["state"], "pending")
        self.assertEqual(core.get_job(self.sd, "z3")["status"], "blocked")

    def test_question_row_with_same_kind_is_never_adopted(self):
        # A planner-question callback row must not count as a report even
        # when its action key collides: adoption requires the
        # terminal_report reason in the row meta.
        self.submit("z4", session="plan-z4")
        controller._mark_blocked(self.sd, "z4", "z4 blocked")
        kind, _cmd, _meta, _key = self._key_for("z4", "claude", "plan-z4")
        qmeta = {"stage": "planning", "reason": "planner_question",
                 "qid": "q1", "prompt_sha256": "abc"}
        qkey = harnesses.harness_for(kind).action_key(
            kind, ["claude", "--resume", "plan-z4"], qmeta)
        self._insert_row("z4", kind, ["claude", "--resume", "plan-z4"],
                         qmeta, qkey, "plan-z4", claude_ok("plan-z4", "Yes."))
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(1)
            return 0, claude_ok("plan-z4"), ""

        res = controller.deliver_terminal_report(self.sd, "z4", run_cmd=fake)
        self.assertEqual(res["action"], "reported")
        # The report went out as its own send, not as the question's echo.
        self.assertEqual(len(calls), 1)


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

    def test_other_harness_binaries_match_their_resume_forms(self):
        lines = [
            "3001 codex exec resume thr-abc-1 --json",
            "3002 opencode run --session ses-op-1 --dir /tmp/ws --format json",
            "3003 opencode run --session=ses-op-2 --dir /tmp/ws --format json",
            "3004 grok -p hello --cwd /tmp/ws",
        ]
        with mock.patch("runner.adapters.subprocess.run",
                         return_value=self._ps(lines)):
            self.assertEqual(
                adapters.planner_process_in_use("codex", "thr-abc-1"), [3001])
            self.assertEqual(
                adapters.planner_process_in_use("opencode", "ses-op-1"), [3002])
            self.assertEqual(
                adapters.planner_process_in_use("opencode", "ses-op-2"), [3003])
            # Binary-scoped: a codex thread is not a claude session.
            self.assertEqual(
                adapters.planner_process_in_use("claude", "thr-abc-1"), [])
            self.assertEqual(
                adapters.planner_process_in_use("codex", "ses-op-1"), [])


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


class PerHarnessBusyIdle(Base):
    def test_codex_and_opencode_busy_then_idle_deliver_on_retry(self):
        for harness, session in (("codex", "thr-idle"), ("opencode", "ses-idle")):
            with self.subTest(harness=harness):
                rid = f"idle-{harness}"
                self.submit(rid, harness=harness, session=session)
                controller._mark_blocked(self.sd, rid, f"{rid} blocked")
                calls = []

                def fake(cmd, cwd=None, timeout=120, **kw):
                    calls.append(cmd[-1])
                    if harness == "codex":
                        return 0, codex_ok(session), ""
                    return 0, opencode_ok(session), ""

                busy = [True]

                def in_use(binary, sid, exclude_pids=()):
                    return [424246] if busy[0] else []

                def sleep_fn(s):
                    busy[0] = False

                with mock.patch.object(adapters, "planner_process_in_use", in_use):
                    res = controller.deliver_terminal_report(
                        self.sd, rid, run_cmd=fake, sleep_fn=sleep_fn)
                self.assertEqual(res["action"], "reported")
                self.assertEqual(res["attempts"], 2)
                self.assertEqual(len(calls), 1)
                self.assertIn(rid, calls[0])
                self.assertEqual(self.report_record(rid)["state"], "delivered")


class RecordWriteFailure(Base):
    def test_send_success_with_record_failure_is_not_delivery(self):
        self.submit("rf1", session="plan-rf1")
        controller._mark_blocked(self.sd, "rf1", "rf1 blocked")
        before = core.get_job(self.sd, "rf1")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(1)
            return 0, claude_ok("plan-rf1"), ""

        with mock.patch.object(controller, "_save_terminal_report_record",
                               side_effect=OSError("ledger unwritable")):
            res = controller.deliver_terminal_report(self.sd, "rf1", run_cmd=fake)
        self.assertEqual(len(calls), 1)
        # Never represented as delivered when the outcome was not recorded.
        self.assertIn(res["action"], ("report-failed", "report-error"))
        self.assertIn("record_failed", res.get("reason", ""))
        self.assertNotEqual(self.report_record("rf1").get("state"), "delivered")
        after = core.get_job(self.sd, "rf1")
        self.assertEqual(after["status"], before["status"])
        self.assertEqual(after["result_json"], before["result_json"])
        # The failure stays visible in status output_tail.
        view = core.status_view(self.sd, "rf1")
        self.assertIn("record_failed", view.get("output_tail", ""))


class DistinctBlockedEvents(Base):
    def test_same_reason_second_blocked_reports_again_with_new_key(self):
        self.submit("e1", session="plan-e1")
        controller._mark_blocked(self.sd, "e1", "e1 blocked: same reason")
        first_id = controller._terminal_event_id(self.sd, "e1")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(list(cmd))
            return 0, claude_ok("plan-e1"), ""

        first = controller.deliver_terminal_report(self.sd, "e1", run_cmd=fake)
        self.assertEqual(first["action"], "reported")
        self.assertEqual(len(calls), 1)
        first_rec = self.report_record("e1")
        self.assertEqual(first_rec["state"], "delivered")
        # Retry of the same event sends nothing.
        again = controller.deliver_terminal_report(self.sd, "e1", run_cmd=fake)
        self.assertEqual(again["action"], "already-reported")
        self.assertEqual(len(calls), 1)
        # Simulate recover to running: the next deliver clears the old
        # blocked record without sending.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running', block_reason=NULL WHERE request_id='e1'")
            con.commit()
        finally:
            con.close()
        cleared = controller.deliver_terminal_report(self.sd, "e1", run_cmd=fake)
        self.assertEqual(cleared["action"], "noop-not-terminal")
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.report_record("e1"), {})
        # Second blocked with the identical reason is a new event: new
        # ledger id, new action key, and a second send.
        controller._mark_blocked(self.sd, "e1", "e1 blocked: same reason")
        second_id = controller._terminal_event_id(self.sd, "e1")
        self.assertNotEqual(second_id, first_id)
        second = controller.deliver_terminal_report(self.sd, "e1", run_cmd=fake)
        self.assertEqual(second["action"], "reported")
        self.assertEqual(len(calls), 2)
        second_rec = self.report_record("e1")
        self.assertEqual(second_rec["state"], "delivered")
        self.assertNotEqual(second_rec.get("delivered_for"), first_rec.get("delivered_for"))
        self.assertEqual(core.get_job(self.sd, "e1")["status"], "blocked")


class CliAndRecovery(Base):
    def test_cancel_then_recover_reports_once(self):
        self.submit("cr1", session="plan-cr1")
        core.cancel(self.sd, "cr1")
        job = core.get_job(self.sd, "cr1")
        self.assertIn(job["status"], ("cancelled", "blocked"))
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(1)
            return 0, claude_ok("plan-cr1"), ""

        res = controller.deliver_terminal_report(self.sd, "cr1", run_cmd=fake)
        # Cancelled reports; cancellation_pending blocked reports too.
        self.assertIn(res["action"], ("reported", "report-failed", "report-deferred"))
        if res["action"] == "reported":
            self.assertEqual(len(calls), 1)
            rec = self.report_record("cr1")
            self.assertEqual(rec["state"], "delivered")
            # Recovery and restarted controllers send no duplicate.
            out = core.recover_one(self.sd, "cr1")
            self.assertIn(out.get("status"), ("cancelled", "blocked"))
            again = controller.deliver_terminal_report(self.sd, "cr1", run_cmd=fake)
            self.assertEqual(again["action"], "already-reported")
            self.assertEqual(len(calls), 1)

    def test_step_reports_failed_without_changing_result(self):
        self.submit("cr2", session="plan-cr2")
        controller._fail_offline(self.sd, "cr2", {"code": "ESCALATION_EXHAUSTED"})
        before = core.get_job(self.sd, "cr2")
        calls = []

        def fake(cmd, cwd=None, timeout=120, **kw):
            calls.append(cmd[-1])
            return 0, claude_ok("plan-cr2"), ""

        res = controller.step(self.sd, "cr2", run_cmd=fake)
        self.assertEqual(res["action"], "noop-terminal")
        self.assertEqual(len(calls), 1)
        after = core.get_job(self.sd, "cr2")
        self.assertEqual(after["status"], before["status"])
        self.assertEqual(after["result_json"], before["result_json"])
        self.assertEqual(self.report_record("cr2")["state"], "delivered")


if __name__ == "__main__":
    unittest.main()
