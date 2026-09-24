"""Issue #38: durable planner handoff summary and historical compact rows.

Stdlib only, no live CLIs. The runner no longer compacts the planner
session after submit (removed 2026-09-24): submit persists the job and
launches the controller with no planner mutation, callbacks resume the
exact saved planner session, and every callback prompt carries the stored
handoff summary. Old ledgers may still contain ``claude_compact``
invocation rows; they read through the neutral historical path in
``harnesses.harness_for`` so status, result, and recover never fail.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters, controller, core, harnesses, store  # noqa: E402
from tests.fakes import FAKE_CLAUDE, write_fake  # noqa: E402

PY = sys.executable


def claude_callback_out(session, answer="Approved."):
    return json.dumps({"type": "result", "subtype": "success", "is_error": False,
                       "result": answer, "session_id": session,
                       "uuid": "fake-result-uuid", "duration_ms": 12,
                       "num_turns": 1, "total_cost_usd": 0.0,
                       "usage": {"input_tokens": 10, "output_tokens": 3,
                                 "cache_read_input_tokens": 120,
                                 "cache_creation_input_tokens": 7}})


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


class SubmitSummary(Base):
    def test_submit_stores_explicit_summary(self):
        job = core.submit(self.sd, "h1", {"goal": "t"}, self.ws("a"),
                          "planner-1", handoff_summary="Keep the API shape.")
        self.assertEqual(job["handoff_summary"], "Keep the API shape.")
        self.assertEqual(core.get_job(self.sd, "h1")["handoff_summary"],
                         "Keep the API shape.")

    def test_submit_derives_summary_from_packet(self):
        task = {"goal": "fix typo", "issue": "38",
                "decisions": "use policy data",
                "proof": "python3 -B -m unittest discover -s tests"}
        job = core.submit(self.sd, "h2", task, self.ws("b"), "planner-2")
        summary = job["handoff_summary"]
        for needle in ("h2", "38", "use policy data", "unittest"):
            self.assertIn(needle, summary)

    def test_task_packet_handoff_summary_wins(self):
        task = {"goal": "t", "handoff_summary": "Packet summary."}
        job = core.submit(self.sd, "h3", task, self.ws("c"), "planner-3")
        self.assertEqual(job["handoff_summary"], "Packet summary.")

    def test_callback_prompt_carries_summary_before_question(self):
        core.submit(self.sd, "h4", {"goal": "t"}, self.ws("d"),
                    "planner-4", handoff_summary="Decisions: ship it.")
        captured = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            captured["cmd"] = list(cmd)
            captured["meta"] = dict(kw.get("meta") or {})
            # The prompt is the last argv element (-p PROMPT).
            captured["prompt"] = cmd[-1]
            return 0, claude_callback_out("planner-4", "Yes."), ""

        res = controller.planner_callback(self.sd, "h4", "q1",
                                          "Ship now?", run_cmd=fake)
        self.assertEqual(res["action"], "answered")
        prompt = captured["prompt"]
        self.assertIn("HANDOFF SUMMARY", prompt)
        self.assertIn("Decisions: ship it.", prompt)
        self.assertIn("Ship now?", prompt)
        self.assertLess(prompt.index("Decisions: ship it."),
                        prompt.index("Ship now?"))
        # The stored question keeps the dispatcher's prompt; the summary
        # travels only in the callback prompt.
        qs = core.list_questions(self.sd, "h4", only_pending=False)
        self.assertEqual(qs[0]["prompt"], "Ship now?")

    def test_submit_and_start_creates_no_planner_mutation(self):
        # Submit with --start persists the job and launches the controller
        # without touching the planner session: no historical-kind row and
        # no planner-mutating command anywhere.
        calls = []

        def fake_spawn(cmd):
            calls.append(list(cmd))
            return 424242

        task = {"goal": "fix typo", "issue": "38",
                "decisions": "use policy data",
                "proof": "python3 -B -m unittest discover -s tests"}
        core.submit_and_start(self.sd, "c1", task, self.ws("a"),
                              "planner-c1", spawn=fake_spawn)
        invs = [i for i in core._list_invocations(self.sd, "c1")
                if i["kind"] == "claude_compact"]
        self.assertEqual(invs, [])
        for cmd in calls:
            self.assertFalse(any("compact" in str(a).lower() for a in cmd),
                             cmd)


class CallbackMeasurement(Base):
    def test_callback_measures_resumed_context(self):
        core.submit(self.sd, "m1", {"goal": "t"}, self.ws("a"),
                    "planner-m1", handoff_summary="Summary.")
        out = claude_callback_out("planner-m1", "Yes.")

        def fake(cmd, cwd=None, timeout=120, **kw):
            return 0, out, ""

        res = controller.planner_callback(self.sd, "m1", "q1", "Go?",
                                          run_cmd=fake)
        self.assertEqual(res["action"], "answered")
        invs = [i for i in core._list_invocations(self.sd, "m1")
                if i["kind"] == "claude_callback"]
        # planner_callback runs via the injected run_cmd, not the durable
        # path, so no invocation row exists; measure the output directly
        # through the harness seam as the supervisor does.
        self.assertEqual(invs, [])
        usage, _obs, _var, ids = core.measure_output(
            "claude_callback", out, "",
            {"prompt_sha256": "abc"})
        self.assertEqual(usage["source"], "claude")
        for key in ("input_tokens", "cache_read_input_tokens",
                    "cache_creation_input_tokens"):
            self.assertIn(key, usage)
        self.assertEqual(ids["session_id"], "planner-m1")

    def test_durable_callback_usage_visible_in_status(self):
        # A durable claude_callback invocation carries the resumed context
        # verbatim into status/result measurements.
        core.submit(self.sd, "m2", {"goal": "t"}, self.ws("b"),
                    "planner-m2", handoff_summary="Summary.")
        out = claude_callback_out("planner-m2", "Yes.")
        con = store.connect(self.sd)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json,"
                " workspace, owner_token, stdout_path, stderr_path, started_at,"
                " state, rc, ended_at, consumed_at, result_json, session_id,"
                " session_kind, usage_json, elapsed_secs, terminal_class,"
                " schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("inv-m2-1", "m2", "claude_callback", "[]", self.ws("b"),
                 "t", str(self.base / "o"), str(self.base / "e"),
                 "2026-09-20T00:00:00+00:00", "completed", 0,
                 "2026-09-20T00:00:01+00:00", "2026-09-20T00:00:01+00:00",
                 json.dumps({"rc": 0}), "planner-m2", "planner_session_id",
                 json.dumps({"source": "claude", "input_tokens": 10,
                             "cache_read_input_tokens": 120,
                             "cache_creation_input_tokens": 7}),
                 1.0, "completed", store.SCHEMA_VERSION))
            con.execute("COMMIT")
        finally:
            con.close()
        view = core.status_view(self.sd, "m2")
        found = [m for m in view["job"]["measurements"]
                 if m["kind"] == "claude_callback"]
        self.assertEqual(len(found), 1)
        for key in ("input_tokens", "cache_read_input_tokens",
                    "cache_creation_input_tokens"):
            self.assertIn(key, found[0]["usage"])


class CodexPlannerCallback(Base):
    def test_codex_planner_resume_answers_without_human_action(self):
        core.submit(self.sd, "c1", {"goal": "t"}, self.ws("a"),
                    "thr-codex-1", planner_harness="codex",
                    handoff_summary="Ship the fix.")
        captured = {}

        def fake(cmd, cwd=None, timeout=120, **kw):
            captured["cmd"] = list(cmd)
            captured["cwd"] = cwd
            captured["kind"] = kw.get("kind")
            # The question is already persisted before the planner wakes.
            pending = core.list_questions(self.sd, "c1", only_pending=True)
            assert len(pending) == 1 and pending[0]["prompt"] == "Ship now?"
            out = "\n".join(json.dumps(o) for o in [
                {"type": "thread.started", "thread_id": "thr-codex-1"},
                {"type": "item.completed",
                 "item": {"type": "agent_message", "text": "Ship it."}},
                {"type": "turn.completed", "usage": {}},
            ])
            return 0, out, ""

        res = controller.planner_callback(self.sd, "c1", "q1",
                                          "Ship now?", run_cmd=fake)
        self.assertEqual(res["action"], "answered")
        cmd = captured["cmd"]
        # The saved Codex thread resumes, never forks.
        self.assertEqual(cmd[:4], ["codex", "exec", "resume", "thr-codex-1"])
        self.assertIn("--json", cmd)
        self.assertEqual(captured["kind"], "codex_callback")
        self.assertEqual(captured["cwd"],
                         core.get_job(self.sd, "c1")["workspace"])
        prompt = cmd[-1]
        self.assertIn("HANDOFF SUMMARY", prompt)
        self.assertIn("Ship the fix.", prompt)
        self.assertIn("Ship now?", prompt)
        self.assertLess(prompt.index("Ship the fix."),
                        prompt.index("Ship now?"))
        qs = core.list_questions(self.sd, "c1", only_pending=False)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0]["status"], "answered")
        self.assertEqual(qs[0]["answer"], "Ship it.")


class PolicyData(Base):
    def test_astra_fallback_uses_summary_no_session(self):
        core.submit(self.sd, "a1", {"goal": "t"}, self.ws("a"),
                    "planner-a1", handoff_summary="Ship the fix.")
        prompt = controller.astra_fallback_prompt(self.sd, "a1", "q1",
                                                  "Ship now?")
        self.assertIn("Ship the fix.", prompt)
        self.assertIn("Ship now?", prompt)
        self.assertLess(prompt.index("Ship the fix."),
                        prompt.index("Ship now?"))
        # No session is resumed: the prompt carries no session id.
        self.assertNotIn("planner-a1", prompt)


class DurableCallbackResumedContext(Base):
    def test_live_durable_callback_row_carries_three_token_keys(self):
        # Live fake Claude callback shape through the durable measurement
        # path (the same _measure_invocation the detached controller uses).
        bindir = self.base / "fakebin-live"
        bindir.mkdir()
        fake_state = self.base / "fakestate-live"
        fake_state.mkdir()
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        core.submit(self.sd, "m-live", {"goal": "t"}, self.ws("live"),
                    "planner-live", handoff_summary="Summary.")
        cmd = adapters.build_claude_cmd("planner-live", "HANDOFF SUMMARY?\nGo?")
        self.assertEqual(harnesses.kind_for_cmd(cmd), "claude_callback")
        env = dict(os.environ, FAKE_STATE=str(fake_state),
                   PATH=str(bindir) + os.pathsep + os.environ.get("PATH", ""))
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=10, cwd=self.ws("live"), env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr[-1000:])
        out = proc.stdout
        root = store.ensure_state_dir(self.sd)
        inv_id = "inv-live-cb-0001"
        stdout_path, stderr_path = core._invocation_output_paths(
            root, "m-live", inv_id)
        store.secure_write_text(stdout_path, out)
        store.secure_write_text(stderr_path, "")
        con = store.connect(self.sd)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,"
                "workspace,owner_token,stdout_path,stderr_path,started_at,"
                "state,rc,meta_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (inv_id, "m-live", "claude_callback", json.dumps(cmd),
                 self.ws("live"), "tok", str(stdout_path), str(stderr_path),
                 core._utcnow(), "completed", 0,
                 json.dumps({"prompt_sha256": "abc"})))
            con.execute("COMMIT")
        finally:
            con.close()
        core._measure_invocation(self.sd, "m-live", inv_id)
        inv = [i for i in core._list_invocations(self.sd, "m-live")
               if i["invocation_id"] == inv_id][0]
        usage = json.loads(inv["usage_json"] or "null")
        self.assertEqual(usage["source"], "claude")
        for key in ("input_tokens", "cache_read_input_tokens",
                    "cache_creation_input_tokens"):
            self.assertIn(key, usage)
        view = core.status_view(self.sd, "m-live")
        found = [m for m in view["job"]["measurements"]
                 if m["kind"] == "claude_callback"]
        self.assertEqual(len(found), 1)
        for key in ("input_tokens", "cache_read_input_tokens",
                    "cache_creation_input_tokens"):
            self.assertIn(key, found[0]["usage"])


class HandoffMinors(Base):
    def test_null_summary_backfilled_on_resubmit(self):
        task = {"goal": "t", "issue": "38", "decisions": "d",
                "proof": "python3 -B -m unittest discover -s tests"}
        job = core.submit(self.sd, "null1", task, self.ws("n"),
                          "planner-n1", handoff_summary="Keep it.")
        self.assertTrue((job["handoff_summary"] or "").strip())
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET handoff_summary=NULL WHERE request_id='null1'")
            con.commit()
        finally:
            con.close()
        job2 = core.submit(self.sd, "null1", task, self.ws("n"),
                           "planner-n1", handoff_summary="Keep it.")
        self.assertTrue((job2["handoff_summary"] or "").strip())
        self.assertIn("Keep it.", job2["handoff_summary"])

    def test_callback_prompt_with_compact_word_stays_callback(self):
        # A dispatcher question that mentions compact is still an ordinary
        # callback: the runner has no compact command to infer.
        prompt = "Should we mention compact in the question? Yes."
        cmd = adapters.build_claude_cmd("sid-x", prompt)
        self.assertEqual(harnesses.kind_for_cmd(cmd), "claude_callback")


class HistoricalCompactLedger(Base):
    """Pre-2.7.0 ledgers may carry a claude_compact row.

    The kind is historical, never active: it is absent from the harness
    registry and no new command infers it, but status, result, and
    recover must read an old row without raising or producing work.
    """

    def _insert_historical_compact_row(self, rid="old1", sid="planner-old",
                                       consumed=True):
        core.submit(self.sd, rid, {"goal": "t"}, self.ws("old"), sid,
                    handoff_summary="Summary.")
        root = store.ensure_state_dir(self.sd)
        inv_id = f"inv-{rid}-compact-0001"
        stdout_path, stderr_path = core._invocation_output_paths(
            root, rid, inv_id)
        store.secure_write_text(stdout_path, "local_command: compact\n")
        store.secure_write_text(stderr_path, "")
        con = store.connect(self.sd)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json,"
                " workspace, owner_token, stdout_path, stderr_path, started_at,"
                " state, rc, ended_at, consumed_at, result_json, session_id,"
                " session_kind, elapsed_secs, terminal_class,"
                " schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (inv_id, rid, "claude_compact", json.dumps(["claude"]),
                 self.ws("old"), "submit", str(stdout_path), str(stderr_path),
                 "2026-09-20T00:00:00+00:00", "completed", 0,
                 "2026-09-20T00:00:01+00:00",
                 "2026-09-20T00:00:01+00:00" if consumed else None,
                 json.dumps({"rc": 0, "ok": True}), sid, "planner_session_id",
                 1.0, "completed", store.SCHEMA_VERSION))
            con.execute("COMMIT")
        finally:
            con.close()
        return inv_id

    def test_historical_kind_not_active(self):
        self.assertNotIn("claude_compact", harnesses.KIND_TO_HARNESS)
        self.assertNotIn("claude_compact", harnesses.INVOCATION_KINDS)
        self.assertIn("claude_compact", harnesses.HISTORICAL_INVOCATION_KINDS)
        # No new command infers the historical kind: even a prompt that
        # mentions compact is an ordinary callback.
        cmd = adapters.build_claude_cmd("sid-x", "mention compact here")
        self.assertEqual(harnesses.kind_for_cmd(cmd), "claude_callback")

    def test_old_row_reads_in_status_and_result(self):
        self._insert_historical_compact_row()
        view = core.status_view(self.sd, "old1")
        found = [m for m in view["job"]["measurements"]
                 if m["kind"] == "claude_compact"]
        self.assertEqual(len(found), 1)
        result = core.result_view(self.sd, "old1")
        found = [m for m in result["measurements"]
                 if m["kind"] == "claude_compact"]
        self.assertEqual(len(found), 1)

    def test_old_row_survives_recover(self):
        # Nonterminal recovery: the job stays pending/running and the
        # historical row is completed but unconsumed, so recover_one must
        # reconcile it instead of taking the terminal early exit.
        inv_id = self._insert_historical_compact_row(rid="old2", consumed=False)
        job = core.get_job(self.sd, "old2")
        self.assertNotIn(job["status"], store.TERMINAL)
        before = core._list_invocations(self.sd, "old2")
        self.assertEqual(len(before), 1)
        self.assertIsNone(before[0].get("consumed_at"))
        rec = core.recover_one(self.sd, "old2")
        after = core._list_invocations(self.sd, "old2")
        # Recovery creates no new work for the historical row: same single
        # row, same kind, no additional invocation.
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0]["invocation_id"], inv_id)
        self.assertEqual(after[0].get("kind"), "claude_compact")
        self.assertEqual(rec["request_id"], "old2")
        # The original row stays readable in both views.
        view = core.status_view(self.sd, "old2")
        found = [m for m in view["job"]["measurements"]
                 if m["kind"] == "claude_compact"]
        self.assertEqual(len(found), 1)
        result = core.result_view(self.sd, "old2")
        found = [m for m in result["measurements"]
                 if m["kind"] == "claude_compact"]
        self.assertEqual(len(found), 1)

    def test_historical_kind_never_created(self):
        # Behavioral proof through the real durable-run seam: every
        # historical-only kind is rejected before any side effect.
        self.assertIn("claude_compact", harnesses.HISTORICAL_INVOCATION_KINDS)
        for hist_kind in harnesses.HISTORICAL_INVOCATION_KINDS:
            with self.subTest(kind=hist_kind):
                rid = "hist-" + "".join(
                    c if c.isalnum() else "-" for c in hist_kind)
                ws = self.ws("ws-" + rid)
                core.submit(self.sd, rid, {"goal": "t"}, ws,
                            "planner-" + rid, handoff_summary="Summary.")
                con = store.connect(self.sd)
                try:
                    con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id=?",
                                (rid,))
                    con.commit()
                finally:
                    con.close()
                root = store.ensure_state_dir(self.sd)
                before_files = {p.relative_to(root).as_posix()
                                for p in root.rglob("*") if p.is_file()}

                def _snapshot():
                    return (list(core._list_invocations(self.sd, rid)),
                            {p.relative_to(root).as_posix()
                             for p in root.rglob("*") if p.is_file()})

                # Direct durable-run seam with an explicit historical kind.
                with self.assertRaises(core.RunnerError) as ctx:
                    core._durable_run(self.sd, rid, "tok", hist_kind,
                                      ["claude", "--resume", "x"],
                                      cwd=ws, meta={"reason": "test"})
                self.assertIn("historical", str(ctx.exception))
                invs, files = _snapshot()
                self.assertEqual(invs, [])
                self.assertEqual(files, before_files)
                # The factory seam resolves to the same rejection.
                run = core.make_durable_run_cmd(self.sd, rid, "tok")
                with self.assertRaises(core.RunnerError) as ctx2:
                    run(["claude", "--resume", "x"], ws, 5,
                        kind=hist_kind, meta={"reason": "test"})
                self.assertIn("historical", str(ctx2.exception))
                invs, files = _snapshot()
                self.assertEqual(invs, [])
                self.assertEqual(files, before_files)


if __name__ == "__main__":
    unittest.main()
