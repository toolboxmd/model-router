"""Issue #38: compact the planner session at handoff, durable handoff summary.

Stdlib only, no live CLIs. The fake Claude CLI (tests/fakes.py) emulates
the verified headless shape `claude -p --resume <sid> "/compact <focus>"`
returning local_command compact.
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

from runner import adapters, controller, core, harnesses, policy, store  # noqa: E402
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


def claude_compact_out(session):
    return json.dumps({"type": "result", "subtype": "success", "is_error": False,
                       "result": "local_command: compact", "session_id": session,
                       "uuid": "fake-compact-uuid", "duration_ms": 8,
                       "num_turns": 1, "total_cost_usd": 0.0,
                       "usage": {"input_tokens": 50, "output_tokens": 5,
                                 "cache_read_input_tokens": 400,
                                 "cache_creation_input_tokens": 20}})


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


class CompactAfterSubmit(Base):
    def fake_spawn(self, pid=424242):
        def _spawn(cmd):
            return pid
        return _spawn

    def test_compact_invoked_once_per_submit(self):
        calls = []

        def fake_compact(cmd, cwd=None, timeout=120, **kw):
            calls.append((list(cmd), dict(kw.get("meta") or {})))
            sid = cmd[cmd.index("--resume") + 1]
            self.assertEqual(cmd[:4], ["claude", "-p", "--resume", sid])
            self.assertEqual(len(cmd), 5)
            return 0, claude_compact_out(sid), ""

        task = {"goal": "fix typo", "issue": "38",
                "decisions": "use policy data",
                "proof": "python3 -B -m unittest discover -s tests"}
        core.submit_and_start(self.sd, "c1", task, self.ws("a"),
                              "planner-c1", spawn=self.fake_spawn(),
                              run_cmd=fake_compact)
        self.assertEqual(len(calls), 1)
        job = core.get_job(self.sd, "c1")
        expected = "/compact " + core.compact_focus_for_job(job)
        self.assertEqual(calls[0][0][-1], expected)
        self.assertEqual(calls[0][0][-1].count("/compact"), 1)
        focus = calls[0][0][-1]
        for needle in ("c1", "38", "use policy data", "unittest"):
            self.assertIn(needle, focus)
        invs = [i for i in core._list_invocations(self.sd, "c1")
                if i["kind"] == "claude_compact"]
        self.assertEqual(len(invs), 1)
        inv = invs[0]
        self.assertEqual(inv["state"], "completed")
        self.assertIsNotNone(inv["elapsed_secs"])
        self.assertIsNotNone(inv["usage_json"])
        usage = json.loads(inv["usage_json"])
        self.assertEqual(usage["source"], "claude")
        # Identical resubmission returns the stored job with no second compact.
        core.submit_and_start(self.sd, "c1", task, self.ws("a"),
                              "planner-c1", spawn=self.fake_spawn(),
                              run_cmd=fake_compact)
        self.assertEqual(len(calls), 1)

    def test_compact_failure_recorded_never_blocks(self):
        def fake_fail(cmd, cwd=None, timeout=120, **kw):
            sid = cmd[cmd.index("--resume") + 1]
            _ = sid
            return 1, "", "compact failed"

        core.submit_and_start(self.sd, "c2", {"goal": "t"}, self.ws("b"),
                              "planner-c2", spawn=self.fake_spawn(),
                              run_cmd=fake_fail)
        job = core.get_job(self.sd, "c2")
        self.assertNotIn(job["status"], ("blocked", "failed", "cancelled"))
        invs = [i for i in core._list_invocations(self.sd, "c2")
                if i["kind"] == "claude_compact"]
        self.assertEqual(len(invs), 1)
        self.assertEqual(invs[0]["state"], "failed")
        self.assertEqual(invs[0]["terminal_class"], "failed")

    def test_no_compaction_when_flag_off(self):
        calls = []

        def fake_compact(cmd, cwd=None, timeout=120, **kw):
            calls.append(list(cmd))
            sid = cmd[cmd.index("--resume") + 1]
            return 0, claude_compact_out(sid), ""

        prev = dict(policy.COMPACT_ON_SUBMIT)
        policy.COMPACT_ON_SUBMIT["claude"] = False
        try:
            core.submit_and_start(self.sd, "c3", {"goal": "t"},
                                  self.ws("c"), "planner-c3",
                                  spawn=self.fake_spawn(),
                                  run_cmd=fake_compact)
        finally:
            policy.COMPACT_ON_SUBMIT.clear()
            policy.COMPACT_ON_SUBMIT.update(prev)
        self.assertEqual(calls, [])
        invs = [i for i in core._list_invocations(self.sd, "c3")
                if i["kind"] == "claude_compact"]
        self.assertEqual(invs, [])

    def test_fake_cli_compact_shape(self):
        bindir = self.base / "fakebin"
        bindir.mkdir()
        fake_state = self.base / "fakestate"
        fake_state.mkdir()
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        focus = policy.compact_focus("c9", issue="38", decisions="d",
                                     proof="p", summary="s")
        cmd = adapters.build_claude_compact_cmd("planner-c9", focus)
        self.assertEqual(harnesses.kind_for_cmd(cmd), "claude_compact")
        env = dict(os.environ, FAKE_STATE=str(fake_state),
                   PATH=str(bindir) + os.pathsep + os.environ.get("PATH", ""))
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=10, cwd=self.ws("d"), env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr[-1000:])
        parsed = adapters.parse_claude_compact_result(proc.stdout)
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["session_id"], "planner-c9")
        log_lines = (fake_state / "claude.log").read_text().strip().splitlines()
        self.assertEqual(len(log_lines), 1)
        logged_argv = json.loads(log_lines[0])["argv"]
        self.assertEqual(logged_argv, cmd[1:])
        self.assertEqual(logged_argv[-1], "/compact " + focus)
        self.assertEqual(logged_argv[-1].count("/compact"), 1)


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
        # The same usage lands on the ledger for a durable callback turn:
        # record one compact-style row and check the Observer mapping.
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


class PolicyData(Base):
    def test_compact_template_and_flag(self):
        self.assertIn("{request_id}", policy.COMPACT_FOCUS_TEMPLATE)
        for token in ("{issue}", "{decisions}", "{proof}", "{summary}"):
            self.assertIn(token, policy.COMPACT_FOCUS_TEMPLATE)
        self.assertTrue(policy.compact_enabled("claude"))
        self.assertFalse(policy.compact_enabled("codex"))
        self.assertFalse(policy.compact_enabled("unknown-harness"))
        focus = policy.compact_focus("r1", issue="38", decisions="d",
                                     proof="prove it", summary="s")
        for needle in ("r1", "38", "d", "prove it", "s"):
            self.assertIn(needle, focus)
        self.assertEqual(policy.validate_policy(), [])

    def test_astra_fallback_uses_summary_no_session(self):
        core.submit(self.sd, "a1", {"goal": "t"}, self.ws("a"),
                    "planner-a1", handoff_summary="Ship the fix.")
        prompt = controller.astra_fallback_prompt(self.sd, "a1", "q1",
                                                  "Ship now?")
        self.assertIn("Ship the fix.", prompt)
        self.assertIn("Ship now?", prompt)
        self.assertLess(prompt.index("Ship the fix."),
                        prompt.index("Ship now?"))
        # No session is resumed: the prompt carries no session id and the
        # Codex harness never compacts.
        self.assertNotIn("planner-a1", prompt)
        self.assertFalse(policy.compact_enabled("codex"))

    def test_compact_cmd_and_parse(self):
        cmd = adapters.build_claude_compact_cmd("sid-1", "focus words")
        self.assertEqual(cmd, ["claude", "-p", "--resume", "sid-1",
                               "/compact focus words"])
        self.assertEqual(cmd[-1].count("/compact"), 1)
        # The builder owns the single prefix: a caller-carried copy is
        # dropped, never doubled.
        self.assertEqual(
            adapters.build_claude_compact_cmd("sid-1", "/compact focus words"),
            cmd)
        self.assertEqual(harnesses.kind_for_cmd(cmd), "claude_compact")
        self.assertEqual(harnesses.kind_for_cmd(["claude", "--resume", "x"]),
                         "claude_callback")
        ok = adapters.parse_claude_compact_result(claude_compact_out("sid-1"))
        self.assertTrue(ok["ok"])
        self.assertEqual(ok["session_id"], "sid-1")
        bad = adapters.parse_claude_compact_result("not json")
        self.assertFalse(bad["ok"])
        with self.assertRaises(ValueError):
            adapters.build_claude_compact_cmd("", "focus")


class CompactParserRejectsNonCompact(Base):
    def test_normal_planner_answer_is_not_compact(self):
        out = claude_callback_out("sid-1", "Approved.")
        parsed = adapters.parse_claude_compact_result(out)
        self.assertFalse(parsed["ok"])
        self.assertIn("not a compact", parsed["error"])
        # A generic success result without the marker is also rejected.
        generic = json.dumps({"type": "result", "subtype": "success",
                              "is_error": False, "result": "done",
                              "session_id": "sid-1"})
        bad = adapters.parse_claude_compact_result(generic)
        self.assertFalse(bad["ok"])

    def test_compact_subtype_marker_counts(self):
        alt = json.dumps({"type": "result", "subtype": "compact",
                          "is_error": False, "result": "summarized",
                          "session_id": "sid-9"})
        parsed = adapters.parse_claude_compact_result(alt)
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["session_id"], "sid-9")

    def test_plain_text_marker_requires_session_id(self):
        # Non-JSON output naming the compact marker but carrying no
        # session id is a recorded failure, never success.
        bad = adapters.parse_claude_compact_result(
            "local_command: compact done")
        self.assertFalse(bad["ok"])
        self.assertIn("session", (bad["error"] or "").lower())
        # The same marker plus a JSON session_id line counts.
        good_text = ("local_command: compact\n"
                     + json.dumps({"session_id": "sid-1"}))
        good = adapters.parse_claude_compact_result(good_text)
        self.assertTrue(good["ok"])
        self.assertEqual(good["session_id"], "sid-1")
        # End to end: the harness records a failure reason for the
        # session-less marker.
        reason = harnesses.harness_named("claude").compact_failure_reason(
            "claude_compact", 0, "local_command: compact done",
            {"planner_session_id": "sid-1"})
        self.assertIsNotNone(reason)

    def test_planner_prose_mentioning_compact_is_not_compact(self):
        # A normal planner answer that mentions the word compact in its
        # decisions prose must not count as a compact turn: only the
        # verified live marker (local_command compact) counts.
        prose = ("We decided to compact the scope and keep the API shape.")
        out = claude_callback_out("sid-1", prose)
        parsed = adapters.parse_claude_compact_result(out)
        self.assertFalse(parsed["ok"])
        self.assertIn("not a compact", parsed["error"])
        # The live shape still counts.
        live = adapters.parse_claude_compact_result(
            claude_compact_out("sid-1"))
        self.assertTrue(live["ok"])


class CompactForkRejected(Base):
    def fake_spawn(self, pid=424243):
        def _spawn(cmd):
            return pid
        return _spawn

    def test_forked_compact_recorded_as_failed(self):
        def fake_fork(cmd, cwd=None, timeout=120, **kw):
            return 0, claude_compact_out("forked-session-xyz"), ""

        core.submit_and_start(self.sd, "c-fork", {"goal": "t"},
                              self.ws("f"), "planner-orig",
                              spawn=self.fake_spawn(), run_cmd=fake_fork)
        job = core.get_job(self.sd, "c-fork")
        # A forked compact never blocks the job.
        self.assertNotIn(job["status"], ("blocked", "failed", "cancelled"))
        invs = [i for i in core._list_invocations(self.sd, "c-fork")
                if i["kind"] == "claude_compact"]
        self.assertEqual(len(invs), 1)
        self.assertEqual(invs[0]["state"], "failed")
        err = (json.loads(invs[0]["result_json"] or "{}").get("error") or "")
        self.assertIn("mismatch", err.lower())


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


class CompactMinors(Base):
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

    def test_callback_mentioning_compact_stays_callback(self):
        prompt = "Should we mention /compact in the question? Yes."
        cmd = adapters.build_claude_cmd("sid-x", prompt)
        self.assertEqual(harnesses.kind_for_cmd(cmd), "claude_callback")
        compact = adapters.build_claude_compact_cmd("sid-x", "focus words")
        self.assertEqual(harnesses.kind_for_cmd(compact), "claude_compact")


if __name__ == "__main__":
    unittest.main()
