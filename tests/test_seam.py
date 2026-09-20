"""Harness seam: one interface, no harness branches in core or supervisor, capability checks."""
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import controller, core, harnesses, policy, store  # noqa: E402


class Seam(unittest.TestCase):
    def test_no_harness_branches_in_core_or_supervisor(self):
        pattern = re.compile(r"\bkind (==|!=|in|not in) |kind\.startswith|\"(codex_dispatch|codex_resume|"
                             r"claude_callback|opencode_control|opencode_serve|opencode_run)\"")
        for name in ("runner/core.py", "runner/supervisor.py"):
            src = (ROOT / name).read_text()
            self.assertIsNone(pattern.search(src), name)

    def test_three_harnesses_behind_one_interface(self):
        self.assertEqual(set(harnesses.HARNESSES), {"codex", "claude", "opencode"})
        for h in harnesses.HARNESSES.values():
            for method in ("spawn_spec", "drive", "parse_session", "parse_report", "classify_signal",
                           "measure", "capabilities"):
                self.assertTrue(hasattr(h, method), (h.name, method))
        self.assertIs(harnesses.harness_for("codex_resume"), harnesses.HARNESSES["codex"])
        self.assertIs(harnesses.harness_for("claude_callback"), harnesses.HARNESSES["claude"])
        self.assertIs(harnesses.harness_for("opencode_control"), harnesses.HARNESSES["opencode"])
        self.assertTrue(harnesses.HARNESSES["opencode"].owned_server)
        self.assertEqual(harnesses.kind_for_cmd(["opencode", "serve"]), "opencode_serve")
        self.assertEqual(harnesses.kind_for_cmd(["claude", "--resume", "x"]), "claude_callback")

    def test_every_stage_route_has_its_capabilities(self):
        for stage, spec in policy.STAGES.items():
            for route in spec["routes"] + spec.get("overrides", []):
                self.assertIsNone(harnesses.route_capability_blocker(route, stage), (stage, route))

    def test_mismatch_is_refused_with_a_precise_reason(self):
        blocker = harnesses.route_capability_blocker("fable-5.1/max", "implementation_default")
        self.assertIn("lacks workspace_write", blocker)
        self.assertIn("implementation_default", blocker)
        self.assertIn("unsupported route", harnesses.route_capability_blocker("nope", "dispatch"))
        self.assertIsNone(harnesses.route_capability_blocker("luna-go/max", "dispatch"))

    def test_durable_runner_refuses_mismatch_before_invocation(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "cap1", {"goal": "t"}, str(ws), "planner-1")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='cap1'")
        finally:
            con.close()
        run = core.make_durable_run_cmd(sd, "cap1", "tok")
        with self.assertRaises(core.RunnerError) as ctx:
            run(["claude", "--resume", "x"], str(ws), 5, kind="claude_callback",
                meta={"stage": "implementation_default", "route": "fable-5.1/max",
                      "qid": "q1", "reason": "test"})
        self.assertIn("route_capability_mismatch", str(ctx.exception))
        self.assertIn("workspace_write", str(ctx.exception))
        self.assertEqual(core._list_invocations(sd, "cap1"), [])


class DispatchRouteAtomic(unittest.TestCase):
    def _submit(self, base, rid="d1"):
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir(exist_ok=True)
        core.submit(sd, rid, {"goal": "t"}, str(ws), "planner-1")
        return sd

    def _fallback_run_cmd(self, session="ses_fallback_001"):
        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            body = {"opencode_session_id": session, "ok": True, "rc": 0,
                    "assistant_text": json.dumps({"action": "completion",
                                                  "output": "PLANNED", "artifact": ""}),
                    "finish": "stop",
                    "actual_model": {"providerID": "opencode-go", "modelID": "gpt-5.6-luna"}}
            return 0, "RUNNER_RESULT " + json.dumps(body) + "\n", ""
        return run

    def test_fallback_persists_route_with_session(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = self._submit(base)
        res = controller._dispatch_on_opencode(
            sd, "d1", "luna-go/max", "prompt", self._fallback_run_cmd(), "initial")
        self.assertEqual(res["action"], "dispatched")
        job = core.get_job(sd, "d1")
        self.assertEqual(job["codex_task_id"], "ses_fallback_001")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("dispatch_route"), "luna-go/max")

    def test_route_survives_a_crash_between_writes(self):
        # The fallback must not depend on a second _set_phase write: even
        # when _set_phase fails, the session and its route are already saved.
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = self._submit(base)
        with mock.patch.object(controller, "_set_phase",
                               side_effect=AssertionError("must not be called")):
            res = controller._dispatch_on_opencode(
                sd, "d1", "luna-go/max", "prompt", self._fallback_run_cmd(), "initial")
        self.assertEqual(res["action"], "dispatched")
        job = core.get_job(sd, "d1")
        self.assertEqual(job["codex_task_id"], "ses_fallback_001")
        self.assertEqual(json.loads(job["controller_state"] or "{}").get("dispatch_route"),
                         "luna-go/max")


class OpenCodeIdentity(unittest.TestCase):
    def test_parse_and_identity(self):
        h = harnesses.harness_named("opencode")
        body = {"opencode_session_id": "ses_abc", "ok": True}
        out = "RUNNER_RESULT " + json.dumps(body) + "\n"
        sid, skind = h.parse_session("opencode_control", out, "", None)
        self.assertEqual((sid, skind), ("ses_abc", "opencode_session_id"))
        self.assertEqual(h.parse_session("opencode_control", "no result\n", "", None), (None, None))
        self.assertTrue(h.identity_ok("opencode_control", "ses_abc", "ses_abc"))
        self.assertTrue(h.identity_ok("opencode_control", "ses_abc", None))
        self.assertFalse(h.identity_ok("opencode_control", "ses_other", "ses_abc"))
        self.assertFalse(h.identity_ok("opencode_control", None, "ses_abc"))
        self.assertFalse(h.identity_ok("opencode_control", "", "ses_abc"))

    def _dispatch_inv(self, sd, ws, inv_id, session, action="completion", rc=0, state="completed"):
        root = store.ensure_state_dir(sd)
        outp = root / "outputs" / f"{inv_id}.stdout"
        errp = root / "outputs" / f"{inv_id}.stderr"
        body = {}
        if session is not None:
            body["opencode_session_id"] = session
        body.update({"ok": True, "rc": 0,
                     "assistant_text": json.dumps({"action": action, "output": "x"}),
                     "finish": "stop"})
        store.secure_write_text(outp, "RUNNER_RESULT " + json.dumps(body) + "\n")
        store.secure_write_text(errp, "")
        con = store.connect(sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                "owner_token,pid,pgid,stdout_path,stderr_path,started_at,state,rc,"
                "task_json,timeout_secs,meta_json,action_key,stage,requested_route,"
                "policy_version,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (inv_id, "oid", "opencode_control", json.dumps(["opencode", "serve"]),
                 str(ws), "tok", None, None, str(outp), str(errp), core._utcnow(),
                 state, rc, "{}", 5, json.dumps({"stage": "dispatch"}), None,
                 "dispatch", "luna-go/max", policy.POLICY_VERSION, "initial"))
        finally:
            con.close()

    def test_recovery_rejects_mismatched_and_missing_sessions(self):
        for session, tag in (("ses_other", "mismatch"), (None, "missing")):
            with self.subTest(tag=tag):
                tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
                self.addCleanup(tmp.cleanup)
                base = Path(tmp.name)
                sd = str(base / "state")
                ws = base / "ws"
                ws.mkdir()
                core.submit(sd, "oid", {"goal": "t"}, str(ws), "planner-1")
                con = store.connect(sd)
                try:
                    con.execute("UPDATE jobs SET codex_task_id='ses_saved', status='running'"
                                " WHERE request_id='oid'")
                finally:
                    con.close()
                self._dispatch_inv(sd, ws, f"inv-{tag}", session)
                applied = core.consume_finished_invocations(sd, "oid")
                self.assertTrue(applied, tag)
                job = core.get_job(sd, "oid")
                self.assertEqual(job["status"], "blocked", tag)
                self.assertIn("luna_task_mismatch", job["block_reason"], tag)
                self.assertNotEqual(job["status"], "succeeded")


class CodexFileFallback(unittest.TestCase):
    def test_infer_and_turn_ok_use_the_last_message_file(self):
        h = harnesses.harness_named("codex")
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        last = Path(tmp.name) / "last.json"
        last.write_text(json.dumps({"action": "completion", "output": "done"}))
        cmd = ["codex", "exec", "--json", "--output-last-message", str(last),
               "--model", "gpt-5.6-luna"]
        stdout = json.dumps({"type": "thread.started", "thread_id": "thr-1"}) + "\n"
        self.assertEqual(h.infer_rc("codex_dispatch", stdout, cmd), 0)
        self.assertTrue(h.turn_ok("codex_dispatch", 0, stdout, cmd))
        self.assertEqual(h.luna_action("codex_dispatch", stdout, cmd)["action"], "completion")
        # Without the file the same stdout does not count.
        self.assertEqual(h.infer_rc("codex_dispatch", stdout, ["codex", "exec"]), 1)
        self.assertFalse(h.turn_ok("codex_dispatch", 0, stdout, ["codex", "exec"]))

    def test_recovery_consumes_a_file_only_turn(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        core.submit(sd, "fid", {"goal": "t"}, str(ws), "planner-1")
        root = store.ensure_state_dir(sd)
        last = root / "outputs" / "fid.dispatch-last.json"
        store.secure_write_text(last, json.dumps({"action": "completion", "output": "FILE_DONE"}))
        outp = root / "outputs" / "inv1.stdout"
        errp = root / "outputs" / "inv1.stderr"
        store.secure_write_text(outp, json.dumps({"type": "thread.started",
                                                  "thread_id": "thr-file-1"}) + "\n")
        store.secure_write_text(errp, "")
        cmd = ["codex", "exec", "--json", "--output-last-message", str(last),
               "--model", "gpt-5.6-luna"]
        con = store.connect(sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                "owner_token,pid,pgid,stdout_path,stderr_path,started_at,state,rc,"
                "task_json,timeout_secs,meta_json,action_key,stage,requested_route,"
                "policy_version,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("inv1", "fid", "codex_dispatch", json.dumps(cmd), str(ws), "tok",
                 None, None, str(outp), str(errp), core._utcnow(), "failed", None,
                 "{}", 5, json.dumps({}), None, "dispatch", "luna/max",
                 policy.POLICY_VERSION, "initial"))
        finally:
            con.close()
        applied = core.consume_finished_invocations(sd, "fid")
        self.assertTrue(applied)
        job = core.get_job(sd, "fid")
        self.assertEqual(job["status"], "succeeded")
        self.assertIn("FILE_DONE", job.get("result_json") or "")


class ControllerThroughSeam(unittest.TestCase):
    def test_no_direct_harness_parsing_in_controller(self):
        src = (ROOT / "runner" / "controller.py").read_text()
        for banned in ("adapters.parse_codex_task_id", "adapters.parse_codex_agent_envelope",
                       "adapters.parse_claude_result", "adapters.read_last_message_file",
                       "adapters.last_message_path_from_cmd", "_runner_result("):
            self.assertNotIn(banned, src, banned)
        for required in ('harness_named("codex")', 'harness_named("opencode")',
                         'harness_named("claude")', ".parse_session(", ".parse_report(",
                         ".luna_action(", ".turn_ok(", ".identity_ok("):
            self.assertIn(required, src, required)

    def test_opencode_fallback_reads_through_the_harness(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        core.submit(sd, "ocd", {"goal": "t"}, str(ws), "planner-1")
        body = {"opencode_session_id": "ses_harness_001", "ok": True, "rc": 0,
                "assistant_text": json.dumps({"action": "completion", "output": "H"}),
                "finish": "stop"}
        seen = {}

        real = harnesses.harness_named("opencode").parse_report

        def spy(kind, stdout, cmd):
            seen["called"] = True
            return real(kind, stdout, cmd)

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            return 0, "RUNNER_RESULT " + json.dumps(body) + "\n", ""

        with mock.patch.object(harnesses.HARNESSES["opencode"], "parse_report", spy):
            res = controller._dispatch_on_opencode(
                sd, "ocd", "luna-go/max", "prompt", run, "initial")
        self.assertTrue(seen.get("called"))
        self.assertEqual(res["action"], "dispatched")


class SupervisorThroughSeam(unittest.TestCase):
    def test_signals_go_through_the_harness(self):
        from runner import adapters
        src = (ROOT / "runner" / "supervisor.py").read_text()
        self.assertIn("harness.classify_signal", src)
        self.assertNotIn("_policy.classify_signal", src)
        self.assertNotIn("policy.classify_signal", src)
        h = harnesses.harness_named("opencode")
        err = adapters.OpenCodeHTTPError(503, "busy")
        self.assertEqual(h.classify_signal(err), "overloaded")
        self.assertIsNone(policy.classify_signal(err))


if __name__ == "__main__":
    unittest.main()
