"""Issue #50: shell proof, completion refused on failed proof, truthful supply.

Deterministic only. No live model CLIs.
"""
from __future__ import annotations

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

from runner import adapters, controller, core, direction, policy, store  # noqa: E402
from runner import supervisor as supervisor_mod  # noqa: E402
from tests.fakes import FAKE_CLAUDE, FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable

FAKE_LOADER_OK = r"""
import json
payload = {
    "status": "ready",
    "repository_root": "/fake/ws",
    "boundary": "test boundary",
    "files": [
        {"name": "VISION.md", "path": "/fake/ws/VISION.md",
         "sha256": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
         "content": "vision content"},
        {"name": "MISSION.md", "path": "/fake/ws/MISSION.md",
         "sha256": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
         "content": "mission content"},
        {"name": "OBJECTIVE.md", "path": "/fake/ws/OBJECTIVE.md",
         "sha256": "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
         "content": "objective content"},
    ],
    "instructions": {"target": "/fake/AGENTS.md",
                     "resolved_target": "/fake/canonical/AGENTS.md",
                     "sha256": "d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
                     "status": "valid-stable-link"},
    "preferences": {"status": "absent"},
}
block = "<<<AGENTSMD_PROJECT_DIRECTION_V1>>>" + json.dumps(payload) + "<<<END_AGENTSMD_PROJECT_DIRECTION_V1>>>"
print(json.dumps({"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                                         "additionalContext": block}}))
"""


def cli(state_dir, *args, env=None, timeout=25):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(ROOT), env=env)
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(fn, secs=40.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def _set_controller_state(sd, rid, **fields):
    con = store.connect(sd)
    try:
        cur = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (rid,)).fetchone()
        try:
            st = json.loads(cur["controller_state"] or "{}") if cur else {}
        except ValueError:
            st = {}
        if not isinstance(st, dict):
            st = {}
        st.update(fields)
        con.execute("UPDATE jobs SET controller_state=? WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), rid))
        con.commit()
    finally:
        con.close()


class TestProofThroughShell(unittest.TestCase):
    def _report(self, proof):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "proof50", {"goal": "x", "proof": proof}, str(ws), "pl")
        job = core.get_job(sd, "proof50")
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(
            sd, "proof50", job, 1, "muse-spark-xhigh-free", full, "ses")
        return report

    def test_and_passing(self):
        report = self._report("true && true")
        self.assertEqual(report["proof_exit_code"], 0)
        self.assertEqual(report["status"], "ok")
        log = Path(report["proof_log"]).read_text()
        self.assertIn("true && true", log)
        self.assertIn("exit 0", log)

    def test_and_failing(self):
        report = self._report("true && false")
        self.assertEqual(report["proof_exit_code"], 1)
        self.assertEqual(report["status"], "failed")
        self.assertIn("proof_failed rc=1", str(report.get("error")))
        log = Path(report["proof_log"]).read_text()
        self.assertIn("true && false", log)
        self.assertIn("exit 1", log)

    def test_rehearsal_shape_runs_as_one_argv(self):
        # The rehearsal proof failed with rc 2 because it ran as one argv;
        # through the shell both commands run and the log names them.
        report = self._report(
            "python3 -m runner.policy validate && python3 -B -m unittest tests.test_policy_v2")
        log = Path(report["proof_log"]).read_text()
        self.assertIn("python3 -m runner.policy validate && python3 -B -m unittest", log)
        self.assertIn("exit ", log)
        # Exit code is the shell's (0 only when both pass); the point is
        # the command ran as written instead of printing policy usage.
        self.assertNotIn("usage: policy.main", log)

    def test_pipe_and_quoting(self):
        report = self._report("printf 'a b' | grep -q 'a b' && echo ok")
        self.assertEqual(report["proof_exit_code"], 0)
        log = Path(report["proof_log"]).read_text()
        self.assertIn("ok", log)
        self.assertIn("exit 0", log)

    def test_runner_environment_reaches_proof(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "proofenv", {"goal": "x", "proof": "echo $PROOF50_MARKER"}, str(ws), "pl")
        job = core.get_job(sd, "proofenv")
        saved = os.environ.get("PROOF50_MARKER")
        os.environ["PROOF50_MARKER"] = "marker-50-ok"
        try:
            full = {"assistant_text": "t", "usage": None, "native_ids": {},
                    "finish": "stop", "actual_model": None}
            report = controller._write_turn_report(
                sd, "proofenv", job, 1, "muse-spark-xhigh-free", full, "ses")
        finally:
            if saved is None:
                os.environ.pop("PROOF50_MARKER", None)
            else:
                os.environ["PROOF50_MARKER"] = saved
        self.assertEqual(report["proof_exit_code"], 0)
        self.assertIn("marker-50-ok", Path(report["proof_log"]).read_text())

    def test_proof_exception_path_redacts_secrets(self):
        import subprocess as _sp

        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        secret_cmd = "echo password=hunter2-secret-shape"
        core.submit(sd, "proofredact", {"goal": "x", "proof": secret_cmd},
                    str(ws), "pl")
        job = core.get_job(sd, "proofredact")
        full = {"assistant_text": "t", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        orig_popen = _sp.Popen

        def _boom(*a, **k):
            raise OSError("boom password=hunter2-secret-shape")

        _sp.Popen = _boom  # type: ignore
        try:
            report = controller._write_turn_report(
                sd, "proofredact", job, 1, "muse-spark-xhigh-free", full,
                "ses")
        finally:
            _sp.Popen = orig_popen
        self.assertEqual(report["proof_exit_code"], 127)
        self.assertEqual(report["proof_class"], "error")
        log = Path(report["proof_log"]).read_text()
        self.assertNotIn("hunter2-secret-shape", log)
        self.assertIn("password=<redacted>", log)


class TestCompletionRefused(unittest.TestCase):
    def _failed_job(self, rid="cref-1", proof="false"):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        core.submit(sd, rid, {"goal": "x", "proof": proof}, str(ws), "pl")
        job = core.get_job(sd, rid)
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(
            sd, rid, job, 1, "muse-spark-xhigh-free", full, "ses")
        self.assertEqual(report["status"], "failed")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id=?",
                        (rid,))
        finally:
            con.close()
        _set_controller_state(sd, rid, seq=1,
                              last_action={"action": "completion", "output": "DONE"},
                              last_action_name="completion")
        return tmp, sd, ws, report

    def _completion_resume(self, output="STILL DONE"):
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

    def test_latest_report_and_reason(self):
        _tmp, sd, _ws, report = self._failed_job("cref-unit-1")
        latest = core.latest_turn_report(sd, "cref-unit-1")
        self.assertIsNotNone(latest)
        self.assertEqual(latest["proof_exit_code"], 1)
        reason = core.completion_refusal_reason(latest)
        self.assertEqual(reason, "completion_refused: proof failed rc=1")
        # A passing last turn lets completion proceed.
        latest_ok = dict(latest)
        latest_ok["proof_exit_code"] = 0
        latest_ok["status"] = "ok"
        self.assertIsNone(core.completion_refusal_reason(latest_ok))
        self.assertIsNone(core.completion_refusal_reason(None))

    def test_failed_status_with_passing_proof_is_not_proof_failure(self):
        # A worker failure with a passing proof (rc 0, including bool
        # False normalized) still refuses completion, but names the turn
        # failure instead of mislabeling it as proof failed rc=0.
        reason = core.completion_refusal_reason(
            {"seq": 1, "status": "failed", "proof_exit_code": 0,
             "error": "worker blew up"})
        self.assertTrue(str(reason).startswith("completion_refused:"),
                        reason)
        self.assertNotIn("proof failed rc=0", str(reason))
        self.assertIn("last turn failed", str(reason))
        reason_bool = core.completion_refusal_reason(
            {"seq": 1, "status": "failed", "proof_exit_code": False,
             "error": "worker blew up"})
        self.assertNotIn("proof failed rc=0", str(reason_bool))
        self.assertIn("last turn failed", str(reason_bool))

    def test_latest_turn_report_prefers_suffixed_retry(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        rid = "tiebreak-1"
        core.submit(sd, rid, {"goal": "x", "proof": "true"}, str(ws), "pl")
        root = store.ensure_state_dir(sd)
        job_dir = store.job_dir_for(root, rid)
        orig = job_dir / "turn-1"
        orig.mkdir(parents=True, exist_ok=True)
        (orig / "report.json").write_text(json.dumps(
            {"seq": 1, "status": "failed", "proof_exit_code": 1,
             "error": "proof_failed rc=1"}))
        retry = job_dir / "turn-1-1"
        retry.mkdir(parents=True, exist_ok=True)
        (retry / "report.json").write_text(json.dumps(
            {"seq": 1, "status": "ok", "proof_exit_code": 0}))
        latest = core.latest_turn_report(sd, rid)
        self.assertIsNotNone(latest)
        self.assertEqual(latest["status"], "ok")
        self.assertEqual(latest["proof_exit_code"], 0)
        self.assertTrue(str(latest.get("report_path", "")).endswith(
            "turn-1-1/report.json"))
        self.assertIsNone(core.completion_refusal_reason(latest))
        # The reverse: a failed retry supersedes a passing original.
        (orig / "report.json").write_text(json.dumps(
            {"seq": 1, "status": "ok", "proof_exit_code": 0}))
        (retry / "report.json").write_text(json.dumps(
            {"seq": 1, "status": "failed", "proof_exit_code": 2,
             "error": "proof_failed rc=2"}))
        latest2 = core.latest_turn_report(sd, rid)
        self.assertEqual(latest2["status"], "failed")
        self.assertEqual(
            core.completion_refusal_reason(latest2),
            "completion_refused: proof failed rc=2")
        # A second retry wins over the first.
        retry2 = job_dir / "turn-1-2"
        retry2.mkdir(parents=True, exist_ok=True)
        (retry2 / "report.json").write_text(json.dumps(
            {"seq": 1, "status": "ok", "proof_exit_code": 0}))
        latest3 = core.latest_turn_report(sd, rid)
        self.assertTrue(str(latest3.get("report_path", "")).endswith(
            "turn-1-2/report.json"))

    def test_first_refusal_resumes_then_insistence_blocks(self):
        _tmp, sd, _ws, report = self._failed_job("cref-2")
        run = self._completion_resume()
        first = controller.step(sd, "cref-2", run_cmd=run)
        self.assertEqual(first["action"], "completion-refused-resumed")
        job = core.get_job(sd, "cref-2")
        self.assertEqual(job["status"], "running")
        st = controller._load_controller_state(job)
        self.assertEqual(st.get("completion_refused_seq"), 1)
        # The refusal is visible in status recent events.
        status = core.status_view(sd, "cref-2")
        kinds = [e["kind"] for e in status["recent_events"]]
        self.assertIn("completion_refused", kinds)
        # The dispatcher insists on completion for the same failed turn.
        second = controller.step(sd, "cref-2", run_cmd=run)
        self.assertEqual(second["action"], "blocked")
        self.assertEqual(second["reason"], "completion_refused")
        job2 = core.get_job(sd, "cref-2")
        self.assertEqual(job2["status"], "blocked")
        self.assertTrue((job2["block_reason"] or "").startswith(
            "completion_refused: proof failed rc=1"), job2["block_reason"])
        status2 = core.status_view(sd, "cref-2")
        self.assertTrue((status2["job"]["block_reason"] or "").startswith(
            "completion_refused"))

    def test_recovery_consume_refuses_completion(self):
        _tmp, sd, _ws, report = self._failed_job("cref-3")
        root = store.ensure_state_dir(sd)
        env = {"action": "completion", "output": "RECOVERY_DONE", "artifact": ""}
        lines = [json.dumps({"type": "thread.started", "thread_id": "thr"}),
                 json.dumps({"type": "item.completed",
                             "item": {"type": "agent_message",
                                      "text": json.dumps(env)}}),
                 json.dumps({"type": "turn.completed"})]
        store.secure_write_text(root / "outputs" / "crec1.stdout",
                                "\n".join(lines) + "\n")
        store.secure_write_text(root / "outputs" / "crec1.stderr", "")
        con = store.connect(sd)
        try:
            con.execute("INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                        "pid,pgid,stdout_path,stderr_path,started_at,state,rc) VALUES"
                        "('crec1','cref-3','codex_resume','[]',?,'old',999999,999999,?,?,?,?,?)",
                        (str(_ws), str(root / "outputs" / "crec1.stdout"),
                         str(root / "outputs" / "crec1.stderr"), core._utcnow(),
                         "completed", 0))
        finally:
            con.close()
        applied = core.consume_finished_invocations(sd, "cref-3")
        self.assertTrue(any(a.get("action") == "blocked" for a in applied), applied)
        job = core.get_job(sd, "cref-3")
        self.assertEqual(job["status"], "blocked")
        self.assertTrue((job["block_reason"] or "").startswith("completion_refused"))

    def _passing_job(self, rid, completion, job_kind="ordinary", proof="true",
                       with_origin=False):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        if with_origin:
            # PR-possible workspace: a Git checkout with an origin push
            # remote, so the ordinary PR-URL gate applies.
            subprocess.run(["git", "init"], cwd=str(ws), check=True,
                           capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.test"],
                           cwd=str(ws), check=True, capture_output=True)
            subprocess.run(["git", "config", "user.name", "Test"],
                           cwd=str(ws), check=True, capture_output=True)
            subprocess.run(["git", "remote", "add", "origin",
                            "https://example.test/repo.git"],
                           cwd=str(ws), check=True, capture_output=True)
        core.submit(sd, rid, {"goal": "x", "proof": proof}, str(ws), "pl",
                    job_kind=job_kind)
        job = core.get_job(sd, rid)
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(
            sd, rid, job, 1, "muse-spark-xhigh-free", full, "ses")
        self.assertEqual(report["status"], "ok")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id=?",
                        (rid,))
        finally:
            con.close()
        _set_controller_state(sd, rid, seq=1,
                              last_action=completion,
                              last_action_name="completion")
        return tmp, sd, ws, report

    def test_ordinary_completion_without_pr_url_refuses_once_then_blocks(self):
        # With an origin push remote, every unusable pr_url shape refuses
        # on the live path; none succeeds, and the reason names the
        # missing PR URL (never a proof failure, which passes here).
        bad = [{"action": "completion", "output": "DONE"},
               {"action": "completion", "output": "DONE", "pr_url": None},
               {"action": "completion", "output": "DONE", "pr_url": 123},
               {"action": "completion", "output": "DONE", "pr_url": ""},
               {"action": "completion", "output": "DONE", "pr_url": "   "}]
        for i, completion in enumerate(bad):
            with self.subTest(completion=completion):
                _tmp, sd, _ws, _rep = self._passing_job(f"nopr-{i}", completion,
                                                        with_origin=True)
                run = self._completion_resume()
                first = controller.step(sd, f"nopr-{i}", run_cmd=run)
                self.assertEqual(first["action"], "completion-refused-resumed")
                job = core.get_job(sd, f"nopr-{i}")
                self.assertNotEqual(job["status"], "succeeded", job)
                st = controller._load_controller_state(job)
                self.assertEqual(st.get("completion_refused_seq"), 1)
                reason = st.get("completion_refused_reason") or ""
                self.assertIn("pr_url", reason)
                self.assertNotIn("proof failed", reason)
        # Insistence on the same turn blocks durably with that reason.
        _tmp, sd, _ws, _rep = self._passing_job(
            "nopr-block", {"action": "completion", "output": "DONE"},
            with_origin=True)
        run = self._completion_resume()
        self.assertEqual(controller.step(sd, "nopr-block", run_cmd=run)["action"],
                         "completion-refused-resumed")
        second = controller.step(sd, "nopr-block", run_cmd=run)
        self.assertEqual(second["action"], "blocked")
        self.assertEqual(second["reason"], "completion_refused")
        job2 = core.get_job(sd, "nopr-block")
        self.assertEqual(job2["status"], "blocked")
        self.assertTrue((job2["block_reason"] or "").startswith("completion_refused:"),
                        job2["block_reason"])
        self.assertIn("pr_url", job2["block_reason"])
        # A valid PR URL still succeeds on the live path and is preserved.
        # The live PR read is stubbed (no network or auth in unit tests);
        # the gate logic itself is proved in tests/test_issue87.py.
        _tmp, sd, _ws, _rep = self._passing_job(
            "pr-ok", {"action": "completion", "output": "DONE",
                      "pr_url": "https://example.test/pr/1"},
            with_origin=True)
        saved_verifier = core.PR_VERIFIER
        core.PR_VERIFIER = lambda workspace, pr_url: {
            "ok": True, "state": "OPEN", "is_draft": False,
            "head_sha": _rep.get("head_commit"), "repo": None, "reason": ""}
        try:
            done = controller.step(sd, "pr-ok", run_cmd=self._completion_resume())
        finally:
            core.PR_VERIFIER = saved_verifier
        self.assertEqual(done["action"], "completed")
        self.assertEqual(core.get_job(sd, "pr-ok")["status"], "succeeded")
        self.assertIn("https://example.test/pr/1",
                      core.get_job(sd, "pr-ok").get("result_json") or "")
        # Experiment jobs keep their current behavior: no PR URL needed.
        _tmp, sd, _ws, _rep = self._passing_job(
            "pr-exp", {"action": "completion", "output": "DONE"},
            job_kind="experiment")
        exp = controller.step(sd, "pr-exp", run_cmd=self._completion_resume())
        self.assertEqual(exp["action"], "completed")
        self.assertEqual(core.get_job(sd, "pr-exp")["status"], "succeeded")

    def test_ordinary_completion_without_remote_succeeds_and_records_null(self):
        # Without an origin push remote, ordinary completion succeeds as
        # before and records pr_url as JSON null, on the live path.
        for ws_kind, setup in (("plain", None), ("git-no-remote", "git")):
            with self.subTest(workspace=ws_kind):
                tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
                self.addCleanup(tmp.cleanup)
                base = Path(tmp.name)
                sd = str(base / "state")
                ws = base / "ws"
                ws.mkdir()
                if setup == "git":
                    subprocess.run(["git", "init"], cwd=str(ws), check=True,
                                   capture_output=True)
                    subprocess.run(["git", "config", "user.email",
                                    "test@example.test"], cwd=str(ws),
                                   check=True, capture_output=True)
                    subprocess.run(["git", "config", "user.name", "Test"],
                                   cwd=str(ws), check=True, capture_output=True)
                rid = f"noremote-live-{ws_kind}"
                core.submit(sd, rid, {"goal": "x", "proof": "true"},
                            str(ws), "pl")
                job = core.get_job(sd, rid)
                full = {"assistant_text": "did stuff", "usage": None,
                        "native_ids": {}, "finish": "stop",
                        "actual_model": None}
                report = controller._write_turn_report(
                    sd, rid, job, 1, "muse-spark-xhigh-free", full, "ses")
                self.assertEqual(report["status"], "ok")
                con = store.connect(sd)
                try:
                    con.execute("UPDATE jobs SET codex_task_id='thr',"
                                " status='running' WHERE request_id=?", (rid,))
                finally:
                    con.close()
                _set_controller_state(sd, rid, seq=1,
                                      last_action={"action": "completion",
                                                   "output": "DONE"},
                                      last_action_name="completion")
                done = controller.step(sd, rid,
                                       run_cmd=self._completion_resume())
                self.assertEqual(done["action"], "completed", done)
                finished = core.get_job(sd, rid)
                self.assertEqual(finished["status"], "succeeded", finished)
                outer = json.loads(finished.get("result_json") or "null")
                self.assertTrue(outer.get("ok"), outer)
                # Live offline writes a flat record, the leased path nests
                # the payload as a JSON string: both carry pr_url null.
                if "pr_url" in outer:
                    self.assertIsNone(outer.get("pr_url"), outer)
                else:
                    inner = json.loads(outer.get("output") or "{}")
                    self.assertIn("pr_url", inner, outer)
                    self.assertIsNone(inner.get("pr_url"), outer)

    def test_recovery_consume_blocks_ordinary_completion_without_pr_url(self):
        _tmp, sd, ws, _rep = self._passing_job(
            "nopr-rec", {"action": "completion", "output": "DONE"},
            with_origin=True)
        root = store.ensure_state_dir(sd)
        env = {"action": "completion", "output": "RECOVERY_DONE", "artifact": ""}
        lines = [json.dumps({"type": "thread.started", "thread_id": "thr"}),
                 json.dumps({"type": "item.completed",
                             "item": {"type": "agent_message",
                                      "text": json.dumps(env)}}),
                 json.dumps({"type": "turn.completed"})]
        store.secure_write_text(root / "outputs" / "nrec1.stdout",
                                "\n".join(lines) + "\n")
        store.secure_write_text(root / "outputs" / "nrec1.stderr", "")
        con = store.connect(sd)
        try:
            con.execute("INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                        "pid,pgid,stdout_path,stderr_path,started_at,state,rc) VALUES"
                        "('nrec1','nopr-rec','codex_resume','[]',?,'old',999999,999999,?,?,?,?,?)",
                        (str(ws), str(root / "outputs" / "nrec1.stdout"),
                         str(root / "outputs" / "nrec1.stderr"), core._utcnow(),
                         "completed", 0))
        finally:
            con.close()
        applied = core.consume_finished_invocations(sd, "nopr-rec")
        self.assertTrue(any(a.get("action") == "blocked" for a in applied), applied)
        job = core.get_job(sd, "nopr-rec")
        self.assertEqual(job["status"], "blocked")
        self.assertNotEqual(job["status"], "succeeded")
        self.assertTrue((job["block_reason"] or "").startswith("completion_refused:"),
                        job["block_reason"])
        self.assertIn("pr_url", job["block_reason"])
        # An experiment job with the same envelope still succeeds in recovery.
        _tmp2, sd2, ws2, _rep2 = self._passing_job(
            "nopr-rec-exp", {"action": "completion", "output": "DONE"},
            job_kind="experiment")
        root2 = store.ensure_state_dir(sd2)
        store.secure_write_text(root2 / "outputs" / "nrec2.stdout",
                                "\n".join(lines) + "\n")
        store.secure_write_text(root2 / "outputs" / "nrec2.stderr", "")
        con = store.connect(sd2)
        try:
            con.execute("INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                        "pid,pgid,stdout_path,stderr_path,started_at,state,rc) VALUES"
                        "('nrec2','nopr-rec-exp','codex_resume','[]',?,'old',999999,999999,?,?,?,?,?)",
                        (str(ws2), str(root2 / "outputs" / "nrec2.stdout"),
                         str(root2 / "outputs" / "nrec2.stderr"), core._utcnow(),
                         "completed", 0))
        finally:
            con.close()
        applied2 = core.consume_finished_invocations(sd2, "nopr-rec-exp")
        self.assertTrue(any(a.get("action") == "completed" for a in applied2), applied2)
        self.assertEqual(core.get_job(sd2, "nopr-rec-exp")["status"], "succeeded")

    def test_recovery_consume_succeeds_without_remote_and_records_null(self):
        # Without an origin push remote, recovery consumes an ordinary
        # completion without a PR URL as success, recording null.
        _tmp, sd, ws, _rep = self._passing_job(
            "nopr-rec-ok", {"action": "completion", "output": "DONE"})
        root = store.ensure_state_dir(sd)
        env = {"action": "completion", "output": "RECOVERY_DONE", "artifact": ""}
        lines = [json.dumps({"type": "thread.started", "thread_id": "thr"}),
                 json.dumps({"type": "item.completed",
                             "item": {"type": "agent_message",
                                      "text": json.dumps(env)}}),
                 json.dumps({"type": "turn.completed"})]
        store.secure_write_text(root / "outputs" / "nrecok1.stdout",
                                "\n".join(lines) + "\n")
        store.secure_write_text(root / "outputs" / "nrecok1.stderr", "")
        con = store.connect(sd)
        try:
            con.execute("INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                        "pid,pgid,stdout_path,stderr_path,started_at,state,rc) VALUES"
                        "('nrecok1','nopr-rec-ok','codex_resume','[]',?,'old',999999,999999,?,?,?,?,?)",
                        (str(ws), str(root / "outputs" / "nrecok1.stdout"),
                         str(root / "outputs" / "nrecok1.stderr"), core._utcnow(),
                         "completed", 0))
        finally:
            con.close()
        applied = core.consume_finished_invocations(sd, "nopr-rec-ok")
        self.assertTrue(any(a.get("action") == "completed" for a in applied), applied)
        job = core.get_job(sd, "nopr-rec-ok")
        self.assertEqual(job["status"], "succeeded", job)
        outer = json.loads(job.get("result_json") or "null")
        self.assertTrue(outer.get("ok"), outer)
        inner = json.loads(outer.get("output") or "{}")
        self.assertIn("pr_url", inner, outer)
        self.assertIsNone(inner.get("pr_url"), outer)


class TestSupplyLabels(unittest.TestCase):
    def test_runner_hook_none_labels(self):
        self.assertEqual(direction.supply_for_harness("opencode", True), "runner")
        self.assertEqual(direction.supply_for_harness("codex", True), "hook")
        self.assertEqual(direction.supply_for_harness("claude", True), "hook")
        self.assertEqual(direction.supply_for_harness("grok", True), "hook")
        self.assertEqual(direction.supply_for_harness("opencode", False), "none")

    def test_session_input_hash_in_every_injected_case(self):
        payload = {"status": "ready",
                   "files": [{"name": "VISION.md"}, {"name": "MISSION.md"},
                             {"name": "OBJECTIVE.md"}],
                   "instructions": {"resolved_target": "/fake/AGENTS.md",
                                    "sha256": "abc"}}
        block = (direction.BLOCK_START + json.dumps(payload)
                 + direction.BLOCK_END)
        din = {"ok": True, "block": block, "payload": payload,
               "status": "ready", "reason": None}
        runner_prompt, rec = direction.session_input("do work", None, "opencode", din)
        self.assertEqual(rec["supply"], "runner")
        self.assertIn(direction.BLOCK_START, runner_prompt)
        self.assertIsNotNone(rec["block_hash"])
        hook_prompt, rec2 = direction.session_input("do work", None, "codex", din)
        self.assertEqual(rec2["supply"], "hook")
        self.assertEqual(hook_prompt, "do work")
        self.assertIsNotNone(rec2["block_hash"])
        _none_prompt, rec3 = direction.session_input(
            "do work", None, "opencode",
            {"ok": False, "block": None, "status": "gap",
             "reason": "loader missing"})
        self.assertEqual(rec3["supply"], "none")
        self.assertIsNone(rec3["block_hash"])

    def test_worker_and_dispatcher_kits_resolve(self):
        worker, _, _ = direction.kit_for_invocation("muse-spark-xhigh-free",
                                                    "implementation")
        self.assertEqual(worker, "worker")
        dispatcher, _, _ = direction.kit_for_invocation("luna-go/max", "dispatch")
        self.assertEqual(dispatcher, "dispatcher")
        native, _, _ = direction.kit_for_invocation("luna/max", "dispatch")
        self.assertEqual(native, "dispatcher")

    def test_owned_server_drive_persists_runner_supply(self):
        # The drive-time ledger update records what was actually sent, so a
        # hook-kit turn records hook (no duplicate block) and never stays
        # `none`. Exercises the real supervisor drive path, not a manual
        # UPDATE. The worker kit names agentsmd-project-direction (ISSUE_52).
        # The runner-injection shape (kit without the plugin) is proven by
        # tests/test_supply52.py.
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        core.submit(sd, "supply50", {"goal": "x"}, str(ws), "pl")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='supply50'")
            root = store.ensure_state_dir(sd)
            stdout = root / "outputs" / "supply50.inv1.stdout"
            stderr = root / "outputs" / "supply50.inv1.stderr"
            store.secure_write_text(
                stdout, "opencode server listening on http://127.0.0.1:18777\n")
            store.secure_write_text(stderr, "")
            block = (direction.BLOCK_START + json.dumps({"status": "ready"})
                     + direction.BLOCK_END)
            meta = {"prompt": "do work", "route": "muse-spark-xhigh-free",
                    "stage": "implementation", "direction_block": block,
                    "direction_status": "ready", "kit": "worker",
                    "model": "opencode/muse-spark-1.3-contributor-free",
                    "variant": "xhigh", "agent": "build"}
            con.execute("INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                        "owner_token,pid,pgid,stdout_path,stderr_path,started_at,state,meta_json,"
                        "stage,requested_route,direction_supply,direction_hash) VALUES"
                        "('inv1','supply50','opencode_control','[]',?,'tok',NULL,NULL,?,?,?,"
                        "'running',?,'implementation','muse-spark-xhigh-free','none',NULL)",
                        (str(ws), str(stdout), str(stderr), core._utcnow(),
                         json.dumps(meta)))
        finally:
            con.close()
        job = core.get_job(sd, "supply50")

        class _FakeProc:
            def poll(self):
                return None

        captured = {}

        class _FakeClient:
            def __init__(self, base_url, password, directory=None, request_func=None):
                self.prompted = False
                self.sid = "ses_test123456"

            def health(self):
                return {"healthy": True}

            def create_session(self, title=None, permission=None):
                return {"id": self.sid}

            @staticmethod
            def session_id_from(payload):
                if isinstance(payload, dict):
                    val = payload.get("id")
                    if isinstance(val, str) and val.startswith("ses"):
                        return val
                return None

            def messages(self, sid):
                if not self.prompted:
                    return []
                return [
                    {"info": {"id": "msg_user_1", "role": "user",
                              "sessionID": sid},
                     "parts": [{"type": "text", "text": "do work"}]},
                    {"info": {"id": "msg_asst_1", "role": "assistant",
                              "sessionID": sid,
                              "time": {"created": 1, "completed": 2},
                              "providerID": "opencode",
                              "modelID": "muse-spark-1.3-contributor-free",
                              "variant": "xhigh", "finish": "stop",
                              "tokens": {"input": 10, "output": 5},
                              "cost": 0.0},
                     "parts": [{"type": "text", "text": "IMPLEMENTED"}]},
                ]

            def prompt_async(self, sid, text, model=None, variant=None,
                             agent=None):
                self.prompted = True
                captured["prompt"] = text
                captured["model"] = model
                return None

            def session_status(self, sid):
                return {"type": "idle"}

            def abort(self, sid):
                return True

            def wait_idle(self, sid, timeout=30.0, interval=0.5):
                return {"idle": True, "status": {"type": "idle"}}

        fake_client = _FakeClient("http://127.0.0.1:1", "pw")
        orig_client = adapters.OpenCodeClient
        adapters.OpenCodeClient = lambda *a, **k: fake_client  # type: ignore
        try:
            result, saved, skind = supervisor_mod._drive_opencode_control(
                sd, "supply50", "inv1", _FakeProc(),
                str(stdout), str(stderr), "pw", dict(meta), job,
                time.monotonic() + 20.0, str(ws))
        finally:
            adapters.OpenCodeClient = orig_client
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("rc"), 0, result)
        # The worker kit is a hook host: no duplicate block is sent, but
        # the hash from the runner's own loader read is recorded.
        self.assertNotIn(direction.BLOCK_START, captured.get("prompt", ""))
        self.assertTrue(captured.get("prompt", "").rstrip().endswith("do work"))
        meas = {m["kind"]: m for m in core.invocation_measurements(sd, "supply50")}
        self.assertEqual(meas["opencode_control"]["supply"], "hook")
        self.assertIsNotNone(meas["opencode_control"]["direction_hash"])
        self.assertEqual(meas["opencode_control"]["direction_status"], "ready")
        self.assertEqual(
            meas["opencode_control"]["direction_hash"],
            direction.block_hash(block))


class TestPublicFallbackDrill(unittest.TestCase):
    def test_cli_fallback_dispatch_worker_proof_passes(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        (ws / "VISION.md").write_text("# vision\n")
        (ws / "MISSION.md").write_text("# mission\n")
        (ws / "OBJECTIVE.md").write_text("# objective\n")
        (ws / "AGENTS.md").write_text("# workspace agents\n")
        bindir = base / "bin"
        bindir.mkdir()
        fs = base / "fakestate"
        fs.mkdir()
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        loader_path = bindir / "project-direction"
        loader_path.write_text("#!" + PY + "\n" + FAKE_LOADER_OK,
                               encoding="utf-8")
        loader_path.chmod(0o700)
        env = dict(os.environ)
        env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""),
                   FAKE_STATE=str(fs), FAKE_OC_DELAY="0.2",
                   FAKE_OC_WRITE="fix.txt",
                   FAKE_OC_PLAN="implement_then_complete",
                   PYTHONDONTWRITEBYTECODE="1")
        env.pop("MODEL_ROUTER_PROJECT_DIRECTION_BIN", None)
        rid = "i50-cli-1"
        rc, _out, err = cli(sd, "submit", "--request-id", rid,
                            "--task", '{"goal":"issue50 drill","proof":"true && true"}',
                            "--workspace", str(ws), "--planner-session", "p-i50",
                            env=env)
        self.assertEqual(rc, 0, err)
        core.record_capacity(sd, "luna/max", "exhausted",
                             {"source": "codex", "message": "usage limit reached"},
                             reset_at="2026-09-22T09:51:00+00:00",
                             reset_source="provider")
        self.addCleanup(lambda: self._cleanup(sd, rid))
        rc, _out, err = cli(sd, "start", "--request-id", rid, env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(
            lambda: core.get_job(sd, rid)["status"] in
            ("succeeded", "failed", "blocked"), 40),
            core.get_job(sd, rid))
        job = core.get_job(sd, rid)
        self.assertEqual(job["status"], "succeeded", job.get("block_reason"))
        self.assertTrue((ws / "fix.txt").exists())
        reports = sorted((store.job_dir_for(
            store.ensure_state_dir(sd), rid)).glob("turn-*/report.json"))
        self.assertTrue(reports)
        last = json.loads(reports[-1].read_text())
        self.assertEqual(last["proof_exit_code"], 0)
        self.assertIn("true && true", last["proof_command"])
        log = Path(last["proof_log"]).read_text()
        self.assertIn("true && true", log)
        self.assertIn("exit 0", log)
        # The exhausted Codex dispatch route fell back to the owned
        # server: the dispatcher ran on luna-go/max, not luna/max.
        st = controller._load_controller_state(job)
        self.assertEqual(st.get("dispatch_route"), "luna-go/max", st)
        self.assertEqual(st.get("route_reason"), "preflight_exhausted", st)
        con = store.connect(sd)
        try:
            rows = con.execute(
                "SELECT kind,payload_json FROM events WHERE request_id=?",
                (rid,)).fetchall()
        finally:
            con.close()
        switched = []
        for r in rows:
            if r["kind"] != "route_switched":
                continue
            try:
                payload = json.loads(r["payload_json"] or "{}")
            except ValueError:
                payload = {}
            if payload.get("to") == "luna-go/max":
                switched.append(payload)
        self.assertTrue(switched, [dict(r) for r in rows])
        # Supply labels (ISSUE_52): the fallback dispatcher and the worker
        # both ran on the owned server with kits naming
        # agentsmd-project-direction, so both record hook with a direction
        # hash and no duplicate block; the dispatcher kit is dispatcher,
        # the worker kit is worker. A dispatcher on a host hook would
        # record hook instead (covered by test_supply30's Codex-native
        # drill). The runner-injection shape (kit without the plugin) is
        # proven by tests/test_supply52.py.
        meas = core.invocation_measurements(sd, rid)
        by_kit_stage = {(m.get("kit"), m.get("stage")): m for m in meas}
        disp = by_kit_stage.get(("dispatcher", "dispatch"))
        self.assertIsNotNone(disp, meas)
        self.assertEqual(disp["requested_route"], "luna-go/max", disp)
        self.assertEqual(disp["supply"], "hook", disp)
        self.assertIsNotNone(disp["direction_hash"], disp)
        self.assertEqual(disp["direction_status"], "ready", disp)
        workers = [m for m in meas if m.get("kit") == "worker"]
        self.assertTrue(workers, meas)
        worker = workers[0]
        self.assertEqual(worker["supply"], "hook", worker)
        self.assertIsNotNone(worker["direction_hash"], worker)
        self.assertEqual(worker["direction_status"], "ready", worker)

    def _cleanup(self, sd, request_id):
        try:
            job = core.get_job(sd, request_id)
        except Exception:
            return
        if job.get("owner_pid"):
            try:
                os.kill(int(job["owner_pid"]), signal.SIGKILL)
            except Exception:
                pass
        for inv in core._list_invocations(sd, request_id):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass


if __name__ == "__main__":
    unittest.main()
