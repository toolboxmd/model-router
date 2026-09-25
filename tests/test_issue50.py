"""Issue #50: shell proof, completion refused on failed proof, truthful supply.

Deterministic only. No live model CLIs.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, store  # noqa: E402
from tests.fakes import use_fake_t3  # noqa: E402

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
        core.submit(sd, "proof50", {"goal": "x", "proof": proof}, str(ws), "pl", planner_t3_thread="planner-t3")
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
        core.submit(sd, "proofenv", {"goal": "x", "proof": "echo $PROOF50_MARKER"}, str(ws), "pl", planner_t3_thread="planner-t3")
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
                    str(ws), "pl", planner_t3_thread="planner-t3")
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
        core.submit(sd, rid, {"goal": "x", "proof": proof}, str(ws), "pl", planner_t3_thread="planner-t3")
        job = core.get_job(sd, rid)
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(
            sd, rid, job, 1, "muse-spark-xhigh-free", full, "ses")
        self.assertEqual(report["status"], "failed")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id=?",
                        (rid,))
        finally:
            con.close()
        _set_controller_state(sd, rid, seq=1,
                              last_action={"action": "completion", "output": "DONE"},
                              last_action_name="completion")
        return tmp, sd, ws, report

    def _completion_resume(self, sd, rid, output="STILL DONE"):
        # The dispatcher thread answers every resume with a completion.
        use_fake_t3(self, sd, rid, default={"action": "completion",
                                            "output": output, "artifact": ""})

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
        core.submit(sd, rid, {"goal": "x", "proof": "true"}, str(ws), "pl", planner_t3_thread="planner-t3")
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
        self._completion_resume(sd, "cref-2")
        first = controller.step(sd, "cref-2")
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
        second = controller.step(sd, "cref-2")
        self.assertEqual(second["action"], "blocked")
        self.assertEqual(second["reason"], "completion_refused")
        job2 = core.get_job(sd, "cref-2")
        self.assertEqual(job2["status"], "blocked")
        self.assertTrue((job2["block_reason"] or "").startswith(
            "completion_refused: proof failed rc=1"), job2["block_reason"])
        status2 = core.status_view(sd, "cref-2")
        self.assertTrue((status2["job"]["block_reason"] or "").startswith(
            "completion_refused"))


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
                    job_kind=job_kind, planner_t3_thread="planner-t3")
        job = core.get_job(sd, rid)
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        report = controller._write_turn_report(
            sd, rid, job, 1, "muse-spark-xhigh-free", full, "ses")
        self.assertEqual(report["status"], "ok")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')), status='running' WHERE request_id=?",
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
                self._completion_resume(sd, f"nopr-{i}")
                first = controller.step(sd, f"nopr-{i}")
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
        self._completion_resume(sd, "nopr-block")
        self.assertEqual(controller.step(sd, "nopr-block")["action"],
                         "completion-refused-resumed")
        second = controller.step(sd, "nopr-block")
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
            done = controller.step(sd, "pr-ok")
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
        exp = controller.step(sd, "pr-exp")
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
                            str(ws), "pl", planner_t3_thread="planner-t3")
                job = core.get_job(sd, rid)
                full = {"assistant_text": "did stuff", "usage": None,
                        "native_ids": {}, "finish": "stop",
                        "actual_model": None}
                report = controller._write_turn_report(
                    sd, rid, job, 1, "muse-spark-xhigh-free", full, "ses")
                self.assertEqual(report["status"], "ok")
                con = store.connect(sd)
                try:
                    con.execute("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')),"
                                " status='running' WHERE request_id=?", (rid,))
                finally:
                    con.close()
                _set_controller_state(sd, rid, seq=1,
                                      last_action={"action": "completion",
                                                   "output": "DONE"},
                                      last_action_name="completion")
                done = controller.step(sd, rid)
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


if __name__ == "__main__":
    unittest.main()
