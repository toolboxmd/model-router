"""FINDINGS_1 regressions: free-text redaction, status evidence, ladder seq, blockers.

Deterministic fakes only. No live model CLIs.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters, controller, core, store  # noqa: E402


def _submit(sd: str, rid: str, ws: Path):
    core.submit(sd, rid, {"goal": "findings1"}, str(ws), "planner")


class TestFreeTextRedaction(unittest.TestCase):
    def test_nested_masks_free_text_shapes(self):
        secret = "hunter2-secret-xyz"
        out = adapters.redact_nested({"class": f"password={secret}",
                                      "detail": f"Bearer {secret}",
                                      "nested": {"msg": f"api_key={secret}"}})
        blob = json.dumps(out)
        self.assertNotIn(secret, blob)
        self.assertIn("<redacted>", blob)

    def test_persisted_last_error_report_and_evidence_are_clean(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        _submit(sd, "r1", ws)
        secret = "hunter2-secret-xyz"
        redacted = controller._persist_error_evidence(
            sd, "r1", {"class": f"password={secret}", "detail": f"Bearer {secret}"})
        self.assertNotIn(secret, json.dumps(redacted))
        job = core.get_job(sd, "r1")
        self.assertNotIn(secret, job.get("last_error_json") or "")
        self.assertIn("<redacted>", job.get("last_error_json") or "")
        con = store.connect(sd)
        try:
            rows = [dict(r) for r in con.execute(
                "SELECT * FROM events WHERE request_id=?", ("r1",)).fetchall()]
        finally:
            con.close()
        self.assertNotIn(secret, json.dumps(rows))
        # Report error uses the same redaction.
        full = {"assistant_text": "ok", "usage": {"source": "opencode"},
                "native_ids": {"session_id": "s"},
                "actual_model": {"providerID": "opencode", "modelID": "m", "variant": "xhigh"}}
        report = controller._write_turn_report(
            sd, "r1", core.get_job(sd, "r1"), 1, "muse-spark-xhigh-free",
            full, "ses_1", status="failed", error=f"password={secret}")
        on_disk = json.loads(Path(report["report_path"]).read_text())
        self.assertNotIn(secret, json.dumps(on_disk["error"]))
        self.assertIn("<redacted>", json.dumps(on_disk["error"]))
        # Dispatcher evidence forwards the redacted error, never the secret.
        evidence = controller._implementation_evidence(core.get_job(sd, "r1"),
                                                       {"action": "implementation_failed",
                                                        "report": on_disk})
        self.assertNotIn(secret, evidence)
        self.assertIn("<redacted>", evidence)


class TestStatusViewKeepsEvidence(unittest.TestCase):
    def test_status_shows_signal_evidence_and_retry_fields(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        _submit(sd, "s1", ws)
        controller._persist_error_evidence(sd, "s1", {
            "source": "opencode_control", "rc": 4, "signal": "overloaded",
            "evidence": {"source": "transport", "status": 503},
            "idle_confirmed": True, "retry_next": 999, "retry_next_capped": 20.0,
            "overload_retries": 3})
        view = core.status_view(sd, "s1")
        le = view["job"]["last_error_json"]
        self.assertEqual(le.get("signal"), "overloaded")
        self.assertEqual(le.get("evidence"), {"source": "transport", "status": 503})
        self.assertEqual(float(le.get("retry_next_capped") or 0), 20.0)
        self.assertEqual(int(le.get("overload_retries") or 0), 3)
        self.assertTrue(le.get("idle_confirmed"))


class TestLadderSeqFallback(unittest.TestCase):
    def _job_with_invocation(self, sd: str, ws: Path, seq: int):
        _submit(sd, "j", ws)
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='thr', status='running' WHERE request_id='j'")
            con.execute(
                "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json, workspace,"
                " owner_token, stdout_path, stderr_path, started_at, state, meta_json)"
                " VALUES('inv-seq','j','opencode_control','[]',?,'tok','/dev/null','/dev/null',?,"
                " 'running',?)",
                (str(ws), core._utcnow(), json.dumps({"seq": seq})))
        finally:
            con.close()

    def test_reused_invocation_with_unset_state_seq_counts_once(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        self._job_with_invocation(sd, ws, 5)
        # Controller state has no seq (recovered with unset seq); the turn's
        # own report carries seq 5 from the same invocation.
        impl = {"action": "implementation_failed",
                "report": {"proof_exit_code": None, "seq": 5}}
        controller._record_turn_outcome(sd, "j", impl)
        self.assertEqual(controller._ladder(core.get_job(sd, "j"))["failures"], 1)
        controller._record_turn_outcome(sd, "j", impl)
        self.assertEqual(controller._ladder(core.get_job(sd, "j"))["failures"], 1)

    def test_invocation_seq_covers_missing_report_seq(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        self._job_with_invocation(sd, ws, 7)
        impl = {"action": "implementation_failed", "report": {}}
        controller._record_turn_outcome(sd, "j", impl)
        self.assertEqual(controller._ladder(core.get_job(sd, "j"))["failures"], 1)
        controller._record_turn_outcome(sd, "j", impl)
        self.assertEqual(controller._ladder(core.get_job(sd, "j"))["failures"], 1)


class TestBlockersForwarding(unittest.TestCase):
    def test_harness_blockers_are_forwarded_redacted(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        _submit(sd, "b1", ws)
        job = core.get_job(sd, "b1")
        secret = "hunter2-secret-xyz"
        full = {"assistant_text": "ok", "usage": {"source": "opencode"},
                "native_ids": {"session_id": "s"},
                "actual_model": {"providerID": "opencode", "modelID": "m", "variant": "xhigh"},
                "blockers": ["need a decision on scope", f"password={secret}"]}
        report = controller._write_turn_report(
            sd, "b1", job, 3, "muse-spark-xhigh-free", full, "ses_1")
        on_disk = json.loads(Path(report["report_path"]).read_text())
        self.assertIn("need a decision on scope", on_disk["blockers"][0])
        self.assertNotIn(secret, json.dumps(on_disk["blockers"]))
        self.assertIn("<redacted>", json.dumps(on_disk["blockers"]))

    def test_no_harness_blockers_stays_empty(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        _submit(sd, "b2", ws)
        job = core.get_job(sd, "b2")
        full = {"assistant_text": "ok",
                "actual_model": {"providerID": "opencode", "modelID": "m", "variant": "xhigh"}}
        report = controller._write_turn_report(
            sd, "b2", job, 4, "muse-spark-xhigh-free", full, "ses_1")
        on_disk = json.loads(Path(report["report_path"]).read_text())
        self.assertEqual(on_disk["blockers"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
