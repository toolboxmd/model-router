"""PREREVIEW_24 regressions: overload episodes, git renames, error redaction.

Deterministic fakes only. No live model CLIs.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, store  # noqa: E402
from tests.fakes import FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable


class OwnedBase(unittest.TestCase):
    def _setup(self, mode):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        fake_state = base / "fakestate"
        fake_state.mkdir()
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_OC_MODE")}
        def restore():
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.addCleanup(restore)
        os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
        os.environ["FAKE_STATE"] = str(fake_state)
        os.environ["FAKE_OC_MODE"] = mode
        os.environ["FAKE_OC_DELAY"] = "0.3"
        os.environ["FAKE_OC_WRITE"] = "fix.txt"
        core.submit(sd, "oc1", {"goal": "prereview24"}, str(ws), "planner")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='oc1'")
        finally:
            con.close()
        self.sd = sd
        self.ws = ws
        self.fake_state = fake_state
        self.addCleanup(self._kill_owned)
        run = core.make_durable_run_cmd(sd, "oc1", "tok")
        def timed_run(cmd, cwd=None, timeout=None, **kw):
            return run(cmd, cwd, timeout, **kw)
        return timed_run

    def _kill_owned(self):
        try:
            for inv in core._list_invocations(self.sd, "oc1"):
                for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                    if pg:
                        try:
                            os.killpg(int(pg), signal.SIGKILL)
                        except Exception:
                            pass
        except Exception:
            pass

    def _requests(self):
        f = self.fake_state / "opencode-requests.jsonl"
        return [json.loads(l) for l in f.read_text().splitlines()] if f.exists() else []


class TestOverloadEpisodes(OwnedBase):
    def test_one_retry_across_several_polls_counts_single_attempt(self):
        """A sticky retry (same attempt number across polls) counts once.

        The fake holds attempt 1 for 3.5s, covering at least three 1s
        supervisor polls, then succeeds. Per-poll counting would reach 3
        and abort (retries=2); per-episode counting succeeds with one.
        """
        run = self._setup("sticky_retry")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok", res)
        polls = [r for r in self._requests() if r["path"] == "/session/status"]
        self.assertGreaterEqual(len(polls), 3, f"expected several polls, got {len(polls)}")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")

    def test_three_distinct_retries_abort_after_second(self):
        """Three distinct retry episodes abort on the third (after two)."""
        run = self._setup("three_retries")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "route_switched")
        self.assertEqual(res["reason"], "lateral")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "glm-5.3-flash-go")
        last = json.loads(job["last_error_json"] or "{}")
        self.assertEqual(last.get("signal"), "overloaded")
        self.assertEqual(int(last.get("overload_retries") or 0), 3)


class TestGitChanges(unittest.TestCase):
    def test_rename_and_quoted_path_with_spaces(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x",
                   GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@x")
        subprocess.run(["git", "init", "-q", str(ws)], check=True, env=env)
        (ws / "a.txt").write_text("base\n")
        subprocess.run(["git", "-C", str(ws), "add", "a.txt"], check=True, env=env)
        subprocess.run(["git", "-C", str(ws), "commit", "-q", "-m", "base"], check=True, env=env)
        subprocess.run(["git", "-C", str(ws), "mv", "a.txt", "b.txt"], check=True, env=env)
        (ws / "my file.txt").write_text("spaced-body\n")
        files, diff, note = controller._git_changes(str(ws))
        self.assertIsNone(note)
        self.assertIn("b.txt", files)
        self.assertIn("my file.txt", files)
        self.assertFalse(any("->" in f for f in files), files)
        self.assertIn("spaced-body", diff)
        self.assertIn("b.txt", diff)


class TestErrorClassRedaction(unittest.TestCase):
    def test_secret_in_raw_error_never_reaches_events(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "r1", {"g": 1}, str(ws), "p")
        secret = "hunter2-secret-xyz"
        controller._persist_error_evidence(sd, "r1", {"class": f"password={secret}", "detail": f"Bearer {secret}"})
        con = store.connect(sd)
        try:
            rows = [dict(r) for r in con.execute("SELECT * FROM events WHERE request_id=?", ("r1",)).fetchall()]
            job = dict(con.execute("SELECT * FROM jobs WHERE request_id=?", ("r1",)).fetchone())
        finally:
            con.close()
        blob = json.dumps(rows) + (job.get("last_error_json") or "")
        # The events table (error_class) must never carry the secret; the
        # redacted last_error keeps the shape but masked.
        events_blob = json.dumps([r for r in rows if r["kind"] == "provider_error_evidence"])
        self.assertNotIn(secret, events_blob)
        self.assertIn("<redacted>", events_blob)


if __name__ == "__main__":
    unittest.main(verbosity=2)
