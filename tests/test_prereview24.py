"""PREREVIEW_24 regressions: overload episodes, git renames, error redaction.

Deterministic fakes only. No live model CLIs.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, store  # noqa: E402

PY = sys.executable


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
        core.submit(sd, "r1", {"g": 1}, str(ws), "p", planner_t3_thread="planner-t3")
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
