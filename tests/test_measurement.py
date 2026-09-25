"""Measurement and provenance: job fields, invocation records, reports."""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import core, store  # noqa: E402


class Provenance(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name="ws", git=False):
        d = self.base / name
        d.mkdir(exist_ok=True)
        if git:
            env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x", GIT_COMMITTER_NAME="t",
                       GIT_COMMITTER_EMAIL="t@x")
            subprocess.run(["git", "init", "-q", str(d)], check=True, env=env)
            (d / "f.txt").write_text("1\n")
            subprocess.run(["git", "-C", str(d), "add", "f.txt"], check=True, env=env)
            subprocess.run(["git", "-C", str(d), "commit", "-q", "-m", "base"], check=True, env=env)
        return str(d)

    def test_job_kind_replay_and_planner_harness(self):
        job = core.submit(self.sd, "o1", {"g": 1}, self.ws("a"), "p", planner_t3_thread="planner-t3")
        self.assertEqual((job["job_kind"], job["replay_of"], job["planner_harness"]), ("ordinary", None, "t3"))
        job = core.submit(self.sd, "r1", {"g": 1}, self.ws("b"), "p", job_kind="replay", replay_of="o1",
                          planner_harness="claude", planner_t3_thread="planner-t3")
        self.assertEqual((job["job_kind"], job["replay_of"], job["planner_harness"]), ("replay", "o1", "claude"))
        # Every supported planner harness is accepted; unknown ones are not.
        for harness in ("claude", "codex", "opencode", "grok"):
            job = core.submit(self.sd, f"plan-{harness}", {"g": 1},
                              self.ws(f"ws-{harness}"), "p",
                              planner_harness=harness, planner_t3_thread="planner-t3")
            self.assertEqual(job["planner_harness"], harness)
        with self.assertRaises(ValueError):
            core.submit(self.sd, "bad-harness", {"g": 1}, self.ws("ws-bad"),
                        "p", planner_harness="smoke", planner_t3_thread="planner-t3")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "bad1", {"g": 1}, self.ws("c"), "p", job_kind="replay", planner_t3_thread="planner-t3")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "bad2", {"g": 1}, self.ws("d"), "p", replay_of="o1", planner_t3_thread="planner-t3")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "bad3", {"g": 1}, self.ws("e"), "p", job_kind="benchmark", planner_t3_thread="planner-t3")
        # Identical resubmission is idempotent; a changed kind conflicts.
        self.assertEqual(core.submit(self.sd, "o1", {"g": 1}, self.ws("a"), "p", planner_t3_thread="planner-t3")["request_id"], "o1")
        with self.assertRaises(core.ConflictError):
            core.submit(self.sd, "o1", {"g": 1}, self.ws("a"), "p", job_kind="experiment", planner_t3_thread="planner-t3")

    def test_base_and_head_commit_from_git_workspace(self):
        ws = self.ws("g", git=True)
        head = subprocess.run(["git", "-C", ws, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
        job = core.submit(self.sd, "git1", {"g": 1}, ws, "p", planner_t3_thread="planner-t3")
        self.assertEqual(job["base_commit"], head)
        self.assertIsNone(job["head_commit"])
        plain = core.submit(self.sd, "plain", {"g": 1}, self.ws("h"), "p", planner_t3_thread="planner-t3")
        self.assertIsNone(plain["base_commit"])
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok', status='running' WHERE request_id='git1'")
        finally:
            con.close()
        done = core.complete(self.sd, "git1", "tok", "ok")
        self.assertEqual(done["head_commit"], head)
        view = core.result_view(self.sd, "git1")
        self.assertEqual((view["base_commit"], view["head_commit"], view["job_kind"]), (head, head, "ordinary"))
        self.assertEqual(view["measurements"], [])

    def test_events_carry_schema_version(self):
        core.submit(self.sd, "e1", {"g": 1}, self.ws("i"), "p", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            rows = con.execute("SELECT schema_version FROM events WHERE request_id='e1'").fetchall()
        finally:
            con.close()
        self.assertTrue(rows)
        self.assertEqual({r["schema_version"] for r in rows}, {store.SCHEMA_VERSION})
        self.assertEqual(store.SCHEMA_VERSION, 2)


if __name__ == "__main__":
    unittest.main()
