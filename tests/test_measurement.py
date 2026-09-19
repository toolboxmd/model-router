"""Measurement and provenance: job fields, invocation records, reports."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import core, policy, store  # noqa: E402


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
        job = core.submit(self.sd, "o1", {"g": 1}, self.ws("a"), "p")
        self.assertEqual((job["job_kind"], job["replay_of"], job["planner_harness"]), ("ordinary", None, "claude"))
        job = core.submit(self.sd, "r1", {"g": 1}, self.ws("b"), "p", job_kind="replay", replay_of="o1",
                          planner_harness="codex")
        self.assertEqual((job["job_kind"], job["replay_of"], job["planner_harness"]), ("replay", "o1", "codex"))
        with self.assertRaises(ValueError):
            core.submit(self.sd, "bad1", {"g": 1}, self.ws("c"), "p", job_kind="replay")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "bad2", {"g": 1}, self.ws("d"), "p", replay_of="o1")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "bad3", {"g": 1}, self.ws("e"), "p", job_kind="benchmark")
        # Identical resubmission is idempotent; a changed kind conflicts.
        self.assertEqual(core.submit(self.sd, "o1", {"g": 1}, self.ws("a"), "p")["request_id"], "o1")
        with self.assertRaises(core.ConflictError):
            core.submit(self.sd, "o1", {"g": 1}, self.ws("a"), "p", job_kind="experiment")

    def test_base_and_head_commit_from_git_workspace(self):
        ws = self.ws("g", git=True)
        head = subprocess.run(["git", "-C", ws, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
        job = core.submit(self.sd, "git1", {"g": 1}, ws, "p")
        self.assertEqual(job["base_commit"], head)
        self.assertIsNone(job["head_commit"])
        plain = core.submit(self.sd, "plain", {"g": 1}, self.ws("h"), "p")
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
        core.submit(self.sd, "e1", {"g": 1}, self.ws("i"), "p")
        con = store.connect(self.sd)
        try:
            rows = con.execute("SELECT schema_version FROM events WHERE request_id='e1'").fetchall()
        finally:
            con.close()
        self.assertTrue(rows)
        self.assertEqual({r["schema_version"] for r in rows}, {store.SCHEMA_VERSION})
        self.assertEqual(store.SCHEMA_VERSION, 2)


class Measures(unittest.TestCase):
    def test_measure_codex_output(self):
        out = "\n".join(json.dumps(o) for o in [
            {"type": "thread.started", "thread_id": "thr_1"},
            {"type": "item.completed", "item": {"id": "item_7", "type": "agent_message", "text": "{}"}},
            {"type": "turn.completed", "usage": {"input_tokens": 12, "cached_input_tokens": 3,
                                                  "output_tokens": 5, "reasoning_output_tokens": 2}}])
        usage, observed, ids = core.measure_output("codex_dispatch", out, "")
        self.assertEqual(usage, {"source": "codex", "input_tokens": 12, "cached_input_tokens": 3,
                                 "output_tokens": 5, "reasoning_output_tokens": 2})
        self.assertEqual(ids["thread_id"], "thr_1")
        self.assertEqual(ids["agent_message_ids"], ["item_7"])
        self.assertIsNone(observed)

    def test_measure_claude_output(self):
        out = json.dumps({"type": "result", "subtype": "success", "is_error": False, "result": "yes",
                          "session_id": "s1", "uuid": "u1", "duration_ms": 40, "num_turns": 1,
                          "total_cost_usd": 0.01, "usage": {"input_tokens": 9, "output_tokens": 2}})
        usage, observed, ids = core.measure_output("claude_callback", out, "", {"prompt_sha256": "abc"})
        self.assertEqual(usage["source"], "claude")
        self.assertEqual((usage["input_tokens"], usage["duration_ms"], usage["total_cost_usd"]), (9, 40, 0.01))
        self.assertEqual(ids, {"session_id": "s1", "uuid": "u1", "prompt_sha256": "abc"})

    def test_measure_opencode_control_output(self):
        summary = {"usage": {"source": "opencode", "messages": [{"id": "msg_1", "tokens": {"input": 1}}]},
                   "native_ids": {"session_id": "ses_1", "assistant_message_ids": ["msg_1"]},
                   "actual_model": {"providerID": "opencode-go", "modelID": "glm-5.3", "variant": None}}
        out = "junk\nRUNNER_RESULT " + json.dumps(summary) + "\n"
        usage, observed, ids = core.measure_output("opencode_control", out, "")
        self.assertEqual(usage["messages"][0]["id"], "msg_1")
        self.assertEqual(observed, "opencode-go/glm-5.3")
        self.assertEqual(ids["session_id"], "ses_1")

    def test_terminal_classes(self):
        self.assertEqual(core.terminal_class_for(0, {}), "completed")
        self.assertEqual(core.terminal_class_for(3, {"signal": "exhausted"}), "quota")
        self.assertEqual(core.terminal_class_for(4, {"signal": "overloaded"}), "overloaded")
        self.assertEqual(core.terminal_class_for(1, {"signal": "hard"}), "hard_error")
        self.assertEqual(core.terminal_class_for(124, {}), "timeout")
        self.assertEqual(core.terminal_class_for(143, {}), "cancelled")
        self.assertEqual(core.terminal_class_for(None, None, crashed=True), "crashed")
        self.assertEqual(core.terminal_class_for(2, None), "failed")


if __name__ == "__main__":
    unittest.main()
