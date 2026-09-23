"""#71: every Codex invocation of a job shares one sessions directory, so the
dispatcher resume finds the thread its dispatch wrote."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from runner import harnesses, kits
from tests.test_kits import make_fake_homes

THREAD = "01a0cf28-4cdf-7893-9fc2-5cee2ccb2d61"


class CodexSessionsShared(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        homes = make_fake_homes(self.base)
        saved = {k: os.environ.get(k) for k in homes}
        os.environ.update(homes)
        self.addCleanup(lambda: [os.environ.pop(k, None) if v is None
                                 else os.environ.__setitem__(k, v)
                                 for k, v in saved.items()])
        self.state = self.base / "state"
        (self.state / "outputs").mkdir(parents=True)

    def inv(self, invocation_id, kind, cmd):
        return {"request_id": "job-71", "invocation_id": invocation_id, "kind": kind,
                "cmd_json": json.dumps(cmd),
                "stdout_path": str(self.state / "outputs" / f"job-71.{invocation_id}.stdout"),
                "meta_json": json.dumps({"stage": "dispatch", "route": "luna/max"})}

    def spawn(self, inv):
        env, _ = harnesses.harness_named("codex").spawn_spec(inv, {})
        return Path(env["CODEX_HOME"])

    def test_dispatch_and_resume_kits_share_one_sessions_directory(self):
        dispatch_home = self.spawn(self.inv("d1", "codex_dispatch",
                                            ["codex", "exec", "--json", "prompt"]))
        rollout = dispatch_home / "sessions" / "2026" / "09" / "23" / f"rollout-x-{THREAD}.jsonl"
        rollout.parent.mkdir(parents=True)
        rollout.write_text('{"type": "session_meta"}\n')
        resume_home = self.spawn(self.inv("r1", "codex_resume",
                                          ["codex", "exec", "resume", THREAD, "--json"]))
        self.assertNotEqual(dispatch_home, resume_home)
        found = list((resume_home / "sessions").rglob(f"rollout-*-{THREAD}.jsonl"))
        self.assertEqual(len(found), 1)
        self.assertEqual((dispatch_home / "sessions").resolve(),
                         (resume_home / "sessions").resolve())
        self.assertEqual((resume_home / "sessions").resolve(),
                         kits.codex_sessions_dir_for(self.state, "job-71").resolve())

    def test_resume_adopts_a_rollout_written_before_sessions_were_shared(self):
        self.ledger([("job-71", "d0")])
        legacy = self.state / "kits" / "job-71.d0.dispatcher" / "sessions" / "2026" / "09" / "23"
        legacy.mkdir(parents=True)
        (legacy / f"rollout-x-{THREAD}.jsonl").write_text('{"type": "session_meta"}\n')
        resume_home = self.spawn(self.inv("r2", "codex_resume",
                                          ["codex", "exec", "resume", THREAD, "--json"]))
        found = list((resume_home / "sessions").rglob(f"rollout-*-{THREAD}.jsonl"))
        self.assertEqual(len(found), 1)
        # The earlier kit keeps its copy as evidence.
        self.assertTrue((legacy / f"rollout-x-{THREAD}.jsonl").is_file())

    def test_request_ids_that_sanitize_alike_get_separate_directories(self):
        self.assertNotEqual(kits.codex_sessions_dir_for(self.state, "a/b"),
                            kits.codex_sessions_dir_for(self.state, "a_b"))

    def test_adoption_takes_only_this_jobs_own_kits(self):
        # "a/b" and "a_b" share the sanitized kit prefix "a_b"; only the
        # invocation the ledger records for "a/b" may be adopted from.
        self.ledger([("a_b", "inv-other")])
        other = self.state / "kits" / "a_b.inv-other.dispatcher" / "sessions"
        other.mkdir(parents=True)
        (other / f"rollout-x-{THREAD}.jsonl").write_text("{}\n")
        shared = kits.codex_sessions_dir_for(self.state, "a/b")
        self.assertIsNone(kits.adopt_codex_thread(self.state, "a/b", THREAD, shared))
        self.ledger([("a/b", "inv-mine")])
        mine = self.state / "kits" / "a_b.inv-mine.dispatcher" / "sessions"
        mine.mkdir(parents=True)
        (mine / f"rollout-y-{THREAD}.jsonl").write_text("{}\n")
        adopted = kits.adopt_codex_thread(self.state, "a/b", THREAD, shared)
        self.assertEqual(adopted.name, f"rollout-y-{THREAD}.jsonl")

    def ledger(self, rows):
        con = sqlite3.connect(self.state / "jobs.db")
        con.execute("CREATE TABLE IF NOT EXISTS invocations (invocation_id TEXT, request_id TEXT)")
        con.executemany("INSERT INTO invocations VALUES(?, ?)", [(i, r) for r, i in rows])
        con.commit()
        con.close()

    def test_other_jobs_and_unknown_threads_are_not_adopted(self):
        other = self.state / "kits" / "job-99.d0.dispatcher" / "sessions"
        other.mkdir(parents=True)
        (other / f"rollout-x-{THREAD}.jsonl").write_text("{}\n")
        resume_home = self.spawn(self.inv("r3", "codex_resume",
                                          ["codex", "exec", "resume", THREAD, "--json"]))
        self.assertEqual(list((resume_home / "sessions").rglob("*.jsonl")), [])
        self.assertIsNone(kits.adopt_codex_thread(self.state, "job-71", "", self.base))


if __name__ == "__main__":
    unittest.main()
