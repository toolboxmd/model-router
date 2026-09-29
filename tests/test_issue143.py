"""Issue 143: a PR merged by the job blocks once, clearly, instead of looping.

The runner never merges a PR (RUNNER.md step 6). When a job's PR is
already merged at completion, no dispatcher turn can restore an open PR,
so resuming the dispatcher with "update the existing open PR" loops on
``pr_not_open``. A merged PR now refuses as ``pr_merged`` naming the
merge commit and blocks the job at once for the planner; a closed,
unmerged PR keeps the existing once-then-block refusal.
"""
import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import controller, core  # noqa: E402
from tests.fakes import use_fake_t3  # noqa: E402
from tests import test_issue87 as t87  # noqa: E402
from tests.test_issue87 import set_controller_state, sql, stub_pr_verifier  # noqa: E402

PR = "https://github.com/example/repo/pull/7"
MERGE = "1e6bb1d7a0000000000000000000000000000000"


def setUpModule():
    t87.setUpModule()


class MergedPrRefusal(unittest.TestCase):
    def test_merged_pr_refuses_as_pr_merged_with_merge_commit(self):
        seen = {"ok": True, "state": "MERGED", "is_draft": False,
                "head_sha": "abc123", "repo": "example/repo",
                "merge_commit": MERGE}
        reason = core.verify_pr_for_completion(
            "/tmp", PR, "abc123", json.dumps({"goal": "x"}),
            verifier=stub_pr_verifier(seen))
        self.assertIn("completion_refused: pr_merged", reason)
        self.assertIn(MERGE[:12], reason)
        self.assertNotIn("pr_not_open", reason)

    def test_closed_unmerged_pr_still_refuses_as_not_open(self):
        seen = {"ok": True, "state": "CLOSED", "is_draft": False,
                "head_sha": "abc123", "repo": "example/repo"}
        reason = core.verify_pr_for_completion(
            "/tmp", PR, "abc123", json.dumps({"goal": "x"}),
            verifier=stub_pr_verifier(seen))
        self.assertIn("pr_not_open", reason)
        self.assertNotIn("pr_merged", reason)

    def test_default_verifier_reads_merge_commit(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        gh = Path(tmp.name) / "gh"
        gh.write_text("#!/bin/sh\n"
                      "echo '{\"state\":\"MERGED\",\"isDraft\":false,"
                      "\"headRefOid\":\"abc123\",\"url\":\"" + PR + "\","
                      "\"mergeCommit\":{\"oid\":\"" + MERGE + "\"}}'\n")
        gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
        old = os.environ.get("PATH", "")
        os.environ["PATH"] = tmp.name + os.pathsep + old
        self.addCleanup(os.environ.__setitem__, "PATH", old)
        seen = core.default_pr_verifier(None, PR)
        self.assertEqual(seen["state"], "MERGED")
        self.assertEqual(seen["merge_commit"], MERGE)


class MergedPrBlocksWithoutLoop(unittest.TestCase):
    _git_ws = t87.CompletionBinding._git_ws
    _passing_report = t87.CompletionBinding._passing_report

    def _job(self, rid):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = self._git_ws(base, "ws", with_origin=True)
        core.submit(sd, rid, {"goal": "x", "proof": "true"}, str(ws), "p",
                    planner_t3_thread="planner-t3")
        self._passing_report(sd, rid, ws)
        sql(sd, "UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),"
                "'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp',"
                "'route','luna/max')), status='running' WHERE request_id=?", (rid,))
        fake = use_fake_t3(self, sd, rid, default={"action": "completion",
                                                   "output": "STILL DONE"})
        set_controller_state(sd, rid, seq=1,
                             last_action={"action": "completion", "output": "DONE",
                                          "pr_url": PR},
                             last_action_name="completion")
        return sd, ws, fake

    def _verifier(self, ws, state, **extra):
        saved = core.PR_VERIFIER
        self.addCleanup(setattr, core, "PR_VERIFIER", saved)
        core.PR_VERIFIER = stub_pr_verifier(dict(
            {"ok": True, "state": state, "is_draft": False,
             "head_sha": core._workspace_head(str(ws)), "repo": "example/repo"},
            **extra))

    def test_merged_pr_blocks_at_once_without_resuming_dispatcher(self):
        sd, ws, fake = self._job("merged")
        self._verifier(ws, "MERGED", merge_commit=MERGE)
        commands_before = len(fake.commands)
        out = controller.step(sd, "merged")
        self.assertEqual(out["action"], "blocked")
        job = core.get_job(sd, "merged")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("pr_merged", job["block_reason"])
        self.assertIn(MERGE[:12], job["block_reason"])
        self.assertNotIn("pr_not_open", job["block_reason"])
        # No resume: the dispatcher is not asked to restore an open PR;
        # the planner gets one terminal report naming the merge.
        sent = fake.commands[commands_before:]
        self.assertFalse([c for c in sent
                          if c.get("threadId") == "sub.planner-t3.disp"])
        reports = [c for c in sent if c.get("threadId") == "planner-t3"]
        self.assertEqual(len(reports), 1)
        self.assertIn("pr_merged", reports[0]["message"]["text"])
        self.assertIsNone(core.known_pr_url(sd, "merged"))

    def test_closed_pr_still_resumes_dispatcher_once(self):
        sd, ws, _fake = self._job("closed")
        self._verifier(ws, "CLOSED")
        out = controller.step(sd, "closed")
        self.assertEqual(out["action"], "completion-refused-resumed")


if __name__ == "__main__":
    unittest.main()
