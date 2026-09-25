"""Issue #116 (#111 fixes 4 and 5): baseline proof and correction fallback.

Stdlib only, no live T3. Real temporary Git repositories prove the base
commit check; the in-memory T3 fake proves no thread starts before it.

- A proof that already fails on the base commit blocks before dispatch
  with the failing tail; a passing base proceeds and never reruns, also
  across a controller restart; a dirty workspace is proved in a scratch
  worktree that is removed afterwards.
- A capacity signal on the correction route moves into the job's lane
  instead of blocking; with nothing eligible it still blocks with the
  true reason.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, store, t3snapshot  # noqa: E402
from tests.fakes import use_fake_t3  # noqa: E402

PLANNER = "planner-t3"
GIT_ENV = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@x")


def git(ws, *args):
    subprocess.run(["git", "-C", str(ws), *args], check=True, env=GIT_ENV,
                   capture_output=True)


class Base(unittest.TestCase):
    def setUp(self):
        t3snapshot.reset()
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def repo(self, name, passing=True):
        ws = self.base / name
        ws.mkdir()
        git(ws, "init", "-q")
        (ws / "ok.txt").write_text("yes\n" if passing else "no\n")
        git(ws, "add", "ok.txt")
        git(ws, "commit", "-q", "-m", "base")
        return ws

    def submit(self, rid, ws, proof, **task):
        self.runs = self.base / f"{rid}.runs"
        cmd = f"echo run >> {self.runs} && {proof}"
        return core.submit(self.sd, rid, {"goal": "t", "proof": cmd, **task}, str(ws),
                           f"s-{rid}", planner_t3_thread=PLANNER)

    def runs_count(self):
        return len(self.runs.read_text().splitlines()) if self.runs.exists() else 0


class BaselineProof(Base):
    def test_failing_base_blocks_before_any_thread(self):
        ws = self.repo("fail", passing=False)
        self.submit("b1", ws, "grep -q yes ok.txt || (echo FAIL: base is red; exit 3)")
        fake = use_fake_t3(self, self.sd, "b1")
        # A fresh job: no dispatcher thread saved yet.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state='{}' WHERE request_id='b1'")
        finally:
            con.close()
        res = controller.step(self.sd, "b1")
        self.assertEqual(res["reason"], "baseline_proof_failed")
        job = core.get_job(self.sd, "b1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("baseline_proof_failed: rc=3", job["block_reason"])
        self.assertIn("base is red", job["block_reason"])
        self.assertEqual([c for c in fake.commands if c["type"] == "thread.create"], [])
        rec = controller._load_controller_state(job)["baseline_proof"]
        self.assertEqual((rec["rc"], rec["where"]), (3, "workspace"))
        self.assertTrue(Path(rec["log"]).exists())

    def test_passing_base_proceeds_once_across_restarts(self):
        ws = self.repo("pass")
        self.submit("b2", ws, "grep -q yes ok.txt")
        fake = use_fake_t3(self, self.sd, "b2",
                           default={"action": "planner_question", "qid": "q1",
                                    "prompt": "which?"})
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state='{}' WHERE request_id='b2'")
        finally:
            con.close()
        first = controller.dispatch(self.sd, "b2")
        self.assertEqual(first["action"], "dispatched", first)
        self.assertEqual(self.runs_count(), 1)
        self.assertTrue(any(c["type"] == "thread.create" for c in fake.commands))
        # A restarted controller re-entering dispatch adopts the recorded
        # baseline and the saved dispatcher thread: no second proof run.
        controller.dispatch(self.sd, "b2")
        controller._baseline_proof_gate(self.sd, "b2")
        self.assertEqual(self.runs_count(), 1)

    def test_dirty_workspace_uses_a_scratch_worktree_and_removes_it(self):
        ws = self.repo("dirty")
        (ws / "ok.txt").write_text("no\n")  # uncommitted planner work
        self.submit("b3", ws, "grep -q yes ok.txt")
        self.assertIsNone(controller._baseline_proof_gate(self.sd, "b3"))
        rec = controller._load_controller_state(core.get_job(self.sd, "b3"))["baseline_proof"]
        self.assertEqual((rec["rc"], rec["where"]), (0, "scratch"))
        out = subprocess.run(["git", "-C", str(ws), "worktree", "list"], capture_output=True,
                             text=True).stdout
        self.assertEqual(len(out.strip().splitlines()), 1, out)
        self.assertEqual((ws / "ok.txt").read_text(), "no\n", "the user's work is untouched")

    def test_skips_are_recorded(self):
        ws = self.base / "plain"
        ws.mkdir()
        core.submit(self.sd, "s1", {"goal": "t", "proof": "true"}, str(ws), "s",
                    planner_t3_thread=PLANNER)
        self.assertIsNone(controller._baseline_proof_gate(self.sd, "s1"))
        rec = controller._load_controller_state(core.get_job(self.sd, "s1"))["baseline_proof"]
        self.assertEqual(rec, {"skipped": "no base commit"})
        ws2 = self.repo("optout", passing=False)
        self.submit("s2", ws2, "grep -q yes ok.txt", baseline_proof=False)
        self.assertIsNone(controller._baseline_proof_gate(self.sd, "s2"))
        self.assertEqual(self.runs_count(), 0)


class DispatcherEvidence(Base):
    def test_evidence_carries_the_branch_commits(self):
        ws = self.repo("ev")
        git(ws, "checkout", "-q", "-b", "fix/ev")
        self.submit("e1", ws, "true")
        (ws / "fix.txt").write_text("done\n")
        git(ws, "add", "fix.txt")
        git(ws, "commit", "-q", "-m", "fix: the thing")
        job = core.get_job(self.sd, "e1")
        text = controller._implementation_evidence(job, {"action": "implementation_ok",
                                                         "report": {}})
        fields = json.loads(text.splitlines()[0])
        self.assertEqual(fields["branch"], "fix/ev")
        self.assertEqual(len(fields["branch_commits"]), 1)
        self.assertIn("fix: the thing", fields["branch_commits"][0])
        self.assertIn("may already be done", text)


class CorrectionFallback(Base):
    def _job_on_correction(self, rid, lane="default"):
        ws = self.base / rid
        ws.mkdir()
        core.submit(self.sd, rid, {"goal": "t"}, str(ws), "s", lane=lane,
                    planner_t3_thread=PLANNER)
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route='kimi-k2.7-code-go', status='running'"
                        " WHERE request_id=?", (rid,))
        finally:
            con.close()

    def _signal(self, rid, signal):
        job = core.get_job(self.sd, rid)
        return controller._finish_worker_turn(
            self.sd, rid, job, "kimi-k2.7-code-go", 2, None,
            {"ok": False, "rc": 1, "signal": signal, "quota": signal == "exhausted",
             "signal_evidence": {"message": signal}, "error": signal,
             "idle_confirmed": True},
            "sub.planner-t3.c1", 1, source=controller.T3_TURN_KIND)

    def test_exhaustion_on_correction_moves_into_the_lane(self):
        self._job_on_correction("c1")
        res = self._signal("c1", "exhausted")
        # Go rests as a whole, so the lane's Zen free route takes over.
        self.assertEqual(res["action"], "route_switched", res)
        self.assertEqual(core.get_job(self.sd, "c1")["route"], "muse-spark-xhigh-free")

    def test_overload_on_correction_moves_to_another_family(self):
        self._job_on_correction("c2")
        res = self._signal("c2", "overloaded")
        self.assertEqual(res["action"], "route_switched", res)
        self.assertEqual(core.get_job(self.sd, "c2")["route"], "muse-spark-xhigh-free")

    def test_nothing_eligible_still_blocks_with_the_reason(self):
        self._job_on_correction("c3")
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "exhausted",
                             {"message": "Free usage exceeded"},
                             reset_at=core.degraded_until(), reset_source="provider")
        res = self._signal("c3", "exhausted")
        self.assertEqual(res["reason"], "capacity_exhausted")
        self.assertIn("after exhausted on kimi-k2.7-code-go",
                      core.get_job(self.sd, "c3")["block_reason"])


if __name__ == "__main__":
    unittest.main()
