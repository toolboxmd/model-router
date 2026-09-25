"""Issue #128: the dispatcher supplies the proof command.

Stdlib only, no live T3. A fake dispatcher sends implementation envelopes
carrying ``payload.proof``: the runner binds the first one when the task
names none, keeps it against later envelopes and restarts, lets a task
``proof`` win, runs the bound proof after worker turns (a failing one
counts on the ladder) and for the baseline.
"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters, controller, core, store  # noqa: E402
from tests.fakes import isolate_t3_env, use_fake_t3  # noqa: E402

PLANNER = "planner-t3"


def setUpModule():
    isolate_t3_env()


def impl(proof):
    return {"action": "implementation", "artifact": "a1",
            "payload": {"instructions": "do it", "proof": proof}}


class Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def submit(self, rid, task, git=False):
        ws = self.base / rid
        ws.mkdir()
        if git:
            for args in (["init", "-q"], ["-c", "user.email=t@t", "-c", "user.name=t",
                                          "commit", "-q", "--allow-empty", "-m", "base"]):
                subprocess.run(["git", "-C", str(ws), *args], check=True)
        core.submit(self.sd, rid, task, str(ws), f"session-{rid}",
                    planner_t3_thread=PLANNER, handoff_summary=f"Summary for {rid}.",
                    lane="small")

    def fresh_dispatch(self, rid, replies, default):
        fake = use_fake_t3(self, self.sd, rid, replies=replies, default=default)
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state='{}', status='running'"
                        " WHERE request_id=?", (rid,))
            con.commit()
        finally:
            con.close()
        return fake

    def reports(self, rid):
        paths = sorted(store.job_dir_for(store.ensure_state_dir(self.sd), rid)
                       .glob("turn-*/report.json"))
        return [json.loads(p.read_text()) for p in paths]

    def state(self, rid):
        return controller._load_controller_state(core.get_job(self.sd, rid))


class DispatcherProof(Base):
    def test_first_envelope_binds_and_a_failing_proof_climbs_the_ladder(self):
        self.submit("b1", {"goal": "t"})
        # The first envelope names a failing proof; every later one tries
        # to swap in a passing command, which must not take.
        self.fresh_dispatch("b1", replies=[impl("false")], default=impl("true"))
        for _ in range(12):
            res = controller.step(self.sd, "b1")
            if res.get("action") in ("recovery-exhausted-question", "blocked"):
                break
        self.assertEqual(self.state("b1").get("dispatcher_proof"), "false")
        reports = self.reports("b1")
        self.assertGreaterEqual(len(reports), 2)
        for rep in reports:
            self.assertEqual(rep["proof_command"], "false")
            self.assertEqual(rep["proof_class"], "failed")
        ladder = controller._ladder(core.get_job(self.sd, "b1"))
        self.assertGreaterEqual(ladder["failures"], 2)

    def test_passing_bound_proof_is_reported(self):
        self.submit("p1", {"goal": "t"})
        self.fresh_dispatch("p1", replies=[impl("true")],
                            default={"action": "completion", "output": "done"})
        for _ in range(4):
            controller.step(self.sd, "p1")
        (rep,) = self.reports("p1")
        self.assertEqual((rep["proof_command"], rep["proof_class"],
                          rep["proof_exit_code"]), ("true", "pass", 0))

    def test_task_proof_wins(self):
        self.submit("t1", {"goal": "t", "proof": "true"})
        self.fresh_dispatch("t1", replies=[impl("false")],
                            default={"action": "completion", "output": "done"})
        for _ in range(4):
            controller.step(self.sd, "t1")
        self.assertNotIn("dispatcher_proof", self.state("t1"))
        rep = self.reports("t1")[0]
        self.assertEqual((rep["proof_command"], rep["proof_class"]), ("true", "pass"))

    def test_binding_survives_restart_and_later_envelopes(self):
        self.submit("r1", {"goal": "t"})
        use_fake_t3(self, self.sd, "r1")
        self.assertIsNone(controller._bind_dispatcher_proof(
            self.sd, "r1", {"proof": " make test "}))
        # A restarted controller reads the job back from the store.
        job = core.get_job(self.sd, "r1")
        self.assertEqual(controller._proof_command(job), "make test")
        controller._bind_dispatcher_proof(self.sd, "r1", {"proof": "true"})
        self.assertEqual(controller._proof_command(core.get_job(self.sd, "r1")),
                         "make test")

    def test_bound_proof_runs_the_skipped_baseline(self):
        self.submit("g1", {"goal": "t"}, git=True)
        use_fake_t3(self, self.sd, "g1")
        self.assertIsNone(controller._baseline_proof_gate(self.sd, "g1"))
        self.assertEqual(self.state("g1")["baseline_proof"],
                         {"skipped": "no proof command"})
        res = controller._bind_dispatcher_proof(self.sd, "g1", {"proof": "false"})
        self.assertEqual(res, {"action": "blocked", "reason": "baseline_proof_failed"})
        self.assertEqual(self.state("g1")["baseline_proof"]["rc"], 1)

    def test_proof_bound_after_a_worker_turn_skips_the_baseline(self):
        self.submit("l1", {"goal": "t"}, git=True)
        use_fake_t3(self, self.sd, "l1")
        controller._baseline_proof_gate(self.sd, "l1")
        # A worker turn already ran: its report exists.
        turn = store.job_dir_for(store.ensure_state_dir(self.sd), "l1") / "turn-1"
        turn.mkdir(parents=True)
        (turn / "report.json").write_text(json.dumps({"seq": 1, "proof_class": "none"}))
        self.assertIsNone(controller._bind_dispatcher_proof(
            self.sd, "l1", {"proof": "false"}))
        self.assertEqual(self.state("l1")["baseline_proof"],
                         {"skipped": "bound after worker turns"})
        self.assertNotEqual(core.get_job(self.sd, "l1")["status"], "blocked")

    def test_replay_after_a_crash_before_the_gate_runs_the_baseline(self):
        self.submit("c1", {"goal": "t"}, git=True)
        use_fake_t3(self, self.sd, "c1")
        controller._baseline_proof_gate(self.sd, "c1")
        # The crash left the binding persisted without a baseline result.
        controller._set_phase(self.sd, "c1", dispatcher_proof="false",
                              baseline_proof=None)
        res = controller._bind_dispatcher_proof(self.sd, "c1", {"proof": "true"})
        self.assertEqual(res, {"action": "blocked", "reason": "baseline_proof_failed"})
        self.assertEqual(self.state("c1")["dispatcher_proof"], "false")
        self.assertEqual(self.state("c1")["baseline_proof"]["rc"], 1)

    def test_dispatcher_prompt_asks_for_payload_proof(self):
        self.assertIn('"proof":"<the target project', adapters.LUNA_ACTION_PROTOCOL)
        self.assertIn("On the first implementation action, set payload.proof",
                      adapters.LUNA_ACTION_PROTOCOL)


if __name__ == "__main__":
    unittest.main()
