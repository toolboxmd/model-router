"""Recovery onto the installed runtime retains incompatible state.

Issue 75: recovery continues on the currently installed runtime only
when the stored job state is explicitly compatible with it; a foreign
policy or unreadable controller state blocks with its specific reason
and never starts execution, and finished jobs are never assessed.
"""
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import core, store  # noqa: E402


def sql(sd, q, args=()):
    con = store.connect(sd)
    try:
        con.execute(q, args)
    finally:
        con.close()


def events(sd, request_id):
    con = store.connect(sd)
    try:
        return [dict(r) for r in con.execute(
            "SELECT kind, payload_json FROM events WHERE request_id=? ORDER BY id",
            (request_id,)).fetchall()]
    finally:
        con.close()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.sd = str(self.base / "state")


    def ws(self, name="w"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)


class IncompatibleReplacement(Base):
    def _recover_blocked(self, rid):
        real_start = core.start_controller
        core.start_controller = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("incompatible state must not start execution"))
        try:
            return core.recover_one(self.sd, rid)
        finally:
            core.start_controller = real_start

    def test_foreign_policy_is_retained_with_its_reason(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p",
                    policy_id="other-policy-v9", planner_t3_thread="planner-t3")
        before = core.get_job(self.sd, "r1")
        out = self._recover_blocked("r1")
        self.assertEqual(out["action"], "blocked-incompatible-runtime")
        job = core.get_job(self.sd, "r1")
        self.assertTrue(job["block_reason"].startswith("runtime_incompatible:"))
        self.assertIn("policy_incompatible", job["block_reason"])
        self.assertEqual((job["route"], job["task_json"], job["policy_id"]),
                         (before["route"], before["task_json"], "other-policy-v9"))

    def test_unreadable_controller_state_is_retained_with_its_reason(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        before = core.get_job(self.sd, "r1")
        sql(self.sd, "UPDATE jobs SET controller_state='not-json{{' WHERE request_id='r1'")
        out = self._recover_blocked("r1")
        self.assertEqual(out["action"], "blocked-incompatible-runtime")
        job = core.get_job(self.sd, "r1")
        self.assertIn("controller_state_unreadable", job["block_reason"])
        self.assertEqual((job["route"], job["task_json"]), (before["route"], before["task_json"]))


class CompatibilityAdoptsHealthyWork(Base):
    """Finding 3: the compatibility check never stops healthy owned work.

    An incompatible replacement still blocks with its specific reason
    when no controller or child is alive, but a live child or an
    advertised healthy controller is adopted first.
    """


    def test_terminal_jobs_are_not_assessed(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws(), "p", planner_t3_thread="planner-t3")
        sql(self.sd, "UPDATE jobs SET status='succeeded' WHERE request_id='r1'")
        out = core.recover_one(self.sd, "r1")
        self.assertEqual(out["action"], "noop-terminal")
        after = [e["kind"] for e in events(self.sd, "r1")]
        self.assertNotIn("runtime_assessed", after,
                         "recover --all must not assess finished jobs")
        self.assertNotIn("runtime_recovered", after)


if __name__ == "__main__":
    unittest.main()
