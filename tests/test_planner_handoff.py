"""Issue #38: durable planner handoff summary.

Stdlib only. Submit stores the handoff summary (explicit, from the task
packet, or derived) with no planner mutation, and every planner question
posted into the planner's T3 thread carries it before the question.
"""
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, store  # noqa: E402
from tests.fakes import use_fake_t3  # noqa: E402

PY = sys.executable


class Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name="ws"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)


class SubmitSummary(Base):
    def test_submit_stores_explicit_summary(self):
        job = core.submit(self.sd, "h1", {"goal": "t"}, self.ws("a"),
                          "planner-1", handoff_summary="Keep the API shape.", planner_t3_thread="planner-t3")
        self.assertEqual(job["handoff_summary"], "Keep the API shape.")
        self.assertEqual(core.get_job(self.sd, "h1")["handoff_summary"],
                         "Keep the API shape.")

    def test_submit_derives_summary_from_packet(self):
        task = {"goal": "fix typo", "issue": "38",
                "decisions": "use policy data",
                "proof": "python3 -B -m unittest discover -s tests"}
        job = core.submit(self.sd, "h2", task, self.ws("b"), "planner-2", planner_t3_thread="planner-t3")
        summary = job["handoff_summary"]
        for needle in ("h2", "38", "use policy data", "unittest"):
            self.assertIn(needle, summary)

    def test_task_packet_handoff_summary_wins(self):
        task = {"goal": "t", "handoff_summary": "Packet summary."}
        job = core.submit(self.sd, "h3", task, self.ws("c"), "planner-3", planner_t3_thread="planner-t3")
        self.assertEqual(job["handoff_summary"], "Packet summary.")

    def test_planner_question_carries_summary_before_question(self):
        core.submit(self.sd, "h4", {"goal": "t"}, self.ws("d"),
                    "planner-4", handoff_summary="Decisions: ship it.", planner_t3_thread="planner-t3")
        fake = use_fake_t3(self, self.sd, "h4")
        fake.planner_replies = ["Yes."]
        res = controller.planner_callback(self.sd, "h4", "q1",
                                          "Ship now?")
        self.assertEqual(res["action"], "answered")
        prompt = [text for tid, text in fake.posts if tid == "planner-t3"][0]
        self.assertIn("HANDOFF SUMMARY", prompt)
        self.assertIn("Decisions: ship it.", prompt)
        self.assertIn("Ship now?", prompt)
        self.assertLess(prompt.index("Decisions: ship it."),
                        prompt.index("Ship now?"))

    def test_submit_and_start_creates_no_planner_mutation(self):
        # Submit with --start persists the job and launches the controller
        # without touching the planner session: no historical-kind row and
        # no planner-mutating command anywhere.
        calls = []

        def fake_spawn(cmd):
            calls.append(list(cmd))
            return 424242

        task = {"goal": "fix typo", "issue": "38",
                "decisions": "use policy data",
                "proof": "python3 -B -m unittest discover -s tests"}
        core.submit_and_start(self.sd, "c1", task, self.ws("a"),
                              "planner-c1", spawn=fake_spawn, planner_t3_thread="planner-t3")
        invs = [i for i in core._list_invocations(self.sd, "c1")
                if i["kind"] == "claude_compact"]
        self.assertEqual(invs, [])
        for cmd in calls:
            self.assertFalse(any("compact" in str(a).lower() for a in cmd),
                             cmd)


class HandoffMinors(Base):
    def test_null_summary_backfilled_on_resubmit(self):
        task = {"goal": "t", "issue": "38", "decisions": "d",
                "proof": "python3 -B -m unittest discover -s tests"}
        job = core.submit(self.sd, "null1", task, self.ws("n"),
                          "planner-n1", handoff_summary="Keep it.", planner_t3_thread="planner-t3")
        self.assertTrue((job["handoff_summary"] or "").strip())
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET handoff_summary=NULL WHERE request_id='null1'")
            con.commit()
        finally:
            con.close()
        job2 = core.submit(self.sd, "null1", task, self.ws("n"),
                           "planner-n1", handoff_summary="Keep it.", planner_t3_thread="planner-t3")
        self.assertTrue((job2["handoff_summary"] or "").strip())
        self.assertIn("Keep it.", job2["handoff_summary"])


if __name__ == "__main__":
    unittest.main()
