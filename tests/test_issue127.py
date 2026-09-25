"""Issue #127: single model lists outside the worker; Retry and Escalation switches.

Stdlib only, no live T3:

- dispatcher, reviewer, Retry (``correction``) and Escalation
  (``recovery``) read one ``models`` list; the worker keeps its lanes;
- an older snapshot without ``models`` still routes through the role's
  entry for the job's lane;
- ``enabled: false`` on Retry or Escalation drops its ladder rungs; both
  off sends the first failure straight to the planner question.
"""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, policy, t3snapshot  # noqa: E402
from tests.fakes import prism_provider, prism_snapshot  # noqa: E402
from tests import test_escalation as esc  # noqa: E402
from tests.test_t3snapshot import full_providers  # noqa: E402

GROK = {"instanceId": "grok", "model": "grok-4.6", "effort": "medium"}
OPUS = {"instanceId": "claudeAgent", "model": "claude-opus-5-5", "effort": "medium"}
OPUS_ROUTE = "t3:claudeAgent:claude-opus-5-5@medium"


def providers():
    return full_providers() + [prism_provider("claudeAgent", ["claude-opus-5-5"])]


class SingleLists(unittest.TestCase):
    def setUp(self):
        t3snapshot.reset()
        self.addCleanup(t3snapshot.reset)

    def test_non_worker_roles_read_models_and_the_worker_keeps_lanes(self):
        snap = prism_snapshot(
            providers(),
            lanes={"worker": {"hard": [OPUS]},
                   # Lanes that disagree with ``models`` lose.
                   "recovery": {"hard": [OPUS]}},
            kits={role: {"models": [GROK]} for role in
                  ("dispatcher", "reviewer", "correction", "recovery")})
        got = t3snapshot.stage_preferences(snap, "implementation_hard")
        for stage in ("dispatch", "review", "correction", "recovery"):
            self.assertEqual(got[stage], ["grok-4.6-build"], stage)
        self.assertEqual(got["implementation_hard"], [OPUS_ROUTE])
        self.assertNotIn("implementation_default", got)

    def test_empty_models_keeps_the_policy_order(self):
        snap = prism_snapshot(providers(), lanes={"recovery": {"medium": [OPUS]}},
                              kits={"recovery": {"models": []}})
        self.assertNotIn("recovery", t3snapshot.stage_preferences(snap, None))

    def test_old_snapshot_uses_the_jobs_lane_entry(self):
        snap = prism_snapshot(providers(), lanes={
            "correction": {"easy": [GROK], "medium": [OPUS]},
            "reviewer": {"hard": [GROK]}})
        self.assertEqual(t3snapshot.stage_preferences(snap, "implementation_small")["correction"],
                         ["grok-4.6-build"])
        got = t3snapshot.stage_preferences(snap, "implementation_default")
        self.assertEqual(got["correction"], [OPUS_ROUTE])
        self.assertNotIn("review", got)
        self.assertEqual(t3snapshot.stage_preferences(snap, "implementation_hard")["review"],
                         ["grok-4.6-build"])

    def test_enabled_flags_default_on(self):
        self.assertEqual(t3snapshot.ladder_enabled(None),
                         {"correction": True, "recovery": True})
        snap = prism_snapshot(providers(), kits={"correction": {"enabled": False},
                                                 "recovery": {"enabled": True}})
        self.assertEqual(t3snapshot.ladder_enabled(snap),
                         {"correction": False, "recovery": True})


class Switches(unittest.TestCase):
    # The escalation tests' job fixture, without rerunning their tests.
    job = esc.Ladder.job
    _events = esc.Ladder._events

    def setUp(self):
        esc.Ladder.setUp(self)
        t3snapshot.reset()
        self.addCleanup(t3snapshot.reset)

    def switch(self, **enabled):
        t3snapshot.apply(prism_snapshot(
            providers(), kits={role: {"enabled": on} for role, on in enabled.items()}))

    def rung_after(self, failures, **prior):
        esc._set_state(self.sd, "j", ladder={"failures": failures, **prior})
        out = controller._apply_ladder(self.sd, "j")
        return out, self.job()

    def asked_planner(self, out):
        self.assertEqual((out["action"], out["reason"]),
                         ("recovery-exhausted-question", "recovery_exhausted"))
        self.assertEqual(self.job()["status"], "question_pending")
        self.assertEqual([q["qid"] for q in core.list_questions(self.sd, "j")],
                         ["recovery-decision"])

    def test_retry_off_escalates_on_the_first_failure(self):
        self.switch(correction=False)
        out, j = self.rung_after(1)
        self.assertIsNone(out)
        self.assertEqual(controller._ladder(j)["rung"], "recovery")
        self.assertEqual(j["route"], policy.stage_routes("recovery")[0])
        self.assertTrue(controller._ladder(j)["escalated"])
        out, _ = self.rung_after(2, rung="recovery", escalated=True)
        self.asked_planner(out)

    def test_escalation_off_asks_the_planner_after_the_retries(self):
        self.switch(recovery=False)
        self.assertIsNone(self.rung_after(1)[0])
        self.assertEqual(controller._ladder(self.job())["rung"], "correction")
        out, j = self.rung_after(2, rung="correction")
        self.assertIsNone(out)
        self.assertEqual(controller._ladder(j)["rung"], "correction_fresh")
        self.assertFalse(controller._ladder(j)["escalated"])
        out, _ = self.rung_after(3, rung="correction_fresh")
        self.asked_planner(out)
        reasons = [json.loads(r["payload_json"])["reason"]
                   for r in self._events("route_switched")]
        self.assertNotIn("escalation", reasons)

    def test_both_off_sends_the_first_failure_to_the_planner(self):
        self.switch(correction=False, recovery=False)
        route = self.job()["route"]
        out, j = self.rung_after(1)
        self.asked_planner(out)
        self.assertEqual(j["route"], route)
        # After the planner's one authorized attempt fails, the job ends.
        esc._set_state(self.sd, "j", planner_recovery_authorized=True,
                   planner_recovery_used=True, recovery_question_qid="recovery-decision")
        out, j = self.rung_after(2, rung="recovery_directed", ran=[])
        self.assertEqual((out["action"], out["reason"]), ("failed", "escalation_exhausted"))
        err = json.loads(j["result_json"])["error"]
        self.assertEqual(err["message"], "initial turn failed")

    def test_retry_switched_off_mid_job_still_escalates(self):
        self.assertIsNone(self.rung_after(1)[0])
        out, j = self.rung_after(2, **{k: v for k, v in controller._ladder(self.job()).items()
                                       if k != "failures"})
        self.assertEqual(controller._ladder(j)["rung"], "correction_fresh")
        self.switch(correction=False)
        esc._set_state(self.sd, "j", ladder={**controller._ladder(j), "failures": 3})
        self.assertIsNone(controller._apply_ladder(self.sd, "j"))
        j = self.job()
        self.assertEqual(controller._ladder(j)["rung"], "recovery")
        self.assertEqual(j["route"], policy.stage_routes("recovery")[0])
        self.assertEqual(controller._ladder(j)["ran"],
                         ["correction", "correction_fresh", "recovery"])

    def test_retry_switched_on_after_an_escalation_never_escalates_twice(self):
        self.switch(correction=False)
        self.assertIsNone(self.rung_after(1)[0])
        self.assertEqual(controller._ladder(self.job())["rung"], "recovery")
        self.switch()
        esc._set_state(self.sd, "j", ladder={**controller._ladder(self.job()), "failures": 2})
        # Retry is on again but lies before the escalation: the planner decides.
        self.asked_planner(controller._apply_ladder(self.sd, "j"))
        reasons = [json.loads(r["payload_json"])["reason"]
                   for r in self._events("route_switched")]
        self.assertEqual(reasons.count("escalation"), 1)
        # The exhaustion evidence names the rungs that ran, not the switches.
        esc._set_state(self.sd, "j", planner_recovery_authorized=True,
                       planner_recovery_used=True, recovery_question_qid="recovery-decision")
        out, j = self.rung_after(3, rung="recovery_directed", escalated=True, ran=["recovery"])
        self.assertEqual(out["reason"], "escalation_exhausted")
        self.assertEqual(json.loads(j["result_json"])["error"]["message"],
                         "initial turn, one escalation failed")

    def test_a_rung_is_taken_once_per_failure(self):
        self.assertIsNone(self.rung_after(1)[0])
        self.assertIsNone(controller._apply_ladder(self.sd, "j"))
        self.assertEqual(controller._ladder(self.job())["rung"], "correction")


if __name__ == "__main__":
    unittest.main()
