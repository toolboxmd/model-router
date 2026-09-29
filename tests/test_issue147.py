"""Issue #147: the dispatcher reports router defects as Issues.

Stdlib only. The duty lives in the dispatcher's protocol text, which every
dispatcher prompt and resumed turn carries; these tests pin its parts: the
trigger, the evidence to read, the duplicate search, the report contents,
and the packet-mistake exception.
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters  # noqa: E402

P = adapters.LUNA_ACTION_PROTOCOL


class DispatcherReportsRouterDefects(unittest.TestCase):
    def test_duty_names_trigger_evidence_search_and_report(self):
        for part in (
            "when this job blocks, loops, or the runner acts against RUNNER.md",
            "the runner's status for this request ID and its "
            "outputs/<request ID>/ files",
            "search the open Issues in toolboxmd/model-router",
            "comment on the matching Issue or open one with the request ID, "
            "the evidence, and the expected behavior",
            "continue or block as the runner allows",
        ):
            with self.subTest(part=part):
                self.assertIn(part, P)

    def test_packet_mistakes_go_to_the_planner_not_an_issue(self):
        self.assertIn("A packet mistake (a missing proof, a wrong workspace) is "
                      "not a router defect: send it to the planner as a "
                      "planner_question, not an Issue.", P)

    def test_no_tool_calls_rule_allows_the_report(self):
        self.assertIn("Make no tool calls unless the task and ISSUES lack a fact "
                      "the routing decision needs, or you are reporting a "
                      "router defect.", P)

    def test_every_dispatcher_turn_carries_the_duty_before_the_reply_shapes(self):
        for text in (adapters.build_luna_prompt('{"goal": "t"}'),
                     adapters.build_luna_followup("ANSWER", "go")):
            with self.subTest(text=text[:20]):
                self.assertIn("ROUTER DEFECTS:", text)
                self.assertLess(text.index("ROUTER DEFECTS:"),
                                text.index("REPLY PROTOCOL"))


if __name__ == "__main__":
    unittest.main()
