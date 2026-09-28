"""Start T3 when it stays unreachable (toolboxmd/model-router#140).

After 60 continuous seconds without an answer the job runs the launch
command once, then again every 5 minutes while T3 stays down. A failed
launch is recorded and the wait goes on; an empty ``T3_LAUNCH_CMD``
turns launching off.
"""
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core  # noqa: E402
from tests.test_issue133 import Base  # noqa: E402
from tests.fakes import default_prism, isolate_t3_env  # noqa: E402


def setUpModule():
    isolate_t3_env()


def launch_events(sd, rid):
    return [json.loads(e["payload_json"]) for e in core.status_view(sd, rid)["recent_events"]
            if e["kind"] == "launching_t3"]


def stop_step(steps):
    def step(state_dir, request_id, token=None):
        steps.append(1)
        return {"action": "blocked", "reason": "test_stop"}
    return step


class Launch(Base):
    def setUp(self):
        super().setUp()
        patcher = mock.patch.dict("os.environ", {"T3_LAUNCH_CMD": "start-t3 --quiet"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_no_launch_before_60_seconds(self):
        self.submit()
        steps = []
        # Rereads at 5, 15, 35 seconds; back at 35.
        self.run_controller("j", default_prism(), reads=[None] * 4, step=stop_step(steps))
        self.assertEqual(self.slept, [5.0, 10.0, 20.0, 40.0])
        self.assertEqual(self.launched, [])
        self.assertEqual(launch_events(self.sd, "j"), [])
        self.assertEqual(steps, [1])

    def test_launch_after_60_seconds_then_continue_once_back(self):
        self.submit()
        steps = []
        self.run_controller("j", default_prism(), reads=[None] * 6, step=stop_step(steps))
        # Unreachable at 0, 5, 15, 35, 75: one launch at 75, back at 135.
        self.assertEqual(self.launched, [(75.0, "start-t3 --quiet")])
        self.assertEqual(steps, [1])
        self.assertNotIn("Prism", core.get_job(self.sd, "j")["block_reason"] or "")
        events = launch_events(self.sd, "j")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["command"], "start-t3 --quiet")
        self.assertEqual(events[0]["result"], "ok")
        self.assertTrue(events[0]["at"])

    def test_relaunch_every_5_minutes_while_down(self):
        self.submit()
        steps = []
        self.run_controller("j", default_prism(), reads=[None] * 20, step=stop_step(steps))
        times = [t for t, _ in self.launched]
        self.assertEqual(times, [75.0, 375.0, 675.0, 975.0])
        self.assertEqual(steps, [1])

    def test_a_failed_launch_is_recorded_and_the_wait_goes_on(self):
        self.submit()
        steps = []
        self.launch_result = lambda command: "FileNotFoundError: start-t3"
        self.run_controller("j", default_prism(), reads=[None] * 12, step=stop_step(steps))
        self.assertEqual([t for t, _ in self.launched], [75.0, 375.0])
        self.assertEqual([e["result"] for e in launch_events(self.sd, "j")],
                         ["FileNotFoundError: start-t3"] * 2)
        self.assertEqual(steps, [1])
        self.assertNotIn("Prism", core.get_job(self.sd, "j")["block_reason"] or "")

    def test_empty_command_turns_launching_off(self):
        self.submit()
        steps = []
        with mock.patch.dict("os.environ", {"T3_LAUNCH_CMD": "  "}):
            self.run_controller("j", default_prism(), reads=[None] * 20, step=stop_step(steps))
        self.assertEqual(self.launched, [])
        self.assertEqual(launch_events(self.sd, "j"), [])
        self.assertEqual(steps, [1])


class Command(unittest.TestCase):
    def test_default_is_chromeria_on_macos_and_off_elsewhere(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch.object(controller.sys, "platform", "darwin"):
                self.assertEqual(controller._t3_launch_command(), "/usr/bin/open -g -a Chromeria")
            with mock.patch.object(controller.sys, "platform", "linux"):
                self.assertEqual(controller._t3_launch_command(), "")
        with mock.patch.dict("os.environ", {"T3_LAUNCH_CMD": "x y"}):
            self.assertEqual(controller._t3_launch_command(), "x y")

    def test_launch_runs_without_a_shell_and_reports_failures(self):
        self.assertEqual(controller._launch_t3(f"{sys.executable} -c pass"), "ok")
        self.assertTrue(controller._launch_t3(
            f"{sys.executable} -c 'import sys; sys.exit(3)'").startswith("exit 3"))
        self.assertTrue(controller._launch_t3("/nonexistent/start-t3").startswith(
            "FileNotFoundError"))
        self.assertTrue(controller._launch_t3("'unbalanced").startswith("ValueError"))
        # No shell: a metacharacter is an argument, not a command separator.
        self.assertEqual(controller._launch_t3(f"{sys.executable} -c pass ; false"), "ok")
        with mock.patch.object(controller, "T3_LAUNCH_TIMEOUT_SECS", 0.2):
            self.assertTrue(controller._launch_t3(
                f"{sys.executable} -c 'import time; time.sleep(5)'").startswith("TimeoutExpired"))


if __name__ == "__main__":
    unittest.main()
