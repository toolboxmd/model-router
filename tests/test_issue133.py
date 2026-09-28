"""Route only from Prism (toolboxmd/model-router#133).

The policy names no models. An unreadable Prism snapshot, or an empty
list the job needs, stops the job before any thread starts and reports
the cause to the planner through the blocked terminal report.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from unittest import mock  # noqa: E402

from runner import controller, core, policy, t3exec, t3snapshot  # noqa: E402
from tests.fakes import (default_prism, install_prism, isolate_t3_env,  # noqa: E402
                         use_fake_t3)


def setUpModule():
    isolate_t3_env()


class Base(unittest.TestCase):
    def setUp(self):
        install_prism()
        self.addCleanup(install_prism)
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def submit(self, rid="j", **kw):
        ws = self.base / rid
        ws.mkdir(exist_ok=True)
        return core.submit(self.sd, rid, {"goal": "t"}, str(ws), "s",
                           planner_t3_thread="planner-t3", **kw)

    def run_controller(self, rid, prism, reads=None, step=None):
        """Run the controller loop in-process against a fake T3 serving
        ``prism``; returns the fake. ``reads`` scripts the first snapshot
        reads (None fails like an unreachable server, an exception is
        raised as is); ``step`` replaces
        the controller step. Waits run on a fake clock (``self.slept``);
        T3 launches go to a fake (``self.launched``)."""
        fake = use_fake_t3(self, self.sd, rid)
        fake.prism = prism
        if reads is not None:
            script, serve = list(reads), fake.prism_snapshot

            def prism_snapshot(project_id=None):
                item = script.pop(0) if script else True
                if item is None or isinstance(item, Exception):
                    fake.prism_reads.append(project_id)
                    raise item or t3exec.T3Error("T3 GET /api/prism/snapshot unreachable: "
                                                 "connection refused", unreachable=True)
                return serve(project_id)
            fake.prism_snapshot = prism_snapshot
        self.slept, now = [], [0.0]

        self.routes_while_waiting = []

        def sleep(secs):
            self.slept.append(secs)
            if getattr(self, "on_sleep", None):
                self.on_sleep()
            self.routes_while_waiting.append(policy.stage_routes("dispatch"))
            now[0] += secs
        for name, value in (("_sleep", sleep), ("_clock", lambda: now[0])):
            patcher = mock.patch.object(controller, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        # Never start the real T3 (#140); record what would launch.
        self.launched = []

        def launch(command):
            self.launched.append((now[0], command))
            return self.launch_result(command) if hasattr(self, "launch_result") else "ok"
        patcher = mock.patch.object(controller, "_launch_t3", launch)
        patcher.start()
        self.addCleanup(patcher.stop)
        if step is not None:
            patcher = mock.patch.object(controller, "step", step)
            patcher.start()
            self.addCleanup(patcher.stop)
        info = core.start_controller(self.sd, rid, spawn=lambda cmd: 1)
        saved = dict(controller._LEASE), dict(controller._LOCK)

        def restore():
            if controller._LOCK.get("fd") is not None:
                os.close(controller._LOCK["fd"])
            controller._LEASE.update(saved[0])
            controller._LOCK.update(saved[1])
        self.addCleanup(restore)
        t3snapshot.reset()  # a fresh controller process has read nothing
        controller.run_controller_process(self.sd, rid, info["token"])
        return fake

    def assert_stopped(self, rid, fake, reason):
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"].startswith(reason), job["block_reason"])
        # No thread started: no child create and no turn on any child.
        types = [c.get("type") for c in fake.commands]
        self.assertNotIn("thread.create", types)
        self.assertEqual([t for t, _ in fake.posts if t != fake.planner], [])
        # The planner hears it in its own thread.
        planner_posts = [text for t, text in fake.posts if t == fake.planner]
        self.assertTrue(any(reason in text for text in planner_posts), planner_posts)


class Unreadable(Base):
    def test_unreadable_prism_stops_the_job_and_tells_the_planner(self):
        # T3 answers (HTTP 404, older server): block at once (#136).
        self.submit()
        steps = []
        fake = self.run_controller("j", None, step=lambda *a, **k: steps.append(1))
        self.assert_stopped("j", fake, "Prism unreadable: ")
        self.assertIn("HTTP 404", core.get_job(self.sd, "j")["block_reason"])
        self.assertEqual(self.slept, [])
        self.assertEqual(len(fake.prism_reads), 1)
        self.assertEqual(steps, [])

    def test_a_read_that_recovers_within_the_window_continues_the_job(self):
        self.submit()
        seen = []

        def step(state_dir, request_id, token=None):
            # Routes come from the fresh snapshot, never an older one.
            seen.append(policy.stage_routes("implementation_default"))
            return {"action": "blocked", "reason": "test_stop"}
        fake = self.run_controller("j", default_prism(), reads=[None, None], step=step)
        self.assertEqual(self.slept, [5.0, 10.0])
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0][0], "muse-spark-xhigh-free")
        self.assertEqual(len(fake.prism_reads), 3)
        job = core.get_job(self.sd, "j")
        self.assertNotIn("Prism", job["block_reason"] or "")

    def test_no_routes_while_waiting(self):
        # A readable snapshot applied earlier is never reused during a wait.
        self.submit()
        self.assertTrue(policy.stage_routes("dispatch"))
        self.run_controller("j", default_prism(), reads=[None, None],
                            step=lambda *a, **k: {"action": "blocked"})
        self.assertEqual(self.routes_while_waiting, [[], []])

    def test_empty_worker_lane_stops_the_job(self):
        self.submit()
        prism = default_prism()
        prism["roles"]["worker"]["lanes"]["medium"] = []
        fake = self.run_controller("j", prism)
        self.assert_stopped("j", fake, "Prism Worker medium lane is empty")
        self.assertEqual(self.slept, [])  # a readable setting blocks at once

    def test_empty_dispatcher_list_stops_the_job(self):
        self.submit()
        prism = default_prism()
        prism["roles"]["dispatcher"]["models"] = []
        fake = self.run_controller("j", prism)
        self.assert_stopped("j", fake, "Prism Dispatcher list is empty")


class BlockedReason(Base):
    def test_readable_full_snapshot_routes(self):
        self.assertIsNone(t3snapshot.blocked_reason(default_prism(), "implementation_hard"))

    def test_unknown_snapshot_names_the_cause(self):
        t3snapshot._CACHE["error"] = "no T3 bearer token"
        self.assertEqual(t3snapshot.blocked_reason(None), "Prism unreadable: no T3 bearer token")

    def test_each_needed_list(self):
        cases = [("reviewer", "Prism Reviewer list is empty"),
                 ("correction", "Prism Retry list is empty"),
                 ("recovery", "Prism Escalation list is empty")]
        for role, reason in cases:
            prism = default_prism()
            prism["roles"][role]["models"] = []
            t3snapshot.apply(prism)
            self.assertEqual(t3snapshot.blocked_reason(prism), reason, role)

    def test_switched_off_ladder_steps_need_no_list(self):
        prism = default_prism()
        for role in ("correction", "recovery"):
            prism["roles"][role]["models"] = []
            prism["roles"][role]["enabled"] = False
        t3snapshot.apply(prism)
        self.assertIsNone(t3snapshot.blocked_reason(prism))

    def test_only_the_jobs_lane_is_needed(self):
        prism = default_prism()
        prism["roles"]["worker"]["lanes"]["hard"] = []
        t3snapshot.apply(prism)
        self.assertIsNone(t3snapshot.blocked_reason(prism, "implementation_default"))
        self.assertEqual(t3snapshot.blocked_reason(prism, "implementation_hard"),
                         "Prism Worker hard lane is empty")


class SubmitWithoutPrism(Base):
    def test_job_waits_without_a_route_then_takes_the_first_prism_route(self):
        t3snapshot.reset()
        job = self.submit(lane="hard")
        self.assertEqual((job["route"], job["lane"]), ("", "implementation_hard"))
        # Prism reads by the time the worker turn starts.
        install_prism("implementation_hard")
        use_fake_t3(self, self.sd, "j")
        out = controller.run_implementation(self.sd, "j")
        self.assertEqual(out["action"], "route_switched")
        self.assertEqual(core.get_job(self.sd, "j")["route"],
                         policy.stage_routes("implementation_hard")[0])

    def test_explicit_route_needs_prism(self):
        t3snapshot.reset()
        with self.assertRaises(ValueError):
            self.submit(route="muse-spark-xhigh-free")

    def test_prime_applies_only_a_readable_snapshot(self):
        before = dict(policy.STAGE_OVERRIDES)
        self.assertIsNone(t3snapshot.prime_for_submit("http://127.0.0.1:9", "planner-t3"))
        self.assertEqual(policy.STAGE_OVERRIDES, before)


if __name__ == "__main__":
    unittest.main()
