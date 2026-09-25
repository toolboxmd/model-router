"""Issue #120: durable T3 child ownership and cancellation."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from runner import controller, core, store, t3exec
from tests.fakes import FakeT3Client, snap


PLANNER = "planner-t3"


class T3Persistence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.sd = str(self.base / "state")
        ws = self.base / "workspace"
        ws.mkdir()
        core.submit(self.sd, "r1", {"goal": "t", "proof": "true"},
                    str(ws), "session-r1", planner_t3_thread=PLANNER)
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='r1'",
                        (json.dumps({"t3_threads": {"dispatch": {
                            "thread_id": "sub.planner-t3.dispatch",
                            "route": "luna/max", "created": True,
                            "turn_started": True}}}),))
            con.commit()
        finally:
            con.close()

    def _recover_into(self, fn):
        seen = []
        with mock.patch.object(core, "start_controller", side_effect=fn):
            seen.append(core.recover_one(self.sd, "r1"))
        return seen[0]

    def test_child_is_saved_before_start_failure_and_reused_on_recovery(self):
        fake = FakeT3Client(planner=PLANNER)
        original_post = fake.post_message
        failed = [False]

        def fail_after_create(*args, **kwargs):
            if not failed[0]:
                failed[0] = True
                raise RuntimeError("controller crashed after child creation")
            return original_post(*args, **kwargs)

        fake.post_message = fail_after_create
        with self.assertRaises(RuntimeError):
            controller.run_implementation(self.sd, "r1", t3_client=fake)

        job = core.get_job(self.sd, "r1")
        saved = controller._t3_thread_for(job, "impl_0")
        self.assertIsNotNone(saved)
        self.assertTrue(saved["created"])
        self.assertFalse(saved["turn_started"])
        child_id = saved["thread_id"]
        fake.scripts[child_id] = snap(child_id, state="completed", text="Done")

        result = self._recover_into(
            lambda sd, rid: controller.run_implementation(sd, rid, t3_client=fake))
        self.assertEqual(result["action"], "resumed-controller")
        creates = [c for c in fake.commands if c.get("type") == "thread.create"]
        self.assertEqual([c["threadId"] for c in creates], [child_id])

    def test_saved_fallback_dispatcher_is_adopted_with_its_route_on_recovery(self):
        state = {"t3_threads": {
            "dispatch": {"thread_id": "sub.planner-t3.dispatch",
                          "route": "luna-go/max", "created": True,
                          "turn_started": True}}}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=?, status='running' WHERE request_id='r1'",
                        (json.dumps(state),))
            con.commit()
        finally:
            con.close()
        fake = FakeT3Client(planner=PLANNER)
        fake.scripts["sub.planner-t3.dispatch"] = snap(
            "sub.planner-t3.dispatch", state="completed",
            text=json.dumps({"action": "implementation", "artifact": "a1"}))
        resumed = []

        def resume(sd, rid):
            resumed.append(controller.dispatch(sd, rid, t3_client=fake))
            return {"pid": None}

        out = self._recover_into(resume)
        self.assertEqual(out["action"], "resumed-controller")
        self.assertEqual(resumed[0]["action"], "dispatched")
        creates = [c for c in fake.commands if c.get("type") == "thread.create"]
        self.assertEqual(creates, [])
        starts = [c for c in fake.commands if c.get("type") == "thread.turn.start"]
        self.assertEqual(starts, [])

    def test_crash_before_create_return_is_reconciled_on_recovery(self):
        fake = FakeT3Client(planner=PLANNER)
        original_create = fake.create_child
        crashed = [False]

        def crash_once(*args, **kwargs):
            if not crashed[0]:
                crashed[0] = True
                original_create(*args, **kwargs)
                raise RuntimeError("controller crashed before create returned")
            return original_create(*args, **kwargs)

        fake.create_child = crash_once
        with self.assertRaises(RuntimeError):
            controller.run_implementation(self.sd, "r1", t3_client=fake)
        saved = controller._t3_thread_for(core.get_job(self.sd, "r1"), "impl_0")
        self.assertFalse(saved["created"])
        child_id = saved["thread_id"]
        fake.scripts[child_id] = snap(child_id, state="completed", text="Done")
        result = self._recover_into(
            lambda sd, rid: controller.run_implementation(sd, rid, t3_client=fake))
        self.assertEqual(result["action"], "resumed-controller")
        self.assertTrue(controller._t3_thread_for(core.get_job(self.sd, "r1"), "impl_0")["created"])
        self.assertTrue(controller._t3_thread_for(core.get_job(self.sd, "r1"), "impl_0")["turn_started"])
        creates = [c for c in fake.commands if c.get("type") == "thread.create"]
        self.assertEqual(len(creates), 1)

    def test_snapshot_error_after_post_never_posts_a_second_turn(self):
        fake = FakeT3Client(planner=PLANNER)
        original_post = fake.post_message
        posted = [False]

        def post_then_crash(*args, **kwargs):
            result = original_post(*args, **kwargs)
            if not posted[0]:
                posted[0] = True
                raise RuntimeError("controller crashed after post")
            return result

        fake.post_message = post_then_crash
        with self.assertRaises(RuntimeError):
            controller.run_implementation(self.sd, "r1", t3_client=fake)
        child_id = controller._t3_thread_for(
            core.get_job(self.sd, "r1"), "impl_0")["thread_id"]
        child_reads = [t3exec.T3Error("snapshot unavailable"),
                       snap(child_id, state="completed", text="Done")]

        def snapshot_after_connectivity_returns(tid):
            if tid == PLANNER:
                return snap(PLANNER, state="completed")
            if child_reads:
                item = child_reads.pop(0)
                if isinstance(item, Exception):
                    raise item
                return item
            return snap(child_id, state="completed", text="Done")

        fake.thread_snapshot = mock.Mock(side_effect=snapshot_after_connectivity_returns)
        result = self._recover_into(
            lambda sd, rid: controller.run_implementation(sd, rid, t3_client=fake))
        self.assertEqual(result["action"], "resumed-controller")
        self.assertNotEqual(core.get_job(self.sd, "r1")["status"], "blocked")
        result = self._recover_into(
            lambda sd, rid: controller.run_implementation(sd, rid, t3_client=fake))
        self.assertEqual(result["action"], "resumed-controller")
        self.assertEqual([p[0] for p in fake.posts].count(child_id), 1)

    def test_adopted_running_turn_has_no_recovery_deadline(self):
        state = {"t3_threads": {
            "dispatch": {"thread_id": "sub.planner-t3.dispatch",
                          "route": "luna/max", "created": True,
                          "turn_started": True}}}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='r1'",
                        (json.dumps(state),))
            con.commit()
        finally:
            con.close()
        fake = FakeT3Client(planner=PLANNER)
        fake.scripts["sub.planner-t3.dispatch"] = [snap(
            "sub.planner-t3.dispatch", state="completed", text="old")]
        with mock.patch.object(t3exec, "watch_turn", return_value={
                "state": "completed", "assistant_text": "done"}) as watch:
            result = self._recover_into(
                lambda sd, rid: controller.dispatch(sd, rid, t3_client=fake))
        self.assertEqual(result["action"], "resumed-controller")
        self.assertNotIn("timeout_secs", watch.call_args.kwargs)


class T3Cancellation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.sd = str(self.base / "state")
        ws = self.base / "workspace"
        ws.mkdir()
        core.submit(self.sd, "r1", {"goal": "t"}, str(ws), "session-r1",
                    planner_t3_thread=PLANNER)
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='r1'",
                        (json.dumps({"t3_threads": {"dispatch": {
                            "thread_id": "sub.planner-t3.dispatch",
                            "route": "luna/max", "created": True,
                            "turn_started": True}}}),))
            con.commit()
        finally:
            con.close()

    def test_cancel_interrupts_each_saved_active_child_and_records_result(self):
        state = {"t3_threads": {
            "dispatch": {"thread_id": "sub.planner-t3.dispatch", "route": "luna/max"},
            "impl_0": {"thread_id": "sub.planner-t3.worker", "route": "muse-spark-xhigh-free"},
            "impl_1": {"thread_id": "sub.planner-t3.done", "route": "muse-spark-xhigh-free"},
        }}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='r1'",
                        (json.dumps(state),))
            con.commit()
        finally:
            con.close()

        fake = FakeT3Client(planner=PLANNER)
        for tid in ("sub.planner-t3.dispatch", "sub.planner-t3.worker"):
            fake.scripts[tid] = snap(tid, state="running")
        fake.scripts["sub.planner-t3.done"] = snap(
            "sub.planner-t3.done", state="completed", text="done")
        original_dispatch = fake.dispatch

        def interrupt_settles(command):
            result = original_dispatch(command)
            if command.get("type") == "thread.turn.interrupt":
                fake.scripts[command["threadId"]] = snap(
                    command["threadId"], state="interrupted")
            return result

        fake.dispatch = interrupt_settles
        with mock.patch.object(t3exec, "client_for_job", return_value=fake):
            job = core.cancel(self.sd, "r1")

        self.assertEqual(job["status"], "cancelled")
        interrupts = [c for c in fake.commands
                      if c.get("type") == "thread.turn.interrupt"]
        self.assertEqual({c["threadId"] for c in interrupts},
                         {"sub.planner-t3.dispatch", "sub.planner-t3.worker"})
        con = store.connect(self.sd)
        try:
            events = [dict(row) for row in con.execute(
                "SELECT * FROM events WHERE request_id='r1'").fetchall()]
        finally:
            con.close()
        self.assertEqual(sum(e["kind"] == "t3_turn_interrupt" for e in events), 2)

    def test_unreachable_t3_keeps_cancellation_non_terminal_until_retry(self):
        state = {"t3_threads": {
            "dispatch": {"thread_id": "sub.planner-t3.dispatch",
                          "route": "luna/max", "created": True,
                          "turn_started": True}}}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='r1'",
                        (json.dumps(state),))
            con.commit()
        finally:
            con.close()
        unreachable = mock.patch.object(
            t3exec, "client_for_job", side_effect=t3exec.T3Error("unreachable"))
        with unreachable:
            first = core.cancel(self.sd, "r1")
            self.assertEqual(first["status"], "cancelling")
            # Recover under the same patch: a live T3_SERVER_URL must never be reached.
            self.assertEqual(core.recover_one(self.sd, "r1")["status"], "cancelling")

        fake = FakeT3Client(planner=PLANNER)
        fake.scripts["sub.planner-t3.dispatch"] = snap(
            "sub.planner-t3.dispatch", state="running")
        original_dispatch = fake.dispatch

        def interrupt_settles(command):
            result = original_dispatch(command)
            if command.get("type") == "thread.turn.interrupt":
                fake.scripts[command["threadId"]] = snap(
                    command["threadId"], state="interrupted")
            return result

        fake.dispatch = interrupt_settles
        with mock.patch.object(t3exec, "client_for_job", return_value=fake):
            final = core.recover_one(self.sd, "r1")
        self.assertEqual(final["status"], "cancelled")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "cancelled")

    def test_recover_fences_controller_before_reconciling_a_racing_turn(self):
        state = {"t3_threads": {
            "dispatch": {"thread_id": "sub.planner-t3.dispatch",
                          "route": "luna/max", "created": True,
                          "turn_started": False}}}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=?, cancel_requested=1, "
                        "status='cancelling' WHERE request_id='r1'",
                        (json.dumps(state),))
            con.commit()
        finally:
            con.close()
        fake = FakeT3Client(planner=PLANNER)
        fake.scripts["sub.planner-t3.dispatch"] = [snap(
            "sub.planner-t3.dispatch", state="completed", text="old")]
        original_drain = core._drain_owned_children

        def racing_drain(*args, **kwargs):
            fake.scripts["sub.planner-t3.dispatch"] = snap(
                "sub.planner-t3.dispatch", state="running")
            return original_drain(*args, **kwargs)

        with mock.patch.object(t3exec, "client_for_job", return_value=fake), \
             mock.patch.object(core, "_drain_owned_children", side_effect=racing_drain):
            result = core.recover_one(self.sd, "r1")
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(sum(c.get("type") == "thread.turn.interrupt"
                             for c in fake.commands), 1)

    def test_cancel_keeps_claim_when_controller_exit_is_unconfirmed(self):
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET owner_token='controller', owner_pid=12345, "
                        "owner_start='start', status='running' WHERE request_id='r1'")
            con.commit()
        finally:
            con.close()
        with mock.patch.object(core, "_signal_pid", return_value=True), \
             mock.patch.object(core, "_wait_controller_exit", return_value=False):
            job = core.cancel(self.sd, "r1")
        self.assertEqual(job["status"], "cancelling")
        self.assertEqual(job["owner_token"], "controller")

    def test_recover_confirms_never_created_child_absent(self):
        state = {"t3_threads": {
            "dispatch": {"thread_id": "sub.planner-t3.404",
                          "route": "luna/max", "created": False,
                          "turn_started": False}}}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='r1'",
                        (json.dumps(state),))
            con.commit()
        finally:
            con.close()
        with mock.patch.object(t3exec, "client_for_job",
                               side_effect=t3exec.T3Error("unreachable")):
            self.assertEqual(core.cancel(self.sd, "r1")["status"], "cancelling")
        fake = FakeT3Client(planner=PLANNER)
        fake.thread_snapshot = mock.Mock(
            side_effect=t3exec.T3NotFoundError(
                "T3 child sub.planner-t3.404 absent"))
        with mock.patch.object(t3exec, "client_for_job", return_value=fake):
            result = core.recover_one(self.sd, "r1")
        self.assertEqual(result["status"], "cancelled")


if __name__ == "__main__":
    unittest.main()
