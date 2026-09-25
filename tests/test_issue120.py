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
        child_id = saved["thread_id"]
        fake.scripts[child_id] = snap(child_id, state="completed", text="Done")

        result = controller.run_implementation(self.sd, "r1", t3_client=fake)
        self.assertEqual(result["action"], "implementation_ok")
        creates = [c for c in fake.commands if c.get("type") == "thread.create"]
        self.assertEqual([c["threadId"] for c in creates], [child_id])


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


if __name__ == "__main__":
    unittest.main()
