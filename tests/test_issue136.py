"""Wait for T3 instead of blocking when Prism is unreachable
(toolboxmd/model-router#136).

T3 that does not answer (quit, restarting, installing a rebuild) makes
the job wait with capped backoff, up to 24 hours, launching nothing. T3
that answers with an error blocks the job at once, as does an empty list.
"""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, store, t3exec, t3snapshot  # noqa: E402
from tests.test_issue133 import Base  # noqa: E402
from tests.fakes import default_prism, install_prism, isolate_t3_env  # noqa: E402


def setUpModule():
    isolate_t3_env()


def wait_events(sd, rid):
    return [e for e in core.status_view(sd, rid)["recent_events"]
            if e["kind"] == "waiting_for_t3"]


class Unreachable(Base):
    def test_unreachable_then_back_continues_the_job(self):
        self.submit()
        steps = []

        def step(state_dir, request_id, token=None):
            steps.append(1)
            return {"action": "blocked", "reason": "test_stop"}
        fake = self.run_controller("j", default_prism(), reads=[None] * 8, step=step)
        # Backoff doubles from 5s and caps at 60s; no thread while waiting.
        self.assertEqual(self.slept, [5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0, 60.0])
        self.assertEqual(self.routes_while_waiting, [[]] * 8)
        self.assertNotIn("thread.create", [c.get("type") for c in fake.commands])
        self.assertEqual(steps, [1])
        self.assertNotIn("Prism", core.get_job(self.sd, "j")["block_reason"] or "")
        # status shows when the wait began and why.
        events = wait_events(self.sd, "j")
        self.assertEqual(len(events), 1)
        payload = json.loads(events[0]["payload_json"])
        self.assertTrue(payload["since"])
        self.assertIn("connection refused", payload["cause"])

    def test_unreachable_past_the_cap_blocks(self):
        self.submit()
        steps = []
        fake = self.run_controller("j", None, reads=[None] * 5000,
                                   step=lambda *a, **k: steps.append(1))
        self.assert_stopped("j", fake, "Prism unreadable: T3 unreachable for 24h (")
        self.assertIn("connection refused", core.get_job(self.sd, "j")["block_reason"])
        self.assertEqual(sum(self.slept), controller.T3_UNREACHABLE_WAIT_SECS)
        self.assertLessEqual(max(self.slept), controller.PRISM_RETRY_MAX_SECS)
        self.assertEqual(steps, [])

    def test_cancel_during_the_wait_ends_it_at_the_next_reread(self):
        self.submit()
        steps = []

        def cancel():
            if len(self.slept) == 3:
                con = store.connect(self.sd)
                try:
                    con.execute("UPDATE jobs SET cancel_requested=1, status='cancelling'"
                                " WHERE request_id='j'")
                    con.commit()
                finally:
                    con.close()
        self.on_sleep = cancel

        def step(state_dir, request_id, token=None):
            steps.append(core.get_job(state_dir, request_id)["cancel_requested"])
            return {"action": "cancelled"}
        fake = self.run_controller("j", None, reads=[None] * 5000, step=step)
        self.assertEqual(self.slept, [5.0, 10.0, 20.0])
        self.assertEqual(len(fake.prism_reads), 3)  # no reread after the cancel
        self.assertEqual(steps, [1])  # the step records the cancel
        self.assertNotEqual(core.get_job(self.sd, "j")["status"], "blocked")


class AnsweredWithAnError(Base):
    def test_http_errors_and_bad_bodies_block_at_once(self):
        cases = [("401", t3exec.T3Error("T3 GET /api/prism/snapshot failed: HTTP 401 ", 401)),
                 ("403", t3exec.T3Error("T3 GET /api/prism/snapshot failed: HTTP 403 ", 403)),
                 ("404", t3exec.T3NotFoundError("T3 GET /api/prism/snapshot failed: HTTP 404 ")),
                 ("non-JSON", t3exec.T3Error("T3 GET /api/prism/snapshot returned non-JSON"))]
        for i, (label, error) in enumerate(cases):
            rid = f"j{i}"
            self.submit(rid)
            fake = self.run_controller(rid, None, reads=[error])
            self.assert_stopped(rid, fake, "Prism unreadable: ")
            self.assertIn(label, core.get_job(self.sd, rid)["block_reason"])
            self.assertEqual(self.slept, [], label)
            self.assertEqual(wait_events(self.sd, rid), [])


class Classification(unittest.TestCase):
    def test_a_closed_port_is_unreachable_and_an_http_error_is_not(self):
        self.addCleanup(install_prism)
        self.assertIsNone(t3snapshot.read(t3exec.T3Client("http://127.0.0.1:9", "x"), force=True))
        self.assertTrue(t3snapshot.unreachable())

        class Answered:
            server_url = "answered"

            def prism_snapshot(self, project_id=None):
                raise t3exec.T3Error("HTTP 401", status=401)
        self.assertIsNone(t3snapshot.read(Answered(), force=True))
        self.assertFalse(t3snapshot.unreachable())


if __name__ == "__main__":
    unittest.main()
