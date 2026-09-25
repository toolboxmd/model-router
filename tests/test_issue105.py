"""Issue #105: one repair message for an unparsable dispatcher reply.

Stdlib only, no live T3. A fake dispatcher drives the controller: non-JSON
prose and JSON cut inside a string each cost one repair message on the same
saved dispatcher thread; a valid repaired reply continues; a second bad
reply blocks with the parse error and reply length; recover retries it.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core  # noqa: E402
from tests.fakes import FakeT3Client, PLANNER, snap  # noqa: E402

IMPL = json.dumps({"action": "implementation", "artifact": "a1"})
PROSE = "just some thinking prose with no envelope at all"
CUT = ('{"action": "implementation", "payload": '
       '{"instructions": "do it all and never stop')


class RepairTurn(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def dispatch_with(self, rid, first_text, replies):
        """Dispatch whose first turn says ``first_text``; later posts get
        ``replies`` in order (the dispatch first message consumes none)."""
        ws = self.base / rid
        ws.mkdir()
        core.submit(self.sd, rid, {"goal": "t"}, str(ws), f"session-{rid}",
                    handoff_summary="Summary.", planner_t3_thread=PLANNER,
                    t3_server_url="http://127.0.0.1:3999")
        fake = FakeT3Client()
        created = []
        orig_dispatch, orig_post = fake.dispatch, fake.post_message

        def located(command):
            out = orig_dispatch(command)
            if command.get("type") == "thread.create":
                created.append(command["threadId"])
                fake.scripts[command["threadId"]] = snap(
                    command["threadId"], state="completed", text=first_text,
                    turn="ct0")
            return out

        def post(thread_id, text, **kw):
            if len(fake.posts) == 0:
                fake.child_replies = []
                try:
                    return orig_post(thread_id, text, **kw)
                finally:
                    fake.child_replies = list(replies)
            return orig_post(thread_id, text, **kw)

        fake.dispatch, fake.post_message = located, post
        res = controller.dispatch(self.sd, rid, t3_client=fake)
        return res, fake, created[0]

    def repairs(self, fake, child):
        return [t for tid, t in fake.posts
                if tid == child and t.startswith("ENVELOPE REPAIR")]

    def test_prose_and_cut_string_get_one_repair_and_continue(self):
        for rid, first, error in (("p1", PROSE, "no JSON object"),
                                  ("c1", CUT, "Unterminated string")):
            with self.subTest(first=first):
                res, fake, child = self.dispatch_with(rid, first, [IMPL])
                self.assertEqual(res["action"], "dispatched", res)
                self.assertEqual(res["luna_action"]["action"], "implementation")
                self.assertEqual(res["t3_thread_id"], child)
                (repair,) = self.repairs(fake, child)
                self.assertIn(error, repair)
                self.assertIn("envelope only", repair)
                self.assertNotIn("never stop", repair)
                self.assertNotIn("thinking prose", repair)
                self.assertEqual(len([c for c in fake.commands
                                      if c["type"] == "thread.create"]), 1)

    def test_resume_invalid_gets_repair_and_continues(self):
        res, fake, child = self.dispatch_with("r1", IMPL, [PROSE, IMPL])
        self.assertEqual(res["action"], "dispatched", res)
        out = controller.resume_luna(self.sd, "r1", "worker evidence",
                                     t3_client=fake)
        self.assertEqual(out["action"], "resumed", out)
        self.assertEqual(len(self.repairs(fake, child)), 1)

    def test_second_bad_reply_blocks_with_error_and_length_only(self):
        res, fake, child = self.dispatch_with("b1", PROSE, [CUT])
        self.assertEqual(res["action"], "blocked", res)
        job = core.get_job(self.sd, "b1")
        self.assertEqual(job["status"], "blocked")
        reason = job["block_reason"]
        self.assertTrue(reason.startswith("luna_missing_action: "), reason)
        self.assertIn("Unterminated string", reason)
        self.assertIn(f"reply {len(CUT)} chars", reason)
        self.assertNotIn("never stop", reason)
        self.assertEqual(len(self.repairs(fake, child)), 1)

    def test_recover_retries_dispatch_block(self):
        _res, _fake, child = self.dispatch_with("b2", PROSE, [PROSE])
        with mock.patch.object(core, "start_controller",
                               return_value={"pid": 999}):
            out = core.recover_one(self.sd, "b2")
        self.assertEqual(out["action"], "resumed-controller", out)
        fake2 = FakeT3Client()
        fake2.scripts[child] = snap(child, state="completed", text=PROSE,
                                    turn="ct9")
        fake2.child_replies = [IMPL]
        res = controller.dispatch(self.sd, "b2", t3_client=fake2)
        self.assertEqual(res["action"], "dispatched", res)
        self.assertEqual(len(self.repairs(fake2, child)), 1)

    def test_recover_retries_resume_block(self):
        _res, fake, child = self.dispatch_with("b3", IMPL, [PROSE, PROSE])
        out = controller.resume_luna(self.sd, "b3", "worker evidence",
                                     t3_client=fake)
        self.assertEqual(out["action"], "blocked", out)
        with mock.patch.object(core, "start_controller",
                               return_value={"pid": 999}):
            rec = core.recover_one(self.sd, "b3")
        self.assertEqual(rec["action"], "resumed-controller", rec)
        fake.child_replies = [IMPL]
        with mock.patch("runner.t3exec.client_for_job", return_value=fake):
            stepped = controller.step(self.sd, "b3")
        self.assertEqual(stepped["action"], "resumed", stepped)
        self.assertEqual(stepped["luna_action"]["action"], "implementation")
        self.assertEqual(len(self.repairs(fake, child)), 2)


if __name__ == "__main__":
    unittest.main()
