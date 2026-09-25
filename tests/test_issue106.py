"""Issue #106: router jobs run as T3 child threads of the planner thread.

Stdlib only, no live T3. A fake in-memory orchestration client drives the
controller branches, and a fake HTTP orchestration server proves the wire
shapes (dispatch command, thread snapshot, bearer auth). Coverage:

- child identity (``sub.<parent>.<suffix>`` + ``parentThreadId`` payload,
  first message names the job and links the planner thread),
- thread tree (#113): dispatcher under the planner thread, workers under
  the dispatcher thread,
- route mapping (provider instance, model, effort; unknown routes raise),
- submit persistence and idempotency of the T3 fields,
- dispatcher and worker turns through T3 with envelope/report handling,
- terminal state posted into the planner thread (exactly once),
- activity-based liveness (streaming/tool activity healthy, silence with
  no running tool flagged within about a minute and probed immediately,
  explicit provider errors at once, long tool runs never stalled),
- restart behavior (Codex-family turns continue, Grok turns re-sent),
- submit requires a planner T3 thread (the only execution path, #110),
- provider errors (exhausted dispatch falls back, hard errors block),
- unavailable T3 blocks instead of silently running direct.
"""
import copy
import json
import sys
import tempfile
import threading
import unittest

from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, store, t3exec  # noqa: E402
from tests.fakes import (FakeT3Client, NOW, PLANNER, PROJECT,  # noqa: E402,F401
                         act, iso, msg, snap)

def luna_impl_envelope():
    return json.dumps({"action": "implementation", "artifact": "a1"})


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

    def submit_t3(self, rid, **kw):
        kw.setdefault("handoff_summary", f"Summary for {rid}.")
        kw.setdefault("planner_t3_thread", PLANNER)
        kw.setdefault("t3_server_url", "http://127.0.0.1:3999")
        return core.submit(self.sd, rid, {"goal": "t"}, self.ws(rid),
                           f"session-{rid}", **kw)


class ThreadIdentity(Base):
    def test_child_id_convention_and_parent_link(self):
        child = t3exec.child_thread_id(PLANNER, suffix="abc123")
        self.assertEqual(child, f"sub.{PLANNER}.abc123")
        self.assertEqual(t3exec.parent_of_child(child), PLANNER)
        self.assertTrue(t3exec.is_t3_job({"planner_t3_thread": PLANNER}))
        self.assertFalse(t3exec.is_t3_job({"planner_t3_thread": None}))
        self.assertFalse(t3exec.is_t3_job({}))

    def test_bad_thread_ids_rejected(self):
        for bad in ("", "  ", "has space", "a/b", "../x", ".lead", "trail.",
                    "x" * 300):
            with self.subTest(bad=bad[:12]):
                with self.assertRaises(ValueError):
                    t3exec.validate_thread_id(bad)

    def test_create_command_carries_parent_link_and_route(self):
        cmd = t3exec.child_create_command("sub.planner-1.s1", PLANNER,
                                          PROJECT, "title", "luna/max",
                                          role="dispatch")
        self.assertEqual(cmd["type"], "thread.create")
        self.assertEqual(cmd["parentThreadId"], PLANNER)
        self.assertEqual(cmd["modelSelection"]["model"], "gpt-5.6-luna")
        # Default mode: T3 plan mode would ask for <proposed_plan> blocks
        # and user input instead of the dispatcher envelope.
        self.assertEqual(cmd["interactionMode"], "default")
        self.assertEqual(cmd["runtimeMode"], "auto")
        worker = t3exec.child_create_command("sub.planner-1.s2", PLANNER,
                                             PROJECT, "title",
                                             "muse-spark-xhigh-free")
        self.assertEqual(worker["interactionMode"], "default")
        self.assertEqual(worker["runtimeMode"], "full-access")

    def test_first_message_names_job_and_parent(self):
        text = t3exec.child_first_message("r1", "dispatcher", PLANNER,
                                          "luna/max", "Do the thing.")
        for needle in ("r1", "dispatcher", PLANNER, "luna/max",
                       "Do the thing."):
            self.assertIn(needle, text)


class RouteMapping(Base):
    def test_selections(self):
        sel = t3exec.route_model_selection("luna/max")
        # Canonical option arrays with each T3 adapter's option id
        # (Codex/Grok reasoningEffort, OpenCode variant plus agent).
        self.assertEqual(sel, {"instanceId": "codex", "model": "gpt-5.6-luna",
                               "options": [{"id": "reasoningEffort", "value": "max"}]})
        sel = t3exec.route_model_selection("muse-spark-xhigh-free")
        self.assertEqual(sel["instanceId"], "opencode")
        # OpenCode keeps provider/model: the adapter rejects bare ids and
        # the prefix separates Zen free from Go.
        self.assertEqual(sel["model"], "opencode/muse-spark-1.3-contributor-free")
        self.assertEqual(sel["options"], [{"id": "variant", "value": "xhigh"},
                                          {"id": "agent", "value": "build"}])
        sel = t3exec.route_model_selection("grok-4.6-build")
        self.assertEqual(sel["instanceId"], "grok")
        self.assertEqual(sel["model"], "grok-4.6")
        self.assertEqual(sel["options"], [{"id": "reasoningEffort", "value": "medium"}])
        # An OpenCode route on the xAI pool runs on the OpenCode instance.
        sel = t3exec.route_model_selection("grok-4.6-xai")
        self.assertEqual(sel["instanceId"], "opencode")
        self.assertEqual(sel["model"], "xai/grok-4.6")
        sel = t3exec.route_model_selection("luna-go/max")
        self.assertEqual(sel["model"], "opencode-go/gpt-5.6-luna")
        self.assertEqual(sel["options"], [{"id": "agent", "value": "plan"}])

    def test_unknown_route_raises_never_substitutes(self):
        with self.assertRaises(ValueError):
            t3exec.route_model_selection("nope/not-a-route")

    def test_instance_override_env(self):
        with mock.patch.dict("os.environ",
                             {"MODEL_ROUTER_T3_INSTANCE_OPENCODE": "custom-oc"}):
            self.assertEqual(t3exec.route_instance_id("luna-go/max"),
                             "custom-oc")
            self.assertEqual(t3exec.route_instance_id("luna/max"), "codex")


class SubmitT3(Base):
    def test_fields_persist_and_resubmit_is_identical(self):
        job = self.submit_t3("r1")
        self.assertEqual(job["planner_t3_thread"], PLANNER)
        self.assertEqual(job["t3_server_url"], "http://127.0.0.1:3999")
        again = core.submit(self.sd, "r1", {"goal": "t"}, self.ws("r1"),
                            "session-r1", planner_t3_thread=PLANNER,
                            t3_server_url="http://127.0.0.1:3999",
                            handoff_summary="Summary for r1.")
        self.assertEqual(again["request_id"], "r1")

    def test_changed_thread_conflicts(self):
        self.submit_t3("r2")
        with self.assertRaises(core.ConflictError):
            core.submit(self.sd, "r2", {"goal": "t"}, self.ws("r2"),
                        "session-r2", planner_t3_thread="other-thread",
                        handoff_summary="Summary for r2.")

    def test_validation(self):
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r3", {"goal": "t"}, self.ws("r3"),
                        "s", planner_t3_thread="has space")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r4", {"goal": "t"}, self.ws("r4"),
                        "s", t3_server_url="http://127.0.0.1:3999")
        with self.assertRaises(ValueError):
            core.submit(self.sd, "r6", {"goal": "t"}, self.ws("r6"), "s")
        job = core.submit(self.sd, "r5", {"goal": "t"}, self.ws("r5"), "s",
                          planner_harness="claude", planner_t3_thread=PLANNER)
        self.assertEqual(job["planner_harness"], "claude")


class DispatchViaT3(Base):
    def _dispatched(self, rid, fake, text):
        # Script the child snapshot before dispatch: the fake serves it on
        # every read after create (dict script repeats).
        created = []

        orig_dispatch = fake.dispatch

        def located(command):
            out = orig_dispatch(command)
            if command.get("type") == "thread.create":
                created.append(command["threadId"])
                fake.scripts[command["threadId"]] = snap(
                    command["threadId"], state="completed", text=text)
            return out

        fake.dispatch = located
        res = controller.dispatch(self.sd, rid, t3_client=fake)
        self.assertEqual(len(created), 1)
        return res, created[0]

    def test_child_shape_and_first_message(self):
        self.submit_t3("d2")
        fake = FakeT3Client()
        res, child = self._dispatched("d2", fake, luna_impl_envelope())
        self.assertEqual(res["action"], "dispatched", res)
        self.assertTrue(child.startswith(f"sub.{PLANNER}."))
        creates = [c for c in fake.commands if c["type"] == "thread.create"]
        self.assertEqual(len(creates), 1)
        self.assertEqual(creates[0]["parentThreadId"], PLANNER)
        self.assertEqual(creates[0]["modelSelection"]["model"],
                         "gpt-5.6-luna")
        starts = [c for c in fake.commands
                  if c["type"] == "thread.turn.start"]
        self.assertTrue(starts)
        first = starts[0]["message"]["text"]
        for needle in ("d2", "dispatcher", PLANNER, "luna/max"):
            self.assertIn(needle, first)
        # Recorded for adoption + Observer.
        threads = controller._t3_threads_map(core.get_job(self.sd, "d2"))
        self.assertEqual(threads["dispatch"]["thread_id"], child)
        kinds = [r["kind"] for r in
                 controller.store.connect(self.sd).execute(
                     "SELECT kind FROM child_calls WHERE request_id='d2'").fetchall()]
        self.assertIn("t3_turn", kinds)

    def test_redispatch_adopts_without_new_child(self):
        self.submit_t3("d3")
        fake = FakeT3Client()
        res, child = self._dispatched("d3", fake, luna_impl_envelope())
        self.assertEqual(res["action"], "dispatched")
        creates = len([c for c in fake.commands
                       if c["type"] == "thread.create"])
        res2 = controller.dispatch(self.sd, "d3", t3_client=fake)
        self.assertEqual(res2["action"], "already-dispatched")
        self.assertEqual(res2["t3_thread_id"], child)
        self.assertEqual(len([c for c in fake.commands
                              if c["type"] == "thread.create"]), creates)

    def test_missing_envelope_blocks(self):
        self.submit_t3("d4")
        fake = FakeT3Client()
        _res, _child = self._dispatched("d4", fake, "just prose, no JSON")
        job = core.get_job(self.sd, "d4")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("luna_missing_action", job["block_reason"])


class DispatchFallbackAndErrors(Base):
    def _exhausted_then_ok(self, rid):
        self.submit_t3(rid)
        fake = FakeT3Client()
        seen = []

        orig = fake.dispatch

        def located(command):
            out = orig(command)
            if command.get("type") == "thread.create":
                tid = command["threadId"]
                seen.append(command["modelSelection"]["model"])
                if len(seen) == 1:
                    fake.scripts[tid] = snap(
                        tid, state="running", session_status="error",
                        last_error="You have hit your usage limit")
                else:
                    fake.scripts[tid] = snap(tid, state="completed",
                                             text=luna_impl_envelope())
            return out

        fake.dispatch = located
        res = controller.dispatch(self.sd, rid, t3_client=fake)
        return res, fake, seen

    def test_exhausted_dispatch_falls_back_and_marks_capacity(self):
        res, fake, seen = self._exhausted_then_ok("e1")
        self.assertEqual(res["action"], "dispatched", res)
        self.assertEqual(res["route"], "luna-go/max")
        self.assertEqual(seen, ["gpt-5.6-luna", "opencode-go/gpt-5.6-luna"])
        self.assertIn("luna/max", core.exhausted_routes(self.sd))

    def test_unavailable_t3_blocks_without_silent_fallback(self):
        self.submit_t3("e2")
        fake = FakeT3Client()
        fake.dispatch_error = t3exec.T3Error("connection refused")
        res = controller.dispatch(self.sd, "e2", t3_client=fake)
        self.assertEqual(res["action"], "blocked")
        job = core.get_job(self.sd, "e2")
        self.assertIn("t3_unavailable", job["block_reason"])
        # No direct-path child started.
        kinds = [r["kind"] for r in
                 controller.store.connect(self.sd).execute(
                     "SELECT kind FROM child_calls WHERE request_id='e2'").fetchall()]
        self.assertNotIn("codex_dispatch", kinds)
        self.assertNotIn("opencode_control", kinds)


class WorkerViaT3(Base):
    def test_completed_worker_reports(self):
        self.submit_t3("w1")
        fake = FakeT3Client()
        created = []

        orig = fake.dispatch

        def located(command):
            out = orig(command)
            if command.get("type") == "thread.create":
                created.append((command["threadId"],
                                command["modelSelection"]["model"]))
                fake.scripts[command["threadId"]] = snap(
                    command["threadId"], state="completed",
                    text="Done. PR: https://example.test/pr/9")
            return out

        fake.dispatch = located
        res = controller.run_implementation(self.sd, "w1", t3_client=fake)
        self.assertEqual(res["action"], "implementation_ok", res)
        self.assertIn("https://example.test/pr/9", res["output"])
        self.assertEqual(created[0][1], "opencode/muse-spark-1.3-contributor-free")
        report = res["report"]
        self.assertTrue(Path(report["report_path"]).exists())
        # One-turn accounting sees the T3 turn.
        self.assertEqual(
            controller._turns_by_route(self.sd, "w1").get(
                "muse-spark-xhigh-free"), 1)

    def test_interrupted_worker_continues_on_same_thread(self):
        self.submit_t3("w2")
        fake = FakeT3Client()
        created = []

        orig = fake.dispatch

        def located(command):
            out = orig(command)
            if command.get("type") == "thread.create":
                created.append(command["threadId"])
                # Watch sees the cut turn; the continue reads it once more
                # for its prior turn id, then watches the new turn t2.
                fake.scripts[command["threadId"]] = [
                    snap(command["threadId"], state="interrupted"),
                    snap(command["threadId"], state="interrupted"),
                    snap(command["threadId"], state="completed", turn="t2",
                         text="Recovered. PR: https://example.test/pr/10"),
                ]
            return out

        fake.dispatch = located
        res = controller.run_implementation(self.sd, "w2", t3_client=fake)
        self.assertEqual(res["action"], "implementation_ok", res)
        self.assertIn("https://example.test/pr/10", res["output"])
        self.assertEqual(len(created), 1)
        turns = [c for c in fake.commands
                 if c.get("type") == "thread.turn.start"
                 and c.get("threadId") == created[0]]
        self.assertEqual(len(turns), 2)


class ThreadTree(Base):
    """#113: dispatcher under the planner, workers under the dispatcher."""

    def _scripted(self, fake, created):
        orig = fake.dispatch

        def located(command):
            out = orig(command)
            if command.get("type") == "thread.create":
                tid = command["threadId"]
                created.append(command)
                text = (luna_impl_envelope() if len(created) == 1
                        else "Done. PR: https://example.test/pr/11")
                fake.scripts[tid] = snap(tid, state="completed", text=text)
            return out

        fake.dispatch = located

    def test_worker_is_a_child_of_the_dispatcher_thread(self):
        self.submit_t3("tree1")
        fake = FakeT3Client()
        created = []
        self._scripted(fake, created)
        res = controller.dispatch(self.sd, "tree1", t3_client=fake)
        self.assertEqual(res["action"], "dispatched", res)
        dispatcher = res["t3_thread_id"]
        res = controller.run_implementation(self.sd, "tree1", t3_client=fake)
        self.assertEqual(res["action"], "implementation_ok", res)
        self.assertEqual(len(created), 2)
        disp_create, worker_create = created
        self.assertEqual(disp_create["parentThreadId"], PLANNER)
        self.assertEqual(t3exec.parent_of_child(dispatcher), PLANNER)
        worker = worker_create["threadId"]
        self.assertTrue(worker.startswith(f"sub.{dispatcher}."))
        self.assertEqual(worker_create["parentThreadId"], dispatcher)
        self.assertEqual(t3exec.parent_of_child(worker), dispatcher)
        # Same T3 project as the planner thread.
        self.assertEqual(worker_create["projectId"], PROJECT)
        # The worker's first message names the job and links the planner.
        first = [t for tid, t in fake.posts if tid == worker][0]
        for needle in ("tree1", "worker seq", f"planner thread {PLANNER}"):
            self.assertIn(needle, first)
        # Recovery still finds every job thread in the one map.
        threads = controller._t3_threads_map(core.get_job(self.sd, "tree1"))
        self.assertEqual(threads["dispatch"]["thread_id"], dispatcher)
        self.assertEqual(threads["impl_1"]["thread_id"], worker)
        events = [json.loads(r["payload_json"]) for r in
                  controller.store.connect(self.sd).execute(
                      "SELECT payload_json FROM events WHERE request_id='tree1' "
                      "AND kind='t3_thread'").fetchall()]
        self.assertIn(worker, [e["thread_id"] for e in events])

    def test_worker_without_a_saved_dispatcher_falls_back_to_planner(self):
        self.submit_t3("tree2")
        fake = FakeT3Client()
        created = []
        orig = fake.dispatch

        def located(command):
            out = orig(command)
            if command.get("type") == "thread.create":
                created.append(command)
                fake.scripts[command["threadId"]] = snap(
                    command["threadId"], state="completed", text="Done.")
            return out

        fake.dispatch = located
        res = controller.run_implementation(self.sd, "tree2", t3_client=fake)
        self.assertEqual(res["action"], "implementation_ok", res)
        self.assertEqual(created[0]["parentThreadId"], PLANNER)
        self.assertTrue(created[0]["threadId"].startswith(f"sub.{PLANNER}."))

    def test_run_t3_turn_links_planner_in_first_message(self):
        fake = FakeT3Client()
        dispatcher = t3exec.child_thread_id(PLANNER, suffix="disp")
        orig = fake.dispatch

        def located(command):
            out = orig(command)
            if command.get("type") == "thread.create":
                fake.scripts[command["threadId"]] = snap(
                    command["threadId"], state="completed", text="ok")
            return out

        fake.dispatch = located
        out = t3exec.run_t3_turn(
            fake, request_id="r9", kind_label="worker seq 1",
            parent_thread_id=dispatcher, project_id=PROJECT,
            route="muse-spark-xhigh-free", role="implementation",
            prompt="Do it.", title="t", child_suffix="w1",
            planner_thread_id=PLANNER,
            watch_kwargs={"timeout_secs": 5})
        self.assertEqual(out["thread_id"], f"sub.{dispatcher}.w1")
        self.assertEqual(out["parent_thread_id"], dispatcher)
        self.assertEqual(out["planner_thread_id"], PLANNER)
        first = fake.posts[0][1]
        self.assertIn(f"planner thread {PLANNER}", first)
        self.assertNotIn(f"planner thread {dispatcher}", first)


class TerminalViaT3(Base):
    def _blocked_job(self, rid):
        self.submit_t3(rid)
        controller._mark_blocked(self.sd, rid, f"{rid} blocked: needs judgment")

    def test_terminal_post_names_job_and_state(self):
        self._blocked_job("t1")
        fake = FakeT3Client()
        res = controller.deliver_terminal_report(self.sd, "t1",
                                                 t3_client=fake)
        self.assertEqual(res["action"], "reported", res)
        self.assertEqual(len(fake.posts), 1)
        tid, text = fake.posts[0]
        self.assertEqual(tid, PLANNER)
        for needle in ("t1", "blocked", "needs judgment", "Summary for t1."):
            self.assertIn(needle, text)
        job = core.get_job(self.sd, "t1")
        self.assertEqual(job["status"], "blocked")
        rec = json.loads(job["controller_state"] or "{}")["terminal_report"]
        self.assertEqual(rec["state"], "delivered")

    def test_succeeded_post_carries_pr_url(self):
        self.submit_t3("t2")
        controller._complete_job(
            self.sd, "t2", None, "done", None, "https://example.test/pr/7")
        job = core.get_job(self.sd, "t2")
        self.assertEqual(job["status"], "succeeded")
        # _complete_job's internal best-effort delivery had no token, so it
        # recorded failed; the real post lands here with the fake client.
        fake = FakeT3Client()
        res = controller.deliver_terminal_report(self.sd, "t2",
                                                 t3_client=fake)
        self.assertEqual(res["action"], "reported", res)
        _tid, text = fake.posts[0]
        self.assertIn("ready to merge", text)
        self.assertIn("https://example.test/pr/7", text)

    def test_exactly_once_and_adopted(self):
        self._blocked_job("t3")
        fake = FakeT3Client()
        first = controller.deliver_terminal_report(self.sd, "t3",
                                                   t3_client=fake)
        self.assertEqual(first["action"], "reported")
        second = controller.deliver_terminal_report(self.sd, "t3",
                                                    t3_client=fake)
        self.assertEqual(second["action"], "already-reported")
        self.assertEqual(len(fake.posts), 1)
        # A restarted controller with a fresh client still sends nothing:
        # the finished child_calls row is adopted.
        con = store.connect(self.sd)
        con.execute("UPDATE jobs SET controller_state='{}' WHERE request_id='t3'")
        con.commit()
        con.close()
        third = controller.deliver_terminal_report(self.sd, "t3",
                                                   t3_client=FakeT3Client())
        self.assertEqual(third["action"], "reported")
        self.assertEqual(len(fake.posts), 1)

    def test_post_failure_records_without_changing_job(self):
        self._blocked_job("t4")
        fake = FakeT3Client()
        fake.dispatch_error = t3exec.T3Error("down")
        res = controller.deliver_terminal_report(self.sd, "t4",
                                                 t3_client=fake)
        self.assertEqual(res["action"], "report-failed", res)
        job = core.get_job(self.sd, "t4")
        self.assertEqual(job["status"], "blocked")


class PlannerQuestionViaT3(Base):
    """Planner questions go into the planner's T3 thread as a message and
    the planner's in-thread reply is the answer; no second headless
    process ever resumes the planner session."""

    def _no_cli(self, *a, **k):
        raise AssertionError("a T3-hosted planner must never be resumed by CLI")

    def test_question_posted_in_thread_and_reply_is_the_answer(self):
        self.submit_t3("q1")
        fake = FakeT3Client()
        fake.planner_replies = ["Use approach B."]
        res = controller.planner_callback(self.sd, "q1", "qq", "Which approach?", t3_client=fake)
        self.assertEqual(res["action"], "answered", res)
        self.assertEqual(len(fake.posts), 1)
        tid, text = fake.posts[0]
        self.assertEqual(tid, PLANNER)
        self.assertIn("Which approach?", text)
        self.assertIn("Summary for q1.", text)
        turn = [c for c in fake.commands if c.get("type") == "thread.turn.start"]
        self.assertEqual(len(turn), 1)
        self.assertEqual(turn[0]["threadId"], PLANNER)
        answered = core.list_questions(self.sd, "q1", only_pending=False)
        self.assertEqual([(q["qid"], q["status"], q["answer"]) for q in answered],
                         [("qq", "answered", "Use approach B.")])
        kinds = [r["kind"] for r in store.connect(self.sd).execute(
            "SELECT kind FROM child_calls WHERE request_id='q1'").fetchall()]
        self.assertIn(controller.T3_QUESTION_POST_KIND, kinds)
        self.assertIn(controller.T3_ANSWER_KIND, kinds)
        self.assertFalse([k for k in kinds if k.endswith("_callback")], kinds)

    def test_earlier_planner_turn_never_answers(self):
        # The planner thread already holds a completed turn ("planning
        # here"); that text must never be taken as the answer.
        self.submit_t3("q2")
        fake = FakeT3Client()
        fake.planner_replies = [None]
        with mock.patch.object(controller, "_t3_watch_kwargs",
                               return_value={"sleep_fn": lambda s: None,
                                             "silence_secs": 0.0}):
            res = controller.planner_callback(self.sd, "q2", "qq", "Which?",
                                              t3_client=fake)
        self.assertEqual(res["action"], "blocked", res)
        self.assertEqual(res["reason"], "planner_question_pending")
        pending = core.list_questions(self.sd, "q2")
        self.assertEqual([q["qid"] for q in pending], ["qq"])
        self.assertIn("planner T3 thread",
                      core.get_job(self.sd, "q2")["block_reason"])

    def test_retry_adopts_the_recorded_post(self):
        self.submit_t3("q3")
        fake = FakeT3Client()
        fake.planner_replies = [None]
        with mock.patch.object(controller, "_t3_watch_kwargs",
                               return_value={"sleep_fn": lambda s: None,
                                             "silence_secs": 0.0}):
            controller.planner_callback(self.sd, "q3", "qq", "Which?", t3_client=fake)
        self.assertEqual(len(fake.posts), 1)
        # The planner answers late; a recovering controller adopts the
        # same post and reads the reply without asking twice.
        fake.planner_turns += 1
        fake.planner_messages.append(msg("Late answer: A.",
                                         turn=f"pt{fake.planner_turns}"))
        con = store.connect(self.sd)
        con.execute("UPDATE jobs SET status='running', block_reason=NULL "
                    "WHERE request_id='q3'")
        con.commit()
        con.close()
        res = controller.planner_callback(self.sd, "q3", "qq", "Which?", t3_client=fake)
        self.assertEqual(res["action"], "answered", res)
        self.assertEqual(len(fake.posts), 1)
        answered = core.list_questions(self.sd, "q3", only_pending=False)
        self.assertEqual(answered[0]["answer"], "Late answer: A.")

    def test_question_action_relays_the_in_thread_answer_to_dispatcher(self):
        self.submit_t3("q4")
        fake = FakeT3Client()
        fake.planner_replies = ["Go with the smaller change."]
        resumed = []

        def fake_resume(sd, rid, prompt, run_cmd=None, label="CONTEXT",
                        t3_client=None):
            resumed.append((label, prompt))
            return {"action": "resumed", "luna_action": {"action": "noop"}}

        with mock.patch.object(t3exec, "client_for_job", return_value=fake), \
                mock.patch.object(controller, "resume_luna", fake_resume):
            controller._handle_question_action(
                self.sd, "q4", {"action": "planner_question", "qid": "q9",
                                "prompt": "Big or small change?"})
        self.assertEqual(len(resumed), 1)
        self.assertEqual(resumed[0][0], "PLANNER ANSWER")
        self.assertIn("Go with the smaller change.", resumed[0][1])
        self.assertEqual(fake.posts[0][0], PLANNER)


class StepLoopViaT3(Base):
    """The controller's step loop drives a T3 job past dispatch: the saved
    dispatcher child thread, not a Codex task id, marks it dispatched."""

    def test_question_is_answered_in_thread_and_dispatcher_resumes(self):
        self.submit_t3("s1")
        fake = FakeT3Client()
        fake.child_replies = [
            json.dumps({"action": "planner_question", "qid": "q1",
                        "prompt": "Which greeting?"}),
            json.dumps({"action": "implementation", "artifact": "a1"}),
        ]
        fake.planner_replies = ["hello from the planner"]

        def no_cli(*a, **k):
            raise AssertionError("the T3 path never runs a harness CLI")

        with mock.patch.object(t3exec, "client_for_job", return_value=fake):
            first = controller.step(self.sd, "s1")
            self.assertEqual(first["action"], "dispatched", first)
            second = controller.step(self.sd, "s1")
        self.assertEqual(second["action"], "question-answered-resumed", second)
        # One dispatcher child, two turns on it; one post into the planner.
        creates = [c for c in fake.commands if c["type"] == "thread.create"]
        self.assertEqual(len(creates), 1)
        child = creates[0]["threadId"]
        self.assertTrue(child.startswith(f"sub.{PLANNER}."))
        child_posts = [t for t, _ in fake.posts if t == child]
        self.assertEqual(len(child_posts), 2)
        self.assertIn("hello from the planner", fake.posts[-1][1])
        planner_posts = [t for t, _ in fake.posts if t == PLANNER]
        self.assertEqual(len(planner_posts), 1)
        q = core.list_questions(self.sd, "s1", only_pending=False)
        self.assertEqual(q[0]["answer"], "hello from the planner")
        job = core.get_job(self.sd, "s1")
        self.assertEqual(job["status"], "running")
        self.assertEqual(controller._load_controller_state(job)
                         ["last_action"]["action"], "implementation")

    def test_recover_resumes_a_t3_job_with_a_saved_dispatcher(self):
        self.submit_t3("s2")
        fake = FakeT3Client()
        fake.child_replies = [json.dumps({"action": "planner_question",
                                          "qid": "q1", "prompt": "Which?"})]
        with mock.patch.object(t3exec, "client_for_job", return_value=fake):
            controller.step(self.sd, "s2")
        job = core.get_job(self.sd, "s2")
        self.assertIsNone(job["codex_task_id"])
        self.assertTrue(core._dispatcher_saved(job))


class Liveness(Base):
    def test_streaming_and_tool_activity_are_healthy(self):
        now = NOW.timestamp()
        running = t3exec.evaluate_turn(
            snap("t", text="partial", streaming=True, age_secs=5), now=now)
        self.assertEqual(running["state"], "running")
        # A long test run: the tool opened 300 s ago and is still open.
        tool = t3exec.evaluate_turn(
            snap("t", activities=[act("tool.started", age_secs=300, call="c1")],
                 age_secs=300), now=now)
        self.assertEqual(tool["state"], "running")
        self.assertEqual(tool["running_tools"], 1)
        task = t3exec.evaluate_turn(
            snap("t", activities=[act("task.started", age_secs=900, task="k1"),
                                  act("task.progress", age_secs=800, task="k1")],
                 age_secs=900), now=now)
        self.assertEqual(task["state"], "running")

    def test_finished_tool_does_not_hide_model_silence(self):
        now = NOW.timestamp()
        done = t3exec.evaluate_turn(
            snap("t", activities=[act("tool.started", age_secs=200, call="c1"),
                                  act("tool.completed", age_secs=90, call="c1")],
                 age_secs=200), now=now, silence_secs=60)
        self.assertEqual(done["state"], "stalled", done)
        self.assertGreaterEqual(done["silence"], 89)
        # A tool of another turn never keeps this turn alive.
        other = t3exec.evaluate_turn(
            snap("t", activities=[act("tool.started", age_secs=200, call="c9",
                                      turn="t0")],
                 age_secs=200), now=now, silence_secs=60)
        self.assertEqual(other["state"], "stalled", other)

    def test_tool_waiting_on_approval_is_not_running(self):
        # Live T3 shape: approval.requested then tool.started, nobody
        # answers. The tool is not running; silence counts.
        now = NOW.timestamp()
        waiting = snap("t", activities=[
            {**act("approval.requested", age_secs=90, tone="approval"),
             "payload": {"requestId": "r1"}},
            act("tool.started", age_secs=90, call="c1")], age_secs=90)
        ev = t3exec.evaluate_turn(waiting, now=now, silence_secs=60)
        self.assertEqual(ev["state"], "stalled", ev)
        waiting["thread"]["activities"].append(
            {**act("approval.resolved", age_secs=80, tone="approval"),
             "payload": {"requestId": "r1"}})
        ev = t3exec.evaluate_turn(waiting, now=now, silence_secs=60)
        self.assertEqual(ev["state"], "running", ev)

    def test_stuck_stream_goes_silent(self):
        now = NOW.timestamp()
        stuck = t3exec.evaluate_turn(
            snap("t", text="partial", streaming=True, age_secs=120),
            now=now, silence_secs=60)
        self.assertEqual(stuck["state"], "stalled", stuck)

    def test_resumed_thread_waits_for_its_new_turn(self):
        fake = FakeT3Client()
        old = snap("r1", state="completed", text="old answer", turn="t1",
                   age_secs=600)
        new = snap("r1", state="completed", text="new answer", turn="t2")
        fake.scripts["r1"] = [old, old, new]
        out = t3exec.post_and_watch(fake, "r1", "follow-up",
                                    watch_kwargs={"now_fn": lambda: NOW.timestamp(),
                                                  "sleep_fn": lambda s: None,
                                                  "timeout_secs": 30})
        self.assertEqual(out["state"], "completed", out)
        self.assertEqual(out["assistant_text"], "new answer")

    def test_refused_turn_start_errors_at_once(self):
        # Live T3: an unknown provider instance errors the session with no
        # latestTurn at all; the watch must act at once, not wait a minute.
        fake = FakeT3Client()
        refused = snap("x1", session_status="error",
                       last_error="references unknown provider instance 'nope'")
        refused["thread"]["latestTurn"] = None
        fake.scripts["x1"] = [refused]
        out = t3exec.watch_turn(fake, "x1", now_fn=lambda: NOW.timestamp(),
                                sleep_fn=lambda s: None, timeout_secs=30,
                                await_new_turn=True)
        self.assertEqual(out["state"], "error", out)
        self.assertIn("unknown provider instance", out["reason"])
        self.assertEqual(fake.reads["x1"], 1)

    def test_stale_last_error_does_not_fail_a_new_turn(self):
        now = NOW.timestamp()
        ev = t3exec.evaluate_turn(
            snap("t", state="running", session_status="running",
                 last_error="You have hit your usage limit", age_secs=5),
            now=now)
        self.assertEqual(ev["state"], "running", ev)

    def test_silence_without_tool_stalls_and_probes_immediately(self):
        fake = FakeT3Client()
        quiet = snap("c1", age_secs=300, user_text="start",
                     requested_age_secs=300)
        fake.scripts["c1"] = [quiet, copy.deepcopy(quiet)]
        out = t3exec.watch_turn(fake, "c1", now_fn=lambda: NOW.timestamp(),
                                sleep_fn=lambda s: None, silence_secs=60,
                                timeout_secs=30)
        self.assertEqual(out["state"], "stalled", out)
        self.assertTrue(out.get("probed"))
        # Flagged within about a minute, then probed at once: two reads.
        self.assertEqual(fake.reads["c1"], 2)

    def test_activity_between_reads_clears_probation(self):
        fake = FakeT3Client()
        quiet = snap("c2", age_secs=300, user_text="start",
                     requested_age_secs=300)
        live = snap("c2", activities=[act("tool.started", age_secs=1, call="c1")],
                    age_secs=1)
        done = snap("c2", state="completed", text="finished")
        fake.scripts["c2"] = [quiet, live, done]
        out = t3exec.watch_turn(fake, "c2", now_fn=lambda: NOW.timestamp(),
                                sleep_fn=lambda s: None, silence_secs=60,
                                timeout_secs=30)
        self.assertEqual(out["state"], "completed", out)
        self.assertEqual(out["assistant_text"], "finished")

    def test_explicit_provider_error_acts_at_once(self):
        now = NOW.timestamp()
        err = t3exec.evaluate_turn(
            snap("t", state="running", session_status="error",
                 last_error="You have hit your usage limit"), now=now)
        self.assertEqual(err["state"], "error")
        self.assertEqual(err["signal"], "exhausted")
        tool_err = t3exec.evaluate_turn(
            snap("t", activities=[{**act("tool.bash"), "tone": "error",
                                          "summary": "RateLimitError: slow down"}]),
            now=now)
        self.assertEqual(tool_err["state"], "error")
        self.assertEqual(tool_err["signal"], "overloaded")

    def test_error_classification(self):
        self.assertEqual(t3exec.classify_provider_error("FreeUsageLimitError"), "exhausted")
        self.assertEqual(t3exec.classify_provider_error("HTTP 503 busy"), "overloaded")
        self.assertEqual(t3exec.classify_provider_error("401 Unauthorized"), "hard")
        self.assertIsNone(t3exec.classify_provider_error("all good"))
        self.assertIsNone(t3exec.classify_provider_error(None))


class Restart(Base):
    def test_post_restart_actions(self):
        interrupted = snap("t", state="interrupted")
        # An open tool call: running regardless of wall-clock age.
        running = snap("t", state="running",
                       activities=[act("tool.started", age_secs=2, call="c1")])
        self.assertEqual(
            t3exec.post_restart_action("codex", interrupted)["action"],
            "continue")
        self.assertEqual(
            t3exec.post_restart_action("claude", interrupted)["action"],
            "continue")
        self.assertEqual(
            t3exec.post_restart_action("opencode", interrupted)["action"],
            "continue")
        self.assertEqual(
            t3exec.post_restart_action("grok", interrupted)["action"],
            "resend")
        self.assertEqual(
            t3exec.post_restart_action("grok", running)["action"],
            "continue")

    def test_existing_thread_is_adopted_not_created(self):
        self.submit_t3("r1x")
        fake = FakeT3Client()
        controller._save_t3_thread(self.sd, "r1x", "dispatch",
                                   "sub.planner-1.adopted", "luna/max")
        fake.scripts["sub.planner-1.adopted"] = snap(
            "sub.planner-1.adopted", state="completed",
            text=luna_impl_envelope())
        res = controller.dispatch(self.sd, "r1x", t3_client=fake)
        self.assertEqual(res["action"], "dispatched", res)
        self.assertEqual(res["t3_thread_id"], "sub.planner-1.adopted")
        self.assertEqual([c for c in fake.commands
                          if c["type"] == "thread.create"], [])


class Discovery(Base):
    def test_url_and_token_precedence(self):
        with mock.patch.dict("os.environ",
                             {"T3_SERVER_URL": "http://env:9999",
                              "T3_SERVER_TOKEN": "env-token"}, clear=False):
            self.assertEqual(t3exec.discover_server_url(), "http://env:9999")
            self.assertEqual(t3exec.discover_server_url("http://x:1/"),
                             "http://x:1")
            self.assertEqual(t3exec.discover_token(), "env-token")
            self.assertEqual(t3exec.discover_token("explicit"), "explicit")

    def test_missing_token_blocks_loudly(self):
        env = {k: v for k, v in
               __import__("os").environ.items() if k != "T3_SERVER_TOKEN"}
        with mock.patch.dict("os.environ", env, clear=True):
            with mock.patch.dict("os.environ", {"T3_BIN": "/nonexistent/t3"}):
                with self.assertRaises(t3exec.T3Error):
                    t3exec.discover_token()


class Wire(unittest.TestCase):
    """Real HTTP client against a fake orchestration server."""

    def test_dispatch_snapshot_and_auth(self):
        seen = {}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _auth(self):
                return self.headers.get("Authorization") == "Bearer sekrit"

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                seen["post_path"] = self.path
                seen["post_auth"] = self.headers.get("Authorization")
                seen["post_body"] = json.loads(body or b"{}")
                if not self._auth():
                    self.send_response(401)
                    self.end_headers()
                    self.wfile.write(b'{"code":"auth_invalid"}')
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"sequence": 7}')

            def do_GET(self):
                seen["get_path"] = self.path
                if not self._auth():
                    self.send_response(401)
                    self.end_headers()
                    self.wfile.write(b'{"code":"auth_invalid"}')
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                payload = {"snapshotSequence": 3,
                           "thread": {"id": "planner-1",
                                      "projectId": "proj-1", "messages": [],
                                      "activities": [], "session": None,
                                      "latestTurn": None}}
                self.wfile.write(json.dumps(payload).encode())

        server = HTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        url = f"http://127.0.0.1:{port}"
        client = t3exec.T3Client(url, "sekrit")
        res = client.dispatch({"type": "thread.turn.start",
                               "commandId": "c1", "threadId": "planner-1",
                               "message": {"messageId": "m1", "role": "user",
                                           "text": "hi", "attachments": []},
                               "runtimeMode": "auto",
                               "interactionMode": "plan",
                               "createdAt": "2026-09-25T00:00:00+00:00"})
        self.assertEqual(res, {"sequence": 7})
        self.assertEqual(seen["post_path"], "/api/orchestration/dispatch")
        self.assertEqual(seen["post_auth"], "Bearer sekrit")
        self.assertEqual(seen["post_body"]["type"], "thread.turn.start")
        snap_ = client.thread_snapshot("planner-1")
        self.assertEqual(seen["get_path"],
                         "/api/orchestration/threads/planner-1")
        self.assertEqual(snap_["thread"]["projectId"], "proj-1")
        bad = t3exec.T3Client(url, "wrong")
        with self.assertRaises(t3exec.T3Error):
            bad.thread_snapshot("planner-1")
        down = t3exec.T3Client("http://127.0.0.1:1", "sekrit",
                               timeout_secs=1)
        with self.assertRaises(t3exec.T3Error):
            down.thread_snapshot("planner-1")


class IsolatedServerFlow(Base):
    """Full controller flow over real HTTP against an isolated server.

    A scripted fake orchestration server on a free port (never the
    user's 3773 instance) serves the real production ``T3Client``:
    submit -> dispatcher child -> terminal post into the planner thread.
    """

    def _serve(self, replies):
        received = {"dispatch": [], "snapshots": 0}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, obj, code=200):
                body = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _authed(self):
                return (self.headers.get("Authorization")
                        == "Bearer isolated-token")

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                cmd = json.loads(self.rfile.read(length) or b"{}")
                if not self._authed():
                    return self._send({"code": "auth_invalid"}, 401)
                received["dispatch"].append(cmd)
                if cmd.get("type") == "thread.create":
                    threads[cmd["threadId"]] = {
                        "id": cmd["threadId"],
                        "projectId": cmd["projectId"],
                        "messages": [], "activities": [],
                        "parent": cmd.get("parentThreadId"),
                        "selection": cmd.get("modelSelection"),
                        "turns": 0, "turn_state": "completed",
                        "assistant": None}
                elif cmd.get("type") == "thread.turn.start":
                    th = threads.get(cmd["threadId"])
                    if th is None:
                        return self._send({"code": "not_found"}, 404)
                    th["messages"].append(cmd["message"])
                    th["turns"] += 1
                    label = ("dispatcher" if "dispatcher" in
                             cmd["message"]["text"] else "worker")
                    th["assistant"] = replies[label]
                    th["turn_state"] = "completed"
                elif cmd.get("type") == "thread.turn.interrupt":
                    th = threads.get(cmd["threadId"])
                    if th is not None:
                        th["turn_state"] = "interrupted"
                return self._send({"sequence": len(received["dispatch"])})

            def do_GET(self):
                if not self._authed():
                    return self._send({"code": "auth_invalid"}, 401)
                received["snapshots"] += 1
                tid = self.path.rsplit("/", 1)[-1]
                if tid == PLANNER:
                    return self._send({
                        "snapshotSequence": 1,
                        "thread": {"id": PLANNER, "projectId": PROJECT,
                                   "messages": [], "activities": [],
                                   "session": None, "latestTurn": None}})
                th = threads.get(tid)
                if th is None:
                    return self._send({"code": "not_found"}, 404)
                messages = list(th["messages"])
                if th["assistant"] is not None:
                    messages = messages + [{
                        "id": "m-a", "role": "assistant",
                        "text": th["assistant"], "turnId": "t1",
                        "streaming": False,
                        "createdAt": iso(NOW), "updatedAt": iso(NOW)}]
                return self._send({
                    "snapshotSequence": 1,
                    "thread": {"id": tid, "projectId": th["projectId"],
                               "messages": messages, "activities": [],
                               "session": {"threadId": tid,
                                           "status": "running" if
                                           th["turn_state"] == "running"
                                           else "ready",
                                           "activeTurnId": None,
                                           "lastError": None,
                                           "updatedAt": iso(NOW)},
                               "latestTurn": {"turnId": "t1",
                                              "state": th["turn_state"],
                                              "requestedAt": iso(NOW),
                                              "startedAt": iso(NOW),
                                              "completedAt": iso(NOW),
                                              "assistantMessageId": "m-a"}}})

        threads = {PLANNER: {"id": PLANNER, "projectId": PROJECT,
                               "messages": [], "activities": [],
                               "parent": None, "selection": None,
                               "turns": 0, "turn_state": "completed",
                               "assistant": "noted"}}
        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        url = f"http://127.0.0.1:{server.server_address[1]}"
        self.assertNotEqual(server.server_address[1], 3773)
        return url, threads, received

    def test_submit_dispatch_terminal_over_http(self):
        url, threads, received = self._serve({
            "dispatcher": luna_impl_envelope(),
            "worker": "Done."})
        rid = "iso1"
        core.submit(self.sd, rid, {"goal": "t"}, self.ws(rid),
                    "session-iso1", planner_t3_thread=PLANNER,
                    t3_server_url=url,
                    handoff_summary="Isolated flow.")
        client = t3exec.T3Client(url, "isolated-token")
        res = controller.dispatch(self.sd, rid, t3_client=client)
        self.assertEqual(res["action"], "dispatched", res)
        child = res["t3_thread_id"]
        self.assertTrue(child.startswith(f"sub.{PLANNER}."))
        self.assertEqual(threads[child]["parent"], PLANNER)
        self.assertEqual(threads[child]["selection"]["model"],
                         "gpt-5.6-luna")
        first = threads[child]["messages"][0]["text"]
        for needle in (rid, "dispatcher", PLANNER):
            self.assertIn(needle, first)
        controller._mark_blocked(self.sd, rid, "iso1 blocked: witness")
        out = controller.deliver_terminal_report(self.sd, rid,
                                                 t3_client=client)
        self.assertEqual(out["action"], "reported", out)
        planner_turns = [c for c in received["dispatch"]
                         if c.get("type") == "thread.turn.start"
                         and c.get("threadId") == PLANNER]
        self.assertEqual(len(planner_turns), 1)
        self.assertIn("iso1 blocked", planner_turns[0]["message"]["text"])
        self.assertGreater(received["snapshots"], 0)


if __name__ == "__main__":
    unittest.main()
