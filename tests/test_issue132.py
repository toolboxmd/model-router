"""Prism jobs never go silent (toolboxmd/model-router#132).

Deterministic: every T3 call runs against the in-memory fake T3 client.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runner import controller, core, store, t3exec, t3salvage, t3snapshot  # noqa: E402
from tests.fakes import (NOW, FakeT3Client, isolate_t3_env, snap,  # noqa: E402
                         use_fake_t3)


def setUpModule():
    isolate_t3_env()


APPROVE = {"verdict": "approve", "findings": ""}
CHANGES = {"verdict": "request_changes", "findings": "secret leaks into proof.log"}
PLANNER = "planner-t3"


def git(ws, *args):
    subprocess.run(["git", *args], cwd=str(ws), check=True, capture_output=True)


def events(sd, rid, kind):
    con = store.connect(sd)
    try:
        rows = con.execute("SELECT payload_json FROM events WHERE request_id=? AND kind=?"
                           " ORDER BY id", (rid, kind)).fetchall()
    finally:
        con.close()
    return [json.loads(r["payload_json"] or "{}") for r in rows]


def update_job(sd, rid, **cols):
    con = store.connect(sd)
    try:
        sets = ", ".join(f"{k}=?" for k in cols)
        con.execute(f"UPDATE jobs SET {sets} WHERE request_id=?", (*cols.values(), rid))
        con.commit()
    finally:
        con.close()


def set_state(sd, rid, **fields):
    job = core.get_job(sd, rid)
    st = json.loads(job.get("controller_state") or "{}")
    st.update(fields)
    update_job(sd, rid, controller_state=json.dumps(st, sort_keys=True), status="running")


class SettlesLater(FakeT3Client):
    """Applies an interrupt only after ``delay_reads`` more snapshot reads,
    like T3 applying ``thread.turn.interrupt`` asynchronously."""

    def __init__(self, *a, delay_reads=3, **k):
        super().__init__(*a, **k)
        self.delay_reads = delay_reads
        self.pending = {}

    def dispatch(self, command):
        self.commands.append(dict(command))
        if command.get("type") == "thread.turn.interrupt":
            self.pending[command["threadId"]] = self.delay_reads
            return {"sequence": len(self.commands)}
        self.commands.pop()
        return super().dispatch(command)

    def thread_snapshot(self, thread_id):
        left = self.pending.get(thread_id)
        if left is not None:
            if left <= 0:
                self.pending.pop(thread_id)
                self.scripts[thread_id] = snap(thread_id, state="interrupted")
            else:
                self.pending[thread_id] = left - 1
                self.reads[thread_id] = self.reads.get(thread_id, 0) + 1
                return snap(thread_id, state="running")
        return super().thread_snapshot(thread_id)


class ControllerLoop(unittest.TestCase):
    """Continuation follows the job's status, never an action allowlist."""

    def setUp(self):
        t3snapshot.reset()
        self.addCleanup(t3snapshot.reset)
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        self.sd = str(base / "state")
        self.ws = base / "ws"
        self.ws.mkdir()
        git(self.ws, "init")
        git(self.ws, "config", "user.email", "test@example.test")
        git(self.ws, "config", "user.name", "Test")
        (self.ws / "a.txt").write_text("one\n")
        git(self.ws, "add", "-A")
        git(self.ws, "commit", "-m", "one")
        self.addCleanup(self._release_lock)

    def _release_lock(self):
        fd = controller._LOCK.get("fd")
        if fd is not None:
            os.close(fd)
            controller._LOCK["fd"] = None
        controller._LEASE["token"] = None

    def job_at_completion(self, rid, replies):
        core.submit(self.sd, rid, {"goal": "x", "proof": "true"}, str(self.ws), "p",
                    planner_t3_thread=PLANNER)
        fake = use_fake_t3(self, self.sd, rid, replies=list(replies))
        job = core.get_job(self.sd, rid)
        full = {"assistant_text": "did stuff", "usage": None, "native_ids": {},
                "finish": "stop", "actual_model": None}
        controller._write_turn_report(self.sd, rid, job, 1, "muse-spark-xhigh-free",
                                      full, "ses")
        set_state(self.sd, rid, seq=1, last_action_name="completion",
                  last_action={"action": "completion", "output": "DONE"})
        update_job(self.sd, rid, owner_token="tok")
        return fake

    def test_review_changes_run_correction_rereview_and_approve_in_one_run(self):
        fake = self.job_at_completion(
            "rc", [CHANGES, "fixed the leak",
                   {"action": "completion", "output": "DONE AGAIN"}, APPROVE])
        first_head = core._workspace_head(str(self.ws))
        post = fake.post_message

        def worker_commits(thread_id, text, **kw):
            # The correction worker fixes and commits: a new head to review.
            if "worker seq" in text:
                (self.ws / "a.txt").write_text("two\n")
                git(self.ws, "commit", "-am", "fix")
            return post(thread_id, text, **kw)

        fake.post_message = worker_commits
        self.assertEqual(controller.run_controller_process(self.sd, "rc", "tok"), 0)
        job = core.get_job(self.sd, "rc")
        self.assertEqual(job["status"], "succeeded", job.get("block_reason"))
        new_head = core._workspace_head(str(self.ws))
        self.assertNotEqual(first_head, new_head)
        verdicts = events(self.sd, "rc", "review_verdict")
        self.assertEqual([(v["round"], v["verdict"], v["reviewed_sha"]) for v in verdicts],
                         [(1, "request_changes", first_head), (2, "approve", new_head)])
        # One controller run did it all: launched once, exited once.
        self.assertEqual(len(events(self.sd, "rc", "controller_exited")), 1)
        # Every watched turn left its stream statistics in the ledger.
        stream = events(self.sd, "rc", "t3_turn_stream")
        self.assertTrue(stream)
        self.assertTrue(all("max_silence" in e and "first_token_secs" in e
                            and "window" in e for e in stream), stream)

    def test_step_stop_with_active_job_blocks_with_named_reason(self):
        self.job_at_completion("un", [])
        with mock.patch.object(controller, "step",
                               return_value={"action": "blocked", "reason": "odd"}):
            controller.run_controller_process(self.sd, "un", "tok")
        job = core.get_job(self.sd, "un")
        self.assertEqual(job["status"], "blocked")
        self.assertEqual(job["block_reason"], "controller_exit_unhandled: blocked (odd)")

    def test_unlisted_action_with_running_job_keeps_stepping(self):
        self.job_at_completion("go", [])
        calls = []

        def fake_step(sd, rid, token=None):
            calls.append(1)
            if len(calls) == 3:
                controller._mark_blocked(sd, rid, "planner decides")
            return {"action": "some-new-action"}

        with mock.patch.object(controller, "step", side_effect=fake_step):
            controller.run_controller_process(self.sd, "go", "tok")
        self.assertEqual(len(calls), 3)
        self.assertEqual(core.get_job(self.sd, "go")["block_reason"], "planner decides")


class InterruptSettles(unittest.TestCase):
    """After an interrupt, poll until the turn settles."""

    def test_polls_until_the_delayed_interrupt_lands(self):
        fake = SettlesLater(planner=PLANNER, delay_reads=3)
        fake.scripts["w"] = snap("w", state="running")
        naps = []
        res = t3exec.interrupt_and_settle(fake, "w", sleep_fn=naps.append)
        self.assertTrue(res["settled"])
        self.assertEqual(res["state"], "interrupted")
        self.assertEqual(naps, [t3exec.T3_SETTLE_POLL_SECS] * 3)

    def test_stopped_session_without_active_turn_counts_as_settled(self):
        stopped = snap("w", state="running", session_status="stopped")
        stopped["thread"]["session"]["activeTurnId"] = None
        self.assertTrue(t3exec.turn_settled(stopped))
        self.assertFalse(t3exec.turn_settled(snap("w", state="running")))

    def test_no_latest_turn_with_an_active_session_turn_is_not_settled(self):
        live = snap("w", state="running")
        live["thread"]["latestTurn"] = None
        self.assertFalse(t3exec.turn_settled(live))
        live["thread"]["session"]["activeTurnId"] = None
        self.assertTrue(t3exec.turn_settled(live))

    def test_active_session_turn_outranks_a_terminal_latest_turn(self):
        newer = snap("w", state="completed", text="old")
        newer["thread"]["session"]["activeTurnId"] = "t2"
        self.assertFalse(t3exec.turn_settled(newer))

    def test_cancel_interrupts_when_session_runs_a_newer_turn(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "c3", {"goal": "t"}, str(ws), "s", planner_t3_thread=PLANNER)
        update_job(sd, "c3", controller_state=json.dumps({"t3_threads": {"dispatch": {
            "thread_id": "sub.planner-t3.d", "route": "luna/max", "created": True,
            "turn_started": True, "turn_state": "completed"}}}))
        fake = SettlesLater(planner=PLANNER, delay_reads=1)
        newer = snap("sub.planner-t3.d", state="completed", text="old")
        newer["thread"]["session"]["activeTurnId"] = "t2"
        fake.scripts["sub.planner-t3.d"] = newer
        with mock.patch.object(t3exec, "client_for_job", return_value=fake), \
             mock.patch.object(t3exec.time, "sleep"):
            job = core.cancel(sd, "c3")
        self.assertEqual(job["status"], "cancelled")
        self.assertEqual(sum(c.get("type") == "thread.turn.interrupt"
                             for c in fake.commands), 1)

    def test_cancel_interrupts_a_live_turn_missing_from_latest_turn(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "c2", {"goal": "t"}, str(ws), "s", planner_t3_thread=PLANNER)
        update_job(sd, "c2", controller_state=json.dumps({"t3_threads": {"dispatch": {
            "thread_id": "sub.planner-t3.d", "route": "luna/max", "created": True,
            "turn_started": True, "turn_state": "running"}}}))
        fake = SettlesLater(planner=PLANNER, delay_reads=1)
        live = snap("sub.planner-t3.d", state="running")
        live["thread"]["latestTurn"] = None
        fake.scripts["sub.planner-t3.d"] = live
        with mock.patch.object(t3exec, "client_for_job", return_value=fake), \
             mock.patch.object(t3exec.time, "sleep"):
            job = core.cancel(sd, "c2")
        self.assertEqual(job["status"], "cancelled")
        self.assertEqual(sum(c.get("type") == "thread.turn.interrupt"
                             for c in fake.commands), 1)

    def test_gives_up_after_the_cap(self):
        fake = SettlesLater(planner=PLANNER, delay_reads=10 ** 6)
        clock = [0.0]

        def nap(secs):
            clock[0] += secs

        res = t3exec.interrupt_and_settle(fake, "w", sleep_fn=nap,
                                          now_fn=lambda: clock[0])
        self.assertFalse(res["settled"])
        self.assertGreaterEqual(clock[0], t3exec.T3_SETTLE_MAX_SECS)

    def test_worker_stall_path_confirms_idle_after_delayed_interrupt(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "st", {"goal": "t"}, str(ws), "s", planner_t3_thread=PLANNER)
        fake = SettlesLater(planner=PLANNER, delay_reads=3)
        job = core.get_job(sd, "st")
        seen = {}

        def capture(state_dir, request_id, job, route, seq, artifact, full,
                    thread_id, rc, source=None):
            seen.update(full)
            return {"action": "captured"}

        stalled = {"state": "stalled", "silence": 400.0, "last_part": "message:assistant",
                   "probed": True, "thread_id": "sub.planner-t3.w1"}
        with mock.patch.object(controller, "_t3_run_turn", return_value=stalled), \
             mock.patch.object(controller, "_finish_worker_turn", side_effect=capture), \
             mock.patch.object(t3exec.time, "sleep"):
            fake.scripts["sub.planner-t3.w1"] = snap("sub.planner-t3.w1", state="running")
            controller._run_t3_worker_turn(sd, "st", job, str(ws), "muse-spark-xhigh-free",
                                           "p", 1, "r", t3_client=fake)
        self.assertEqual(seen.get("signal"), "stalled")
        # A single read right after the interrupt still saw the live turn;
        # polling waits for T3 to apply it, so the stall may move routes.
        self.assertTrue(seen.get("idle_confirmed"), seen)


class Liveness(unittest.TestCase):
    """Measured per-driver windows; host sleep is never silence."""

    def running(self, instance, age):
        s = snap("w", state="running", text="thinking", age_secs=age,
                 requested_age_secs=age + 5)
        s["thread"]["modelSelection"] = {"instanceId": instance, "model": "m"}
        return s

    def test_window_follows_the_thread_driver(self):
        now = NOW.timestamp()
        for instance, window in (("codex", 300.0), ("opencode", 210.0),
                                 ("claudeAgent", 90.0), ("grok", 300.0)):
            quiet = t3exec.evaluate_turn(self.running(instance, window - 5), now=now)
            self.assertEqual(quiet["state"], "running", instance)
            dead = t3exec.evaluate_turn(self.running(instance, window + 5), now=now)
            self.assertEqual(dead["state"], "stalled", instance)
            self.assertEqual(dead["window"], window)

    def test_old_sixty_second_window_no_longer_stalls_opencode(self):
        # #132 job -2: 105.7 s of wall-clock silence on an OpenCode worker.
        ev = t3exec.evaluate_turn(self.running("opencode", 105.7), now=NOW.timestamp())
        self.assertEqual(ev["state"], "running")

    def test_host_sleep_is_not_silence(self):
        # The last token 4 s before a 99 s sleep: 103 s of wall clock, 4 s awake.
        wall = [NOW.timestamp()]
        mono = [1000.0]
        fake = FakeT3Client(planner=PLANNER)
        s = self.running("claudeAgent", 4)
        fake.scripts["w"] = s
        reads = []

        def nap(secs):
            reads.append(secs)
            if len(reads) == 1:
                wall[0] += 99.0 + secs  # the host sleeps: only wall time moves
                mono[0] += secs
            elif len(reads) >= 3:
                fake.scripts["w"] = snap("w", state="completed", text="done")
            else:
                wall[0] += secs
                mono[0] += secs

        out = t3exec.watch_turn(fake, "w", now_fn=lambda: wall[0],
                                mono_fn=lambda: mono[0], sleep_fn=nap)
        self.assertEqual(out["state"], "completed", out)
        self.assertGreaterEqual(out["stream"]["slept"], 99.0)
        self.assertLess(out["stream"]["max_silence"], 20.0)

    def test_awake_silence_past_window_still_stalls(self):
        wall = [NOW.timestamp()]
        fake = FakeT3Client(planner=PLANNER)
        fake.scripts["w"] = self.running("claudeAgent", 0)

        def nap(secs):
            wall[0] += 50.0

        out = t3exec.watch_turn(fake, "w", now_fn=lambda: wall[0],
                                mono_fn=lambda: wall[0], sleep_fn=nap)
        self.assertEqual(out["state"], "stalled")
        self.assertGreaterEqual(out["silence"], 90.0)
        self.assertEqual(out["stream"]["slept"], 0.0)

    def test_first_token_secs(self):
        s = snap("w", state="completed", text="hi", age_secs=0, requested_age_secs=7)
        self.assertAlmostEqual(t3exec.first_token_secs(s), 7.0, places=2)

    def test_awake_secs_subtracts_only_overlap(self):
        self.assertEqual(t3exec.awake_secs(0, 100, [(50, 149)]), 50)
        self.assertEqual(t3exec.awake_secs(0, 100, [(-50, -10)]), 100)


class StreamLiveness(unittest.TestCase):
    running = Liveness.running
    def watch(self, liveness, age=1):
        fake = FakeT3Client(planner=PLANNER)
        fake.liveness = liveness
        fake.scripts["w"] = self.running("codex", age)
        clock = [NOW.timestamp()]
        naps = []

        def nap(seconds):
            naps.append(seconds)
            clock[0] += seconds
            if len(naps) == 3:
                fake.scripts["w"] = snap("w", state="completed", text="done")

        result = t3exec.watch_turn(fake, "w", now_fn=lambda: clock[0], sleep_fn=nap)
        return result, fake, naps

    def test_stale_true_returns_in_one_poll_without_router_confirmation(self):
        evidence = {"stale": True, "staleSince": NOW.isoformat(),
                    "silenceMs": 20000, "thresholdMs": 18000,
                    "thresholdSource": "measured", "reason": "stream silent"}
        result, fake, naps = self.watch(evidence)
        self.assertEqual(result["state"], "stalled")
        self.assertEqual(result["liveness"], evidence)
        self.assertEqual(fake.liveness_reads, ["w"])
        self.assertEqual(fake.reads["w"], 1)
        self.assertEqual(naps, [])

    def test_stale_false_never_uses_router_silence_window(self):
        result, fake, naps = self.watch({"stale": False, "silenceMs": 3600000}, age=3600)
        self.assertEqual(result["state"], "completed")
        self.assertEqual(naps, [1.0, 1.0, 1.0])
        self.assertGreaterEqual(len(fake.liveness_reads), 3)

    def test_missing_field_and_unreachable_route_use_driver_fallback(self):
        for liveness in ({"silenceMs": 3600000},
                         t3exec.T3Error("offline", unreachable=True)):
            with self.subTest(liveness=liveness):
                result, fake, naps = self.watch(liveness, age=301)
                self.assertEqual(result["state"], "stalled")
                self.assertEqual(result["window"], 300.0)
                self.assertEqual(result["liveness"]["thresholdMs"], 300000)
                self.assertEqual(result["liveness"]["thresholdSource"], "router-fallback")
                self.assertTrue(result["probed"])
                self.assertEqual(naps, [])

    def test_client_gets_liveness_for_the_worker_thread(self):
        client = t3exec.T3Client("http://127.0.0.1:9", "fake")
        with mock.patch.object(client, "_request", return_value={"stale": False}) as request:
            self.assertEqual(client.prism_liveness("sub.p.w"), {"stale": False})
        request.assert_called_once_with("GET", "/api/prism/liveness?threadId=sub.p.w")

    def test_stale_false_while_waiting_for_new_turn_does_not_use_local_deadline(self):
        fake = FakeT3Client(planner=PLANNER)
        fake.liveness = {"stale": False}
        fake.scripts["w"] = snap("w", state="completed", text="old", turn="old")
        clock = [NOW.timestamp()]
        naps = []

        def nap(seconds):
            clock[0] += 3600
            naps.append(seconds)
            if len(naps) == 2:
                fake.scripts["w"] = snap("w", state="completed", text="new", turn="new")

        result = t3exec.watch_turn(fake, "w", now_fn=lambda: clock[0], sleep_fn=nap,
                                   prior_turn_id="old", await_new_turn=True)
        self.assertEqual(result["assistant_text"], "new")
        self.assertEqual(naps, [1.0, 1.0])

    def test_route_healthy_at_fallback_confirmation_cancels_stall(self):
        fake = FakeT3Client(planner=PLANNER)
        fake.scripts["w"] = self.running("codex", 3600)
        with mock.patch.object(fake, "prism_liveness", side_effect=[{}, {"stale": False}, {"stale": False}]):
            result = t3exec.watch_turn(fake, "w", now_fn=lambda: NOW.timestamp(),
                                       sleep_fn=lambda _: fake.complete("w", "done"))
        self.assertEqual(result["state"], "completed")

    def test_stale_prior_turn_does_not_hide_behind_a_queued_message(self):
        fake = FakeT3Client(planner=PLANNER)
        fake.liveness = {"stale": True, "reason": "provider stopped"}
        fake.scripts["w"] = snap("w", state="running", turn="prior")
        naps = []

        def nap(seconds):
            naps.append(seconds)
            fake.scripts["w"] = snap("w", state="completed", turn="new")

        result = t3exec.watch_turn(fake, "w", now_fn=lambda: NOW.timestamp(),
                                   sleep_fn=nap, prior_turn_id="prior", await_new_turn=True)
        self.assertEqual(result["state"], "stalled")
        self.assertEqual(naps, [])


class StaleEvidence(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.sd = str(Path(tmp.name) / "state")
        self.ws = Path(tmp.name) / "ws"
        self.ws.mkdir()
        core.submit(self.sd, "salvage", {"goal": "t"}, str(self.ws), "s",
                    planner_t3_thread=PLANNER)
        self.thread = "sub.planner-t3.worker"
        self.route = "muse-spark-xhigh-free"
        set_state(self.sd, "salvage", t3_threads={"impl_1": {
            "thread_id": self.thread, "route": self.route,
            "created": True, "turn_started": True, "turn_state": "running"}})
        self.artifact = self.ws / "review.log"
        self.artifact.write_text("request_changes: seven major findings\napi_key=secret-value\n")
        self.fake = FakeT3Client(planner=PLANNER)
        self.fake.liveness = {"stale": True, "staleSince": NOW.isoformat(),
                              "silenceMs": 23000, "thresholdMs": 18000,
                              "thresholdSource": "measured", "reason": "no stream events"}
        self.fake.scripts[self.thread] = snap(
            self.thread, state="running", text=f"Review found problems: `{self.artifact}`")

    def test_salvage_ledger_and_planner_post_precede_interrupt_and_survive_route_move(self):
        dispatch = self.fake.dispatch
        interrupts = []

        def check_before_interrupt(command):
            if command["type"] == "thread.turn.interrupt":
                records = controller._load_controller_state(core.get_job(self.sd, "salvage"))["stale_events"]
                saved = Path(records[0]["salvage"]["artifacts"][0]["saved"])
                self.assertIn("seven major findings", saved.read_text())
                self.assertNotIn("secret-value", saved.read_text())
                self.assertEqual(self.fake.posts[0][0], PLANNER)
                self.assertIn("seven major findings", self.fake.posts[0][1])
                self.artifact.unlink()  # The worker's temp artifact can now disappear.
                interrupts.append(command)
            return dispatch(command)

        self.fake.dispatch = check_before_interrupt
        with mock.patch.object(controller, "_move_after_signal", return_value={"action": "route_switched"}) as move:
            result = controller._run_t3_worker_turn(
                self.sd, "salvage", core.get_job(self.sd, "salvage"), str(self.ws),
                self.route, "work", 1, "test", t3_client=self.fake)
        self.assertEqual(result["action"], "route_switched")
        self.assertEqual(len(interrupts), 1)
        self.assertEqual(self.fake.liveness_reads, [self.thread])
        self.assertEqual(move.call_args.args[3], "stalled")
        self.assertTrue(Path(result["report"]["salvage_path"]).exists())
        evidence = events(self.sd, "salvage", "t3_turn_stale")
        self.assertEqual(len(evidence), 1)
        for field in ("silenceMs", "thresholdMs", "thresholdSource", "reason"):
            self.assertEqual(evidence[0][field], self.fake.liveness[field])
        self.assertTrue(events(self.sd, "salvage", "t3_stale_notification")[0]["posted"])
        # Subsequent work must not replace salvaged findings in the eventual report.
        controller._set_phase(self.sd, "salvage", implementation_output="later worker completed")
        update_job(self.sd, "salvage", status="blocked", block_reason="need judgment")
        report = controller.deliver_terminal_report(self.sd, "salvage", t3_client=self.fake)
        self.assertEqual(report["action"], "reported")
        terminal = self.fake.posts[-1][1]
        self.assertIn("seven major findings", terminal)
        self.assertIn(str(self.artifact), terminal)
        self.assertIn("Review found problems", terminal)
        self.assertNotIn("secret-value", terminal)

    def test_failed_notification_is_recorded_and_evidence_still_reaches_terminal_report(self):
        with mock.patch.object(self.fake, "post_message", side_effect=t3exec.T3Error("offline")):
            outcome = t3exec.watch_turn(self.fake, self.thread)
            controller._note_t3_turn(self.sd, "salvage", "impl_1", self.thread,
                                     self.route, outcome, self.fake)
        self.assertFalse(events(self.sd, "salvage", "t3_stale_notification")[0]["posted"])
        update_job(self.sd, "salvage", status="failed", block_reason="offline")
        controller.deliver_terminal_report(self.sd, "salvage", t3_client=self.fake)
        self.assertIn("seven major findings", self.fake.posts[-1][1])

    def test_every_stale_event_notifies_and_success_report_keeps_all_findings(self):
        for number in (1, 2):
            self.fake.scripts[self.thread] = snap(self.thread, text=f"finding {number}")
            outcome = t3exec.watch_turn(self.fake, self.thread)
            controller._note_t3_turn(self.sd, "salvage", f"impl_{number}", self.thread,
                                     self.route, outcome, self.fake)
        self.assertEqual([tid for tid, _ in self.fake.posts], [PLANNER, PLANNER])
        self.assertEqual(len(events(self.sd, "salvage", "t3_turn_stale")), 2)
        update_job(self.sd, "salvage", status="succeeded")
        controller.deliver_terminal_report(self.sd, "salvage", t3_client=self.fake)
        self.assertIn("finding 1", self.fake.posts[-1][1])
        self.assertIn("finding 2", self.fake.posts[-1][1])

    def test_artifact_in_tool_output_is_preserved_and_unsafe_reference_is_not_read(self):
        snapshot = snap(self.thread, text="Review is written.", activities=[{
            "turnId": "t1", "payload": {"output": f"Saved `{self.artifact}`"}}])
        result = t3salvage.capture(snapshot, "", str(self.ws), self.ws / "saved")
        self.assertIn("seven major findings", result["artifacts"][0]["excerpt"])
        self.assertNotIn("secret-value", json.dumps(result))
        snapshot = snap(self.thread, text="Reference `/etc/hosts.txt`")
        result = t3salvage.capture(snapshot, "", str(self.ws), self.ws / "saved2")
        self.assertIn("reference only", result["artifacts"][0]["note"])

    def test_bare_artifact_and_spaced_markdown_path_are_copied(self):
        (self.ws / "review notes.md").write_text("Important review findings.")
        snapshot = snap(self.thread, text="See `review.log` and [notes](review notes.md).")
        result = t3salvage.capture(snapshot, "", str(self.ws), self.ws / "saved")
        self.assertEqual(len(result["artifacts"]), 2)
        self.assertIn("seven major findings", result["artifacts"][0]["excerpt"])
        self.assertIn("Important review findings", result["artifacts"][1]["excerpt"])


class CancelWithT3Unreachable(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(self.sd, "c1", {"goal": "t"}, str(ws), "s", planner_t3_thread=PLANNER)

    def saved(self, **turn_states):
        threads = {slot: {"thread_id": f"sub.planner-t3.{slot}", "route": "luna/max",
                          "created": True, "turn_started": True, "turn_state": state}
                   for slot, state in turn_states.items()}
        update_job(self.sd, "c1", controller_state=json.dumps({"t3_threads": threads}))

    def cancel_unreachable(self):
        with mock.patch.object(t3exec, "client_for_job",
                               side_effect=t3exec.T3Error("no T3 bearer token")):
            return core.cancel(self.sd, "c1")

    def test_all_turns_terminal_finalizes_cancelled_and_frees_workspace(self):
        self.saved(dispatch="completed", impl_1="interrupted", review_1="error")
        job = self.cancel_unreachable()
        self.assertEqual(job["status"], "cancelled")
        self.assertNotIn(job["status"], store.ACTIVE_WORKSPACE_STATUSES)

    def test_live_turn_names_t3_unreachability(self):
        self.saved(dispatch="completed", impl_1="running")
        job = self.cancel_unreachable()
        self.assertEqual(job["status"], "cancelling")
        self.assertTrue(job["block_reason"].startswith(
            "cancellation_pending: T3 unreachable: no T3 bearer token"), job["block_reason"])
        self.assertNotIn("process group", job["block_reason"])
        failed = events(self.sd, "c1", "t3_turn_interrupt_failed")
        self.assertEqual([(e["slot"], e["last_known_state"]) for e in failed],
                         [("impl_1", "running")])


class TurnStateRecord(unittest.TestCase):
    def test_saved_thread_keeps_route_when_a_note_has_none(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "n1", {"goal": "t"}, str(ws), "s", planner_t3_thread=PLANNER)
        controller._save_t3_thread(sd, "n1", "dispatch", "sub.x", "luna/max",
                                   created=True, turn_started=True)
        rec = controller._t3_thread_for(core.get_job(sd, "n1"), "dispatch")
        self.assertEqual(rec["turn_state"], "running")
        controller._note_t3_turn(sd, "n1", "dispatch", "sub.x", None,
                                 {"state": "completed",
                                  "stream": {"max_silence": 3.0, "first_token_secs": 1.0,
                                             "window": 300.0, "slept": 0.0}})
        rec = controller._t3_thread_for(core.get_job(sd, "n1"), "dispatch")
        self.assertEqual((rec["route"], rec["turn_state"]), ("luna/max", "completed"))
        self.assertEqual(events(sd, "n1", "t3_turn_stream")[0]["max_silence"], 3.0)


if __name__ == "__main__":
    unittest.main()
