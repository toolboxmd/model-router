"""Issue #33: stall detection from stream activity, assumed resets, probes.

Deterministic fakes only. No live model CLIs. The silence window is
overridden per test (policy default stays 180 seconds); since #88 there
is no outer per-turn timeout, so stall (genuine stream silence) is the
only time-based end for an active turn.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters, controller, core, harnesses, policy, store  # noqa: E402
from tests.fakes import FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable


def kill_pid(pid):
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        return


class StallPolicyData(unittest.TestCase):
    def test_silence_window_default_with_evidence(self):
        self.assertEqual(policy.STALL_SILENCE_SECS, 180)
        self.assertIn("321", policy.STALL_EVIDENCE)
        self.assertIn("110", policy.STALL_EVIDENCE)
        self.assertIn("#33", policy.STALL_EVIDENCE)
        self.assertGreater(policy.STALL_POLL_SECS, 0)

    def test_signal_classes_cover_stalled_and_context(self):
        self.assertEqual(policy.SIGNAL_CLASSES["stalled"]["action"], "next_family")
        self.assertEqual(policy.SIGNAL_CLASSES["stalled"]["retries"],
                         policy.SIGNAL_CLASSES["overloaded"]["retries"])
        self.assertEqual(policy.SIGNAL_CLASSES["stalled"]["window_secs"],
                         policy.SIGNAL_CLASSES["overloaded"]["window_secs"])
        self.assertEqual(policy.SIGNAL_CLASSES["context"]["action"], "next_larger_context")
        # Stalled is detected from silence, never matched from provider text.
        self.assertNotEqual(policy.classify_signal(
            {"type": "retry", "action": {"reason": "rate_limit"}}), "stalled")
        self.assertEqual(policy.classify_signal(
            {"type": "retry", "action": {"reason": "rate_limit"}}), "overloaded")
        ctx = {"type": "error", "error": {"name": "APIError",
               "data": {"responseBody": "context_length_exceeded"}}}
        self.assertEqual(policy.classify_signal(ctx), "context")
        for name in ("AuthError", "RegionError", "DataPolicyError"):
            self.assertEqual(policy.classify_signal({"name": name}), "hard")

    def test_every_route_has_a_context_window(self):
        for route in policy.ROUTES:
            self.assertIsInstance(policy.route_context_window(route), int, route)
        self.assertLess(policy.route_context_window("muse-spark-xhigh-free"),
                        policy.route_context_window("glm-5.3-flash-go"))
        self.assertEqual(policy.next_larger_context_route(
            "muse-spark-xhigh-free", lane="default"), "glm-5.3-flash-go")
        # glm-5.1-go is the last default-lane route: nothing larger is left.
        self.assertIsNone(policy.next_larger_context_route("glm-5.1-go", lane="default"))
        with self.assertRaises(ValueError):
            policy.route_context_window("bogus/0")

    def test_assumed_reset_windows(self):
        now = 1758350000.0
        five = policy.assumed_reset_at("5h", now)
        self.assertAlmostEqual(core._parse_ts(five) - now, 5 * 3600, delta=1)
        week = policy.assumed_reset_at("weekly", now)
        self.assertAlmostEqual(core._parse_ts(week) - now, 7 * 24 * 3600, delta=1)
        month = policy.assumed_reset_at("monthly", now)
        self.assertEqual(month, "2025-10-01T00:00:00+00:00")
        # Unknown windows fall back to the 5-hour default.
        self.assertEqual(policy.assumed_reset_at("daily", now), five)

    def test_provider_reset_parsing(self):
        now = 1758350000.0
        # Codex resets_at in epoch seconds, verbatim.
        self.assertEqual(policy.parse_provider_reset({"resets_at": 1758353600}, now),
                         "2025-09-20T07:33:20+00:00")
        # Go Retry-After seconds: now plus the delay, the exact reset.
        got = policy.parse_provider_reset({"retry_after": 120}, now)
        self.assertAlmostEqual(core._parse_ts(got) - now, 120, delta=2)
        # Claude's reset time carried in message text.
        self.assertEqual(
            policy.parse_provider_reset({"message": "limit resets at 2026-09-21T03:00:00Z"}, now),
            "2026-09-21T03:00:00+00:00")
        # Zen free and Grok name no reset: the assumed rule applies.
        self.assertIsNone(policy.parse_provider_reset({"message": "try tomorrow"}, now))
        self.assertIsNone(policy.parse_provider_reset({"class": "GrokLimit"}, now))
        self.assertIsNone(policy.parse_provider_reset("HTTP 429", now))
        # A stray date inside a structured response body never invents one.
        self.assertIsNone(policy.parse_provider_reset(
            {"name": "APIError",
             "data": {"responseBody": "error at 2026-09-21T03:00:00Z"}}, now))

    def test_probe_schedule_lengthens_and_caps(self):
        self.assertEqual(policy.probe_delay_secs(0), 3600)
        self.assertEqual(policy.probe_delay_secs(1), 7200)
        self.assertEqual(policy.probe_delay_secs(2), 14400)
        self.assertEqual(policy.probe_delay_secs(9), 21600)


class StreamSeam(unittest.TestCase):
    def test_part_activity_tracks_creates_updates_and_tools(self):
        h = harnesses.harness_named("opencode")
        tracker = h.new_activity_tracker(now=100.0)
        base = {"user_1"}
        msgs = [{"info": {"id": "user_1", "role": "user"}, "parts": []}]
        changed, detail = h.note_part_activity(tracker, msgs, base, 101.0)
        self.assertFalse(changed)
        self.assertIsNone(detail["last_part_type"])
        self.assertEqual(tracker.silence(150.0), 50.0)
        # A text part arrives: activity, last part type recorded.
        msgs.append({"info": {"id": "a1", "role": "assistant"},
                     "parts": [{"type": "text", "text": "working"}]})
        changed, detail = h.note_part_activity(tracker, msgs, base, 160.0)
        self.assertTrue(changed)
        self.assertEqual(detail["last_part_type"], "text")
        self.assertEqual(tracker.silence(161.0), 1.0)
        self.assertEqual(tracker.longest, 60.0)
        # No change: silence grows.
        changed, _ = h.note_part_activity(tracker, msgs, base, 170.0)
        self.assertFalse(changed)
        # A reasoning part updates: activity again.
        msgs[-1]["parts"].append({"type": "reasoning", "text": "hmm"})
        changed, detail = h.note_part_activity(tracker, msgs, base, 180.0)
        self.assertTrue(changed)
        self.assertEqual(detail["last_part_type"], "reasoning")
        # A tool part in running state counts as activity while alive,
        # even with no new parts.
        msgs[-1]["parts"].append({"type": "tool", "state": "running"})
        for tick in (200.0, 400.0, 800.0):
            changed, detail = h.note_part_activity(tracker, msgs, base, tick)
            self.assertTrue(changed)
            self.assertTrue(detail["running_tool"])
        self.assertEqual(tracker.silence(801.0), 1.0)

    def test_cli_output_growth_is_activity(self):
        h = harnesses.harness_named("codex")
        tracker = h.new_activity_tracker(now=0.0)
        changed, _ = h.note_cli_output(tracker, "", "", 1.0)
        self.assertFalse(changed)
        changed, detail = h.note_cli_output(
            tracker, '{"type":"thread.started"}\n', "", 2.0)
        self.assertTrue(changed)
        self.assertEqual(detail["lines"], 2)
        changed, _ = h.note_cli_output(
            tracker, '{"type":"thread.started"}\n', "", 12.0)
        self.assertFalse(changed)
        self.assertEqual(tracker.silence(12.0), 10.0)
        self.assertEqual(tracker.longest, 2.0)
        # A per-invocation override wins over the policy default.
        self.assertEqual(h.stall_window_secs({"stall_secs": 3}), 3.0)
        # Per-harness policy data (#73): Codex dispatch waits out silent
        # max-effort reasoning, OpenCode workers keep the 180s default.
        self.assertEqual(h.stall_window_secs({}),
                         policy.STALL_SILENCE_SECS_BY_HARNESS["codex"])
        self.assertGreater(h.stall_window_secs({}), 181)
        oc = harnesses.harness_named("opencode")
        self.assertEqual(oc.stall_window_secs({}), policy.STALL_SILENCE_SECS)


class StallDrillBase(unittest.TestCase):
    def _setup(self, mode, timeout_secs=None, probe_mode=None, go_mode=None):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        self.sd = str(base / "state")
        self.ws = base / "ws"
        self.ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        self.fake_state = base / "fakestate"
        self.fake_state.mkdir()
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_OC_MODE",
                                                "FAKE_OC_MODE_GO", "FAKE_OC_PROBE_MODE")}
        def restore():
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.addCleanup(restore)
        os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
        os.environ["FAKE_STATE"] = str(self.fake_state)
        os.environ["FAKE_OC_MODE"] = mode
        if go_mode is not None:
            os.environ["FAKE_OC_MODE_GO"] = go_mode
        if probe_mode is not None:
            os.environ["FAKE_OC_PROBE_MODE"] = probe_mode
        core.submit(self.sd, "oc1", {"goal": "stall drill"}, str(self.ws), "planner")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='oc1'")
        finally:
            con.close()
        self.addCleanup(self._kill_groups)
        run = core.make_durable_run_cmd(self.sd, "oc1", "tok")
        def timed_run(cmd, cwd=None, timeout=None, **kw):
            return run(cmd, cwd, timeout_secs or timeout, **kw)
        return timed_run

    def _patch_window(self, secs):
        # The supervisor drives the turn in its own process, so the drill
        # window travels by environment (inherited across the spawn), while
        # production leaves RUNNER_STALL_SECS unset and uses the policy.
        prev = os.environ.get("RUNNER_STALL_SECS")
        os.environ["RUNNER_STALL_SECS"] = str(secs)
        def restore():
            if prev is None:
                os.environ.pop("RUNNER_STALL_SECS", None)
            else:
                os.environ["RUNNER_STALL_SECS"] = prev
        self.addCleanup(restore)

    def _kill_groups(self):
        try:
            invs = core._list_invocations(self.sd, "oc1")
        except Exception:
            return
        for inv in invs:
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass

    def _requests(self):
        f = self.fake_state / "opencode-requests.jsonl"
        return [json.loads(l) for l in f.read_text().splitlines()] if f.exists() else []

    def _worker_prompts(self):
        return [r for r in self._requests()
                if r["path"].endswith("/prompt_async")
                and "stall probe" not in (r["body"]["parts"][0]["text"] or "")]

    def _last_error(self):
        return json.loads(core.get_job(self.sd, "oc1")["last_error_json"] or "{}")


class TestStallDrill(StallDrillBase):
    def test_silent_turn_retries_bounded_then_moves_laterally(self):
        run = self._setup("stall")
        self._patch_window(2)
        started = time.monotonic()
        res1 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        elapsed1 = time.monotonic() - started
        # Within the window plus one poll and the probe: not the timeout.
        self.assertGreaterEqual(elapsed1, 2.0)
        self.assertLess(elapsed1, 15.0)
        self.assertEqual(res1["action"], "stalled_retry")
        self.assertEqual(core.get_job(self.sd, "oc1")["route"], "muse-spark-xhigh-free")
        # The ledger records stalled with the last-activity age and part type.
        last = self._last_error()
        self.assertEqual(last.get("signal"), "stalled")
        ev = last.get("evidence") or {}
        self.assertEqual(ev.get("source"), "stream_silence")
        self.assertGreater(ev.get("silence_secs", 0), 1.5)
        self.assertEqual(ev.get("last_part_type"), "text")
        self.assertEqual((ev.get("probe") or {}).get("signal"), "unknown")
        self.assertTrue(last.get("idle_confirmed"))
        self.assertIsNotNone(last.get("longest_silence_secs"))
        # The turn report, the invocation row, and the measurements agree.
        report = json.loads(Path(res1["report"]["report_path"]).read_text())
        self.assertEqual(report["status"], "stalled")
        self.assertIsNotNone(report.get("longest_silence_secs"))
        inv = [i for i in core._list_invocations(self.sd, "oc1")
               if i["kind"] == "opencode_control"][-1]
        self.assertEqual(inv["terminal_class"], "stalled")
        self.assertIsNotNone(inv["longest_silence_secs"])
        self.assertGreaterEqual(inv["longest_silence_secs"], 1.9)
        m = core.status_view(self.sd, "oc1")["job"]["measurements"][-1]
        self.assertIsNotNone(m["longest_silence_secs"])
        # Bounded: a second silent turn retries the same route once more,
        # the third moves laterally with the route degraded. No hot loop,
        # no duplicate writer: three turns, one session.
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "stalled_retry")
        res3 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res3["action"], res3["reason"]), ("route_switched", "lateral"))
        self.assertEqual(core.get_job(self.sd, "oc1")["route"], "glm-5.3-flash-go")
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        self.assertEqual(len(self._worker_prompts()), 3)
        job = core.get_job(self.sd, "oc1")
        self.assertTrue(job["opencode_session_id"].startswith("ses_"))

    def test_stall_probe_exhausted_moves_pools(self):
        run = self._setup("stall", probe_mode="exhausted")
        self._patch_window(2)
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res["action"], res["reason"]),
                         ("transferred_to_go", "pool_move"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-go")
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        ev = (self._last_error().get("evidence") or {})
        self.assertEqual((ev.get("probe") or {}).get("signal"), "exhausted")

    def test_stall_probe_overloaded_moves_family(self):
        run = self._setup("stall", probe_mode="overloaded")
        self._patch_window(2)
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "lateral"))
        self.assertEqual(core.get_job(self.sd, "oc1")["route"], "glm-5.3-flash-go")
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))


class TestAssumedResets(StallDrillBase):
    def test_limit_without_reset_skipped_then_retried_after_passing(self):
        run = self._setup("free_limit")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "transferred_to_go")
        rows = [r for r in core.list_capacity(self.sd)
                if r["route"] == "muse-spark-xhigh-free"]
        self.assertTrue(rows)
        # No provider reset: the 5-hour default is assumed and flagged, so
        # preflight skips the route for the window instead of forever.
        for row in rows:
            self.assertEqual(row["reset_source"], "assumed")
            self.assertIsNotNone(row["reset_at"])
            self.assertAlmostEqual(core._parse_ts(row["reset_at"]) - time.time(),
                                   5 * 3600, delta=120)
        move = controller._preflight_move(self.sd, "oc1", "muse-spark-xhigh-free")
        self.assertIsNotNone(move)
        self.assertEqual(move["reason"], "preflight_exhausted")
        # #34 reconciliation: after reset_at the route still needs one fresh
        # probe or one successful request before it is eligible again, so the
        # expired mark stays until revalidated and shows up as probe-due.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE capacity SET reset_at='2000-01-01T00:00:00+00:00'"
                        " WHERE route='muse-spark-xhigh-free'")
            con.execute("UPDATE jobs SET route='muse-spark-xhigh-free', status='running',"
                        " controller_state=? WHERE request_id='oc1'",
                        (json.dumps({"seq": 1,
                                     "last_action": {"action": "implementation"}}),))
        finally:
            con.close()
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        self.assertTrue([r for r in core.probe_due_routes(self.sd)
                         if r["route"] == "muse-spark-xhigh-free"])
        out = core.record_probe_outcome(self.sd, "muse-spark-xhigh-free",
                                        "unknown", True, {"revalidation": True})
        self.assertTrue(out["cleared"])
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        self.assertIsNone(controller._preflight_move(self.sd, "oc1", "muse-spark-xhigh-free"))
        os.environ["FAKE_OC_MODE"] = "ok"
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        self.assertTrue(Path(res2["report"]["report_path"]).exists())

    def test_provider_reset_stored_verbatim(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "pre1", {"g": 1}, str(ws), "p")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET route='muse-spark-xhigh-go',"
                        " lane='implementation_default', status='running'"
                        " WHERE request_id='pre1'")
        finally:
            con.close()
        future = time.time() + 3600
        res = controller._move_after_signal(sd, "pre1", "muse-spark-xhigh-go",
                                            "exhausted", {"resets_at": future})
        self.assertEqual(res["reason"], "lateral")
        rows = [r for r in core.list_capacity(sd) if r["route"] == "muse-spark-xhigh-go"]
        self.assertEqual(len(rows), 3)  # the three Go windows stay separate
        for row in rows:
            self.assertEqual(row["reset_source"], "provider")
            self.assertAlmostEqual(core._parse_ts(row["reset_at"]), future, delta=1)
        # A Retry-After delay is the exact reset: now plus the delay.
        con = store.connect(sd)
        try:
            con.execute("DELETE FROM capacity")
            con.execute("UPDATE jobs SET route='muse-spark-xhigh-go' WHERE request_id='pre1'")
        finally:
            con.close()
        before = time.time()
        controller._move_after_signal(sd, "pre1", "muse-spark-xhigh-go",
                                      "exhausted", {"retry_after": 120})
        row = [r for r in core.list_capacity(sd)
               if r["route"] == "muse-spark-xhigh-go"][0]
        self.assertEqual(row["reset_source"], "provider")
        self.assertAlmostEqual(core._parse_ts(row["reset_at"]) - before, 120, delta=10)


class TestAssumedProbeSchedule(unittest.TestCase):
    def test_weekly_mark_probed_on_schedule_and_cleared_on_success(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "pr1", {"g": 1}, str(ws), "p")
        reset = policy.assumed_reset_at("weekly")
        core.record_capacity(sd, "grok-4.6-go", "exhausted", {"class": "GoUsageLimitError"},
                             reset_at=reset, window="weekly", reset_source="assumed")
        rows = [r for r in core.list_capacity(sd) if r["route"] == "grok-4.6-go"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["probe_failures"], 0)
        first_due = core._parse_ts(rows[0]["next_probe_at"])
        self.assertAlmostEqual(first_due - time.time(), 3600, delta=120)
        self.assertEqual(core.probe_due_routes(sd), [])
        due = core.probe_due_routes(sd, now_ts=time.time() + 3700)
        self.assertEqual(len(due), 1)
        # Failures lengthen the schedule; every outcome is recorded.
        out1 = core.record_probe_outcome(sd, "grok-4.6-go", "weekly", False, {"try": 1})
        self.assertFalse(out1["cleared"])
        row = [r for r in core.list_capacity(sd) if r["route"] == "grok-4.6-go"][0]
        self.assertAlmostEqual(core._parse_ts(row["next_probe_at"]) - time.time(),
                               7200, delta=120)
        core.record_probe_outcome(sd, "grok-4.6-go", "weekly", False, {"try": 2})
        row = [r for r in core.list_capacity(sd) if r["route"] == "grok-4.6-go"][0]
        self.assertAlmostEqual(core._parse_ts(row["next_probe_at"]) - time.time(),
                               14400, delta=120)
        out3 = core.record_probe_outcome(sd, "grok-4.6-go", "weekly", True, {"try": 3})
        self.assertTrue(out3["cleared"])
        self.assertNotIn("grok-4.6-go", core.exhausted_routes(sd))
        probes = core.list_probes(sd, "grok-4.6-go", "weekly")
        self.assertEqual([p["ok"] for p in probes], [0, 0, 1])
        # A 5h mark never clears the weekly mark: windows stay separate.
        core.record_capacity(sd, "grok-4.6-go", "exhausted", {"class": "GoUsageLimitError"},
                             reset_at=policy.assumed_reset_at("weekly"),
                             window="weekly", reset_source="assumed")
        core.record_capacity(sd, "grok-4.6-go", "exhausted", {"class": "GoUsageLimitError"},
                             reset_at=policy.assumed_reset_at("5h"),
                             window="5h", reset_source="assumed")
        core.record_probe_outcome(sd, "grok-4.6-go", "5h", True, {"try": 1})
        left = {r["window"] for r in core.list_capacity(sd)
                if r["route"] == "grok-4.6-go"}
        self.assertEqual(left, {"weekly"})


class TestContextSignal(StallDrillBase):
    def _set_route(self, route):
        lane = policy.lane_of_route(route) or "implementation_default"
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route=?, lane=?, status='running'"
                        " WHERE request_id='oc1'", (route, lane))
        finally:
            con.close()

    def test_context_moves_to_larger_context_route(self):
        run = self._setup("context_error")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res["action"], res["reason"]),
                         ("route_switched", "larger_context"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "glm-5.3-flash-go")
        # Context pressure is not availability: no capacity mark.
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        self.assertNotIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))
        report = json.loads(Path(res["report"]["report_path"]).read_text())
        self.assertEqual(report["status"], "context")
        inv = [i for i in core._list_invocations(self.sd, "oc1")
               if i["kind"] == "opencode_control"][-1]
        self.assertEqual(inv["terminal_class"], "context")
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        models = [p["body"]["model"]["modelID"] for p in self._worker_prompts()]
        self.assertEqual(models, ["muse-spark-1.3-contributor-free", "glm-5.3-flash"])

    def test_context_without_larger_route_ends_failed(self):
        run = self._setup("ok", go_mode="context_error")
        self._set_route("glm-5.1-go")  # last default-lane route, nothing larger after it
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_failed")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "glm-5.1-go")
        inv = [i for i in core._list_invocations(self.sd, "oc1")
               if i["kind"] == "opencode_control"][-1]
        self.assertEqual(inv["terminal_class"], "context")


class TestCLIHarnessStall(unittest.TestCase):
    def _run_cli_kind(self, kind, route, stage):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        script = bindir / ("codex" if kind.startswith("codex") else "claude")
        script.write_text(
            "#!" + PY + "\n"
            "import json, sys, time\n"
            "print(json.dumps({'type': 'thread.started', 'thread_id': 'stall-thread'}), flush=True)\n"
            "sys.stdout.flush()\n"
            "time.sleep(20)\n")
        script.chmod(0o700)
        saved = os.environ.get("PATH")
        os.environ["PATH"] = str(bindir) + os.pathsep + (saved or "")
        self.addCleanup(lambda: os.environ.__setitem__("PATH", saved) if saved is not None
                        else os.environ.pop("PATH", None))
        core.submit(sd, "cl1", {"g": 1}, str(ws), "p")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='cl1'")
        finally:
            con.close()
        started = time.monotonic()
        rc, out, err = core._durable_run(sd, "cl1", "tok", kind, [str(script)],
                                         cwd=str(ws), timeout=30,
                                         meta={"stage": stage, "route": route,
                                               "reason": "test", "stall_secs": 1})
        elapsed = time.monotonic() - started
        for inv in core._list_invocations(sd, "cl1"):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass
        return rc, out, err, elapsed, sd

    def test_codex_stream_silence_aborts_within_window_plus_poll(self):
        rc, out, err, elapsed, sd = self._run_cli_kind(
            "codex_dispatch", "luna/max", "dispatch")
        self.assertEqual(rc, 4)
        self.assertLess(elapsed, 15.0)
        inv = core._list_invocations(sd, "cl1")[-1]
        result = json.loads(inv["result_json"] or "{}")
        self.assertEqual(result.get("signal"), "stalled")
        self.assertEqual(result["signal_evidence"]["source"], "stream_silence")
        self.assertIsNotNone(inv["longest_silence_secs"])
        self.assertEqual(inv["terminal_class"], "stalled")
        self.assertIn("stalled", err)

    def test_claude_stream_silence_aborts_within_window_plus_poll(self):
        rc, out, err, elapsed, sd = self._run_cli_kind(
            "claude_callback", "fable-5.1/max", "planning")
        self.assertEqual(rc, 4)
        self.assertLess(elapsed, 15.0)
        inv = core._list_invocations(sd, "cl1")[-1]
        result = json.loads(inv["result_json"] or "{}")
        self.assertEqual(result.get("signal"), "stalled")
        self.assertEqual(inv["terminal_class"], "stalled")


class TestStallDocs(unittest.TestCase):
    def test_runner_and_skill_cover_stall_context_and_assumed_resets(self):
        runner_doc = (ROOT / "RUNNER.md").read_text()
        skill = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        for name in ("stalled", "context_length_exceeded", "silence window",
                     "assumed", "longest_silence_secs", "180"):
            self.assertIn(name, runner_doc, name)
            self.assertIn(name, skill, name)


class TestProviderResetProducers(unittest.TestCase):
    """Provider-reset-first through real producers, not synthetic dicts.

    The transport (Go Retry-After header), the stall probe's structured
    answer, and Claude's reset text must all reach
    reset_at_for_evidence as provider resets; the assumed window applies
    only when none is present.
    """

    def test_transport_retry_after_header_is_provider_reset(self):
        before = time.time()
        err = adapters.OpenCodeHTTPError(429, '{"type":"error"}',
                                         {"Retry-After": "120"})
        self.assertEqual(err.retry_after, "120")
        ev = adapters.http_error_evidence(err)
        self.assertEqual(ev["source"], "transport")
        self.assertEqual(ev["retry_after"], "120")
        got = policy.parse_provider_reset(ev, before)
        self.assertAlmostEqual(core._parse_ts(got) - before, 120, delta=5)
        reset_at, source = core.reset_at_for_evidence(
            "muse-spark-xhigh-go", ev, "exhausted", now_ts=before)
        self.assertEqual(source, "provider")
        self.assertAlmostEqual(core._parse_ts(reset_at) - before, 120, delta=5)

    def test_transport_body_resets_at_is_provider_reset(self):
        # Codex-shaped resets_at carried in the transport body JSON.
        future = time.time() + 3600
        err = adapters.OpenCodeHTTPError(
            429, json.dumps({"resets_at": future, "message": "limit"}), {})
        ev = adapters.http_error_evidence(err)
        self.assertEqual(ev["resets_at"], future)
        reset_at, source = core.reset_at_for_evidence(
            "muse-spark-xhigh-go", ev, "exhausted")
        self.assertEqual(source, "provider")
        self.assertAlmostEqual(core._parse_ts(reset_at), future, delta=2)

    def test_probe_nested_reset_counts_without_reshaping(self):
        future = time.time() + 7200
        stall_ev = {"source": "stream_silence", "silence_secs": 200.0,
                    "last_part_type": "text",
                    "probe": {"signal": "exhausted",
                              "evidence": {"source": "stall_probe",
                                           "message_error_detail": {
                                               "name": "APIError",
                                               "data": {"responseBody": "GoUsageLimitError"},
                                               "resets_at": future}}}}
        got = policy.parse_provider_reset(stall_ev)
        self.assertAlmostEqual(core._parse_ts(got), future, delta=2)
        # Claude's reset text nested in the probe counts too.
        stall_ev["probe"]["evidence"] = {
            "source": "stall_probe",
            "message": "limit resets at 2026-09-21T03:00:00Z"}
        self.assertEqual(policy.parse_provider_reset(stall_ev),
                         "2026-09-21T03:00:00+00:00")
        # A stray date in a response body never invents one, even nested.
        stall_ev["probe"]["evidence"] = {
            "source": "stall_probe",
            "error": {"name": "APIError",
                      "data": {"responseBody": "error at 2026-09-21T03:00:00Z"}}}
        self.assertIsNone(policy.parse_provider_reset(stall_ev))

    def test_stall_probe_transport_retry_after_stored_as_provider(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "probe1", {"g": 1}, str(ws), "p")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET route='muse-spark-xhigh-free',"
                        " lane='implementation_default', status='running'"
                        " WHERE request_id='probe1'")
        finally:
            con.close()
        before = time.time()
        job = core.get_job(sd, "probe1")
        stall_ev = {"source": "stream_silence", "silence_secs": 200.0,
                    "last_part_type": "text",
                    "probe": {"signal": "exhausted",
                              "evidence": {"source": "stall_probe",
                                           "transport_evidence": {
                                               "source": "transport",
                                               "status": 429,
                                               "retry_after": "180"}}}}
        full = {"probe_signal": "exhausted", "idle_confirmed": True,
                "error": "worker turn stalled: no stream activity for 200s"}
        res = controller._handle_stalled(sd, "probe1", job, 1,
                                         "muse-spark-xhigh-free", full,
                                         "ses_probe", stall_ev)
        self.assertIn(res["action"], ("transferred_to_go", "route_switched"))
        rows = [r for r in core.list_capacity(sd)
                if r["route"] == "muse-spark-xhigh-free"]
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(row["reset_source"], "provider")
            self.assertAlmostEqual(core._parse_ts(row["reset_at"]) - before,
                                   180, delta=30)

    def test_kimi_route_family_is_kimi(self):
        self.assertEqual(policy.ROUTES["kimi-k2.6-go"]["family"], "kimi")
        self.assertEqual(policy.ROUTES["kimi-k2.7-code-go"]["family"], "kimi")


if __name__ == "__main__":
    unittest.main(verbosity=2)
