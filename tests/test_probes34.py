"""Issue #34: usage probes per harness with reset times, reconciled.

Deterministic fakes only: harness probe parsers run on synthetic payloads,
the ledger runs on throwaway state dirs, and no live CLI is ever spawned.
The live task in #17 records at least one real reading per installed
harness; per the coordinator adjudication that live run happens after
merge, not in this diff.
"""
from __future__ import annotations

import datetime
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, harnesses, policy, store  # noqa: E402

NOW = 1758350000.0


def iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(
        ts, datetime.timezone.utc).isoformat()


def fresh_state(testcase) -> tuple[str, Path]:
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    testcase.addCleanup(tmp.cleanup)
    base = Path(tmp.name)
    sd = str(base / "state")
    ws = base / "ws"
    ws.mkdir()
    core.submit(sd, "p1", {"g": 1}, str(ws), "planner")
    return sd, base


class ReadingRecord(unittest.TestCase):
    def test_reading_sources_and_validation(self):
        self.assertEqual(tuple(policy.READING_SOURCES),
                         ("provider_reported", "measured", "derived", "assumed"))
        sd, _ = fresh_state(self)
        spec = policy.ROUTES["muse-spark-xhigh-go"]
        got = core.record_reading(
            sd, spec["pool"], spec["model"], "5h", 4.0, 12.0,
            iso(NOW + 3600), observed_at=iso(NOW),
            source="provider_reported", detail={"via": "test"})
        self.assertEqual((got["used"], got["limit"], got["source"]), (4.0, 12.0, "provider_reported"))
        self.assertIsNone(got["revalidated"])
        for bad_source in ("live", "", None):
            with self.assertRaises(ValueError):
                core.record_reading(sd, spec["pool"], spec["model"], "5h",
                                    1.0, 2.0, None, observed_at=iso(NOW),
                                    source=bad_source)
        with self.assertRaises(ValueError):
            core.record_reading(sd, spec["pool"], spec["model"], "5h",
                                1.0, 2.0, "not-a-time", observed_at=iso(NOW),
                                source="measured")
        # Unknown (failed probe) readings store and list.
        unknown = core.note_probe_failure(sd, "muse-spark-xhigh-go", "weekly",
                                          source="measured",
                                          detail={"error": "timeout"},
                                          observed_at=iso(NOW))
        self.assertIsNone(unknown["used"])
        self.assertEqual(unknown["source"], "measured")

    def test_readings_exposed_by_capacity_and_status(self):
        sd, _ = fresh_state(self)
        spec = policy.ROUTES["glm-5.3-flash-go"]
        core.record_reading(sd, spec["pool"], spec["model"], "weekly",
                            10.0, 30.0, iso(NOW + 7200),
                            observed_at=iso(NOW), source="measured")
        readings = core.list_readings(sd)
        self.assertEqual(len(readings), 1)
        row = readings[0]
        for key in ("pool", "model", "window", "used", "limit",
                    "reset_at", "observed_at", "source"):
            self.assertIn(key, row, key)
        self.assertEqual(row["limit"], 30.0)
        # Route-filtered listing and per-window states.
        self.assertEqual(len(core.list_readings(sd, route="glm-5.3-flash-go")), 1)
        self.assertEqual(len(core.list_readings(sd, route="muse-spark-xhigh-go")), 0)
        states = core.route_reading_states(sd, "glm-5.3-flash-go", now_ts=NOW)
        self.assertEqual(states, {"weekly": "ok"})
        # The capacity CLI and the status/result views expose both tables.
        self.assertEqual(core.list_capacity(sd), [])
        view = core.status_view(sd, "p1")
        self.assertEqual(len(view["readings"]), 1)
        self.assertEqual(view["capacity"], [])
        result = core.result_view(sd, "p1")
        self.assertEqual(len(result["readings"]), 1)
        self.assertEqual(result["capacity"], [])


class HarnessProbes(unittest.TestCase):
    def test_codex_rate_limits_read(self):
        h = harnesses.harness_named("codex")
        self.assertEqual(h.probe_interval_secs(), 300)
        payload = {"plan": "plus", "windows": [
            {"usedPercent": 94.0, "windowDurationMins": 10080,
             "resetsAt": 1758353600},
            {"usedPercent": 12.0, "windowDurationMins": 300,
             "resetsAt": "2026-09-21T03:00:00Z"},
        ]}
        readings = h.probe_rate_limits(payload, iso(NOW))
        by_window = {r["window"]: r for r in readings}
        self.assertEqual(set(by_window), {"weekly", "5h"})
        weekly = by_window["weekly"]
        self.assertEqual((weekly["pool"], weekly["used"], weekly["limit"],
                          weekly["source"]),
                         ("codex", 94.0, 100.0, "provider_reported"))
        self.assertEqual(weekly["reset_at"], "2025-09-20T07:33:20+00:00")
        self.assertEqual(by_window["5h"]["reset_at"], "2026-09-21T03:00:00+00:00")
        # Misshapen payloads raise so the caller records unknown.
        with self.assertRaises(ValueError):
            h.probe_rate_limits({"windows": []}, iso(NOW))

    def test_codex_rollout_zero_cost_reading(self):
        h = harnesses.harness_named("codex")
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        rollout = Path(tmp.name) / "rollout.jsonl"
        rollout.write_text("\n".join(json.dumps(o) for o in [
            {"type": "event", "rate_limits": {"primary": {
                "used_percent": 40.0, "window_minutes": 10080,
                "resets_at": 1758353600, "plan_type": "plus"}}},
            {"type": "noise"},
        ]) + "\n")
        readings = h.read_rollout(str(rollout))
        self.assertTrue(isinstance(readings, list) and readings)
        for reading in readings:
            self.assertEqual((reading["pool"], reading["window"], reading["used"],
                              reading["source"]),
                             ("codex", "weekly", 40.0, "provider_reported"))
            self.assertIsNotNone(reading["observed_at"])
        # Account-level rollout fans out to every codex-pool route model.
        self.assertEqual({r["model"] for r in readings},
                         {s["model"] for s in policy.ROUTES.values()
                          if s.get("pool") == "codex"})
        with self.assertRaises(ValueError):
            h.read_rollout(str(Path(tmp.name) / "missing.jsonl"))

    def test_claude_oauth_usage_and_statusline(self):
        h = harnesses.harness_named("claude")
        self.assertEqual(h.probe_interval_secs(), 180)
        payload = {"five_hour": {"utilization": 22.5,
                                 "resets_at": "2026-09-21T03:00:00Z"},
                   "seven_day": {"utilization": 61.0,
                                 "resets_at": "2026-09-22T09:51:00Z"}}
        readings = h.probe_oauth_usage(payload, iso(NOW))
        by_window = {}
        for r in readings:
            by_window.setdefault(r["window"], []).append(r)
        self.assertEqual(set(by_window), {"5h", "weekly"})
        self.assertEqual(by_window["5h"][0]["used"], 22.5)
        self.assertEqual(by_window["weekly"][0]["reset_at"],
                         "2026-09-22T09:51:00+00:00")
        for r in readings:
            self.assertEqual(r["source"], "provider_reported")
        # Subscription-level usage fans out to every claude-pool model.
        self.assertEqual({r["model"] for r in readings},
                         {s["model"] for s in policy.ROUTES.values()
                          if s.get("pool") == "claude"})
        # The statusline feed parses through the same fields for free.
        self.assertEqual(h.probe_statusline(payload, iso(NOW)), readings)
        with self.assertRaises(ValueError):
            h.probe_oauth_usage({"five_hour": {}}, iso(NOW))

    def test_claude_slash_usage_text(self):
        h = harnesses.harness_named("claude")
        text = ("Session: 15% used, resets 2026-09-21T03:00:00Z\n"
                "Weekly: 61% used, resets 2026-09-22T09:51:00Z\n")
        readings = h.probe_usage_text(text, iso(NOW))
        by_window = {r["window"]: r for r in readings}
        self.assertEqual(by_window["session"]["used"], 15.0)
        self.assertEqual(by_window["weekly"]["used"], 61.0)
        self.assertEqual(by_window["session"]["reset_at"],
                         "2026-09-21T03:00:00+00:00")
        with self.assertRaises(ValueError):
            h.probe_usage_text("nothing usable here", iso(NOW))

    def test_claude_transcript_zero_cost_reading(self):
        h = harnesses.harness_named("claude")
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        transcript = Path(tmp.name) / "session.jsonl"
        transcript.write_text(json.dumps(
            {"type": "assistant",
             "quotaLimits": {"rateLimitType": "five_hour",
                             "resetsAt": "2026-09-21T03:00:00Z"}}) + "\n")
        readings = h.read_transcript(str(transcript))
        self.assertTrue(isinstance(readings, list) and readings)
        for reading in readings:
            self.assertEqual((reading["pool"], reading["window"],
                              reading["source"]),
                             ("claude", "5h", "provider_reported"))
            self.assertEqual(reading["reset_at"], "2026-09-21T03:00:00+00:00")
        self.assertEqual({r["model"] for r in readings},
                         {s["model"] for s in policy.ROUTES.values()
                          if s.get("pool") == "claude"})

    def test_opencode_go_measured_costs(self):
        h = harnesses.harness_named("opencode")
        model = "opencode-go/glm-5.3-flash"  # $60 tier
        rows = [{"cost": 20.0, "ts": NOW - 3600},
                {"cost": 15.0, "ts": NOW - 86400},
                {"cost": 5.0, "ts": NOW - 20 * 86400}]
        readings = h.probe_go_costs(rows, model, iso(NOW), now_ts=NOW)
        by_window = {r["window"]: r for r in readings}
        # 5h: 20 of 12 (exhausted); weekly: 35 of 30 (exhausted);
        # monthly: 40 of 60 (healthy).
        self.assertEqual(by_window["5h"]["used"], 20.0)
        self.assertEqual(by_window["5h"]["limit"], 12.0)
        self.assertEqual(by_window["weekly"]["limit"], 30.0)
        self.assertEqual(by_window["monthly"]["used"], 40.0)
        for r in readings:
            self.assertEqual(r["source"], "measured")
        with self.assertRaises(ValueError):
            h.probe_go_costs(rows, "opencode-go/unknown-tier-model", iso(NOW))

    def test_opencode_cost_db_reader(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        db = str(Path(tmp.name) / "opencode.db")
        con = sqlite3.connect(db)
        try:
            con.execute("CREATE TABLE message (id TEXT, model TEXT, cost REAL, created REAL)")
            con.execute("INSERT INTO message VALUES('m1','glm-5.3-flash',1.5,?)", (NOW,))
            con.execute("COMMIT")
        finally:
            con.close()
        rows = harnesses.harness_named("opencode").read_cost_rows(db)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["cost"], 1.5)
        self.assertEqual(
            harnesses.harness_named("opencode").read_cost_rows(
                str(Path(tmp.name) / "missing.db")), [])

    def test_zen_free_measured_with_assumed_cap(self):
        h = harnesses.harness_named("opencode")
        reading = h.probe_zen_free([NOW - 100, NOW - 200, NOW - 10 * 3600],
                                   "5h", iso(NOW), now_ts=NOW)
        self.assertEqual((reading["pool"], reading["used"],
                          reading["limit"], reading["source"]),
                         ("zen-free", 2.0,
                          float(policy.ZEN_FREE_ASSUMED_REQUESTS), "measured"))
        self.assertTrue(reading["detail"]["assumed_cap"])

    def test_grok_billing_monthly_and_weekly_assumption(self):
        h = harnesses.harness_named("grok")
        payload = {"creditUsagePercent": 33.0,
                   "includedUsed": 10.0, "monthlyLimit": 30.0,
                   "billingPeriodStart": "2026-09-01T00:00:00Z",
                   "billingPeriodEnd": "2026-10-01T00:00:00Z"}
        readings = h.probe_billing(payload, iso(NOW))
        self.assertTrue(isinstance(readings, list) and readings)
        for reading in readings:
            self.assertEqual((reading["pool"], reading["window"], reading["used"],
                              reading["limit"], reading["source"]),
                             ("xai", "monthly", 33.0, 100.0, "provider_reported"))
            self.assertEqual(reading["reset_at"], "2026-10-01T00:00:00+00:00")
        # Both xai-pool Grok routes share the subscription: native and xAI.
        self.assertEqual({r["model"] for r in readings},
                         {s["model"] for s in policy.ROUTES.values()
                          if s.get("pool") == "xai"})
        # No 5-hour or weekly window exists on Grok: the native error path
        # marks the weekly window assumed with error-driven cool-off.
        rep = h.parse_report("grok_control",
                             json.dumps({"type": "error", "message":
                                         "You hit your weekly limit."}), None)
        self.assertEqual(rep["signal"], "exhausted")
        self.assertEqual(rep["signal_evidence"]["window"], "weekly")
        with self.assertRaises(ValueError):
            h.probe_billing({"nothing": 1}, iso(NOW))

    def test_probe_intervals_and_due(self):
        self.assertEqual(policy.PROBE_INTERVAL_SECS["codex"], 300)
        self.assertEqual(policy.PROBE_INTERVAL_SECS["claude"], 180)
        for name in ("codex", "claude", "opencode", "grok"):
            h = harnesses.harness_named(name)
            self.assertTrue(h.probe_due(None, now_ts=NOW))
            self.assertFalse(h.probe_due(iso(NOW), now_ts=NOW))
            self.assertTrue(h.probe_due(iso(NOW - 10000), now_ts=NOW))
        with self.assertRaises(ValueError):
            core.probe_due_for("nope", iso(NOW), now_ts=NOW)
        self.assertTrue(core.probe_due_for("codex", iso(NOW - 301), now_ts=NOW))
        self.assertFalse(core.probe_due_for("codex", iso(NOW - 299), now_ts=NOW))
        self.assertFalse(core.probe_due_for("claude", iso(NOW - 179), now_ts=NOW))


class LimitErrorResets(unittest.TestCase):
    def test_each_harness_reset_carrier(self):
        # Codex resets_at verbatim.
        codex_reset, codex_source = core.reset_at_for_evidence(
            "luna/max", {"resets_at": NOW + 600}, "exhausted", now_ts=NOW)
        self.assertEqual(codex_source, "provider")
        self.assertAlmostEqual(core._parse_ts(codex_reset), NOW + 600, delta=2)
        # OpenCode Go Retry-After seconds: now plus the delay, exact.
        go_reset, go_source = core.reset_at_for_evidence(
            "muse-spark-xhigh-go", {"retry_after": 180}, "exhausted",
            now_ts=NOW)
        self.assertEqual(go_source, "provider")
        self.assertAlmostEqual(core._parse_ts(go_reset), NOW + 180, delta=2)
        # Claude reset text naming an ISO moment.
        claude_reset, claude_source = core.reset_at_for_evidence(
            "fable-5.1/max",
            {"message": "limit resets at 2026-09-21T03:00:00Z"},
            "exhausted", now_ts=NOW)
        self.assertEqual(claude_source, "provider")
        self.assertEqual(claude_reset, "2026-09-21T03:00:00+00:00")
        # Absent resets use the assumed-window rule.
        assumed_reset, assumed_source = core.reset_at_for_evidence(
            "muse-spark-xhigh-free", {"message": "try tomorrow"},
            "exhausted", now_ts=NOW)
        self.assertEqual(assumed_source, "assumed")
        self.assertAlmostEqual(core._parse_ts(assumed_reset) - NOW,
                               5 * 3600, delta=2)


class Reconciliation(unittest.TestCase):
    def _mark(self, sd, route, evidence, reset_at, source, window=None):
        core.record_capacity(sd, route, "exhausted", evidence,
                             reset_at=reset_at, window=window,
                             reset_source=source)

    def test_error_overrides_probe(self):
        sd, _ = fresh_state(self)
        spec = policy.ROUTES["muse-spark-xhigh-go"]
        # A healthy probe first: the route stays eligible.
        core.record_reading(sd, spec["pool"], spec["model"], "5h",
                            2.0, 12.0, None, observed_at=iso(NOW),
                            source="measured")
        self.assertNotIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        # Then the error for the same window: the route skips even though
        # the last probe was healthy.
        self._mark(sd, "muse-spark-xhigh-go",
                   {"resets_at": NOW + 3600}, iso(NOW + 3600),
                   "provider", window="5h")
        self.assertIn("muse-spark-xhigh-go", core.exhausted_routes(sd))

    def test_healthy_probe_never_clears_provider_mark_before_reset(self):
        sd, _ = fresh_state(self)
        self._mark(sd, "muse-spark-xhigh-go",
                   {"resets_at": NOW + 3600}, iso(NOW + 3600),
                   "provider", window="5h")
        # A healthy probe and a healthy fresh reading both hold the mark.
        out = core.record_probe_outcome(sd, "muse-spark-xhigh-go", "5h",
                                        True, {"probe": 1}, now_ts=NOW)
        self.assertFalse(out["cleared"])
        self.assertTrue(out["held"])
        self.assertIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        spec = policy.ROUTES["muse-spark-xhigh-go"]
        core.record_reading(sd, spec["pool"], spec["model"], "5h",
                            1.0, 12.0, None, observed_at=iso(NOW),
                            source="measured")
        self.assertIn("muse-spark-xhigh-go", core.exhausted_routes(sd))

    def test_assumed_mark_clears_on_first_healthy_probe(self):
        sd, _ = fresh_state(self)
        self._mark(sd, "muse-spark-xhigh-go",
                   {"class": "GoUsageLimitError"},
                   iso(NOW + 7 * 24 * 3600), "assumed", window="weekly")
        out = core.record_probe_outcome(sd, "muse-spark-xhigh-go",
                                        "weekly", True, {"probe": 1},
                                        now_ts=NOW)
        self.assertTrue(out["cleared"])
        self.assertNotIn("muse-spark-xhigh-go", core.exhausted_routes(sd))

    def test_after_reset_needs_fresh_probe_or_success(self):
        sd, _ = fresh_state(self)
        self._mark(sd, "muse-spark-xhigh-go",
                   {"resets_at": NOW - 10}, iso(NOW - 10),
                   "provider", window="5h")
        # Expired but sticky: still skipped, and listed as probe-due.
        self.assertIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        self.assertTrue([r for r in core.probe_due_routes(sd, now_ts=NOW)
                         if r["route"] == "muse-spark-xhigh-go"])
        # One fresh probe revalidates.
        out = core.record_probe_outcome(sd, "muse-spark-xhigh-go", "5h",
                                        True, {"probe": 1}, now_ts=NOW)
        self.assertTrue(out["cleared"])
        self.assertNotIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        # And a successful request revalidates the same way.
        self._mark(sd, "muse-spark-xhigh-go",
                   {"resets_at": NOW - 10}, iso(NOW - 10),
                   "provider", window="5h")
        cleared = core.record_route_success(sd, "muse-spark-xhigh-go",
                                            now_ts=NOW)
        self.assertEqual(cleared["cleared"], ["5h"])
        self.assertNotIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        # A future provider mark survives even a success elsewhere.
        self._mark(sd, "muse-spark-xhigh-go",
                   {"resets_at": NOW + 3600}, iso(NOW + 3600),
                   "provider", window="5h")
        cleared = core.record_route_success(sd, "muse-spark-xhigh-go",
                                            now_ts=NOW)
        self.assertEqual(cleared["cleared"], [])
        self.assertIn("muse-spark-xhigh-go", core.exhausted_routes(sd))


class PreflightReadings(unittest.TestCase):
    def _job_on(self, sd, request_id, route):
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET route=?, lane='implementation_default',"
                        " status='running' WHERE request_id=?", (route, request_id))
        finally:
            con.close()

    def test_margin_degrades_and_exhaustion_skips(self):
        self.assertEqual(policy.USAGE_DEGRADED_FRACTION, 0.8)
        sd, _ = fresh_state(self)
        self._job_on(sd, "p1", "muse-spark-xhigh-go")
        spec = policy.ROUTES["muse-spark-xhigh-go"]
        live = time.time()
        reset = iso(live + 3600)
        observed = iso(live)
        # 85 percent of the 5h window: degraded, moves laterally.
        core.record_reading(sd, spec["pool"], spec["model"], "5h",
                            10.2, 12.0, reset,
                            observed_at=observed, source="measured")
        self.assertIn("muse-spark-xhigh-go", core.degraded_routes(sd))
        self.assertNotIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        move = controller._preflight_move(sd, "p1", "muse-spark-xhigh-go")
        self.assertIsNotNone(move)
        self.assertEqual(move["reason"], "preflight_degraded")
        # 100 percent: exhausted, skips until reset_at.
        con = store.connect(sd)
        try:
            con.execute("DELETE FROM readings")
            con.execute("UPDATE jobs SET route='muse-spark-xhigh-go' WHERE request_id='p1'")
        finally:
            con.close()
        core.record_reading(sd, spec["pool"], spec["model"], "5h",
                            12.0, 12.0, reset,
                            observed_at=observed, source="measured")
        self.assertIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        move = controller._preflight_move(sd, "p1", "muse-spark-xhigh-go")
        self.assertIsNotNone(move)
        self.assertEqual(move["reason"], "preflight_exhausted")
        # Below the margin the route is untouched.
        con = store.connect(sd)
        try:
            con.execute("DELETE FROM readings")
            con.execute("UPDATE jobs SET route='muse-spark-xhigh-go' WHERE request_id='p1'")
        finally:
            con.close()
        core.record_reading(sd, spec["pool"], spec["model"], "5h",
                            2.0, 12.0, reset,
                            observed_at=observed, source="measured")
        self.assertIsNone(controller._preflight_move(sd, "p1", "muse-spark-xhigh-go"))

    def test_failed_probe_never_blocks(self):
        sd, _ = fresh_state(self)
        self._job_on(sd, "p1", "muse-spark-xhigh-go")
        core.note_probe_failure(sd, "muse-spark-xhigh-go", "5h",
                                source="measured",
                                detail={"error": "probe timeout"},
                                observed_at=iso(NOW))
        states = core.route_reading_states(sd, "muse-spark-xhigh-go",
                                           now_ts=NOW)
        self.assertEqual(states.get("5h"), "unknown")
        self.assertNotIn("muse-spark-xhigh-go", core.exhausted_routes(sd))
        self.assertNotIn("muse-spark-xhigh-go", core.degraded_routes(sd))
        self.assertIsNone(controller._preflight_move(sd, "p1", "muse-spark-xhigh-go"))
        # The failed probe outcome is on the ledger for the observer.
        probes = core.list_probes(sd, "muse-spark-xhigh-go", "5h")
        self.assertEqual([p["ok"] for p in probes], [0])

    def test_dispatch_probe_hook_never_blocks(self):
        sd, base = fresh_state(self)
        calls = []

        def flaky_probe(state_dir, request_id, route):
            calls.append(route)
            raise RuntimeError("probe exploded")

        before = core.get_job(sd, "p1")
        self.assertFalse(before.get("codex_task_id"))
        # A failing probe must not stop the dispatch from starting: the
        # test only proves the hook ran and dispatch attempted, using an
        # unreachable run_cmd so no child starts.
        def boom(cmd, cwd=None, timeout=None, **kw):
            raise AssertionError("no child in this unit test")

        try:
            controller.dispatch(sd, "p1", run_cmd=boom, probe=flaky_probe)
        except AssertionError:
            pass
        self.assertEqual(calls, [policy.stage_routes("dispatch")[0]])


class ProbeJoins(unittest.TestCase):
    def _store_all(self, sd, readings):
        for r in readings:
            core.record_reading(sd, r["pool"], r["model"], r["window"],
                                r["used"], r["limit"], r["reset_at"],
                                observed_at=r["observed_at"],
                                source=r["source"], detail=r.get("detail"))

    def test_codex_parser_output_drives_preflight(self):
        live = time.time()
        observed = iso(live)
        reset = iso(live + 3600)
        payload = {"plan": "plus", "windows": [
            {"usedPercent": 100.0, "windowDurationMins": 10080,
             "resetsAt": reset},
        ]}
        readings = harnesses.harness_named("codex").probe_rate_limits(
            payload, observed)
        self.assertTrue(readings)
        sd, _ = fresh_state(self)
        self._store_all(sd, readings)
        for route in ("luna/max", "astra/max"):
            self.assertIn(route, core.exhausted_routes(sd),
                          route)
            states = core.route_reading_states(sd, route, now_ts=live)
            self.assertEqual(states.get("weekly"), "exhausted", route)

    def test_claude_parser_output_drives_preflight(self):
        live = time.time()
        observed = iso(live)
        reset = iso(live + 3600)
        payload = {"seven_day": {"utilization": 100.0, "resets_at": reset}}
        readings = harnesses.harness_named("claude").probe_oauth_usage(
            payload, observed)
        self.assertTrue(readings)
        sd, _ = fresh_state(self)
        self._store_all(sd, readings)
        for route in ("fable-5.1/max", "opus-5.5/high"):
            self.assertIn(route, core.exhausted_routes(sd), route)

    def test_grok_monthly_joins_both_xai_routes_not_go(self):
        live = time.time()
        observed = iso(live)
        reset = iso(live + 30 * 86400)
        payload = {"creditUsagePercent": 100.0,
                   "billingPeriodEnd": reset}
        readings = harnesses.harness_named("grok").probe_billing(
            payload, observed)
        models = {r["model"] for r in readings}
        self.assertIn("grok-4.6", models)
        self.assertIn("xai/grok-4.6", models)
        self.assertNotIn("opencode-go/grok-4.6", models)
        sd, _ = fresh_state(self)
        self._store_all(sd, readings)
        self.assertIn("grok-4.6-build", core.exhausted_routes(sd))
        self.assertIn("grok-4.6-xai", core.exhausted_routes(sd))
        self.assertNotIn("grok-4.6-go", core.exhausted_routes(sd))

    def test_unknown_codex_window_skipped(self):
        self.assertIsNone(harnesses._minutes_to_window(60))
        self.assertIsNone(harnesses._minutes_to_window(1440))
        live = time.time()
        observed = iso(live)
        payload = {"plan": "plus", "windows": [
            {"usedPercent": 100.0, "windowDurationMins": 60,
             "resetsAt": iso(live + 3600)},
        ]}
        self.assertEqual(
            harnesses.parse_codex_rate_limits(payload, observed), [])
        with self.assertRaises(ValueError):
            harnesses.harness_named("codex").probe_rate_limits(
                payload, observed)

    def test_session_window_never_blocks(self):
        sd, _ = fresh_state(self)
        spec = policy.ROUTES["fable-5.1/max"]
        core.record_reading(sd, spec["pool"], spec["model"], "session",
                            100.0, 100.0, iso(time.time() + 3600),
                            observed_at=iso(time.time()),
                            source="provider_reported")
        self.assertNotIn("fable-5.1/max", core.exhausted_routes(sd))
        self.assertNotIn("fable-5.1/max", core.degraded_routes(sd))
        states = core.route_reading_states(sd, "fable-5.1/max")
        self.assertEqual(states.get("session"), "ok")

    def test_compact_five_hour_labels(self):
        self.assertEqual(harnesses._claude_text_window("5hour"), "5h")
        self.assertEqual(harnesses._claude_text_window("5h"), "5h")
        self.assertEqual(harnesses._claude_text_window("5-hour"), "5h")
        for label in ("5hour: 22% used, resets 2026-09-21T03:00:00Z\n",
                      "5h: 22% used, resets 2026-09-21T03:00:00Z\n"):
            text = (label + "Weekly: 61% used, resets 2026-09-22T09:51:00Z\n")
            readings = harnesses.harness_named("claude").probe_usage_text(
                text, iso(NOW))
            by_window = {}
            for r in readings:
                by_window.setdefault(r["window"], []).append(r)
            self.assertIn("5h", by_window, label)
            self.assertEqual(by_window["5h"][0]["used"], 22.0, label)


class ProbeDocs(unittest.TestCase):
    def test_runner_glossary_and_skill_cover_probes(self):
        runner_doc = (ROOT / "RUNNER.md").read_text()
        glossary = (ROOT / "GLOSSARY.md").read_text()
        skill = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        for name in ("Reading", "provider_reported", "measured",
                     "reconcile", "revalidat", "300", "180",
                     "USAGE_DEGRADED_FRACTION", "unknown"):
            self.assertIn(name, runner_doc, name)
        for name in ("Reading", "Usage probe", "Revalidation"):
            self.assertIn(name, glossary, name)
        for name in ("provider_reported", "measured", "300", "180",
                     "revalidat", "unknown"):
            self.assertIn(name, skill, name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
