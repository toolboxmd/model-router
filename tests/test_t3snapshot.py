"""Issue #116: eligible models and limits from the T3 provider snapshot.

Stdlib only, no live T3. A fake HTTP server serves ``/api/prism/snapshot``
in the toolboxmd/t3code#19 shape for the wire test; the routing rules run
against an in-memory fake client:

- a model turned off (or its instance disabled) in T3 leaves routing;
- non-empty Prism (role, lane) lists replace the policy order, and
  entries without a policy route run as ``t3:`` routes;
- a usage window at 100 percent rests every route on that meter until
  ``resetsAt`` (or until a later read without one), 80 percent degrades,
  a passed ``resetsAt`` is eligible again;
- an unreadable snapshot (older server, 404) filters nothing;
- Zen free keeps its own mark from its error's ``next`` (epoch ms);
- a Go limit error rests the whole Go pool, never a lateral Go move.
"""
import json
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, policy, t3exec, t3snapshot  # noqa: E402
from tests.fakes import (FakeT3Client, prism_provider, prism_snapshot,  # noqa: E402
                         use_fake_t3)

PLANNER = "planner-t3"
FREE = "opencode/muse-spark-1.3-contributor-free"
GO_MODELS = [policy.ROUTES[r]["model"] for r in policy.ROUTES
             if policy.ROUTES[r]["instance"] == "opencode"]


def iso_in(secs):
    return (datetime.now(timezone.utc) + timedelta(seconds=secs)).isoformat()


def window(pct, resets_in=None, label="5 hour"):
    w = {"id": "rolling", "kind": "session", "label": label, "usedPercent": pct}
    if resets_in is not None:
        w["resetsAt"] = iso_in(resets_in)
    return w


def full_providers(go_windows=None, opencode_models=None, grok_enabled=True):
    """Every policy route enabled, with optional Go meter windows."""
    return [
        prism_provider("codex", ["gpt-5.6-luna"]),
        prism_provider("opencode", opencode_models or GO_MODELS, windows=go_windows),
        prism_provider("grok", ["grok-4.6"], enabled=grok_enabled),
    ]


class Base(unittest.TestCase):
    def setUp(self):
        t3snapshot.reset()
        self.addCleanup(t3snapshot.reset)
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def apply(self, snapshot, lane="implementation_default"):
        fake = FakeT3Client(planner=PLANNER)
        fake.prism = snapshot
        t3snapshot.refresh(fake, "proj-1", lane, force=True)
        return fake

    def submit(self, rid, **kw):
        return core.submit(self.sd, rid, {"goal": "t"}, self.ws(rid), f"s-{rid}",
                           planner_t3_thread=PLANNER, **kw)


class Eligibility(Base):
    def test_model_turned_off_leaves_routing(self):
        models = [m for m in GO_MODELS if m != FREE]
        self.apply(prism_snapshot(full_providers(opencode_models=models)))
        skipped = core.exhausted_routes(self.sd)
        self.assertIn("muse-spark-xhigh-free", skipped)
        self.assertNotIn("muse-spark-xhigh-go", skipped)
        self.assertEqual(core.sticky_home_route(self.sd, "default"), "muse-spark-xhigh-go")
        why = t3snapshot.view()["routes"]["muse-spark-xhigh-free"]
        self.assertEqual(why["state"], "ineligible")
        self.assertIn("not enabled", why["reason"])

    def test_disabled_instance_removes_all_its_routes(self):
        self.apply(prism_snapshot(full_providers(grok_enabled=False)))
        skipped = core.exhausted_routes(self.sd)
        self.assertIn("grok-4.6-build", skipped)
        self.assertNotIn("grok-4.6-xai", skipped)

    def test_missing_instance_is_ineligible(self):
        self.apply(prism_snapshot([prism_provider("opencode", GO_MODELS)]))
        self.assertIn("luna/max", core.exhausted_routes(self.sd))
        # The dispatch preflight falls back to Luna on Go.
        self.assertEqual(controller._dispatch_fallback_routes("luna/max"), ["luna-go/max"])

    def test_unknown_snapshot_filters_nothing(self):
        fake = self.apply(None)
        self.assertEqual(fake.prism_reads, ["proj-1"])
        self.assertEqual(core.exhausted_routes(self.sd), set())
        self.assertEqual(core.degraded_routes(self.sd), set())
        self.assertEqual(t3snapshot.view()["snapshot"], "unknown")
        self.assertEqual(core.sticky_home_route(self.sd, "default"), "muse-spark-xhigh-free")


class RolePreferences(Base):
    def test_lane_lists_replace_the_policy_order(self):
        lanes = {"worker": {"medium": [
            {"instanceId": "opencode", "model": "opencode-go/muse-spark-1.3-contributor",
             "effort": "xhigh"},
            {"instanceId": "claudeAgent", "model": "claude-opus-5-5", "effort": "medium"}]},
            "recovery": {"medium": [{"instanceId": "grok", "model": "grok-4.6",
                                     "effort": "medium"}]}}
        providers = full_providers() + [prism_provider("claudeAgent", ["claude-opus-5-5"])]
        self.apply(prism_snapshot(providers, lanes), lane="implementation_default")
        order = policy.stage_routes("implementation_default")
        self.assertEqual(order, ["muse-spark-xhigh-go", "t3:claudeAgent:claude-opus-5-5@medium"])
        self.assertEqual(policy.stage_routes("recovery"), ["grok-4.6-build"])
        # Lanes without a list keep the policy default.
        self.assertEqual(policy.stage_routes("implementation_hard")[0], "muse-spark-xhigh-free")
        self.assertEqual(core.sticky_home_route(self.sd, "default"), "muse-spark-xhigh-go")
        # A snapshot route is a T3 selection with the Claude effort option.
        sel = t3exec.route_model_selection("t3:claudeAgent:claude-opus-5-5@medium")
        self.assertEqual(sel, {"instanceId": "claudeAgent", "model": "claude-opus-5-5",
                               "options": [{"id": "effort", "value": "medium"}]})
        spec = policy.route_spec("t3:claudeAgent:claude-opus-5-5@medium")
        self.assertEqual((spec["pool"], spec["family"]), ("claude", "claude"))
        # The overload fallback walks the Prism list: Opus follows Muse.
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-go", lane="default"),
                         "t3:claudeAgent:claude-opus-5-5@medium")

    def test_opencode_selection_carries_variant_and_agent(self):
        sel = t3exec.route_model_selection("luna-go/max")
        self.assertEqual(sel["options"], [{"id": "agent", "value": "plan"}])
        sel = t3exec.route_model_selection("muse-spark-xhigh-free")
        self.assertEqual(sel["options"], [{"id": "variant", "value": "xhigh"},
                                          {"id": "agent", "value": "build"}])
        self.assertEqual(t3exec.route_model_selection("grok-4.6-build")["options"],
                         [{"id": "reasoningEffort", "value": "medium"}])


class UsageWindows(Base):
    def test_full_go_window_rests_every_go_route_until_reset(self):
        self.apply(prism_snapshot(full_providers(go_windows=[window(100, 3600)])))
        skipped = core.exhausted_routes(self.sd)
        go = {r for r in policy.ROUTES if policy.ROUTES[r]["pool"] == "go"}
        self.assertTrue(go <= skipped, go - skipped)
        for other in ("muse-spark-xhigh-free", "grok-4.6-build", "grok-4.6-xai", "luna/max"):
            self.assertNotIn(other, skipped)
        reason = t3snapshot.view()["routes"]["glm-5.3-go"]["reason"]
        self.assertIn("100%", reason)

    def test_passed_reset_is_eligible(self):
        self.apply(prism_snapshot(full_providers(go_windows=[window(100, -60)])))
        self.assertEqual(core.exhausted_routes(self.sd), set())

    def test_full_window_without_reset_holds_until_a_later_read(self):
        fake = self.apply(prism_snapshot(full_providers(go_windows=[window(100)])))
        self.assertIn("glm-5.3-go", core.exhausted_routes(self.sd))
        self.assertIn("next T3 refresh", t3snapshot.view()["routes"]["glm-5.3-go"]["reason"])
        fake.prism = prism_snapshot(full_providers(go_windows=[window(40)]))
        t3snapshot.refresh(fake, "proj-1", force=True)
        self.assertNotIn("glm-5.3-go", core.exhausted_routes(self.sd))

    def test_eighty_percent_degrades(self):
        self.apply(prism_snapshot(full_providers(go_windows=[window(85, 3600)])))
        self.assertIn("glm-5.3-go", core.degraded_routes(self.sd))
        self.assertNotIn("glm-5.3-go", core.exhausted_routes(self.sd))
        self.assertNotIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))


class ErrorMarks(Base):
    def test_zen_free_error_next_sets_the_mark_and_it_expires(self):
        next_ms = int((time.time() + 1800) * 1000)
        evidence = t3exec.evidence_dict(
            'Free usage exceeded, subscribe to Go {"type": "retry", "next": %d}' % next_ms)
        reset_at, source = core.reset_at_for_evidence("muse-spark-xhigh-free", evidence,
                                                      "exhausted")
        self.assertEqual(source, "provider")
        self.assertAlmostEqual(datetime.fromisoformat(reset_at).timestamp(),
                               next_ms / 1000, delta=1)
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "exhausted", evidence,
                             reset_at, reset_source=source)
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        # Past the provider reset the route is eligible again with no
        # operator step (#111: the stale Zen free mark).
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "exhausted", evidence,
                             datetime.fromtimestamp(time.time() - 5, timezone.utc).isoformat(),
                             reset_source="provider")
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))

    def test_error_without_reset_assumes_one_window(self):
        reset_at, source = core.reset_at_for_evidence(
            "muse-spark-xhigh-free", {"message": "Free usage exceeded"}, "exhausted")
        self.assertEqual(source, "assumed")
        self.assertAlmostEqual(datetime.fromisoformat(reset_at).timestamp(),
                               time.time() + policy.ASSUMED_RESET_SECS, delta=5)

    def test_prose_never_invents_a_reset(self):
        self.assertIsNone(policy.parse_provider_reset(
            {"message": "It will reset in 3 hours at 2026-09-25T14:02:25Z"}))

    def test_go_limit_rests_the_pool_and_moves_off_go(self):
        text = ("5 hour usage limit reached. It will reset in 3 hours 3 minutes. "
                "To continue using this model now, enable usage from your available balance")
        self.assertEqual(t3exec.classify_provider_error(text), "exhausted")
        self.assertEqual(t3exec.classify_provider_error(
            "account_rate_limit: Go limit reached, " + text), "exhausted")
        self.submit("g1", lane="hard", route="glm-5.3-go")
        use_fake_t3(self, self.sd, "g1")
        job = core.get_job(self.sd, "g1")
        res = controller._finish_worker_turn(
            self.sd, "g1", job, "glm-5.3-go", 1, None,
            {"ok": False, "rc": 1, "signal": "exhausted", "quota": True,
             "signal_evidence": t3exec.evidence_dict(text), "error": text,
             "idle_confirmed": True},
            "sub.planner-t3.w1", 1, source=controller.T3_TURN_KIND)
        # Never a lateral move to another Go model: Grok Build on xAI next.
        self.assertEqual(core.get_job(self.sd, "g1")["route"], "grok-4.6-build", res)
        skipped = core.exhausted_routes(self.sd)
        for go_route in ("muse-spark-xhigh-go", "deepseek-v4-pro-go", "grok-4.6-go",
                         "luna-go/max"):
            self.assertIn(go_route, skipped)
        self.assertNotIn("muse-spark-xhigh-free", skipped)

    def test_success_clears_an_assumed_pool_mark(self):
        core.record_capacity(self.sd, "glm-5.3-go", "exhausted", {"message": "x"},
                             policy.assumed_reset_at(), reset_source="assumed")
        self.assertIn("hy3-go", core.exhausted_routes(self.sd))
        self.assertEqual(core.record_route_success(self.sd, "hy3-go")["cleared"], ["glm-5.3-go"])
        self.assertNotIn("hy3-go", core.exhausted_routes(self.sd))


class ControllerRefresh(Base):
    def test_job_refresh_reads_the_planner_project(self):
        self.submit("r1")
        fake = use_fake_t3(self, self.sd, "r1")
        fake.prism = prism_snapshot(full_providers(go_windows=[window(100, 600)]))
        snap = t3snapshot.refresh_for_job(core.get_job(self.sd, "r1"), force=True)
        self.assertIsNotNone(snap)
        self.assertEqual(fake.prism_reads, ["proj-1"])
        self.assertIn("muse-spark-xhigh-go", core.exhausted_routes(self.sd))
        # A worker preflight moves off the resting Go meter before a thread starts.
        con = core.store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route='muse-spark-xhigh-go', status='running'"
                        " WHERE request_id='r1'")
        finally:
            con.close()
        move = controller._preflight_move(self.sd, "r1", "muse-spark-xhigh-go")
        # The default lane is all Go after Muse Go: blocked with the reason.
        self.assertEqual(move["reason"], "capacity_exhausted")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "blocked")
        self.assertIn("capacity_exhausted", core.get_job(self.sd, "r1")["block_reason"])

    def test_refresh_never_raises(self):
        def boom(*a, **k):
            raise t3exec.T3Error("no token")
        t3exec_client = t3exec.client_for_job
        t3exec.client_for_job = boom
        self.addCleanup(setattr, t3exec, "client_for_job", t3exec_client)
        self.assertIsNone(t3snapshot.refresh_for_job({"lane": "implementation_hard"}))
        self.assertEqual(t3snapshot.view()["snapshot"], "unknown")


class Wire(unittest.TestCase):
    def setUp(self):
        t3snapshot.reset()
        self.addCleanup(t3snapshot.reset)

    def serve(self, status, body):
        seen = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                seen.append((self.path, self.headers.get("Authorization")))
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}", seen

    def test_get_snapshot_with_project_and_bearer(self):
        body = prism_snapshot(full_providers(go_windows=[window(100, 600)]))
        url, seen = self.serve(200, body)
        client = t3exec.T3Client(url, "tok-1")
        snap = t3snapshot.refresh(client, "proj 1", "implementation_default", force=True)
        self.assertEqual(snap["projectId"], "proj-1")
        self.assertEqual(seen, [("/api/prism/snapshot?projectId=proj%201", "Bearer tok-1")])
        self.assertIn("glm-5.3-go", t3snapshot.skip_sets()[0])
        # Cached: a second read inside the window makes no request.
        t3snapshot.refresh(client, "proj 1", "implementation_default")
        self.assertEqual(len(seen), 1)

    def test_older_server_404_is_unknown(self):
        url, _seen = self.serve(404, {"error": "not found"})
        snap = t3snapshot.refresh(t3exec.T3Client(url, "tok"), None, force=True)
        self.assertIsNone(snap)
        view = t3snapshot.view()
        self.assertEqual(view["snapshot"], "unknown")
        self.assertIn("404", view["error"])
        self.assertEqual(t3snapshot.skip_sets(), (set(), set()))


if __name__ == "__main__":
    unittest.main()
