"""Policy v2: data consistency, lanes, caps, and signal classes."""
import json
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import cli, controller, core, policy, store  # noqa: E402
from tests.fakes import isolate_t3_env  # noqa: E402


def setUpModule():
    isolate_t3_env()


class PolicyData(unittest.TestCase):
    def test_policy_consistent(self):
        self.assertEqual(policy.validate_policy(), [])
        self.assertEqual(policy.POLICY_ID, "durable-runner-policy-v2")
        self.assertTrue(policy.POLICY_SOURCE and policy.POLICY_EVIDENCE)

    def test_every_route_is_a_t3_selection_on_a_subscription_pool(self):
        for name, spec in policy.ROUTES.items():
            self.assertTrue(spec["instance"] and spec["model"], name)
            self.assertIn(spec["pool"], policy.POOLS, name)
            self.assertTrue(policy.is_supported(name))
            for gone in ("harness", "agent", "variant", "sandbox"):
                self.assertNotIn(gone, spec, name)
        self.assertEqual(policy.validate_policy(), [])
        self.assertFalse(policy.ALLOW_ZEN_OVERFLOW)
        self.assertFalse(policy.ALLOW_DIRECT_PAID_API)

    def test_no_gemini_or_antigravity_routes(self):
        models = " ".join(s["model"] for s in policy.ROUTES.values())
        self.assertNotRegex(models, r"gemini|antigravity")


    def test_scarce_models_get_one_turn(self):
        self.assertEqual({r for r in policy.ROUTES if policy.one_turn_per_job(r)},
                         {"glm-5.3-go", "deepseek-v4-pro-go", "grok-4.6-go", "luna-go/max",
                          "deepseek-v4.1-flash-go"})
        self.assertTrue(policy.one_turn_routes_used("deepseek-v4.1-flash-go",
                                                    {"deepseek-v4.1-flash-go": 1}))
        self.assertFalse(policy.one_turn_routes_used("deepseek-v4.1-flash-go", {}))
        self.assertFalse(policy.one_turn_routes_used("muse-spark-xhigh-go", {"muse-spark-xhigh-go": 3}))
        # A used one-turn route is skipped as a lateral target.
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-go", lane="hard",
                                                  turns_by_route={"glm-5.3-go": 1}),
                         "deepseek-v4-pro-go")

    def test_stage_table_matches_parent_decision(self):
        s = policy.STAGES
        self.assertEqual(s["implementation_default"]["routes"],
                         ["muse-spark-xhigh-free", "muse-spark-xhigh-go",
                          "glm-5.3-flash-go", "qwen3.8-flash-go", "deepseek-v4.1-flash-go",
                          "hy3-go", "minimax-m3-go", "mimo-v2.5-go", "minimax-m2.7-go",
                          "longcat-2.0-go", "glm-5.2-go", "kimi-k2.6-go", "glm-5.1-go"])
        # small: Muse free first, then Muse Go, then from GLM 5.3 Flash onward.
        self.assertEqual(s["implementation_small"]["routes"],
                         ["muse-spark-xhigh-free", "muse-spark-xhigh-go"] +
                         s["implementation_default"]["routes"][2:])
        # hard: Muse free, Muse Go, GLM-5.3, DeepSeek V4 Pro, Grok 4.6 Go, Build, xAI.
        self.assertEqual(s["implementation_hard"]["routes"],
                         ["muse-spark-xhigh-free", "muse-spark-xhigh-go", "glm-5.3-go",
                          "deepseek-v4-pro-go", "grok-4.6-go", "grok-4.6-build",
                          "grok-4.6-xai"])
        self.assertEqual(s["implementation_hard"]["routes"][-2:], ["grok-4.6-build", "grok-4.6-xai"])
        self.assertEqual(s["correction"]["routes"], ["kimi-k2.7-code-go"])
        self.assertEqual(s["recovery"]["routes"], ["grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"])
        self.assertEqual(s["dispatch"]["routes"], ["luna/max", "luna-go/max"])
        self.assertNotIn("manual", s["dispatch"])
        # Planning and review are the planner's own; the runner lists none.
        for gone in ("planning", "planner_rungs", "review_ticket", "review_final"):
            self.assertNotIn(gone, s)
        self.assertEqual(s["critical"]["executor"], "planner")
        self.assertEqual(s["critical"]["routes"], [])
        self.assertEqual(policy.ROUTES["luna/max"]["instance"], "codex")

    def test_planner_chosen_rungs_are_not_implementation_lanes(self):
        for stage in policy.IMPLEMENTATION_LANES:
            for route in policy.stage_routes(stage):
                self.assertFalse(policy.ROUTES[route].get("planner_chosen"), (stage, route))
                self.assertFalse(policy.ROUTES[route].get("manual"), (stage, route))
        for route in policy.implementation_routes() + policy.stage_routes("correction"):
            self.assertFalse(policy.route_spec(route).get("manual"))


    def test_correction_advances_to_recovery(self):
        self.assertEqual(policy.next_recovery_route("kimi-k2.7-code-go"), "grok-4.6-go")
        self.assertEqual(policy.next_recovery_route("grok-4.6-go"), "grok-4.6-build")
        self.assertEqual(policy.next_recovery_route("grok-4.6-build"), "grok-4.6-xai")
        self.assertIsNone(policy.next_recovery_route("grok-4.6-xai"))
        # Rungs the job already used are skipped. The native Grok Build
        # route sits between Go Grok and the OpenCode xAI fallback.
        self.assertIsNone(policy.next_recovery_route(
            "kimi-k2.7-code-go",
            {"grok-4.6-go": 1, "grok-4.6-build": 1, "grok-4.6-xai": 1}))
        self.assertEqual(policy.next_recovery_route(
            "kimi-k2.7-code-go",
            {"grok-4.6-go": 1, "grok-4.6-xai": 1}),
            "grok-4.6-build")
        self.assertEqual(policy.next_recovery_route("kimi-k2.7-code-go", {"grok-4.6-go": 1}),
                         "grok-4.6-build")
        self.assertEqual(policy.next_recovery_route("grok-4.6-go", {"grok-4.6-xai": 1}),
                         "grok-4.6-build")
        self.assertEqual(policy.next_recovery_route("grok-4.6-go", {}), "grok-4.6-build")
        self.assertEqual(policy.next_recovery_route("grok-4.6-build", {}), "grok-4.6-xai")
        self.assertIsNone(policy.next_recovery_route("grok-4.6-build", {"grok-4.6-xai": 1}))


    def test_signal_classes(self):
        free = {"type": "retry", "action": {"reason": "free_tier_limit", "provider": "opencode"}}
        go_limit = {"name": "APIError", "data": {"statusCode": 429,
                    "responseBody": json.dumps({"error": {"type": "GoUsageLimitError"}})}}
        rate = {"name": "APIError", "data": {"statusCode": 429,
                "responseBody": json.dumps({"error": {"type": "RateLimitError"}})}}
        over = {"name": "APIError", "data": {"statusCode": 529, "responseBody": "overloaded"}}
        ctx = {"type": "error", "error": {"name": "context_length_exceeded"}}
        text_only = {"message": "FreeUsageLimitError mentioned by the model"}
        self.assertEqual(policy.classify_signal(free), "exhausted")
        self.assertEqual(policy.classify_signal(go_limit), "exhausted")
        self.assertEqual(policy.classify_signal(rate), "overloaded")
        self.assertEqual(policy.classify_signal(over), "overloaded")
        self.assertEqual(policy.classify_signal(ctx), "context")
        self.assertIsNone(policy.classify_signal(text_only))
        self.assertIsNone(policy.classify_signal("HTTP 429"))
        # Pool moves need exact exhaustion evidence; free Muse keeps its strict rule.
        self.assertEqual(policy.next_implementation_route("muse-spark-xhigh-free", free), "muse-spark-xhigh-go")
        self.assertIsNone(policy.next_implementation_route("muse-spark-xhigh-free", go_limit))
        self.assertEqual(policy.next_implementation_route("grok-4.6-go", go_limit), "grok-4.6-build")
        self.assertIsNone(policy.next_implementation_route("muse-spark-xhigh-go", go_limit))
        self.assertFalse(policy.classify_quota_exhaustion(go_limit))


class Lanes(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name="ws"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def test_lane_selects_first_route_and_records_params(self):
        job = core.submit(self.sd, "small", {"g": 1}, self.ws("a"), "p", lane="small", planner_t3_thread="planner-t3")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual((job["model"], job["effort"]), ("opencode/muse-spark-1.3-contributor-free", "xhigh"))
        self.assertEqual(job["lane"], "implementation_small")
        job = core.submit(self.sd, "hard", {"g": 1}, self.ws("b"), "p", lane="hard",
                          route="glm-5.3-go", planner_t3_thread="planner-t3")
        self.assertEqual((job["route"], job["lane"]), ("glm-5.3-go", "implementation_hard"))
        job = core.submit(self.sd, "plain", {"g": 1}, self.ws("c"), "p", planner_t3_thread="planner-t3")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual((job["effort"], job["lane"]), ("xhigh", "implementation_default"))
        with self.assertRaises(ValueError):
            core.submit(self.sd, "mismatch", {"g": 1}, self.ws("g"), "p", lane="small",
                        route="grok-4.6-go", planner_t3_thread="planner-t3")

    def test_sticky_home_spreads_parallel_jobs(self):
        core.submit(self.sd, "h1", {"g": 1}, self.ws("a"), "p", lane="hard", planner_t3_thread="planner-t3")
        self.assertEqual(core.get_job(self.sd, "h1")["route"], "muse-spark-xhigh-free")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='h1'")
        finally:
            con.close()
        # Muse free carries no concurrency cap and takes every new job:
        # parallel jobs open parallel Muse free sessions by design, so the
        # second hard job also starts on Muse free (no spread to Muse Go).
        new_ws = self.ws("b")
        job = core.submit(self.sd, "h2", {"g": 1}, new_ws, "p", lane="hard", planner_t3_thread="planner-t3")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")

    def test_three_parallel_jobs_all_start_on_muse_free(self):
        # Amendment: Muse free takes every new job on every lane.
        for lane in ("default", "small", "hard"):
            for i in (1, 2, 3):
                rid = f"{lane}-{i}"
                ws = self.ws(f"{lane}-{i}")
                job = core.submit(self.sd, rid, {"g": i}, ws, "p", lane=lane, planner_t3_thread="planner-t3")
                self.assertEqual(job["route"], "muse-spark-xhigh-free", (lane, rid))
                con = store.connect(self.sd)
                try:
                    con.execute("UPDATE jobs SET status='running' WHERE request_id=?", (rid,))
                finally:
                    con.close()
        counts = core.running_job_counts(self.sd)
        self.assertEqual(counts.get("muse-spark-xhigh-free"), 9)
        # Muse free has no concurrency cap, so it is never full.
        self.assertIsNone(policy.route_max_concurrent("muse-spark-xhigh-free"))
        self.assertFalse(core.route_concurrency_full(self.sd, "muse-spark-xhigh-free"))

    def test_capped_routes_spread_by_fewest_running(self):
        # The `fewest running jobs` spread applies only among capped routes:
        # with every uncapped hard-lane route exhausted, the sticky home
        # spreads across the capped rungs by load.
        # (Degraded, not exhausted: exhaustion is pool-wide and would rest
        # every Go route at once.)
        for route in [r for r in policy.stage_routes("implementation_hard")
                      if policy.route_max_concurrent(r) is None]:
            core.record_capacity(self.sd, route, "degraded", {"source": "test"},
                                 reset_at=core.degraded_until())
        core.submit(self.sd, "cap1", {"g": 1}, self.ws("cap1"), "p",
                    lane="hard", route="glm-5.3-go", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='cap1'")
        finally:
            con.close()
        # glm-5.3-go has max_concurrent 1 and one running job on it, so the
        # sticky home skips the full route and spreads to the least-loaded
        # capped route (deepseek and grok-go tie at 0; the earlier wins).
        self.assertTrue(core.route_concurrency_full(self.sd, "glm-5.3-go"))
        self.assertEqual(core.sticky_home_route(self.sd, "hard"), "deepseek-v4-pro-go")

    def test_limit_error_on_muse_free_moves_to_muse_go(self):
        # A provider limit error on Muse free moves the same model to the
        # next pool (Muse on Go), on the small lane as well.
        core.submit(self.sd, "lim1", {"g": 1}, self.ws("lim1"), "p", lane="small", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='lim1'")
        finally:
            con.close()
        free_evidence = {"type": "retry",
                         "action": {"reason": "free_tier_limit", "provider": "opencode"}}
        self.assertEqual(policy.next_implementation_route("muse-spark-xhigh-free", free_evidence),
                          "muse-spark-xhigh-go")
        res = controller._move_after_signal(self.sd, "lim1", "muse-spark-xhigh-free",
                                            "exhausted", free_evidence)
        self.assertEqual(res["route"], "muse-spark-xhigh-go")
        self.assertIn("pool", res["reason"])
        self.assertEqual(core.get_job(self.sd, "lim1")["route"], "muse-spark-xhigh-go")
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))

    def test_concurrency_cap_excludes_full_routes(self):
        core.submit(self.sd, "h1", {"g": 1}, self.ws("a"), "p", lane="hard",
                    route="glm-5.3-go", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='h1'")
        finally:
            con.close()
        # glm-5.3-go has max_concurrent 1 and one running job on it.
        self.assertTrue(core.route_concurrency_full(self.sd, "glm-5.3-go"))
        self.assertFalse(core.route_concurrency_full(self.sd, "glm-5.3-go", exclude="h1"))
        self.assertFalse(core.route_concurrency_full(self.sd, "grok-4.6-build"))
        self.assertFalse(core.route_concurrency_full(self.sd, "grok-4.6-xai"))
        self.assertFalse(core.route_concurrency_full(self.sd, "muse-spark-xhigh-free"))
        # Sticky home skips the capped route; ties go to the earlier route.
        self.assertEqual(core.sticky_home_route(self.sd, "hard"), "muse-spark-xhigh-free")
        # A full lane returns None.
        for route in ("muse-spark-xhigh-free", "muse-spark-xhigh-go", "glm-5.3-go",
                      "deepseek-v4-pro-go", "grok-4.6-go", "grok-4.6-build",
                      "grok-4.6-xai"):
            con = store.connect(self.sd)
            try:
                core.submit(self.sd, "cap-" + route, {"g": 1}, self.ws("ws-" + route),
                            "p", lane="hard", route=route, planner_t3_thread="planner-t3")
                con2 = store.connect(self.sd)
                con2.execute("UPDATE jobs SET status='running' WHERE request_id=?",
                             ("cap-" + route,))
                con2.close()
            finally:
                con.close()
        # A lane whose capped routes are all full still starts on an
        # uncapped route; the earliest uncapped route wins.
        self.assertEqual(core.sticky_home_route(self.sd, "hard"), "muse-spark-xhigh-free")
        # next_capable_route follows lane order past the capped routes; the
        # native Grok Build route comes before the OpenCode xAI fallback.
        self.assertEqual(core.next_capable_route(self.sd, "muse-spark-xhigh-go",
                                                 "implementation_hard", exclude="cap"),
                         "grok-4.6-build")
        self.assertEqual(core.next_capable_route(self.sd, "grok-4.6-build",
                                                 "implementation_hard", exclude="cap"),
                         "grok-4.6-xai")

    def test_critical_lane_is_planner_executed(self):
        with self.assertRaises(ValueError) as cm:
            core.submit(self.sd, "crit", {"g": 1}, self.ws("d"), "p", lane="critical", planner_t3_thread="planner-t3")
        self.assertIn("planner", str(cm.exception))
        with self.assertRaises(ValueError):
            core.submit(self.sd, "rev", {"g": 1}, self.ws("e"), "p", route="luna/max", planner_t3_thread="planner-t3")

    def test_research_action_is_reserved_and_blocks(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws("f"), "p", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running', controller_state=?"
                        " WHERE request_id='r1'",
                        (json.dumps({"seq": 1, "last_action": {"action": "research", "qid": "x"},
                                     "t3_threads": {"dispatch": {"thread_id": "sub.planner-t3.disp",
                                                                 "route": "luna/max"}}}),))
        finally:
            con.close()
        self.assertIn("research", policy.VALID_ACTIONS)
        res = controller.step(self.sd, "r1")
        self.assertEqual(res["reason"], "unsupported_luna_action")
        self.assertEqual(core.get_job(self.sd, "r1")["status"], "blocked")


class StickyIdempotent(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name="ws"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def test_identical_resubmission_returns_stored_route(self):
        first = core.submit(self.sd, "idem", {"g": 1}, self.ws("a"), "p", lane="hard", planner_t3_thread="planner-t3")
        self.assertEqual(first["route"], "muse-spark-xhigh-free")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='idem'")
        finally:
            con.close()
        # Running counts changed (muse-free now occupied); an identical
        # resubmission must still return the stored job, not conflict.
        second = core.submit(self.sd, "idem", {"g": 1}, self.ws("a"), "p", lane="hard", planner_t3_thread="planner-t3")
        self.assertEqual(second["route"], "muse-spark-xhigh-free")
        self.assertEqual(second["request_id"], "idem")


class ValidateFixes(unittest.TestCase):
    def test_muse_free_first_and_uncapped(self):
        # Every implementation lane starts on Muse free, which carries no cap.
        for lane in policy.IMPLEMENTATION_LANES:
            self.assertEqual(policy.STAGES[lane]["routes"][0], "muse-spark-xhigh-free", lane)
        self.assertIsNone(policy.route_max_concurrent("muse-spark-xhigh-free"))
        orig = dict(policy.ROUTES["muse-spark-xhigh-free"])
        try:
            policy.ROUTES["muse-spark-xhigh-free"]["max_concurrent"] = 1
            problems = policy.validate_policy()
            self.assertTrue(any("muse-spark-xhigh-free" in p and "no concurrency cap" in p
                                for p in problems), problems)
        finally:
            policy.ROUTES["muse-spark-xhigh-free"].clear()
            policy.ROUTES["muse-spark-xhigh-free"].update(orig)
        orig_routes = list(policy.STAGES["implementation_small"]["routes"])
        try:
            policy.STAGES["implementation_small"]["routes"] = orig_routes[1:]
            problems = policy.validate_policy()
            self.assertTrue(any("implementation_small" in p and "muse-spark-xhigh-free" in p
                                for p in problems), problems)
        finally:
            policy.STAGES["implementation_small"]["routes"] = orig_routes
        self.assertEqual(policy.validate_policy(), [])


class ControllerCapsAndRecovery(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")

    def ws(self, name="ws"):
        d = self.base / name
        d.mkdir(exist_ok=True)
        return str(d)

    def _mark_running(self, rid):
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id=?", (rid,))
        finally:
            con.close()

    def _set_route(self, rid, route, lane=None):
        con = store.connect(self.sd)
        try:
            if lane is None:
                lane = policy.lane_of_route(route, None) or "implementation_default"
            con.execute("UPDATE jobs SET route=?, lane=?, status='running' WHERE request_id=?",
                        (route, lane, rid))
        finally:
            con.close()

    def test_preflight_full_capped_route_moves_to_next(self):
        core.submit(self.sd, "j1", {"g": 1}, self.ws("a"), "p", lane="default",
                    route="qwen3.8-flash-go", planner_t3_thread="planner-t3")
        core.submit(self.sd, "j2", {"g": 1}, self.ws("b"), "p", lane="default",
                    route="qwen3.8-flash-go", planner_t3_thread="planner-t3")
        core.submit(self.sd, "j3", {"g": 1}, self.ws("c"), "p", lane="default",
                    route="deepseek-v4.1-flash-go", planner_t3_thread="planner-t3")
        for rid in ("j1", "j2", "j3"):
            self._mark_running(rid)
        # j1 sits on qwen (cap 1, filled by j2); deepseek is also full (j3).
        # Preflight must skip both capped routes to hy3-go.
        move = controller._preflight_move(self.sd, "j1", "qwen3.8-flash-go")
        self.assertIsNotNone(move)
        self.assertEqual(move["route"], "hy3-go")
        self.assertIn("concurrent", move["reason"])
        self.assertEqual(core.get_job(self.sd, "j1")["route"], "hy3-go")

    def test_concurrent_reservations_yield_one_winner(self):
        import threading
        core.submit(self.sd, "c1", {"g": 1}, self.ws("a"), "p", lane="default",
                    route="muse-spark-xhigh-free", planner_t3_thread="planner-t3")
        core.submit(self.sd, "c2", {"g": 1}, self.ws("b"), "p", lane="default",
                    route="muse-spark-xhigh-free", planner_t3_thread="planner-t3")
        self._mark_running("c1")
        self._mark_running("c2")
        # Both target the same capped route; the atomic reservation inside
        # _switch_route lets exactly one winner keep it.
        results = {}
        barrier = threading.Barrier(2)

        def move(rid):
            barrier.wait(timeout=5)
            try:
                results[rid] = controller._switch_route(self.sd, rid, "qwen3.8-flash-go",
                                                        "test", {"source": "test"})
            except Exception as e:  # noqa: BLE001
                results[rid] = {"error": f"{type(e).__name__}: {e}"}

        t1 = threading.Thread(target=move, args=("c1",))
        t2 = threading.Thread(target=move, args=("c2",))
        t1.start()
        t2.start()
        t1.join(15)
        t2.join(15)
        self.assertNotIn("error", str(results))
        routes = sorted([core.get_job(self.sd, "c1")["route"],
                         core.get_job(self.sd, "c2")["route"]])
        # One winner on qwen, the loser on the next free capped route.
        self.assertIn("qwen3.8-flash-go", routes)
        self.assertEqual(len(set(routes)), 2)
        counts = core.running_job_counts(self.sd)
        self.assertLessEqual(counts.get("qwen3.8-flash-go", 0), 1)

    def test_dispatch_fallback_respects_cap(self):
        core.submit(self.sd, "d1", {"g": 1}, self.ws("a"), "p", planner_t3_thread="planner-t3")
        core.submit(self.sd, "d2", {"g": 1}, self.ws("b"), "p", planner_t3_thread="planner-t3")
        self._mark_running("d1")
        self._mark_running("d2")
        controller._set_phase(self.sd, "d1", dispatch_route="luna-go/max")
        self.assertTrue(core.dispatch_route_full(self.sd, "luna-go/max"))
        # The capped fallback is refused before any dispatcher thread starts.
        res = controller._claim_capped_dispatch_route(self.sd, "d2", "luna-go/max")
        self.assertEqual(res["reason"], "capacity_exhausted")
        self.assertEqual(core.get_job(self.sd, "d2")["status"], "blocked")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running', block_reason=NULL WHERE request_id='d2'")
        finally:
            con.close()
        # When free, the reservation succeeds without blocking.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='succeeded' WHERE request_id='d1'")
        finally:
            con.close()
        self.assertFalse(core.dispatch_route_full(self.sd, "luna-go/max"))
        self.assertTrue(controller._reserve_dispatch_route(self.sd, "d2", "luna-go/max"))

    def _insert_turn(self, rid, route, seq=0):
        # A worker turn is recorded as its T3 child thread slot.
        job = core.get_job(self.sd, rid)
        st = json.loads(job.get("controller_state") or "{}")
        st.setdefault("t3_threads", {})[f"impl_{seq}"] = {
            "thread_id": f"sub.planner-t3.{rid}{seq}", "route": route}
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id=?",
                        (json.dumps(st, sort_keys=True), rid))
        finally:
            con.close()

    def test_recovery_after_used_rung_for_small_lane(self):
        core.submit(self.sd, "s1", {"g": 1}, self.ws("a"), "p", lane="small", planner_t3_thread="planner-t3")
        self._mark_running("s1")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')) WHERE request_id='s1'")
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='s1'",
                        (json.dumps({"ladder": {"failures": 3, "rung": "correction_fresh"}}),))
        finally:
            con.close()
        self._insert_turn("s1", "grok-4.6-go", seq=0)
        # grok-go already used: escalation must skip to the native Build
        # route first, never straight to the OpenCode fallback.
        self.assertEqual(policy.next_recovery_route(None, {"grok-4.6-go": 1}), "grok-4.6-build")
        res = controller._apply_ladder(self.sd, "s1")
        self.assertIsNone(res)
        self.assertEqual(core.get_job(self.sd, "s1")["route"], "grok-4.6-build")

    def test_overload_move_during_recovery(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws("a"), "p", lane="small", planner_t3_thread="planner-t3")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route='grok-4.6-go', lane='implementation_small',"
                        " status='running', controller_state=json_set(COALESCE(controller_state,'{}'),'$.t3_threads.dispatch',json_object('thread_id','sub.planner-t3.disp','route','luna/max')) WHERE request_id='r1'")
        finally:
            con.close()
        # Overload on the recovery rung must move inside recovery, never raise.
        # The native Build route is next; the OpenCode fallback follows it.
        res = controller._move_after_signal(self.sd, "r1", "grok-4.6-go", "overloaded",
                                            {"name": "RateLimitError"})
        self.assertEqual(res["route"], "grok-4.6-build")
        self.assertEqual(core.get_job(self.sd, "r1")["route"], "grok-4.6-build")
        self.assertIn("grok-4.6-go", core.degraded_routes(self.sd))
        # Overload on the native route falls back to OpenCode's xAI provider.
        res = controller._move_after_signal(self.sd, "r1", "grok-4.6-build", "overloaded",
                                            {"name": "RateLimitError"})
        self.assertEqual(res["route"], "grok-4.6-xai")
        self.assertEqual(core.get_job(self.sd, "r1")["route"], "grok-4.6-xai")


if __name__ == "__main__":
    unittest.main()
