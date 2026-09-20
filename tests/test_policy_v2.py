"""Policy v2: data consistency, derived skill table, lanes, and signal classes."""
import json
import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import cli, controller, core, policy, store  # noqa: E402


class PolicyData(unittest.TestCase):
    def test_policy_consistent(self):
        self.assertEqual(policy.validate_policy(), [])
        self.assertEqual(policy.POLICY_ID, "durable-runner-policy-v2")
        self.assertTrue(policy.POLICY_SOURCE and policy.POLICY_EVIDENCE)

    def test_every_route_has_adapter_and_subscription_pool(self):
        for name, spec in policy.ROUTES.items():
            self.assertIn(spec["harness"], ("codex", "claude", "opencode"), name)
            self.assertIn(spec["pool"], policy.POOLS, name)
            self.assertTrue(policy.is_operational(name))
            self.assertIsNone(policy.route_blocker(name))
            if spec["harness"] == "opencode":
                provider = spec["model"].split("/", 1)[0]
                self.assertEqual(provider, policy.POOLS[spec["pool"]]["provider"], name)
        self.assertFalse(policy.ALLOW_ZEN_OVERFLOW)
        self.assertFalse(policy.ALLOW_DIRECT_PAID_API)
        self.assertNotIn("api-key", {p["credential"] for p in policy.POOLS.values()})

    def test_older_generations_absent(self):
        models = {s["model"].split("/", 1)[-1].removesuffix("-free") for s in policy.ROUTES.values()}
        self.assertFalse(models & set(policy.EXCLUDED_MODELS))
        self.assertNotIn("gpt-5.6-sol", models)

    def test_never_implementers_absent_from_lanes(self):
        lane_routes = set(policy.implementation_routes()) | set(policy.stage_routes("correction"))
        for route in lane_routes:
            spec = policy.ROUTES[route]
            if spec["pool"] != "go":
                continue
            model_id = spec["model"].split("/", 1)[1]
            self.assertNotIn(model_id, policy.GO_IMPLEMENTER_EXCLUDED, route)
            if model_id == "grok-4.6":
                self.assertIn(route, policy.GO_IMPLEMENTER_GROK_ROUTES, route)

    def test_no_gemini_or_antigravity_routes(self):
        models = " ".join(s["model"] for s in policy.ROUTES.values())
        self.assertNotRegex(models, r"gemini|antigravity")

    def test_go_tiers_come_from_go_plan(self):
        for route in policy.implementation_routes():
            spec = policy.ROUTES[route]
            if spec["pool"] == "go":
                model_id = spec["model"].split("/", 1)[1]
                self.assertIn(policy.monthly_limit_usd(route), (60, 30, 15), (route, model_id))
        # DeepSeek V4.1 Flash is a 15 USD route from 2026-09-20.
        self.assertEqual(policy.monthly_limit_usd("deepseek-v4.1-flash-go"), 15)
        self.assertEqual(policy.DEEPSEEK_V4_1_FLASH_TIER_FROM, "2026-09-20")
        # Models with no known tier are not routes.
        with self.assertRaises(ValueError):
            policy.route_spec("qwen3.7-plus-go")

    def test_max_concurrent_on_15_and_30_tiers(self):
        capped = {r for r in policy.ROUTES if policy.route_max_concurrent(r) == 1}
        for route in policy.ROUTES:
            limit = policy.monthly_limit_usd(route)
            if limit is None:
                self.assertIsNone(policy.route_max_concurrent(route), route)
            elif limit in (15, 30):
                self.assertIn(route, capped, route)
                self.assertTrue(policy.ROUTES[route]["max_concurrent"])
            else:
                self.assertIsNone(policy.route_max_concurrent(route), route)
        self.assertIn("deepseek-v4.1-flash-go", capped)

    def test_scarce_models_get_one_turn(self):
        self.assertEqual({r for r in policy.ROUTES if policy.one_turn_per_job(r)},
                         {"glm-5.3-go", "deepseek-v4-pro-go", "grok-4.6-go", "luna-go/max",
                          "deepseek-v4.1-flash-go", "luna-go-review"})
        self.assertTrue(policy.one_turn_routes_used("deepseek-v4.1-flash-go",
                                                    {"deepseek-v4.1-flash-go": 1}))
        self.assertFalse(policy.one_turn_routes_used("deepseek-v4.1-flash-go", {}))
        self.assertFalse(policy.one_turn_routes_used("muse-spark-xhigh-go", {"muse-spark-xhigh-go": 3}))
        # A used one-turn route is skipped as a lateral target.
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-go", lane="hard",
                                                  turns_by_route={"glm-5.3-go": 1}),
                         "deepseek-v4-pro-go")
        self.assertIsNone(policy.monthly_limit_usd("muse-spark-xhigh-free"))
        self.assertEqual(policy.monthly_limit_usd("muse-spark-xhigh-go"), 60)

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
        # hard: Muse free, Muse Go, GLM-5.3, DeepSeek V4 Pro, Grok 4.6 Go, xAI.
        self.assertEqual(s["implementation_hard"]["routes"],
                         ["muse-spark-xhigh-free", "muse-spark-xhigh-go", "glm-5.3-go",
                          "deepseek-v4-pro-go", "grok-4.6-go", "grok-4.6-xai"])
        self.assertEqual(s["correction"]["routes"], ["kimi-k2.7-code-go"])
        self.assertEqual(s["recovery"]["routes"], ["grok-4.6-go", "grok-4.6-xai"])
        self.assertEqual(s["dispatch"]["routes"], ["luna/max", "luna-go/max"])
        self.assertEqual(s["dispatch"]["manual"], ["terra/max"])
        self.assertTrue(policy.ROUTES["terra/max"]["manual"])
        self.assertEqual(policy.ROUTES["luna-go/max"]["agent"], "plan")
        # Planner and review fallback routes on other subscriptions.
        self.assertEqual(s["planning"]["routes"], ["fable-5.1/max", "astra/max"])
        self.assertEqual(s["planner_rungs"]["executor"], "planner")
        self.assertEqual(s["planner_rungs"]["routes"], ["astra/medium", "opus-5/high"])
        self.assertTrue(s["planner_rungs"]["planner_selects"])
        self.assertTrue(policy.ROUTES["astra/medium"]["planner_chosen"])
        self.assertTrue(policy.ROUTES["opus-5/high"]["planner_chosen"])
        self.assertEqual(policy.ROUTES["astra/max"]["model"], "gpt-6-astra")
        self.assertEqual(policy.ROUTES["astra/max"]["pool"], "codex")
        self.assertEqual(policy.ROUTES["opus-5/high"]["harness"], "claude")
        self.assertEqual(s["review_ticket"]["routes"], ["luna-max-review", "luna-go-review"])
        self.assertEqual(policy.ROUTES["luna-go-review"]["agent"], "plan")
        self.assertEqual(s["review_final"]["routes"],
                         ["opus-5/high-review", "astra/high-review", "luna-max-review"])
        self.assertEqual(s["review_final"]["executor"], "host")
        self.assertEqual(s["critical"]["executor"], "planner")
        self.assertEqual(s["critical"]["routes"], [])
        self.assertEqual(policy.ROUTES["luna/max"]["sandbox"], "read-only")

    def test_planner_chosen_rungs_are_not_implementation_lanes(self):
        for stage in policy.IMPLEMENTATION_LANES:
            for route in policy.stage_routes(stage):
                self.assertFalse(policy.ROUTES[route].get("planner_chosen"), (stage, route))
                self.assertFalse(policy.ROUTES[route].get("manual"), (stage, route))
        for route in policy.implementation_routes() + policy.stage_routes("correction"):
            self.assertNotEqual(policy.route_spec(route).get("variant"), "terra")

    def test_route_params_and_pool_moves(self):
        self.assertEqual(policy.opencode_route_params("glm-5.3-flash-go"),
                         ("opencode-go/glm-5.3-flash", None, "build"))
        self.assertEqual(policy.opencode_route_params("grok-4.6-xai"), ("xai/grok-4.6", "medium", "build"))
        with self.assertRaises(ValueError):
            policy.opencode_route_params("bogus")
        with self.assertRaises(ValueError):
            policy.opencode_route_params("luna/max")
        self.assertEqual(policy.next_pool_route("muse-spark-xhigh-free"), "muse-spark-xhigh-go")
        self.assertEqual(policy.next_pool_route("grok-4.6-go"), "grok-4.6-xai")
        self.assertIsNone(policy.next_pool_route("muse-spark-xhigh-go"))
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free"), "glm-5.3-flash-go")
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free",
                                                  degraded={"glm-5.3-flash-go", "qwen3.8-flash-go",
                                                            "deepseek-v4.1-flash-go", "hy3-go"}),
                         "minimax-m3-go")
        # The stored lane decides where a shared route moves.
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free", lane="hard"),
                         "glm-5.3-go")
        self.assertEqual(policy.next_capacity_route("muse-spark-xhigh-go", set(), lane="hard"),
                         ("glm-5.3-go", None))
        self.assertEqual(policy.lane_of_route("muse-spark-xhigh-free", "hard"), "implementation_hard")
        self.assertEqual(policy.lane_of_route("muse-spark-xhigh-free"), "implementation_default")
        self.assertEqual(policy.validate_route("grok-4.6-xai")["allowance"], "xai-subscription")
        self.assertEqual(policy.route_allowance("grok-4.6-xai"), "xai-subscription")

    def test_correction_advances_to_recovery(self):
        self.assertEqual(policy.next_recovery_route("kimi-k2.7-code-go"), "grok-4.6-go")
        self.assertEqual(policy.next_recovery_route("grok-4.6-go"), "grok-4.6-xai")
        self.assertIsNone(policy.next_recovery_route("grok-4.6-xai"))
        # Rungs the job already used are skipped.
        self.assertIsNone(policy.next_recovery_route("kimi-k2.7-code-go",
                                                     {"grok-4.6-go": 1, "grok-4.6-xai": 1}))
        self.assertEqual(policy.next_recovery_route("kimi-k2.7-code-go", {"grok-4.6-go": 1}),
                         "grok-4.6-xai")
        self.assertIsNone(policy.next_recovery_route("grok-4.6-go", {"grok-4.6-xai": 1}))
        self.assertEqual(policy.next_recovery_route("grok-4.6-go", {}), "grok-4.6-xai")

    def test_session_permission_flags(self):
        rules = {r["permission"]: r["action"]
                 for r in policy.session_permissions("glm-5.3-flash-go")}
        self.assertEqual(rules["external_directory"], "allow")
        self.assertEqual(rules["webfetch"], "allow")
        self.assertEqual(rules["websearch"], "allow")
        self.assertEqual(rules["doom_loop"], "allow")
        self.assertEqual(rules["question"], "deny")
        self.assertEqual(rules["task"], "deny")
        # Missing or unknown routes resolve to read-only (least privilege).
        rules = {r["permission"]: r["action"] for r in policy.session_permissions()}
        self.assertEqual(rules["external_directory"], "deny")
        self.assertEqual(rules["task"], "deny")
        rules = {r["permission"]: r["action"] for r in policy.session_permissions(None)}
        self.assertEqual(rules["external_directory"], "deny")
        from runner import adapters
        self.assertEqual(tuple(adapters.SESSION_PERMISSION_RULES),
                         tuple(policy.DEFAULT_SESSION_PERMISSIONS))

    def test_harness_defaults_come_from_policy(self):
        from runner import adapters
        self.assertEqual(adapters.CODEX_MODEL, policy.ROUTES["luna/max"]["model"])
        self.assertEqual(adapters.CODEX_EFFORT, policy.ROUTES["luna/max"]["variant"])
        self.assertEqual(adapters.CLAUDE_MODEL, policy.ROUTES["fable-5.1/max"]["model"])
        self.assertEqual(adapters.CLAUDE_LIVE_MODEL, policy.ROUTES["sonnet/medium"]["model"])
        self.assertEqual(adapters.OPENCODE_FREE_MODEL, policy.ROUTES["muse-spark-xhigh-free"]["model"])
        cmd = adapters.build_codex_dispatch_cmd("/tmp/ws", "p", model="gpt-x", effort="high")
        self.assertIn("gpt-x", cmd)
        self.assertIn('model_reasoning_effort="high"', cmd)
        cmd = adapters.build_codex_resume_cmd("t1", "p", model="gpt-y", effort="low")
        self.assertIn("gpt-y", cmd)
        self.assertIn('model_reasoning_effort="low"', cmd)
        with self.assertRaises(ValueError):
            policy.lane_of_route("glm-5.3-go", "small")
        self.assertIsNone(policy.lane_of_route("luna/max", "small"))

    def test_skill_reference_is_generated_from_policy(self):
        rendered = policy.render_skill_table()
        on_disk = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        self.assertEqual(on_disk, rendered, "run policy.main(['render-skill']) and commit the result")
        self.assertNotIn("sol", rendered.lower())
        self.assertIn("critical", rendered)
        self.assertIn("python -m runner submit", rendered)
        self.assertEqual(policy.main(["validate"]), 0)

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
        self.assertEqual(policy.classify_signal(ctx), "hard")
        self.assertIsNone(policy.classify_signal(text_only))
        self.assertIsNone(policy.classify_signal("HTTP 429"))
        # Pool moves need exact exhaustion evidence; free Muse keeps its strict rule.
        self.assertEqual(policy.next_implementation_route("muse-spark-xhigh-free", free), "muse-spark-xhigh-go")
        self.assertIsNone(policy.next_implementation_route("muse-spark-xhigh-free", go_limit))
        self.assertEqual(policy.next_implementation_route("grok-4.6-go", go_limit), "grok-4.6-xai")
        self.assertIsNone(policy.next_implementation_route("muse-spark-xhigh-go", go_limit))
        self.assertFalse(policy.classify_quota_exhaustion(go_limit))

    def test_cli_submit_uses_policy_planner_default_and_explicit_overrides(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state_dir = str(Path(tmp.name) / "state")
        default_workspace = Path(tmp.name) / "default-workspace"
        explicit_workspace = Path(tmp.name) / "explicit-workspace"
        default_workspace.mkdir()
        explicit_workspace.mkdir()

        with redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main([
                "--state-dir", state_dir, "submit", "--request-id", "cli-default",
                "--task", '{"goal":"default"}', "--workspace", str(default_workspace),
                "--planner-session", "claude-1", "--no-start",
            ]), 0)
            self.assertEqual(cli.main([
                "--state-dir", state_dir, "submit", "--request-id", "cli-explicit",
                "--task", '{"goal":"explicit"}', "--workspace", str(explicit_workspace),
                "--planner-session", "claude-1", "--planner-model", "custom-model",
                "--planner-effort", "custom-effort", "--no-start",
            ]), 0)

        planning = policy.route_spec(policy.STAGES["planning"]["routes"][0])
        default_job = core.get_job(state_dir, "cli-default")
        self.assertEqual(default_job["planner_model"], planning["model"])
        self.assertEqual(default_job["planner_effort"], planning["variant"])
        explicit_job = core.get_job(state_dir, "cli-explicit")
        self.assertEqual(explicit_job["planner_model"], "custom-model")
        self.assertEqual(explicit_job["planner_effort"], "custom-effort")


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
        job = core.submit(self.sd, "small", {"g": 1}, self.ws("a"), "p", lane="small")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual((job["model"], job["effort"]), ("opencode/muse-spark-1.3-contributor-free", "xhigh"))
        self.assertEqual(job["lane"], "implementation_small")
        job = core.submit(self.sd, "hard", {"g": 1}, self.ws("b"), "p", lane="hard",
                          route="glm-5.3-go")
        self.assertEqual((job["route"], job["lane"]), ("glm-5.3-go", "implementation_hard"))
        job = core.submit(self.sd, "plain", {"g": 1}, self.ws("c"), "p")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual((job["effort"], job["lane"]), ("xhigh", "implementation_default"))
        with self.assertRaises(ValueError):
            core.submit(self.sd, "mismatch", {"g": 1}, self.ws("g"), "p", lane="small",
                        route="grok-4.6-go")

    def test_sticky_home_spreads_parallel_jobs(self):
        core.submit(self.sd, "h1", {"g": 1}, self.ws("a"), "p", lane="hard")
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
        job = core.submit(self.sd, "h2", {"g": 1}, new_ws, "p", lane="hard")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")

    def test_three_parallel_jobs_all_start_on_muse_free(self):
        # Amendment: Muse free takes every new job on every lane.
        for lane in ("default", "small", "hard"):
            for i in (1, 2, 3):
                rid = f"{lane}-{i}"
                ws = self.ws(f"{lane}-{i}")
                job = core.submit(self.sd, rid, {"g": i}, ws, "p", lane=lane)
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
        for route in [r for r in policy.stage_routes("implementation_hard")
                      if policy.route_max_concurrent(r) is None]:
            core.record_capacity(self.sd, route, "exhausted", {"source": "test"})
        core.submit(self.sd, "cap1", {"g": 1}, self.ws("cap1"), "p",
                    lane="hard", route="glm-5.3-go")
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
        core.submit(self.sd, "lim1", {"g": 1}, self.ws("lim1"), "p", lane="small")
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
                    route="glm-5.3-go")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='h1'")
        finally:
            con.close()
        # glm-5.3-go has max_concurrent 1 and one running job on it.
        self.assertTrue(core.route_concurrency_full(self.sd, "glm-5.3-go"))
        self.assertFalse(core.route_concurrency_full(self.sd, "glm-5.3-go", exclude="h1"))
        self.assertFalse(core.route_concurrency_full(self.sd, "grok-4.6-xai"))
        self.assertFalse(core.route_concurrency_full(self.sd, "muse-spark-xhigh-free"))
        # Sticky home skips the capped route; ties go to the earlier route.
        self.assertEqual(core.sticky_home_route(self.sd, "hard"), "muse-spark-xhigh-free")
        # A full lane returns None.
        for route in ("muse-spark-xhigh-free", "muse-spark-xhigh-go", "glm-5.3-go",
                      "deepseek-v4-pro-go", "grok-4.6-go", "grok-4.6-xai"):
            con = store.connect(self.sd)
            try:
                core.submit(self.sd, "cap-" + route, {"g": 1}, self.ws("ws-" + route),
                            "p", lane="hard", route=route)
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
        # last route (no cap) still has free concurrency.
        self.assertEqual(core.next_capable_route(self.sd, "muse-spark-xhigh-go",
                                                 "implementation_hard", exclude="cap"),
                         "grok-4.6-xai")

    def test_critical_lane_is_planner_executed(self):
        with self.assertRaises(ValueError) as cm:
            core.submit(self.sd, "crit", {"g": 1}, self.ws("d"), "p", lane="critical")
        self.assertIn("planner", str(cm.exception))
        with self.assertRaises(ValueError):
            core.submit(self.sd, "rev", {"g": 1}, self.ws("e"), "p", route="luna/max")

    def test_research_action_is_reserved_and_blocks(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws("f"), "p")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='thread', status='running', controller_state=?"
                        " WHERE request_id='r1'",
                        (json.dumps({"seq": 1, "last_action": {"action": "research", "qid": "x"}}),))
        finally:
            con.close()
        self.assertIn("research", policy.VALID_ACTIONS)
        res = controller.step(self.sd, "r1", run_cmd=lambda *a, **k: (1, "", "unexpected"))
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
        first = core.submit(self.sd, "idem", {"g": 1}, self.ws("a"), "p", lane="hard")
        self.assertEqual(first["route"], "muse-spark-xhigh-free")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='running' WHERE request_id='idem'")
        finally:
            con.close()
        # Running counts changed (muse-free now occupied); an identical
        # resubmission must still return the stored job, not conflict.
        second = core.submit(self.sd, "idem", {"g": 1}, self.ws("a"), "p", lane="hard")
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

    def test_max_concurrent_must_be_exactly_one(self):
        orig = dict(policy.ROUTES["qwen3.8-flash-go"])
        try:
            policy.ROUTES["qwen3.8-flash-go"]["max_concurrent"] = 2
            problems = policy.validate_policy()
            self.assertTrue(any("qwen3.8-flash-go" in p and "max_concurrent must be 1" in p
                                for p in problems), problems)
        finally:
            policy.ROUTES["qwen3.8-flash-go"].update(orig)
        # A 60 USD route must carry no cap.
        orig60 = dict(policy.ROUTES["glm-5.3-flash-go"])
        try:
            policy.ROUTES["glm-5.3-flash-go"]["max_concurrent"] = 1
            problems = policy.validate_policy()
            self.assertTrue(any("glm-5.3-flash-go" in p and "max_concurrent must be None" in p
                                for p in problems), problems)
        finally:
            if "max_concurrent" in orig60:
                policy.ROUTES["glm-5.3-flash-go"]["max_concurrent"] = orig60["max_concurrent"]
            else:
                policy.ROUTES["glm-5.3-flash-go"].pop("max_concurrent", None)
        self.assertEqual(policy.validate_policy(), [])

    def test_guarded_route_in_lane_recovery_or_dispatch_fails(self):
        orig_routes = list(policy.STAGES["implementation_default"]["routes"])
        try:
            policy.STAGES["implementation_default"]["routes"] = orig_routes + ["terra/max"]
            problems = policy.validate_policy()
            self.assertTrue(any("implementation_default" in p and "never" in p for p in problems),
                            problems)
        finally:
            policy.STAGES["implementation_default"]["routes"] = orig_routes
        orig_rec = list(policy.STAGES["recovery"]["routes"])
        try:
            policy.STAGES["recovery"]["routes"] = orig_rec + ["terra/max"]
            problems = policy.validate_policy()
            self.assertTrue(any("recovery" in p and "never" in p for p in problems), problems)
        finally:
            policy.STAGES["recovery"]["routes"] = orig_rec
        self.assertEqual(policy.validate_policy(), [])


class SessionPermissionsByRole(unittest.TestCase):
    def test_implementation_gets_full_access_dispatch_gets_read_only(self):
        full = {r["permission"]: r["action"]
                for r in policy.session_permissions("glm-5.3-flash-go")}
        self.assertEqual(full["external_directory"], "allow")
        self.assertEqual(full["webfetch"], "allow")
        self.assertEqual(full["websearch"], "allow")
        self.assertEqual(full["doom_loop"], "allow")
        self.assertEqual(full["question"], "deny")
        self.assertEqual(full["task"], "deny")
        ro = {r["permission"]: r["action"]
              for r in policy.session_permissions("luna-go/max")}
        self.assertEqual(ro["edit"], "deny")
        self.assertEqual(ro["external_directory"], "deny")
        self.assertEqual(ro["doom_loop"], "deny")
        self.assertEqual(ro["question"], "deny")
        self.assertEqual(ro["task"], "deny")
        ro_rev = {r["permission"]: r["action"]
                  for r in policy.session_permissions("luna-go-review")}
        self.assertEqual(ro_rev["edit"], "deny")
        self.assertEqual(ro_rev["external_directory"], "deny")
        rec = {r["permission"]: r["action"]
               for r in policy.session_permissions("grok-4.6-go")}
        self.assertEqual(rec["external_directory"], "allow")
        self.assertEqual(rec["doom_loop"], "allow")

    def test_both_sets_reach_create_session(self):
        from runner import adapters
        seen = {}

        def fake_request(method, path, body):
            base = path.split("?", 1)[0]
            if method == "POST" and base == "/session":
                seen[base + str(len(seen))] = list(body.get("permission") or [])
                sid = f"ses_{len(seen):06d}"
                return {"id": sid, "directory": "/tmp"}
            if base == "/global/health":
                return {"healthy": True}
            return {}

        for route in ("glm-5.3-flash-go", "luna-go/max"):
            client = adapters.OpenCodeClient("http://127.0.0.1:9", "pwd",
                                             directory="/tmp",
                                             request_func=fake_request)
            got = client.create_session(title="t",
                                        permission=policy.session_permissions(route))
            self.assertTrue(got["id"].startswith("ses_"))
        perms = list(seen.values())
        self.assertEqual(len(perms), 2)
        full = {r["permission"]: r["action"] for r in perms[0]}
        ro = {r["permission"]: r["action"] for r in perms[1]}
        self.assertEqual(full["external_directory"], "allow")
        self.assertEqual(ro["edit"], "deny")
        self.assertEqual(ro["external_directory"], "deny")


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
                    route="qwen3.8-flash-go")
        core.submit(self.sd, "j2", {"g": 1}, self.ws("b"), "p", lane="default",
                    route="qwen3.8-flash-go")
        core.submit(self.sd, "j3", {"g": 1}, self.ws("c"), "p", lane="default",
                    route="deepseek-v4.1-flash-go")
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
                    route="muse-spark-xhigh-free")
        core.submit(self.sd, "c2", {"g": 1}, self.ws("b"), "p", lane="default",
                    route="muse-spark-xhigh-free")
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
        core.submit(self.sd, "d1", {"g": 1}, self.ws("a"), "p")
        core.submit(self.sd, "d2", {"g": 1}, self.ws("b"), "p")
        self._mark_running("d1")
        self._mark_running("d2")
        controller._set_phase(self.sd, "d1", dispatch_route="luna-go/max")
        self.assertTrue(core.dispatch_route_full(self.sd, "luna-go/max"))
        called = []

        def fake_run(cmd, cwd=None, timeout=None, **kw):
            called.append(cmd)
            return 0, "", ""

        res = controller._dispatch_on_opencode(self.sd, "d2", "luna-go/max", "prompt",
                                               fake_run, reason="test")
        self.assertEqual(res["reason"], "capacity_exhausted")
        self.assertEqual(called, [])
        # When free, the reservation succeeds without blocking.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET status='succeeded' WHERE request_id='d1'")
        finally:
            con.close()
        self.assertFalse(core.dispatch_route_full(self.sd, "luna-go/max"))
        self.assertTrue(controller._reserve_dispatch_route(self.sd, "d2", "luna-go/max"))

    def _insert_turn(self, rid, route, seq=0):
        root = store.ensure_state_dir(self.sd)
        stdout = root / "outputs" / f"{rid}-{seq}.stdout"
        stderr = root / "outputs" / f"{rid}-{seq}.stderr"
        store.secure_write_text(stdout, "")
        store.secure_write_text(stderr, "")
        con = store.connect(self.sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                "stdout_path,stderr_path,started_at,state,meta_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (f"{rid}-{seq}", rid, "opencode_control", "[]", self.ws("a"), "tok",
                 str(stdout), str(stderr), core._utcnow(), "completed",
                 json.dumps({"route": route, "seq": seq})))
        finally:
            con.close()

    def test_recovery_after_used_rung_for_small_lane(self):
        core.submit(self.sd, "s1", {"g": 1}, self.ws("a"), "p", lane="small")
        self._mark_running("s1")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET codex_task_id='th' WHERE request_id='s1'")
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='s1'",
                        (json.dumps({"ladder": {"failures": 3, "rung": "correction_fresh"}}),))
        finally:
            con.close()
        self._insert_turn("s1", "grok-4.6-go", seq=0)
        # grok-go already used: escalation must skip to xai, not block.
        self.assertEqual(policy.next_recovery_route(None, {"grok-4.6-go": 1}), "grok-4.6-xai")
        res = controller._apply_ladder(self.sd, "s1")
        self.assertIsNone(res)
        self.assertEqual(core.get_job(self.sd, "s1")["route"], "grok-4.6-xai")

    def test_overload_move_during_recovery(self):
        core.submit(self.sd, "r1", {"g": 1}, self.ws("a"), "p", lane="small")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route='grok-4.6-go', lane='implementation_small',"
                        " status='running', codex_task_id='th' WHERE request_id='r1'")
        finally:
            con.close()
        # Overload on the recovery rung must move inside recovery, never raise.
        res = controller._move_after_signal(self.sd, "r1", "grok-4.6-go", "overloaded",
                                            {"name": "RateLimitError"})
        self.assertEqual(res["route"], "grok-4.6-xai")
        self.assertEqual(core.get_job(self.sd, "r1")["route"], "grok-4.6-xai")
        self.assertIn("grok-4.6-go", core.degraded_routes(self.sd))


if __name__ == "__main__":
    unittest.main()
