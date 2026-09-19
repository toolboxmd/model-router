"""Policy v2: data consistency, derived skill table, lanes, and signal classes."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import controller, core, policy, store  # noqa: E402


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

    def test_scarce_models_get_one_turn(self):
        self.assertEqual({r for r in policy.ROUTES if policy.one_turn_per_job(r)},
                         {"glm-5.3-go", "kimi-k3-go", "deepseek-v4-pro-go", "grok-4.6-go"})
        self.assertTrue(policy.ROUTES["kimi-k3-go"]["one_turn_per_job"])
        self.assertTrue(policy.one_turn_routes_used("kimi-k3-go", {"kimi-k3-go": 1}))
        self.assertFalse(policy.one_turn_routes_used("kimi-k3-go", {}))
        self.assertFalse(policy.one_turn_routes_used("muse-spark-xhigh-go", {"muse-spark-xhigh-go": 3}))
        # A used one-turn route is skipped as a lateral target.
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-go", lane="hard",
                                                  turns_by_route={"kimi-k3-go": 1}), "deepseek-v4-pro-go")
        self.assertIsNone(policy.monthly_limit_usd("muse-spark-xhigh-free"))
        self.assertEqual(policy.monthly_limit_usd("muse-spark-xhigh-go"), 60)

    def test_stage_table_matches_parent_decision(self):
        s = policy.STAGES
        self.assertEqual(s["implementation_default"]["routes"],
                         ["muse-spark-xhigh-free", "muse-spark-xhigh-go", "glm-5.3-go"])
        self.assertEqual(s["implementation_small"]["routes"],
                         ["glm-5.3-flash-go", "qwen3.8-flash-go", "minimax-m3-go"])
        self.assertEqual(s["implementation_hard"]["routes"][-1], "grok-4.6-xai")
        self.assertEqual(s["correction"]["routes"], ["kimi-k2.7-code-go"])
        self.assertEqual(s["recovery"]["routes"], ["grok-4.6-go", "grok-4.6-xai"])
        self.assertEqual(s["dispatch"]["routes"], ["luna/max"])
        self.assertEqual(s["critical"]["executor"], "planner")
        self.assertEqual(s["critical"]["routes"], [])
        self.assertEqual(policy.ROUTES["luna/max"]["sandbox"], "read-only")

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
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free"), "glm-5.3-go")
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free", degraded={"glm-5.3-go"}), None)
        # The stored lane decides where a shared route moves.
        self.assertEqual(policy.next_family_route("muse-spark-xhigh-free", lane="hard"), "kimi-k3-go")
        self.assertEqual(policy.next_capacity_route("muse-spark-xhigh-go", set(), lane="hard"),
                         ("kimi-k3-go", None))
        self.assertEqual(policy.lane_of_route("muse-spark-xhigh-free", "hard"), "implementation_hard")
        self.assertEqual(policy.lane_of_route("muse-spark-xhigh-free"), "implementation_default")
        self.assertEqual(policy.validate_route("grok-4.6-xai")["allowance"], "xai-subscription")
        self.assertEqual(policy.route_allowance("grok-4.6-xai"), "xai-subscription")

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
        self.assertEqual(job["route"], "glm-5.3-flash-go")
        self.assertEqual((job["model"], job["effort"]), ("opencode-go/glm-5.3-flash", "default"))
        self.assertEqual(job["lane"], "implementation_small")
        job = core.submit(self.sd, "hard", {"g": 1}, self.ws("b"), "p", lane="hard", route="kimi-k3-go")
        self.assertEqual((job["route"], job["lane"]), ("kimi-k3-go", "implementation_hard"))
        job = core.submit(self.sd, "plain", {"g": 1}, self.ws("c"), "p")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertEqual((job["effort"], job["lane"]), ("xhigh", "implementation_default"))
        with self.assertRaises(ValueError):
            core.submit(self.sd, "mismatch", {"g": 1}, self.ws("g"), "p", lane="small", route="kimi-k3-go")

    def test_critical_lane_is_planner_executed(self):
        with self.assertRaises(ValueError) as cm:
            core.submit(self.sd, "crit", {"g": 1}, self.ws("d"), "p", lane="critical")
        self.assertIn("planner", str(cm.exception))
        with self.assertRaises(ValueError):
            core.submit(self.sd, "rev", {"g": 1}, self.ws("e"), "p", route="grok-4.6-go")

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


if __name__ == "__main__":
    unittest.main()
