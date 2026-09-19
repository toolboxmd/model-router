"""Versioned routing policy for the durable local runner (policy v2).

Policy is data. This module names the pools, the routes on them, the stages
(lanes) that order those routes, each Go model's monthly dollar limit and its
usage windows, the provider signal classes, and the policy's provenance.
Behavior lives in the controller and supervisor; project workflow,
authority, proof, and review stay with AgentsMD and the target project.

The Codex skill table is rendered from this module (``policy.main(["render-skill"])``)
and a test keeps the two equal.
"""
from __future__ import annotations

import sys

POLICY_ID = "durable-runner-policy-v2"
POLICY_VERSION = "2.0.0"
# Provenance: who decided this policy and where the evidence lives.
POLICY_SOURCE = "human decision, toolboxmd/model-router#10 (amended 2026-09-19) and #12"
POLICY_EVIDENCE = "https://github.com/toolboxmd/model-router/issues/10"

# Subscription pools only. No Zen balance overflow, no pay-per-token APIs.
ALLOW_ZEN_OVERFLOW = False
ALLOW_DIRECT_PAID_API = False

# Go usage windows as a share of each model's monthly dollar limit.
WINDOWS = {"5h": 0.20, "weekly": 0.50, "monthly": 1.00}

# Worker pools in preference order, then the two host pools.
POOLS = {
    "zen-free": {"order": 0, "provider": "opencode", "allowance": "free",
                 "credential": "subscription-issued key"},
    "go": {"order": 1, "provider": "opencode-go", "allowance": "go-included",
           "credential": "subscription-issued key", "windows": WINDOWS},
    "xai": {"order": 2, "provider": "xai", "allowance": "xai-subscription",
            "credential": "oauth"},
    "codex": {"order": 3, "provider": None, "allowance": "codex-subscription",
              "credential": "login"},
    "claude": {"order": 4, "provider": None, "allowance": "claude-subscription",
               "credential": "login"},
}
WORKER_POOL_ORDER = ["zen-free", "go", "xai"]

# Monthly dollar limits per Go model (opencode.ai/docs/go, 2026-09-19).
GO_MONTHLY_LIMIT_USD = {
    "muse-spark-1.3-contributor": 60, "glm-5.3-flash": 60, "glm-5.3": 15,
    "kimi-k3": 15, "kimi-k2.7-code": 60, "minimax-m3": 60,
    "qwen3.8-flash": 30, "deepseek-v4-pro": 15, "grok-4.6": 15,
    "gpt-5.6-luna": 15,
}
SCARCE_MONTHLY_LIMIT_USD = 15  # such models get one turn per job

# Older generations of pooled models are never routes.
EXCLUDED_MODELS = (
    "glm-5.1", "glm-5.2", "kimi-k2.6", "qwen3.6-plus", "qwen3.7-plus",
    "qwen3.7-max", "muse-spark-1.2-contributor", "hy3", "minimax-m2.7",
    "minimax-m2.5", "deepseek-v4-flash", "deepseek-v4.1-flash",
)

# Route fields: harness (codex|claude|opencode), pool, model (provider/model
# for opencode), variant (None = provider default), agent (opencode agent),
# role, family, next_pool (same model on the next pool), one_turn_per_job,
# override_only, planner_requested.
ROUTES = {
    "fable-5.1/max": {"harness": "claude", "pool": "claude", "model": "claude-fable-5-1",
                      "variant": "max", "role": "planning", "family": "claude"},
    "sonnet/medium": {"harness": "claude", "pool": "claude", "model": "claude-sonnet-5",
                      "variant": "medium", "role": "planning", "family": "claude",
                      "override_only": True,
                      "note": "explicit live-test override only, never a default"},
    "luna/max": {"harness": "codex", "pool": "codex", "model": "gpt-5.6-luna",
                 "variant": "max", "role": "dispatch", "family": "gpt",
                 "sandbox": "read-only"},
    "luna-max-review": {"harness": "codex", "pool": "codex", "model": "gpt-5.6-luna",
                        "variant": "max", "role": "review", "family": "gpt"},
    "opus-5/high-review": {"harness": "claude", "pool": "claude", "model": "claude-opus-5",
                           "variant": "high", "role": "review", "family": "claude",
                           "planner_requested": True},
    "muse-spark-xhigh-free": {"harness": "opencode", "pool": "zen-free",
                              "model": "opencode/muse-spark-1.3-contributor-free",
                              "variant": "xhigh", "agent": "build", "role": "implementation",
                              "family": "muse", "next_pool": "muse-spark-xhigh-go"},
    "muse-spark-xhigh-go": {"harness": "opencode", "pool": "go",
                            "model": "opencode-go/muse-spark-1.3-contributor",
                            "variant": "xhigh", "agent": "build", "role": "implementation",
                            "family": "muse"},
    "glm-5.3-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/glm-5.3",
                   "variant": None, "agent": "build", "role": "implementation",
                   "family": "glm", "one_turn_per_job": True},
    "glm-5.3-flash-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/glm-5.3-flash",
                         "variant": None, "agent": "build", "role": "implementation",
                         "family": "glm"},
    "qwen3.8-flash-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/qwen3.8-flash",
                         "variant": None, "agent": "build", "role": "implementation",
                         "family": "qwen"},
    "minimax-m3-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/minimax-m3",
                      "variant": None, "agent": "build", "role": "implementation",
                      "family": "minimax"},
    "kimi-k3-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/kimi-k3",
                   "variant": None, "agent": "build", "role": "implementation",
                   "family": "kimi", "one_turn_per_job": True},
    "deepseek-v4-pro-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/deepseek-v4-pro",
                           "variant": None, "agent": "build", "role": "implementation",
                           "family": "deepseek", "one_turn_per_job": True},
    "kimi-k2.7-code-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/kimi-k2.7-code",
                          "variant": None, "agent": "build", "role": "correction",
                          "family": "kimi"},
    "grok-4.6-go": {"harness": "opencode", "pool": "go", "model": "opencode-go/grok-4.6",
                    "variant": "medium", "agent": "build", "role": "recovery",
                    "family": "grok", "next_pool": "grok-4.6-xai", "one_turn_per_job": True},
    "grok-4.6-xai": {"harness": "opencode", "pool": "xai", "model": "xai/grok-4.6",
                     "variant": "medium", "agent": "build", "role": "recovery",
                     "family": "grok"},
}

# Stages in flow order. executor: host (the human-facing session or its
# native subagent), runner (dispatched by the runner), planner (done by the
# planner itself, never dispatched).
STAGES = {
    "planning": {"executor": "host", "routes": ["fable-5.1/max"], "overrides": ["sonnet/medium"],
                 "capabilities": ["session_resume"],
                 "note": "Fable 5.1 in Claude Code; Sonnet medium only as the explicit live-test override"},
    "dispatch": {"executor": "runner", "routes": ["luna/max"],
                 "capabilities": ["read_only", "session_resume", "structured_output"],
                 "note": "read-only Codex sandbox; Go Luna fallback arrives with the adapter seam (#16)"},
    "implementation_default": {"executor": "runner",
                               "routes": ["muse-spark-xhigh-free", "muse-spark-xhigh-go", "glm-5.3-go"],
                               "capabilities": ["workspace_write", "session_resume"],
                               "note": "overload moves to the next family, exhaustion to the next pool"},
    "implementation_small": {"executor": "runner",
                             "routes": ["glm-5.3-flash-go", "qwen3.8-flash-go", "minimax-m3-go"],
                             "capabilities": ["workspace_write", "session_resume"],
                             "note": "small bounded edits; $60 and $30 models"},
    "implementation_hard": {"executor": "runner",
                            "routes": ["muse-spark-xhigh-free", "muse-spark-xhigh-go", "kimi-k3-go",
                                       "deepseek-v4-pro-go", "grok-4.6-xai"],
                            "capabilities": ["workspace_write", "session_resume"],
                            "note": "$15 models get one turn per job"},
    "critical": {"executor": "planner", "routes": [], "capabilities": [],
                 "note": "a load-bearing step or prose the rest depends on is done by the planner "
                         "itself in its own host session; the runner never dispatches it"},
    "correction": {"executor": "runner", "routes": ["kimi-k2.7-code-go"],
                   "capabilities": ["workspace_write", "session_resume"],
                   "note": "once per job; the same worker session is tried first"},
    "recovery": {"executor": "runner", "routes": ["grok-4.6-go", "grok-4.6-xai"],
                 "capabilities": ["workspace_write"],
                 "note": "one escalation per job, same model across two pools, then the planner"},
    "review_ticket": {"executor": "host", "routes": ["luna-max-review"],
                      "capabilities": ["read_only"], "note": "native Codex subagent"},
    "review_final": {"executor": "host", "routes": ["opus-5/high-review"],
                     "capabilities": ["read_only"], "note": "only when the planner asks"},
}
IMPLEMENTATION_LANES = ["implementation_default", "implementation_small", "implementation_hard"]
LANE_ALIASES = {"default": "implementation_default", "small": "implementation_small",
                "hard": "implementation_hard", "critical": "critical"}
DEFAULT_LANE = "implementation_default"

# Provider signal classes. Definitions only; the controller applies them.
SIGNAL_CLASSES = {
    "exhausted": {"action": "next_pool", "retries": 0,
                  "retry_reasons": ["free_tier_limit"],
                  "error_names": ["FreeUsageLimitError", "GoUsageLimitError", "insufficient_quota"]},
    "overloaded": {"action": "next_family", "retries": 2, "window_secs": 45,
                   "next_cap_secs": 20, "degraded_secs": 900,
                   "retry_reasons": ["overloaded", "rate_limit", "account_rate_limit"],
                   "error_names": ["overloaded_error", "rate_limit_exceeded", "RateLimitError"],
                   "status_codes": [503, 529]},
    "hard": {"action": "block", "retries": 0,
             "retry_reasons": ["auth", "region", "consent"],
             "error_names": ["context_length_exceeded", "AuthError", "RegionError", "DataPolicyError"]},
}

# Vendor-shaped free-exhaustion evidence (OpenCode 1.18.31). Free exhaustion
# is proven only by the exact vendor class or by a session retry action with
# reason free_tier_limit and provider opencode. Nothing else moves a task
# off the free route.
VENDOR_FREE_ERROR_CLASS = "FreeUsageLimitError"
VENDOR_FREE_RETRY_REASON = "free_tier_limit"
VENDOR_FREE_PROVIDER = "opencode"


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def route_spec(route: str) -> dict:
    if route not in ROUTES:
        raise ValueError(f"unsupported route: {route!r}")
    return ROUTES[route]


def validate_route(route: str) -> dict:
    """The derived compatibility view of a route (model, effort, role,
    allowance, harness, pool, operational)."""
    route_spec(route)
    return SUPPORTED_ROUTES[route]


def is_supported(route: str) -> bool:
    return route in ROUTES


def is_operational(route: str) -> bool:
    """Every policy route has an adapter; unknown routes are not operational."""
    return route in ROUTES


def route_blocker(route: str) -> str | None:
    return None if route in ROUTES else f"unsupported route: {route!r}"


def route_allowance(route: str) -> str:
    return POOLS[route_spec(route)["pool"]]["allowance"]


def route_pool(route: str) -> str:
    return route_spec(route)["pool"]


def stage_routes(stage: str) -> list[str]:
    if stage not in STAGES:
        raise ValueError(f"unsupported stage: {stage!r}")
    return list(STAGES[stage]["routes"])


def implementation_routes() -> list[str]:
    seen: list[str] = []
    for lane in IMPLEMENTATION_LANES:
        for r in STAGES[lane]["routes"]:
            if r not in seen:
                seen.append(r)
    return seen


def resolve_lane(lane: str) -> str:
    stage = LANE_ALIASES.get(lane, lane)
    if stage not in STAGES:
        raise ValueError(f"unsupported lane: {lane!r}")
    return stage


def lane_default_route(lane: str) -> str:
    """First route of an implementation lane. The critical lane has none:
    the planner does that work itself."""
    stage = resolve_lane(lane)
    if STAGES[stage]["executor"] == "planner":
        raise ValueError(
            f"lane {lane!r} is planner-executed: do the load-bearing step in the planner "
            "session, then submit the remaining work")
    if stage not in IMPLEMENTATION_LANES:
        raise ValueError(f"lane {lane!r} is not an implementation lane")
    return STAGES[stage]["routes"][0]


def lane_of_route(route: str, lane: str | None = None) -> str | None:
    """The lane to reason in. A route shared by lanes (Muse sits in the
    default and hard lanes) is ambiguous, so callers pass the job's stored
    lane; without one, the first lane listing the route is used."""
    if lane:
        stage = resolve_lane(lane)
        if stage in IMPLEMENTATION_LANES and route in STAGES[stage]["routes"]:
            return stage
    for cand in IMPLEMENTATION_LANES:
        if route in STAGES[cand]["routes"]:
            return cand
    return None


def one_turn_routes_used(route: str, turns_by_route: dict | None = None) -> bool:
    """True when ``route`` is a one-turn-per-job route that already ran once."""
    if not route_spec(route).get("one_turn_per_job"):
        return False
    return int((turns_by_route or {}).get(route, 0)) >= 1


def validate_implementation_route(route: str) -> dict:
    """A job route must belong to an implementation lane."""
    info = route_spec(route)
    if route not in implementation_routes():
        raise ValueError(f"route {route!r} is a {info.get('role')} route, not an implementation "
                         f"route; supported: {', '.join(implementation_routes())}")
    return info


def opencode_route_params(route: str | None) -> tuple[str, str | None, str]:
    """(model, variant, agent) the OpenCode adapter sends for a route.
    Unknown routes and routes on other harnesses raise: the runner never
    substitutes a model silently."""
    spec = route_spec(route or "")
    if spec.get("harness") != "opencode":
        raise ValueError(f"route {route!r} runs on {spec.get('harness')}, not on the OpenCode adapter")
    return spec["model"], spec.get("variant"), spec.get("agent") or "build"


def monthly_limit_usd(route: str) -> int | None:
    spec = route_spec(route)
    if spec["pool"] != "go":
        return None
    return GO_MONTHLY_LIMIT_USD.get(spec["model"].split("/", 1)[1])


def one_turn_per_job(route: str) -> bool:
    """The explicit route field; ``validate_policy`` keeps it equal to the
    $15 limit rule so the data cannot drift from the table."""
    return bool(route_spec(route).get("one_turn_per_job"))


def next_pool_route(route: str) -> str | None:
    """Same model on the next pool, or None."""
    return route_spec(route).get("next_pool")


def next_family_route(route: str, exhausted=None, degraded=None, lane: str | None = None,
                      turns_by_route: dict | None = None) -> str | None:
    """Next route in the job's lane from a different model family that is
    neither exhausted, degraded, nor a one-turn route already used."""
    stage = lane_of_route(route, lane)
    if stage is None:
        return None
    skip = set(exhausted or ()) | set(degraded or ())
    family = route_spec(route)["family"]
    order = STAGES[stage]["routes"]
    for cand in order[order.index(route) + 1:]:
        if cand in skip or ROUTES[cand]["family"] == family:
            continue
        if one_turn_routes_used(cand, turns_by_route):
            continue
        return cand
    return None


def next_capacity_route(current: str | None, exhausted: set[str] | None = None,
                        lane: str | None = None) -> tuple[str | None, str | None]:
    """Next eligible route in the job's lane, skipping exhausted ones.
    Returns (route, blocker); blocker is always None because every policy
    route has an adapter, and (None, None) ends the lane."""
    exhausted = exhausted or set()
    stage = lane_of_route(current, lane) if current else resolve_lane(lane or DEFAULT_LANE)
    if stage is None:
        return None, None
    order = STAGES[stage]["routes"]
    start = order.index(current) + 1 if current in order else 0
    for route in order[start:]:
        if route in exhausted:
            continue
        return route, None
    return None, None


def next_recovery_route(current: str | None) -> str | None:
    """Recovery order: one escalation to Grok 4.6, same model across the Go
    and xAI pools, then the planner (None)."""
    order = STAGES["recovery"]["routes"]
    if current is None:
        return order[0]
    if current in order:
        idx = order.index(current)
        return order[idx + 1] if idx + 1 < len(order) else None
    if current in implementation_routes() or current in ("luna/max", "sonnet/medium"):
        return order[0]
    return None


# ---------------------------------------------------------------------------
# Signal classification
# ---------------------------------------------------------------------------

def is_free_tier_retry_status(status) -> bool:
    """OpenCode session status proving free-allowance exhaustion."""
    if not isinstance(status, dict) or status.get("type") != "retry":
        return False
    action = status.get("action")
    return (isinstance(action, dict)
            and action.get("reason") == VENDOR_FREE_RETRY_REASON
            and action.get("provider") == VENDOR_FREE_PROVIDER)


def is_free_usage_api_error(error) -> bool:
    """Provider ``APIError`` whose response body names the free limit."""
    if not isinstance(error, dict) or error.get("name") != "APIError":
        return False
    data = error.get("data")
    body = data.get("responseBody") if isinstance(data, dict) else None
    return isinstance(body, str) and VENDOR_FREE_ERROR_CLASS in body


def classify_quota_exhaustion(evidence) -> bool:
    """True only for exact provider-shaped free-exhaustion evidence."""
    if not isinstance(evidence, dict):
        return False
    if is_free_tier_retry_status(evidence) or is_free_usage_api_error(evidence):
        return True
    return evidence.get("type") == "error" and is_free_usage_api_error(evidence.get("error"))


classify_provider_free_exhaustion = classify_quota_exhaustion


def _signal_facts(evidence) -> tuple[str | None, list[str], int | None]:
    """(retry reason, error class names, status code) from provider evidence.
    Model-authored text is never inspected: only structured fields count."""
    if not isinstance(evidence, dict):
        return None, [], None
    reason = None
    action = evidence.get("action")
    if evidence.get("type") == "retry" and isinstance(action, dict):
        reason = action.get("reason")
    names: list[str] = []
    code = None
    err = evidence.get("error") if evidence.get("type") == "error" else evidence
    if isinstance(err, dict):
        for key in ("name", "type", "code", "class"):
            val = err.get(key)
            if isinstance(val, str):
                names.append(val)
        data = err.get("data") if isinstance(err.get("data"), dict) else {}
        if isinstance(data.get("statusCode"), int):
            code = data["statusCode"]
        body = data.get("responseBody")
        if isinstance(body, str):
            for cls in SIGNAL_CLASSES.values():
                for name in cls["error_names"]:
                    if name in body:
                        names.append(name)
    return reason, names, code


def classify_signal(evidence) -> str | None:
    """``exhausted``, ``overloaded``, ``hard``, or None for provider evidence."""
    reason, names, code = _signal_facts(evidence)
    for cls in ("exhausted", "overloaded", "hard"):
        spec = SIGNAL_CLASSES[cls]
        if reason in spec.get("retry_reasons", ()):
            return cls
        if any(n in spec["error_names"] for n in names):
            return cls
        if code is not None and code in spec.get("status_codes", ()):
            return cls
    return None


def next_implementation_route(current: str, error) -> str | None:
    """Same model on the next pool, only on exact exhaustion evidence."""
    validate_route(current)
    if classify_signal(error) == "exhausted" and (
            current != "muse-spark-xhigh-free" or classify_quota_exhaustion(error)):
        return next_pool_route(current)
    return None


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------

# ``research`` is reserved for a later recon stage: the controller blocks it.
VALID_ACTIONS = ("implementation", "planner_question", "review", "completion", "research")


def make_action(action: str, route: str, payload: dict | None = None,
                artifact: str | None = None, evidence: str | None = None) -> dict:
    if action not in VALID_ACTIONS:
        raise ValueError(f"unsupported action: {action!r}")
    validate_route(route)
    return {"policy": POLICY_ID, "action": action, "route": route,
            "payload": payload or {}, "artifact": artifact, "evidence": evidence}


def make_result(action: str, ok: bool, output: str | None = None,
                error: dict | str | None = None, artifact: str | None = None) -> dict:
    if action not in VALID_ACTIONS:
        raise ValueError(f"unsupported action: {action!r}")
    return {"policy": POLICY_ID, "action": action, "ok": bool(ok),
            "output": output, "error": error, "artifact": artifact}


# ---------------------------------------------------------------------------
# Derived compatibility views
# ---------------------------------------------------------------------------

def _derived_routes() -> dict:
    out = {}
    for name, spec in ROUTES.items():
        out[name] = {"model": spec["model"], "effort": spec.get("variant"),
                     "role": spec["role"], "allowance": POOLS[spec["pool"]]["allowance"],
                     "harness": spec["harness"], "pool": spec["pool"], "operational": True}
    return out


SUPPORTED_ROUTES = _derived_routes()
OPERATIONAL_ROUTES = set(ROUTES)
IMPLEMENTATION_ORDER = implementation_routes()
RECOVERY_ORDER = list(STAGES["recovery"]["routes"])


# ---------------------------------------------------------------------------
# Consistency and rendering
# ---------------------------------------------------------------------------

def validate_policy() -> list[str]:
    """Problems in the policy data, empty when consistent."""
    problems: list[str] = []
    for name, spec in ROUTES.items():
        if spec["harness"] not in ("codex", "claude", "opencode"):
            problems.append(f"{name}: unknown harness {spec['harness']}")
        if spec["pool"] not in POOLS:
            problems.append(f"{name}: unknown pool {spec['pool']}")
        if spec["harness"] == "opencode":
            provider, sep, model_id = spec["model"].partition("/")
            if not sep or provider != POOLS[spec["pool"]]["provider"]:
                problems.append(f"{name}: model {spec['model']} is not on pool {spec['pool']}")
            if spec["pool"] == "zen-free" and not model_id.endswith("-free"):
                problems.append(f"{name}: zen-free route must use a -free model")
            if spec["pool"] == "go" and model_id not in GO_MONTHLY_LIMIT_USD:
                problems.append(f"{name}: no monthly limit recorded for {model_id}")
            if model_id.removesuffix("-free") in EXCLUDED_MODELS:
                problems.append(f"{name}: excluded older generation {model_id}")
            if spec["pool"] == "go":
                scarce = GO_MONTHLY_LIMIT_USD.get(model_id, 0) <= SCARCE_MONTHLY_LIMIT_USD
                if scarce != bool(spec.get("one_turn_per_job")):
                    problems.append(f"{name}: one_turn_per_job must be {scarce} for a "
                                    f"${GO_MONTHLY_LIMIT_USD.get(model_id)} model")
        nxt = spec.get("next_pool")
        if nxt is not None:
            if nxt not in ROUTES:
                problems.append(f"{name}: next_pool {nxt} unknown")
            elif ROUTES[nxt]["family"] != spec["family"] or \
                    POOLS[ROUTES[nxt]["pool"]]["order"] <= POOLS[spec["pool"]]["order"]:
                problems.append(f"{name}: next_pool {nxt} is not the same model on a later pool")
    for stage, spec in STAGES.items():
        for r in spec["routes"]:
            if r not in ROUTES:
                problems.append(f"stage {stage}: unknown route {r}")
        if spec["executor"] == "planner" and spec["routes"]:
            problems.append(f"stage {stage}: planner-executed stage lists routes")
        if stage in IMPLEMENTATION_LANES and not spec["routes"]:
            problems.append(f"stage {stage}: empty implementation lane")
    return problems


def render_skill_table() -> str:
    """The Codex skill reference, generated from this policy."""
    lines = [
        "# Codex routing policy",
        "",
        f"Generated from `runner/policy.py` (`{POLICY_ID}` {POLICY_VERSION}); do not edit by hand.",
        f"Source: {POLICY_SOURCE}. Evidence: {POLICY_EVIDENCE}.",
        "",
        "| Stage | Routes in order | Notes |",
        "| --- | --- | --- |",
    ]
    for stage, spec in STAGES.items():
        routes = ", ".join(f"`{r}`" for r in spec["routes"]) or "planner itself"
        if spec.get("overrides"):
            routes += " (override: " + ", ".join(f"`{r}`" for r in spec["overrides"]) + ")"
        lines.append(f"| {stage} | {routes} | {spec['executor']}; {spec['note']} |")
    lines += [
        "",
        "Route models, in policy order: "
        + "; ".join(f"`{r}` = `{s['model']}`" + (f" {s['variant']}" if s.get("variant") else "")
                    for r, s in ROUTES.items())
        + ".",
        "",
        "Rules:",
        "",
        "- Implementation, correction, and recovery go through the runner "
        "(`python -m runner submit`), which owns the routes above, capacity, and "
        "fallback. Use a native Codex subagent only for `review_ticket`.",
        "- Classify by consequences, not file type. A step or prose that the rest "
        "of the work depends on is `critical`: do it in the planner session yourself, "
        "then submit the remainder. Changes to agent instructions, security rules, "
        "and specifications are at least `implementation_hard`.",
        f"- Pools in order: {', '.join(WORKER_POOL_ORDER)}. Subscription logins only; "
        "no pay-per-token API keys and no paid balance overflow.",
        "- Exhaustion (" + ", ".join(SIGNAL_CLASSES["exhausted"]["retry_reasons"]
                                      + SIGNAL_CLASSES["exhausted"]["error_names"])
        + ") moves the same model to the next pool with no retries. Overload ("
        + ", ".join(SIGNAL_CLASSES["overloaded"]["retry_reasons"]) + ", HTTP "
        + ", ".join(str(c) for c in SIGNAL_CLASSES["overloaded"]["status_codes"])
        + f") allows {SIGNAL_CLASSES['overloaded']['retries']} retries inside "
        f"{SIGNAL_CLASSES['overloaded']['window_secs']} seconds, then the next family. "
        "Hard errors block with a reason.",
        f"- Go models with a ${SCARCE_MONTHLY_LIMIT_USD} monthly limit get one turn per job. "
        f"Windows: 5-hour {int(WINDOWS['5h'] * 100)} percent, weekly {int(WINDOWS['weekly'] * 100)} "
        "percent, monthly 100 percent of the model's limit.",
        "- One escalation per job; afterwards evidence returns to the planner. No "
        "duplicate attempts, no retry loops. Never substitute a route silently; if "
        "the selected route is unavailable, stop that dispatch with the reason.",
        "- Record the policy version, the requested and observed route, and any "
        "override or escalation in the existing handoff. Instructions guide "
        "behavior; the runner enforces it.",
        "",
    ]
    return "\n".join(lines)


def main(argv=None) -> int:
    """``render-skill`` prints the skill reference; ``validate`` exits 1 on
    problems. Run as ``python -c "from runner import policy; policy.main([...])"``."""
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv == ["render-skill"]:
        sys.stdout.write(render_skill_table())
        return 0
    if argv == ["validate"]:
        problems = validate_policy()
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    print("usage: policy.main(['render-skill'] | ['validate'])", file=sys.stderr)
    return 2
