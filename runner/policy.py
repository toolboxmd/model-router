"""Versioned routing policy for the durable local runner (policy v2).

Policy is data. This module names the routes the runner dispatches as T3
threads (provider instance, model, effort), the stages (lanes) that order
them as the default role preferences, the provider signal classes, and
the policy's provenance. Chromeria's Prism settings may replace a
stage's order with the role's preferred models for that lane, and the T3
provider snapshot decides which routes are enabled and within their
usage windows (``runner/t3snapshot.py``). Behavior lives in the
controller; project workflow, authority, proof, and review stay with
AgentsMD and the target project.
"""
from __future__ import annotations

import datetime
import re
import sys
import time

POLICY_ID = "durable-runner-policy-v2"
POLICY_VERSION = "2.10.0"
# Provenance: who decided this policy and where the evidence lives.
POLICY_SOURCE = ("human decision, toolboxmd/model-router#10 (amended 2026-09-19), "
                 "#12, #26 (Go plan, 2026-09-20), "
                 "#62 (manual-only dispatch route removed, human direction, 2026-09-23), "
                 "#88 (no elapsed deadline for agent turns or jobs, human direction, 2026-09-24), "
                 "and #115/#116 (routes are T3 model selections; models, limits and role "
                 "preferences come from the T3 provider snapshot, human direction, 2026-09-25), "
                 "and #126 (an independent review turn before a job completes, human "
                 "direction, 2026-09-25)")
POLICY_EVIDENCE = "https://github.com/toolboxmd/model-router/issues/115"

# Subscription pools only. No Zen balance overflow, no pay-per-token APIs.
ALLOW_ZEN_OVERFLOW = False
ALLOW_DIRECT_PAID_API = False

# A limit error that names no reset is assumed to last one 5-hour window,
# flagged as assumed; the mark expires on its own at that time.
ASSUMED_RESET_SECS = 5 * 3600
# Where a capacity reset time came from: verbatim provider evidence, a
# window assumption, or the documented overload cooldown.
RESET_SOURCES = ("provider", "assumed", "cooldown")

# Subscription allowances a route draws on. ``meter`` names the T3
# provider driver whose usage windows measure the pool (T3 reads OpenCode
# Go as one account meter); Zen free and OpenCode's xAI provider have no
# T3 reader and rely on their own limit errors. An exhaustion error marks
# the whole pool: the allowance is shared by every model on it.
POOLS = {
    "zen-free": {"order": 0, "allowance": "free", "meter": None},
    "go": {"order": 1, "allowance": "go-included", "meter": "opencode"},
    "xai": {"order": 2, "allowance": "xai-subscription", "meter": "grok"},
    "codex": {"order": 3, "allowance": "codex-subscription", "meter": "codex"},
    "claude": {"order": 4, "allowance": "claude-subscription", "meter": "claudeAgent"},
}

# Route fields: instance (T3 provider instance id), model (the T3 model
# slug; OpenCode keeps its provider/model form), effort (Codex and Grok
# reasoningEffort, Claude effort, OpenCode variant; None = provider
# default), role, family, pool, next_pool (same model on the next pool),
# one_turn_per_job, max_concurrent (running jobs allowed on the route),
# context_window (input tokens, a routing hint for context-overflow moves).
# OpenCode turns run the ``plan`` agent for the dispatcher and ``build``
# otherwise. A Prism preference with no route here runs as a ``t3:`` route
# (see :func:`preference_route`).
ROUTES = {
    "luna/max": {"instance": "codex", "model": "gpt-5.6-luna", "effort": "max",
                 "role": "dispatch", "family": "gpt", "pool": "codex",
                 "context_window": 400_000},
    "luna-go/max": {"instance": "opencode", "model": "opencode-go/gpt-5.6-luna",
                    "effort": None, "role": "dispatch", "family": "gpt", "pool": "go",
                    "one_turn_per_job": True, "max_concurrent": 1,
                    "context_window": 400_000},
    "muse-spark-xhigh-free": {"instance": "opencode",
                              "model": "opencode/muse-spark-1.3-contributor-free",
                              "effort": "xhigh", "role": "implementation", "family": "muse",
                              "pool": "zen-free", "next_pool": "muse-spark-xhigh-go",
                              "context_window": 200_000},
    "muse-spark-xhigh-go": {"instance": "opencode",
                            "model": "opencode-go/muse-spark-1.3-contributor",
                            "effort": "xhigh", "role": "implementation", "family": "muse",
                            "pool": "go", "context_window": 200_000},
    "glm-5.3-go": {"instance": "opencode", "model": "opencode-go/glm-5.3", "effort": None,
                   "role": "implementation", "family": "glm", "pool": "go",
                   "one_turn_per_job": True, "max_concurrent": 1, "context_window": 128_000},
    "deepseek-v4-pro-go": {"instance": "opencode", "model": "opencode-go/deepseek-v4-pro",
                           "effort": None, "role": "implementation", "family": "deepseek",
                           "pool": "go", "one_turn_per_job": True, "max_concurrent": 1,
                           "context_window": 128_000},
    "kimi-k2.7-code-go": {"instance": "opencode", "model": "opencode-go/kimi-k2.7-code",
                          "effort": None, "role": "correction", "family": "kimi", "pool": "go",
                          "context_window": 256_000},
    "glm-5.3-flash-go": {"instance": "opencode", "model": "opencode-go/glm-5.3-flash",
                         "effort": None, "role": "implementation", "family": "glm", "pool": "go",
                         "context_window": 256_000},
    "qwen3.8-flash-go": {"instance": "opencode", "model": "opencode-go/qwen3.8-flash",
                         "effort": None, "role": "implementation", "family": "qwen", "pool": "go",
                         "max_concurrent": 1, "context_window": 256_000},
    "deepseek-v4.1-flash-go": {"instance": "opencode", "model": "opencode-go/deepseek-v4.1-flash",
                               "effort": None, "role": "implementation", "family": "deepseek",
                               "pool": "go", "one_turn_per_job": True, "max_concurrent": 1,
                               "context_window": 128_000},
    "hy3-go": {"instance": "opencode", "model": "opencode-go/hy3", "effort": None,
               "role": "implementation", "family": "hy", "pool": "go",
               "context_window": 200_000},
    "minimax-m3-go": {"instance": "opencode", "model": "opencode-go/minimax-m3", "effort": None,
                      "role": "implementation", "family": "minimax", "pool": "go",
                      "context_window": 200_000},
    "mimo-v2.5-go": {"instance": "opencode", "model": "opencode-go/mimo-v2.5", "effort": None,
                     "role": "implementation", "family": "mimo", "pool": "go",
                     "context_window": 200_000},
    "minimax-m2.7-go": {"instance": "opencode", "model": "opencode-go/minimax-m2.7",
                        "effort": None, "role": "implementation", "family": "minimax",
                        "pool": "go", "context_window": 256_000},
    "longcat-2.0-go": {"instance": "opencode", "model": "opencode-go/longcat-2.0",
                       "effort": None, "role": "implementation", "family": "longcat",
                       "pool": "go", "context_window": 256_000},
    "glm-5.2-go": {"instance": "opencode", "model": "opencode-go/glm-5.2", "effort": None,
                   "role": "implementation", "family": "glm", "pool": "go",
                   "context_window": 200_000},
    "kimi-k2.6-go": {"instance": "opencode", "model": "opencode-go/kimi-k2.6", "effort": None,
                     "role": "implementation", "family": "kimi", "pool": "go",
                     "context_window": 256_000},
    "glm-5.1-go": {"instance": "opencode", "model": "opencode-go/glm-5.1", "effort": None,
                   "role": "implementation", "family": "glm", "pool": "go",
                   "context_window": 200_000},
    "grok-4.6-go": {"instance": "opencode", "model": "opencode-go/grok-4.6", "effort": "medium",
                    "role": "recovery", "family": "grok", "pool": "go",
                    "next_pool": "grok-4.6-build", "one_turn_per_job": True,
                    "max_concurrent": 1, "context_window": 2_000_000},
    "grok-4.6-build": {"instance": "grok", "model": "grok-4.6", "effort": "medium",
                       "role": "recovery", "family": "grok", "pool": "xai",
                       "next_pool": "grok-4.6-xai", "context_window": 2_000_000},
    "grok-4.6-xai": {"instance": "opencode", "model": "xai/grok-4.6", "effort": "medium",
                     "role": "recovery", "family": "grok", "pool": "xai",
                     "context_window": 2_000_000},
}

_IMPLEMENTERS = ["muse-spark-xhigh-free", "muse-spark-xhigh-go", "glm-5.3-flash-go",
                 "qwen3.8-flash-go", "deepseek-v4.1-flash-go", "hy3-go", "minimax-m3-go",
                 "mimo-v2.5-go", "minimax-m2.7-go", "longcat-2.0-go", "glm-5.2-go",
                 "kimi-k2.6-go", "glm-5.1-go"]

# Stages in flow order: the default role preferences. executor: runner
# (dispatched by the runner as T3 threads) or planner (done by the planner
# itself, never dispatched).
STAGES = {
    "dispatch": {"executor": "runner", "role": "dispatcher",
                 "routes": ["luna/max", "luna-go/max"],
                 "note": "Luna max on Codex first; the same model on Go in OpenCode plan "
                         "mode when Codex cannot run it"},
    "implementation_default": {"executor": "runner", "role": "worker", "prism_lane": "medium",
                               "routes": list(_IMPLEMENTERS),
                               "note": "every new job starts on Muse free (no concurrency "
                                       "cap, parallel sessions by design), then Go"},
    "implementation_small": {"executor": "runner", "role": "worker", "prism_lane": "easy",
                             "routes": list(_IMPLEMENTERS),
                             "note": "small bounded edits"},
    "implementation_hard": {"executor": "runner", "role": "worker", "prism_lane": "hard",
                            "routes": ["muse-spark-xhigh-free", "muse-spark-xhigh-go",
                                       "glm-5.3-go", "deepseek-v4-pro-go", "grok-4.6-go",
                                       "grok-4.6-build", "grok-4.6-xai"],
                            "note": "$15 Go models get one turn per job; Grok 4.6 closes "
                                    "the lane on Go, then Grok Build, then OpenCode's xAI"},
    "critical": {"executor": "planner", "routes": [],
                 "note": "a load-bearing step is done by the planner itself before "
                         "submission; the runner never dispatches it"},
    "correction": {"executor": "runner", "role": "correction", "routes": ["kimi-k2.7-code-go"],
                   "note": "once per job; the same worker thread is tried first; a capacity "
                           "signal moves on into the job's lane"},
    "recovery": {"executor": "runner", "role": "recovery",
                 "routes": ["grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"],
                 "note": "one escalation per job; pool moves are not second escalations; "
                         "then the planner decides through a planner question"},
    "review": {"executor": "runner", "role": "reviewer",
               "routes": ["luna/max", "luna-go/max"],
               "note": "one read-only review turn on the candidate's exact head before "
                       "a job completes; Luna max on Codex, then the same model on Go"},
}
IMPLEMENTATION_LANES = ["implementation_default", "implementation_small", "implementation_hard"]
LANE_ALIASES = {"default": "implementation_default", "small": "implementation_small",
                "hard": "implementation_hard", "critical": "critical"}
DEFAULT_LANE = "implementation_default"
# Prism lanes (easy, medium, hard) for each implementation lane.
PRISM_LANE_OF = {"implementation_small": "easy", "implementation_default": "medium",
                 "implementation_hard": "hard"}

# Stage orders taken from the T3 snapshot's role preferences for the job
# being routed (set by ``t3snapshot.apply``); empty means policy defaults.
STAGE_OVERRIDES: dict[str, list[str]] = {}

# Provider signal classes. Definitions only; the controller applies them.
# ``stalled`` is detected from activity silence, never from provider text,
# and is treated like overload (a lateral move with the route degraded).
# ``context`` moves to a larger-context route in the lane.
SIGNAL_CLASSES = {
    "exhausted": {"action": "next_pool", "retries": 0,
                  "retry_reasons": ["free_tier_limit"],
                  "error_names": ["FreeUsageLimitError", "GoUsageLimitError", "insufficient_quota",
                                  "UsageLimitExceeded"]},
    "overloaded": {"action": "next_family", "retries": 2, "window_secs": 45,
                   "next_cap_secs": 20, "degraded_secs": 900,
                   "retry_reasons": ["overloaded", "rate_limit"],
                   "error_names": ["overloaded_error", "rate_limit_exceeded", "RateLimitError"],
                   "status_codes": [503, 529]},
    "stalled": {"action": "next_family", "retries": 2, "window_secs": 45,
                "degraded_secs": 900,
                "retry_reasons": [], "error_names": []},
    "context": {"action": "next_larger_context", "retries": 0,
                "retry_reasons": [],
                "error_names": ["context_length_exceeded"]},
    "hard": {"action": "implementation_failed", "retries": 0,
             "retry_reasons": ["auth", "region", "consent"],
             "error_names": ["AuthError", "RegionError", "DataPolicyError",
                             "Unauthorized", "AuthenticationError",
                             "MissingAuthentication"],
             "status_codes": [401, 403]},
}

# Vendor-shaped free-exhaustion evidence. Free exhaustion is proven only
# by the exact vendor class or by a retry action with reason
# free_tier_limit and provider opencode.
VENDOR_FREE_ERROR_CLASS = "FreeUsageLimitError"
VENDOR_FREE_RETRY_REASON = "free_tier_limit"
VENDOR_FREE_PROVIDER = "opencode"


# ---------------------------------------------------------------------------
# Snapshot routes (``t3:<instance>:<model>[@<effort>]``)
# ---------------------------------------------------------------------------

DYNAMIC_PREFIX = "t3:"
_ROLE_OF_STAGE = {"dispatch": "dispatch", "correction": "correction", "recovery": "recovery",
                  "review": "review"}


def preference_route(instance: str, model: str, effort: str | None = None) -> str:
    """The route for one Prism preference entry.

    A policy route with the same instance, model and effort keeps its
    name (and its caps and context window); any other entry runs as a
    ``t3:`` route derived from the entry alone.
    """
    for name, spec in ROUTES.items():
        if (spec["instance"], spec["model"], spec.get("effort")) == (instance, model, effort or None):
            return name
    route = f"{DYNAMIC_PREFIX}{instance}:{model}"
    return f"{route}@{effort}" if effort else route


def _pool_for(instance: str, model: str) -> str:
    if instance.startswith("opencode"):
        provider = model.split("/", 1)[0] if "/" in model else ""
        return {"opencode-go": "go", "opencode": "zen-free"}.get(provider, provider or instance)
    for prefix, pool in (("codex", "codex"), ("claude", "claude"), ("grok", "xai")):
        if instance.startswith(prefix):
            return pool
    return instance


def _dynamic_spec(route: str) -> dict:
    body = route[len(DYNAMIC_PREFIX):]
    instance, sep, rest = body.partition(":")
    model, _at, effort = rest.partition("@")
    if not sep or not instance or not model:
        raise ValueError(f"unsupported route: {route!r}")
    base = model.split("/", 1)[-1]
    family = (re.match(r"[a-z]+", base.lower()) or re.match(r".*", base)).group(0) or base
    role = "implementation"
    for stage, spec_role in _ROLE_OF_STAGE.items():
        if route in STAGE_OVERRIDES.get(stage, ()):
            role = spec_role
    return {"instance": instance, "model": model, "effort": effort or None,
            "role": role, "family": family, "pool": _pool_for(instance, model),
            "dynamic": True}


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def route_spec(route: str) -> dict:
    if isinstance(route, str) and route in ROUTES:
        return ROUTES[route]
    if isinstance(route, str) and route.startswith(DYNAMIC_PREFIX):
        return _dynamic_spec(route)
    raise ValueError(f"unsupported route: {route!r}")


def validate_route(route: str) -> dict:
    """A route's compatibility view (model, effort, role, allowance, pool)."""
    spec = route_spec(route)
    pool = spec["pool"]
    return {"model": spec["model"], "effort": spec.get("effort"), "role": spec["role"],
            "allowance": POOLS.get(pool, {}).get("allowance", pool),
            "instance": spec["instance"], "pool": pool, "operational": True}


def is_supported(route: str) -> bool:
    try:
        route_spec(route)
    except ValueError:
        return False
    return True


def route_pool(route: str) -> str:
    return route_spec(route)["pool"]


def pool_meter(pool: str) -> str | None:
    """The T3 driver whose usage windows measure ``pool``, or None."""
    return POOLS.get(pool, {}).get("meter")


def stage_routes(stage: str) -> list[str]:
    if stage not in STAGES:
        raise ValueError(f"unsupported stage: {stage!r}")
    override = STAGE_OVERRIDES.get(stage)
    return list(override) if override else list(STAGES[stage]["routes"])


def known_routes() -> list[str]:
    """Policy routes plus the snapshot routes currently in any stage."""
    out = list(ROUTES)
    for routes in STAGE_OVERRIDES.values():
        out.extend(r for r in routes if r not in out)
    return out


def implementation_routes() -> list[str]:
    seen: list[str] = []
    for lane in IMPLEMENTATION_LANES:
        for r in stage_routes(lane):
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
    return stage_routes(stage)[0]


def lane_of_route(route: str, lane: str | None = None) -> str | None:
    """The lane to reason in. A route shared by lanes is ambiguous, so
    callers pass the job's stored lane; without one, the first lane
    listing the route is used. A job already on a recovery route reasons
    in the recovery stage from any lane."""
    first = next((c for c in IMPLEMENTATION_LANES if route in stage_routes(c)), None)
    if lane:
        stage = resolve_lane(lane)
        if route in stage_routes("recovery"):
            return "recovery"
        if stage in IMPLEMENTATION_LANES and route in stage_routes(stage):
            return stage
        if first is not None:
            # An implementation route outside the stated lane is a mismatch,
            # never a silent move to another lane.
            raise ValueError(f"route {route!r} is not in lane {lane!r}")
    return first


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


def is_dispatcher_assignable(route: str) -> bool:
    """True when the dispatcher may assign this route to a worker turn:
    implementation-lane, correction, and recovery routes. Dispatch routes
    and unknown names never qualify, so a bad name can never silently
    substitute a model."""
    if not is_supported(route):
        return False
    if route in stage_routes("recovery") or route in stage_routes("correction"):
        return True
    return route in implementation_routes()


def worker_model_variant(route: str | None) -> tuple[str, str | None]:
    """(model, effort) a worker route carries."""
    spec = route_spec(route or "")
    return spec["model"], spec.get("effort")


def one_turn_per_job(route: str) -> bool:
    return bool(route_spec(route).get("one_turn_per_job"))


def route_max_concurrent(route: str) -> int | None:
    """At most this many running jobs may sit on the route; None means no
    cap. Non-int caps (including bool True) return None so a type error
    cannot masquerade as a unit cap."""
    cap = route_spec(route).get("max_concurrent")
    return cap if type(cap) is int else None


def next_pool_route(route: str) -> str | None:
    """Same model on the next pool (or through another instance), or None."""
    return route_spec(route).get("next_pool")


def next_family_route(route: str, exhausted=None, degraded=None, lane: str | None = None,
                      turns_by_route: dict | None = None) -> str | None:
    """Next route in the job's lane from a different model family that is
    neither exhausted, degraded, nor a one-turn route already used.
    Recovery is same-model across pools, so the recovery stage ignores the
    family filter and skips any rung the job already used. The correction
    route sits in no lane: it moves laterally into the job's lane."""
    stage = lane_of_route(route, lane)
    if stage is None:
        if route in stage_routes("correction"):
            return lane_fallback_route(route, lane, exhausted, degraded, turns_by_route)
        return None
    skip = set(exhausted or ()) | set(degraded or ())
    order = stage_routes(stage)
    if stage == "recovery":
        used = turns_by_route or {}
        for cand in order[order.index(route) + 1:]:
            if cand in skip or int(used.get(cand, 0)) >= 1:
                continue
            if one_turn_routes_used(cand, turns_by_route):
                continue
            return cand
        return None
    family = route_spec(route)["family"]
    for cand in order[order.index(route) + 1:]:
        if cand in skip or route_spec(cand)["family"] == family:
            continue
        if one_turn_routes_used(cand, turns_by_route):
            continue
        return cand
    return None


def lane_fallback_route(route: str, lane: str | None, exhausted=None, degraded=None,
                        turns_by_route: dict | None = None) -> str | None:
    """A lateral route in the job's lane for a route outside every lane
    (the correction route): the first eligible lane route, preferring
    another family, else any eligible one. None when nothing is left."""
    stage = resolve_lane(lane or DEFAULT_LANE)
    if stage not in IMPLEMENTATION_LANES:
        stage = DEFAULT_LANE
    skip = set(exhausted or ()) | set(degraded or ()) | {route}
    family = route_spec(route)["family"]
    candidates = [c for c in stage_routes(stage)
                  if c not in skip and not one_turn_routes_used(c, turns_by_route)]
    for cand in candidates:
        if route_spec(cand)["family"] != family:
            return cand
    return candidates[0] if candidates else None


def route_context_window(route: str) -> int:
    """Input context in tokens for a route (a routing hint, not a vendor
    guarantee). Routes without a known size raise."""
    size = route_spec(route).get("context_window")
    if type(size) is not int or size <= 0:
        raise ValueError(f"route {route!r} has no positive context_window")
    return size


def next_capacity_route(current: str | None, exhausted: set[str] | None = None,
                        lane: str | None = None) -> tuple[str | None, str | None]:
    """Next eligible route in the job's lane, skipping exhausted ones.
    Returns (route, blocker); the blocker is always None, and (None, None)
    ends the lane."""
    exhausted = exhausted or set()
    stage = lane_of_route(current, lane) if current else resolve_lane(lane or DEFAULT_LANE)
    if stage is None:
        return None, None
    order = stage_routes(stage)
    start = order.index(current) + 1 if current in order else 0
    for route in order[start:]:
        if route not in exhausted:
            return route, None
    return None, None


def next_recovery_route(current: str | None, turns_by_route: dict | None = None) -> str | None:
    """Recovery order: one escalation, same model across pools, then the
    planner (None). Rungs the job already ran are skipped."""
    order = stage_routes("recovery")
    used = turns_by_route or {}
    if current in order:
        candidates = order[order.index(current) + 1:]
    elif current is None or current in implementation_routes() \
            or current in stage_routes("correction"):
        candidates = order
    else:
        return None
    return next((c for c in candidates if int(used.get(c, 0)) == 0), None)


# ---------------------------------------------------------------------------
# Signal classification and resets
# ---------------------------------------------------------------------------

def is_free_tier_retry_status(status) -> bool:
    """Provider retry status proving free-allowance exhaustion."""
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
                names.extend(n for n in cls["error_names"] if n in body)
    return reason, names, code


def classify_signal(evidence) -> str | None:
    """``exhausted``, ``overloaded``, ``context``, ``hard``, or None for
    structured provider evidence."""
    reason, names, code = _signal_facts(evidence)
    for cls in ("exhausted", "overloaded", "context", "hard"):
        spec = SIGNAL_CLASSES[cls]
        if reason in spec.get("retry_reasons", ()):
            return cls
        if any(n in spec["error_names"] for n in names):
            return cls
        if code is not None and code in spec.get("status_codes", ()):
            return cls
    return None


def _reset_moment(value, now_ts: float) -> str | None:
    """An ISO reset from a provider value: an ISO string, or an epoch in
    milliseconds or seconds. Moments more than a day in the past are
    stale and ignored."""
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and value.strip().isdigit():
        value = int(value.strip())
    if isinstance(value, (int, float)):
        secs = float(value) / 1000.0 if value > 1e11 else float(value)
        try:
            moment = datetime.datetime.fromtimestamp(secs, datetime.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str) and value.strip():
        try:
            moment = datetime.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=datetime.timezone.utc)
    else:
        return None
    if moment.timestamp() < now_ts - 86400:
        return None
    return moment.isoformat()


def parse_provider_reset(evidence, now_ts: float | None = None) -> str | None:
    """A provider-named reset (ISO string) from structured evidence, or None.

    Reads ``next`` (OpenCode's retry status: epoch milliseconds), the
    reset keys (``resetsAt``, ``resets_at``, ``reset_at``) and a
    ``retry_after`` delay in seconds, at any nesting depth. Prose is never
    parsed, so model-authored text can never invent a reset.
    """
    now = now_ts if now_ts is not None else time.time()
    stack, seen = [evidence], set()
    while stack:
        cur = stack.pop()
        if isinstance(cur, list):
            stack.extend(v for v in cur if isinstance(v, (dict, list)))
            continue
        if not isinstance(cur, dict) or id(cur) in seen:
            continue
        seen.add(id(cur))
        for key in ("next", "resetsAt", "resets_at", "reset_at", "resetAt"):
            if cur.get(key) is not None:
                moment = _reset_moment(cur.get(key), now)
                if moment is not None:
                    return moment
        for key in ("retry_after", "retryAfter", "Retry-After"):
            raw = cur.get(key)
            if isinstance(raw, (int, float)) and not isinstance(raw, bool) and raw > 0:
                return (datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
                        + datetime.timedelta(seconds=float(raw))).isoformat()
        stack.extend(v for k, v in cur.items()
                     if k != "responseBody" and isinstance(v, (dict, list)))
    return None


def assumed_reset_at(now_ts: float | None = None) -> str:
    """The assumed reset for a limit error that names none: one 5-hour
    window from now, stored flagged as assumed."""
    now = now_ts if now_ts is not None else time.time()
    return (datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
            + datetime.timedelta(seconds=ASSUMED_RESET_SECS)).isoformat()


def next_implementation_route(current: str, error) -> str | None:
    """Same model on the next pool, only on exact exhaustion evidence."""
    validate_route(current)
    if classify_signal(error) == "exhausted" and (
            route_pool(current) != "zen-free" or classify_quota_exhaustion(error)):
        return next_pool_route(current)
    return None


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------

# ``research`` is reserved for a later recon stage: the controller blocks it.
VALID_ACTIONS = ("implementation", "planner_question", "review", "completion", "research")


# ---------------------------------------------------------------------------
# Consistency
# ---------------------------------------------------------------------------

def validate_policy() -> list[str]:
    """Problems in the policy data, empty when consistent."""
    problems: list[str] = []
    for cls in ("exhausted", "overloaded", "stalled", "context", "hard"):
        spec = SIGNAL_CLASSES.get(cls)
        if not isinstance(spec, dict) or spec.get("action") not in (
                "next_pool", "next_family", "next_larger_context", "implementation_failed"):
            problems.append(f"signal class {cls}: unknown action")
    if ROUTES.get("muse-spark-xhigh-free", {}).get("max_concurrent") is not None:
        problems.append("muse-spark-xhigh-free: must carry no concurrency cap "
                        "(parallel Muse free sessions by design)")
    for lane in IMPLEMENTATION_LANES:
        routes = STAGES[lane]["routes"]
        if not routes or routes[0] != "muse-spark-xhigh-free":
            problems.append(f"stage {lane}: first route must be muse-spark-xhigh-free "
                            "(Muse free takes every new job)")
    for name, spec in ROUTES.items():
        if not spec.get("instance") or not spec.get("model"):
            problems.append(f"{name}: needs a T3 instance and model")
        if spec["pool"] not in POOLS:
            problems.append(f"{name}: unknown pool {spec['pool']}")
        elif spec["pool"] != _pool_for(spec["instance"], spec["model"]):
            problems.append(f"{name}: model {spec['model']} on {spec['instance']} is not "
                            f"on pool {spec['pool']}")
        size = spec.get("context_window")
        if type(size) is not int or size <= 0:
            problems.append(f"{name}: context_window must be a positive int of tokens")
        cap = spec.get("max_concurrent")
        if cap is not None and (type(cap) is not int or cap < 1):
            problems.append(f"{name}: max_concurrent must be a positive int or absent")
        nxt = spec.get("next_pool")
        if nxt is not None:
            if nxt not in ROUTES:
                problems.append(f"{name}: next_pool {nxt} unknown")
            elif ROUTES[nxt]["family"] != spec["family"] or not (
                    POOLS[ROUTES[nxt]["pool"]]["order"] > POOLS[spec["pool"]]["order"]
                    or (ROUTES[nxt]["pool"] == spec["pool"]
                        and ROUTES[nxt]["instance"] != spec["instance"])):
                problems.append(f"{name}: next_pool {nxt} is not the same model on a later pool "
                                "or the same pool through another instance")
    for stage, spec in STAGES.items():
        for r in spec["routes"]:
            if r not in ROUTES:
                problems.append(f"stage {stage}: unknown route {r}")
        if spec["executor"] == "planner" and spec["routes"]:
            problems.append(f"stage {stage}: planner-executed stage lists routes")
        if stage in IMPLEMENTATION_LANES and not spec["routes"]:
            problems.append(f"stage {stage}: empty implementation lane")
    return problems


def main(argv=None) -> int:
    """``validate`` exits 1 on problems. Run as
    ``python -m runner.policy validate``."""
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv == ["validate"]:
        problems = validate_policy()
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    print("usage: policy.main(['validate'])", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
