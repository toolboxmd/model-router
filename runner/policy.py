"""Versioned model policy for the durable local runner.

Thin coordination only. Project workflow, authority, proof, and review
ownership stay with AgentsMD and the target project. This module only
names allowed routes, the free-first then Go-included order, recovery
order, and the explicit-evidence rule for quota exhaustion.
"""

POLICY_ID = "durable-runner-policy-v1"
POLICY_VERSION = "1.0.0"

# No Zen balance overflow. No direct paid APIs.
ALLOW_ZEN_OVERFLOW = False
ALLOW_DIRECT_PAID_API = False

# Canonical supported routes. Keys are the only accepted route strings.
SUPPORTED_ROUTES = {
    # Dispatch through persistent Luna dispatcher.
    "luna/max": {
        "model": "gpt-5.6-luna",
        "effort": "max",
        "role": "dispatch",
    },
    # Implementation: Muse Spark 1.3 Contributor xhigh, free allowance first.
    "muse-spark-xhigh-free": {
        "model": "muse-spark-1.3-contributor",
        "effort": "xhigh",
        "role": "implementation",
        "allowance": "free",
    },
    # Same model through Go included allowance, only after confirmed free exhaustion.
    "muse-spark-xhigh-go": {
        "model": "muse-spark-1.3-contributor",
        "effort": "xhigh",
        "role": "implementation",
        "allowance": "go-included",
    },
    # Independent ticket review.
    "luna-max-review": {
        "model": "gpt-5.6-luna",
        "effort": "max",
        "role": "review",
    },
    # Final combined review.
    "opus-5/high-review": {
        "model": "claude-opus-5",
        "effort": "high",
        "role": "review",
    },
    "opus-5/high": {
        "model": "claude-opus-5",
        "effort": "high",
        "role": "recovery",
    },
    # Planning.
    "fable-5.1/max": {
        "model": "claude-fable-5-1",
        "effort": "max",
        "role": "planning",
    },
    "astra/max": {
        "model": "gpt-6-astra",
        "effort": "max",
        "role": "planning",
    },
    # Recovery sequence after bounded implementation failure.
    "grok-4.6/medium": {
        "model": "grok-4.6",
        "effort": "medium",
        "role": "recovery",
        "harness": "grok-build",
    },
    "astra/medium": {
        "model": "gpt-6-astra",
        "effort": "medium",
        "role": "recovery",
    },
    # Bounded live integration test override only. Exact slug is
    # claude-sonnet-5 with medium; this is not a production default.
    "sonnet/medium": {
        "model": "claude-sonnet-5",
        "effort": "medium",
        "role": "live-test-override",
        "operational": False,
        "blocker": "sonnet/medium is the explicit live-test override only; deterministic tests never call it",
    },
    # Luna coordination fallback. Recorded policy; no named adapter is
    # live-exercised in this runner.
    "terra/max": {
        "model": "gpt-5.6-terra",
        "effort": "max",
        "role": "dispatch-fallback",
        "operational": False,
        "blocker": "Terra fallback is recorded after demonstrated Luna coordination failure; no named adapter is live-exercised",
    },
    # Go included capacity after trusted free exhaustion + Go Muse.
    "go-deepseek-v4.1-flash": {
        "model": "opencode-go/deepseek-v4.1-flash",
        "role": "implementation",
        "allowance": "go-included",
        "operational": False,
        "blocker": "Go DeepSeek V4.1 Flash has no live-exercised adapter in this runner",
    },
    "go-glm-5.3-flash": {
        "model": "opencode-go/glm-5.3-flash",
        "role": "implementation",
        "allowance": "go-included",
        "operational": False,
        "blocker": "Go GLM 5.3 Flash has no live-exercised adapter in this runner",
    },
    "go-minimax-m3": {
        "model": "opencode-go/minimax-m3",
        "role": "implementation",
        "allowance": "go-included",
        "operational": False,
        "blocker": "Go MiniMax M3 has no live-exercised adapter in this runner",
    },
    "mimo-2.5": {
        "model": "mimo-2.5",
        "role": "implementation",
        "operational": False,
        "blocker": "MiMo 2.5 is supplemental capacity with no live-exercised adapter",
    },
    "longcat-2.0": {
        "model": "longcat-2.0",
        "role": "implementation",
        "operational": False,
        "blocker": "LongCat 2.0 is supplemental capacity with no live-exercised adapter",
    },
    # After the initial worker plus one bounded correction.
    "kimi-k2.7-code": {
        "model": "kimi-k2.7-code",
        "role": "implementation-correction",
        "operational": False,
        "blocker": "Kimi K2.7 Code is eligible only after the initial worker plus one bounded correction; no named adapter is live-exercised",
    },
}

# Agreed orders.
IMPLEMENTATION_ORDER = ["muse-spark-xhigh-free", "muse-spark-xhigh-go"]
GO_CAPACITY_ORDER = [
    "muse-spark-xhigh-go",
    "go-deepseek-v4.1-flash",
    "go-glm-5.3-flash",
    "go-minimax-m3",
]
SUPPLEMENTAL_CAPACITY_ORDER = ["mimo-2.5", "longcat-2.0"]
IMPLEMENTATION_CAPACITY_ORDER = (
    ["muse-spark-xhigh-free"] + GO_CAPACITY_ORDER + SUPPLEMENTAL_CAPACITY_ORDER
)
RECOVERY_ORDER = ["grok-4.6/medium", "astra/medium", "opus-5/high"]
# Adapters actually driven on the public CLI path (fake or live).
# Recovery/review names are policy-recorded; they are not live-exercised
# by this runner and must not be claimed operational.
OPERATIONAL_ROUTES = {
    "luna/max",
    "muse-spark-xhigh-free",
    "muse-spark-xhigh-go",
    "fable-5.1/max",
}


# Vendor-shaped evidence (OpenCode 1.18.31, see quota-status-evidence.md).
# Free exhaustion is proven only by the exact vendor class
# "FreeUsageLimitError" (including decoded responseBody JSON) or by a
# session retry action with reason "free_tier_limit" and provider "opencode".
# Generic RateLimitError/rate_limit, bare 429, timeout, DataPolicyError,
# RegionError, AuthError, and consent/permission errors never prove it.
VENDOR_FREE_ERROR_CLASS = "FreeUsageLimitError"
VENDOR_FREE_RETRY_REASON = "free_tier_limit"
VENDOR_FREE_PROVIDER = "opencode"


def is_supported(route: str) -> bool:
    return route in SUPPORTED_ROUTES


def validate_implementation_route(route: str) -> dict:
    """A job route must be an implementation route with a runner adapter."""
    info = validate_route(route)
    if route not in IMPLEMENTATION_ORDER:
        raise ValueError(f"route {route!r} is a {info.get('role')} route, not an implementation "
                         f"route; supported: {', '.join(IMPLEMENTATION_ORDER)}")
    return info


def validate_route(route: str) -> dict:
    if route not in SUPPORTED_ROUTES:
        raise ValueError(f"unsupported route: {route!r}")
    return SUPPORTED_ROUTES[route]


def is_operational(route: str) -> bool:
    spec = SUPPORTED_ROUTES.get(route) or {}
    if spec.get("operational") is False:
        return False
    return route in OPERATIONAL_ROUTES


def route_blocker(route: str) -> str | None:
    """Precise blocker for an unavailable eligible route, else None."""
    if route not in SUPPORTED_ROUTES:
        return f"unsupported route: {route!r}"
    spec = SUPPORTED_ROUTES[route]
    if spec.get("operational") is False:
        return str(spec.get("blocker") or f"route {route} is not operational")
    if route not in OPERATIONAL_ROUTES:
        role = spec.get("role") or "route"
        return (
            f"{route} ({role}) is recorded policy with no live-exercised adapter"
        )
    return None


def next_capacity_route(current: str | None, exhausted: set[str] | None = None) -> tuple[str | None, str | None]:
    """Next eligible implementation route, skipping known-exhausted ones.

    Returns (route, blocker). blocker is set when the next eligible
    route exists in policy but is not operational. Never waits on an
    exhausted route when another authorized route remains.
    """
    exhausted = exhausted or set()
    order = IMPLEMENTATION_CAPACITY_ORDER
    start = 0
    if current in order:
        start = order.index(current) + 1
    for route in order[start:]:
        if route in exhausted:
            continue
        blocker = route_blocker(route)
        if blocker:
            return route, blocker
        return route, None
    return None, None


def is_free_tier_retry_status(status) -> bool:
    """OpenCode session status proving free-allowance exhaustion."""
    if not isinstance(status, dict) or status.get("type") != "retry":
        return False
    action = status.get("action")
    return (isinstance(action, dict)
            and action.get("reason") == VENDOR_FREE_RETRY_REASON
            and action.get("provider") == VENDOR_FREE_PROVIDER)


def is_free_usage_api_error(error) -> bool:
    """Provider ``APIError`` whose response body names the free limit.

    This is OpenCode's own rule (``responseBody`` includes
    ``FreeUsageLimitError``). The body comes from the provider HTTP
    response, not from model output.
    """
    if not isinstance(error, dict) or error.get("name") != "APIError":
        return False
    data = error.get("data")
    body = data.get("responseBody") if isinstance(data, dict) else None
    return isinstance(body, str) and VENDOR_FREE_ERROR_CLASS in body


def classify_quota_exhaustion(evidence) -> bool:
    """True only for exact provider-shaped free-exhaustion evidence.

    Accepted: a retry status with reason ``free_tier_limit`` from provider
    ``opencode``; an ``APIError`` whose ``responseBody`` names
    ``FreeUsageLimitError``; or an ``opencode run`` error event carrying
    that ``APIError``. Strings, nested free text, generic 429,
    ``RateLimitError``, ``account_rate_limit``, ``GoUsageLimitError``,
    timeouts, consent, region, and auth errors never count.
    """
    if not isinstance(evidence, dict):
        return False
    if is_free_tier_retry_status(evidence) or is_free_usage_api_error(evidence):
        return True
    return evidence.get("type") == "error" and is_free_usage_api_error(evidence.get("error"))


classify_provider_free_exhaustion = classify_quota_exhaustion


def next_implementation_route(current: str, error) -> str | None:
    """Free-first then Go-included only after confirmed exhaustion."""
    validate_route(current)
    if current != "muse-spark-xhigh-free":
        return None
    if classify_quota_exhaustion(error):
        return "muse-spark-xhigh-go"
    return None


def next_recovery_route(current: str | None) -> str | None:
    """Bounded recovery order. None means no further route."""
    if current is None:
        return RECOVERY_ORDER[0]
    if current not in RECOVERY_ORDER:
        # Implementation routes enter recovery at the head.
        if current in IMPLEMENTATION_ORDER or current in ("luna/max", "sonnet/medium"):
            return RECOVERY_ORDER[0]
        return None
    idx = RECOVERY_ORDER.index(current)
    if idx + 1 < len(RECOVERY_ORDER):
        return RECOVERY_ORDER[idx + 1]
    return None


# Structured action/result envelope. Represents coordination only;
# it does not duplicate workflow, proof, or authority.
VALID_ACTIONS = ("implementation", "planner_question", "review", "completion")


def make_action(action: str, route: str, payload: dict | None = None,
                artifact: str | None = None, evidence: str | None = None) -> dict:
    if action not in VALID_ACTIONS:
        raise ValueError(f"unsupported action: {action!r}")
    validate_route(route)
    return {
        "policy": POLICY_ID,
        "action": action,
        "route": route,
        "payload": payload or {},
        "artifact": artifact,
        "evidence": evidence,
    }


def make_result(action: str, ok: bool, output: str | None = None,
                error: dict | str | None = None,
                artifact: str | None = None) -> dict:
    if action not in VALID_ACTIONS:
        raise ValueError(f"unsupported action: {action!r}")
    return {
        "policy": POLICY_ID,
        "action": action,
        "ok": bool(ok),
        "output": output,
        "error": error,
        "artifact": artifact,
    }
