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
        "model": "opus-5",
        "effort": "high",
        "role": "review",
    },
    "opus-5/high": {
        "model": "opus-5",
        "effort": "high",
        "role": "recovery",
    },
    # Planning.
    "fable-5.1/max": {
        "model": "fable-5.1",
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

# Only this explicit code proves free-quota exhaustion and permits the
# free -> Go transition. Everything else must not switch quota.
EXPLICIT_FREE_EXHAUSTED_CODE = "FREE_ALLOWANCE_EXHAUSTED"

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


def _decoded_response_bodies(obj):
    """Yield decoded JSON bodies from responseBody-shaped keys."""
    import json as _json

    if isinstance(obj, dict):
        for key in ("responseBody", "response_body", "responsebody"):
            if key in obj and isinstance(obj[key], str):
                raw = obj[key]
                try:
                    yield _json.loads(raw)
                except ValueError:
                    continue
        for v in obj.values():
            yield from _decoded_response_bodies(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _decoded_response_bodies(v)


def _contains_exact_free_error(obj, _depth=0) -> bool:
    """True iff the exact vendor class FreeUsageLimitError appears."""
    if _depth > 12:
        return False
    if isinstance(obj, str):
        return VENDOR_FREE_ERROR_CLASS in obj
    if isinstance(obj, dict):
        for v in obj.values():
            if _contains_exact_free_error(v, _depth + 1):
                return True
        for decoded in _decoded_response_bodies(obj):
            if _contains_exact_free_error(decoded, _depth + 1):
                return True
        return False
    if isinstance(obj, (list, tuple)):
        return any(_contains_exact_free_error(v, _depth + 1) for v in obj)
    return False


def _reason_and_provider_in_dict(d: dict) -> tuple[str | None, str | None]:
    reason = d.get("reason")
    provider = d.get("provider")
    action = d.get("action")
    if isinstance(action, dict):
        if reason is None:
            reason = action.get("reason")
        if provider is None:
            provider = action.get("provider")
    if not isinstance(reason, str) and isinstance(d.get("retry"), dict):
        reason = d["retry"].get("reason")
    return (reason if isinstance(reason, str) else None,
            provider if isinstance(provider, str) else None)


def _collect_retry_markers(obj, _depth=0, _reasons=None, _providers=None):
    if _reasons is None:
        _reasons = set()
        _providers = set()
    if _depth > 12:
        return _reasons, _providers
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if lk == "reason" and isinstance(v, str):
                _reasons.add(v)
            if lk in ("provider", "vendor") and isinstance(v, str):
                _providers.add(v)
            # Nested action envelope: {"action": {"reason": ...}}.
            if lk == "action" and isinstance(v, dict):
                r = v.get("reason")
                if isinstance(r, str):
                    _reasons.add(r)
                p = v.get("provider") or v.get("vendor")
                if isinstance(p, str):
                    _providers.add(p)
            _collect_retry_markers(v, _depth + 1, _reasons, _providers)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _collect_retry_markers(v, _depth + 1, _reasons, _providers)
    return _reasons, _providers


def is_free_tier_retry_status(obj, _depth=0) -> bool:
    """True iff a session retry envelope names free_tier_limit + opencode."""
    if _depth > 12:
        return False
    if isinstance(obj, dict):
        reason, provider = _reason_and_provider_in_dict(obj)
        if reason == VENDOR_FREE_RETRY_REASON and provider == VENDOR_FREE_PROVIDER:
            return True
        if any(is_free_tier_retry_status(v, _depth + 1) for v in obj.values()):
            return True
        # Envelope-level fallback: reason and provider may live at
        # different nesting depths in the same provider envelope
        # (e.g. {"status": {"action": {"reason": ...}}, "provider": ...}).
        # Only the top call applies this so nested accidental matches
        # do not widen the rule.
        if _depth == 0:
            reasons, providers = _collect_retry_markers(obj)
            if VENDOR_FREE_RETRY_REASON in reasons and VENDOR_FREE_PROVIDER in providers:
                return True
        return False
    if isinstance(obj, (list, tuple)):
        if any(is_free_tier_retry_status(v, _depth + 1) for v in obj):
            return True
        if _depth == 0:
            reasons, providers = _collect_retry_markers(obj)
            if VENDOR_FREE_RETRY_REASON in reasons and VENDOR_FREE_PROVIDER in providers:
                return True
        return False
    return False


def classify_provider_free_exhaustion(error) -> bool:
    """Provider-shaped free-exhaustion check (no text-only 429).

    True only for the exact vendor class FreeUsageLimitError (including
    decoded responseBody JSON) or a retry action with reason
    free_tier_limit and provider opencode. Generic RateLimitError,
    rate_limit, 429, timeout, DataPolicyError, RegionError, AuthError,
    and consent/permission envelopes return False.
    """
    if error is None:
        return False
    if isinstance(error, (bytes, bytearray)):
        try:
            error = error.decode("utf-8", errors="replace")
        except Exception:
            return False
    if _contains_exact_free_error(error):
        return True
    if is_free_tier_retry_status(error):
        return True
    return False


def classify_quota_exhaustion(error) -> bool:
    """Return True only on explicit confirmed free-exhaustion evidence.

    Accepted evidence shapes:
      {"code": "FREE_ALLOWANCE_EXHAUSTED", "confirmed": True}
    or a string containing "FREE_ALLOWANCE_EXHAUSTED:confirmed".
    or provider-shaped evidence with the exact vendor class
      FreeUsageLimitError (including decoded responseBody JSON)
    or a session retry action with reason free_tier_limit and
      provider opencode.

    Generic 429, timeouts, permission/consent, invalid-plan,
    RateLimitError/rate_limit, DataPolicyError, RegionError, AuthError,
    and GoUsageLimitError/account_rate_limit return False.
    """
    if isinstance(error, dict):
        code = str(error.get("code", ""))
        confirmed = error.get("confirmed") is True
        allowance = str(error.get("allowance", "free"))
        if code == EXPLICIT_FREE_EXHAUSTED_CODE and confirmed and allowance == "free":
            return True
        if classify_provider_free_exhaustion(error):
            return True
        return False
    if isinstance(error, str):
        if "FREE_ALLOWANCE_EXHAUSTED:confirmed" in error:
            return True
        if classify_provider_free_exhaustion(error):
            # Only the exact vendor class in text form; bare 429 stays False.
            return True
        return False
    if isinstance(error, (list, tuple)):
        return classify_provider_free_exhaustion(error)
    return False


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
