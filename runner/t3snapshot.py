"""Models, limits and role preferences from the T3 provider snapshot.

Stdlib only. The Chromeria fork serves ``GET /api/prism/snapshot``
(toolboxmd/t3code#19): every provider instance with its enabled state,
its models and its usage windows, plus the Prism role kits resolved for
the project. The router reads it before choosing a route:

- Eligible route = its role preference whose T3 instance is enabled and
  whose model the instance offers. Turning a model off in T3 removes it
  from routing.
- A non-empty Prism list replaces the policy's order for that stage;
  entries without a policy route run as ``t3:`` routes. The worker has
  one list per lane; every other role has one ``models`` list
  (toolboxmd/model-router#127). Older snapshots without ``models`` still
  work through the role's entry for the job's lane.
- ``enabled: false`` on Retry (``correction``) or Escalation
  (``recovery``) switches that ladder step off (``ladder_enabled``).
- A usage window at 100 percent with ``resetsAt`` in the future makes
  every route on that meter exhausted until then; at 100 percent with no
  ``resetsAt`` it stays exhausted until a later read says otherwise; at
  80 percent or more the route is degraded. A pool's meter is the T3
  driver named in ``policy.POOLS`` (all Go routes share OpenCode's one
  account meter; Zen free has no reader and keeps its error marks).

A snapshot that cannot be read (older server, unreachable, bad body) is
``unknown``: nothing is filtered and the policy defaults plus error marks
apply, so routing never depends on the endpoint existing.
"""
from __future__ import annotations

import datetime
import time

from . import policy, t3exec

# Reuse one read for this long; T3 refreshes provider state on its own.
CACHE_SECS = 30.0
EXHAUSTED_PERCENT = 100.0
DEGRADED_PERCENT = 80.0

# Prism role -> router stage, for the roles the router dispatches. The
# Retry and Escalation roles keep their internal keys ``correction`` and
# ``recovery`` so the snapshot contract stays stable.
ROLE_STAGES = {"dispatcher": "dispatch", "correction": "correction",
               "recovery": "recovery", "reviewer": "review"}

_CACHE: dict = {"key": None, "at": 0.0, "snapshot": None, "error": None,
                "lane": None}


def _ts(value) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        moment = datetime.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return moment.timestamp()


def _providers(snapshot: dict) -> list[dict]:
    rows = snapshot.get("providers") if isinstance(snapshot, dict) else None
    return [p for p in rows or [] if isinstance(p, dict) and p.get("instanceId")]


def stage_preferences(snapshot: dict, lane_stage: str | None) -> dict[str, list[str]]:
    """Stage orders from the snapshot's role lists (non-empty lists only).

    Worker lanes map onto the three implementation stages. The
    dispatcher, reviewer, Retry and Escalation roles read their single
    ``models`` list; a snapshot without it (older fork) falls back to the
    role's entry for the job's lane.
    """
    roles = snapshot.get("roles") if isinstance(snapshot, dict) else None
    if not isinstance(roles, dict):
        return {}

    def routes(role: str, prism_lane: str, single: bool = False) -> list[str]:
        kit = roles.get(role)
        if single and isinstance(kit, dict) and isinstance(kit.get("models"), list):
            entries = kit["models"]
        else:
            lanes = kit.get("lanes") if isinstance(kit, dict) else None
            entries = lanes.get(prism_lane) if isinstance(lanes, dict) else None
        out: list[str] = []
        for e in entries or []:
            if not isinstance(e, dict):
                continue
            instance, model = e.get("instanceId"), e.get("model")
            if isinstance(instance, str) and instance and isinstance(model, str) and model:
                effort = e.get("effort") if isinstance(e.get("effort"), str) else None
                route = policy.preference_route(instance, model, effort)
                if route not in out:
                    out.append(route)
        return out

    overrides: dict[str, list[str]] = {}
    for stage, prism_lane in policy.PRISM_LANE_OF.items():
        worker = routes("worker", prism_lane)
        if worker:
            overrides[stage] = worker
    job_lane = policy.PRISM_LANE_OF.get(lane_stage or policy.DEFAULT_LANE, "medium")
    for role, stage in ROLE_STAGES.items():
        found = routes(role, job_lane, single=True)
        if found:
            overrides[stage] = found
    return overrides


def ladder_enabled(snapshot: dict | None = None) -> dict[str, bool]:
    """Whether Retry (``correction``) and Escalation (``recovery``) are on.

    Reads the role's ``enabled`` flag from the given snapshot, or the last
    applied one; only an explicit ``false`` switches a step off, so an
    unknown or older snapshot keeps both on.
    """
    snap = snapshot if snapshot is not None else _CACHE.get("applied")
    roles = snap.get("roles") if isinstance(snap, dict) else None
    out = {}
    for role in ("correction", "recovery"):
        kit = roles.get(role) if isinstance(roles, dict) else None
        out[role] = not (isinstance(kit, dict) and kit.get("enabled") is False)
    return out


def _window_state(windows, now: float) -> tuple[str | None, str | None]:
    """(state, detail) for a meter: exhausted, degraded, or None."""
    worst: tuple[str | None, str | None] = (None, None)
    for w in windows or []:
        if not isinstance(w, dict):
            continue
        try:
            pct = float(w.get("usedPercent"))
        except (TypeError, ValueError):
            continue
        resets = _ts(w.get("resetsAt"))
        if resets is not None and resets <= now:
            continue  # the window already reset
        label = str(w.get("label") or w.get("id") or "window")
        until = w.get("resetsAt") if resets is not None else "next T3 refresh"
        if pct >= EXHAUSTED_PERCENT:
            return "exhausted", f"{label} {pct:g}% until {until}"
        if pct >= DEGRADED_PERCENT and worst[0] is None:
            worst = ("degraded", f"{label} {pct:g}% until {until}")
    return worst


def route_states(snapshot: dict | None, routes, now: float | None = None) -> dict[str, tuple[str, str]]:
    """Route -> (state, reason) for routes the snapshot rules out or limits.

    ``ineligible`` (instance missing or disabled, model not offered),
    ``exhausted`` or ``degraded`` (usage windows). Routes that are fine
    are absent. An unknown snapshot rules out nothing.
    """
    if not isinstance(snapshot, dict):
        return {}
    now = now if now is not None else time.time()
    providers = _providers(snapshot)
    by_instance = {p["instanceId"]: p for p in providers}
    meters: dict[str, tuple[str | None, str | None]] = {}
    for p in providers:
        driver = p.get("driver")
        limits = p.get("usageLimits") if isinstance(p.get("usageLimits"), dict) else {}
        if isinstance(driver, str) and driver not in meters:
            meters[driver] = _window_state(limits.get("windows"), now)
    out: dict[str, tuple[str, str]] = {}
    for route in routes:
        try:
            spec = policy.route_spec(route)
        except ValueError:
            continue
        provider = by_instance.get(spec["instance"])
        if provider is None:
            out[route] = ("ineligible", f"T3 has no provider instance {spec['instance']}")
            continue
        if not provider.get("enabled", True):
            out[route] = ("ineligible", f"{spec['instance']} is disabled in T3")
            continue
        slugs = {m.get("slug") for m in provider.get("models") or [] if isinstance(m, dict)}
        if spec["model"] not in slugs:
            out[route] = ("ineligible", f"{spec['model']} is not enabled on {spec['instance']}")
            continue
        meter = policy.pool_meter(spec["pool"])
        state, detail = meters.get(meter, (None, None)) if meter else (None, None)
        if state is not None:
            out[route] = (state, f"{meter} meter {detail}")
    return out


def _cache_key(client, project_id) -> tuple:
    return (getattr(client, "server_url", id(client)), project_id)


def read(client, project_id: str | None = None, force: bool = False) -> dict | None:
    """The snapshot for a project, cached for ``CACHE_SECS``; None when unknown."""
    key = _cache_key(client, project_id)
    now = time.time()
    if not force and _CACHE["key"] == key and now - _CACHE["at"] < CACHE_SECS:
        return _CACHE["snapshot"]
    try:
        snap = client.prism_snapshot(project_id)
        error = None
        if not isinstance(snap, dict) or not isinstance(snap.get("providers"), list):
            snap, error = None, "snapshot body carries no providers"
    except (t3exec.T3Error, OSError, ValueError, AttributeError) as e:
        snap, error = None, str(e)[:300]
    _CACHE.update(key=key, at=now, snapshot=snap, error=error)
    return snap


def apply(snapshot: dict | None, lane_stage: str | None = None) -> None:
    """Install the snapshot's stage orders as the policy's overrides."""
    policy.STAGE_OVERRIDES.clear()
    if isinstance(snapshot, dict):
        policy.STAGE_OVERRIDES.update(stage_preferences(snapshot, lane_stage))
    _CACHE["lane"] = lane_stage
    _CACHE["applied"] = snapshot


def refresh(client, project_id: str | None = None, lane_stage: str | None = None,
            force: bool = False) -> dict | None:
    """Read (or reuse) the snapshot and apply it; returns it or None."""
    snap = read(client, project_id, force=force)
    apply(snap, lane_stage)
    return snap


def refresh_for_job(job: dict, client=None, force: bool = False) -> dict | None:
    """Refresh for a job's T3 server and planner project. Never raises:
    any failure leaves the snapshot unknown (no filtering)."""
    try:
        client = client or t3exec.client_for_job(job)
        project = None
        planner = (job or {}).get("planner_t3_thread")
        if planner:
            try:
                project = t3exec.project_id_for_thread(client, planner)
            except (t3exec.T3Error, ValueError):
                project = None
        return refresh(client, project, (job or {}).get("lane"), force=force)
    except Exception as e:  # noqa: BLE001 - unknown, never blocks routing
        _CACHE.update(key=None, at=0.0, snapshot=None, error=str(e)[:300])
        apply(None, (job or {}).get("lane"))
        return None


def current_states(now: float | None = None) -> dict[str, tuple[str, str]]:
    """Route states from the last applied snapshot ({} when unknown)."""
    return route_states(_CACHE.get("applied"), policy.known_routes(), now)


def skip_sets(now: float | None = None) -> tuple[set[str], set[str]]:
    """(exhausted, degraded) routes from the last applied snapshot.

    Ineligible routes join the exhausted set: both are skipped outright.
    """
    exhausted, degraded = set(), set()
    for route, (state, _reason) in current_states(now).items():
        (degraded if state == "degraded" else exhausted).add(route)
    return exhausted, degraded


def view() -> dict:
    """Operator view for ``capacity``: snapshot status and route states."""
    snap = _CACHE.get("applied")
    if not isinstance(snap, dict):
        return {"snapshot": "unknown", "error": _CACHE.get("error"),
                "stage_overrides": dict(policy.STAGE_OVERRIDES)}
    return {"snapshot": "read", "generated_at": snap.get("generatedAt"),
            "project_id": snap.get("projectId"),
            "stage_overrides": dict(policy.STAGE_OVERRIDES),
            "ladder_enabled": ladder_enabled(snap),
            "routes": {r: {"state": s, "reason": why}
                       for r, (s, why) in sorted(current_states().items())}}


def reset() -> None:
    """Forget the cached snapshot and overrides (tests and new jobs)."""
    _CACHE.update(key=None, at=0.0, snapshot=None, error=None, lane=None, applied=None)
    policy.STAGE_OVERRIDES.clear()
