"""Installed-runtime discovery, provenance, and recovery compatibility.

Stdlib only. A job's controller runs from one installed Model Router
runtime (the plugin directory holding ``bin/model-router`` and
``runner/``). Recovery continues on the currently installed runtime,
but only when the stored job state is explicitly compatible with it;
unsupported state is retained with its specific reason instead of
being migrated.
"""
from __future__ import annotations

import json
from pathlib import Path

PKG_ROOT = str(Path(__file__).resolve().parents[1])


def version(root: str | None = None) -> str:
    """The runtime package version, or ``unknown`` when unreadable."""
    try:
        text = (Path(root or PKG_ROOT) / "VERSION").read_text(encoding="utf-8")
    except OSError:
        return "unknown"
    return text.strip() or "unknown"


# The version of this loaded package, read once at import while its own
# directory provably exists. ``installed_runtime()`` reports this instead
# of re-reading ``VERSION`` at call time, so provenance recorded after
# the old runtime path is removed still names the real old version
# instead of ``unknown``.
IMPORT_VERSION = version()


def installed_runtime(root: str | None = None) -> dict:
    """Provenance of the runtime executing this call.

    Recorded on submit and on every recovery assessment, so the ledger shows which runtime and policy actually
    ran before and after a recovery. Values are plain strings and ints
    only.
    """
    from . import policy, store

    here = str(root or PKG_ROOT)
    if root is None or here == PKG_ROOT:
        ver = IMPORT_VERSION
    else:
        ver = version(here)
    return {
        "root": here,
        "version": ver,
        "policy_id": policy.POLICY_ID,
        "policy_version": policy.POLICY_VERSION,
        "schema_version": store.SCHEMA_VERSION,
    }


def latest_provenance(events: list[dict]) -> dict | None:
    """The most recent runtime provenance recorded before recovery.

    Read from the submit event, so the ``before`` side of a recovery
    assessment names the runtime that accepted the job. None when
    nothing recorded one: reported as unknown, never invented.
    """
    for ev in events or []:
        try:
            payload = json.loads(ev.get("payload_json") or "{}") or {}
        except ValueError:
            continue
        if isinstance(payload, dict) and isinstance(payload.get("runtime"), dict):
            return dict(payload["runtime"])
    return None


def check_compatible(job: dict) -> tuple[bool, str]:
    """Smallest explicit compatibility decision for the recovery boundary.

    Compatible only when this runtime can read the stored job state as
    is: the controller state parses as a JSON object, the job policy is
    this runtime's policy, and the job route is known to this runtime's
    policy.
    Anything else returns the specific incompatibility; the caller
    retains the work and never migrates, substitutes a route, or
    invents provenance. Additive database migrations are not consulted:
    they move old rows forward inside one runtime, they do not bless an
    arbitrary replacement runtime.
    """
    from . import policy, store

    raw_state = job.get("controller_state")
    if raw_state:
        try:
            state = json.loads(raw_state)
        except ValueError:
            return (False, "controller_state_unreadable: stored controller state "
                           "is not JSON; retaining work, refusing to migrate")
        if not isinstance(state, dict):
            return (False, "controller_state_unreadable: stored controller state "
                           "is not a JSON object; retaining work, refusing to migrate")
    job_policy = job.get("policy_id")
    if job_policy != policy.POLICY_ID:
        return (False, f"policy_incompatible: job policy {job_policy!r} != "
                       f"runtime policy {policy.POLICY_ID!r}; retaining work, "
                       "refusing to migrate or change the route")
    route = job.get("route")
    if route and not policy.is_supported(route):
        return (False, f"route_incompatible: job route {route!r} is unknown to "
                       f"runtime policy {policy.POLICY_VERSION}; explicit route "
                       "preserved, refusing to substitute")
    return (True, f"compatible: policy {policy.POLICY_ID} {policy.POLICY_VERSION}, "
                  f"schema {store.SCHEMA_VERSION} reads the stored job state")
