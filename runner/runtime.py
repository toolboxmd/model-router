"""Installed-runtime discovery, provenance, and recovery compatibility.

Stdlib only. A job's controller and supervisors run from one installed
Model Router runtime (the plugin directory holding ``bin/model-router``
and ``runner/``). When that directory disappears between turns (a plugin
update removes the old plugin-cache path), the next spawn fails before
any child exists. Recovery continues on the currently installed runtime,
but only for failures proven to never have started, and only when the
stored job and invocation state is explicitly compatible with this
runtime. Anything else keeps its existing sticky semantics: live,
successful, and uncertain actions are never replayed, and unsupported
state is retained with its specific reason instead of being migrated.
"""
from __future__ import annotations

import errno
import json
import os
import sys
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

    Recorded on submit, on every invocation, and on every recovery
    assessment, so the ledger shows which runtime and policy actually
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


def diagnose_spawn_error(exc: BaseException, runtime_root: str | None = None) -> dict:
    """Concrete cause of a supervisor spawn failure.

    Returns ``{"runtime_missing": bool, "cause": str}``. ``runtime_missing``
    is True only when the runner's own runtime root (or its Python) is
    gone: the failure provably never started a child and the installed
    runtime can repair it. Any other spawn error (a missing job
    workspace, a bad command, resource exhaustion) is also provably
    never started, but recovery cannot repair it, so it keeps the
    existing sticky semantics. Empty output alone never decides this;
    only the witnessed ``OSError`` does.
    """
    root = str(runtime_root or PKG_ROOT)
    if isinstance(exc, OSError) and not os.path.isdir(root):
        return {
            "runtime_missing": True,
            "cause": (
                f"old runtime path {root} is gone "
                f"({type(exc).__name__}: {exc}); "
                "no supervisor or child started"
            ),
        }
    if isinstance(exc, OSError) and exc.errno in (errno.ENOENT,):
        missing = str(exc.filename or "") if exc.filename else ""
        if missing and os.path.abspath(missing) == os.path.abspath(root):
            return {
                "runtime_missing": True,
                "cause": (
                    f"old runtime path {root} is gone "
                    f"({type(exc).__name__}: {exc}); "
                    "no supervisor or child started"
                ),
            }
    if isinstance(exc, OSError) and not os.path.exists(sys.executable):
        return {
            "runtime_missing": True,
            "cause": (
                f"runner python {sys.executable} is gone "
                f"({type(exc).__name__}: {exc}); "
                "no supervisor or child started"
            ),
        }
    return {
        "runtime_missing": False,
        "cause": f"supervisor spawn failed: {type(exc).__name__}: {exc}",
    }


def _result_obj(inv: dict) -> dict | None:
    try:
        obj = json.loads(inv.get("result_json") or "null")
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def is_runtime_missing_row(inv: dict) -> bool:
    """True only for a never-started row whose cause was the missing runtime.

    This is the only retryable subset: the installed runtime repairs
    exactly this cause. Never-started rows from other causes (a missing
    job workspace, a bad command) keep their sticky semantics.
    """
    obj = _result_obj(inv)
    return bool(
        obj is not None
        and obj.get("never_started") is True
        and obj.get("runtime_missing") is True
    )


def latest_provenance(invocations: list[dict], events: list[dict]) -> dict | None:
    """The most recent runtime provenance recorded before recovery.

    Prefers the newest invocation meta, then the submit event, so the
    ``before`` side of a recovery assessment names the runtime that
    actually ran. None when nothing recorded one (pre-recovery jobs):
    reported as unknown, never invented.
    """
    for inv in sorted(invocations, key=lambda i: i.get("id") or 0, reverse=True):
        try:
            meta = json.loads(inv.get("meta_json") or "{}") or {}
        except ValueError:
            continue
        if isinstance(meta, dict) and isinstance(meta.get("runtime"), dict):
            return dict(meta["runtime"])
    for ev in events or []:
        try:
            payload = json.loads(ev.get("payload_json") or "{}") or {}
        except ValueError:
            continue
        if isinstance(payload, dict) and isinstance(payload.get("runtime"), dict):
            return dict(payload["runtime"])
    return None


def check_compatible(job: dict, invocations: list[dict]) -> tuple[bool, str]:
    """Smallest explicit compatibility decision for the recovery boundary.

    Compatible only when this runtime can read the stored job and
    invocation state as is: the controller state parses as a JSON
    object, the job policy is this runtime's policy, the job route is
    known to this runtime's policy, no stored schema is newer than this
    runtime's, and every stored invocation meta parses as a JSON object.
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
    for inv in invocations or []:
        if inv.get("state") == "abandoned":
            continue
        schema = inv.get("schema_version")
        if schema is not None and int(schema) > int(store.SCHEMA_VERSION):
            return (False, f"schema_incompatible: invocation "
                           f"{str(inv.get('invocation_id') or '')[:8]} schema {schema} "
                           f"is newer than runtime schema {store.SCHEMA_VERSION}; "
                           "retaining work, refusing to migrate")
        try:
            meta = json.loads(inv.get("meta_json") or "{}") or {}
        except ValueError:
            return (False, f"invocation_meta_unreadable: invocation "
                           f"{str(inv.get('invocation_id') or '')[:8]} meta is not "
                           "JSON; retaining work, refusing to migrate")
        if not isinstance(meta, dict):
            return (False, f"invocation_meta_unreadable: invocation "
                           f"{str(inv.get('invocation_id') or '')[:8]} meta is not "
                           "a JSON object; retaining work, refusing to migrate")
    return (True, f"compatible: policy {policy.POLICY_ID} {policy.POLICY_VERSION}, "
                  f"schema {store.SCHEMA_VERSION} reads the stored job state")
