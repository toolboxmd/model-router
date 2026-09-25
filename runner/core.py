"""Core durable operations. Stdlib only.

State machine:
  pending -> running <-> question_pending -> succeeded | failed | cancelled
  pending -> cancelled
  running | question_pending -> blocked (unknown ownership, explicit reason)
  blocked -> running (manual recover after the foreign worker is gone)
  Terminal states (succeeded, failed, cancelled) are final: never
  resurrected, never relaunched.

Ownership: jobs.owner_token + jobs.owner_pid is the lease. The live
worker advertises its token in workers/<id>.json. A worker is owned
iff advertised token == lease token AND pid matches AND pid is alive
AND the heartbeat is fresh. PID aliveness alone never proves ownership.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
import secrets
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from . import adapters
from . import harnesses
from . import policy
from . import runtime
from . import store

REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
HEARTBEAT_FRESH_SECS = 15.0


class RunnerError(Exception):
    pass


class NotFoundError(RunnerError):
    pass


class ConflictError(RunnerError):
    pass


class WorkspaceConflictError(RunnerError):
    pass


class TerminalError(RunnerError):
    pass


class BlockedError(RunnerError):
    pass


class OwnershipError(RunnerError):
    pass


def _utcnow() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _parse_ts(ts: str | None) -> float | None:
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(ts).timestamp()
    except ValueError:
        return None


def _task_hash(task_json: str) -> str:
    return hashlib.sha256(task_json.encode("utf-8")).hexdigest()


def _canonical_task(task) -> str:
    if isinstance(task, str):
        try:
            obj = json.loads(task)
        except ValueError:
            # Plain-text task: wrap deterministically.
            return json.dumps({"text": task}, sort_keys=True, separators=(",", ":"))
        return json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return json.dumps(task, sort_keys=True, separators=(",", ":"))


def _validate_request_id(request_id: str) -> None:
    if not request_id or not REQUEST_ID_RE.match(request_id):
        raise ValueError(f"invalid request ID: {request_id!r}")


def _canonical_workspace(workspace: str, must_exist: bool = True) -> str:
    if not workspace:
        raise ValueError("missing workspace")
    # Physical path: a symlink and its target are one workspace. The
    # directory must exist so claims compare device and inode.
    real = os.path.realpath(os.path.expanduser(workspace))
    if must_exist and not os.path.isdir(real):
        raise ValueError(f"workspace is not an existing directory: {workspace!r}")
    return real


def _dir_chain(path: str) -> list:
    """(device, inode) of a directory and every ancestor.

    Identity survives case variants and firmlink aliases that string
    comparison misses on macOS.
    """
    chain = []
    cur = os.path.realpath(path)
    if not os.path.isdir(cur):
        return chain  # not created yet: compare paths instead
    while True:
        try:
            st = os.stat(cur)
            chain.append((st.st_dev, st.st_ino))
        except OSError:
            pass
        parent = os.path.dirname(cur)
        if parent == cur:
            return chain
        cur = parent


def _paths_overlap(a: str, b: str) -> bool:
    """True when one workspace equals, contains, or is inside the other."""
    ca, cb = _dir_chain(a), _dir_chain(b)
    if ca and cb:
        return ca[0] in cb or cb[0] in ca
    a = a.rstrip(os.sep) + os.sep
    b = b.rstrip(os.sep) + os.sep
    return a.startswith(b) or b.startswith(a)


def _reap_if_zombie(pid: int) -> bool:
    """True if pid was a reaped zombie (hence not alive)."""
    try:
        waited, _ = os.waitpid(int(pid), os.WNOHANG)
        return waited == int(pid)
    except ChildProcessError:
        return False
    except (ValueError, OverflowError, OSError):
        return False


def _is_pid_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        pid = int(pid)
    except (ValueError, OverflowError):
        return False
    # Reap zombies first: a zombie answers kill(pid,0) but is not alive.
    if _reap_if_zombie(pid):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _is_pgid_alive(pgid: int | None) -> bool:
    """True if the recorded process group still has at least one member."""
    if pgid is None:
        return False
    try:
        pgid = int(pgid)
    except (ValueError, OverflowError):
        return False
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _pgid_for_pid(pid: int | None) -> int | None:
    if pid is None:
        return None
    try:
        return os.getpgid(int(pid))
    except (ProcessLookupError, OSError, ValueError):
        return None


def _process_start_time(pid: int | None) -> int | None:
    """Best-effort process start identity (stdlib-only, seconds since epoch).

    Used together with PID/PGID to reduce reuse collisions. Returns None
    when unavailable without adding non-stdlib dependencies.
    """
    if pid is None:
        return None
    try:
        pid = int(pid)
    except (ValueError, OverflowError):
        return None
    # /proc/<pid>/stat starttime is Linux-only and not stdlib-friendly.
    # Use the executable's stat-based birth time approximation on platforms
    # where /proc/<pid> exists; otherwise fall back to None.
    try:
        st = os.stat(f"/proc/{pid}")
        return int(st.st_mtime)
    except (OSError, ValueError):
        return None


def _heartbeat_fresh(updated: str | None) -> bool:
    ts = _parse_ts(updated)
    if ts is None:
        return False
    return (time.time() - ts) <= HEARTBEAT_FRESH_SECS


def _row_to_job(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


# ---------------------------------------------------------------------------
# Durable child ownership (invocations)
# ---------------------------------------------------------------------------

# Detached helpers run as `python -m runner.*` from the package root, so
# they import this package whatever the caller's working directory is.
_PKG_ROOT = str(Path(__file__).resolve().parents[1])

RECOVER_OWNED_BLOCKS = ("unresolved invocation", "claimed by live pid",
                        "unknown worker ownership", "live invocation",
                        "orphaned server", "timeout_pending", "cancellation_pending",
                        "controller_step_budget_exhausted", "runtime_missing",
                        "unresolved proof ownership")
# Steps across every launch of one job; a launch has MAX_LOOP_STEPS of them.
MAX_JOB_STEPS = 48
LAUNCH_ACK_GRACE_SECS = 60.0

INVOCATION_KINDS = harnesses.INVOCATION_KINDS
LIVE_INVOCATION_STATES = ("running", "cancelling")


def _infer_invocation_kind(cmd: list[str]) -> str:
    """Infer the invocation kind from a built-in adapter command."""
    return harnesses.kind_for_cmd(cmd)


def _is_dispatch_row(inv: dict) -> bool:
    """A ledger row that ran the dispatcher, on whichever harness."""
    return inv.get("stage") == "dispatch" or harnesses.harness_for(inv.get("kind")).is_dispatch(inv.get("kind"))

def _process_start_identity(pid: int | None) -> str | None:
    from .supervisor import process_start_identity
    return process_start_identity(pid)


class LeaseLostError(OwnershipError):
    pass


def _identity_state(pid, recorded: str | None) -> str:
    """``dead``, ``match``, ``mismatch`` (the PID now belongs to another
    process, so the recorded one ended), or ``unknown`` (alive but no
    recorded or readable start identity)."""
    if not pid or not _is_pid_alive(pid):
        return "dead"
    if not recorded:
        return "unknown"
    current = _process_start_identity(pid)
    if not current:
        return "unknown"
    return "match" if current == recorded else "mismatch"


def _identity_matches(pid, recorded: str | None) -> bool:
    """True only when ``pid`` is proven to be the recorded process."""
    return _identity_state(pid, recorded) == "match"


def _possibly_alive(pid, recorded: str | None) -> bool:
    """Conservative liveness for ownership: an unproven identity counts as
    alive so the runner blocks instead of replacing a possible owner."""
    return _identity_state(pid, recorded) in ("match", "unknown")


def _owner_state(job) -> str:
    try:
        start = job["owner_start"]
    except (KeyError, IndexError):
        start = None
    return _identity_state(job["owner_pid"], start)


def _owner_alive(job) -> bool:
    return _owner_state(job) in ("match", "unknown")


def _signal_pid(pid, recorded: str | None, sig) -> bool:
    """Signal only a process proven to be the recorded one."""
    if pid and _identity_matches(pid, recorded):
        try:
            os.kill(int(pid), sig)
            return True
        except (ProcessLookupError, PermissionError, ValueError, OSError):
            pass
    return False


def _signal_group(pgid, leader_pid, recorded: str | None, sig) -> None:
    """Signal a recorded process group. A live leader must be proven to be
    the recorded process; a dead leader leaves only original members, and
    a PGID cannot be reused while any member exists."""
    if not pgid:
        return
    if leader_pid and _is_pid_alive(leader_pid) and not _identity_matches(leader_pid, recorded):
        return
    try:
        os.killpg(int(pgid), sig)
    except (ProcessLookupError, PermissionError, ValueError, OSError):
        pass


def _supervisor_state(inv: dict) -> str:
    return _identity_state(inv.get("supervisor_pid"), inv.get("supervisor_start"))


def _supervisor_alive(inv: dict) -> bool:
    return _supervisor_state(inv) in ("match", "unknown")


NEVER_STARTED_GRACE_SECS = 60.0


def _with_child_record(inv: dict) -> dict:
    """Fill an uncommitted child PID from the supervisor's private side
    record, written right after spawn and before the database commit."""
    if inv.get("pid") is not None or inv.get("supervisor_pid") is None \
            or inv.get("state") not in LIVE_INVOCATION_STATES:
        return inv
    from .supervisor import child_record_path
    try:
        rec = json.loads(Path(child_record_path(inv["stdout_path"])).read_text())
    except (OSError, ValueError, KeyError, TypeError):
        return inv
    if not isinstance(rec, dict) or not rec.get("pid"):
        return inv
    return dict(inv, pid=rec["pid"], pgid=rec.get("pgid") or rec["pid"],
                process_start=rec.get("start"))


def _invocation_ownership(inv: dict) -> str:
    """Classify a recorded invocation.

    ``finished``: state recorded. ``live``: the child or its supervisor
    still runs (or cannot be proven gone). ``orphaned``: an owned OpenCode
    server outlived its supervisor; only its task-owned group may be
    stopped. ``dead``: both are gone and output can be consumed once.
    ``never_started``: no supervisor ever claimed the row. ``unresolved``:
    a child PID was never recorded while its supervisor is gone, or the
    child's identity cannot be proven; never treated as death.
    """
    state = inv.get("state")
    if state not in LIVE_INVOCATION_STATES:
        return "finished"
    inv = _with_child_record(inv)
    pid = inv.get("pid")
    pgid = inv.get("pgid")
    if pid is None or pgid is None:
        if inv.get("supervisor_pid") is None:
            started = _parse_ts(inv.get("started_at"))
            if started is not None and time.time() - started > NEVER_STARTED_GRACE_SECS:
                return "never_started"
            return "unresolved"
        # The supervisor claimed the row and records the child next.
        if _supervisor_alive(inv):
            return "live"
        # The child writes its side record before exec, so a dead
        # supervisor without one after the grace never started a child.
        started = _parse_ts(inv.get("started_at"))
        if started is not None and time.time() - started > NEVER_STARTED_GRACE_SECS:
            return "never_started"
        return "unresolved"
    child = _identity_state(pid, inv.get("process_start"))
    if child == "unknown":
        return "unresolved" if not _supervisor_alive(inv) else "live"
    if child == "match" or (child == "mismatch" and _is_pgid_alive(pgid)
                            and not _is_pid_alive_as_leader(pid, pgid)):
        if harnesses.harness_for(inv.get("kind")).owned_server \
                and inv.get("supervisor_pid") and _supervisor_state(inv) in ("dead", "mismatch"):
            return "orphaned"
        return "live"
    if child == "dead" and _is_pgid_alive(pgid):
        # The leader exited but group members remain.
        if harnesses.harness_for(inv.get("kind")).owned_server \
                and _supervisor_state(inv) in ("dead", "mismatch"):
            return "orphaned"
        return "live"
    if _supervisor_alive(inv):
        return "live"
    return "dead"


def _is_pid_alive_as_leader(pid, pgid) -> bool:
    """True when ``pid`` currently leads ``pgid`` (a reused PID usually does not)."""
    try:
        return os.getpgid(int(pid)) == int(pgid)
    except (ProcessLookupError, PermissionError, ValueError, OSError):
        return False


def _abandon_never_started(state_dir, request_id: str, lock_holder: bool = False) -> int:
    """Close invocation rows that never got a child.

    A row counts when no supervisor claimed it within the grace, or when its
    supervisor is proven dead and the child never wrote its side record
    (the child writes it before exec). Rows are left alone while another
    process holds the controller lock: that controller may still start
    them. The update is guarded on ``pid IS NULL`` under the write lock.
    """
    if not lock_holder and controller_lock_held(state_dir, request_id):
        return 0
    n = 0
    for inv in _list_invocations(state_dir, request_id):
        if _invocation_ownership(inv) != "never_started":
            continue
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE invocations SET state='abandoned', rc=125, ended_at=?, consumed_at=?"
                " WHERE invocation_id=? AND pid IS NULL AND state IN ('running','cancelling')"
                " AND (supervisor_pid IS NULL OR supervisor_pid=?)",
                (_utcnow(), _utcnow(), inv["invocation_id"], inv.get("supervisor_pid")))
            if cur.rowcount:
                n += 1
                _event(con, request_id, "invocation_never_started",
                       {"invocation_id": inv["invocation_id"][:16]})
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            con.close()
    return n


def _stop_orphaned_invocations(state_dir, request_id: str) -> int:
    """Stop owned OpenCode servers whose supervisor died.

    Their password existed only in the dead supervisor, so no process can
    observe or abort the session; the recorded group is task-owned.
    """
    stopped = 0
    for inv in _list_invocations(state_dir, request_id):
        if _invocation_ownership(inv) != "orphaned":
            continue
        for sig in (signal.SIGTERM, signal.SIGKILL):
            _signal_group(inv["pgid"], inv.get("pid"), inv.get("process_start"), sig)
            end = time.monotonic() + 5.0
            while time.monotonic() < end and _is_pgid_alive(inv["pgid"]):
                time.sleep(0.1)
            if not _is_pgid_alive(inv["pgid"]):
                break
        dead = not _is_pgid_alive(inv["pgid"])
        stopped += 1 if dead else 0
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            _event(con, request_id,
                   "orphaned_server_stopped" if dead else "orphaned_server_stop_failed",
                   {"invocation_id": inv["invocation_id"][:16]})
            con.execute("COMMIT")
        finally:
            con.close()
    return stopped


def _invocation_output_paths(root: Path, request_id: str, invocation_id: str) -> tuple[Path, Path]:
    return (
        root / "outputs" / f"{request_id}.{invocation_id}.stdout",
        root / "outputs" / f"{request_id}.{invocation_id}.stderr",
    )


def _parse_session_from_output(kind: str, stdout: str, stderr: str,
                               job: dict | None = None) -> tuple[str | None, str | None]:
    """(session_id, session_kind) as the harness behind ``kind`` reports it."""
    return harnesses.harness_for(kind).parse_session(kind, stdout, stderr, job)

def _redact_cmd(cmd: list[str]) -> list[str]:
    return [
        ("<redacted>" if any(s in str(c).lower() for s in ("password", "bearer", "token=")) else c)
        for c in cmd
    ]


# A failed model turn is never replayed automatically: the job blocks
# with its reason. Replaying could fork a task or repeat a side effect.
MAX_FAILED_ATTEMPTS_PER_ACTION = 1


def _redact_meta(meta: dict) -> dict:
    """Key redaction plus free-text secret shapes, without truncation.

    The supervisor re-reads the prompt from this ledger row, so the
    value stays complete while credentials are masked.
    """
    redacted = store.redact_for_log(dict(meta or {}))

    def _walk(obj):
        if isinstance(obj, dict):
            return {k: _walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_walk(v) for v in obj]
        if isinstance(obj, str):
            return adapters.redact_text(obj)
        return obj

    return _walk(redacted)


def _action_key(kind: str, cmd: list[str], meta: dict | None) -> str:
    """Identity of one logical side effect, stable across controllers.

    The harness behind ``kind`` decides what is stable: volatile values
    such as a saved session learned by an earlier attempt are excluded
    so a restarted controller finds the same key.
    ``try`` numbers repeated worker turns for one dispatcher turn (seq),
    so a bounded same-route retry after a stall is a new attempt, never a
    silent reuse and never a duplicate writer. Only stalled failures take a
    new number; every other outcome reuses its original identity.
    """
    return harnesses.harness_for(kind).action_key(kind, cmd, meta)


def _read_invocation_output(inv: dict) -> tuple[str, str]:
    out = err = ""
    try:
        out = Path(inv["stdout_path"]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    try:
        err = Path(inv["stderr_path"]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    return out, err


def _mark_collected(state_dir, request_id: str, invocation_id: str, reused: bool) -> None:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "UPDATE invocations SET consumed_at=COALESCE(consumed_at, ?) WHERE invocation_id=?",
            (_utcnow(), invocation_id))
        if reused:
            _event(con, request_id, "invocation_reused",
                   {"invocation_id": invocation_id[:16]})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _prior_action_result(state_dir, request_id: str, key: str):
    """Return (rc, out, err) of a finished attempt to reuse, else None.

    Raises OwnershipError while an attempt for the same action is live,
    orphaned, or unresolved: a second copy would be a duplicate writer.
    Rows with explicit never-started runtime-missing evidence are skipped:
    the installed runtime repairs exactly that cause, so the same action
    is made fresh instead of memoized. Every other failed row keeps its
    memoization; live, successful, and uncertain actions are never replayed.
    """
    prior = [i for i in _list_invocations(state_dir, request_id)
             if i.get("action_key") == key and i.get("state") != "abandoned"
             and not runtime.is_runtime_missing_row(i)]
    if not prior:
        return None
    if any(_invocation_ownership(i) == "never_started" for i in prior):
        _abandon_never_started(state_dir, request_id, lock_holder=True)
        prior = [i for i in _list_invocations(state_dir, request_id)
                 if i.get("action_key") == key and i.get("state") != "abandoned"
                 and not runtime.is_runtime_missing_row(i)]
        if not prior:
            return None
    for inv in prior:
        own = _invocation_ownership(inv)
        if own == "live":
            # Adopt: the same action is already running under its own
            # supervisor. Wait for its durable result instead of copying it.
            # No elapsed deadline (#88): a productive turn stays running
            # regardless of age, so adoption waits until ownership leaves
            # live, or until cancellation/terminal state ends the wait.
            while True:
                cur = [i for i in _list_invocations(state_dir, request_id)
                       if i["invocation_id"] == inv["invocation_id"]]
                if not cur or _invocation_ownership(cur[0]) != "live":
                    break
                try:
                    _job = get_job(state_dir, request_id)
                except NotFoundError:
                    break
                if _job.get("cancel_requested") or _job.get("status") in store.TERMINAL:
                    break
                time.sleep(0.2)
            own = _invocation_ownership(
                [i for i in _list_invocations(state_dir, request_id)
                 if i["invocation_id"] == inv["invocation_id"]][0])
        if own in ("live", "orphaned", "unresolved"):
            raise OwnershipError(
                f"action already owned by invocation {inv['invocation_id'][:8]} ({own}); "
                "refusing duplicate")
    if any(i.get("state") in LIVE_INVOCATION_STATES for i in prior):
        consume_finished_invocations(state_dir, request_id)
        prior = [i for i in _list_invocations(state_dir, request_id)
                 if i.get("action_key") == key and i.get("state") != "abandoned"
                 and not runtime.is_runtime_missing_row(i)]
    done = [i for i in prior if i.get("state") == "completed" and i.get("rc") == 0]
    failed = [i for i in prior if i.get("state") != "completed" or i.get("rc") != 0]
    pick = done[-1] if done else (
        failed[-1] if len(failed) >= MAX_FAILED_ATTEMPTS_PER_ACTION else None)
    if pick is None:
        return None
    out, err = _read_invocation_output(pick)
    _mark_collected(state_dir, request_id, pick["invocation_id"], reused=True)
    rc = pick.get("rc")
    return (int(rc) if rc is not None else 1), out, err


def _wait_other_invocations(state_dir, request_id: str, key: str) -> None:
    for inv in _list_invocations(state_dir, request_id):
        if inv.get("action_key") == key or inv.get("state") not in LIVE_INVOCATION_STATES:
            continue
        # No elapsed deadline (#88): wait until the other action's
        # ownership leaves live, or until cancellation/terminal state.
        # A productive turn stays running regardless of age; only
        # unresolvable ownership still refuses the duplicate.
        own = _invocation_ownership(inv)
        while own == "live":
            time.sleep(0.2)
            cur = [i for i in _list_invocations(state_dir, request_id)
                   if i["invocation_id"] == inv["invocation_id"]]
            own = _invocation_ownership(cur[0]) if cur else "finished"
            if own == "live":
                try:
                    _job = get_job(state_dir, request_id)
                except NotFoundError:
                    break
                if _job.get("cancel_requested") or _job.get("status") in store.TERMINAL:
                    break
        if own in ("live", "unresolved", "orphaned", "never_started"):
            raise OwnershipError(
                f"job {request_id}: invocation {inv['invocation_id'][:8]} is {own}; run recover")
    consume_finished_invocations(state_dir, request_id)


def _durable_run(state_dir, request_id: str, owner_token: str, kind: str,
                 cmd: list[str], cwd: str | None = None,
                 timeout: int | None = None, meta: dict | None = None) -> tuple[int, str, str]:
    """Persist spawn intent, then hand the child to an independent supervisor.

    The invocation row is committed with NULL pid/pgid before any Popen.
    That window is unresolved ownership, never proof of death. The
    supervisor process group is detached from the controller so killing
    only the controller cannot destroy output, IDs, or completion. A
    finished attempt of the same action is reused, never rerun.

    ``timeout`` is a legacy compatibility slot and is never enforced:
    since #88 agent turns carry no elapsed deadline, so the value (when
    given) is only recorded on the row for readability of pre-#88
    ledgers. New rows store NULL (no deadline). Explicit cancellation,
    real terminal failures, process ownership, and stream-stall handling
    are the only ends for an active turn.
    """
    if (kind or "") in harnesses.HISTORICAL_INVOCATION_KINDS:
        raise RunnerError(
            f"historical invocation kind {kind!r} is readable but never creatable")
    harness = harnesses.harness_for(kind)
    if timeout is not None:
        try:
            timeout = int(timeout)
        except (TypeError, ValueError):
            timeout = None
    stage_name = (meta or {}).get("stage") or harness.stage_for(kind)
    route_name = (meta or {}).get("route")
    if route_name:
        blocker = harnesses.route_capability_blocker(route_name, stage_name)
        if blocker:
            raise RunnerError(f"route_capability_mismatch: {blocker}")
    key = _action_key(kind, cmd, meta)
    reused = _prior_action_result(state_dir, request_id, key)
    if reused is not None:
        return reused
    root = store.ensure_state_dir(state_dir)
    invocation_id = secrets.token_hex(8)
    stdout_path, stderr_path = _invocation_output_paths(root, request_id, invocation_id)
    store.secure_write_text(stdout_path, "")
    store.secure_write_text(stderr_path, "")

    job = get_job(state_dir, request_id)
    workspace = cwd or job["workspace"]
    # Direction supply through the harness seam (ISSUE_30, ISSUE_52): the
    # installed AgentsMD loader owns the block. On the owned OpenCode
    # server the route's kit decides: a kit naming
    # agentsmd-project-direction records hook with the block hash and no
    # duplicate block; a kit without it gets the verbatim block (runner).
    # Hook hosts (Codex, Claude, Grok) record the hash without duplication;
    # a missing/failed loader records none with its reason and the job
    # continues. Every loader call uses a unique session_id
    # (model-router-<request>-<invocation>-<uuid>) so the loader's
    # per-session hook cache never suppresses a block; the loader is still
    # called exactly once per invocation here. Computed before the insert
    # so every invocation row carries kit, supply, and hashes; the action
    # key above stays on the original prompt (direction fields are extra
    # keys the harness seam ignores), so retries keep one identity.
    _dir_info: dict = {"ok": False, "block": None, "status": "gap",
                       "reason": "loader missing"}
    _dir_supply = "none"
    _dir_hash: str | None = None
    _kit_name: str | None = None
    _kit_hash: str | None = None
    _kit_skills: list = []
    _dir_session_id: str | None = None
    try:
        from . import direction as _direction
        _harness_name = getattr(harness, "name", None) or "opencode"
        _kit_name, _kit_hash, _kit_skills = _direction.kit_for_invocation(
            route_name or (meta or {}).get("route"), stage_name)
        try:
            import secrets as _secrets
            _dir_session_id = (
                f"model-router-{request_id}-{invocation_id}-{_secrets.token_hex(4)}")
        except Exception:
            _dir_session_id = f"model-router-{request_id}-{invocation_id}"
        try:
            _dir_info = _direction.load_direction(
                workspace, host=_harness_name, session_id=_dir_session_id)
        except Exception:
            _dir_info = {"ok": False, "block": None, "status": "gap",
                         "reason": "loader failed: exception",
                         "session_id": _dir_session_id}
        _dir_supply = _direction.supply_for_harness(
            _harness_name, bool(_dir_info.get("ok")), _kit_name)
        if bool(_dir_info.get("ok")) and _dir_info.get("block"):
            _dir_hash = _direction.block_hash(_dir_info.get("block"))
    except Exception:
        pass
    _meta_store = dict(meta or {})
    # Supervisor re-reads the prompt plus the verbatim block from this row;
    # the block stays in the private database (0600), never in public logs.
    try:
        if _dir_info.get("block"):
            _meta_store["direction_block"] = _dir_info.get("block")
    except Exception:
        pass
    _meta_store["direction_supply"] = _dir_supply
    if _dir_hash:
        _meta_store["direction_hash"] = _dir_hash
    if _dir_info.get("status"):
        _meta_store["direction_status"] = _dir_info.get("status")
    if _dir_info.get("reason"):
        _meta_store["direction_reason"] = _dir_info.get("reason")
    if _kit_name:
        _meta_store["kit"] = _kit_name
    if _kit_hash:
        _meta_store["kit_hash"] = _kit_hash
    if _dir_session_id:
        _meta_store["direction_session_id"] = _dir_session_id
    # Runtime provenance travels on every invocation so recovery can name
    # the runtime that actually ran before a transfer. It is added after
    # the action key is computed, so retries keep one identity.
    try:
        _meta_store["runtime"] = runtime.installed_runtime()
    except Exception:
        pass
    # Key redaction cannot see inside free-text values (for example a
    # credential in the task prompt), so mask secret shapes as well. No
    # truncation: the supervisor re-reads this prompt from the ledger.
    meta_json = json.dumps(_redact_meta(_meta_store), sort_keys=True)
    # One writer per job: any other live child (a different action from a
    # previous controller) is adopted by waiting, never run beside.
    _wait_other_invocations(state_dir, request_id, key)
    started_at = _utcnow()

    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        # Lease and action identity are rechecked under the write lock: a
        # controller that lost its lease, or lost a race for the same
        # action, never spawns.
        cur = con.execute("SELECT owner_token, status, cancel_requested FROM jobs"
                          " WHERE request_id=?", (request_id,)).fetchone()
        if cur is None or cur["owner_token"] != owner_token:
            con.execute("ROLLBACK")
            raise LeaseLostError(f"job {request_id}: controller no longer holds the lease")
        if cur["cancel_requested"] or cur["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            raise LeaseLostError(f"job {request_id}: cancelled or terminal")
        other = con.execute(
            "SELECT invocation_id FROM invocations WHERE request_id=? AND state IN ('running','cancelling')"
            " AND (action_key IS NULL OR action_key != ?)", (request_id, key)).fetchone()
        if other is not None:
            con.execute("ROLLBACK")
            raise OwnershipError(f"job {request_id}: invocation {other['invocation_id'][:8]} still owns the job")
        rows = con.execute("SELECT invocation_id, result_json FROM invocations"
                           " WHERE request_id=? AND action_key=? AND state != 'abandoned'",
                           (request_id, key)).fetchall()
        live_rows = [r for r in rows
                     if not runtime.is_runtime_missing_row({"result_json": r["result_json"]})]
        if live_rows:
            con.execute("ROLLBACK")
            reused = _prior_action_result(state_dir, request_id, key)
            if reused is not None:
                return reused
            raise OwnershipError(f"job {request_id}: action already has an attempt; run recover")
        # Only never-started runtime-missing rows remain for this action:
        # the installed runtime repairs exactly that cause, so this
        # controller makes the action fresh under the write lock. The
        # check and the insert below are one atomic transaction, so two
        # controllers cannot both pass it.
        try:
            _skills_json = json.dumps(_kit_skills, sort_keys=True)
        except Exception:
            _skills_json = "[]"
        con.execute(
            "INSERT INTO invocations("
            " invocation_id, request_id, kind, cmd_json, workspace, owner_token,"
            " pid, pgid, process_start, stdout_path, stderr_path, started_at,"
            " state, task_json, timeout_secs, meta_json, action_key,"
            " stage, requested_route, policy_version, reason, harness_version, schema_version,"
            " kit, kit_hash, direction_supply, direction_reason, direction_hash,"
            " direction_status, skills_json, tools_json"
            ") VALUES(?,?,?,?,?,?,NULL,NULL,NULL,?,?,?,'running',?,?,?,?,?,?,?,?,?,?,"
            "?,?,?,?,?,?,?,?)",
            (invocation_id, request_id, kind, json.dumps(list(cmd)),
             workspace, owner_token, str(stdout_path), str(stderr_path),
             started_at, job.get("task_json"), timeout, meta_json, key,
             (meta or {}).get("stage") or harness.stage_for(kind),
             (meta or {}).get("route") or harness.default_route(kind, job),
             policy.POLICY_VERSION, (meta or {}).get("reason") or "initial",
             harness.version(), store.SCHEMA_VERSION,
             _kit_name, _kit_hash, _dir_supply,
             str(_dir_info.get("reason") or "") or None,
             _dir_hash, str(_dir_info.get("status") or "gap"),
             _skills_json, "[]"),
        )
        _event(con, request_id, "invocation_attempting",
               {"invocation_id": invocation_id[:16], "kind": kind})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()

    sup_cmd = [sys.executable, "-m", "runner.supervisor",
               "--state-dir", str(root),
               "--request-id", request_id,
               "--invocation-id", invocation_id]
    try:
        sup = subprocess.Popen(
            sup_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True, close_fds=True, cwd=_PKG_ROOT,
        )
    except OSError as e:
        # Popen raised before any supervisor or child existed: this action
        # provably never started. Only a missing runtime root is repairable
        # (recovery continues on the installed runtime); every other spawn
        # error keeps the existing sticky semantics. The evidence on the row
        # decides, never empty output.
        diag = runtime.diagnose_spawn_error(e)
        now = _utcnow()
        payload = {"never_started": True,
                   "runtime_missing": bool(diag.get("runtime_missing")),
                   "error": diag.get("cause"),
                   "runtime": runtime.installed_runtime()}
        detail = {"invocation_id": invocation_id[:16], "kind": kind,
                  "never_started": True,
                  "runtime_missing": bool(diag.get("runtime_missing")),
                  "cause": diag.get("cause"),
                  "next_action": "recover"}
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "UPDATE invocations SET state='failed', rc=127, ended_at=?, consumed_at=?,"
                " result_json=? WHERE invocation_id=?",
                (now, now, json.dumps(payload, sort_keys=True), invocation_id),
            )
            _event(con, request_id, "supervisor_spawn_failed", detail)
            con.execute("COMMIT")
        finally:
            con.close()
        _measure_invocation(state_dir, request_id, invocation_id)
        if diag.get("runtime_missing"):
            return 127, "", (f"runtime_missing: {diag.get('cause')}; run recover "
                             "on the installed runtime (do not restore cache directories)")
        return 127, "", f"supervisor spawn failed: {e}"

    # The supervisor records its own PID and start identity when it claims
    # the row; the controller never writes them, so an unclaimed row stays
    # recognizable as never started.

    # The supervisor owns the turn's end (completion, stall, explicit
    # cancellation, or real failure) and records the result; the
    # controller adopts that durable result with no elapsed deadline
    # (#88). A productive turn stays running regardless of age. The
    # supervisor reap below (10s) and the drain/cleanup bounds elsewhere
    # are purpose-specific limits, not agent deadlines.
    rc = 125
    while True:
        invs = [i for i in _list_invocations(state_dir, request_id)
                if i["invocation_id"] == invocation_id]
        if invs and invs[0].get("state") not in LIVE_INVOCATION_STATES:
            try:
                rc = int(invs[0]["rc"]) if invs[0].get("rc") is not None else 125
            except (TypeError, ValueError):
                rc = 0
            break
        if sup.poll() is not None:
            # Supervisor exited; consume whatever it persisted.
            time.sleep(0.05)
            invs = [i for i in _list_invocations(state_dir, request_id)
                    if i["invocation_id"] == invocation_id]
            if invs and invs[0].get("state") not in LIVE_INVOCATION_STATES:
                try:
                    rc = int(invs[0]["rc"]) if invs[0].get("rc") is not None else 125
                except (TypeError, ValueError):
                    rc = 0
            else:
                consume_finished_invocations(state_dir, request_id)
                invs = [i for i in _list_invocations(state_dir, request_id)
                        if i["invocation_id"] == invocation_id]
                if invs and invs[0].get("state") not in LIVE_INVOCATION_STATES:
                    try:
                        rc = int(invs[0]["rc"]) if invs[0].get("rc") is not None else 125
                    except (TypeError, ValueError):
                        rc = 125
            break
        time.sleep(0.05)

    stdout_text = ""
    stderr_text = ""
    try:
        stdout_text = Path(stdout_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    try:
        stderr_text = Path(stderr_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass

    try:
        sup.wait(timeout=10)  # reap the finished supervisor
    except Exception:
        pass
    _mark_collected(state_dir, request_id, invocation_id, reused=False)
    _measure_invocation(state_dir, request_id, invocation_id)
    try:
        log = store.output_path_for(root, request_id)
        store.append_text(log, f"[{kind} invocation={invocation_id[:8]} rc={rc}]\n")
    except OSError:
        pass

    return rc, stdout_text, stderr_text


def make_durable_run_cmd(state_dir: str, request_id: str, owner_token: str):
    """Factory for a run_cmd closure that creates durable invocations.

    The returned callable matches the (cmd, cwd, timeout) -> (rc, out, err)
    signature used by controller.dispatch and friends.
    """
    def run_cmd(cmd: list[str], cwd: str | None = None, timeout: int | None = None,
                kind: str | None = None, meta: dict | None = None):
        kind = kind or _infer_invocation_kind(cmd)
        return _durable_run(state_dir, request_id, owner_token, kind,
                            cmd, cwd=cwd, timeout=timeout, meta=meta)
    return run_cmd


def runtime_missing_for_action(state_dir, request_id: str, kind: str,
                               cmd: list[str], meta: dict | None) -> dict | None:
    """Never-started runtime-missing evidence for one action, if still current.

    Recomputes the action key exactly as :func:`_durable_run` did and
    returns the newest matching row's persisted ``{"cause": ...}`` detail,
    but only when that newest non-abandoned row itself carries explicit
    runtime-missing evidence. An older missing-runtime row never wins
    after a newer attempt for the same action completed, failed
    otherwise, or otherwise superseded it: the repaired retry must not
    be reblocked by the stale row it already repaired. Callers use it
    to trade an opaque sticky failure for a recoverable
    ``runtime_missing`` block. Rows without the explicit marker never
    count, however empty their output is.
    """
    try:
        key = _action_key(kind, cmd, meta)
    except Exception:
        return None
    matches = [i for i in _list_invocations(state_dir, request_id)
               if i.get("action_key") == key and i.get("state") != "abandoned"]
    if not matches:
        return None
    row = matches[-1]
    if not runtime.is_runtime_missing_row(row):
        return None
    try:
        obj = json.loads(row.get("result_json") or "null")
    except ValueError:
        obj = None
    detail = dict(obj) if isinstance(obj, dict) else {}
    if not detail.get("cause"):
        detail["cause"] = detail.get("error") or \
            "old runtime path is gone; no supervisor or child started"
    return detail


def _list_invocations(state_dir, request_id: str,
                      state: str | None = None) -> list[dict]:
    con = store.connect(state_dir)
    try:
        if state is None:
            rows = con.execute(
                "SELECT * FROM invocations WHERE request_id=? ORDER BY id",
                (request_id,),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM invocations WHERE request_id=? AND state=? ORDER BY id",
                (request_id, state),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def _reconcile_dead_invocations(state_dir, request_id: str) -> list[dict]:
    """Inspect recorded identity. Never treat NULL pid/pgid as death."""
    now = _utcnow()
    dead = []
    unresolved = 0
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        placeholders = ",".join("?" for _ in LIVE_INVOCATION_STATES)
        rows = con.execute(
            f"SELECT * FROM invocations WHERE request_id=? AND state IN ({placeholders})",
            (request_id, *LIVE_INVOCATION_STATES),
        ).fetchall()
        for r in rows:
            inv = dict(r)
            own = _invocation_ownership(inv)
            if own == "unresolved":
                unresolved += 1
                continue
            if own == "dead":
                # Leave state running so consume_finished_invocations can
                # apply durable output exactly once; only stamp ended_at.
                if not inv.get("ended_at"):
                    con.execute(
                        "UPDATE invocations SET ended_at=? WHERE invocation_id=? AND ended_at IS NULL",
                        (now, inv["invocation_id"]),
                    )
                dead.append(inv)
        _event(con, request_id, "invocations_reconciled",
               {"reconciled_dead": len(dead), "unresolved": unresolved})
        con.execute("COMMIT")
        return dead
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _any_live_invocation(state_dir, request_id: str) -> list[dict]:
    """Return invocations whose recorded child or supervisor is still live."""
    live = []
    for inv in _list_invocations(state_dir, request_id):
        if _invocation_ownership(inv) in ("live", "orphaned"):
            live.append(inv)
    return live


def _any_unresolved_invocation(state_dir, request_id: str) -> list[dict]:
    return [inv for inv in _list_invocations(state_dir, request_id)
            if _invocation_ownership(inv) == "unresolved"]


def consume_finished_invocations(state_dir, request_id: str) -> list[dict]:
    """Consume durable child output exactly once after the child has exited.

    If the child finished while the controller was dead, persist
    rc/session/action/result from the file-backed output and do not
    rerun completed work.
    """
    applied = []
    job = None
    try:
        job = get_job(state_dir, request_id)
    except NotFoundError:
        return applied
    for inv in _list_invocations(state_dir, request_id):
        if inv.get("consumed_at"):
            continue
        own = _invocation_ownership(inv)
        if own in ("live", "unresolved", "orphaned", "never_started"):
            continue
        applied.append(_consume_one_invocation(state_dir, request_id, inv, job))
        try:
            job = get_job(state_dir, request_id)
        except NotFoundError:
            break
    return [a for a in applied if a]


def _consume_one_invocation(state_dir, request_id: str, inv: dict, job: dict) -> dict:
    stdout_text = ""
    stderr_text = ""
    try:
        stdout_text = Path(inv["stdout_path"]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    try:
        stderr_text = Path(inv["stderr_path"]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    kind = inv.get("kind") or ""
    sid, skind = _parse_session_from_output(kind, stdout_text, stderr_text, job)
    head_commit = _workspace_head(job.get("workspace"))  # before the write lock
    try:
        inv_cmd = json.loads(inv.get("cmd_json") or "[]")
    except ValueError:
        inv_cmd = []
    harness = harnesses.harness_for(kind)
    try:
        inv_meta = json.loads(inv.get("meta_json") or "{}") or {}
    except ValueError:
        inv_meta = {}
    # Only the harness's own final record can carry the dispatcher's action.
    envelope = harness.parse_report(kind, stdout_text, inv_cmd) if inv.get("stage") == "dispatch" \
        or harness.is_dispatch(kind) else None
    if envelope is not None and not (isinstance(envelope, dict) and envelope.get("action") in policy.VALID_ACTIONS):
        # An OpenCode summary carries the Luna envelope in its assistant
        # text; the harness seam extracts it so callers never parse
        # harness output directly.
        envelope = harness.luna_action(kind, stdout_text, inv_cmd) \
            if isinstance(envelope, dict) else None
    planner_answer = None
    result_obj = None
    if inv.get("result_json"):
        try:
            result_obj = json.loads(inv["result_json"])
        except ValueError:
            result_obj = None
    rc = inv.get("rc")
    if rc is None:
        # Child and supervisor are dead: the harness's own record decides
        # whether the turn counts, including the last-message file fallback.
        rc = harness.infer_rc(kind, stdout_text, inv_cmd)
    # Completion gates that need no write lock (files, git, the live PR
    # read): the bound proof, named acceptance, PR presence and identity,
    # and the PR's live state. Computed here so the transaction below
    # only persists; a completion the live path would refuse refuses
    # here too instead of succeeding through recovery.
    pre_refusal = None
    if isinstance(envelope, dict) and envelope.get("action") == "completion" \
            and job.get("status") not in store.TERMINAL \
            and not job.get("cancel_requested"):
        _latest0 = latest_turn_report(state_dir, request_id)
        pre_refusal = completion_refusal_reason(_latest0)
        if pre_refusal is None:
            pre_refusal = incomplete_proof_reason(
                job.get("task_json"), _latest0, job.get("workspace"))
        if pre_refusal is None:
            pre_refusal = incomplete_acceptance_reason(job.get("task_json"), envelope)
        if pre_refusal is None:
            pre_refusal = missing_pr_url_reason(
                job.get("job_kind"), envelope, job.get("workspace"))
        if pre_refusal is None and pr_gate_applies(job.get("job_kind"),
                                                   job.get("workspace")):
            _cur0 = envelope.get("pr_url")
            if isinstance(_cur0, str) and _cur0.strip():
                pre_refusal = duplicate_pr_reason(
                    known_pr_url(state_dir, request_id), envelope)
                if pre_refusal is None:
                    _head0 = _latest0.get("head_commit") \
                        if isinstance(_latest0, dict) else None
                    pre_refusal = verify_pr_for_completion(
                        job.get("workspace"), _cur0.strip(), _head0,
                        job.get("task_json"))
    if rc == 0:
        planner_answer = harness.planner_answer(kind, stdout_text, inv_meta, job)
    state = "completed" if rc == 0 else "failed"
    now = _utcnow()
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        # Re-check consumed under the write lock.
        row = con.execute(
            "SELECT consumed_at FROM invocations WHERE invocation_id=?",
            (inv["invocation_id"],),
        ).fetchone()
        if row is not None and row["consumed_at"]:
            con.execute("ROLLBACK")
            return {"action": "already-consumed", "invocation_id": inv["invocation_id"]}
        payload = result_obj if isinstance(result_obj, dict) else {}
        payload = dict(payload)
        payload["rc"] = rc
        if envelope is not None:
            payload["envelope"] = envelope
        if sid:
            payload["session_id"] = sid
            payload["session_kind"] = skind
        con.execute(
            "UPDATE invocations SET rc=?, state=?, ended_at=COALESCE(ended_at, ?),"
            " consumed_at=?, result_json=?, session_id=COALESCE(?, session_id),"
            " session_kind=COALESCE(?, session_kind) WHERE invocation_id=?",
            (rc, state, now, now, json.dumps(payload, sort_keys=True),
             sid, skind, inv["invocation_id"]),
        )
        if sid:
            harness.record_session(con, request_id, sid, skind, kind, job, inv_meta, now)
        job_row = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        job_status = job_row["status"] if job_row else None
        applied = "consumed"
        cancelled = bool(job_row is not None and job_row["cancel_requested"])
        is_dispatch = inv.get("stage") == "dispatch" or harness.is_dispatch(kind)
        turn_ok = harness.turn_ok(kind, rc, stdout_text, inv_cmd)
        # A dispatcher turn counts only from the saved task: a resume that
        # reports another thread, or none, is never applied.
        thread_ok = True
        if is_dispatch:
            saved_thread = job_row["codex_task_id"] if job_row is not None else None
            thread_ok = harness.identity_ok(kind, sid, saved_thread)
        live_job = job_row is not None and job_status not in store.TERMINAL and not cancelled
        # Full measurement in this same transaction, before any planner or
        # envelope application, for every recovered invocation kind. A
        # measurement failure blocks safely here and skips all envelope
        # effects, so no completed event, completion envelope, or
        # successful result can commit. Unknown never blocks, and only
        # genuinely missing or unreadable rollout cases stay unknown
        # inside the rollout reader's file-read boundary. Any other
        # observation or measurement failure (kit lookup, parser logic,
        # database write) blocks safely with evidence.
        _measured_observed = None
        measure_failed = False
        model_gate_applied = False
        try:
            _m_full = _compute_invocation_measurement(
                state_dir, request_id, inv["invocation_id"], kind,
                stdout_text, stderr_text, inv_meta,
                inv.get("started_at"), inv.get("ended_at") or now,
                rc, payload, crashed=inv.get("rc") is None)
            _write_invocation_measurement(con, inv["invocation_id"], _m_full)
            _obs = _m_full["observed"]
            if not (isinstance(_obs, str) and _obs):
                _obs = None
            _measured_observed = _obs
        except Exception as e:
            measure_failed = True
            model_gate_applied = True
            if live_job:
                try:
                    _detail = f"{type(e).__name__}: {str(e)[:150]}"
                except Exception:
                    _detail = "measurement failed"
                _is_codex = getattr(harness, "name", None) == "codex"
                _prefix = "model_observation_failed" if _is_codex else "measurement_failed"
                _mreason = f"{_prefix}: {kind} {_detail}"[:300]
                con.execute(
                    "UPDATE jobs SET status='blocked', block_reason=?, updated_at=?"
                    " WHERE request_id=? AND status NOT IN ('succeeded','failed','cancelled')",
                    (_mreason, now, request_id))
                _event(con, request_id, "blocked", {"reason": _mreason[:120]})
                applied = "blocked"
        if not measure_failed and planner_answer and job_row is not None \
                and job_status not in store.TERMINAL \
                and not job_row["cancel_requested"]:
            qid, text = planner_answer
            upd = con.execute(
                "UPDATE questions SET status='answered', answer=?, answered_at=?"
                " WHERE request_id=? AND qid=? AND status='pending'",
                (text, now, request_id, qid))
            if upd.rowcount:
                _event(con, request_id, "answer_persisted", {"qid": qid, "via": "consumed-invocation"})
                left = con.execute(
                    "SELECT COUNT(*) AS n FROM questions WHERE request_id=? AND status='pending'",
                    (request_id,)).fetchone()["n"]
                if left == 0 and (job_status == "question_pending" or (
                        job_status == "blocked"
                        and str(job_row["block_reason"] or "").startswith("planner_"))):
                    con.execute("UPDATE jobs SET status='running', updated_at=? WHERE request_id=?",
                                (now, request_id))
                    job_status = "running"
                applied = "answer-persisted"
        # Observed-model gate for Codex dispatch turns in recovery, before
        # any failure classification or envelope, using the measured
        # observed_model above consistently. A known mismatch blocks with
        # model_mismatch and no envelope. Failed turns gate alike, beating
        # the generic failure handling below.
        if not measure_failed and live_job and is_dispatch \
                and getattr(harness, "name", None) == "codex":
            try:
                _m_observed = _measured_observed
                _req_route = inv.get("requested_route")
                if not (isinstance(_req_route, str) and _req_route in policy.ROUTES):
                    _meta_route = inv_meta.get("route")
                    if isinstance(_meta_route, str) and _meta_route in policy.ROUTES:
                        _req_route = _meta_route
                    else:
                        try:
                            _st = json.loads(job_row["controller_state"] or "{}") \
                                if job_row is not None else {}
                        except ValueError:
                            _st = {}
                        _dr = _st.get("dispatch_route") if isinstance(_st, dict) else None
                        if isinstance(_dr, str) and _dr in policy.ROUTES:
                            _req_route = _dr
                        else:
                            _req_route = None
                _req_model = policy.ROUTES[_req_route].get("model") if _req_route else None
                _obs_model = _m_observed
                if isinstance(_req_model, str) and _req_model \
                        and isinstance(_obs_model, str) and _obs_model:
                    if _req_model != _obs_model:
                        _mm = f"model_mismatch: requested={_req_model} observed={_obs_model}"
                        con.execute(
                            "UPDATE jobs SET status='blocked', block_reason=?, updated_at=?"
                            " WHERE request_id=? AND status NOT IN ('succeeded','failed','cancelled')",
                            (_mm, now, request_id))
                        _event(con, request_id, "blocked", {"reason": _mm[:120]})
                        applied = "blocked"
                        model_gate_applied = True
            except Exception as e:
                try:
                    _detail = f"{type(e).__name__}: {str(e)[:150]}"
                except Exception:
                    _detail = "observation failed"
                _oreason = f"model_observation_failed: {kind} {_detail}"[:300]
                con.execute(
                    "UPDATE jobs SET status='blocked', block_reason=?, updated_at=?"
                    " WHERE request_id=? AND status NOT IN ('succeeded','failed','cancelled')",
                    (_oreason, now, request_id))
                _event(con, request_id, "blocked", {"reason": _oreason[:120]})
                applied = "blocked"
                model_gate_applied = True
        planner_reason = None
        if not measure_failed and live_job and inv_meta.get("qid") is not None:
            # A planner turn matters only while its question is still open;
            # a question answered meanwhile (public `answer`) wins.
            cb_qid = inv_meta.get("qid")
            still_open = con.execute(
                "SELECT 1 FROM questions WHERE request_id=? AND qid=? AND status='pending'",
                (request_id, cb_qid)).fetchone() is not None
            if still_open and not planner_answer:
                planner_reason = harness.callback_failure_reason(kind, rc, stdout_text, dict(job_row))
        if planner_reason:
            con.execute("UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?"
                        " AND status NOT IN ('succeeded','failed','cancelled')",
                        (planner_reason, now, request_id))
            _event(con, request_id, "blocked", {"reason": planner_reason[:120]})
            applied = "blocked"
        elif not model_gate_applied and live_job and is_dispatch and (not turn_ok or not thread_ok) \
                and not runtime.is_runtime_missing_row(inv):
            # The controller would have blocked on this result; recovery
            # records the same durable, sticky reason instead of leaving
            # the job running without an owner. Muse turns are left to the
            # restarted controller, which may switch routes on evidence.
            # A row with explicit never-started runtime-missing evidence is
            # exempt: the restarted controller remakes that action fresh on
            # the installed runtime instead of inheriting a sticky block.
            reason = (f"luna_task_mismatch: consumed {kind} reported {str(sid or 'no thread')[:24]}"
                      if turn_ok and not thread_ok else f"{kind}_failed rc={rc} (consumed by recovery)")
            con.execute("UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?"
                        " AND status NOT IN ('succeeded','failed','cancelled')", (reason, now, request_id))
            _event(con, request_id, "blocked", {"reason": reason[:120]})
            applied = "blocked"
        if not model_gate_applied and job_row is not None and job_status not in store.TERMINAL \
                and isinstance(envelope, dict) and turn_ok and thread_ok and not cancelled:
            action_name = envelope.get("action")
            # Persist the Luna envelope before any completion effect.
            st = {}
            if job_row["controller_state"]:
                try:
                    st = json.loads(job_row["controller_state"]) or {}
                except ValueError:
                    st = {}
            st["phase"] = "consumed-invocation"
            st["last_action"] = envelope
            st["last_action_name"] = action_name
            st["seq"] = int(st.get("seq") or 0) + 1
            # One job owns one PR: the envelope's identity is preserved
            # here only after the live check passed (pre_refusal is None
            # covers the bound proof, acceptance, PR presence, identity,
            # and live-PR gates computed above), so an invalid first URL
            # never poisons the job and a corrected URL is accepted
            # instead of refused as a duplicate. Correction and recovery
            # update the existing PR instead of opening another.
            _cur = envelope.get("pr_url") if isinstance(envelope, dict) else None
            if action_name == "completion" and pre_refusal is None \
                    and isinstance(_cur, str) and _cur.strip() and not (
                    isinstance(st.get("pr_url"), str) and st["pr_url"].strip()):
                st["pr_url"] = _cur.strip()
                _event(con, request_id, "pr_identity_preserved",
                       {"pr_url": _cur.strip()[:200]})
            con.execute(
                "UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                (json.dumps(st, sort_keys=True), now, request_id),
            )
            _event(con, request_id, "luna_action",
                   {"action": str(action_name or "unknown")[:64], "phase": "consumed-invocation"})
            if action_name == "completion":
                # A job can never succeed while its last implementation
                # turn's proof failed: refuse the envelope, record the
                # refusal, and block with its reason. The same bound-proof,
                # acceptance, PR-presence, PR-identity, and live-PR gates
                # as the live path apply here (precomputed above without
                # the write lock); without a remote the job succeeds as
                # before, and experiment and replay jobs skip the PR gate.
                _refusal = pre_refusal
                if _refusal is not None:
                    con.execute("UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?"
                                " AND status NOT IN ('succeeded','failed','cancelled')",
                                (_refusal, now, request_id))
                    _event(con, request_id, "completion_refused",
                           {"reason": _refusal[:200], "via": "consumed-invocation"})
                    _event(con, request_id, "blocked", {"reason": _refusal[:120]})
                    applied = "blocked"
                else:
                    output = str(envelope.get("output") or "done")
                    _pr = (envelope.get("pr_url") if isinstance(envelope, dict) else None)
                    _pr_norm = _pr if isinstance(_pr, str) and _pr.strip() else None
                    _acc = (envelope.get("acceptance_evidence")
                            if isinstance(envelope, dict) else None)
                    _acc_norm = _acc if isinstance(_acc, str) and _acc.strip() else None
                    result = {"ok": True, "output": json.dumps({"output": output,
                                                                "artifact": envelope.get("artifact"),
                                                                "pr_url": _pr_norm,
                                                                "acceptance_evidence": _acc_norm,
                                                                "head_commit": head_commit},
                                                               sort_keys=True)}
                    con.execute(
                        "UPDATE jobs SET status='succeeded', result_json=?, error_class=NULL,"
                        " head_commit=?, updated_at=? WHERE request_id=?",
                        (json.dumps(result), head_commit, now, request_id),
                    )
                    _event(con, request_id, "completed", {"via": "consumed-invocation"})
                    applied = "completed"
            elif action_name == "planner_question":
                qid = str(envelope.get("qid") or envelope.get("question_id") or "q1")
                prompt = str(envelope.get("prompt") or envelope.get("question") or
                             envelope.get("text") or "Planner input requested.")
                existing_q = con.execute(
                    "SELECT qid, prompt FROM questions WHERE request_id=? AND qid=?",
                    (request_id, qid),
                ).fetchone()
                if existing_q is not None and (existing_q["prompt"] or "") != prompt:
                    # A reused qid with a different prompt is a conflict, on
                    # the live path and in recovery alike: block with the
                    # documented reason instead of accepting the new prompt.
                    # Clear it with `questions --clear QID` or use a new qid.
                    reason = (f"planner_question_conflict: {qid} was stored for a different prompt; "
                              f"clear it with `questions --clear {qid}` or use a new qid")
                    con.execute("UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?"
                                " AND status NOT IN ('succeeded','failed','cancelled')",
                                (reason, now, request_id))
                    _event(con, request_id, "blocked", {"reason": reason[:120]})
                    applied = "blocked"
                else:
                    if existing_q is None:
                        con.execute(
                            "INSERT INTO questions(request_id,qid,prompt,status,created_at)"
                            " VALUES(?,?,?,'pending',?)",
                            (request_id, qid, prompt, now),
                        )
                    if job_status != "question_pending":
                        con.execute(
                            "UPDATE jobs SET status='question_pending', updated_at=? WHERE request_id=?",
                            (now, request_id),
                        )
                    _event(con, request_id, "question_posted", {"qid": qid, "via": "consumed-invocation"})
                    applied = "question-persisted"
        _event(con, request_id, "invocation_consumed",
               {"invocation_id": inv["invocation_id"][:16], "applied": applied, "rc": rc})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    # Recovery measurement already ran inside the transaction above: no
    # post-commit repeat. A measurement failure blocks in-transaction
    # before any envelope, so no completion can commit.
    if applied == "completed":
        try:
            root = store.ensure_state_dir(state_dir)
            _mpr = (envelope or {}).get("pr_url") if isinstance(envelope, dict) else None
            _mpr_norm = _mpr if isinstance(_mpr, str) and _mpr.strip() else None
            _macc = (envelope or {}).get("acceptance_evidence") \
                if isinstance(envelope, dict) else None
            _macc_norm = _macc if isinstance(_macc, str) and _macc.strip() else None
            _mirror_result(state_dir, request_id,
                json.dumps({"request_id": request_id, "status": "succeeded",
                            "result": {"ok": True, "output": (envelope or {}).get("output"),
                                       "artifact": (envelope or {}).get("artifact"),
                                       "pr_url": _mpr_norm,
                                       "acceptance_evidence": _macc_norm,
                                       "head_commit": head_commit}}),
            )
        except OSError:
            pass
    return {"action": applied, "invocation_id": inv["invocation_id"], "rc": rc}


def _capture_invocation_sessions(state_dir, request_id: str,
                                 invocations: list[dict] | None = None) -> bool:
    """Read durable output files of live invocations and persist any IDs found."""
    if invocations is None:
        invocations = _any_live_invocation(state_dir, request_id)
    if not invocations:
        return False
    # Also scan cancelling invocations that may have emitted IDs before being stopped.
    seen_ids = {inv["invocation_id"] for inv in invocations}
    cancelling = [
        inv for inv in _list_invocations(state_dir, request_id, state="cancelling")
        if inv["invocation_id"] not in seen_ids
    ]
    invocations = invocations + cancelling
    job = get_job(state_dir, request_id)
    captured = False
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        for inv in invocations:
            if inv.get("session_id"):
                captured = True
                continue
            try:
                stdout = Path(inv["stdout_path"]).read_text(encoding="utf-8", errors="replace")
            except OSError:
                stdout = ""
            try:
                stderr = Path(inv["stderr_path"]).read_text(encoding="utf-8", errors="replace")
            except OSError:
                stderr = ""
            sid, skind = _parse_session_from_output(inv["kind"], stdout, stderr, job)
            if sid:
                captured = True
                con.execute(
                    "UPDATE invocations SET session_id=?, session_kind=? WHERE invocation_id=?",
                    (sid, skind, inv["invocation_id"]),
                )
                try:
                    cap_meta = json.loads(inv.get("meta_json") or "{}") or {}
                except ValueError:
                    cap_meta = {}
                harnesses.harness_for(inv["kind"]).record_session(
                    con, request_id, sid, skind, inv["kind"], job, cap_meta, _utcnow())
                _event(con, request_id, "invocation_session_reconciled",
                       {"invocation_id": inv["invocation_id"][:16],
                        "session_kind": skind, "session": sid[:24]})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return captured


def _mark_invocations_cancelling(state_dir, request_id: str) -> None:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "UPDATE invocations SET state='cancelling' WHERE request_id=? AND state='running'",
            (request_id,),
        )
        _event(con, request_id, "invocations_cancelling", {})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _terminate_invocations(state_dir, request_id: str,
                           owner=None, signal_no: int = signal.SIGTERM) -> None:
    """Signal every owned child and supervisor group, and the lease holder.

    Every target is checked against its recorded start identity first, so
    a reused PID of an unrelated process is never signalled.
    """
    for inv in _list_invocations(state_dir, request_id):
        if inv.get("state") not in LIVE_INVOCATION_STATES:
            continue
        inv = _with_child_record(inv)
        _signal_group(inv.get("pgid"), inv.get("pid"), inv.get("process_start"), signal_no)
        _signal_group(inv.get("supervisor_pgid"), inv.get("supervisor_pid"),
                      inv.get("supervisor_start"), signal_no)
        _signal_pid(inv.get("pid"), inv.get("process_start"), signal_no)
        _signal_pid(inv.get("supervisor_pid"), inv.get("supervisor_start"), signal_no)
    if owner is not None:
        _signal_pid(owner.get("owner_pid"), owner.get("owner_start"), signal_no)


def _wait_invocations_dead(state_dir, request_id: str, timeout: float = 10.0) -> bool:
    """Wait until all owned child process groups are confirmed stopped."""
    end = time.monotonic() + max(0.0, timeout)
    while time.monotonic() < end:
        _reconcile_dead_invocations(state_dir, request_id)
        live = _any_live_invocation(state_dir, request_id)
        unresolved = _any_unresolved_invocation(state_dir, request_id)
        if not live and not unresolved:
            consume_finished_invocations(state_dir, request_id)
            return True
        time.sleep(0.1)
    return False


def _raise_if_unowned_invocation(state_dir, request_id: str) -> None:
    """Block a new controller only when ownership cannot be established.

    A live supervised child is adopted by the next controller, which
    waits for that action's durable result instead of starting a copy.
    Unresolved or orphaned ownership still blocks; run recover.
    """
    for inv in _list_invocations(state_dir, request_id):
        own = _invocation_ownership(inv)
        if own in ("unresolved", "orphaned", "never_started"):
            raise OwnershipError(
                f"job {request_id} has {own} invocation {inv['invocation_id']}; run recover")


def _raise_if_live_invocation(state_dir, request_id: str) -> None:
    """Block duplicate launches while any owned child process group lives.

    Read-only with respect to durable metadata: it must not start a write
    transaction because callers already hold the main lease transaction.
    Unresolved NULL-pid ownership also blocks; it is never treated as death.
    """
    unresolved = _any_unresolved_invocation(state_dir, request_id)
    if unresolved:
        raise OwnershipError(
            f"job {request_id} has unresolved invocation {unresolved[0]['invocation_id']}; run recover")
    live = _any_live_invocation(state_dir, request_id)
    if not live:
        return
    has_session = any(inv.get("session_id") for inv in live)
    if has_session:
        raise OwnershipError(
            f"job {request_id} has live invocation {live[0]['invocation_id']}; refusing duplicate launch")
    raise OwnershipError(
        f"job {request_id} has live invocation without handshake; run recover")


def _drain_owned_children(state_dir, request_id: str, owner=None) -> bool:
    """Signal and wait for all owned child process groups to stop.

    Returns True only after every owned process group is confirmed dead.
    """
    _mark_invocations_cancelling(state_dir, request_id)
    _terminate_invocations(state_dir, request_id, owner, signal.SIGTERM)
    if _wait_invocations_dead(state_dir, request_id, timeout=10.0):
        return True
    _terminate_invocations(state_dir, request_id, owner, signal.SIGKILL)
    if _wait_invocations_dead(state_dir, request_id, timeout=5.0):
        return True
    _reconcile_dead_invocations(state_dir, request_id)
    consume_finished_invocations(state_dir, request_id)
    return (not _any_live_invocation(state_dir, request_id)
            and not _any_unresolved_invocation(state_dir, request_id))


def get_job(state_dir, request_id: str) -> dict:
    con = store.connect(state_dir)
    try:
        row = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
    finally:
        con.close()
    if row is None:
        raise NotFoundError(f"unknown request: {request_id}")
    job = _row_to_job(row)
    return job


def _mirror_result(state_dir, request_id: str, text: str) -> None:
    """Write the 0600 result mirror; a failed write is recorded, not hidden.
    The database row stays the source of truth."""
    try:
        store.secure_write_text(store.result_path_for(store.ensure_state_dir(state_dir),
                                                      request_id), text)
    except OSError as e:
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            _event(con, request_id, "result_mirror_failed", {"error": type(e).__name__})
            con.execute("COMMIT")
        except Exception:
            pass
        finally:
            con.close()


def _event(con: sqlite3.Connection, request_id: str, kind: str, payload: dict) -> None:
    safe = store.redact_for_log(dict(payload))
    con.execute(
        "INSERT INTO events(request_id, ts, kind, payload_json, schema_version) VALUES(?,?,?,?,?)",
        (request_id, _utcnow(), kind, json.dumps(safe, sort_keys=True), store.SCHEMA_VERSION),
    )


def proof_owner_path(state_dir, request_id: str):
    """Durable proof-process ownership file for one job."""
    return store.job_dir_for(store.ensure_state_dir(state_dir),
                             request_id) / "proof-owner.json"


def record_proof_owner(state_dir, request_id: str,
                       pid: int | None, pgid: int | None) -> None:
    """Persist the running proof's process group before it runs.

    Lets cancel and recover block on unresolved proof ownership when
    the controller dies mid-proof instead of treating the job as
    stopped while the proof tree keeps running. Records the leader
    start identity with the PID and PGID so a reused group ID never
    reads as alive. Never raises past the caller: an unrecorded proof
    still runs, it just reads as unknown.
    """
    try:
        pid_start = _process_start_identity(pid) if pid is not None else None
    except Exception:
        pid_start = None
    try:
        path = proof_owner_path(state_dir, request_id)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        store.secure_write_text(path, json.dumps(
            {"pid": pid, "pgid": pgid, "pid_start": pid_start,
             "started_at": _utcnow()},
            sort_keys=True))
    except Exception:
        pass


def clear_proof_owner(state_dir, request_id: str) -> None:
    """Forget the proof owner after the proof tree is reaped."""
    try:
        proof_owner_path(state_dir, request_id).unlink()
    except Exception:
        pass


def proof_owner_alive(state_dir, request_id: str) -> bool:
    """True when a recorded proof process group may still be running.

    No proof-owner file means no proof owner is recorded, which reads
    as not alive. A present but unreadable record, or a record without
    group identities, is ambiguous ownership and reads alive: an
    unproven owner is never treated as safely dead, and cancel and
    recover retain the workspace claim until ownership resolves. A
    live group blocks cancel and recover until it is reaped. PID reuse
    protection: the leader start identity is verified, so a reused
    group ID never reads as alive. A dead leader with a live group
    still reads alive (leftover members need draining); a mismatched
    leader proven to sit in the same recorded group proves reuse and
    reads dead.
    """
    try:
        raw = proof_owner_path(state_dir, request_id).read_text(
            encoding="utf-8", errors="replace")
    except OSError:
        return False
    try:
        rec = json.loads(raw or "null")
    except ValueError:
        return True
    if not isinstance(rec, dict):
        return True
    pgid = rec.get("pgid")
    if pgid is None:
        return True
    if not _is_pgid_alive(pgid):
        return False
    pid = rec.get("pid")
    pid_start = rec.get("pid_start")
    if pid is None:
        return True
    try:
        state = _identity_state(pid, pid_start)
    except Exception:
        return True
    if state == "match":
        return True
    if state == "dead":
        return True
    if state == "mismatch":
        try:
            if _pgid_for_pid(pid) == int(pgid):
                return False
        except (TypeError, ValueError):
            pass
        return True
    return True


def _drain_proof_group(state_dir, request_id: str) -> bool:
    """Signal and wait for the recorded proof process group to stop.

    Uses the stored start identity so a reused PID or PGID is never
    signalled. Returns True only after the proof owner reads dead. A
    missing file means nothing to drain. An unreadable record or one
    without group identities cannot be drained and returns False, so
    the workspace claim is retained until ownership resolves.
    """
    try:
        raw = proof_owner_path(state_dir, request_id).read_text(
            encoding="utf-8", errors="replace")
    except OSError:
        return True
    try:
        rec = json.loads(raw or "null")
    except ValueError:
        return False
    if not isinstance(rec, dict) or rec.get("pgid") is None:
        return False
    if not proof_owner_alive(state_dir, request_id):
        return True
    _signal_group(rec.get("pgid"), rec.get("pid"), rec.get("pid_start"),
                  signal.SIGTERM)
    end = time.monotonic() + 5.0
    while time.monotonic() < end:
        if not proof_owner_alive(state_dir, request_id):
            return True
        time.sleep(0.1)
    _signal_group(rec.get("pgid"), rec.get("pid"), rec.get("pid_start"),
                  signal.SIGKILL)
    end = time.monotonic() + 2.0
    while time.monotonic() < end:
        if not proof_owner_alive(state_dir, request_id):
            return True
        time.sleep(0.1)
    return not proof_owner_alive(state_dir, request_id)


def record_verification_attempt(state_dir, request_id: str, seq,
                                  route: str | None, proof_command: str | None,
                                  proof_rc: int | None, proof_class: str | None,
                                  started_at: str | None, ended_at: str | None,
                                  report_path: str | None,
                                  proof_log: str | None) -> None:
    """Persist one executed verification attempt as an observable row.

    The task's own proof run becomes a durable invocation row with
    ``stage='verification'`` (kind ``proof``), its start/end timestamps,
    exit code, proof class, and elapsed time, so Agent Observer can
    measure verification outcomes from the existing invocation records
    instead of the report files it never reads. The row is inserted
    already consumed with its measurement: it is evidence, never work
    to rerun, and recovery never replays it. Never raises past the
    caller: a missed row leaves the report file itself, never a
    fabricated attempt.
    """
    try:
        started = _parse_ts(started_at) if started_at else None
    except Exception:
        started = None
    try:
        ended = _parse_ts(ended_at) if ended_at else None
    except Exception:
        ended = None
    elapsed = round(max(0.0, ended - started), 3) \
        if started is not None and ended is not None else None
    now = _utcnow()
    invocation_id = secrets.token_hex(8)
    try:
        job = get_job(state_dir, request_id)
        owner_token = job.get("owner_token") or ""
        workspace = job.get("workspace") or ""
        _proof_cancel_intent = bool(job.get("cancel_requested"))
    except Exception:
        owner_token, workspace, _proof_cancel_intent = "", "", False
    try:
        tclass = terminal_class_for(proof_rc, None,
                                    cancel_requested=_proof_cancel_intent)
    except Exception:
        tclass = "failed" if proof_rc else "completed"
    try:
        seq_i = int(seq) if seq is not None else None
    except (TypeError, ValueError):
        seq_i = None
    meta = {"seq": seq_i, "route": route, "stage": "verification",
            "proof_class": proof_class, "report": report_path}
    log = str(proof_log or "")
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "INSERT INTO invocations("
            " invocation_id, request_id, kind, cmd_json, workspace, owner_token,"
            " stdout_path, stderr_path, started_at, ended_at, state, rc,"
            " consumed_at, result_json, meta_json, action_key,"
            " stage, requested_route, policy_version, reason, schema_version,"
            " terminal_class, elapsed_secs, report_path"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (invocation_id, request_id, "proof",
             json.dumps(["/bin/sh", "-c", proof_command or ""]),
             workspace, owner_token, log, log,
             started_at or now, ended_at or now,
             "completed" if proof_rc == 0 else "failed", proof_rc, now,
             json.dumps({"proof_class": proof_class, "seq": seq_i},
                        sort_keys=True),
             json.dumps(meta, sort_keys=True), f"proof:{seq_i}",
             "verification", route, policy.POLICY_VERSION,
             proof_class or "unknown", store.SCHEMA_VERSION,
             tclass, elapsed,
             str(report_path or "")),
        )
        _event(con, request_id, "verification_attempt",
               {"seq": seq_i, "route": route, "rc": proof_rc,
                "proof_class": proof_class})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def record_recovery_decision(state_dir, request_id: str, failures: int,
                             rung: str, target: str | None, failed_seq,
                             reason: str,
                             lease_token: str | None = None) -> dict:
    """Persist one recovery decision linking the failed attempt onward.

    The decision records which attempt failed (``failed_seq``, the
    ladder's counted seq), the rung chosen for the correction, the route
    target (None for a same-route correction), and the reason. The next
    attempt seq is unknown at decision time (planner questions, refusals,
    and completion envelopes may consume seq numbers first), so
    ``next_attempt_seq`` stays None here: the controller emits a
    ``recovery_next_attempt`` event with the actual seq when the next
    worker invocation starts, and a ``recovery_attempt_result`` event
    when that attempt's outcome is recorded. ``failed_at`` comes from
    the failed turn's own proof timestamps when the report carries
    them, else unknown: Observer joins these events to the next
    attempt's report and invocation rows to measure failure-to-restart
    and recovery success. Returns the payload.
    """
    try:
        failed_i = int(failed_seq) if failed_seq is not None else None
    except (TypeError, ValueError):
        failed_i = None
    failed_at = None
    try:
        report = latest_turn_report(state_dir, request_id)
        if isinstance(report, dict) and (failed_i is None or report.get("seq") == failed_i):
            failed_at = report.get("proof_ended_at") or report.get("proof_started_at")
    except Exception:
        failed_at = None
    decided_at = _utcnow()
    payload = {"failures": int(failures), "rung": rung,
               "target": target, "failed_seq": failed_i,
               "next_attempt_seq": None,
               "failed_at": failed_at, "decided_at": decided_at,
               "reason": reason}
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        if lease_token is not None:
            _check_lease_locked(con, request_id, lease_token)
        _event(con, request_id, "recovery_decision", payload)
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return payload


def exhaustion_context(state_dir, request_id: str) -> dict:
    """Evidence summary for an exhausted escalation, from existing records.

    Returns ``{"rungs": [...], "routes_tried": [...], "reports": [...],
    "proof": {...}|None}``: the ladder rungs used so far, the routes
    with turn counts, the turn reports (seq, route, status,
    failure class, proof exit, path), and the latest proof outcome.
    Everything derives from the ledger and the report files; missing
    pieces stay absent, never invented.
    """
    ctx: dict = {"rungs": [], "routes_tried": [], "reports": [], "proof": None}
    try:
        job = get_job(state_dir, request_id)
    except NotFoundError:
        return ctx
    try:
        ladder = json.loads(job.get("controller_state") or "{}").get("ladder") or {}
    except ValueError:
        ladder = {}
    if isinstance(ladder, dict):
        if ladder.get("rung"):
            ctx["rungs"].append(str(ladder["rung"]))
        if ladder.get("failures") is not None:
            ctx["failures"] = ladder["failures"]
    try:
        root = store.ensure_state_dir(state_dir)
        job_dir = store.job_dir_for(root, request_id)
        paths = sorted(job_dir.glob("turn-*/report.json")) if job_dir.exists() else []
    except Exception:
        paths = []
    for path in paths:
        try:
            rep = json.loads(Path(path).read_text(encoding="utf-8", errors="replace"))
        except (OSError, ValueError):
            continue
        if not isinstance(rep, dict):
            continue
        ctx["reports"].append({
            "seq": rep.get("seq"), "route": rep.get("route"),
            "status": rep.get("status"),
            "failure_class": rep.get("failure_class"),
            "proof_exit_code": rep.get("proof_exit_code"),
            "report": str(path),
        })
    routes: dict = {}
    for rep in ctx["reports"]:
        route = rep.get("route")
        if route:
            routes[route] = routes.get(route, 0) + 1
    ctx["routes_tried"] = sorted(routes.items())
    try:
        latest = latest_turn_report(state_dir, request_id)
    except Exception:
        latest = None
    if isinstance(latest, dict):
        ctx["proof"] = {"exit_code": latest.get("proof_exit_code"),
                        "class": latest.get("proof_class"),
                        "report": latest.get("report_path")}
    return ctx


# ---------------------------------------------------------------------------
# Measurement (Agent Observer compatible)
# ---------------------------------------------------------------------------

JOB_KINDS = ("ordinary", "experiment", "replay")
# The planner callback resumes the saved planner session in its own
# harness (Claude --resume, Codex exec resume, OpenCode run --session);
# a harness without a usable resume path answers from a fresh session
# seeded with the stored handoff summary.
PLANNER_HARNESSES = ("claude", "codex", "opencode", "grok")


def _workspace_head(workspace: str | None) -> str | None:
    """HEAD of the workspace when it is a Git checkout, else None."""
    if not workspace:
        return None
    try:
        out = subprocess.run(["git", "-C", workspace, "rev-parse", "HEAD"], capture_output=True,
                             text=True, timeout=5, stdin=subprocess.DEVNULL)
    except Exception:
        return None
    sha = (out.stdout or "").strip()
    return sha if out.returncode == 0 and len(sha) >= 7 else None


def harness_version_for(kind: str) -> str | None:
    """``<binary> --version`` once per process for the harness behind a kind."""
    return harnesses.harness_for(kind).version()

def terminal_class_for(rc, result_obj, crashed: bool = False,
                       cancel_requested: bool = False) -> str:
    """Terminal class for one finished invocation, for Observer measurement.

    An rc143 (or -15) stop reads ``cancelled`` only when the owning job
    carries explicit cancellation intent (``cancel_requested``). Without
    that intent a stop is ``infrastructure`` when the result carries stop
    evidence and ``unknown`` when it carries none: the exit code alone
    never invents a cancellation. Intentional cancellation stays a job
    property, separate from ordinary failure reporting.
    """
    if crashed:
        return "crashed"
    signal_name = None
    startup_error = ""
    if isinstance(result_obj, dict):
        signal_name = result_obj.get("signal")
        if signal_name is None and isinstance(result_obj.get("envelope"), dict):
            signal_name = result_obj["envelope"].get("signal")
        for container in (result_obj,
                          result_obj.get("envelope")
                          if isinstance(result_obj.get("envelope"), dict) else {}):
            if not isinstance(container, dict):
                continue
            err = container.get("error")
            if isinstance(err, str) and err:
                startup_error += " " + err
    if signal_name == "exhausted":
        return "quota"
    if signal_name == "overloaded":
        return "overloaded"
    if signal_name == "stalled":
        return "stalled"
    if signal_name == "context":
        return "context"
    if signal_name == "hard":
        return "hard_error"
    if rc == 0:
        return "completed"
    if rc == 124:
        # A supervisor-level rc124 with no proof run (an OpenCode startup
        # failure) is infrastructure, never a proof timeout: the suite
        # never ran, so nothing timed out. A proof that ran past its
        # budget carries no such marker and stays timeout. This keeps the
        # invocation row class aligned with the turn report's
        # failure_class for the same attempt.
        if ("did not emit a localhost URL" in startup_error
                or "exited before emitting" in startup_error
                or "terminated during server startup" in startup_error):
            return "infrastructure"
        return "timeout"
    if rc in (143, -15):
        if cancel_requested:
            return "cancelled"
        if isinstance(result_obj, dict) and result_obj:
            return "infrastructure"
        return "unknown"
    return "failed"


# Failure taxonomy for issue 87 (toolboxmd/model-router#87). Turn-level
# classes Agent Observer measures: timeout (a turn or proof that ran out
# of time), stall (stream silence past the harness window), provider (a
# capacity signal: exhausted, overloaded, or context pressure),
# infrastructure (a hard provider error, a missing runtime, a lost
# supervisor, or an unconfirmed stop), implementation (the worker ran and
# errored without a capacity signal), verification (the task's own proof
# ran and failed). Intentional cancellation is separate and lives on the
# job, never on a turn report. Missing causes stay unknown: this helper
# never invents one.
FAILURE_CLASSES = ("timeout", "stall", "provider", "infrastructure",
                   "implementation", "verification", "cancelled", "unknown")

# Proof-attempt classes for one verification run. ``timeout`` (the proof
# ran past its budget and its whole process tree was stopped) is distinct
# from ``not_found`` (the proof executable was missing, rc 127).
# ``skipped`` means the runner deliberately did not run the suite for an
# exhausted, stalled, crashed, or otherwise incomplete turn; the report's
# ``proof_skipped`` names the reason truthfully.
PROOF_CLASSES = ("pass", "failed", "timeout", "not_found", "skipped",
                 "none", "error")


def failure_class_for(signal=None, proof_class=None, rc=None,
                      runtime_missing: bool = False,
                      cancelled: bool = False) -> str:
    """Classify one failed turn or verification attempt for the ledger.

    Precedence follows evidence strength: an explicit cancellation
    intent first (kept separate from every failure), then the harness
    signal, then the proof outcome, then the raw exit code. Anything
    without evidence is ``unknown``, never a guess.
    """
    if cancelled:
        return "cancelled"
    if runtime_missing or signal == "hard":
        return "infrastructure"
    if signal == "stalled":
        return "stall"
    if signal in ("exhausted", "overloaded", "context"):
        return "provider"
    if proof_class == "timeout" or rc == 124:
        return "timeout"
    if proof_class in ("failed", "not_found", "error"):
        return "verification"
    if rc is not None and rc != 0:
        if rc in (143, -15, 125):
            # An unconfirmed stop or a lost supervisor: ownership is
            # ambiguous, so this is infrastructure, never a timeout and
            # never an intentional cancellation (cancellation intent
            # lives on the job's cancel_requested flag).
            return "infrastructure"
        return "implementation"
    return "unknown"


def _runner_result_line(stdout: str) -> dict:
    found = {}
    for line in (stdout or "").splitlines():
        if line.startswith("RUNNER_RESULT "):
            try:
                found = json.loads(line[len("RUNNER_RESULT "):])
            except ValueError:
                continue
    return found if isinstance(found, dict) else {}


def measure_output(kind: str, stdout: str, stderr: str, meta: dict | None = None,
                   inv_ctx: dict | None = None) -> tuple:
    """(usage, observed_model, observed_variant, native_ids) from the harness.

    Counters are copied verbatim under a ``source`` label and never folded;
    a harness that reports nothing leaves usage None. Model and variant stay
    separate; the harness seam is the single place that knows them.
    ``inv_ctx`` carries request/invocation identity (request_id,
    invocation_id, state_dir) for harnesses that read their own records
    (Codex rollouts); it never enters public measurement fields."""
    measured = harnesses.harness_for(kind).measure(kind, stdout, stderr, meta or {}, inv_ctx)
    if len(measured) == 3:
        usage, observed, ids = measured
        return usage, observed, None, ids
    return measured

def _compute_invocation_measurement(state_dir, request_id: str, invocation_id: str,
                                     kind: str, stdout: str, stderr: str,
                                     meta: dict, started_at, ended_at,
                                     rc, result_obj, crashed: bool = False) -> dict:
    """Full measurement values for one finished invocation, without any write.

    Shared by the live ``_measure_invocation`` path and recovery, so both
    persist the same fields: elapsed time, terminal class, usage, observed
    model and variant, native identities, longest silence, tools, and
    skills. Expected missing or unreadable rollout cases stay unknown
    inside the harness file-read boundary; any other observation failure
    propagates so the caller blocks safely.
    """
    inv_ctx = {"state_dir": state_dir, "request_id": request_id,
               "invocation_id": invocation_id}
    usage, observed, variant, ids = measure_output(kind or "", stdout, stderr, meta or {},
                                                   inv_ctx)
    started = _parse_ts(started_at)
    ended = _parse_ts(ended_at) or time.time()
    elapsed = round(max(0.0, ended - started), 3) if started is not None else None
    try:
        _cancel_intent = bool(get_job(state_dir, request_id).get("cancel_requested"))
    except Exception:
        _cancel_intent = False
    tclass = terminal_class_for(rc, result_obj, crashed=crashed,
                                cancel_requested=_cancel_intent)
    longest = None
    if isinstance(result_obj, dict):
        # Owned-server turns nest the drive result under ``envelope``; CLI
        # turns carry it top-level. Either way the longest observed silence
        # lands on the invocation row for window tuning.
        raw_longest = result_obj.get("longest_silence_secs")
        if raw_longest is None and isinstance(result_obj.get("envelope"), dict):
            raw_longest = result_obj["envelope"].get("longest_silence_secs")
        try:
            longest = float(raw_longest) if raw_longest is not None else None
        except (TypeError, ValueError):
            longest = None
        if longest is not None and longest < 0:
            longest = None
    tools: list = []
    try:
        if isinstance(result_obj, dict):
            for container in (result_obj,
                              result_obj.get("envelope") if isinstance(result_obj.get("envelope"), dict) else {}):
                if not isinstance(container, dict):
                    continue
                for key in ("tools_called", "tools"):
                    val = container.get(key)
                    if isinstance(val, list) and all(isinstance(v, str) for v in val):
                        tools = sorted(set(tools) | set(val))
                        break
    except Exception:
        pass
    try:
        tools_json = json.dumps(tools, sort_keys=True)
    except Exception:
        tools_json = "[]"
    try:
        from . import kits as _kits_meas
        _observed = _kits_meas.observed_skills_for_invocation(
            state_dir, request_id, invocation_id)
    except Exception:
        _observed = None
    try:
        _skills_json = json.dumps(_observed, sort_keys=True) if isinstance(_observed, list) else None
    except Exception:
        _skills_json = None
    return {"elapsed": elapsed, "terminal_class": tclass, "usage": usage,
            "observed": observed, "variant": variant, "ids": ids,
            "longest": longest, "tools_json": tools_json,
            "skills_json": _skills_json}


def _write_invocation_measurement(con, invocation_id: str, m: dict) -> None:
    """Persist a computed measurement on an open connection, no commit."""
    con.execute(
        "UPDATE invocations SET elapsed_secs=?, terminal_class=?, usage_json=?,"
        " observed_model=COALESCE(?, observed_model),"
        " observed_variant=COALESCE(?, observed_variant),"
        " native_ids_json=?, schema_version=?,"
        " longest_silence_secs=COALESCE(?, longest_silence_secs),"
        " tools_json=COALESCE(NULLIF(tools_json,'[]'), ?)"
        " WHERE invocation_id=?",
        (m["elapsed"], m["terminal_class"],
         json.dumps(m["usage"], sort_keys=True) if m["usage"] is not None else None,
         m["observed"], m["variant"],
         json.dumps(m["ids"], sort_keys=True) if m["ids"] else None,
         store.SCHEMA_VERSION, m["longest"], m["tools_json"], invocation_id))
    if m["skills_json"] is not None:
        con.execute(
            "UPDATE invocations SET skills_json=? WHERE invocation_id=?",
            (m["skills_json"], invocation_id))
    # First measurement wins for tools when the row still holds the
    # insert-time empty list; later measures keep observed tools.
    con.execute(
        "UPDATE invocations SET tools_json=? WHERE invocation_id=?"
        " AND (tools_json IS NULL OR tools_json='[]')",
        (m["tools_json"], invocation_id))


def _measure_invocation(state_dir, request_id: str, invocation_id: str, crashed: bool = False) -> None:
    """Fill elapsed time, terminal class, usage, observed model and variant,
    and native identities on a finished invocation from its own files."""
    rows = [i for i in _list_invocations(state_dir, request_id) if i["invocation_id"] == invocation_id]
    if not rows:
        return
    inv = rows[0]
    stdout, stderr = _read_invocation_output(inv)
    try:
        result_obj = json.loads(inv.get("result_json") or "null")
    except ValueError:
        result_obj = None
    try:
        meta = json.loads(inv.get("meta_json") or "{}") or {}
    except ValueError:
        meta = {}
    m = _compute_invocation_measurement(
        state_dir, request_id, invocation_id, inv.get("kind") or "",
        stdout, stderr, meta, inv.get("started_at"), inv.get("ended_at"),
        inv.get("rc"), result_obj, crashed=crashed)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _write_invocation_measurement(con, invocation_id, m)
        con.execute("COMMIT")
    except Exception:
        # A failed observed_model write must never read as unknown and let a
        # completion through: roll back and propagate so the live controller
        # and recovery paths block with evidence before any envelope.
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def invocation_measurements(state_dir, request_id: str) -> list[dict]:
    """Per-invocation measurements for ``status`` and ``result``.

    Agent Observer mapping: kit identity and hash, supply mechanism
    (hook, runner, none), hash of the supplied block, skills loaded, and
    tools called, alongside the existing route, model, and usage fields.
    """
    out = []
    for inv in _list_invocations(state_dir, request_id):
        try:
            usage = json.loads(inv.get("usage_json") or "null")
        except ValueError:
            usage = None
        try:
            native_ids = json.loads(inv.get("native_ids_json") or "null")
        except ValueError:
            native_ids = None
        try:
            skills = json.loads(inv.get("skills_json") or "null")
        except ValueError:
            skills = None
        if not isinstance(skills, list):
            # Backfill from the kit row when the invocation predates the
            # skills column: the kit's skills are what the session loaded.
            skills = None
            try:
                from . import direction as _direction
                _kn, _kh, _sk = _direction.kit_for_invocation(
                    inv.get("requested_route"), inv.get("stage"))
                skills = _sk
            except Exception:
                skills = []
        # Observed wins when a materialized kit recorded it: the ledger
        # reports what the session could actually invoke (the kit dir's
        # skills/*/SKILL.md names plus OpenCode's built-in on the owned
        # server), not the policy list.
        try:
            from . import kits as _kits_obs
            _obs = _kits_obs.observed_skills_for_invocation(
                state_dir, request_id, inv.get("invocation_id") or "")
            if isinstance(_obs, list):
                skills = _obs
        except Exception:
            pass
        try:
            tools = json.loads(inv.get("tools_json") or "null")
        except ValueError:
            tools = None
        if not isinstance(tools, list):
            tools = []
        try:
            from . import kits as _kits
            _kit_contents = None
            _kit_auth = {"linked": [], "missing": []}
            try:
                _dirs = _kits.kit_dirs_for_invocation(
                    state_dir, request_id, inv.get("invocation_id") or "")
            except Exception:
                _dirs = []
            for _d in _dirs or []:
                try:
                    _c = _kits.kit_contents_for_ledger(_d)
                except Exception:
                    _c = None
                if isinstance(_c, dict):
                    _kit_contents = _c
                    _auth = _c.get("auth")
                    if isinstance(_auth, dict):
                        _kit_auth = {"linked": [v for v in (_auth.get("linked") or [])
                                                if isinstance(v, str)],
                                     "missing": [v for v in (_auth.get("missing") or [])
                                                 if isinstance(v, str)]}
                    break
        except Exception:
            _kit_contents = None
            _kit_auth = {"linked": [], "missing": []}
        out.append({"invocation": inv["invocation_id"][:8], "kind": inv.get("kind"),
                    "stage": inv.get("stage"), "requested_route": inv.get("requested_route"),
                    "policy_version": inv.get("policy_version"), "reason": inv.get("reason"),
                    "observed_model": inv.get("observed_model"),
                    "observed_variant": inv.get("observed_variant"),
                    "terminal_class": inv.get("terminal_class"), "elapsed_secs": inv.get("elapsed_secs"),
                    "usage": usage, "native_ids": native_ids,
                    "harness_version": inv.get("harness_version"),
                    "schema_version": inv.get("schema_version"),
                    "report_path": inv.get("report_path"), "started_at": inv.get("started_at"),
                    "ended_at": inv.get("ended_at"),
                    "longest_silence_secs": inv.get("longest_silence_secs"),
                    "kit": inv.get("kit"), "kit_hash": inv.get("kit_hash"),
                    "supply": inv.get("direction_supply"),
                    "supply_reason": inv.get("direction_reason"),
                    "direction_hash": inv.get("direction_hash"),
                    "direction_status": inv.get("direction_status"),
                    "skills_loaded": skills, "tools_called": tools,
                    "kit_contents": _kit_contents, "kit_auth": _kit_auth})
    return out


def _derive_handoff_summary(task, request_id: str,
                            handoff_summary: str | None = None) -> str:
    """Durable handoff summary stored on the job.

    An explicit summary wins; a task packet carrying ``handoff_summary``
    is next; otherwise the summary is derived from the packet's Issue,
    decisions, proof command, and goal so a callback can be answered from
    the ledger alone, including by the Astra fallback in a fresh session.
    Free-text secrets are masked before persisting.
    """
    if isinstance(handoff_summary, str) and handoff_summary.strip():
        return adapters.redact_text(handoff_summary.strip())
    obj = None
    if isinstance(task, dict):
        obj = task
    elif isinstance(task, str):
        try:
            obj = json.loads(task)
        except ValueError:
            obj = None
    if isinstance(obj, dict):
        explicit = obj.get("handoff_summary")
        if isinstance(explicit, str) and explicit.strip():
            return adapters.redact_text(explicit.strip())
        issue = obj.get("issue", obj.get("issue_id", obj.get("github_issue", "")))
        decisions = obj.get("decisions", obj.get("decision", obj.get("notes", "")))
        proof = obj.get("proof", obj.get("proof_command", ""))
        goal = obj.get("goal", obj.get("objective", obj.get("task", "")))
        parts = [f"request {request_id}"]
        if isinstance(issue, (str, int)) and str(issue).strip():
            parts.append(f"Issue {str(issue).strip()}")
        if isinstance(goal, str) and goal.strip():
            parts.append(f"goal: {goal.strip()}")
        if isinstance(decisions, str) and decisions.strip():
            parts.append(f"decisions: {decisions.strip()}")
        elif isinstance(decisions, list) and decisions:
            parts.append("decisions: " + "; ".join(str(d) for d in decisions))
        if isinstance(proof, str) and proof.strip():
            parts.append(f"proof: {proof.strip()}")
        if len(parts) > 1:
            return adapters.redact_text("\n".join(parts))
    try:
        canonical = _canonical_task(task)
    except Exception:
        canonical = str(task)
    return adapters.redact_text(canonical)


def submit(state_dir, request_id: str, task, workspace: str,
           planner_session_id: str, route: str | None = None,
           policy_id: str | None = None, max_attempts: int = 5,
           timeout_secs: int | None = None,
           planner_model: str | None = None,
           planner_effort: str | None = None,
           lane: str | None = None,
           job_kind: str = "ordinary", replay_of: str | None = None,
           planner_harness: str = "claude",
           handoff_summary: str | None = None) -> dict:
    """Persist a prepared task before acknowledging acceptance.

    Built-in defaults mean no executor/callback commands are required:
    only the prepared task, workspace, stable request ID, and original
    planner session ID are needed. The planner session may live in any
    supported harness (claude, codex, opencode, grok); its harness and
    session ID are recorded so dispatcher questions wake it there.
    Planner model/effort default to the policy's planning route and may
    be overridden explicitly. The handoff summary is stored on the job
    (explicit argument wins, else derived from the task packet) so a
    callback prompt can carry it. Planner callbacks run in the job
    workspace. ``timeout_secs`` is a legacy compatibility slot, recorded
    but never enforced: jobs carry no age deadline since #88.
    """
    _validate_request_id(request_id)
    if not planner_session_id:
        raise ValueError("missing planner session ID")
    # An existing request is compared first, so an identical resubmission
    # returns it even if its directory was removed since. Sticky-home
    # selection runs only for a new request, inside the same write
    # transaction that records the route, so the count and the insert are
    # atomic.
    ws = _canonical_workspace(workspace, must_exist=False)
    # The lane picks an implementation route unless an explicit route is
    # given; the critical lane is planner-executed and raises here so the
    # planner keeps that step.
    explicit_route = route
    if lane:
        lane_route = policy.lane_default_route(lane)
    else:
        lane_route = None
    if explicit_route is not None:
        policy.validate_implementation_route(explicit_route)
        # The job remembers its lane: Muse sits in two lanes, so later moves
        # must not guess from the route alone. An explicit route must belong
        # to the explicit lane (initial submit never starts in recovery, even
        # for dual-listed Grok rungs).
        if lane is not None:
            req_stage = policy.resolve_lane(lane)
            if req_stage in policy.IMPLEMENTATION_LANES \
                    and explicit_route not in policy.STAGES[req_stage]["routes"]:
                raise ValueError(f"route {explicit_route!r} is not in lane {lane!r}")
            explicit_lane_stage = req_stage
        else:
            explicit_lane_stage = policy.lane_of_route(explicit_route, None)
    else:
        explicit_lane_stage = None
    pid = policy_id or policy.POLICY_ID
    if not pid:
        raise ValueError("missing policy identity")
    if max_attempts < 1 or max_attempts > 10:
        raise ValueError("max_attempts must be 1..10")
    if job_kind not in JOB_KINDS:
        raise ValueError(f"job_kind must be one of {', '.join(JOB_KINDS)}")
    if job_kind == "replay" and not replay_of:
        raise ValueError("a replay names the request it replays (replay_of)")
    if job_kind != "replay" and replay_of:
        raise ValueError("replay_of applies to replay jobs only")
    if planner_harness not in PLANNER_HARNESSES:
        raise ValueError(f"planner_harness must be one of {', '.join(PLANNER_HARNESSES)}")
    # Planner defaults come from the first route in the policy's planning
    # stage; explicit overrides are preserved verbatim. Never fork a new
    # session implicitly.
    _planning_route = policy.STAGES["planning"]["routes"][0]
    _planning_spec = policy.route_spec(_planning_route)
    _pm_default = _planning_spec["model"]
    _pe_default = _planning_spec["variant"]
    planner_model = planner_model or _pm_default
    planner_effort = planner_effort or _pe_default
    task_json = _canonical_task(task)
    thash = _task_hash(task_json)
    summary = _derive_handoff_summary(task, request_id, handoff_summary)
    pcwd = ws
    root = store.ensure_state_dir(state_dir)
    out_path = str(store.output_path_for(root, request_id))
    now = _utcnow()
    executor_session = "exec-" + secrets.token_hex(8)
    base_commit = _workspace_head(ws)  # before the write lock
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        existing = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if existing is not None:
            if explicit_route is not None:
                same_route = (existing["route"] == explicit_route)
                same_lane = (existing["lane"] is None
                             or existing["lane"] == explicit_lane_stage)
            else:
                # Sticky resubmission: the stored route wins; running counts
                # must not turn an identical request into a conflict.
                same_route = True
                if lane is not None:
                    req_stage = policy.resolve_lane(lane)
                else:
                    req_stage = policy.resolve_lane(policy.DEFAULT_LANE)
                same_lane = (existing["lane"] is None
                             or existing["lane"] == req_stage)
            same = (
                existing["task_hash"] == thash
                and existing["workspace"] == ws
                and existing["planner_session_id"] == planner_session_id
                and existing["policy_id"] == pid
                and same_route
                and int(existing["max_attempts"]) == int(max_attempts)
                and existing["timeout_secs"] == timeout_secs
            )
            # New fields participate when already persisted (NULL = wildcard
            # for pre-bridge rows so baseline idempotency is preserved).
            try:
                _em = existing["planner_model"]
                _ee = existing["planner_effort"]
            except Exception:
                _em, _ee = None, None
            if _em is not None and _em != planner_model:
                same = False
            if _ee is not None and _ee != planner_effort:
                same = False
            # The planner_cwd column stays readable for old ledgers, but no
            # longer participates: callbacks run in the job workspace.
            if not same_lane:
                same = False
            if existing["job_kind"] is not None and (
                    existing["job_kind"] != job_kind or existing["replay_of"] != replay_of
                    or existing["planner_harness"] != planner_harness):
                same = False
            try:
                _hs = existing["handoff_summary"]
            except Exception:
                _hs = None
            if _hs is not None and _hs != summary:
                same = False
            # An explicit route that differs is a different payload; a sticky
            # resubmission with the same payload returns the stored job.
            if explicit_route is not None and existing["route"] != explicit_route:
                same = False
            con.execute("ROLLBACK")
            if same:
                # Pre-#38 rows store NULL: backfill the derived summary so
                # later callbacks carry HANDOFF SUMMARY.
                if _hs is None and summary:
                    try:
                        _con2 = store.connect(state_dir)
                        try:
                            _con2.execute("BEGIN IMMEDIATE")
                            _con2.execute(
                                "UPDATE jobs SET handoff_summary=?, updated_at=? WHERE request_id=?",
                                (summary, _utcnow(), request_id))
                            _con2.execute("COMMIT")
                        except Exception:
                            try:
                                _con2.execute("ROLLBACK")
                            except Exception:
                                pass
                        finally:
                            _con2.close()
                    except Exception:
                        pass
                    try:
                        return get_job(state_dir, request_id)
                    except Exception:
                        return _row_to_job(existing)
                return _row_to_job(existing)
            raise ConflictError(f"request ID {request_id!r} already used with a different payload")
        # New request: sticky-home selection inside the same write
        # transaction that records the route, counting running jobs on each
        # route inside the transaction (atomic reservation).
        if explicit_route is not None:
            route = explicit_route
            lane_stage = explicit_lane_stage
        else:
            # Muse on Zen free takes every new job (no concurrency cap; skipped
            # only when exhausted or degraded); the `fewest running jobs`
            # spread applies only among capped routes. A job walks the lane
            # only on evidence. A new job never starts
            # in recovery, even for dual-listed Grok rungs.
            if lane:
                route = _sticky_home_locked(con, lane) or lane_route
                lane_stage = policy.resolve_lane(lane)
            else:
                route = _sticky_home_locked(con, policy.DEFAULT_LANE) \
                    or policy.lane_default_route(policy.DEFAULT_LANE)
                lane_stage = policy.resolve_lane(policy.DEFAULT_LANE)
            route = route or policy.lane_default_route(policy.DEFAULT_LANE)
            policy.validate_implementation_route(route)
        try:
            _canonical_workspace(ws)  # a new job needs an existing workspace
        except ValueError:
            con.execute("ROLLBACK")
            raise
        # A workspace stays claimed while any job on it (or on a nested or
        # enclosing path) is active or cancelling, or still owns a child.
        placeholders = ",".join("?" for _ in store.ACTIVE_WORKSPACE_STATUSES)
        clash = None
        for other in con.execute(
                f"SELECT request_id, workspace, status FROM jobs WHERE status IN ({placeholders})",
                store.ACTIVE_WORKSPACE_STATUSES).fetchall():
            if _paths_overlap(other["workspace"], ws):
                clash = other
                break
        if clash is None:
            for other in con.execute(
                    "SELECT DISTINCT i.request_id, j.workspace FROM invocations i"
                    " JOIN jobs j ON j.request_id=i.request_id"
                    " WHERE i.state IN ('running','cancelling')").fetchall():
                if _paths_overlap(other["workspace"], ws) and any(
                        _invocation_ownership(inv) in ("live", "unresolved", "orphaned")
                        for inv in _list_invocations(state_dir, other["request_id"])):
                    clash = other
                    break
        if clash is not None:
            con.execute("ROLLBACK")
            raise WorkspaceConflictError(
                f"workspace {ws!r} already owned by {clash['request_id']!r}"
            )
        # Retain the workspace claim after cancel/timeout while the
        # actual child is still alive: any job (even terminal) on the
        # same workspace with a live recorded PID still owns it.
        stale_rows = con.execute(
            "SELECT request_id, workspace, owner_pid, owner_start FROM jobs WHERE owner_pid IS NOT NULL",
        ).fetchall()
        stale_rows = [r for r in stale_rows if _paths_overlap(r["workspace"], ws)]
        for _r in stale_rows:
            try:
                _pid = int(_r["owner_pid"])
            except (TypeError, ValueError):
                continue
            if _owner_alive(_r):
                con.execute("ROLLBACK")
                raise WorkspaceConflictError(
                    f"workspace {ws!r} still claimed by live pid {_pid} "
                    f"({_r['request_id']!r}); refusing duplicate writer"
                )
        con.execute(
            "INSERT INTO jobs(request_id,task_json,task_hash,workspace,policy_id,planner_session_id,"
            "executor_session_id,output_path,status,route,cancel_requested,attempts,max_attempts,timeout_secs,created_at,"
            "updated_at,planner_model,planner_effort,adapter,model,effort,planner_cwd,lane,"
            "job_kind,replay_of,planner_harness,base_commit,handoff_summary)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,0,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (request_id, task_json, thash, ws, pid, planner_session_id,
             executor_session, out_path, "pending", route, max_attempts,
             timeout_secs, now, now,
             planner_model, planner_effort, "controller",
              policy.worker_model_variant(route)[0],
              policy.worker_model_variant(route)[1] or "default", pcwd, lane_stage,
             job_kind, replay_of, planner_harness, base_commit, summary),
        )
        _event(con, request_id, "submitted", {
            "workspace": ws, "policy": pid, "route": route,
            "planner_session": planner_session_id, "task_hash": thash,
            "runtime": runtime.installed_runtime(),
        })
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    # File-backed output created after the durable row commits; the row is
    # the ack. Ensure the file exists with private perms without truncating.
    p = Path(out_path)
    if not p.exists():
        store.secure_write_text(p, "")
    try:
        os.chmod(p, 0o600)
    except OSError:
        pass
    return get_job(state_dir, request_id)


def submit_and_start(state_dir, request_id: str, task, workspace: str,
                     planner_session_id: str, route: str | None = None,
                     policy_id: str | None = None, max_attempts: int = 5,
                     timeout_secs: int | None = None,
                     planner_model: str | None = None,
                     planner_effort: str | None = None,
                     launcher=None, spawn=None,
                     lane: str | None = None,
                     job_kind: str = "ordinary", replay_of: str | None = None,
                     planner_harness: str = "claude",
                     handoff_summary: str | None = None) -> dict:
    """Persist first, then launch the controller.

    The durable row commits before any spawn, so controller death leaves
    recoverable state. ``launcher`` is a legacy test-only seam
    ``(state_dir, request_id) -> dict``; ``spawn`` is the preferred
    test-only seam ``(cmd) -> pid`` that keeps durable attempt rows
    while staying offline. Default spawns a detached controller process
    using built-in adapters.
    """
    job = submit(state_dir, request_id, task, workspace, planner_session_id,
                 route=route, policy_id=policy_id, max_attempts=max_attempts,
                 timeout_secs=timeout_secs, planner_model=planner_model,
                 planner_effort=planner_effort,
                 lane=lane, job_kind=job_kind, replay_of=replay_of,
                 planner_harness=planner_harness, handoff_summary=handoff_summary)
    # Idempotent resubmit of the same payload must not fork a second
    # controller when one already holds the lease or when a durable child
    # process group from a prior controller is still alive.
    if job.get("owner_token") and _owner_alive(job):
        return job
    if job["status"] in store.TERMINAL:
        return job
    # A repeated submission returns the existing job; it never forces a
    # second launch (use recover for ownership reconciliation).
    try:
        _raise_if_unowned_invocation(state_dir, request_id)
        start_controller(state_dir, request_id, launcher=launcher, spawn=spawn)
    except (OwnershipError, BlockedError, TerminalError):
        pass  # the persisted job is the answer; recover reconciles ownership
    return get_job(state_dir, request_id)


def start_controller(state_dir, request_id: str, launcher=None, spawn=None) -> dict:
    """Launch a detached controller using built-in adapters by default.

    Persists the launch attempt before AND after ack (same lease table as
    workers) so a crash between rows is reconciled by recover(). Resume
    always reuses saved Codex/planner/OpenCode session IDs and never forks.
    Pass ``spawn`` (or legacy ``launcher``) only in deterministic tests
    to stay offline.
    """
    if launcher is not None and spawn is None:
        return dict(launcher(state_dir, request_id))
    root = store.ensure_state_dir(state_dir)
    _abandon_never_started(state_dir, request_id)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        if job["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            raise TerminalError(f"job {request_id} is terminal ({job['status']}); refusing to resurrect")
        if job["cancel_requested"] or job["status"] == "cancelling":
            con.execute("ROLLBACK")
            raise TerminalError(f"job {request_id} cancellation requested")
        if job["status"] == "blocked":
            con.execute("ROLLBACK")
            raise BlockedError(f"job {request_id} blocked: {job['block_reason']}")
        # Durable child ownership: never launch while ownership of a prior
        # child is unknown. A live supervised child is adopted, not copied.
        try:
            _raise_if_unowned_invocation(state_dir, request_id)
        except OwnershipError:
            con.execute("ROLLBACK")
            raise
        if job["owner_token"]:
            ident = store.read_worker_identity(root, request_id)
            if ident and ident.get("token") == job["owner_token"] \
                    and _possibly_alive(ident.get("pid"), ident.get("start")):
                con.execute("ROLLBACK")
                raise OwnershipError(
                    f"job {request_id} already has a live owned controller; refusing duplicate launch")
            # A live recorded PID without a matching fresh handshake
            # remains claimed/unknown: never treat it as gone, never
            # start a second writer. Recover reconciles it.
            if job["owner_pid"] is not None and _owner_alive(job):
                con.execute("ROLLBACK")
                raise OwnershipError(
                    f"job {request_id} still claimed by live pid {job['owner_pid']}; run recover")
            if job["owner_pid"] is None and job["status"] in ("pending", "running", "question_pending"):
                pending_attempt = con.execute(
                    "SELECT * FROM launches WHERE request_id=? AND start_token=? AND state='attempting'",
                    (request_id, job["owner_token"]),
                ).fetchone()
                if pending_attempt is not None:
                    con.execute("ROLLBACK")
                    raise OwnershipError(
                        f"job {request_id} launch already in progress; run recover")
        if int(job["attempts"]) >= int(job["max_attempts"]):
            now = _utcnow()
            con.execute(
                "UPDATE jobs SET status='failed', error_class='budget_exhausted',"
                " result_json=?, updated_at=? WHERE request_id=?",
                (json.dumps({"ok": False, "error": {"code": "BUDGET_EXHAUSTED"}}), now, request_id),
            )
            _event(con, request_id, "budget_exhausted", {})
            con.execute("COMMIT")
            raise TerminalError(f"job {request_id} retry budget exhausted")
        token = secrets.token_hex(16)
        attempt_no = int(job["attempts"]) + 1
        now = _utcnow()
        con.execute(
            "INSERT INTO launches(request_id,attempt_no,start_token,state,created_at) VALUES(?,?,?,'attempting',?)",
            (request_id, attempt_no, token, now),
        )
        con.execute(
            "UPDATE jobs SET attempts=?, owner_token=?, owner_pid=NULL, owner_start=NULL, status=?,"
            " updated_at=?, block_reason=NULL WHERE request_id=?",
            (attempt_no, token,
             "running" if job["status"] == "pending" else job["status"], now, request_id),
        )
        _event(con, request_id, "controller_launch_attempting", {"attempt": attempt_no})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    # The lease token travels in the environment, not in visible argv.
    cmd = [sys.executable, "-m", "runner.controller", "--state-dir", str(root),
           "--request-id", request_id]
    try:
        if spawn is not None:
            pid = int(spawn(cmd))
        else:
            proc = subprocess.Popen(
                cmd, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True, close_fds=True, cwd=_PKG_ROOT,
                env=dict(os.environ, DURABLE_RUNNER_TOKEN=token),
            )
            pid = proc.pid
    except OSError as e:
        raise RunnerError(f"controller spawn failed: {e}")
    con2 = store.connect(state_dir)
    try:
        con2.execute("BEGIN IMMEDIATE")
        con2.execute(
            "UPDATE launches SET pid=?, state='acknowledged', ack_at=? WHERE request_id=? AND start_token=?",
            (pid, _utcnow(), request_id, token),
        )
        con2.execute(
            "UPDATE jobs SET owner_pid=?, owner_start=?, updated_at=? WHERE request_id=? AND owner_token=?",
            (pid, _process_start_identity(pid), _utcnow(), request_id, token),
        )
        cur = con2.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if cur is not None and cur["owner_token"] == token:
            _event(con2, request_id, "controller_launch_acknowledged", {"attempt": attempt_no, "pid": pid})
        con2.execute("COMMIT")
    except Exception:
        try:
            con2.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con2.close()
    return {"request_id": request_id, "attempt": attempt_no, "token": token, "pid": pid}


def _controller_lock_path(state_dir, request_id: str) -> Path:
    d = store.ensure_state_dir(state_dir) / "workers"
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    return d / f"{request_id}.lock"


def take_controller_lock(state_dir, request_id: str, wait: float = 10.0):
    """Hold the job's controller lock for this process's lifetime.

    The kernel releases a flock when its holder dies, so a free lock proves
    no controller is running. Returns the open descriptor, or None when
    another controller holds it.
    """
    import fcntl
    fd = os.open(str(_controller_lock_path(state_dir, request_id)),
                 os.O_RDWR | os.O_CREAT, 0o600)
    end = time.monotonic() + wait
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except BlockingIOError:
            if time.monotonic() >= end:
                os.close(fd)
                return None
            time.sleep(0.05)


def controller_lock_held(state_dir, request_id: str) -> bool:
    import fcntl
    path = _controller_lock_path(state_dir, request_id)
    try:
        fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o600)
    except OSError:
        return True  # cannot prove the lock is free
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    except OSError:
        return True
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def acknowledge_controller(state_dir, request_id: str, token: str,
                           pid: int, start: str | None) -> bool:
    """The controller records its own PID and start identity at startup.

    Closes the gap where the launcher died before acknowledging. Returns
    False when the token no longer holds the lease (superseded launch).
    """
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        cur = con.execute("SELECT owner_token FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        if cur is None or cur["owner_token"] != token:
            con.execute("ROLLBACK")
            return False
        con.execute("UPDATE launches SET pid=?, state='acknowledged', ack_at=COALESCE(ack_at, ?)"
                    " WHERE request_id=? AND start_token=?", (pid, _utcnow(), request_id, token))
        con.execute("UPDATE jobs SET owner_pid=?, owner_start=?, updated_at=? WHERE request_id=?",
                    (pid, start, _utcnow(), request_id))
        con.execute("COMMIT")
        return True
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def release_controller(state_dir, request_id: str, token: str) -> None:
    """Drop the lease when the controller exits, so its PID is never reused
    as a claim or signalled later."""
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        cur = con.execute("UPDATE jobs SET owner_token=NULL, owner_pid=NULL, owner_start=NULL,"
                          " updated_at=? WHERE request_id=? AND owner_token=?",
                          (_utcnow(), request_id, token))
        con.execute("UPDATE launches SET state='exited' WHERE request_id=? AND start_token=?"
                    " AND state IN ('attempting','acknowledged')", (request_id, token))
        if cur.rowcount:
            _event(con, request_id, "controller_exited", {})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def _check_lease_locked(con, request_id: str, lease_token: str | None) -> None:
    if lease_token is None:
        return
    row = con.execute("SELECT owner_token, cancel_requested, status FROM jobs WHERE request_id=?",
                      (request_id,)).fetchone()
    if row is None or row["owner_token"] != lease_token or row["cancel_requested"] \
            or row["status"] in store.TERMINAL:
        con.execute("ROLLBACK")
        raise LeaseLostError(f"job {request_id}: lease lost, cancelled, or terminal")


def post_question(state_dir, request_id: str, qid: str, prompt: str,
                  lease_token: str | None = None) -> dict:
    """Persist a Luna/planner question before exposing it."""
    if not qid:
        raise ValueError("missing question ID")
    if not prompt:
        raise ValueError("missing question prompt")
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _check_lease_locked(con, request_id, lease_token)
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        if job["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            raise TerminalError(f"job {request_id} is terminal ({job['status']})")
        now = _utcnow()
        existing = con.execute(
            "SELECT * FROM questions WHERE request_id=? AND qid=?", (request_id, qid)
        ).fetchone()
        if existing is not None:
            if existing["prompt"] != prompt:
                con.execute("ROLLBACK")
                raise ConflictError(f"question {qid!r} already exists with different prompt")
            con.execute("ROLLBACK")
            return dict(existing)
        con.execute(
            "INSERT INTO questions(request_id,qid,prompt,status,created_at) VALUES(?,?,?,'pending',?)",
            (request_id, qid, prompt, now),
        )
        if job["status"] != "question_pending":
            con.execute(
                "UPDATE jobs SET status='question_pending', updated_at=? WHERE request_id=?",
                (now, request_id),
            )
        _event(con, request_id, "question_posted", {"qid": qid})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    con2 = store.connect(state_dir)
    try:
        row = con2.execute(
            "SELECT * FROM questions WHERE request_id=? AND qid=?", (request_id, qid)
        ).fetchone()
        return dict(row)
    finally:
        con2.close()


def clear_question(state_dir, request_id: str, qid: str) -> dict:
    """Operator action: forget a stored question so the dispatcher's next
    question with that id is asked afresh. Clears a planner_question_conflict
    block when that was the reason."""
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        job = con.execute("SELECT status, block_reason FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        cur = con.execute("DELETE FROM questions WHERE request_id=? AND qid=?", (request_id, qid))
        if cur.rowcount == 0:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown question: {qid}")
        if job["status"] == "blocked" and str(job["block_reason"] or "").startswith("planner_question_conflict"):
            con.execute("UPDATE jobs SET status='running', block_reason=NULL, updated_at=? WHERE request_id=?",
                        (_utcnow(), request_id))
        _event(con, request_id, "question_cleared", {"qid": qid})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return {"request_id": request_id, "qid": qid, "cleared": True}


def list_questions(state_dir, request_id: str, only_pending: bool = True) -> list[dict]:
    con = store.connect(state_dir)
    try:
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            raise NotFoundError(f"unknown request: {request_id}")
        if only_pending:
            rows = con.execute(
                "SELECT * FROM questions WHERE request_id=? AND status='pending' ORDER BY id",
                (request_id,),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM questions WHERE request_id=? ORDER BY id", (request_id,)
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def answer(state_dir, request_id: str, qid: str, answer_text: str,
           lease_token: str | None = None) -> dict:
    """Persist an answer (and resume running) before acknowledging it."""
    if not answer_text:
        raise ValueError("missing answer")
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _check_lease_locked(con, request_id, lease_token)
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        if job["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            raise TerminalError(f"job {request_id} is terminal ({job['status']})")
        q = con.execute(
            "SELECT * FROM questions WHERE request_id=? AND qid=?", (request_id, qid)
        ).fetchone()
        if q is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown question: {qid}")
        if q["status"] == "answered":
            if q["answer"] == answer_text:
                con.execute("ROLLBACK")
                return dict(q)
            con.execute("ROLLBACK")
            raise ConflictError(f"question {qid!r} already answered differently")
        now = _utcnow()
        con.execute(
            "UPDATE questions SET status='answered', answer=?, answered_at=? WHERE request_id=? AND qid=?",
            (answer_text, now, request_id, qid),
        )
        _event(con, request_id, "answer_persisted", {"qid": qid})
        remaining = con.execute(
            "SELECT COUNT(*) AS n FROM questions WHERE request_id=? AND status='pending'",
            (request_id,),
        ).fetchone()["n"]
        if remaining == 0 and job["status"] in ("question_pending", "blocked"):
            con.execute(
                "UPDATE jobs SET status='running', block_reason=NULL, updated_at=? WHERE request_id=?",
                (now, request_id),
            )
            _event(con, request_id, "resumed_after_answers", {})
        else:
            con.execute(
                "UPDATE jobs SET updated_at=? WHERE request_id=?", (now, request_id)
            )
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    con2 = store.connect(state_dir)
    try:
        return dict(con2.execute(
            "SELECT * FROM questions WHERE request_id=? AND qid=?", (request_id, qid)
        ).fetchone())
    finally:
        con2.close()


def _terminate_pid(pid: int | None) -> None:
    if pid is None:
        return
    try:
        os.kill(int(pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError, ValueError, OSError):
        return


def cancel(state_dir, request_id: str) -> dict:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        if job["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            return _row_to_job(job)
        now = _utcnow()
        # Persist cancellation intent durably, but do NOT mark terminal
        # cancelled until every owned child process group is confirmed stopped.
        con.execute(
            "UPDATE jobs SET cancel_requested=1, status='cancelling', updated_at=? WHERE request_id=?",
            (now, request_id),
        )
        _event(con, request_id, "cancelling", {})
        con.execute("COMMIT")
        owner_token, owner_pid = job["owner_token"], job["owner_pid"]
        owner = {"owner_pid": job["owner_pid"], "owner_start": job["owner_start"]}
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()

    # Stop every owned child process group, not just the controller PID,
    # including the actual owned proof process group recorded durably
    # while it runs. The workspace claim is retained until all owned
    # children confirm dead.
    children_dead = _drain_owned_children(state_dir, request_id, owner)
    proof_dead = _drain_proof_group(state_dir, request_id)
    all_dead = bool(children_dead and proof_dead)

    # Best-effort stop of the legacy owned worker PID when ownership proven.
    try:
        root = store.ensure_state_dir(state_dir)
        ident = store.read_worker_identity(root, request_id)
        if owner_token and ident and ident.get("token") == owner_token \
                and ident.get("pid") == owner_pid:
            _signal_pid(owner_pid, owner["owner_start"], signal.SIGTERM)
    except OSError:
        pass

    _finalize_stopped(state_dir, request_id, all_dead)
    return get_job(state_dir, request_id)


def _finalize_stopped(state_dir, request_id: str, all_dead: bool) -> None:
    """Finish a cancel or timeout drain in one guarded statement.

    The outcome follows the intent stored at commit time (cancel=1 wins
    over timeout=2, whichever was written last), never a terminal status,
    and the result mirror copies exactly what was committed.
    """
    now = _utcnow()
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        if not all_dead:
            # Keep the workspace claimed until recover confirms the stop.
            cur = con.execute(
                "UPDATE jobs SET status='blocked', block_reason=CASE WHEN cancel_requested=2"
                " THEN 'timeout_pending: live child process group remains'"
                " ELSE 'cancellation_pending: live child process group remains' END,"
                " updated_at=? WHERE request_id=? AND status NOT IN ('succeeded','failed','cancelled')",
                (now, request_id))
            if cur.rowcount:
                _event(con, request_id, "blocked", {"reason": "stop_pending"})
            con.execute("COMMIT")
            return
        cur = con.execute(
            "UPDATE jobs SET status=CASE WHEN cancel_requested=2 THEN 'failed' ELSE 'cancelled' END,"
            " error_class=CASE WHEN cancel_requested=2 THEN 'timeout' ELSE error_class END,"
            " result_json=CASE WHEN cancel_requested=2 THEN ? ELSE ? END, updated_at=?"
            " WHERE request_id=? AND status NOT IN ('succeeded','failed','cancelled')",
            (json.dumps({"ok": False, "error": {"code": "TIMEOUT"}}),
             json.dumps({"cancelled": True, "at": now}), now, request_id))
        row = con.execute("SELECT status, result_json FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        if cur.rowcount:
            _event(con, request_id, "timeout" if row["status"] == "failed" else "cancelled", {})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    try:
        result = json.loads(row["result_json"] or "null")
    except ValueError:
        result = None
    _mirror_result(state_dir, request_id,
                   json.dumps({"request_id": request_id, "status": row["status"], "result": result}))


def _complete_with_token(state_dir, request_id: str, token: str, ok: bool,
                         output: str | None, error, route: str | None = None) -> dict:
    if not token:
        raise ValueError("missing start token")
    head_commit = _workspace_head(get_job(state_dir, request_id).get("workspace"))
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        if job["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            raise TerminalError(f"job {request_id} is terminal ({job['status']})")
        if job["owner_token"] != token:
            con.execute("ROLLBACK")
            raise OwnershipError("start token does not hold the lease")
        if job["cancel_requested"]:
            con.execute("ROLLBACK")
            raise TerminalError(f"job {request_id} cancellation requested")
        now = _utcnow()
        if ok:
            status = "succeeded"
            err_class = None
            result = {"ok": True, "output": output,
                      "head_commit": head_commit}
        else:
            status = "failed"
            err = error if isinstance(error, dict) else {"message": str(error) if error else "failed"}
            err_class = str(err.get("code") or err.get("message") or "failed")[:120]
            result = {"ok": False, "output": output, "error": err}
            # A worker-reported failure never changes routes or capacity:
            # only the controller's owned-session provider evidence does.
        con.execute(
            "UPDATE jobs SET status=?, result_json=?, error_class=?, head_commit=?, updated_at=?"
            " WHERE request_id=?",
            (status, json.dumps(result), err_class, head_commit, now, request_id),
        )
        _event(con, request_id, "completed" if ok else "failed",
               {"route": job["route"]})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    try:
        root = store.ensure_state_dir(state_dir)
        _mirror_result(state_dir, request_id,
            json.dumps({"request_id": request_id, "status": status, "result": result}),
        )
        store.append_text(store.output_path_for(root, request_id), f"[{status}]\n")
    except OSError:
        pass
    return get_job(state_dir, request_id)


def complete(state_dir, request_id: str, token: str, output: str | None = None) -> dict:
    return _complete_with_token(state_dir, request_id, token, True, output, None)


def fail(state_dir, request_id: str, token: str, error, output: str | None = None) -> dict:
    return _complete_with_token(state_dir, request_id, token, False, output, error)




def _known_tokens(con: sqlite3.Connection, request_id: str) -> set[str]:
    rows = con.execute(
        "SELECT start_token FROM launches WHERE request_id=?", (request_id,)
    ).fetchall()
    return {r["start_token"] for r in rows}


def _assess_runtime(state_dir, request_id: str) -> tuple[bool, str, dict | None, dict]:
    """Assess stored state against the currently installed runtime.

    Returns (compatible, reason, old_runtime, new_runtime) and records a
    ``runtime_assessed`` event with the actual runtime and policy used
    before and after. ``old_runtime`` is the newest provenance recorded
    by submit or an invocation, or None when nothing recorded one:
    reported as unknown, never invented. Only NotFoundError propagates;
    any other assessment failure is reported as incompatibility so the
    work is retained instead of migrated.
    """
    new = runtime.installed_runtime()
    job = get_job(state_dir, request_id)
    invs = _list_invocations(state_dir, request_id)
    con = store.connect(state_dir)
    try:
        events = [dict(r) for r in con.execute(
            "SELECT kind, payload_json FROM events WHERE request_id=? AND kind IN "
            "('submitted','runtime_assessed','runtime_recovered') ORDER BY id",
            (request_id,)).fetchall()]
    finally:
        con.close()
    old = runtime.latest_provenance(invs, events)
    try:
        ok, reason = runtime.check_compatible(dict(job), invs)
    except Exception as e:  # noqa: BLE001 - retain work, never migrate blind
        ok, reason = False, (f"compatibility_check_failed: {type(e).__name__}: {e}; "
                             "retaining work, refusing to migrate")
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _event(con, request_id, "runtime_assessed",
               {"compatible": ok, "reason": reason,
                "old_runtime": old, "new_runtime": new})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()
    return ok, reason, old, new


def _record_runtime_recovered(state_dir, request_id: str, old, new, trigger: str) -> None:
    """Record a runtime transfer: provenance before and after, identities kept."""
    try:
        job = get_job(state_dir, request_id)
    except NotFoundError:
        return
    payload = {"trigger": trigger, "old_runtime": old, "new_runtime": new,
               "preserved": {"route": job.get("route"), "policy_id": job.get("policy_id"),
                             "task_hash": job.get("task_hash"),
                             "codex_task_id": bool(job.get("codex_task_id")),
                             "opencode_session_id": bool(job.get("opencode_session_id")),
                             "grok_session_id": bool(job.get("grok_session_id"))}}
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _event(con, request_id, "runtime_recovered", payload)
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def _has_runtime_missing_failure(state_dir, request_id: str) -> bool:
    """A dispatch action whose newest attempt never started for a missing runtime.

    Grouped per action key: only the newest non-abandoned row decides.
    An older missing-runtime row never counts after a newer attempt for
    the same action completed or otherwise superseded it, so a repaired
    retry stops reporting recovery once it succeeds (no false
    ``runtime_recovered`` events on later recoveries).
    """
    by_key: dict[str, list[dict]] = {}
    for i in _list_invocations(state_dir, request_id):
        if i.get("state") == "abandoned" or not i.get("action_key"):
            continue
        by_key.setdefault(i["action_key"], []).append(i)
    for rows in by_key.values():
        newest = rows[-1]
        if (_is_dispatch_row(newest) and newest.get("state") == "failed"
                and runtime.is_runtime_missing_row(newest)):
            return True
    return False


def recover_one(state_dir, request_id: str) -> dict:
    """Reconcile durable records with live ownership; resume only when safe."""
    root = store.ensure_state_dir(state_dir)
    # Reconcile identity, then consume finished child output exactly once
    # before considering the controller lease gone. A completed child must
    # not be rerun. An owned server without its supervisor is unusable
    # (its password died with the supervisor) and is stopped first.
    _stop_orphaned_invocations(state_dir, request_id)
    _abandon_never_started(state_dir, request_id)
    _reconcile_dead_invocations(state_dir, request_id)
    # Terminal jobs are never resurrected and never assessed: recover
    # --all must not add runtime_assessed noise to finished jobs. Rows
    # that finished after the job ended are still consumed once, so
    # their measurements (classified outcome, timestamps) land on the
    # ledger; consumption never changes a terminal status.
    terminal_status = get_job(state_dir, request_id)["status"]
    if terminal_status in store.TERMINAL:
        try:
            late = consume_finished_invocations(state_dir, request_id)
        except Exception as e:  # noqa: BLE001 - reported, never resurrected
            return {"request_id": request_id, "action": "noop-terminal",
                    "status": terminal_status, "consume_error": str(e)[:200]}
        return {"request_id": request_id, "action": "noop-terminal",
                "status": terminal_status, "consumed": len(late)}
    # Runtime recovery assessment for live jobs only: resolve the
    # currently installed runtime, decide explicitly whether it can read
    # the stored job and invocation state as is, and record the actual
    # runtime and policy before and after. It runs before output is
    # consumed because measurement during consume stamps current schema
    # markers: the check must see the stored rows as they are. The
    # compatibility decision itself only gates execution handoffs
    # (starting a replacement controller) further down; healthy owned
    # work adopted there is never interrupted merely because an update
    # occurred.
    compat_ok, compat_reason, old_runtime, new_runtime = _assess_runtime(
        state_dir, request_id)
    # Consume finished child output exactly once before considering the
    # controller lease gone. A completed child must not be rerun.
    consumed = consume_finished_invocations(state_dir, request_id)
    if any(c.get("action") == "completed" for c in consumed):
        return {"request_id": request_id, "action": "consumed-completion",
                "status": get_job(state_dir, request_id)["status"]}
    live_invocations = _any_live_invocation(state_dir, request_id)
    if live_invocations:
        _capture_invocation_sessions(state_dir, request_id, live_invocations)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        status = job["status"]
        if status in store.TERMINAL:
            # Never resurrect completed jobs.
            con.execute("ROLLBACK")
            return {"request_id": request_id, "action": "noop-terminal",
                    "status": status}
        # Cancellation intent persisted previously. Stop owned children and
        # finalize cancellation only after every owned process group is dead,
        # including a proof tree that outlived its controller.
        if job["cancel_requested"]:
            con.execute("COMMIT")
            con.close()
            children_dead = _drain_owned_children(state_dir, request_id, dict(job))
            proof_dead = _drain_proof_group(state_dir, request_id)
            all_dead = bool(children_dead and proof_dead)
            _finalize_stopped(state_dir, request_id, all_dead)
            fin = get_job(state_dir, request_id)
            return {"request_id": request_id,
                    "action": "timeout" if fin["error_class"] == "timeout" else "cancelled",
                    "status": fin["status"]}

        # No job-age deadline (#88): recovery never cancels or drains an
        # active job because of its age. ``timeout_secs`` (including legacy
        # positive values on old rows) is readable but never enforced; only
        # explicit cancellation above stops owned children. Rows already
        # carrying a timeout drain intent (cancel_requested=2, written
        # before #88) still finalize through _finalize_stopped below via
        # the cancellation path, preserving their timeout origin.
        def mark_blocked(reason: str):
            con.execute(
                "UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?",
                (reason, _utcnow(), request_id),
            )
            _event(con, request_id, "blocked", {"reason": reason})

        # Only recover's own ownership blocks are re-evaluated. Any other
        # block (a failed turn, a session mismatch) stays blocked: recover
        # never restarts it. Planner questions unblock through `answer`.
        if status == "blocked" and not str(job["block_reason"] or "").startswith(
                RECOVER_OWNED_BLOCKS):
            con.execute("ROLLBACK")
            return {"request_id": request_id, "action": "blocked-sticky",
                    "status": "blocked", "reason": job["block_reason"]}
        # A dispatch whose supervisor could not spawn was closed as failed by
        # the controller; if that controller died before blocking the job,
        # the job would sit running without an owner. Block it here, unless
        # every failed dispatch row carries explicit never-started
        # runtime-missing evidence: the installed runtime repairs exactly
        # that cause, so recovery falls through and restarts the controller
        # to remake the action fresh. Any other failure keeps its sticky
        # block; live, successful, and uncertain actions are never replayed.
        if status in ("pending", "running") and not job["codex_task_id"]:
            failed_dispatch = [inv for inv in _list_invocations(state_dir, request_id)
                               if _is_dispatch_row(inv) and inv.get("state") == "failed"]
            live_or_open = [inv for inv in _list_invocations(state_dir, request_id)
                            if inv.get("state") in LIVE_INVOCATION_STATES]
            if failed_dispatch and not live_or_open:
                retryable = bool(failed_dispatch) and all(
                    runtime.is_runtime_missing_row(i) for i in failed_dispatch)
                if not retryable:
                    mark_blocked(f"codex_dispatch_failed rc={failed_dispatch[-1].get('rc')} "
                                 "(consumed by recovery): the dispatch never started")
                    con.execute("COMMIT")
                    return {"request_id": request_id, "action": "blocked-failed-dispatch",
                            "status": "blocked"}
        orphans_left = [inv for inv in _list_invocations(state_dir, request_id)
                        if _invocation_ownership(inv) == "orphaned"]
        if orphans_left:
            mark_blocked(f"orphaned server {orphans_left[0]['invocation_id'][:8]} did not stop; "
                         "refusing duplicate")
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "blocked-orphaned-server",
                    "status": "blocked"}
        lease_token = job["owner_token"]
        lease_pid = job["owner_pid"]
        known = _known_tokens(con, request_id)
        ident = store.read_worker_identity(root, request_id)
        ident_token = ident.get("token") if ident else None
        ident_pid = ident.get("pid") if ident else None
        ident_updated = ident.get("updated") if ident else None
        ident_alive = _possibly_alive(ident_pid, ident.get("start")) if ident else False
        ident_fresh = _heartbeat_fresh(ident_updated) if ident else False
        lease_alive = _owner_alive(job)
        # The lease holder advertised itself: alive with the lease token.
        # Heartbeats refresh only between steps, so staleness is not death.
        ident_holds_lease = bool(ident and lease_token and ident_token == lease_token
                                 and _identity_matches(ident_pid, ident.get("start"))
                                 and (lease_pid is None or ident_pid == lease_pid))
        if ident and lease_token and ident_token == lease_token and not ident_holds_lease \
                and _identity_state(ident_pid, ident.get("start")) == "unknown":
            mark_blocked(f"unknown worker ownership: pid={ident_pid} identity unreadable; refusing duplicate")
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "blocked-unknown-owner", "status": "blocked"}
        # A running controller holds the job's flock from before its first
        # write until it exits; the kernel drops it on death. A free lock
        # after the startup grace therefore proves no controller runs, and
        # a controller that starts later is fenced by the lease token.
        lock_held = controller_lock_held(state_dir, request_id)
        launch_pending = False
        if lease_token and lease_pid is None and not ident_holds_lease:
            row = con.execute(
                "SELECT created_at FROM launches WHERE request_id=? AND start_token=? AND state='attempting'",
                (request_id, lease_token)).fetchone()
            created = _parse_ts(row["created_at"]) if row is not None else None
            launch_pending = lock_held or (
                created is not None and time.time() - created < LAUNCH_ACK_GRACE_SECS)
        if launch_pending:
            con.execute("ROLLBACK")
            return {"request_id": request_id, "action": "launch-in-progress", "status": status}
        controller_alive = lease_alive or ident_holds_lease or lock_held

        # If any owned child process group is still alive, the workspace stays
        # claimed. NULL pid/pgid is unresolved ownership, never death.
        unresolved_now = [
            inv for inv in _list_invocations(state_dir, request_id)
            if _invocation_ownership(inv) == "unresolved"
        ]
        starting = [inv for inv in unresolved_now
                    if inv.get("supervisor_pid") is None and inv.get("pid") is None]
        if unresolved_now and controller_alive and len(starting) == len(unresolved_now):
            # A live controller just inserted a row its supervisor has not
            # claimed yet: normal startup, not unknown ownership.
            con.execute("ROLLBACK")
            return {"request_id": request_id, "action": "invocation-starting", "status": status}
        if unresolved_now:
            mark_blocked(
                f"unresolved invocation {unresolved_now[0]['invocation_id'][:8]}... "
                f"(NULL pid/pgid or start-identity mismatch); refusing duplicate")
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "blocked-unresolved-invocation",
                    "status": "blocked"}
        live_invocations = [
            inv for inv in _list_invocations(state_dir, request_id)
            if _invocation_ownership(inv) == "live"
        ]
        if live_invocations:
            lease_pid_alive = controller_alive
            if job["status"] == "blocked":
                con.execute(
                    "UPDATE jobs SET status='running', block_reason=NULL, updated_at=? WHERE request_id=?",
                    (_utcnow(), request_id),
                )
            if not lease_pid_alive:
                if job["owner_token"]:
                    con.execute(
                        "UPDATE launches SET state='dead' WHERE request_id=? AND start_token=?"
                        " AND state IN ('attempting','acknowledged')",
                        (request_id, job["owner_token"]))
                con.execute(
                    "UPDATE jobs SET owner_token=NULL, owner_pid=NULL, owner_start=NULL, updated_at=? WHERE request_id=?",
                    (_utcnow(), request_id))
            _event(con, request_id, "recovered-adopted-live-invocation",
                   {"invocation_id": live_invocations[0]["invocation_id"][:16],
                    "kind": live_invocations[0].get("kind", "")})
            con.execute("COMMIT")
            out = {"request_id": request_id, "action": "adopted-live-invocation"}
            if not lease_pid_alive and (job["codex_task_id"] or any(
                    _is_dispatch_row(i) for i in live_invocations)):
                # A new controller waits for the live child's durable
                # result through the same action identity. The
                # compatibility decision gates this handoff like every
                # other replacement-controller start: healthy owned work
                # was adopted above and is never interrupted, but an
                # unsupported state starts no controller on unreadable
                # rows. The live child finishes on its own; its output is
                # consumed once, and the next recovery blocks with the
                # specific incompatibility instead of executing blind.
                if not compat_ok:
                    con.execute("BEGIN IMMEDIATE")
                    _event(con, request_id, "runtime_incompatible_deferred",
                           {"reason": compat_reason,
                            "invocation_id": live_invocations[0]["invocation_id"][:16]})
                    con.execute("COMMIT")
                    out["resume_error"] = (
                        f"runtime_incompatible: {compat_reason} (no replacement "
                        "controller started while the adopted live child runs)")
                    out["status"] = get_job(state_dir, request_id)["status"]
                    return out
                try:
                    out["pid"] = start_controller(state_dir, request_id).get("pid")
                except (OwnershipError, TerminalError, BlockedError, RunnerError) as e:
                    out["resume_error"] = str(e)[:200]
            out["status"] = get_job(state_dir, request_id)["status"]
            return out

        # Case: unknown ownership. A live worker advertises a token that is
        # neither the lease nor any known launch: stop, never duplicate.
        if ident and ident_alive and ident_fresh and ident_token not in known and ident_token != lease_token:
            mark_blocked(f"unknown worker ownership token={str(ident_token)[:8]}... pid={ident_pid}")
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "blocked-unknown-owner",
                    "status": "blocked"}

        # Case: the lease holder is alive and advertised: adopt, and
        # acknowledge a launch whose acknowledgment was lost. Only the
        # current lease token counts, never a superseded launch.
        if ident_holds_lease:
            con.execute(
                "UPDATE launches SET pid=?, state='acknowledged', ack_at=? WHERE request_id=? AND start_token=? AND state='attempting'",
                (ident_pid, _utcnow(), request_id, lease_token),
            )
            if lease_pid is None:
                con.execute(
                    "UPDATE jobs SET owner_pid=?, owner_start=?, updated_at=? WHERE request_id=? AND owner_token=?",
                    (ident_pid, ident.get("start"), _utcnow(), request_id, lease_token))
            if job["status"] == "blocked":
                con.execute(
                    "UPDATE jobs SET status='running', block_reason=NULL, updated_at=? WHERE request_id=?",
                    (_utcnow(), request_id),
                )
            _event(con, request_id, "recovered-adopted-live-worker", {})
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "adopted-live-worker",
                    "status": get_job(state_dir, request_id)["status"]}

        # Case: live recorded PID without a matching fresh handshake
        # remains claimed/unknown and blocks duplicates. A missing or
        # stale heartbeat is never worker_gone while the recorded PID
        # is still alive.
        if lease_pid is not None and lease_alive:
            # A starting controller holds the lock before it advertises, and
            # needs a moment after the launcher's acknowledgment to get there.
            ack_row = con.execute(
                "SELECT ack_at FROM launches WHERE request_id=? AND start_token=?",
                (request_id, lease_token)).fetchone() if lease_token else None
            acked = _parse_ts(ack_row["ack_at"]) if ack_row is not None else None
            recently_acked = acked is not None and time.time() - acked < LAUNCH_ACK_GRACE_SECS
            if not (ident_holds_lease or lock_held) and recently_acked:
                con.execute("ROLLBACK")
                return {"request_id": request_id, "action": "launch-in-progress", "status": status}
            handshake_ok = ident_holds_lease or lock_held
            if not handshake_ok:
                # Unknown live foreign token with fresh heartbeat stays
                # blocked-unknown-owner (handled above when fresh); any
                # other live-lease-without-handshake stays claimed.
                if not (ident and ident_alive and ident_fresh
                        and ident_token not in known and ident_token != lease_token):
                    mark_blocked(f"claimed by live pid={lease_pid} without fresh handshake; refusing duplicate")
                    con.execute("COMMIT")
                    return {"request_id": request_id, "action": "blocked-claimed-live-pid",
                            "status": "blocked"}

        # Case: multiple attempting launches (launch race) with no live worker
        # yet: keep the newest attempting row as lease, supersede others.
        attempting = con.execute(
            "SELECT * FROM launches WHERE request_id=? AND state='attempting' ORDER BY attempt_no",
            (request_id,),
        ).fetchall()
        if len(attempting) > 1 and not ident_alive and not lease_alive:
            keep = attempting[-1]
            for row in attempting[:-1]:
                con.execute("UPDATE launches SET state='superseded' WHERE id=?", (row["id"],))
            con.execute(
                "UPDATE jobs SET owner_token=?, owner_pid=NULL, updated_at=? WHERE request_id=?",
                (keep["start_token"], _utcnow(), request_id),
            )
            _event(con, request_id, "recovered-launch-race-superseded",
                   {"kept_attempt": keep["attempt_no"]})
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "reconciled-launch-race",
                    "status": get_job(state_dir, request_id)["status"]}

        # Case: worker dead (orphan/stale/partial output preserved).
        # Missing/stale heartbeat alone is not enough: the recorded
        # lease PID must also be dead.
        worker_gone = not controller_alive
        if worker_gone and status in ("running", "question_pending", "pending", "blocked"):
            # A proof process group outlived its controller: the proof
            # tree keeps running without an owner, so no replacement
            # controller starts behind it. The block re-evaluates on the
            # next recover once the group is reaped.
            if proof_owner_alive(state_dir, request_id):
                mark_blocked("unresolved proof ownership: a proof process group "
                             "may still run; refusing duplicate")
                con.execute("COMMIT")
                return {"request_id": request_id, "action": "blocked-unresolved-proof",
                        "status": "blocked"}
            # Mark known attempting/acknowledged launches without live workers dead.
            if lease_token:
                con.execute(
                    "UPDATE launches SET state='dead' WHERE request_id=? AND start_token=? AND state IN ('attempting','acknowledged')",
                    (request_id, lease_token),
                )
            # Question-waiting jobs replay their persisted questions: no spawn
            # while the planner still owes an answer.
            pending_q = con.execute(
                "SELECT COUNT(*) AS n FROM questions WHERE request_id=? AND status='pending'",
                (request_id,),
            ).fetchone()["n"]
            # A job blocked only by a missing runtime still owes the same
            # planner answer: one recover restarts its callback like a
            # question_pending job instead of needing a second call.
            runtime_missing_block = (
                status == "blocked"
                and str(job["block_reason"] or "").startswith("runtime_missing"))
            # A job blocked on an exhausted escalation still owes the
            # planner's decision: one recover restarts its callback like
            # a question_pending job instead of sticking forever.
            recovery_exhausted_block = (
                status == "blocked"
                and str(job["block_reason"] or "").startswith("recovery_exhausted")
                and pending_q > 0)
            if pending_q > 0 and (status == "question_pending" or runtime_missing_block
                                  or recovery_exhausted_block) \
                    and job["codex_task_id"] \
                    and int(job["attempts"]) < int(job["max_attempts"]):
                # The controller asks the saved planner again through the
                # same action identity: a finished callback is reused. The
                # compatibility check gates this handoff: an unsupported
                # state blocks with its specific reason instead of starting
                # execution on state this runtime cannot read.
                if not compat_ok:
                    mark_blocked(f"runtime_incompatible: {compat_reason}")
                    con.execute("COMMIT")
                    return {"request_id": request_id, "action": "blocked-incompatible-runtime",
                            "status": "blocked", "reason": compat_reason}
                con.execute(
                    "UPDATE jobs SET owner_token=NULL, owner_pid=NULL, owner_start=NULL, updated_at=? WHERE request_id=?",
                    (_utcnow(), request_id))
                _event(con, request_id, "recovered-question-callback", {"pending": pending_q})
                con.execute("COMMIT")
                try:
                    info = start_controller(state_dir, request_id)
                    return {"request_id": request_id, "action": "resumed-controller",
                            "status": get_job(state_dir, request_id)["status"],
                            "pid": info.get("pid")}
                except (OwnershipError, TerminalError, BlockedError, RunnerError) as e:
                    return {"request_id": request_id, "action": "replay-questions",
                            "status": get_job(state_dir, request_id)["status"],
                            "pending": pending_q, "resume_error": str(e)[:200]}
            if pending_q > 0:
                if status != "question_pending":
                    con.execute(
                        "UPDATE jobs SET status='question_pending', updated_at=? WHERE request_id=?",
                        (_utcnow(), request_id),
                    )
                _event(con, request_id, "recovered-question-replay", {"pending": pending_q})
                con.execute("COMMIT")
                return {"request_id": request_id, "action": "replay-questions",
                        "status": "question_pending", "pending": pending_q}
            # A dispatched job continues on its saved Luna task. Finished
            # child output is reused by action identity, never rerun.
            # A finished dispatch whose action was not saved yet is resumed
            # too: the new controller reuses that result by action key.
            # A dispatch that provably never started for a missing runtime
            # is resumed as well: its rows prove no child ran, so the new
            # controller remakes the action fresh without duplicating.
            should_resume = bool(job["codex_task_id"]) or any(
                _is_dispatch_row(i) and i.get("state") == "completed"
                for i in _list_invocations(state_dir, request_id))
            runtime_recovery = _has_runtime_missing_failure(state_dir, request_id)
            should_resume = should_resume or runtime_recovery
            # Same-session recovery within budget: clear lease, keep planner
            # and executor session IDs, keep partial log. When a public
            # answer is waiting on a saved Luna task, recover resumes a
            # detached controller instead of stalling on a cleared lease.
            if int(job["attempts"]) >= int(job["max_attempts"]):
                con.execute(
                    "UPDATE jobs SET status='failed', error_class='budget_exhausted',"
                    " result_json=?, updated_at=? WHERE request_id=?",
                    (json.dumps({"ok": False, "error": {"code": "BUDGET_EXHAUSTED"}}),
                     _utcnow(), request_id),
                )
                _event(con, request_id, "budget_exhausted", {})
                con.execute("COMMIT")
                return {"request_id": request_id, "action": "budget-exhausted",
                        "status": "failed"}
            # The installed runtime reads this state only when the
            # recovery-boundary check says so. Healthy owned work was
            # adopted above and is never interrupted; here no controller
            # or child is alive, so an unsupported state or policy
            # transition blocks with its specific reason and retains the
            # work instead of starting execution on unreadable state: no
            # migration, no route substitution, no replay.
            if not compat_ok:
                mark_blocked(f"runtime_incompatible: {compat_reason}")
                con.execute("COMMIT")
                return {"request_id": request_id, "action": "blocked-incompatible-runtime",
                        "status": "blocked", "reason": compat_reason}
            con.execute(
                "UPDATE jobs SET owner_token=NULL, owner_pid=NULL,"
                " status=?, updated_at=? WHERE request_id=?",
                ("pending" if status == "blocked" else status, _utcnow(), request_id),
            )
            _event(con, request_id, "recovered-worker-dead", {"partial_preserved": True})
            con.execute("COMMIT")
            if should_resume:
                if runtime_recovery:
                    _record_runtime_recovered(state_dir, request_id, old_runtime,
                                              new_runtime, "worker-dead")
                try:
                    info = start_controller(state_dir, request_id)
                    return {"request_id": request_id, "action": "resumed-controller",
                            "status": get_job(state_dir, request_id)["status"],
                            "pid": info.get("pid")}
                except (OwnershipError, TerminalError, BlockedError, RunnerError) as e:
                    return {"request_id": request_id, "action": "worker-dead-cleared",
                            "status": get_job(state_dir, request_id)["status"],
                            "resume_error": str(e)[:200]}
            return {"request_id": request_id, "action": "worker-dead-cleared",
                    "status": get_job(state_dir, request_id)["status"]}

        _event(con, request_id, "recovered-noop", {})
        con.execute("COMMIT")
        return {"request_id": request_id, "action": "noop", "status": status}
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def reset_at_for_evidence(route, evidence, state: str, window: str | None = None,
                            now_ts: float | None = None) -> tuple[str | None, str | None]:
    """(reset_at, reset_source) for a capacity mark.

    A provider-named reset wins verbatim (Codex ``resets_at``, Go
    ``Retry-After`` seconds, Claude's reset time in text). A limit event
    with none assumes the named or default window (5-hour, weekly, monthly),
    flagged as assumed rather than provider-reported, so preflight honors it
    and the route is retried once after it passes. The overload cooldown is
    policy-derived. Anything else stays unknown.
    """
    if state == "degraded":
        return degraded_until(now_ts), "cooldown"
    if state == "exhausted":
        moment = policy.parse_provider_reset(evidence, now_ts)
        if moment is not None:
            return moment, "provider"
        win = window
        if not isinstance(win, str) or not win:
            win = evidence.get("window") if isinstance(evidence, dict) else None
            if isinstance(evidence, dict) and isinstance(evidence.get("error"), dict):
                win = win or evidence["error"].get("window")
            if not isinstance(win, str) or not win:
                win = policy.ASSUMED_WINDOW_DEFAULT
        return policy.assumed_reset_at(win, now_ts), "assumed"
    return None, None


def _trusted_reset_at(evidence) -> str | None:
    """Return a provider reset timestamp only from explicit trusted evidence.

    Unknown stays unknown. Assumed windows are derived separately by
    :func:`reset_at_for_evidence` and flagged as assumed.
    """
    if not isinstance(evidence, dict):
        return None
    for key in ("reset_at", "resetAt", "provider_reset_at", "quota_reset_at"):
        val = evidence.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        nested = evidence.get("error")
        if isinstance(nested, dict):
            val = nested.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return None


CAPACITY_STATES = ("unknown", "exhausted", "degraded", "available")


def _capacity_windows_for(route: str, state: str, evidence, window: str | None) -> list[str]:
    """Windows this capacity mark covers. The table keys on pool, model,
    and window, so Go 5-hour, weekly, and monthly windows coexist. An
    explicit window (or one named by evidence) wins; degraded uses the
    documented cooldown; Go exhaustion without a named window covers all
    three Go windows; anything else stays unknown rather than guessed."""
    if isinstance(window, str) and window:
        return [window]
    if isinstance(evidence, dict):
        named = evidence.get("window")
        if isinstance(named, str) and named:
            return [named]
        nested = evidence.get("error") if isinstance(evidence.get("error"), dict) else None
        if isinstance(nested, dict) and isinstance(nested.get("window"), str) and nested.get("window"):
            return [str(nested.get("window"))]
    if state == "degraded":
        return ["cooldown"]
    spec = policy.ROUTES.get(route) or {}
    if state == "exhausted" and spec.get("pool") == "go":
        return [w for w in ("5h", "weekly", "monthly") if w in policy.WINDOWS]
    return ["unknown"]


def _record_capacity_locked(con: sqlite3.Connection, route: str, state: str,
                            evidence=None, reset_at: str | None = None,
                            window: str | None = None,
                            reset_source: str | None = None) -> None:
    """Record capacity keyed by pool, model, and window. ``route`` names one
    pool and model and is kept for display; the primary key is
    (pool, model, window). ``reset_at`` comes from provider evidence
    (verbatim), a flagged window assumption, the documented degraded
    cooldown, or an operator. An assumed weekly or monthly mark also starts
    its probe schedule: the first probe is due after one hour, lengthening
    with each failure, so a days-too-long assumption is corrected by data.
    """
    now = _utcnow()
    ev = None
    if evidence is not None:
        try:
            ev = json.dumps(adapters.redact_nested(evidence), sort_keys=True)[:4000]
        except Exception:
            ev = json.dumps({"recorded": True})
    spec = policy.ROUTES.get(route) or {}
    pool = spec.get("pool") or "unknown"
    model = spec.get("model") or route
    for win in _capacity_windows_for(route, state, evidence, window):
        next_probe = None
        failures = None
        if state == "exhausted" and reset_source == "assumed" and win in ("weekly", "monthly"):
            failures = 0
            next_probe = (datetime.datetime.fromtimestamp(time.time(), datetime.timezone.utc)
                          + datetime.timedelta(seconds=policy.probe_delay_secs(0))).isoformat()
        con.execute(
            "INSERT INTO capacity(route, state, evidence_json, reset_at, updated_at, pool, model, window,"
            " reset_source, next_probe_at, probe_failures)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(pool, model, window) DO UPDATE SET"
            " route=excluded.route, state=excluded.state, evidence_json=excluded.evidence_json,"
            " reset_at=excluded.reset_at, updated_at=excluded.updated_at,"
            " reset_source=excluded.reset_source, next_probe_at=excluded.next_probe_at,"
            " probe_failures=excluded.probe_failures",
            (route, state, ev, reset_at, now, pool, model, win,
             reset_source, next_probe, failures),
        )


def degraded_until(now_ts: float | None = None) -> str:
    """Documented cooldown end for an overloaded route."""
    secs = policy.SIGNAL_CLASSES["overloaded"]["degraded_secs"]
    base = datetime.datetime.fromtimestamp(now_ts if now_ts is not None else time.time(),
                                           datetime.timezone.utc)
    return (base + datetime.timedelta(seconds=secs)).isoformat()


def record_capacity(state_dir, route: str, state: str, evidence=None,
                    reset_at: str | None = None, window: str | None = None,
                    reset_source: str | None = None) -> None:
    policy.validate_route(route)
    if state not in CAPACITY_STATES:
        raise ValueError(f"invalid capacity state: {state!r}")
    if reset_source is not None and reset_source not in policy.RESET_SOURCES:
        raise ValueError(f"invalid reset source: {reset_source!r}")
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _record_capacity_locked(con, route, state, evidence, reset_at, window,
                                reset_source)
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def list_capacity(state_dir) -> list[dict]:
    con = store.connect(state_dir)
    try:
        return [dict(r) for r in con.execute(
            "SELECT * FROM capacity ORDER BY route, window").fetchall()]
    finally:
        con.close()


def clear_capacity(state_dir, route: str) -> dict:
    """Operator action: forget an exhausted route after checking the
    provider. The runner never invents a reset time on its own."""
    policy.validate_route(route)
    spec = policy.ROUTES.get(route) or {}
    pool = spec.get("pool") or "unknown"
    model = spec.get("model") or route
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        # The primary key is (pool, model, window); the route label is
        # display. Clear by both so all windows go and an orphan row
        # whose label was overwritten by a pool/model sharer is not left.
        con.execute("DELETE FROM capacity WHERE route=? OR (pool=? AND model=?)",
                    (route, pool, model))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return {"route": route, "cleared": True}


def _probe_clears_mark(mark: dict, now_ts: float) -> bool:
    """True when a healthy probe (or a successful request) revalidates an
    exhausted capacity mark.

    A probe below 100 percent never clears a provider exhaustion before its
    reset_at; assumed and cooldown marks revalidate on any fresh healthy
    probe, since the reset time itself was derived. Marks with no reset
    time revalidate on the first healthy probe or success.
    """
    reset_at = mark.get("reset_at")
    if not reset_at:
        return True
    try:
        reset_ts = _parse_ts(reset_at)
    except Exception:
        return True
    if reset_ts is None:
        return True
    if (mark.get("reset_source") or "") != "provider":
        return True
    try:
        return float(now_ts) >= float(reset_ts)
    except (TypeError, ValueError):
        return False


def record_probe_outcome(state_dir, route: str, window: str, ok: bool,
                          detail=None, now_ts: float | None = None) -> dict:
    """Record one probe of an assumed capacity mark.

    The first success clears the mark; a failure pushes the next probe out
    on the lengthening schedule (one hour, doubling, six-hour cap). Every
    outcome is kept in ``capacity_probes`` so the real window boundaries are
    learned and handed to the observer.
    """
    spec = policy.route_spec(route)
    pool, model = spec["pool"], spec["model"]
    now = now_ts if now_ts is not None else time.time()
    ts = datetime.datetime.fromtimestamp(now, datetime.timezone.utc).isoformat()
    try:
        detail_json = json.dumps(adapters.redact_nested(detail or {}), sort_keys=True)[:2000]
    except Exception:
        detail_json = json.dumps({"recorded": True})
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "INSERT INTO capacity_probes(pool, model, window, ts, ok, detail_json)"
            " VALUES(?,?,?,?,?,?)", (pool, model, window, ts, 1 if ok else 0, detail_json))
        row = con.execute("SELECT * FROM capacity WHERE pool=? AND model=? AND window=?",
                          (pool, model, window)).fetchone()
        cleared = False
        held = False
        next_probe_at = None
        if row is not None and row["state"] == "exhausted":
            if ok:
                if _probe_clears_mark(dict(row), now):
                    con.execute("DELETE FROM capacity WHERE pool=? AND model=? AND window=?",
                                (pool, model, window))
                    cleared = True
                    _probe_event(con, "capacity_probe_cleared",
                                 {"route": route, "window": window})
                else:
                    # A probe below 100 percent never clears a provider
                    # exhaustion before its reset_at: the mark holds and the
                    # probe outcome stays on the ledger for the observer.
                    held = True
                    _probe_event(con, "capacity_probe_held",
                                 {"route": route, "window": window,
                                  "reset_at": row["reset_at"]})
            else:
                failures = int(row["probe_failures"] or 0) + 1
                next_probe_at = (datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
                                 + datetime.timedelta(
                                     seconds=policy.probe_delay_secs(failures))).isoformat()
                con.execute("UPDATE capacity SET probe_failures=?, next_probe_at=?, updated_at=?"
                            " WHERE pool=? AND model=? AND window=?",
                            (failures, next_probe_at, ts, pool, model, window))
                _probe_event(con, "capacity_probe_failed",
                             {"route": route, "window": window, "failures": failures,
                              "next_probe_at": next_probe_at})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return {"route": route, "window": window, "ok": bool(ok),
            "cleared": cleared, "held": held, "next_probe_at": next_probe_at}


def _probe_event(con, kind: str, payload: dict) -> None:
    """A probe event on the oldest job's ledger, if any job exists.

    Probes are route-level, not job-level, but the observer reads the event
    ledger; without a job there is no ledger to hang the event on and the
    ``capacity_probes`` table stays the record.
    """
    row = con.execute("SELECT request_id FROM jobs ORDER BY created_at LIMIT 1").fetchone()
    if row is not None:
        _event(con, row["request_id"], kind, payload)


def _validate_reading(pool, model, window, used, limit, reset_at,
                        observed_at, source) -> str:
    """Validated observed_at ISO timestamp, or raise."""
    if not isinstance(pool, str) or not pool.strip():
        raise ValueError("reading needs a pool")
    if not isinstance(model, str) or not model.strip():
        raise ValueError("reading needs a model")
    if not isinstance(window, str) or not window.strip():
        raise ValueError("reading needs a window")
    if source not in policy.READING_SOURCES:
        raise ValueError(f"invalid reading source: {source!r}")
    for name, val in (("used", used), ("limit", limit)):
        if val is None:
            continue
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            raise ValueError(f"reading {name} must be a number or None")
        if float(val) < 0:
            raise ValueError(f"reading {name} must not be negative")
    if reset_at is not None:
        if not isinstance(reset_at, str) or not reset_at.strip() \
                or _parse_ts(reset_at) is None:
            raise ValueError(f"unparseable reading reset_at: {reset_at!r}")
    if observed_at is None:
        return _utcnow()
    if not isinstance(observed_at, str) or _parse_ts(observed_at) is None:
        raise ValueError(f"unparseable reading observed_at: {observed_at!r}")
    return observed_at


def _reading_fraction(reading: dict) -> float | None:
    """used / limit, or None when the reading is unknown or has no limit."""
    try:
        used = reading.get("used")
        limit = reading.get("limit")
    except AttributeError:
        return None
    if used is None or limit is None:
        return None
    try:
        if isinstance(used, bool) or isinstance(limit, bool):
            return None
        limit_f = float(limit)
        if limit_f <= 0:
            return None
        return float(used) / limit_f
    except (TypeError, ValueError):
        return None


def _reading_row_to_dict(row) -> dict:
    d = dict(row)
    d["limit"] = d.pop("limit_value", None)
    return d


def record_reading(state_dir, pool: str, model: str, window: str,
                   used, limit, reset_at: str | None,
                   observed_at: str | None = None,
                   source: str = "measured", detail=None,
                   now_ts: float | None = None) -> dict:
    """Store one proactive usage-probe Reading in the capacity ledger.

    Keyed by pool, model, and window: used, limit, reset_at, observed_at,
    and source in {provider_reported, measured, derived, assumed}. used
    None means unknown (a failed or slow probe): it never marks a route.
    A healthy fresh reading revalidates an exhausted mark the same way a
    successful probe does, so provider marks still hold before reset_at
    while assumed marks learn the real boundary sooner. An exhausted
    reading never creates a mark on its own: error evidence does that, so
    an error always overrides a probe for the window it names.
    """
    observed = _validate_reading(pool, model, window, used, limit,
                                 reset_at, observed_at, source)
    observed_ts = _parse_ts(observed)
    if observed_ts is None:
        observed_ts = now_ts if now_ts is not None else time.time()
    try:
        detail_json = json.dumps(adapters.redact_nested(detail or {}), sort_keys=True)[:2000]
    except Exception:
        detail_json = json.dumps({"recorded": True})
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "INSERT INTO readings(pool, model, window, used, limit_value, reset_at,"
            " observed_at, source, detail_json, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(pool, model, window) DO UPDATE SET"
            " used=excluded.used, limit_value=excluded.limit_value,"
            " reset_at=excluded.reset_at, observed_at=excluded.observed_at,"
            " source=excluded.source, detail_json=excluded.detail_json,"
            " updated_at=excluded.updated_at",
            (pool, model, window,
             None if used is None else float(used),
             None if limit is None else float(limit),
             reset_at, observed, source, detail_json, _utcnow()),
        )
        revalidated = None
        fraction = _reading_fraction({"used": used, "limit": limit})
        if fraction is not None and fraction < 1.0 and observed_ts is not None:
            mark = con.execute("SELECT * FROM capacity WHERE pool=? AND model=? AND window=?",
                               (pool, model, window)).fetchone()
            if mark is not None and mark["state"] == "exhausted" \
                    and _probe_clears_mark(dict(mark), observed_ts):
                con.execute("DELETE FROM capacity WHERE pool=? AND model=? AND window=?",
                            (pool, model, window))
                revalidated = window
                _probe_event(con, "capacity_probe_cleared",
                             {"pool": pool, "model": model, "window": window,
                              "via": "reading"})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return {"pool": pool, "model": model, "window": window, "used": used,
            "limit": limit, "reset_at": reset_at, "observed_at": observed,
            "source": source, "revalidated": revalidated}


def note_probe_failure(state_dir, route: str, window: str, source: str = "assumed",
                       detail=None, observed_at: str | None = None) -> dict:
    """A failing or slow probe records unknown and never marks a route.

    The window's reading becomes used None (unknown) with its source
    semantics kept, a failed probe outcome is logged for the observer, and
    the route stays eligible on error evidence alone: capacity marks are
    untouched, so error evidence still governs.
    """
    spec = policy.route_spec(route)
    pool, model = spec["pool"], spec["model"]
    if source not in policy.READING_SOURCES:
        raise ValueError(f"invalid reading source: {source!r}")
    observed = observed_at or _utcnow()
    if _parse_ts(observed) is None:
        raise ValueError(f"unparseable reading observed_at: {observed!r}")
    try:
        detail_json = json.dumps(adapters.redact_nested(detail or {}), sort_keys=True)[:2000]
    except Exception:
        detail_json = json.dumps({"recorded": True})
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "INSERT INTO readings(pool, model, window, used, limit_value, reset_at,"
            " observed_at, source, detail_json, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(pool, model, window) DO UPDATE SET"
            " used=NULL, limit_value=NULL, reset_at=NULL,"
            " observed_at=excluded.observed_at, source=excluded.source,"
            " detail_json=excluded.detail_json, updated_at=excluded.updated_at",
            (pool, model, window, None, None, None,
             observed, source, detail_json, _utcnow()),
        )
        con.execute(
            "INSERT INTO capacity_probes(pool, model, window, ts, ok, detail_json)"
            " VALUES(?,?,?,?,?,?)", (pool, model, window, observed, 0, detail_json))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return {"pool": pool, "model": model, "window": window, "used": None,
            "limit": None, "reset_at": None, "observed_at": observed,
            "source": source}


def log_probe(state_dir, route: str, window: str, ok: bool, detail=None,
              now_ts: float | None = None) -> dict:
    """Append one usage-probe outcome to the probe log without touching
    capacity marks or readings. Failing outcomes are unknown evidence."""
    spec = policy.route_spec(route)
    now = now_ts if now_ts is not None else time.time()
    ts = datetime.datetime.fromtimestamp(now, datetime.timezone.utc).isoformat()
    try:
        detail_json = json.dumps(adapters.redact_nested(detail or {}), sort_keys=True)[:2000]
    except Exception:
        detail_json = json.dumps({"recorded": True})
    con = store.connect(state_dir)
    try:
        con.execute(
            "INSERT INTO capacity_probes(pool, model, window, ts, ok, detail_json)"
            " VALUES(?,?,?,?,?,?)",
            (spec["pool"], spec["model"], window, ts, 1 if ok else 0, detail_json))
        con.execute("COMMIT")
    finally:
        con.close()
    return {"route": route, "window": window, "ok": bool(ok), "ts": ts}


def list_readings(state_dir, route: str | None = None, pool: str | None = None,
                  model: str | None = None, window: str | None = None) -> list[dict]:
    """Usage-probe readings, optionally filtered. ``limit`` is exposed
    under its ledger name (stored as limit_value: LIMIT is reserved)."""
    clauses: list[str] = []
    args: list = []
    if route is not None:
        spec = policy.route_spec(route)
        pool, model = spec["pool"], spec["model"]
    for col, val in (("pool", pool), ("model", model), ("window", window)):
        if val is not None:
            clauses.append(f"{col}=?")
            args.append(val)
    query = "SELECT * FROM readings" + (" WHERE " + " AND ".join(clauses) if clauses else "") \
        + " ORDER BY pool, model, window"
    con = store.connect(state_dir)
    try:
        return [_reading_row_to_dict(r) for r in con.execute(query, args).fetchall()]
    finally:
        con.close()


def _routes_for_pool_model(pool: str, model: str) -> list[str]:
    """Policy routes drawing on this pool and model."""
    return [name for name, spec in policy.ROUTES.items()
            if spec.get("pool") == pool and spec.get("model") == model]


def _is_capacity_window(window) -> bool:
    """True when a reading window drives preflight.

    Only the policy's subscription windows (5h, weekly, monthly) skip or
    degrade routes. Session records and any unrecognized window stay on the
    ledger for the observer but never mark a route.
    """
    try:
        return str(window) in policy.WINDOWS
    except Exception:
        return False


def route_reading_states(state_dir, route: str, now_ts: float | None = None) -> dict:
    """Per-window reading state for a route: exhausted, degraded, ok, or
    unknown. A subscription window above the policy margin is degraded; 100
    percent is exhausted until its reset_at. Session and unrecognized
    windows are always ok (unknown when unreadable): they never mark a
    route. Unknown readings and expired resets are eligible on error
    evidence alone."""
    policy.route_spec(route)
    now = now_ts if now_ts is not None else time.time()
    out: dict = {}
    for reading in list_readings(state_dir, route=route):
        window = reading["window"]
        fraction = _reading_fraction(reading)
        if fraction is None:
            out[window] = "unknown"
            continue
        if not _is_capacity_window(window):
            out[window] = "ok"
            continue
        reset_at = reading.get("reset_at")
        reset_ts = _parse_ts(reset_at) if reset_at else None
        if reset_ts is not None and now >= reset_ts:
            out[window] = "ok"
            continue
        if fraction >= 1.0:
            out[window] = "exhausted"
        elif fraction >= float(policy.USAGE_DEGRADED_FRACTION):
            out[window] = "degraded"
        else:
            out[window] = "ok"
    return out


def reading_exhausted_routes(state_dir, now_ts: float | None = None) -> set[str]:
    """Routes with at least one exhausted subscription-window reading."""
    now = now_ts if now_ts is not None else time.time()
    out: set[str] = set()
    con = store.connect(state_dir)
    try:
        rows = con.execute("SELECT pool, model, window, used, limit_value, reset_at"
                           " FROM readings").fetchall()
    finally:
        con.close()
    for r in rows:
        if not _is_capacity_window(r["window"]):
            continue
        fraction = _reading_fraction({"used": r["used"], "limit": r["limit_value"]})
        if fraction is None or fraction < 1.0:
            continue
        if r["reset_at"]:
            reset_ts = _parse_ts(r["reset_at"])
            if reset_ts is not None and now >= reset_ts:
                continue
        out.update(_routes_for_pool_model(r["pool"], r["model"]))
    return out


def reading_degraded_routes(state_dir, now_ts: float | None = None) -> set[str]:
    """Routes with a subscription window at or above the policy margin."""
    now = now_ts if now_ts is not None else time.time()
    margin = float(policy.USAGE_DEGRADED_FRACTION)
    out: set[str] = set()
    con = store.connect(state_dir)
    try:
        rows = con.execute("SELECT pool, model, window, used, limit_value, reset_at"
                           " FROM readings").fetchall()
    finally:
        con.close()
    for r in rows:
        if not _is_capacity_window(r["window"]):
            continue
        fraction = _reading_fraction({"used": r["used"], "limit": r["limit_value"]})
        if fraction is None or fraction < margin or fraction >= 1.0:
            continue
        if r["reset_at"]:
            reset_ts = _parse_ts(r["reset_at"])
            if reset_ts is not None and now >= reset_ts:
                continue
        out.update(_routes_for_pool_model(r["pool"], r["model"]))
    return out


def record_route_success(state_dir, route: str, window: str | None = None,
                         now_ts: float | None = None) -> dict:
    """A successful request revalidates exhaustion marks the same way a
    healthy probe does: assumed marks clear at once, provider marks only
    after their reset_at. Future provider marks hold: success elsewhere
    never invents a reset. Returns the cleared windows."""
    spec = policy.route_spec(route)
    pool, model = spec["pool"], spec["model"]
    now = now_ts if now_ts is not None else time.time()
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        query = "SELECT * FROM capacity WHERE pool=? AND model=? AND state='exhausted'"
        args: list = [pool, model]
        if window is not None:
            query += " AND window=?"
            args.append(window)
        cleared: list[str] = []
        for mark in con.execute(query, args).fetchall():
            if _probe_clears_mark(dict(mark), now):
                con.execute("DELETE FROM capacity WHERE pool=? AND model=? AND window=?",
                            (pool, model, mark["window"]))
                cleared.append(mark["window"])
        if cleared:
            _probe_event(con, "capacity_success_cleared",
                         {"route": route, "windows": sorted(cleared)})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return {"route": route, "cleared": sorted(cleared)}


def probe_due_for(harness_name: str, observed_at: str | None,
                  now_ts: float | None = None) -> bool:
    """True when a harness usage probe is due: never probed, unparseable,
    or older than the policy interval (Codex 300s, Claude 180s)."""
    interval = policy.PROBE_INTERVAL_SECS.get(harness_name)
    if interval is None:
        raise ValueError(f"unknown harness for probing: {harness_name!r}")
    now = now_ts if now_ts is not None else time.time()
    if not observed_at:
        return True
    try:
        obs_ts = _parse_ts(observed_at)
    except Exception:
        return True
    if obs_ts is None:
        return True
    return (now - obs_ts) >= float(interval)


def probe_due_routes(state_dir, now_ts: float | None = None) -> list[dict]:
    """Exhausted marks needing operator-driven revalidation.

    Assumed weekly and monthly marks whose next probe is due (the probe
    learns the real boundary sooner), plus any exhausted mark whose reset
    has passed: after reset_at the route needs one fresh probe or one
    successful request before it is eligible again, so expired marks stay
    on this worklist until record_probe_outcome, a healthy fresh reading,
    or record_route_success clears them.
    """
    now = now_ts if now_ts is not None else time.time()
    con = store.connect(state_dir)
    try:
        rows = con.execute(
            "SELECT * FROM capacity WHERE state='exhausted'"
            " ORDER BY COALESCE(next_probe_at, reset_at)").fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        probe_ts = _parse_ts(r["next_probe_at"]) if r["next_probe_at"] else None
        if probe_ts is not None and now >= probe_ts:
            out.append(dict(r))
            continue
        reset_ts = _parse_ts(r["reset_at"]) if r["reset_at"] else None
        if reset_ts is not None and now >= reset_ts:
            out.append(dict(r))
    return out


def list_probes(state_dir, route: str | None = None, window: str | None = None) -> list[dict]:
    """Recorded probe outcomes, optionally for one route and window."""
    con = store.connect(state_dir)
    try:
        if route is not None:
            spec = policy.route_spec(route)
            rows = con.execute(
                "SELECT * FROM capacity_probes WHERE pool=? AND model=?"
                + (" AND window=?" if window else "") + " ORDER BY id",
                ((spec["pool"], spec["model"], window) if window
                 else (spec["pool"], spec["model"],))).fetchall()
        else:
            rows = con.execute("SELECT * FROM capacity_probes ORDER BY id").fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def _routes_in_state(state_dir, state: str) -> set[str]:
    now = time.time()
    out = set()
    con = store.connect(state_dir)
    try:
        rows = con.execute("SELECT route, state, reset_at FROM capacity").fetchall()
    finally:
        con.close()
    for r in rows:
        if r["state"] != state:
            continue
        if state == "exhausted":
            # Sticky (#34): an error always overrides a probe, so an
            # exhaustion mark holds until one fresh probe or one successful
            # request revalidates it, even past its reset_at. Degraded
            # cooldowns still expire on their own below.
            out.add(r["route"])
            continue
        reset_at = r["reset_at"]
        if reset_at:
            ts = _parse_ts(reset_at)
            if ts is not None and now >= ts:
                continue
        out.add(r["route"])
    if state == "exhausted":
        out |= reading_exhausted_routes(state_dir, now_ts=now)
    elif state == "degraded":
        out |= reading_degraded_routes(state_dir, now_ts=now)
    return out


def exhausted_routes(state_dir) -> set[str]:
    """Routes known exhausted: sticky error marks plus exhausted window
    readings. A mark holds past its reset_at until one fresh probe or one
    successful request revalidates it; a probe below 100 percent never
    clears it before reset_at."""
    return _routes_in_state(state_dir, "exhausted")


def degraded_routes(state_dir) -> set[str]:
    """Routes resting (overload cooldowns) or with a window reading at or
    above the policy margin but below 100 percent."""
    return _routes_in_state(state_dir, "degraded")


def _running_counts_locked(con, exclude: str | None = None) -> dict[str, int]:
    """Running jobs per route counted inside the caller's write transaction."""
    if exclude:
        rows = con.execute("SELECT route, COUNT(*) AS n FROM jobs"
                           " WHERE status='running' AND request_id != ?"
                           " GROUP BY route", (exclude,)).fetchall()
    else:
        rows = con.execute("SELECT route, COUNT(*) AS n FROM jobs"
                           " WHERE status='running' GROUP BY route").fetchall()
    counts = {r["route"]: int(r["n"]) for r in rows}
    return {r: n for r, n in counts.items() if r in policy.ROUTES}


def running_job_counts(state_dir, exclude: str | None = None) -> dict[str, int]:
    """Running jobs per requested route (capacity windows for sticky homes).

    ``exclude`` drops one job's own count so a running job does not read
    as filling its own route's window.
    """
    con = store.connect(state_dir)
    try:
        return _running_counts_locked(con, exclude=exclude)
    finally:
        con.close()


def _route_full_locked(con, route: str, exclude: str | None = None) -> bool:
    """True when the route's max_concurrent is used, counted in-transaction."""
    cap = policy.route_max_concurrent(route)
    if cap is None:
        return False
    return _running_counts_locked(con, exclude=exclude).get(route, 0) >= cap


def route_concurrency_full(state_dir, route: str, exclude: str | None = None) -> bool:
    """True when the route's max_concurrent is already used by running jobs."""
    con = store.connect(state_dir)
    try:
        return _route_full_locked(con, route, exclude=exclude)
    finally:
        con.close()


def _capacity_sets_locked(con) -> tuple[set[str], set[str]]:
    """(exhausted, degraded) routes from the capacity memory, read inside the
    caller's write transaction so sticky selection and the route record are
    atomic. Exhaustion marks are sticky past reset_at until revalidated;
    degraded cooldowns expire on their own. Window readings join both sets
    so preflight and sticky homes consult them atomically."""
    now = time.time()
    exhausted: set[str] = set()
    degraded: set[str] = set()
    for r in con.execute("SELECT route, state, reset_at FROM capacity").fetchall():
        if r["state"] not in ("exhausted", "degraded"):
            continue
        if r["state"] == "exhausted":
            exhausted.add(r["route"])
            continue
        reset_at = r["reset_at"]
        if reset_at:
            ts = _parse_ts(reset_at)
            if ts is not None and now >= ts:
                continue
        degraded.add(r["route"])
    try:
        margin = float(policy.USAGE_DEGRADED_FRACTION)
    except (TypeError, ValueError):
        margin = 0.8
    for rd in con.execute("SELECT pool, model, window, used, limit_value, reset_at"
                          " FROM readings").fetchall():
        if not _is_capacity_window(rd["window"]):
            continue
        used, limit = rd["used"], rd["limit_value"]
        if used is None or limit is None:
            continue
        try:
            if isinstance(used, bool) or isinstance(limit, bool) \
                    or float(limit) <= 0:
                continue
            fraction = float(used) / float(limit)
        except (TypeError, ValueError):
            continue
        if rd["reset_at"]:
            reset_ts = _parse_ts(rd["reset_at"])
            if reset_ts is not None and now >= reset_ts:
                continue
        for route in _routes_for_pool_model(rd["pool"], rd["model"]):
            if fraction >= 1.0:
                exhausted.add(route)
            elif fraction >= margin:
                degraded.add(route)
    return exhausted, degraded


def _sticky_home_locked(con, lane: str) -> str | None:
    """Muse-free-first sticky home, counted inside the caller's write
    transaction, so the count and the later route record are atomic.

    The lane's first route (Muse on Zen free, no concurrency cap) takes every
    new job and is skipped only when capacity memory marks it exhausted or
    degraded. Uncapped routes never spread by load: the first eligible
    uncapped route in lane order wins, so parallel jobs open parallel Muse
    free sessions by design. Only when no uncapped route is eligible does the
    choice spread among capped routes by fewest running jobs (ties go to the
    earlier route; full, exhausted, or degraded capped routes are excluded).
    None when no route in the lane is eligible."""
    stage = policy.resolve_lane(lane)
    routes = policy.stage_routes(stage)
    counts = _running_counts_locked(con)
    exhausted, degraded = _capacity_sets_locked(con)
    skip = exhausted | degraded
    for route in routes:
        if policy.route_max_concurrent(route) is None:
            if route in skip:
                continue
            return route
    best = None
    for route in routes:
        cap = policy.route_max_concurrent(route)
        if cap is None:
            continue
        if route in skip:
            continue
        if counts.get(route, 0) >= cap:
            continue
        if best is None or counts.get(route, 0) < counts.get(best, 0):
            best = route
    return best


def sticky_home_route(state_dir, lane: str) -> str | None:
    """Muse on Zen free takes every new job (no concurrency cap; skipped only
    when exhausted or degraded); the `fewest running jobs` spread applies only
    among capped routes. None when no route in the lane is eligible."""
    con = store.connect(state_dir)
    try:
        return _sticky_home_locked(con, lane)
    finally:
        con.close()


def _next_capable_locked(con, current: str, lane: str | None = None,
                         exclude: str | None = None,
                         turns_by_route: dict | None = None) -> str | None:
    """Next eligible route with free concurrency, counted inside the write
    transaction that will record the move. Recovery routes reason in the
    recovery stage. Lane membership comes from the stage order itself;
    every candidate also passes capacity state, one-turn use, and cap.
    Callers pass the job's turns (controller._turns_by_route); the core
    never branches on harness kind, per the harness seam."""
    try:
        stage = policy.lane_of_route(current, lane)
    except ValueError:
        return None
    if stage is None:
        return None
    order = policy.stage_routes(stage)
    if current not in order:
        return None
    counts = _running_counts_locked(con, exclude=exclude)
    exhausted, degraded = _capacity_sets_locked(con)
    skip = exhausted | degraded
    turns_by_route = turns_by_route or {}
    for route in order[order.index(current) + 1:]:
        if route in skip:
            continue
        if policy.one_turn_routes_used(route, turns_by_route):
            continue
        cap = policy.route_max_concurrent(route)
        if cap is not None and counts.get(route, 0) >= cap:
            continue
        return route
    return None


def next_capable_route(state_dir, current: str, lane: str | None = None,
                       exclude: str | None = None,
                       turns_by_route: dict | None = None) -> str | None:
    """Next eligible route in the lane, skipping exhausted, degraded,
    already-used one-turn, lane-outside, and concurrency-full routes."""
    con = store.connect(state_dir)
    try:
        return _next_capable_locked(con, current, lane, exclude=exclude,
                                    turns_by_route=turns_by_route)
    finally:
        con.close()


def _dispatch_route_counts_locked(con, exclude: str | None = None) -> dict[str, int]:
    """Running jobs per dispatch route, counted in-transaction from the saved
    ``dispatch_route`` in controller state (jobs.route stays the implementation
    route, so the generic route counts never see dispatch occupancy)."""
    import json as _json
    if exclude:
        rows = con.execute("SELECT request_id, controller_state FROM jobs"
                           " WHERE status='running' AND request_id != ?",
                           (exclude,)).fetchall()
    else:
        rows = con.execute("SELECT request_id, controller_state FROM jobs"
                           " WHERE status='running'").fetchall()
    counts: dict[str, int] = {}
    for r in rows:
        try:
            st = _json.loads(r["controller_state"] or "{}") or {}
        except ValueError:
            continue
        d = st.get("dispatch_route")
        if isinstance(d, str) and d in policy.ROUTES:
            counts[d] = counts.get(d, 0) + 1
    return counts


def _dispatch_route_full_locked(con, route: str, exclude: str | None = None) -> bool:
    cap = policy.route_max_concurrent(route)
    if cap is None:
        return False
    return _dispatch_route_counts_locked(con, exclude=exclude).get(route, 0) >= cap


def dispatch_route_full(state_dir, route: str, exclude: str | None = None) -> bool:
    """True when a dispatch route's max_concurrent is used by running jobs."""
    con = store.connect(state_dir)
    try:
        return _dispatch_route_full_locked(con, route, exclude=exclude)
    finally:
        con.close()


def select_implementation_route(state_dir, current: str, error,
                                lane: str | None = None) -> tuple[str | None, str | None]:
    """Pick the next implementation route in the job's lane, skipping
    known-exhausted ones. Returns (route, blocker); blocker is None because
    every policy route has an adapter. Does not wait on an exhausted route.
    """
    exhausted = exhausted_routes(state_dir)
    if policy.classify_quota_exhaustion(error):
        exhausted.add(current)
        return policy.next_capacity_route(current, exhausted, lane)
    return None, None


def recover_all(state_dir) -> list[dict]:
    con = store.connect(state_dir)
    try:
        rows = con.execute("SELECT request_id FROM jobs ORDER BY created_at").fetchall()
        ids = [r["request_id"] for r in rows]
    finally:
        con.close()
    return [recover_one(state_dir, rid) for rid in ids]


def latest_turn_report(state_dir, request_id: str) -> dict | None:
    """Latest implementation turn report, or None when no turn ran yet.

    Reads every ``turn-*/report.json`` under the job directory and returns
    the one with the highest ``seq`` (ties break on the path, so a suffixed
    retry ``turn-<seq>-<n>`` wins over the original). Never raises: an
    unreadable directory or report is None, which lets completion proceed.
    """
    try:
        root = store.ensure_state_dir(state_dir)
    except Exception:
        return None
    try:
        job_dir = store.job_dir_for(root, request_id)
    except Exception:
        return None
    try:
        exists = job_dir.exists()
    except Exception:
        return None
    if not exists:
        return None
    best: dict | None = None
    best_seq = -1
    best_retry = -1
    best_path = ""
    try:
        candidates = list(job_dir.glob("turn-*/report.json"))
    except Exception:
        return None
    for path in candidates:
        try:
            rep = json.loads(Path(path).read_text(encoding="utf-8", errors="replace"))
        except (OSError, ValueError):
            continue
        if not isinstance(rep, dict):
            continue
        try:
            seq = int(rep.get("seq")) if rep.get("seq") is not None else -1
        except (TypeError, ValueError):
            seq = -1
        spath = str(path)
        try:
            dirname = Path(path).parent.name
        except Exception:
            dirname = ""
        base = f"turn-{seq}"
        retry = 0
        if dirname != base and dirname.startswith(base + "-"):
            suffix = dirname[len(base) + 1:]
            try:
                retry = int(suffix)
            except (TypeError, ValueError):
                # Any suffixed retry still wins over the original.
                retry = 1
        if best is None or seq > best_seq or (
            seq == best_seq
            and (retry, spath) > (best_retry, best_path)
        ):
            best = rep
            best_seq = seq
            best_retry = retry
            best_path = spath
    if best is not None and not best.get("report_path"):
        best = dict(best)
        best["report_path"] = best_path
    return best


def completion_refusal_reason(report: dict | None) -> str | None:
    """Block reason when completion must be refused for a failed last turn.

    A non-zero ``proof_exit_code`` or a ``failed`` report status refuses
    completion as ``completion_refused: ...``. None lets completion proceed
    (no turn yet, or the last turn passed).
    """
    if not isinstance(report, dict):
        return None
    proof_rc = report.get("proof_exit_code")
    status = report.get("status")
    if isinstance(proof_rc, bool):
        proof_rc = int(proof_rc)
    if isinstance(proof_rc, int) and proof_rc != 0:
        # A proof timeout is classified apart from an executable-not-found
        # rc127: the suite ran past its budget and its whole process tree
        # was stopped, which is a different cause with a different remedy.
        if report.get("proof_class") == "timeout":
            return (f"completion_refused: proof timed out rc={proof_rc} "
                    "(the suite ran past its budget and its process tree was stopped)")
        return f"completion_refused: proof failed rc={proof_rc}"
    if status == "failed":
        if isinstance(proof_rc, int) and proof_rc != 0:
            return f"completion_refused: proof failed rc={proof_rc}"
        err = report.get("error")
        try:
            detail = json.dumps(err, sort_keys=True)[:200] if err else "failed"
        except (TypeError, ValueError):
            detail = str(err)[:200] if err else "failed"
        detail = " ".join(str(detail).split())
        return f"completion_refused: last turn failed ({detail[:160]})"
    return None


def workspace_has_origin_push_remote(workspace: str | None) -> bool:
    """True when the workspace can open a PR: a Git checkout with an origin push remote.

    Checked with git exactly where the caller needs the decision (once per
    completion in each path). Non-Git workspaces, Git without origin, or an
    origin without a usable push URL return False so ordinary completion
    succeeds as before.
    """
    if not workspace or not isinstance(workspace, str):
        return False
    try:
        out = subprocess.run(["git", "-C", workspace, "remote", "get-url",
                              "--push", "origin"],
                             capture_output=True, text=True, timeout=5,
                             stdin=subprocess.DEVNULL)
    except Exception:
        return False
    if out.returncode != 0:
        return False
    return bool((out.stdout or "").strip())


def pr_gate_applies(job_kind: str | None, workspace: str | None) -> bool:
    """True when an ordinary completion must carry a verified PR URL.

    Exactly when the workspace is a Git checkout with an origin push
    remote (checked with git once per call) and the job is ordinary.
    Experiment and replay jobs, and workspaces without a push remote,
    complete as before with no PR identity or live check.
    """
    if job_kind in ("experiment", "replay"):
        return False
    return workspace_has_origin_push_remote(workspace)


def missing_pr_url_reason(job_kind: str | None, envelope: dict | None,
                          workspace: str | None = None) -> str | None:
    """Block reason when an ordinary completion carries no usable PR URL.

    The PR-URL requirement applies exactly when the workspace is a Git
    checkout with an origin push remote (checked with git once per call;
    callers invoke this once per completion, so no duplicate checks). A
    non-Git workspace, one without origin, or one without a usable push
    remote completes successfully as before. Experiment and replay jobs
    keep their current behavior (None: the gate does not apply). With a
    remote, missing, None, non-string, empty, and whitespace-only values
    refuse as ``completion_refused: ...`` naming the missing PR URL, never
    as a proof failure; a valid nonblank URL succeeds and is preserved.
    """
    if not pr_gate_applies(job_kind, workspace):
        return None
    pr_url = envelope.get("pr_url") if isinstance(envelope, dict) else None
    if isinstance(pr_url, str) and pr_url.strip():
        return None
    return ("completion_refused: missing pr_url "
            "(an ordinary job succeeds only with an opened PR URL)")


def task_completion_requirements(task_json: str | None) -> dict:
    """Completion requirements named by the task packet, for the gates.

    ``has_proof``: the task names a proof command. ``acceptance``: the
    task's required acceptance evidence (a short description such as
    ``real product interaction with the fleet pane``); completion then
    needs the envelope's ``acceptance_evidence``. ``draft_pr_allowed``:
    the task explicitly authorizes a draft PR. Missing fields stay
    unknown/False: nothing is inferred from a shared session or an Issue
    URL.
    """
    try:
        task = json.loads(task_json or "null")
    except ValueError:
        task = None
    if not isinstance(task, dict):
        return {"has_proof": False, "acceptance": None,
                "draft_pr_allowed": False}
    proof = task.get("proof")
    acceptance = task.get("acceptance")
    return {
        "has_proof": isinstance(proof, str) and bool(proof.strip()),
        "acceptance": acceptance.strip() if isinstance(acceptance, str)
        and acceptance.strip() else None,
        "draft_pr_allowed": task.get("draft_pr_allowed") is True,
    }


def incomplete_proof_reason(task_json: str | None, report: dict | None,
                            workspace: str | None) -> str | None:
    """Refuse completion when the bound proof is missing, stale, or skipped.

    The task's required proof must be bound to the current candidate: the
    latest report carries the workspace HEAD the proof ran against
    (``head_commit``), and completion compares it with the current HEAD.
    A skipped proof (an exhausted, stalled, crashed, or otherwise
    incomplete turn whose suite was deliberately not run) refuses as
    incomplete, never as success: the dispatcher requests implementation
    again for a coherent candidate. Reports that predate the binding
    (no ``head_commit``) read as unknown and pass this gate; required
    final verification is still enforced by ``completion_refusal_reason``.
    """
    reqs = task_completion_requirements(task_json)
    if not reqs["has_proof"]:
        return None
    if not isinstance(report, dict):
        return ("completion_refused: incomplete proof "
                "(no implementation turn ran; request implementation again "
                "so the task proof runs against the candidate)")
    if report.get("proof_class") == "skipped" or (
            report.get("proof_exit_code") is None
            and report.get("proof_command")):
        skipped = report.get("proof_skipped") or \
            "the suite was not run for this turn"
        return (f"completion_refused: incomplete proof ({skipped}); request "
                "implementation again for a coherent candidate instead of "
                "completing without proof")
    head = report.get("head_commit")
    if isinstance(head, str) and head:
        current = _workspace_head(workspace)
        if isinstance(current, str) and current and current != head:
            return (f"completion_refused: stale proof (proof ran against "
                    f"{head[:12]}, workspace HEAD is {current[:12]}; request "
                    "implementation again so proof binds the candidate)")
    return None


def incomplete_acceptance_reason(task_json: str | None,
                                 envelope: dict | None) -> str | None:
    """Refuse completion when named acceptance evidence is missing.

    Completion and acceptance stay distinct and tied to the candidate: a
    worker exit zero, passing helper tests, and an open PR cannot
    establish completion when the task's required acceptance evidence
    (for example a real product interaction or an independent review) is
    missing. The completion envelope then carries it as
    ``acceptance_evidence``. Tasks that name no acceptance pass.
    """
    reqs = task_completion_requirements(task_json)
    if not reqs["acceptance"]:
        return None
    ev = envelope.get("acceptance_evidence") if isinstance(envelope, dict) else None
    if isinstance(ev, str) and ev.strip():
        return None
    return (f"completion_refused: missing acceptance evidence (task requires: "
            f"{reqs['acceptance'][:200]}; carry acceptance_evidence in the "
            "completion envelope)")


def duplicate_pr_reason(known_pr_url: str | None,
                        envelope: dict | None) -> str | None:
    """Refuse a completion that names a different PR than the job's own.

    One job owns exactly one PR identity: correction and recovery update
    the existing PR, never open a second. The first usable PR URL wins;
    a later envelope naming another URL refuses as a duplicate instead
    of forking the job's identity.
    """
    cur = envelope.get("pr_url") if isinstance(envelope, dict) else None
    if not (isinstance(known_pr_url, str) and known_pr_url.strip()):
        return None
    if not (isinstance(cur, str) and cur.strip()):
        return None
    if known_pr_url.strip() == cur.strip():
        return None
    return (f"completion_refused: duplicate PR (job already uses {known_pr_url.strip()}; "
            "update that PR instead of opening another)")


def known_pr_url(state_dir, request_id: str) -> str | None:
    """The job's preserved PR identity, if a completion named one yet."""
    try:
        job = get_job(state_dir, request_id)
    except NotFoundError:
        return None
    try:
        st = json.loads(job.get("controller_state") or "{}") or {}
    except ValueError:
        return None
    known = st.get("pr_url") if isinstance(st, dict) else None
    return known.strip() if isinstance(known, str) and known.strip() else None


def record_known_pr(state_dir, request_id: str, pr_url: str | None,
                    lease_token: str | None = None) -> str | None:
    """Preserve the job's PR identity from a completion envelope.

    The first usable PR URL wins and is returned; a later call with the
    same URL keeps it. A different URL never replaces it (callers refuse
    that envelope via :func:`duplicate_pr_reason`). Never raises past
    the caller on persistence failure: the identity is advisory beside
    the envelope, while the refusal itself is recorded by the caller.
    """
    if not (isinstance(pr_url, str) and pr_url.strip()):
        return known_pr_url(state_dir, request_id)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        if lease_token is not None:
            _check_lease_locked(con, request_id, lease_token)
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        if row is None:
            con.execute("ROLLBACK")
            return None
        try:
            st = json.loads(row["controller_state"] or "{}") or {}
        except ValueError:
            st = {}
        if not isinstance(st, dict):
            st = {}
        if not (isinstance(st.get("pr_url"), str) and st["pr_url"].strip()):
            st["pr_url"] = pr_url.strip()
            con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                        (json.dumps(st, sort_keys=True), _utcnow(), request_id))
            _event(con, request_id, "pr_identity_preserved",
                   {"pr_url": pr_url.strip()[:200]})
        known = st.get("pr_url")
        con.execute("COMMIT")
        return known.strip() if isinstance(known, str) else None
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        return known_pr_url(state_dir, request_id)
    finally:
        con.close()


# The live PR check runs ``gh pr view`` once per completion through this
# seam so deterministic tests stub it without network or auth. The seam
# takes (workspace, pr_url) and returns a dict: {"ok": bool,
# "state": "OPEN"|..., "is_draft": bool, "head_sha": str|None,
# "repo": "owner/name"|None, "reason": str, "unknown": bool}. ``unknown``
# marks an unavailable check (no gh, no auth), which refuses as
# unverified rather than inventing an answer.
PR_VERIFIER = None


def default_pr_verifier(workspace: str | None, pr_url: str) -> dict:
    """Read one PR's live state with ``gh pr view`` (read-only)."""
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", pr_url, "--json",
             "number,state,isDraft,headRefOid,url,headRepository"],
            capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
            cwd=workspace or None)
    except FileNotFoundError:
        return {"ok": False, "unknown": True,
                "reason": "pr verifier unavailable: gh not installed"}
    except OSError as e:
        return {"ok": False, "unknown": True,
                "reason": f"pr verifier unavailable: {type(e).__name__}: {e}"}
    if proc.returncode != 0:
        detail = " ".join(((proc.stderr or "") + " " + (proc.stdout or "")).split())[:200]
        return {"ok": False, "unknown": False,
                "reason": f"gh pr view failed rc={proc.returncode}: {detail or 'no such PR'}"}
    try:
        data = json.loads(proc.stdout or "null")
    except ValueError:
        return {"ok": False, "unknown": True,
                "reason": "pr verifier unreadable: gh printed no JSON"}
    if not isinstance(data, dict):
        return {"ok": False, "unknown": True,
                "reason": "pr verifier unreadable: gh printed no object"}
    repo = data.get("headRepository")
    if isinstance(repo, dict):
        repo = repo.get("nameWithOwner")
    return {"ok": True, "unknown": False,
            "state": data.get("state"), "is_draft": data.get("isDraft") is True,
            "head_sha": data.get("headRefOid")
            if isinstance(data.get("headRefOid"), str) else None,
            "repo": repo if isinstance(repo, str) and repo else None,
            "url": data.get("url") if isinstance(data.get("url"), str) else None,
            "reason": ""}


def _repo_from_pr_url(pr_url: str | None) -> str | None:
    """``owner/name`` from a PR URL path, else None (never guessed)."""
    if not isinstance(pr_url, str) or "github.com" not in pr_url:
        return None
    tail = pr_url.split("github.com", 1)[1].lstrip("/:")
    parts = [p for p in tail.split("/") if p]
    if len(parts) < 2:
        return None
    owner, name = parts[0], parts[1]
    if not owner or not name:
        return None
    return f"{owner}/{name}"


def _origin_repo(workspace: str | None) -> str | None:
    """``owner/name`` of the workspace's GitHub origin push remote, else None.

    Best-effort and offline (one local git read): non-GitHub remotes
    and unparseable URLs stay unknown rather than blocking completion
    on a guess.
    """
    if not workspace or not isinstance(workspace, str):
        return None
    try:
        out = subprocess.run(["git", "-C", workspace, "remote", "get-url",
                              "--push", "origin"],
                             capture_output=True, text=True, timeout=5,
                             stdin=subprocess.DEVNULL)
    except Exception:
        return None
    if out.returncode != 0:
        return None
    text = (out.stdout or "").strip()
    if not text or "github.com" not in text:
        return None
    # https://github.com/owner/name(.git), git@github.com:owner/name(.git),
    # ssh://git@github.com/owner/name(.git).
    tail = text.split("github.com", 1)[1].lstrip("/:")
    parts = [p for p in tail.split("/") if p]
    if len(parts) < 2:
        return None
    owner, name = parts[-2], parts[-1]
    name = name[:-4] if name.endswith(".git") else name
    if not owner or not name:
        return None
    return f"{owner}/{name}"


def verify_pr_for_completion(workspace: str | None, pr_url: str,
                             head_commit: str | None, task_json: str | None,
                             verifier=None) -> str | None:
    """Bind the completion PR to the current candidate, live.

    A nonblank URL alone is insufficient: the identified PR must exist
    in the intended repository, be open, and point at the intended
    current commit (the HEAD the bound proof ran against). A draft PR is
    valid only when the task explicitly authorizes it
    (``draft_pr_allowed``); draft status itself is not a defect. An
    unavailable check refuses as unverified, never as verified. Returns
    None when the PR verifies, else a ``completion_refused`` reason.
    """
    check = verifier or PR_VERIFIER or default_pr_verifier
    try:
        seen = check(workspace, pr_url)
    except Exception as e:  # noqa: BLE001 - an unreadable check is unknown
        return (f"completion_refused: pr_unverified (PR check failed: "
                f"{type(e).__name__}: {str(e)[:150]}; cannot confirm the PR "
                "exists and is open)")
    if not isinstance(seen, dict) or not seen.get("ok"):
        reason = (seen.get("reason") if isinstance(seen, dict) else None) or "PR check failed"
        if isinstance(seen, dict) and seen.get("unknown"):
            return (f"completion_refused: pr_unverified ({reason}; cannot confirm "
                    "the PR exists and is open)")
        return f"completion_refused: pr_check_failed ({reason})"
    if str(seen.get("state") or "").upper() != "OPEN":
        return (f"completion_refused: pr_not_open "
                f"(state={str(seen.get('state') or 'unknown')[:20]}; only an open PR completes a job)")
    if seen.get("is_draft"):
        reqs = task_completion_requirements(task_json)
        if not reqs["draft_pr_allowed"]:
            return ("completion_refused: draft PR not explicitly authorized "
                    "(set draft_pr_allowed in the task to complete with a draft)")
    origin = _origin_repo(workspace)
    repo = seen.get("repo")
    if not (isinstance(repo, str) and repo):
        # gh shapes vary by version (headRepository without
        # nameWithOwner here): fall back to the canonical PR URL the
        # check itself resolved, never a guess.
        repo = _repo_from_pr_url(seen.get("url")) or _repo_from_pr_url(pr_url)
    if origin and isinstance(repo, str) and repo and origin != repo:
        return (f"completion_refused: pr_wrong_repo (PR is in {repo}, "
                f"workspace origin is {origin})")
    head_sha = seen.get("head_sha")
    if isinstance(head_sha, str) and head_sha:
        if isinstance(head_commit, str) and head_commit and head_commit != head_sha:
            return (f"completion_refused: pr_head_mismatch (PR points at "
                    f"{head_sha[:12]}, bound proof ran against {head_commit[:12]}; "
                    "update the PR to the candidate commit)")
        current = _workspace_head(workspace)
        if isinstance(current, str) and current and current != head_sha:
            return (f"completion_refused: pr_head_mismatch (PR points at "
                    f"{head_sha[:12]}, workspace HEAD is {current[:12]})")
    return None


def result_view(state_dir, request_id: str) -> dict:
    """Full terminal result, including Luna's completion report. Printed only
    on explicit request; `status` shows a summary."""
    job = get_job(state_dir, request_id)
    try:
        res = json.loads(job.get("result_json") or "null")
    except ValueError:
        res = None
    if isinstance(res, dict) and isinstance(res.get("output"), str):
        try:
            res["output"] = json.loads(res["output"])
        except ValueError:
            pass
    root = store.ensure_state_dir(state_dir)
    job_dir = store.job_dir_for(root, request_id)
    reports = sorted(str(p) for p in job_dir.glob("turn-*/report.json")) if job_dir.exists() else []
    return {"request_id": request_id, "status": job["status"], "result": res,
            "result_path": str(store.result_path_for(root, request_id)),
            "base_commit": job.get("base_commit"), "head_commit": job.get("head_commit"),
            "job_kind": job.get("job_kind"), "replay_of": job.get("replay_of"),
            "reports": reports, "measurements": invocation_measurements(state_dir, request_id),
            "capacity": list_capacity(state_dir), "readings": list_readings(state_dir)}


def status_view(state_dir, request_id: str) -> dict:
    job = get_job(state_dir, request_id)
    con = store.connect(state_dir)
    try:
        launches = [dict(r) for r in con.execute(
            "SELECT attempt_no,start_token,pid,state,created_at,ack_at FROM launches WHERE request_id=? ORDER BY attempt_no",
            (request_id,)).fetchall()]
        # Redact tokens in the public view to short prefixes; full tokens
        # stay in the DB for ownership checks and never appear in logs.
        for L in launches:
            t = L.get("start_token") or ""
            L["start_token"] = (t[:8] + "...") if len(t) > 8 else "***"
        questions = [dict(r) for r in con.execute(
            "SELECT qid,prompt,status,created_at,answered_at FROM questions WHERE request_id=? ORDER BY id",
            (request_id,)).fetchall()]
        events = [dict(r) for r in con.execute(
            "SELECT ts,kind,payload_json FROM events WHERE request_id=? ORDER BY id DESC LIMIT 20",
            (request_id,)).fetchall()]
    finally:
        con.close()
    root = store.ensure_state_dir(state_dir)
    log = store.output_path_for(root, request_id)
    tail = ""
    try:
        data = log.read_bytes().decode("utf-8", errors="replace")
        tail = data[-2000:]
    except FileNotFoundError:
        tail = ""
    except OSError:
        tail = ""
    job_public = dict(job)
    job_public.pop("task_json", None)  # never echo task bodies in status
    # Model-authored text (Luna envelopes, worker reports, stderr) stays in
    # private files and the database; status shows runner summaries only.
    try:
        st = json.loads(job_public.get("controller_state") or "{}") or {}
    except ValueError:
        st = {}
    job_public["controller_state"] = {k: st.get(k) for k in ("phase", "last_action_name", "seq")}
    try:
        le = json.loads(job_public.get("last_error_json") or "null")
    except ValueError:
        le = None
    job_public["last_error_json"] = ({k: le.get(k) for k in ("source", "rc", "error", "quota", "signal",
                                                             "evidence", "idle_confirmed", "retry_next",
                                                             "retry_next_capped", "overload_retries",
                                                             "probe_signal", "longest_silence_secs")}
                                     if isinstance(le, dict) else None)
    try:
        res = json.loads(job_public.get("result_json") or "null")
    except ValueError:
        res = None
    job_public["result_json"] = ({"ok": res.get("ok"), "cancelled": res.get("cancelled"),
                                  "error_code": (res.get("error") or {}).get("code")
                                  if isinstance(res.get("error"), dict) else None}
                                 if isinstance(res, dict) else None)
    if job_public.get("owner_token"):
        job_public["owner_token"] = "<redacted>"
    job_public["measurements"] = invocation_measurements(state_dir, request_id)
    try:
        installed = runtime.installed_runtime()
    except Exception:
        installed = {"root": None, "version": "unknown", "policy_id": None,
                     "policy_version": None, "schema_version": None}
    assessment = None
    for e in events:
        if e.get("kind") in ("runtime_assessed", "runtime_recovered"):
            try:
                assessment = {"kind": e.get("kind"),
                              **(json.loads(e.get("payload_json") or "{}") or {})}
            except ValueError:
                assessment = {"kind": e.get("kind")}
            break
    return {"job": job_public, "launches": launches, "questions": questions,
            "recent_events": events, "output_tail": tail,
            "capacity": list_capacity(state_dir),
            "readings": list_readings(state_dir),
            "runtime": {"installed": installed, "assessment": assessment}}
