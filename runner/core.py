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
from . import policy
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
                        "orphaned server", "timeout_pending", "cancellation_pending")
LAUNCH_ACK_GRACE_SECS = 60.0

INVOCATION_KINDS = (
    "codex_dispatch", "codex_resume", "claude_callback",
    "opencode_run", "opencode_serve", "opencode_control",
)
LIVE_INVOCATION_STATES = ("running", "cancelling")


def _infer_invocation_kind(cmd: list[str]) -> str:
    """Infer the invocation kind from a built-in adapter command."""
    if not cmd:
        return "unknown"
    name = os.path.basename(cmd[0]).lower() if cmd[0] else ""
    if name == "codex":
        if any(a == "resume" for a in cmd[1:]):
            return "codex_resume"
        return "codex_dispatch"
    if name == "claude":
        return "claude_callback"
    if name == "opencode":
        if any(a == "serve" for a in cmd[1:]):
            return "opencode_serve"
        if any(a == "control" for a in cmd[1:]):
            return "opencode_control"
        return "opencode_run"
    return "unknown"


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
        if inv.get("kind") in ("opencode_control", "opencode_serve") \
                and inv.get("supervisor_pid") and _supervisor_state(inv) in ("dead", "mismatch"):
            return "orphaned"
        return "live"
    if child == "dead" and _is_pgid_alive(pgid):
        # The leader exited but group members remain.
        if inv.get("kind") in ("opencode_control", "opencode_serve") \
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
    """Return (session_id, session_kind) extracted from streamed child output."""
    if kind in ("codex_dispatch", "codex_resume"):
        sid = adapters.parse_codex_task_id(stdout, stderr)
        if sid:
            return sid, "codex_task_id"
        return None, None
    if kind == "claude_callback":
        # The session identity is the exact planner session we resumed.
        if job is not None:
            sid = job.get("planner_session_id")
            if sid:
                return sid, "planner_session_id"
        return None, None
    if kind == "opencode_run":
        for blob in (stdout, stderr):
            if not blob:
                continue
            text = blob.strip()
            for line in text.splitlines():
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(obj, dict):
                    continue
                for key in ("opencode_session_id", "session_id", "sessionId", "session"):
                    val = obj.get(key)
                    if isinstance(val, str) and val:
                        return val, "opencode_session_id"
                    if isinstance(val, dict):
                        for sub in ("id", "session_id", "sessionId"):
                            if isinstance(val.get(sub), str) and val.get(sub):
                                return val.get(sub), "opencode_session_id"
        return None, None
    return None, None


def _redact_cmd(cmd: list[str]) -> list[str]:
    return [
        ("<redacted>" if any(s in str(c).lower() for s in ("password", "bearer", "token=")) else c)
        for c in cmd
    ]


DEFAULT_KIND_TIMEOUTS = {
    "codex_dispatch": 1800, "codex_resume": 1800,
    "claude_callback": 900,
    "opencode_control": 1800, "opencode_serve": 1800, "opencode_run": 1800,
}
# A failed model turn is never replayed automatically: the job blocks
# with its reason. Replaying could fork a task or repeat a side effect.
MAX_FAILED_ATTEMPTS_PER_ACTION = 1


def _action_key(kind: str, cmd: list[str], meta: dict | None) -> str:
    """Identity of one logical side effect, stable across controllers.

    Volatile values such as a saved session learned by an earlier
    attempt are excluded so a restarted controller finds the same key.
    """
    m = dict(meta or {})
    stable = {k: m.get(k) for k in ("prompt", "model", "allowance", "seq", "qid")}
    blob = json.dumps([kind, list(cmd), stable], sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


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
    """
    prior = [i for i in _list_invocations(state_dir, request_id)
             if i.get("action_key") == key and i.get("state") != "abandoned"]
    if not prior:
        return None
    if any(_invocation_ownership(i) == "never_started" for i in prior):
        _abandon_never_started(state_dir, request_id, lock_holder=True)
        prior = [i for i in _list_invocations(state_dir, request_id)
                 if i.get("action_key") == key and i.get("state") != "abandoned"]
        if not prior:
            return None
    for inv in prior:
        own = _invocation_ownership(inv)
        if own == "live":
            # Adopt: the same action is already running under its own
            # supervisor. Wait for its durable result instead of copying it.
            limit = float(inv.get("timeout_secs") or 1800) + 120.0
            end = time.monotonic() + limit
            while time.monotonic() < end:
                cur = [i for i in _list_invocations(state_dir, request_id)
                       if i["invocation_id"] == inv["invocation_id"]]
                if not cur or _invocation_ownership(cur[0]) != "live":
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
                 if i.get("action_key") == key and i.get("state") != "abandoned"]
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
        end = time.monotonic() + float(inv.get("timeout_secs") or 1800) + 120.0
        own = _invocation_ownership(inv)
        while own == "live" and time.monotonic() < end:
            time.sleep(0.2)
            cur = [i for i in _list_invocations(state_dir, request_id)
                   if i["invocation_id"] == inv["invocation_id"]]
            own = _invocation_ownership(cur[0]) if cur else "finished"
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
    """
    if timeout is None:
        timeout = DEFAULT_KIND_TIMEOUTS.get(kind, 1800)
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
    meta_json = json.dumps(store.redact_for_log(dict(meta or {})), sort_keys=True)
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
        if con.execute("SELECT 1 FROM invocations WHERE request_id=? AND action_key=?"
                       " AND state != 'abandoned'", (request_id, key)).fetchone() is not None:
            con.execute("ROLLBACK")
            reused = _prior_action_result(state_dir, request_id, key)
            if reused is not None:
                return reused
            raise OwnershipError(f"job {request_id}: action already has an attempt; run recover")
        con.execute(
            "INSERT INTO invocations("
            " invocation_id, request_id, kind, cmd_json, workspace, owner_token,"
            " pid, pgid, process_start, stdout_path, stderr_path, started_at,"
            " state, task_json, timeout_secs, meta_json, action_key,"
            " stage, requested_route, policy_version, reason, harness_version, schema_version"
            ") VALUES(?,?,?,?,?,?,NULL,NULL,NULL,?,?,?,'running',?,?,?,?,?,?,?,?,?,?)",
            (invocation_id, request_id, kind, json.dumps(list(cmd)),
             workspace, owner_token, str(stdout_path), str(stderr_path),
             started_at, job.get("task_json"), int(timeout), meta_json, key,
             (meta or {}).get("stage") or STAGE_FOR_KIND.get(kind),
             (meta or {}).get("route") or (job.get("route") if kind.startswith("opencode") else None),
             policy.POLICY_VERSION, (meta or {}).get("reason") or "initial",
             harness_version_for(kind), store.SCHEMA_VERSION),
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
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute(
                "UPDATE invocations SET state='failed', rc=127, ended_at=?, consumed_at=? WHERE invocation_id=?",
                (_utcnow(), _utcnow(), invocation_id),
            )
            con.execute("COMMIT")
        finally:
            con.close()
        return 127, "", f"supervisor spawn failed: {e}"

    # The supervisor records its own PID and start identity when it claims
    # the row; the controller never writes them, so an unclaimed row stays
    # recognizable as never started.

    # The supervisor enforces the child timeout, then may need to abort
    # and confirm an idle OpenCode session before it records the result.
    deadline = time.monotonic() + max(1.0, float(timeout) + 60.0)
    rc = 124
    while time.monotonic() < deadline:
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
                        rc = int(invs[0]["rc"]) if invs[0].get("rc") is not None else 124
                    except (TypeError, ValueError):
                        rc = 124
            break
        time.sleep(0.05)
    else:
        _terminate_invocations(state_dir, request_id, signal_no=signal.SIGKILL)
        consume_finished_invocations(state_dir, request_id)
        rc = 124

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
    envelope = None
    planner_answer = None
    if kind in ("codex_dispatch", "codex_resume"):
        # Only Luna's own final agent message can carry an action.
        envelope = adapters.parse_codex_agent_envelope(
            stdout_text, adapters.read_last_message_file(
                adapters.last_message_path_from_cmd(inv_cmd)))
    result_obj = None
    if inv.get("result_json"):
        try:
            result_obj = json.loads(inv["result_json"])
        except ValueError:
            result_obj = None
    rc = inv.get("rc")
    if rc is None:
        # Child and supervisor are dead. A Codex turn counts only with
        # its completion event and a parsed action.
        if kind in ("codex_dispatch", "codex_resume") and "turn.completed" in stdout_text \
                and isinstance(envelope, dict):
            rc = 0
        elif kind == "claude_callback" and adapters.parse_claude_result(stdout_text).get("ok"):
            rc = 0
        else:
            rc = 1
    if kind == "claude_callback" and rc == 0:
        parsed = adapters.parse_claude_result(stdout_text)
        try:
            qid = (json.loads(inv.get("meta_json") or "{}") or {}).get("qid")
        except ValueError:
            qid = None
        if parsed.get("ok") and qid and parsed.get("session_id") == job.get("planner_session_id"):
            planner_answer = (qid, parsed["answer"])
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
        if sid and skind == "codex_task_id" and kind == "codex_dispatch":
            con.execute(
                "UPDATE jobs SET codex_task_id=COALESCE(codex_task_id, ?), adapter='codex',"
                " model=?, effort=?, updated_at=? WHERE request_id=?",
                (sid, adapters.CODEX_MODEL, adapters.CODEX_EFFORT, now, request_id),
            )
        elif sid and skind == "opencode_session_id":
            route = job.get("route") or "muse-spark-xhigh-free"
            r_model, r_variant, _r_agent = adapters.opencode_route_params(route)
            con.execute(
                "UPDATE jobs SET opencode_session_id=COALESCE(opencode_session_id, ?),"
                " adapter='opencode', model=?, effort=?, updated_at=? WHERE request_id=?",
                (sid, r_model, r_variant or "default", now, request_id),
            )
        job_row = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        job_status = job_row["status"] if job_row else None
        applied = "consumed"
        if planner_answer and job_row is not None and job_status not in store.TERMINAL \
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
        cancelled = bool(job_row is not None and job_row["cancel_requested"])
        turn_ok = rc == 0 and (kind not in ("codex_dispatch", "codex_resume")
                               or "turn.completed" in stdout_text)
        # A Luna turn counts only from the saved thread: a resume that
        # reports another thread, or none, is never applied.
        thread_ok = True
        if kind in ("codex_dispatch", "codex_resume"):
            saved_thread = job_row["codex_task_id"] if job_row is not None else None
            thread_ok = bool(sid) and (saved_thread is None or sid == saved_thread)
        live_job = job_row is not None and job_status not in store.TERMINAL and not cancelled
        planner_reason = None
        if live_job and kind == "claude_callback":
            # A planner turn matters only while its question is still open;
            # a question answered meanwhile (public `answer`) wins.
            try:
                cb_qid = (json.loads(inv.get("meta_json") or "{}") or {}).get("qid")
            except ValueError:
                cb_qid = None
            still_open = cb_qid is not None and con.execute(
                "SELECT 1 FROM questions WHERE request_id=? AND qid=? AND status='pending'",
                (request_id, cb_qid)).fetchone() is not None
            if still_open and not planner_answer:
                parsed_cb = adapters.parse_claude_result(stdout_text)
                if rc != 0 or not parsed_cb.get("ok"):
                    planner_reason = f"planner_callback_failed rc={rc} (consumed by recovery)"
                elif parsed_cb.get("session_id") != job_row["planner_session_id"]:
                    planner_reason = "planner_session_mismatch (consumed by recovery)"
        if planner_reason:
            con.execute("UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?"
                        " AND status NOT IN ('succeeded','failed','cancelled')",
                        (planner_reason, now, request_id))
            _event(con, request_id, "blocked", {"reason": planner_reason[:120]})
            applied = "blocked"
        elif live_job and kind in ("codex_dispatch", "codex_resume") \
                and (not turn_ok or not thread_ok):
            # The controller would have blocked on this result; recovery
            # records the same durable, sticky reason instead of leaving
            # the job running without an owner. Muse turns are left to the
            # restarted controller, which may switch routes on evidence.
            reason = (f"luna_task_mismatch: consumed {kind} reported {str(sid or 'no thread')[:24]}"
                      if turn_ok and not thread_ok else f"{kind}_failed rc={rc} (consumed by recovery)")
            con.execute("UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?"
                        " AND status NOT IN ('succeeded','failed','cancelled')", (reason, now, request_id))
            _event(con, request_id, "blocked", {"reason": reason[:120]})
            applied = "blocked"
        if job_row is not None and job_status not in store.TERMINAL and isinstance(envelope, dict) \
                and turn_ok and thread_ok and not cancelled:
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
            con.execute(
                "UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                (json.dumps(st, sort_keys=True), now, request_id),
            )
            _event(con, request_id, "luna_action",
                   {"action": str(action_name or "unknown")[:64], "phase": "consumed-invocation"})
            if action_name == "completion":
                output = str(envelope.get("output") or "done")
                result = {"ok": True, "output": json.dumps({"output": output,
                                                            "artifact": envelope.get("artifact")},
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
                    "SELECT qid FROM questions WHERE request_id=? AND qid=?",
                    (request_id, qid),
                ).fetchone()
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
    _measure_invocation(state_dir, request_id, inv["invocation_id"], crashed=inv.get("rc") is None)
    if applied == "completed":
        try:
            root = store.ensure_state_dir(state_dir)
            _mirror_result(state_dir, request_id,
                json.dumps({"request_id": request_id, "status": "succeeded",
                            "result": {"ok": True, "output": (envelope or {}).get("output")}}),
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
                if skind == "codex_task_id" and inv["kind"] == "codex_dispatch":
                    con.execute(
                        "UPDATE jobs SET codex_task_id=COALESCE(codex_task_id, ?), adapter='codex',"
                        " model=?, effort=?, updated_at=? WHERE request_id=?",
                        (sid, adapters.CODEX_MODEL, adapters.CODEX_EFFORT, _utcnow(), request_id),
                    )
                elif skind == "opencode_session_id":
                    route = job.get("route") or "muse-spark-xhigh-free"
                    r_model, r_variant, _r_agent = adapters.opencode_route_params(route)
                    con.execute(
                        "UPDATE jobs SET opencode_session_id=?, adapter='opencode', model=?, effort=?, updated_at=? WHERE request_id=?",
                        (sid, r_model, r_variant or "default", _utcnow(), request_id),
                    )
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


# ---------------------------------------------------------------------------
# Measurement (Agent Observer compatible)
# ---------------------------------------------------------------------------

STAGE_FOR_KIND = {"codex_dispatch": "dispatch", "codex_resume": "dispatch",
                  "claude_callback": "planning", "opencode_control": "implementation",
                  "opencode_run": "implementation", "opencode_serve": "implementation"}
JOB_KINDS = ("ordinary", "experiment", "replay")
PLANNER_HARNESSES = ("claude", "codex")
_HARNESS_VERSIONS: dict = {}


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
    if kind in ("codex_dispatch", "codex_resume"):
        binary = adapters.CODEX_BIN
    elif kind == "claude_callback":
        binary = adapters.CLAUDE_BIN
    elif kind.startswith("opencode"):
        binary = adapters.OPENCODE_BIN
    else:
        return None
    if binary not in _HARNESS_VERSIONS:
        try:
            out = subprocess.run([binary, "--version"], capture_output=True, text=True,
                                 timeout=5, stdin=subprocess.DEVNULL).stdout
            first = (out or "").strip().splitlines()
            _HARNESS_VERSIONS[binary] = first[0][:80] if first else None
        except Exception:
            _HARNESS_VERSIONS[binary] = None
    return _HARNESS_VERSIONS[binary]


def terminal_class_for(rc, result_obj, crashed: bool = False) -> str:
    if crashed:
        return "crashed"
    signal_name = result_obj.get("signal") if isinstance(result_obj, dict) else None
    if signal_name == "exhausted":
        return "quota"
    if signal_name == "overloaded":
        return "overloaded"
    if signal_name == "hard":
        return "hard_error"
    if rc == 0:
        return "completed"
    if rc == 124:
        return "timeout"
    if rc in (143, -15):
        return "cancelled"
    return "failed"


def _runner_result_line(stdout: str) -> dict:
    found = {}
    for line in (stdout or "").splitlines():
        if line.startswith("RUNNER_RESULT "):
            try:
                found = json.loads(line[len("RUNNER_RESULT "):])
            except ValueError:
                continue
    return found if isinstance(found, dict) else {}


def measure_output(kind: str, stdout: str, stderr: str, meta: dict | None = None) -> tuple:
    """(usage, observed_model, native_ids) from a harness's own records.

    Counters are copied verbatim under a ``source`` label and never folded;
    a harness that reports nothing leaves usage None."""
    usage = None
    observed = None
    ids: dict = {}
    meta = meta or {}
    if kind in ("codex_dispatch", "codex_resume"):
        ids = {"thread_id": None, "turn_ids": [], "agent_message_ids": []}
        for line in (stdout or "").splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict):
                continue
            typ = obj.get("type")
            if typ == "thread.started":
                ids["thread_id"] = obj.get("thread_id") or obj.get("id")
            elif typ == "turn.completed":
                usage = {"source": "codex", **(obj.get("usage") or {})}
                if obj.get("turn_id"):
                    ids["turn_ids"].append(obj["turn_id"])
            elif typ == "item.completed":
                item = obj.get("item") or {}
                if item.get("type") == "agent_message" and item.get("id"):
                    ids["agent_message_ids"].append(item["id"])
    elif kind == "claude_callback":
        obj = None
        text = (stdout or "").strip()
        try:
            obj = json.loads(text) if text else None
        except ValueError:
            for line in reversed(text.splitlines()):
                try:
                    cand = json.loads(line.strip())
                except ValueError:
                    continue
                if isinstance(cand, dict) and cand.get("type") == "result":
                    obj = cand
                    break
        if isinstance(obj, dict):
            usage = {"source": "claude", **(obj.get("usage") or {})}
            for key in ("total_cost_usd", "duration_ms", "num_turns"):
                if obj.get(key) is not None:
                    usage[key] = obj.get(key)
            ids = {"session_id": obj.get("session_id"), "uuid": obj.get("uuid")}
            observed = obj.get("model") if isinstance(obj.get("model"), str) else None
        if meta.get("prompt_sha256"):
            ids["prompt_sha256"] = meta["prompt_sha256"]
    elif kind == "opencode_control":
        summary = _runner_result_line(stdout)
        usage = summary.get("usage") if isinstance(summary.get("usage"), dict) else None
        ids = summary.get("native_ids") if isinstance(summary.get("native_ids"), dict) else {}
        am = summary.get("actual_model") if isinstance(summary.get("actual_model"), dict) else None
        if am and am.get("providerID") and am.get("modelID"):
            observed = f"{am['providerID']}/{am['modelID']}" + (f" {am['variant']}" if am.get("variant") else "")
    return usage, observed, ids


def _measure_invocation(state_dir, request_id: str, invocation_id: str, crashed: bool = False) -> None:
    """Fill elapsed time, terminal class, usage, observed model, and native
    identities on a finished invocation from its own files and result."""
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
    usage, observed, ids = measure_output(inv.get("kind") or "", stdout, stderr, meta)
    started = _parse_ts(inv.get("started_at"))
    ended = _parse_ts(inv.get("ended_at")) or time.time()
    elapsed = round(max(0.0, ended - started), 3) if started is not None else None
    tclass = terminal_class_for(inv.get("rc"), result_obj, crashed=crashed)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "UPDATE invocations SET elapsed_secs=?, terminal_class=?, usage_json=?,"
            " observed_model=COALESCE(?, observed_model), native_ids_json=? WHERE invocation_id=?",
            (elapsed, tclass, json.dumps(usage, sort_keys=True) if usage is not None else None,
             observed, json.dumps(ids, sort_keys=True) if ids else None, invocation_id))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def invocation_measurements(state_dir, request_id: str) -> list[dict]:
    """Per-invocation measurements for ``status`` and ``result``."""
    out = []
    for inv in _list_invocations(state_dir, request_id):
        try:
            usage = json.loads(inv.get("usage_json") or "null")
        except ValueError:
            usage = None
        out.append({"invocation": inv["invocation_id"][:8], "kind": inv.get("kind"),
                    "stage": inv.get("stage"), "requested_route": inv.get("requested_route"),
                    "policy_version": inv.get("policy_version"), "reason": inv.get("reason"),
                    "observed_model": inv.get("observed_model"),
                    "terminal_class": inv.get("terminal_class"), "elapsed_secs": inv.get("elapsed_secs"),
                    "usage": usage, "harness_version": inv.get("harness_version"),
                    "report_path": inv.get("report_path"), "started_at": inv.get("started_at"),
                    "ended_at": inv.get("ended_at")})
    return out


def submit(state_dir, request_id: str, task, workspace: str,
           planner_session_id: str, route: str | None = None,
           policy_id: str | None = None, max_attempts: int = 3,
           timeout_secs: int | None = None,
           planner_model: str | None = None,
           planner_effort: str | None = None,
           planner_cwd: str | None = None,
           lane: str | None = None,
           job_kind: str = "ordinary", replay_of: str | None = None,
           planner_harness: str = "claude") -> dict:
    """Persist a prepared task before acknowledging acceptance.

    Built-in defaults mean no executor/callback commands are required:
    only the prepared task, workspace, stable request ID, and original
    planner session ID are needed. Planner model/effort default to the
    policy's planning route and may be overridden explicitly.
    """
    _validate_request_id(request_id)
    if not planner_session_id:
        raise ValueError("missing planner session ID")
    # An existing request is compared first, so an identical resubmission
    # returns it even if its directory was removed since.
    ws = _canonical_workspace(workspace, must_exist=False)
    # The lane picks the first route of an implementation lane unless an
    # explicit route is given; the critical lane is planner-executed and
    # raises here so the planner keeps that step.
    if lane:
        lane_route = policy.lane_default_route(lane)
        route = route or lane_route
    route = route or policy.lane_default_route(policy.DEFAULT_LANE)
    policy.validate_implementation_route(route)
    # The job remembers its lane: Muse sits in two lanes, so later moves
    # must not guess from the route alone. An explicit route must belong
    # to the explicit lane.
    lane_stage = policy.lane_of_route(route, lane)  # raises on a lane mismatch
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
    pcwd = _canonical_workspace(planner_cwd, must_exist=False) if planner_cwd else ws
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
            same = (
                existing["task_hash"] == thash
                and existing["workspace"] == ws
                and existing["planner_session_id"] == planner_session_id
                and existing["policy_id"] == pid
                and existing["route"] == route
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
            if existing["planner_cwd"] is not None and existing["planner_cwd"] != pcwd:
                same = False
            if existing["lane"] is not None and existing["lane"] != lane_stage:
                same = False
            if existing["job_kind"] is not None and (
                    existing["job_kind"] != job_kind or existing["replay_of"] != replay_of
                    or existing["planner_harness"] != planner_harness):
                same = False
            con.execute("ROLLBACK")
            if same:
                return _row_to_job(existing)
            raise ConflictError(f"request ID {request_id!r} already used with a different payload")
        try:
            _canonical_workspace(ws)  # a new job needs existing directories
            _canonical_workspace(pcwd)
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
            "job_kind,replay_of,planner_harness,base_commit)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,0,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (request_id, task_json, thash, ws, pid, planner_session_id,
             executor_session, out_path, "pending", route, max_attempts,
             timeout_secs, now, now,
             planner_model, planner_effort, "controller",
             adapters.opencode_route_params(route)[0],
             adapters.opencode_route_params(route)[1] or "default", pcwd, lane_stage,
             job_kind, replay_of, planner_harness, base_commit),
        )
        _event(con, request_id, "submitted", {
            "workspace": ws, "policy": pid, "route": route,
            "planner_session": planner_session_id, "task_hash": thash,
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
                     policy_id: str | None = None, max_attempts: int = 3,
                     timeout_secs: int | None = None,
                     planner_model: str | None = None,
                     planner_effort: str | None = None,
                     launcher=None, spawn=None,
                     planner_cwd: str | None = None,
                     lane: str | None = None,
                     job_kind: str = "ordinary", replay_of: str | None = None,
                     planner_harness: str = "claude") -> dict:
    """Persist first, then launch a detached controller with built-ins.

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
                 planner_effort=planner_effort, planner_cwd=planner_cwd,
                 lane=lane, job_kind=job_kind, replay_of=replay_of,
                 planner_harness=planner_harness)
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

    # Stop every owned child process group, not just the controller PID.
    # The workspace claim is retained until all owned children confirm dead.
    all_dead = _drain_owned_children(state_dir, request_id, owner)

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
            result = {"ok": True, "output": output}
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


def launch_worker(state_dir, request_id: str, mode: str = "sleep",
                  duration: float = 30.0, text: str = "") -> dict:
    """Start a detached worker. Persists attempt before AND after ack.

    Crash between the two rows leaves an 'attempting' launch that recover()
    reconciles against the advertised worker identity file.
    """
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
        if job["cancel_requested"]:
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
        # Launch-race guard: never spawn a second writer while the lease
        # holder PID is still alive. A live recorded PID without a
        # matching fresh handshake remains claimed/unknown and blocks
        # duplicates; recover() reconciles it. Missing/stale heartbeats
        # never count as worker_gone while the PID lives.
        if job["owner_token"]:
            ident = store.read_worker_identity(root, request_id)
            if ident and ident.get("token") == job["owner_token"] \
                    and ident.get("pid") == job["owner_pid"] \
                    and _owner_alive(job) \
                    and _heartbeat_fresh(ident.get("updated")):
                con.execute("ROLLBACK")
                raise OwnershipError(
                    f"job {request_id} already has a live owned worker; refusing duplicate launch")
            if job["owner_pid"] is not None and _owner_alive(job):
                con.execute("ROLLBACK")
                raise OwnershipError(
                    f"job {request_id} still claimed by live pid {job['owner_pid']}; run recover")
            # An attempting row with no ack yet is a launch in progress:
            # let recover() reconcile it instead of forking a second writer.
            if job["owner_pid"] is None and job["status"] in ("pending", "running", "question_pending"):
                pending_attempt = con.execute(
                    "SELECT * FROM launches WHERE request_id=? AND start_token=? AND state='attempting'",
                    (request_id, job["owner_token"]),
                ).fetchone()
                if pending_attempt is not None:
                    con.execute("ROLLBACK")
                    raise OwnershipError(
                        f"job {request_id} launch already in progress; run recover")
        if job["attempts"] >= job["max_attempts"]:
            # Bounded: do not loop retries.
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
        _event(con, request_id, "launch_attempting", {"attempt": attempt_no})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()

    # Detached spawn: survives controller exit (new session, no stdio).
    cmd = [sys.executable, "-m", "runner.worker", "--state-dir", str(root),
           "--request-id", request_id, "--token", token,
           "--mode", mode, "--duration", str(duration), "--text", text]
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True, close_fds=True, cwd=_PKG_ROOT,
        )
        pid = proc.pid
    except OSError as e:
        # Launch failed after the attempting row: leave for recover().
        raise RunnerError(f"worker spawn failed: {e}")

    # After-ack persistence.
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
        # A concurrent recover may have superseded this attempt; keep lease only
        # if this token still holds it.
        if cur is not None and cur["owner_token"] == token:
            _event(con2, request_id, "launch_acknowledged", {"attempt": attempt_no, "pid": pid})
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


def _known_tokens(con: sqlite3.Connection, request_id: str) -> set[str]:
    rows = con.execute(
        "SELECT start_token FROM launches WHERE request_id=?", (request_id,)
    ).fetchall()
    return {r["start_token"] for r in rows}


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
    consumed = consume_finished_invocations(state_dir, request_id)
    if any(c.get("action") == "completed" for c in consumed):
        return {"request_id": request_id, "action": "consumed-completion",
                "status": get_job(state_dir, request_id)["status"]}
    live_invocations = _any_live_invocation(state_dir, request_id)
    if live_invocations:
        _capture_invocation_sessions(state_dir, request_id, live_invocations)
    unresolved_invocations = _any_unresolved_invocation(state_dir, request_id)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise NotFoundError(f"unknown request: {request_id}")
        status = job["status"]
        if status in store.TERMINAL:
            # Never resurrect completed jobs; budgets stay as-is.
            con.execute("ROLLBACK")
            return {"request_id": request_id, "action": "noop-terminal",
                    "status": status}
        now = _utcnow()
        # Cancellation intent persisted previously. Stop owned children and
        # finalize cancellation only after every owned process group is dead.
        if job["cancel_requested"]:
            con.execute("COMMIT")
            con.close()
            all_dead = _drain_owned_children(state_dir, request_id, dict(job))
            _finalize_stopped(state_dir, request_id, all_dead)
            fin = get_job(state_dir, request_id)
            return {"request_id": request_id,
                    "action": "timeout" if fin["error_class"] == "timeout" else "cancelled",
                    "status": fin["status"]}

        # Timeout enforcement (bounded, persisted before ack).
        # Mark timing-out intent, stop owned children, then finalize only after
        # every owned process group is confirmed dead.
        if job["timeout_secs"] is not None:
            created = _parse_ts(job["created_at"])
            if created is not None and (time.time() - created) > int(job["timeout_secs"]):
                con.execute(
                    "UPDATE jobs SET status='cancelling', cancel_requested=2, updated_at=? WHERE request_id=?",
                    (now, request_id),
                )
                _event(con, request_id, "timing_out", {})
                con.execute("COMMIT")
                con.close()
                all_dead = _drain_owned_children(state_dir, request_id, dict(job))
                _finalize_stopped(state_dir, request_id, all_dead)
                return {"request_id": request_id, "action": "timeout",
                        "status": get_job(state_dir, request_id)["status"]}
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
                    i.get("kind") == "codex_dispatch" for i in live_invocations)):
                # A new controller waits for the live child's durable
                # result through the same action identity.
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
            if pending_q > 0 and status == "question_pending" and job["codex_task_id"] \
                    and int(job["attempts"]) < int(job["max_attempts"]):
                # The controller asks the saved planner again through the
                # same action identity: a finished callback is reused.
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
            should_resume = bool(job["codex_task_id"]) or any(
                i.get("kind") == "codex_dispatch" and i.get("state") == "completed"
                for i in _list_invocations(state_dir, request_id))
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
            con.execute(
                "UPDATE jobs SET owner_token=NULL, owner_pid=NULL,"
                " status=?, updated_at=? WHERE request_id=?",
                ("pending" if status == "blocked" else status, _utcnow(), request_id),
            )
            _event(con, request_id, "recovered-worker-dead", {"partial_preserved": True})
            con.execute("COMMIT")
            if should_resume:
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


def _trusted_reset_at(evidence) -> str | None:
    """Return a provider reset timestamp only from explicit trusted evidence.

    Unknown stays unknown. Never invent a daily reset or probe loop.
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


def _record_capacity_locked(con: sqlite3.Connection, route: str, state: str,
                            evidence=None, reset_at: str | None = None,
                            window: str | None = None) -> None:
    """Record capacity per route, which names one pool and model. The window
    is the one the evidence names; otherwise it stays unknown rather than
    guessed. ``reset_at`` comes only from provider evidence, the documented
    degraded cooldown, or an operator."""
    now = _utcnow()
    ev = None
    if evidence is not None:
        try:
            ev = json.dumps(adapters.redact_nested(evidence), sort_keys=True)[:4000]
        except Exception:
            ev = json.dumps({"recorded": True})
    spec = policy.ROUTES.get(route) or {}
    if window is None and isinstance(evidence, dict):
        window = evidence.get("window") if isinstance(evidence.get("window"), str) else None
    con.execute(
        "INSERT INTO capacity(route, state, evidence_json, reset_at, updated_at, pool, model, window)"
        " VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(route) DO UPDATE SET"
        " state=excluded.state, evidence_json=excluded.evidence_json,"
        " reset_at=excluded.reset_at, updated_at=excluded.updated_at,"
        " pool=excluded.pool, model=excluded.model, window=excluded.window",
        (route, state, ev, reset_at, now, spec.get("pool"), spec.get("model"),
         window or ("cooldown" if state == "degraded" else "unknown")),
    )


def degraded_until(now_ts: float | None = None) -> str:
    """Documented cooldown end for an overloaded route."""
    secs = policy.SIGNAL_CLASSES["overloaded"]["degraded_secs"]
    base = datetime.datetime.fromtimestamp(now_ts if now_ts is not None else time.time(),
                                           datetime.timezone.utc)
    return (base + datetime.timedelta(seconds=secs)).isoformat()


def record_capacity(state_dir, route: str, state: str, evidence=None,
                    reset_at: str | None = None) -> None:
    policy.validate_route(route)
    if state not in CAPACITY_STATES:
        raise ValueError(f"invalid capacity state: {state!r}")
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _record_capacity_locked(con, route, state, evidence, reset_at)
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
        return [dict(r) for r in con.execute("SELECT * FROM capacity ORDER BY route").fetchall()]
    finally:
        con.close()


def clear_capacity(state_dir, route: str) -> dict:
    """Operator action: forget an exhausted route after checking the
    provider. The runner never invents a reset time on its own."""
    policy.validate_route(route)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute("DELETE FROM capacity WHERE route=?", (route,))
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
        reset_at = r["reset_at"]
        if reset_at:
            ts = _parse_ts(reset_at)
            if ts is not None and now >= ts:
                continue
        out.add(r["route"])
    return out


def exhausted_routes(state_dir) -> set[str]:
    """Routes known exhausted until a trusted reset time has passed."""
    return _routes_in_state(state_dir, "exhausted")


def degraded_routes(state_dir) -> set[str]:
    """Routes marked overloaded until their documented cooldown passes."""
    return _routes_in_state(state_dir, "degraded")


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
            "reports": reports, "measurements": invocation_measurements(state_dir, request_id)}


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
    job_public["last_error_json"] = ({k: le.get(k) for k in ("source", "rc", "quota", "idle_confirmed")}
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
    return {"job": job_public, "launches": launches, "questions": questions,
            "recent_events": events, "output_tail": tail}
