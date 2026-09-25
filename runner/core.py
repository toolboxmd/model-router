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
from . import runtime
from . import store
from . import t3exec
from . import t3snapshot

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
                        "unresolved proof ownership",
                        # An unparsable dispatcher reply is retried (#105).
                        "luna_missing_action")
# Steps across every launch of one job; a launch has MAX_LOOP_STEPS of them.
MAX_JOB_STEPS = 48
LAUNCH_ACK_GRACE_SECS = 60.0


def _process_start_identity(pid: int | None) -> str | None:
    """Best-effort start identity. None when the platform cannot provide it."""
    if pid is None:
        return None
    try:
        pid = int(pid)
    except (TypeError, ValueError, OverflowError):
        return None
    try:
        out = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "lstart="],
            text=True, stderr=subprocess.DEVNULL, timeout=2,
        )
        return (out or "").strip() or None
    except Exception:
        return None


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


def _drain_owned_children(state_dir, request_id: str, owner=None) -> bool:
    """Signal the controller lease holder to stop.

    Turns run as T3 threads on the server, so the controller is the only
    owned process besides the proof group (drained separately). The
    signal is checked against the recorded start identity first, so a
    reused PID of an unrelated process is never signalled.
    """
    if owner is not None and owner.get("owner_pid") is not None:
        _signal_pid(owner.get("owner_pid"), owner.get("owner_start"), signal.SIGTERM)
        if not _wait_controller_exit(owner, 2.0):
            _signal_pid(owner.get("owner_pid"), owner.get("owner_start"), signal.SIGKILL)
            if not _wait_controller_exit(owner, 2.0):
                return False
    # Revoke the lease only after the controller is confirmed gone. A live
    # controller that passed its step guard cannot race the final snapshot.
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute("UPDATE jobs SET owner_token=NULL, owner_pid=NULL, owner_start=NULL"
                    " WHERE request_id=? AND cancel_requested IN (1, 2)",
                    (request_id,))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        return False
    finally:
        con.close()
    return True


def _wait_controller_exit(owner: dict, timeout_secs: float) -> bool:
    """Confirm the recorded controller is gone or its PID was reused."""
    pid = owner.get("owner_pid") if isinstance(owner, dict) else None
    if pid is None:
        return True
    deadline = time.monotonic() + timeout_secs
    while True:
        state = _identity_state(pid, owner.get("owner_start"))
        if state in ("dead", "mismatch"):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)


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
# The harness running inside the planner's T3 thread, recorded for
# evidence only: questions and the terminal report go to the T3 thread.
PLANNER_HARNESSES = ("claude", "codex", "opencode", "grok", "t3")
T3_THREAD_REQUIRED = "submit requires --planner-t3-thread: jobs run as T3 threads"


def _dispatcher_saved(job) -> bool:
    """A saved dispatcher child thread to continue (#106)."""
    try:
        planner_thread = job["planner_t3_thread"]
        state = json.loads(job["controller_state"] or "{}")
    except (KeyError, IndexError, TypeError, ValueError):
        return False
    if not planner_thread or not isinstance(state, dict):
        return False
    threads = state.get("t3_threads")
    rec = threads.get("dispatch") if isinstance(threads, dict) else None
    return isinstance(rec, dict) and bool(rec.get("thread_id"))


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
# of time), stall (activity silence on a T3 turn), provider (a
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


def invocation_measurements(state_dir, request_id: str) -> list[dict]:
    """Per-invocation measurements for ``status`` and ``result``.

    Agent Observer mapping: route, stage, timing, class, and usage per
    recorded row (verification attempts, and older rows as stored).
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
            skills = []
        try:
            tools = json.loads(inv.get("tools_json") or "null")
        except ValueError:
            tools = None
        if not isinstance(tools, list):
            tools = []
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
                    "skills_loaded": skills, "tools_called": tools})
    return out


def _derive_handoff_summary(task, request_id: str,
                            handoff_summary: str | None = None) -> str:
    """Durable handoff summary stored on the job.

    An explicit summary wins; a task packet carrying ``handoff_summary``
    is next; otherwise the summary is derived from the packet's Issue,
    decisions, proof command, and goal so a planner question can be
    answered from the ledger alone.
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
           planner_harness: str = "t3",
           handoff_summary: str | None = None,
           planner_t3_thread: str | None = None,
           t3_server_url: str | None = None) -> dict:
    """Persist a prepared task before acknowledging acceptance.

    Only the prepared task, workspace, stable request ID, planner session
    ID, and planner T3 thread are needed. Jobs run as T3 threads
    (toolboxmd/model-router#106, #110): dispatcher and worker turns run as
    T3 child threads under ``planner_t3_thread``, and questions and the
    terminal state are posted into it as messages. The planner harness
    (the harness inside that thread) is recorded for evidence only. The
    handoff summary is stored on the job (explicit argument wins, else
    derived from the task packet) so the questions can carry it.
    ``timeout_secs`` is a legacy compatibility slot, recorded but never
    enforced: jobs carry no age deadline since #88.

    ``t3_server_url`` optionally pins the T3 server;
    otherwise discovery applies (explicit flag, T3_SERVER_URL, default).
    The bearer token never lands in the ledger: it comes from
    T3_SERVER_TOKEN or ``t3 auth session issue`` at turn time.
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
    # Jobs run as T3 threads of the planner thread (#110). The token never
    # lands in the ledger.
    if not planner_t3_thread:
        raise ValueError(T3_THREAD_REQUIRED)
    planner_t3_thread = t3exec.validate_thread_id(planner_t3_thread)
    t3_url = None
    if t3_server_url is not None:
        t3_url = t3_server_url.strip().rstrip("/")
        if not t3_url:
            raise ValueError("t3_server_url must not be blank")
    # The planner's model and effort are recorded as given: the planner
    # runs in its own T3 thread, so the runner never chooses them.
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
            # T3 path fields participate when already persisted (NULL =
            # wildcard for pre-#106 rows so baseline idempotency holds).
            try:
                _t3t = existing["planner_t3_thread"]
            except Exception:
                _t3t = None
            if _t3t is not None and (_t3t or None) != planner_t3_thread:
                same = False
            try:
                _t3u = existing["t3_server_url"]
            except Exception:
                _t3u = None
            if _t3u is not None and (_t3u or None) != t3_url:
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
            "job_kind,replay_of,planner_harness,base_commit,handoff_summary,"
            "planner_t3_thread,t3_server_url)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,0,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (request_id, task_json, thash, ws, pid, planner_session_id,
             executor_session, out_path, "pending", route, max_attempts,
             timeout_secs, now, now,
             planner_model, planner_effort, "controller",
              policy.worker_model_variant(route)[0],
              policy.worker_model_variant(route)[1] or "default", pcwd, lane_stage,
             job_kind, replay_of, planner_harness, base_commit, summary,
             planner_t3_thread, t3_url),
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
                     planner_harness: str = "t3",
                     handoff_summary: str | None = None,
                     planner_t3_thread: str | None = None,
                     t3_server_url: str | None = None) -> dict:
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
                 planner_harness=planner_harness, handoff_summary=handoff_summary,
                 planner_t3_thread=planner_t3_thread,
                 t3_server_url=t3_server_url)
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
    remote_settled = (_interrupt_saved_t3_threads(state_dir, request_id, dict(job))
                      if children_dead and proof_dead else False)
    all_dead = bool(children_dead and proof_dead and remote_settled)

    # Best-effort stop of the legacy owned worker PID when ownership proven.
    try:
        root = store.ensure_state_dir(state_dir)
        ident = store.read_worker_identity(root, request_id)
        if owner_token and ident and ident.get("token") == owner_token \
                and ident.get("pid") == owner_pid:
            _signal_pid(owner_pid, owner["owner_start"], signal.SIGTERM)
    except OSError:
        pass

    _finalize_stopped(state_dir, request_id, all_dead, remote_settled,
                      local_settled=bool(children_dead and proof_dead),
                      controller_fenced=children_dead)
    return get_job(state_dir, request_id)


def _interrupt_saved_t3_threads(state_dir, request_id: str, job: dict) -> bool:
    """Interrupt and confirm every saved T3 child turn is settled."""
    try:
        state = json.loads(job.get("controller_state") or "{}")
        threads = state.get("t3_threads") if isinstance(state, dict) else {}
    except (ValueError, TypeError):
        threads = {}
    if not isinstance(threads, dict) or not threads:
        return True
    try:
        client = t3exec.client_for_job(job)
    except t3exec.T3Error:
        _record_t3_interrupt_event(
            state_dir, request_id, "t3_turn_interrupt_failed",
            {"error": "T3 client unavailable"})
        return False
    settled = True
    for slot, record in threads.items():
        thread_id = record.get("thread_id") if isinstance(record, dict) else None
        if not thread_id:
            continue
        try:
            snapshot = client.thread_snapshot(thread_id)
            thread = t3exec.snapshot_thread(snapshot)
            if not thread or "latestTurn" not in thread:
                settled = False
                continue
            latest = thread.get("latestTurn")
            state = latest.get("state") if isinstance(latest, dict) else None
            if latest is None or state in ("completed", "error", "interrupted"):
                continue
            result = client.dispatch(t3exec.turn_interrupt_command(thread_id))
            confirmed = client.thread_snapshot(thread_id)
            confirmed_thread = t3exec.snapshot_thread(confirmed)
            if not confirmed_thread or "latestTurn" not in confirmed_thread:
                settled = False
                continue
            confirmed_latest = confirmed_thread.get("latestTurn")
            confirmed_state = (confirmed_latest.get("state")
                               if isinstance(confirmed_latest, dict) else None)
            if (confirmed_latest is not None
                    and confirmed_state not in ("completed", "error", "interrupted")):
                settled = False
            _record_t3_interrupt_event(
                state_dir, request_id, "t3_turn_interrupt",
                {"slot": slot, "thread_id": thread_id,
                 "result": result if isinstance(result, dict) else {},
                 "settled": confirmed_latest is None or confirmed_state in (
                     "completed", "error", "interrupted"),
                 "state": confirmed_state})
        except t3exec.T3NotFoundError:
            # After the controller exits, a typed 404 means this child was
            # never observed remotely. Connection errors are not absence.
            continue
        except Exception as e:  # best effort; local cancellation still proceeds
            settled = False
            _record_t3_interrupt_event(
                state_dir, request_id, "t3_turn_interrupt_failed",
                {"slot": slot, "thread_id": thread_id,
                 "error": str(e)[:300]})
    return settled


def _record_t3_interrupt_event(state_dir, request_id: str, kind: str,
                               payload: dict) -> None:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _event(con, request_id, kind, payload)
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def _finalize_stopped(state_dir, request_id: str, all_dead: bool,
                      remote_settled: bool = True,
                      local_settled: bool = True,
                      controller_fenced: bool = True) -> None:
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
            status = ("cancelling"
                      if not controller_fenced
                      or (not remote_settled and local_settled) else "blocked")
            cur = con.execute(
                "UPDATE jobs SET status=?, block_reason=CASE WHEN cancel_requested=2"
                " THEN 'timeout_pending: live child process group remains'"
                " ELSE 'cancellation_pending: live child process group remains' END,"
                " updated_at=? WHERE request_id=? AND status NOT IN ('succeeded','failed','cancelled')",
                (status, now, request_id))
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
    by submit or a recovery, or None when nothing recorded one:
    reported as unknown, never invented. Only NotFoundError propagates;
    any other assessment failure is reported as incompatibility so the
    work is retained instead of migrated.
    """
    new = runtime.installed_runtime()
    job = get_job(state_dir, request_id)
    con = store.connect(state_dir)
    try:
        events = [dict(r) for r in con.execute(
            "SELECT kind, payload_json FROM events WHERE request_id=? AND kind IN "
            "('submitted','runtime_assessed','runtime_recovered') ORDER BY id",
            (request_id,)).fetchall()]
    finally:
        con.close()
    old = runtime.latest_provenance(events)
    try:
        ok, reason = runtime.check_compatible(dict(job))
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


def recover_one(state_dir, request_id: str) -> dict:
    """Reconcile durable records with live ownership; resume only when safe."""
    root = store.ensure_state_dir(state_dir)
    # Terminal jobs are never resurrected and never assessed: recover
    # --all must not add runtime_assessed noise to finished jobs.
    terminal_status = get_job(state_dir, request_id)["status"]
    if terminal_status in store.TERMINAL:
        return {"request_id": request_id, "action": "noop-terminal",
                "status": terminal_status}
    # Runtime recovery assessment for live jobs only: resolve the
    # currently installed runtime, decide explicitly whether it can read
    # the stored job state as is, and record the actual runtime and
    # policy before and after. The compatibility decision only gates
    # execution handoffs (starting a replacement controller) further
    # down; healthy owned work is never interrupted merely because an
    # update occurred.
    compat_ok, compat_reason, old_runtime, new_runtime = _assess_runtime(
        state_dir, request_id)
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
            remote_settled = (_interrupt_saved_t3_threads(
                state_dir, request_id, dict(job))
                if children_dead and proof_dead else False)
            all_dead = bool(children_dead and proof_dead and remote_settled)
            _finalize_stopped(state_dir, request_id, all_dead, remote_settled,
                              local_settled=bool(children_dead and proof_dead),
                              controller_fenced=children_dead)
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
                    and _dispatcher_saved(job) \
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
            should_resume = _dispatcher_saved(job)
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


def reset_at_for_evidence(route, evidence, state: str,
                          now_ts: float | None = None) -> tuple[str | None, str | None]:
    """(reset_at, reset_source) for a capacity mark.

    A provider-named reset wins (OpenCode's ``next`` in epoch
    milliseconds, an ISO ``resetsAt``, a ``retry_after`` delay). A limit
    error with none assumes one 5-hour window, flagged as assumed. The
    overload cooldown is policy-derived.
    """
    del route
    if state == "degraded":
        return degraded_until(now_ts), "cooldown"
    if state == "exhausted":
        moment = policy.parse_provider_reset(evidence, now_ts)
        if moment is not None:
            return moment, "provider"
        return policy.assumed_reset_at(now_ts), "assumed"
    return None, None


CAPACITY_STATES = ("unknown", "exhausted", "degraded", "available")
# Error marks keep one row per pool, model and window; the window column
# names where the mark came from.
MARK_WINDOW = {"exhausted": "limit", "degraded": "cooldown"}


def _record_capacity_locked(con: sqlite3.Connection, route: str, state: str,
                            evidence=None, reset_at: str | None = None,
                            window: str | None = None,
                            reset_source: str | None = None) -> None:
    """Record a capacity mark keyed by pool, model, and window. ``route``
    is kept for display. Every mark expires at its ``reset_at``."""
    now = _utcnow()
    ev = None
    if evidence is not None:
        try:
            ev = json.dumps(adapters.redact_nested(evidence), sort_keys=True)[:4000]
        except Exception:
            ev = json.dumps({"recorded": True})
    spec = policy.route_spec(route)
    win = window or MARK_WINDOW.get(state, "unknown")
    con.execute(
        "INSERT INTO capacity(route, state, evidence_json, reset_at, updated_at, pool, model, window,"
        " reset_source) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(pool, model, window) DO UPDATE SET"
        " route=excluded.route, state=excluded.state, evidence_json=excluded.evidence_json,"
        " reset_at=excluded.reset_at, updated_at=excluded.updated_at,"
        " reset_source=excluded.reset_source",
        (route, state, ev, reset_at, now, spec["pool"], spec["model"], win, reset_source),
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
    """Operator action: forget a route's marks after checking the provider.
    The runner never invents a reset time on its own."""
    spec = policy.route_spec(route)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        # The primary key is (pool, model, window); the route label is
        # display. Clear by both so every window goes.
        con.execute("DELETE FROM capacity WHERE route=? OR (pool=? AND model=?)",
                    (route, spec["pool"], spec["model"]))
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


def record_route_success(state_dir, route: str) -> dict:
    """A successful turn proves its pool works: assumed exhaustion marks
    on that pool clear at once. Provider-reported marks hold until their
    reset. Returns the cleared routes."""
    pool = policy.route_pool(route)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute("SELECT route FROM capacity WHERE pool=? AND state='exhausted'"
                           " AND reset_source='assumed'", (pool,)).fetchall()
        cleared = sorted({r["route"] for r in rows})
        if cleared:
            con.execute("DELETE FROM capacity WHERE pool=? AND state='exhausted'"
                        " AND reset_source='assumed'", (pool,))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return {"route": route, "cleared": cleared}


def _mark_sets(rows, now: float) -> tuple[set[str], set[str]]:
    """(exhausted, degraded) routes from capacity rows still in force.

    Marks expire at their ``reset_at``. Exhaustion is pool-wide: every
    known route on the exhausted pool is skipped, so a limit on one Go
    model never moves laterally to another Go model.
    """
    exhausted_pools: set[str] = set()
    exhausted: set[str] = set()
    degraded: set[str] = set()
    for r in rows:
        if r["state"] not in ("exhausted", "degraded"):
            continue
        ts = _parse_ts(r["reset_at"]) if r["reset_at"] else None
        if ts is None:
            # A mark without a reset lasts one assumed window (or the
            # overload cooldown) from when it was written.
            written = _parse_ts(r["updated_at"]) or now
            ts = written + (policy.ASSUMED_RESET_SECS if r["state"] == "exhausted"
                            else policy.SIGNAL_CLASSES["overloaded"]["degraded_secs"])
        if now >= ts:
            continue
        if r["state"] == "exhausted":
            exhausted.add(r["route"])
            if r["pool"]:
                exhausted_pools.add(r["pool"])
        else:
            degraded.add(r["route"])
    for route in policy.known_routes():
        try:
            if policy.route_pool(route) in exhausted_pools:
                exhausted.add(route)
        except ValueError:
            continue
    return exhausted, degraded


def _capacity_sets_locked(con) -> tuple[set[str], set[str]]:
    """(exhausted, degraded) routes, read inside the caller's write
    transaction so sticky selection and the route record are atomic:
    error marks in force (pool-wide exhaustion, overload cooldowns) plus
    the last T3 snapshot's ineligible, exhausted and degraded routes."""
    rows = con.execute("SELECT route, state, reset_at, pool, updated_at FROM capacity").fetchall()
    exhausted, degraded = _mark_sets(rows, time.time())
    snap_exhausted, snap_degraded = t3snapshot.skip_sets()
    return exhausted | snap_exhausted, (degraded | snap_degraded) - exhausted - snap_exhausted


def _routes_in_state(state_dir, state: str) -> set[str]:
    con = store.connect(state_dir)
    try:
        exhausted, degraded = _capacity_sets_locked(con)
    finally:
        con.close()
    return exhausted if state == "exhausted" else degraded


def exhausted_routes(state_dir) -> set[str]:
    """Routes to skip: pool-wide exhaustion marks until their reset, and
    routes the T3 snapshot rules out or reports at 100 percent."""
    return _routes_in_state(state_dir, "exhausted")


def degraded_routes(state_dir) -> set[str]:
    """Routes resting (overload cooldowns) or at 80 percent or more of a
    T3 usage window."""
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
    if stage is None and current in policy.stage_routes("correction"):
        # The correction route sits in no lane: fall back into the job's lane.
        stage = policy.resolve_lane(lane or policy.DEFAULT_LANE)
        order = [r for r in policy.stage_routes(stage) if r != current]
    elif stage is None:
        return None
    else:
        order = policy.stage_routes(stage)
        if current not in order:
            return None
        order = order[order.index(current) + 1:]
    counts = _running_counts_locked(con, exclude=exclude)
    exhausted, degraded = _capacity_sets_locked(con)
    skip = exhausted | degraded
    turns_by_route = turns_by_route or {}
    for route in order:
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
            "capacity": list_capacity(state_dir)}


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
    job_public["controller_state"] = {k: st.get(k) for k in ("phase", "last_action_name", "seq",
                                                          "terminal_report")}
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
            "runtime": {"installed": installed, "assessment": assessment}}
