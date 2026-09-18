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


def _canonical_workspace(workspace: str) -> str:
    if not workspace:
        raise ValueError("missing workspace")
    return os.path.abspath(os.path.expanduser(workspace))


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


def _heartbeat_fresh(updated: str | None) -> bool:
    ts = _parse_ts(updated)
    if ts is None:
        return False
    return (time.time() - ts) <= HEARTBEAT_FRESH_SECS


def _row_to_job(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


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


def _event(con: sqlite3.Connection, request_id: str, kind: str, payload: dict) -> None:
    safe = store.redact_for_log(dict(payload))
    con.execute(
        "INSERT INTO events(request_id, ts, kind, payload_json) VALUES(?,?,?,?)",
        (request_id, _utcnow(), kind, json.dumps(safe, sort_keys=True)),
    )


def submit(state_dir, request_id: str, task, workspace: str,
           planner_session_id: str, route: str = "muse-spark-xhigh-free",
           policy_id: str | None = None, max_attempts: int = 3,
           timeout_secs: int | None = None,
           planner_model: str | None = None,
           planner_effort: str | None = None) -> dict:
    """Persist a prepared task before acknowledging acceptance.

    Built-in defaults mean no executor/callback commands are required:
    only the prepared task, workspace, stable request ID, and original
    planner session ID are needed. Planner model/effort default to
    claude-sonnet-5 / medium and may be overridden explicitly.
    """
    _validate_request_id(request_id)
    if not planner_session_id:
        raise ValueError("missing planner session ID")
    ws = _canonical_workspace(workspace)
    policy.validate_route(route)
    pid = policy_id or policy.POLICY_ID
    if not pid:
        raise ValueError("missing policy identity")
    if max_attempts < 1 or max_attempts > 10:
        raise ValueError("max_attempts must be 1..10")
    # Planner defaults: production Fable 5.1 max; explicit overrides
    # preserved verbatim (Sonnet medium is only the bounded live-test
    # override). Never fork a new session implicitly.
    try:
        from . import adapters as _adapters
        _pm_default = _adapters.CLAUDE_MODEL
        _pe_default = _adapters.CLAUDE_EFFORT
    except Exception:
        _pm_default, _pe_default = "fable-5.1", "max"
    planner_model = planner_model or _pm_default
    planner_effort = planner_effort or _pe_default
    task_json = _canonical_task(task)
    thash = _task_hash(task_json)
    root = store.ensure_state_dir(state_dir)
    out_path = str(store.output_path_for(root, request_id))
    now = _utcnow()
    executor_session = "exec-" + secrets.token_hex(8)
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
            con.execute("ROLLBACK")
            if same:
                return _row_to_job(existing)
            raise ConflictError(f"request ID {request_id!r} already used with a different payload")
        clash = con.execute(
            "SELECT request_id FROM jobs WHERE workspace=? AND status IN ('pending','running','question_pending','blocked')",
            (ws,),
        ).fetchone()
        if clash is not None:
            con.execute("ROLLBACK")
            raise WorkspaceConflictError(
                f"workspace {ws!r} already owned by {clash['request_id']!r}"
            )
        # Retain the workspace claim after cancel/timeout while the
        # actual child is still alive: any job (even terminal) on the
        # same workspace with a live recorded PID still owns it.
        stale_rows = con.execute(
            "SELECT request_id, owner_pid FROM jobs WHERE workspace=? AND owner_pid IS NOT NULL",
            (ws,),
        ).fetchall()
        for _r in stale_rows:
            try:
                _pid = int(_r["owner_pid"])
            except (TypeError, ValueError):
                continue
            if _is_pid_alive(_pid):
                con.execute("ROLLBACK")
                raise WorkspaceConflictError(
                    f"workspace {ws!r} still claimed by live pid {_pid} "
                    f"({_r['request_id']!r}); refusing duplicate writer"
                )
        con.execute(
            "INSERT INTO jobs(request_id,task_json,task_hash,workspace,policy_id,"
            "planner_session_id,executor_session_id,output_path,status,route,"
            "cancel_requested,attempts,max_attempts,timeout_secs,created_at,updated_at,"
            "planner_model,planner_effort,adapter,model,effort)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,0,0,?,?,?,?,?,?,?,?,?)",
            (request_id, task_json, thash, ws, pid, planner_session_id,
             executor_session, out_path, "pending", route, max_attempts,
             timeout_secs, now, now,
             planner_model, planner_effort, "controller",
             "opencode/muse-spark-1.3-contributor-free", "xhigh"),
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
                     planner_session_id: str, route: str = "muse-spark-xhigh-free",
                     policy_id: str | None = None, max_attempts: int = 3,
                     timeout_secs: int | None = None,
                     planner_model: str | None = None,
                     planner_effort: str | None = None,
                     launcher=None, spawn=None) -> dict:
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
                 planner_effort=planner_effort)
    # Idempotent resubmit of the same payload must not fork a second
    # controller when one already holds the lease.
    if job.get("owner_token") and job.get("owner_pid") and _is_pid_alive(job.get("owner_pid")):
        return job
    start_controller(state_dir, request_id, launcher=launcher, spawn=spawn)
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
        if job["cancel_requested"]:
            con.execute("ROLLBACK")
            raise TerminalError(f"job {request_id} cancellation requested")
        if job["status"] == "blocked":
            con.execute("ROLLBACK")
            raise BlockedError(f"job {request_id} blocked: {job['block_reason']}")
        if job["owner_token"]:
            ident = store.read_worker_identity(root, request_id)
            if ident and ident.get("token") == job["owner_token"] \
                    and ident.get("pid") == job["owner_pid"] \
                    and _is_pid_alive(job["owner_pid"]) \
                    and _heartbeat_fresh(ident.get("updated")):
                con.execute("ROLLBACK")
                raise OwnershipError(
                    f"job {request_id} already has a live owned controller; refusing duplicate launch")
            # A live recorded PID without a matching fresh handshake
            # remains claimed/unknown: never treat it as gone, never
            # start a second writer. Recover reconciles it.
            if job["owner_pid"] is not None and _is_pid_alive(job["owner_pid"]):
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
            "UPDATE jobs SET attempts=?, owner_token=?, owner_pid=NULL, status=?,"
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
    cmd = [sys.executable, "-m", "runner.controller", "--state-dir", str(root),
           "--request-id", request_id, "--token", token]
    try:
        if spawn is not None:
            pid = int(spawn(cmd))
        else:
            proc = subprocess.Popen(
                cmd, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True, close_fds=True,
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
            "UPDATE jobs SET owner_pid=?, updated_at=? WHERE request_id=? AND owner_token=?",
            (pid, _utcnow(), request_id, token),
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


def post_question(state_dir, request_id: str, qid: str, prompt: str) -> dict:
    """Persist a Luna/planner question before exposing it."""
    if not qid:
        raise ValueError("missing question ID")
    if not prompt:
        raise ValueError("missing question prompt")
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


def answer(state_dir, request_id: str, qid: str, answer_text: str) -> dict:
    """Persist an answer (and resume running) before acknowledging it."""
    if not answer_text:
        raise ValueError("missing answer")
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
        if remaining == 0 and job["status"] == "question_pending":
            con.execute(
                "UPDATE jobs SET status='running', updated_at=? WHERE request_id=?",
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
        result = {"cancelled": True, "at": now}
        con.execute(
            "UPDATE jobs SET cancel_requested=1, status='cancelled', result_json=?, updated_at=? WHERE request_id=?",
            (json.dumps(result), now, request_id),
        )
        _event(con, request_id, "cancelled", {})
        con.execute("COMMIT")
        owner_token, owner_pid = job["owner_token"], job["owner_pid"]
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    # Best-effort stop of the owned worker only when ownership is proven.
    try:
        root = store.ensure_state_dir(state_dir)
        ident = store.read_worker_identity(root, request_id)
        if owner_token and ident and ident.get("token") == owner_token \
                and ident.get("pid") == owner_pid and _is_pid_alive(owner_pid):
            _terminate_pid(owner_pid)
    except OSError:
        pass
    # Mirror terminal result to file (0600) without touching partial log.
    try:
        root = store.ensure_state_dir(state_dir)
        store.secure_write_text(
            store.result_path_for(root, request_id),
            json.dumps({"request_id": request_id, "status": "cancelled",
                        "result": {"cancelled": True}}),
        )
    except OSError:
        pass
    return get_job(state_dir, request_id)


def _complete_with_token(state_dir, request_id: str, token: str, ok: bool,
                         output: str | None, error, route: str | None = None) -> dict:
    if not token:
        raise ValueError("missing start token")
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
            # Quota-exhaustion failover preserves the artifact: switch the
            # stored route to Go only on explicit evidence.
            if route is None:
                nxt = policy.next_implementation_route(job["route"], err)
                if nxt is not None:
                    con.execute(
                        "UPDATE jobs SET route=? WHERE request_id=?", (nxt, request_id)
                    )
                    _event(con, request_id, "route_switched", {"from": job["route"], "to": nxt})
        con.execute(
            "UPDATE jobs SET status=?, result_json=?, error_class=?, updated_at=? WHERE request_id=?",
            (status, json.dumps(result), err_class, now, request_id),
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
        store.secure_write_text(
            store.result_path_for(root, request_id),
            json.dumps({"request_id": request_id, "status": status, "result": result}),
        )
        store.append_text(store.output_path_for(root, request_id),
                          f"[{status}] {str(output or '')[:500]}\n")
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
        # Launch-race guard: never spawn a second writer while the lease
        # holder PID is still alive. A live recorded PID without a
        # matching fresh handshake remains claimed/unknown and blocks
        # duplicates; recover() reconciles it. Missing/stale heartbeats
        # never count as worker_gone while the PID lives.
        if job["owner_token"]:
            ident = store.read_worker_identity(root, request_id)
            if ident and ident.get("token") == job["owner_token"] \
                    and ident.get("pid") == job["owner_pid"] \
                    and _is_pid_alive(job["owner_pid"]) \
                    and _heartbeat_fresh(ident.get("updated")):
                con.execute("ROLLBACK")
                raise OwnershipError(
                    f"job {request_id} already has a live owned worker; refusing duplicate launch")
            if job["owner_pid"] is not None and _is_pid_alive(job["owner_pid"]):
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
            "UPDATE jobs SET attempts=?, owner_token=?, owner_pid=NULL, status=?,"
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
            start_new_session=True, close_fds=True,
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
            "UPDATE jobs SET owner_pid=?, updated_at=? WHERE request_id=? AND owner_token=?",
            (pid, _utcnow(), request_id, token),
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
        # Timeout enforcement (bounded, persisted before ack).
        if job["timeout_secs"] is not None:
            created = _parse_ts(job["created_at"])
            if created is not None and (time.time() - created) > int(job["timeout_secs"]):
                con.execute(
                    "UPDATE jobs SET status='failed', error_class='timeout',"
                    " result_json=?, updated_at=? WHERE request_id=?",
                    (json.dumps({"ok": False, "error": {"code": "TIMEOUT"}}), now, request_id),
                )
                _event(con, request_id, "timeout", {})
                con.execute("COMMIT")
                return {"request_id": request_id, "action": "timeout", "status": "failed"}
        if job["cancel_requested"]:
            con.execute(
                "UPDATE jobs SET status='cancelled',"
                " result_json=?, updated_at=? WHERE request_id=?",
                (json.dumps({"cancelled": True}), now, request_id),
            )
            _event(con, request_id, "cancelled", {})
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "cancelled", "status": "cancelled"}

        lease_token = job["owner_token"]
        lease_pid = job["owner_pid"]
        known = _known_tokens(con, request_id)
        ident = store.read_worker_identity(root, request_id)
        ident_token = ident.get("token") if ident else None
        ident_pid = ident.get("pid") if ident else None
        ident_updated = ident.get("updated") if ident else None
        ident_alive = _is_pid_alive(ident_pid) if ident else False
        ident_fresh = _heartbeat_fresh(ident_updated) if ident else False
        lease_alive = _is_pid_alive(lease_pid) if lease_pid else False

        def mark_blocked(reason: str):
            con.execute(
                "UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?",
                (reason, _utcnow(), request_id),
            )
            _event(con, request_id, "blocked", {"reason": reason})

        # Case: unknown ownership. A live worker advertises a token that is
        # neither the lease nor any known launch: stop, never duplicate.
        if ident and ident_alive and ident_fresh and ident_token not in known and ident_token != lease_token:
            mark_blocked(f"unknown worker ownership token={str(ident_token)[:8]}... pid={ident_pid}")
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "blocked-unknown-owner",
                    "status": "blocked"}

        # Case: lease held and live worker matches lease: adopt, no new spawn.
        if lease_token and ident_token == lease_token and ident_pid == lease_pid \
                and ident_alive and ident_fresh:
            # Adopt an 'attempting' launch interrupted after spawn but before ack.
            con.execute(
                "UPDATE launches SET pid=?, state='acknowledged', ack_at=? WHERE request_id=? AND start_token=? AND state='attempting'",
                (lease_pid, _utcnow(), request_id, lease_token),
            )
            if job["status"] == "blocked":
                con.execute(
                    "UPDATE jobs SET status='running', block_reason=NULL, updated_at=? WHERE request_id=?",
                    (_utcnow(), request_id),
                )
            _event(con, request_id, "recovered-adopted-live-worker", {})
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "adopted-live-worker",
                    "status": get_job(state_dir, request_id)["status"]}

        # Case: attempting launch with a live matching worker but lease pid
        # missing (crash between attempting row and ack): acknowledge it.
        if ident and ident_alive and ident_fresh and ident_token in known and lease_pid is None:
            con.execute(
                "UPDATE launches SET pid=?, state='acknowledged', ack_at=? WHERE request_id=? AND start_token=?",
                (ident_pid, _utcnow(), request_id, ident_token),
            )
            con.execute(
                "UPDATE jobs SET owner_token=?, owner_pid=?, status=?, updated_at=? WHERE request_id=?",
                (ident_token, ident_pid,
                 "running" if job["status"] in ("pending", "blocked") else job["status"],
                 _utcnow(), request_id),
            )
            _event(con, request_id, "recovered-launch-race", {"token": str(ident_token)[:8]})
            con.execute("COMMIT")
            return {"request_id": request_id, "action": "reconciled-launch-race",
                    "status": get_job(state_dir, request_id)["status"]}

        # Case: live recorded PID without a matching fresh handshake
        # remains claimed/unknown and blocks duplicates. A missing or
        # stale heartbeat is never worker_gone while the recorded PID
        # is still alive.
        if lease_pid is not None and lease_alive:
            handshake_ok = (ident and ident_token == lease_token
                            and ident_pid == lease_pid
                            and ident_alive and ident_fresh)
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
        if len(attempting) > 1 and not (ident_alive and ident_fresh) and not lease_alive:
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
        worker_gone = (not lease_alive) and (
            (not ident) or (not ident_alive) or (not ident_fresh)
            or (ident_token != lease_token))
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
            # Same-session recovery within budget: clear lease, keep planner
            # and executor session IDs, keep partial log. The caller (or a
            # manual launch) spawns the next attempt; recover itself does not
            # spawn so a restarted controller cannot fork duplicates.
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


def recover_all(state_dir) -> list[dict]:
    con = store.connect(state_dir)
    try:
        rows = con.execute("SELECT request_id FROM jobs ORDER BY created_at").fetchall()
        ids = [r["request_id"] for r in rows]
    finally:
        con.close()
    return [recover_one(state_dir, rid) for rid in ids]


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
    return {"job": job_public, "launches": launches, "questions": questions,
            "recent_events": events, "output_tail": tail}
