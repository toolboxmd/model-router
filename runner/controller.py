"""Controller flow (stdlib only). See RUNNER.md.

One :func:`step` advances one useful transition: dispatch Luna, ask the
saved planner, run one Muse turn, or complete. The detached
:func:`run_controller_process` loops a bounded number of steps with the
durable run command, so every child is supervised, recorded before spawn,
and reused by action identity after a restart. Resume always reuses saved
session IDs. ``run_cmd`` stays injectable for deterministic tests.
"""
from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import time
from pathlib import Path

from . import adapters, core, policy, store

MAX_LOOP_STEPS = 12

# Lease token of the controller process that owns this job. Every durable
# controller write checks it under the write lock; tests and helpers that
# run without a controller leave it unset.
_LEASE = {"token": None}
_LOCK = {"fd": None}


def _lease_guard(con, request_id: str) -> None:
    token = _LEASE["token"]
    if token is None:
        return
    row = con.execute("SELECT owner_token, cancel_requested, status FROM jobs WHERE request_id=?",
                      (request_id,)).fetchone()
    if row is None or row["owner_token"] != token or row["cancel_requested"] \
            or row["status"] in store.TERMINAL:
        con.execute("ROLLBACK")
        raise core.LeaseLostError(f"job {request_id}: lease lost, cancelled, or terminal")


def default_run_cmd(cmd: list[str], cwd: str | None = None,
                    timeout: int = 120, **_kwargs) -> tuple[int, str, str]:
    """Run a built-in adapter command (stdlib subprocess, no secrets logged)."""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout, cwd=cwd or None)
        return proc.returncode, proc.stdout or "", proc.stderr or ""
    except FileNotFoundError as e:
        return 127, "", f"command not found: {e}"
    except subprocess.TimeoutExpired:
        return 124, "", "command timeout"
    except OSError as e:
        return 127, "", f"spawn failed: {e}"


def parse_luna_action(output_text: str,
                      last_message_text: str | None = None) -> dict | None:
    """Extract a structured Luna action envelope.

    Scans stdout plus the output-last-message envelope; last matching
    valid ``action`` wins (deterministic). Back-compat: single-arg call
    scans just ``output_text``.
    """
    return adapters.parse_luna_envelope_from_texts(output_text,
                                                   last_message_text)


def _task_summary(task_json: str) -> str:
    """Full canonical task content (never silently clipped)."""
    # The durable flow passes the complete task to Luna and adapters.
    # Callers add the explicit action protocol via build_luna_prompt.
    return task_json or ""


def _full_luna_prompt(task_json: str, extra: str = "") -> str:
    return adapters.build_luna_prompt(task_json or "", extra or "")


def _last_message_path(state_dir, request_id: str, suffix: str) -> str:
    """A 0600 file Codex writes its last message into."""
    root = store.ensure_state_dir(state_dir)
    path = root / "outputs" / f"{request_id}.{suffix}.json"
    if not path.exists():
        store.secure_write_text(path, "")
    else:
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    return str(path)


def _ensure_child_table(state_dir) -> None:
    con = store.connect(state_dir)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS child_calls ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " request_id TEXT NOT NULL,"
            " ts TEXT NOT NULL,"
            " kind TEXT NOT NULL,"
            " cmd_json TEXT NOT NULL,"
            " rc INTEGER,"
            " session_id TEXT,"
            " output_path TEXT)")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_child_req ON child_calls(request_id)")
    finally:
        con.close()


def _record_child(state_dir, request_id: str, kind: str,
                  cmd: list[str], rc: int | None,
                  session_id: str | None = None,
                  output_text: str = "") -> None:
    """Persist a durable child invocation record + file output."""
    _ensure_child_table(state_dir)
    out_path = ""
    try:
        root = store.ensure_state_dir(state_dir)
        log = store.output_path_for(root, request_id)
        snippet = f"[{kind} rc={rc}] {' '.join(str(c) for c in cmd[:6])}\n"
        # Never log secrets: redact password-ish tokens from the snippet.
        low_snippet = snippet
        store.append_text(log, low_snippet)
        # Model output stays in the private per-invocation files; the job
        # log shown by `status` records only runner-authored markers.
        out_path = str(log)
    except OSError:
        pass
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        # Redact argv secrets (passwords/tokens) before persisting.
        safe_cmd = [("<redacted>" if any(s in str(c).lower() for s in ("password", "bearer", "token=")) else c) for c in cmd]
        con.execute(
            "INSERT INTO child_calls(request_id,ts,kind,cmd_json,rc,session_id,output_path)"
            " VALUES(?,?,?,?,?,?,?)",
            (request_id, core._utcnow(), kind, json.dumps(safe_cmd),
             rc, session_id, out_path))
        core._event(con, request_id, f"child_{kind}",
                    {"rc": rc, "session": (session_id or "")[:24]})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def _load_controller_state(job: dict) -> dict:
    raw = job.get("controller_state")
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except ValueError:
        return {}


def _persist_envelope(state_dir, request_id: str, envelope: dict | None,
                      phase: str) -> None:
    """Persist the Luna action envelope BEFORE its side effect."""
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        cur = _load_controller_state(dict(job))
        cur["phase"] = phase
        cur["last_action"] = envelope
        if envelope:
            cur["last_action_name"] = envelope.get("action")
            cur["seq"] = int(cur.get("seq") or 0) + 1
        now = core._utcnow()
        con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                    (json.dumps(cur, sort_keys=True), now, request_id))
        if envelope is not None:
            core._event(con, request_id, "luna_action",
                        {"action": str(envelope.get("action") or "unknown")[:64],
                         "phase": phase})
        else:
            core._event(con, request_id, "luna_action_missing", {"phase": phase})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _persist_error_evidence(state_dir, request_id: str, raw_error) -> dict:
    redacted = adapters.redact_nested(raw_error if isinstance(raw_error, dict) else {"error": str(raw_error)[:2000]})
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        now = core._utcnow()
        con.execute("UPDATE jobs SET last_error_json=?, updated_at=? WHERE request_id=?",
                    (json.dumps(redacted, sort_keys=True)[:8000], now, request_id))
        core._event(con, request_id, "provider_error_evidence", {"error_class": str(raw_error)[:200] if not isinstance(raw_error, dict) else str(raw_error.get("class") or raw_error.get("code") or raw_error.get("reason") or "error")[:200]})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return redacted


def _mark_blocked(state_dir, request_id: str, reason: str, extra: dict | None = None) -> dict:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        now = core._utcnow()
        con.execute("UPDATE jobs SET status='blocked', block_reason=?, updated_at=? WHERE request_id=?",
                    (reason, now, request_id))
        payload = {"reason": reason}
        if extra:
            payload.update(store.redact_for_log(dict(extra)))
        core._event(con, request_id, "blocked", payload)
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return core.get_job(state_dir, request_id)


def _save_codex_task(state_dir, request_id: str, task_id: str,
                     model: str = adapters.CODEX_MODEL,
                     effort: str = adapters.CODEX_EFFORT) -> dict:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        now = core._utcnow()
        st = _load_controller_state(dict(job))
        st.update({"phase": "dispatched", "codex_task_id": task_id})
        con.execute("UPDATE jobs SET codex_task_id=?, adapter=?, model=?, effort=?,"
                    " controller_state=?, updated_at=? WHERE request_id=?",
                    (task_id, "codex", model, effort,
                     json.dumps(st, sort_keys=True), now, request_id))
        core._event(con, request_id, "codex_dispatched", {"task": task_id[:12] + "..."})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return core.get_job(state_dir, request_id)


def _save_opencode_session(state_dir, request_id: str, session_id: str,
                           route: str, adapter: str = "opencode",
                           model: str = adapters.OPENCODE_MODEL,
                           effort: str = adapters.OPENCODE_EFFORT) -> dict:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        now = core._utcnow()
        con.execute("UPDATE jobs SET opencode_session_id=?, route=?, adapter=?, model=?, effort=?,"
                    " updated_at=? WHERE request_id=?",
                    (session_id, route, adapter, model, effort, now, request_id))
        core._event(con, request_id, "opencode_session_saved", {"route": route})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    return core.get_job(state_dir, request_id)


def _prompt_digest(text: str) -> str:
    import hashlib
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


def dispatch(state_dir, request_id: str, run_cmd=None) -> dict:
    """Ensure Codex dispatch; persist the task ID before accepting."""
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    st = _load_controller_state(job)
    if job.get("codex_task_id") and st.get("seq"):
        return {"action": "already-dispatched", "codex_task_id": job["codex_task_id"],
                "luna_action": st.get("last_action")}
    # A saved thread without a saved action means the dispatch turn's
    # result was not collected. Re-entering the same action reuses or
    # adopts that invocation; it never starts a second dispatch.
    workspace = job["workspace"]
    prompt = _full_luna_prompt(job["task_json"])
    last_path = _last_message_path(state_dir, request_id, "codex-dispatch-last")
    dispatch_route = policy.stage_routes("dispatch")[0]
    dispatch = policy.ROUTES[dispatch_route]
    cmd = adapters.build_codex_dispatch_cmd(workspace, prompt, model=dispatch["model"],
                                            effort=dispatch["variant"],
                                            last_message_path=last_path)
    rc, out, err = run_cmd(cmd, workspace, None, kind="codex_dispatch",
                           meta={"stage": "dispatch", "route": "luna/max", "reason": "initial"})
    last_text = adapters.read_last_message_file(last_path)
    task_id = adapters.parse_codex_task_id(out)
    _record_child(state_dir, request_id, "codex_dispatch", cmd, rc,
                  session_id=task_id, output_text=(out or "") + (err or ""))
    if not task_id:
        _persist_error_evidence(state_dir, request_id, {"source": "codex_dispatch", "rc": rc, "stderr": (err or "")[:1000], "stdout": (out or "")[:1000]})
        _mark_blocked(state_dir, request_id, f"codex_dispatch_failed rc={rc}: no task ID persisted")
        return {"action": "blocked", "reason": "codex_dispatch_failed"}
    # The thread exists even when the turn failed: save it so recovery
    # resumes it instead of creating a replacement task.
    _save_codex_task(state_dir, request_id, task_id, model=dispatch["model"],
                     effort=dispatch["variant"])
    if rc != 0 or "turn.completed" not in (out or ""):
        # A turn counts only with its completion event, as in recovery.
        _persist_error_evidence(state_dir, request_id, {"source": "codex_dispatch", "rc": rc, "stderr": (err or "")[:1000]})
        _mark_blocked(state_dir, request_id, f"codex_dispatch_failed rc={rc}: turn not completed")
        return {"action": "blocked", "reason": "codex_dispatch_failed", "codex_task_id": task_id}
    luna_action = adapters.parse_codex_agent_envelope(out, last_text)
    # Persist the envelope before any side effect it commands.
    _persist_envelope(state_dir, request_id, luna_action, "dispatched")
    if luna_action is None:
        _mark_blocked(state_dir, request_id, "luna_missing_action: no structured envelope")
        return {"action": "blocked", "reason": "luna_missing_action",
                "codex_task_id": task_id}
    return {"action": "dispatched", "codex_task_id": task_id, "luna_action": luna_action}


def _own_invocation_pids(state_dir, request_id: str) -> set:
    pids = set()
    for inv in core._list_invocations(state_dir, request_id):
        if inv.get("state") in core.LIVE_INVOCATION_STATES:
            for k in ("pid", "supervisor_pid"):
                if inv.get(k):
                    pids.add(int(inv[k]))
    return pids


def planner_callback(state_dir, request_id: str, qid: str, prompt: str,
                     run_cmd=None) -> dict:
    """Persist question before Claude --resume; persist answer before resume.

    Never forks a new planner session. A busy planner, a failed callback,
    or a result from another session becomes a durable blocked state
    with a reason; the question stays pending for ``answer`` + recover.
    """
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    planner_session = job.get("planner_session_id")
    if not planner_session:
        _mark_blocked(state_dir, request_id, "missing planner session: refusing to fork a new session")
        return {"action": "blocked", "reason": "missing_planner_session"}
    # Persist the planner question before calling Claude.
    try:
        core.post_question(state_dir, request_id, qid, prompt, lease_token=_LEASE["token"])
    except core.ConflictError as e:
        _mark_blocked(state_dir, request_id, f"planner_question_conflict: {e}")
        return {"action": "blocked", "reason": "question_conflict"}
    # An earlier controller may already have asked; that attempt is
    # adopted through its action key, so the busy checks do not apply.
    asked = any(i.get("kind") == "claude_callback"
                and json.loads(i.get("meta_json") or "{}").get("qid") == qid
                for i in core._list_invocations(state_dir, request_id)
                if i.get("state") != "abandoned")
    busy = [] if asked else adapters.planner_session_in_use(
        planner_session, exclude_pids=_own_invocation_pids(state_dir, request_id))
    if busy:
        _mark_blocked(state_dir, request_id,
                      f"planner_busy: session in use by pid {busy[0]}; answer the question and recover")
        return {"action": "blocked", "reason": "planner_busy"}
    if not asked and not adapters.wait_planner_quiet(planner_session):
        _mark_blocked(state_dir, request_id,
                      "planner_busy: session transcript is still changing; answer the question and recover")
        return {"action": "blocked", "reason": "planner_busy"}
    planner_model = job.get("planner_model") or adapters.CLAUDE_MODEL
    planner_effort = job.get("planner_effort") or adapters.CLAUDE_EFFORT
    question = ("The runner dispatcher for your submitted request "
                f"{request_id} asks (question {qid}):\n{prompt}\n\n"
                "Answer briefly with the decision only. Do not run tools.")
    try:
        cmd = adapters.build_claude_cmd(planner_session, question,
                                         model=planner_model, effort=planner_effort)
    except ValueError as e:
        _mark_blocked(state_dir, request_id, f"planner_resume_refused: {e}")
        return {"action": "blocked", "reason": "planner_resume_refused"}
    cwd = job.get("planner_cwd") or job["workspace"]
    planner_route = "sonnet/medium" if planner_model == adapters.CLAUDE_LIVE_MODEL else "fable-5.1/max"
    rc, out, err = run_cmd(cmd, cwd, None, kind="claude_callback",
                           meta={"qid": qid, "stage": "planning", "route": planner_route,
                                 "reason": "planner_question",
                                 "prompt_sha256": hashlib.sha256(question.encode("utf-8")).hexdigest()})
    parsed = adapters.parse_claude_result(out)
    _record_child(state_dir, request_id, "claude_callback", cmd, rc,
                  session_id=parsed.get("session_id") or planner_session,
                  output_text=(parsed.get("answer") or "") + "\n" + (err or "")[-1000:])
    stored = [q for q in core.list_questions(state_dir, request_id, only_pending=False)
              if q["qid"] == qid and q["status"] == "answered"]
    if stored and (rc != 0 or not parsed.get("ok") or parsed.get("session_id") != planner_session):
        # Answered publicly while the callback ran: the stored answer wins.
        return {"action": "answered", "qid": qid}
    if rc != 0 or not parsed.get("ok"):
        reason = f"planner_callback_failed rc={rc}: {parsed.get('error') or 'error'}"
        _persist_error_evidence(state_dir, request_id,
                                {"source": "claude_callback", "rc": rc,
                                 "error": parsed.get("error"),
                                 "stderr": (err or "")[:1000]})
        _mark_blocked(state_dir, request_id, reason)
        return {"action": "blocked", "reason": "planner_callback_failed"}
    if parsed.get("session_id") != planner_session:
        _persist_error_evidence(state_dir, request_id,
                                {"source": "claude_callback",
                                 "expected_session": planner_session,
                                 "reported_session": parsed.get("session_id")})
        _mark_blocked(state_dir, request_id,
                      "planner_session_mismatch: resumed result came from another session")
        return {"action": "blocked", "reason": "planner_session_mismatch"}
    # Persist the answer before resuming the same Luna task.
    try:
        core.answer(state_dir, request_id, qid, parsed["answer"], lease_token=_LEASE["token"])
    except core.ConflictError:
        pass  # answered publicly meanwhile: the stored answer wins
    return {"action": "answered", "qid": qid}


def resume_luna(state_dir, request_id: str, prompt: str, run_cmd=None,
                label: str = "CONTEXT") -> dict:
    """Resume only the saved Luna task ID with new context (never fork)."""
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    task_id = job.get("codex_task_id")
    if not task_id:
        _mark_blocked(state_dir, request_id, "missing saved Codex task ID: refusing to fork")
        return {"action": "blocked", "reason": "missing_codex_task"}
    message = adapters.build_luna_followup(label, prompt)
    last_path = _last_message_path(state_dir, request_id,
                                   "codex-resume-" + _prompt_digest(message))
    dispatch = policy.ROUTES[policy.stage_routes("dispatch")[0]]
    cmd = adapters.build_codex_resume_cmd(task_id, message, last_message_path=last_path,
                                          model=dispatch["model"], effort=dispatch["variant"])
    # Resume uses the saved workspace as cwd; never --cd/--reasoning.
    rc, out, err = run_cmd(cmd, job["workspace"], None, kind="codex_resume",
                           meta={"stage": "dispatch", "route": "luna/max", "reason": "resume"})
    last_text = adapters.read_last_message_file(last_path)
    resumed_id = adapters.parse_codex_task_id(out)
    _record_child(state_dir, request_id, "codex_resume", cmd, rc,
                  session_id=resumed_id or task_id, output_text=(out or "") + (err or ""))
    if resumed_id != task_id:
        _mark_blocked(state_dir, request_id,
                      f"luna_task_mismatch: resume reported {str(resumed_id or 'no thread')[:24]}")
        return {"action": "blocked", "reason": "luna_task_mismatch"}
    if rc != 0 or "turn.completed" not in (out or ""):
        _persist_error_evidence(state_dir, request_id,
                                {"source": "codex_resume", "rc": rc,
                                 "stderr": (err or "")[:1000]})
        _mark_blocked(state_dir, request_id, f"codex_resume_failed rc={rc}: turn not completed")
        return {"action": "blocked", "reason": "codex_resume_failed"}
    luna_action = adapters.parse_codex_agent_envelope(out, last_text)
    _persist_envelope(state_dir, request_id, luna_action, "resumed")
    if luna_action is None:
        _mark_blocked(state_dir, request_id, "luna_missing_action: no structured envelope")
        return {"action": "blocked", "reason": "luna_missing_action"}
    return {"action": "resumed", "luna_action": luna_action}


def _route_allowance(route: str) -> str:
    return policy.route_allowance(route) if policy.is_supported(route) else "free"


WORKER_RULES = (
    "WORKER RULES: you are the implementation worker for one runner job. "
    "Edit only what the task and instructions allow, inside this "
    "workspace. Run the task's proof command when it has one. Do not "
    "start other agents, commit, push, or install anything. Finish with a "
    "short report of changed files and proof results."
)


def _implementation_prompt(task_json: str, artifact, payload) -> str:
    prompt = f"TASK (complete):\n{_task_summary(task_json)}\n"
    if isinstance(payload, dict) and isinstance(payload.get("instructions"), str):
        prompt += f"\nDISPATCHER INSTRUCTIONS:\n{payload['instructions']}\n"
    if artifact:
        prompt += f"\nartifact: {artifact}\n"
    if payload:
        try:
            prompt += "\ncontext: " + json.dumps(payload, sort_keys=True) + "\n"
        except Exception:
            prompt += f"\ncontext: {payload!r}\n"
    return prompt + "\n" + WORKER_RULES


def _runner_result(out: str) -> dict | None:
    found = None
    for line in (out or "").splitlines():
        if line.startswith("RUNNER_RESULT "):
            try:
                found = json.loads(line[len("RUNNER_RESULT "):])
            except ValueError:
                continue
    return found


def _structured_run_errors(out: str, err: str) -> list:
    """Error objects from ``opencode run --format json`` (not model text)."""
    found = []
    for blob in (out or "", err or ""):
        for line in blob.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict):
                continue
            if obj.get("type") in ("text", "reasoning", "tool_use", "step_start", "step_finish"):
                continue
            if "error" in obj or "code" in obj or obj.get("type") == "error":
                found.append(obj)
    return found


def _set_phase(state_dir, request_id: str, **fields) -> None:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        cur = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        st = {}
        if cur is not None and cur["controller_state"]:
            try:
                st = json.loads(cur["controller_state"]) or {}
            except ValueError:
                st = {}
        st.update(fields)
        con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), core._utcnow(), request_id))
        core._event(con, request_id, "implementation_output",
                    {"session": str(fields.get("opencode_session_id") or "")[:24],
                     "phase": fields.get("phase")})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _switch_route(state_dir, request_id: str, target: str, reason: str,
                  evidence: dict | None = None, mark: tuple | None = None) -> dict:
    """Move the job to ``target`` and, when ``mark`` is given, record the
    old route's capacity state in the same transaction. ``mark`` is
    (state, route, evidence, reset_at)."""
    evidence = evidence or {}
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        row = con.execute("SELECT route FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        prev = row["route"] if row is not None else None
        model, variant, _agent = adapters.opencode_route_params(target)
        st_row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        try:
            st = json.loads(st_row["controller_state"] or "{}") if st_row is not None else {}
        except ValueError:
            st = {}
        st["route_reason"] = reason
        con.execute("UPDATE jobs SET route=?, model=?, effort=?, controller_state=?, updated_at=?"
                    " WHERE request_id=?",
                    (target, model, variant or "default", json.dumps(st, sort_keys=True),
                     core._utcnow(), request_id))
        core._event(con, request_id, "route_switched",
                    {"from": prev, "to": target, "reason": reason,
                     "evidence": str(evidence.get("source") or evidence.get("class")
                                     or evidence.get("name") or "provider")[:64]})
        if mark is not None:
            state, marked_route, mark_evidence, reset_at = mark
            core._record_capacity_locked(con, marked_route, state, mark_evidence, reset_at)
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    action = "transferred_to_go" if target == "muse-spark-xhigh-go" else "route_switched"
    return {"action": action, "route": target, "reason": reason}


def _move_after_signal(state_dir, request_id: str, route: str, signal: str,
                       evidence: dict) -> dict:
    """Exhaustion: zero retries, the same model on the next pool, else the
    next family. Overload: the next family; the route rests for the
    documented cooldown. Moves stay inside the job's stored lane and skip
    one-turn routes already used. No eligible route blocks with a reason."""
    job = core.get_job(state_dir, request_id)
    lane = job.get("lane")
    turns = _turns_by_route(state_dir, request_id)
    exhausted = core.exhausted_routes(state_dir)
    degraded = core.degraded_routes(state_dir)
    if signal == "exhausted":
        exhausted.add(route)
        pool_target = policy.next_pool_route(route)
        if pool_target and (pool_target in exhausted or policy.one_turn_routes_used(pool_target, turns)):
            pool_target = None
        target = pool_target or policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "pool_move" if target and target == pool_target else "lateral"
        mark = ("exhausted", route, evidence, core._trusted_reset_at(evidence))
    else:
        degraded.add(route)
        target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "lateral"
        mark = ("degraded", route, evidence, core.degraded_until())
    if target is None:
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            _lease_guard(con, request_id)
            core._record_capacity_locked(con, mark[1], mark[0], mark[2], mark[3])
            con.execute("COMMIT")
        finally:
            con.close()
        _mark_blocked(state_dir, request_id,
                      f"capacity_exhausted: no eligible route after {signal} on {route}")
        return {"action": "blocked", "reason": "capacity_exhausted"}
    return _switch_route(state_dir, request_id, target, reason, evidence, mark=mark)


def _preflight_move(state_dir, request_id: str, route: str) -> dict | None:
    """Skip a route the capacity memory knows is exhausted or resting,
    before any child starts. Never overwrites the original evidence."""
    job = core.get_job(state_dir, request_id)
    lane = job.get("lane")
    turns = _turns_by_route(state_dir, request_id)
    exhausted = core.exhausted_routes(state_dir)
    degraded = core.degraded_routes(state_dir)
    if route in exhausted:
        target = policy.next_pool_route(route)
        if not target or target in exhausted or policy.one_turn_routes_used(target, turns):
            target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "preflight_exhausted"
    elif route in degraded:
        target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "preflight_degraded"
    elif policy.one_turn_routes_used(route, turns):
        # A $15 model already had its one turn in this job: move on before dispatch.
        target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "preflight_one_turn"
    else:
        return None
    if target is None:
        _mark_blocked(state_dir, request_id,
                      f"capacity_exhausted: no eligible route before dispatch on {route}")
        return {"action": "blocked", "reason": "capacity_exhausted"}
    return _switch_route(state_dir, request_id, target, reason,
                         {"source": "capacity_memory", "route": route})


def _switch_to_go(state_dir, request_id: str, evidence: dict, record: bool = True) -> dict:
    """Legacy name for the free-to-Go move used by the ``opencode run`` seam."""
    if record:
        return _move_after_signal(state_dir, request_id, "muse-spark-xhigh-free", "exhausted", evidence)
    return _preflight_move(state_dir, request_id, "muse-spark-xhigh-free") or \
        {"action": "blocked", "reason": "capacity_exhausted"}


def _turns_by_route(state_dir, request_id: str) -> dict:
    """Implementation turns already run per route, for the one-turn rule."""
    counts: dict = {}
    for inv in core._list_invocations(state_dir, request_id):
        if inv.get("kind") not in ("opencode_control", "opencode_run") or inv.get("state") == "abandoned":
            continue
        try:
            route = (json.loads(inv.get("meta_json") or "{}") or {}).get("route")
        except ValueError:
            route = None
        if route:
            counts[route] = counts.get(route, 0) + 1
    return counts


def run_implementation(state_dir, request_id: str, artifact: str | None = None,
                       payload: dict | None = None, run_cmd=None,
                       use_owned_server: bool = False) -> dict:
    """Run one Muse implementation turn for the current Luna action.

    The public controller uses an owned ``opencode serve`` driven by the
    supervisor. ``use_owned_server=False`` keeps the ``opencode run``
    test seam. Free to Go needs trusted provider evidence; on the owned
    server the old session must also be confirmed idle. Generic errors
    never select Go.
    """
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    workspace = job["workspace"]
    route = job.get("route") or "muse-spark-xhigh-free"
    allowance = _route_allowance(route)
    model, variant, agent = adapters.opencode_route_params(route)
    saved_session = job.get("opencode_session_id")
    state = _load_controller_state(job)
    seq = state.get("seq", 0)
    route_reason = state.get("route_reason") or "initial"
    prompt = _implementation_prompt(job["task_json"], artifact, payload)
    attempted = any(i.get("kind") == "opencode_control" and i.get("state") != "abandoned"
                    and json.loads(i.get("meta_json") or "{}").get("seq") == seq
                    for i in core._list_invocations(state_dir, request_id))
    if not attempted:
        move = _preflight_move(state_dir, request_id, route)
        if move is not None:
            return move
    if use_owned_server:
        cmd = adapters.build_opencode_serve_cmd()
        rc, out, err = run_cmd(cmd, workspace, None, kind="opencode_control",
                               meta={"prompt": prompt, "allowance": allowance,
                                     "model": model, "variant": variant, "agent": agent,
                                     "session_id": saved_session, "seq": seq,
                                     "stage": "implementation", "route": route,
                                     "reason": route_reason})
        # The result line comes from this invocation's own output file.
        full = _runner_result(out) or {}
        session_id = full.get("opencode_session_id") or saved_session
        _record_child(state_dir, request_id, "opencode_control", cmd, rc,
                      session_id=session_id,
                      output_text=json.dumps({k: full.get(k) for k in (
                          "ok", "rc", "quota", "finish", "actual_model")}, sort_keys=True))
        if rc == 0 and full.get("ok"):
            report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id)
            _set_phase(state_dir, request_id, phase="implemented",
                       opencode_session_id=session_id, artifact=artifact,
                       report_path=report.get("report_path"),
                       implementation_output=str(full.get("assistant_text") or "")[-8000:])
            return {"action": "implementation_ok", "session": session_id,
                    "output": str(full.get("assistant_text") or ""),
                    "finish": full.get("finish"), "actual_model": full.get("actual_model"),
                    "report": report}
        evidence = full.get("free_exhaustion_evidence") or full.get("signal_evidence")
        signal = full.get("signal") or ("exhausted" if full.get("quota") else None)
        _persist_error_evidence(state_dir, request_id,
                                {"source": "opencode_control", "rc": rc,
                                 "error": full.get("error"), "quota": full.get("quota"),
                                 "signal": signal, "evidence": evidence,
                                 "idle_confirmed": full.get("idle_confirmed")})
        if signal in ("exhausted", "overloaded") and isinstance(evidence, dict):
            if not full.get("idle_confirmed"):
                if route == "muse-spark-xhigh-free" and signal == "exhausted":
                    _mark_blocked(state_dir, request_id,
                                  "go_transfer_abort_failed: free session not confirmed idle")
                    return {"action": "blocked", "reason": "go_transfer_abort_failed"}
                _mark_blocked(state_dir, request_id,
                              f"route_transfer_abort_failed: session not confirmed idle after {signal}")
                return {"action": "blocked", "reason": "route_transfer_abort_failed"}
            return _move_after_signal(state_dir, request_id, route, signal, evidence)
        _mark_blocked(state_dir, request_id,
                      f"implementation_failed rc={rc}: {str(full.get('error') or '')[:200]}")
        return {"action": "blocked", "reason": "implementation_failed"}

    cmd = adapters.build_opencode_cmd(workspace, prompt, model=model, variant=variant, agent=agent,
                                      allowance=allowance, session_id=saved_session)
    rc, out, err = run_cmd(cmd, workspace, None, kind="opencode_run",
                           meta={"route": route, "allowance": allowance, "model": model})
    errors = _structured_run_errors(out, err)
    session_id = None
    for blob in (out or "",):
        for line in blob.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if isinstance(obj, dict):
                for key in ("opencode_session_id", "sessionID", "session_id"):
                    if isinstance(obj.get(key), str) and obj.get(key):
                        session_id = obj[key]
                        break
            if session_id:
                break
    if session_id:
        _save_opencode_session(state_dir, request_id, session_id, route,
                               model=model, effort=variant or "default")
    _record_child(state_dir, request_id, "opencode_run", cmd, rc,
                  session_id=(session_id or saved_session), output_text=(out or "") + (err or ""))
    if rc == 0 and not errors:
        _set_phase(state_dir, request_id, phase="implemented",
                   opencode_session_id=session_id or saved_session, artifact=artifact,
                   implementation_output=(out or "")[-8000:])
        return {"action": "implementation_ok", "session": session_id or saved_session,
                "output": (out or "")}
    _persist_error_evidence(state_dir, request_id,
                            errors[-1] if errors else {"source": "opencode", "rc": rc,
                                                       "output": ((out or "") + (err or ""))[:2000]})
    # The run process owned the session and has exited, so it is idle.
    if route == "muse-spark-xhigh-free" and any(policy.classify_quota_exhaustion(e) for e in errors):
        return _move_after_signal(state_dir, request_id, route, "exhausted", errors[-1])
    _mark_blocked(state_dir, request_id, f"implementation_failed rc={rc}")
    return {"action": "blocked", "reason": "implementation_failed"}


def _proof_command(task_json: str) -> str | None:
    """The task's own proof command, when the task JSON names one."""
    try:
        task = json.loads(task_json or "null")
    except ValueError:
        return None
    if isinstance(task, dict) and isinstance(task.get("proof"), str) and task["proof"].strip():
        return task["proof"].strip()
    return None


def _git_changes(workspace: str) -> tuple[list[str], str, str | None]:
    """(changed files, diff text, note) for a Git workspace; a note when not Git."""
    try:
        probe = subprocess.run(["git", "-C", workspace, "rev-parse", "--is-inside-work-tree"],
                               capture_output=True, text=True, timeout=5, stdin=subprocess.DEVNULL)
    except Exception:
        return [], "", "git unavailable"
    if probe.returncode != 0:
        return [], "", "workspace is not a git checkout"
    try:
        status = subprocess.run(["git", "-C", workspace, "status", "--porcelain"],
                                capture_output=True, text=True, timeout=15, stdin=subprocess.DEVNULL).stdout
        diff = subprocess.run(["git", "-C", workspace, "diff"], capture_output=True, text=True,
                              timeout=30, stdin=subprocess.DEVNULL).stdout
    except Exception as e:  # noqa: BLE001
        return [], "", f"git failed: {type(e).__name__}"
    files = [line[3:].strip() for line in status.splitlines() if line.strip()]
    return files, diff, None


def _write_turn_report(state_dir, request_id: str, job: dict, seq: int, route: str,
                       full: dict, session_id: str | None) -> dict:
    """Write report.json, proof.log, diff.patch, and worker.txt for one turn.

    The runner runs the task's own proof command and records the exit code;
    the worker's prose is kept in full but only summarized in the report."""
    root = store.ensure_state_dir(state_dir)
    turn_dir = store.job_dir_for(root, request_id) / f"turn-{seq}"
    turn_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    workspace = job["workspace"]
    worker_text = str(full.get("assistant_text") or "")
    store.secure_write_text(turn_dir / "worker.txt", worker_text)
    files, diff, note = _git_changes(workspace)
    store.secure_write_text(turn_dir / "diff.patch", diff if not note else f"# {note}\n")
    proof_cmd = _proof_command(job.get("task_json") or "")
    proof_rc = None
    proof_log = turn_dir / "proof.log"
    if proof_cmd:
        try:
            run = subprocess.run(shlex.split(proof_cmd), cwd=workspace, capture_output=True, text=True,
                                 timeout=600, stdin=subprocess.DEVNULL)
            proof_rc = run.returncode
            store.secure_write_text(proof_log, (run.stdout or "") + (run.stderr or ""))
        except Exception as e:  # noqa: BLE001
            proof_rc = 127
            store.secure_write_text(proof_log, f"proof command failed to run: {type(e).__name__}: {e}\n")
    else:
        store.secure_write_text(proof_log, "# no proof command in the task\n")
    am = full.get("actual_model") if isinstance(full.get("actual_model"), dict) else {}
    observed = (f"{am.get('providerID')}/{am.get('modelID')}" if am.get("providerID") and am.get("modelID") else None)
    model, variant, _agent = adapters.opencode_route_params(route)
    report = {
        "schema_version": store.SCHEMA_VERSION, "request_id": request_id, "seq": seq,
        "stage": "implementation", "route": route, "policy_version": policy.POLICY_VERSION,
        "model": model, "variant": variant, "observed_model": observed,
        "observed_variant": am.get("variant"), "session_id": session_id,
        "finish": full.get("finish"), "status": "ok",
        "changed_files": files, "workspace_note": note,
        "proof_command": proof_cmd, "proof_exit_code": proof_rc,
        "proof_log": str(proof_log), "diff": str(turn_dir / "diff.patch"),
        "worker_text": str(turn_dir / "worker.txt"), "worker_summary": worker_text[:1500],
        "blockers": [], "tokens": full.get("usage"), "native_ids": full.get("native_ids"),
    }
    report_path = turn_dir / "report.json"
    store.secure_write_text(report_path, json.dumps(report, sort_keys=True, indent=1))
    report["report_path"] = str(report_path)
    # Link the report to the invocation that produced it.
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute("UPDATE invocations SET report_path=? WHERE request_id=? AND kind='opencode_control'"
                    " AND invocation_id=(SELECT invocation_id FROM invocations WHERE request_id=?"
                    " AND kind='opencode_control' ORDER BY id DESC LIMIT 1)",
                    (str(report_path), request_id, request_id))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()
    return report


def _implementation_evidence(job: dict, impl: dict) -> str:
    """The dispatcher's resume message: paths and structured fields, not prose."""
    report = impl.get("report") if isinstance(impl.get("report"), dict) else {}
    tail = ""
    try:
        if report.get("proof_log"):
            lines = Path(report["proof_log"]).read_text(encoding="utf-8", errors="replace").splitlines()
            tail = "\n".join(lines[-40:])
    except OSError:
        tail = ""
    fields = {
        "route": job.get("route"), "model": job.get("model"),
        "observed_model": report.get("observed_model"),
        "session": impl.get("session") or "", "finish": impl.get("finish") or "",
        "changed_files": report.get("changed_files") or [],
        "proof_command": report.get("proof_command"), "proof_exit_code": report.get("proof_exit_code"),
        "blockers": report.get("blockers") or [],
        "report": report.get("report_path"), "proof_log": report.get("proof_log"),
        "diff": report.get("diff"), "worker_text": report.get("worker_text"),
    }
    summary = (report.get("worker_summary") or str(impl.get("output") or "")[:1500])
    return (json.dumps(fields, sort_keys=True) + "\n"
            + (f"proof log tail:\n{tail}\n" if tail else "")
            + f"worker summary:\n{summary}\n"
            "Read the report, proof log, and diff at the paths above. Run the proof "
            "yourself and inspect the workspace before completion.")


def _complete_job(state_dir, request_id: str, token: str | None,
                  output: str, artifact: str | None = None) -> dict:
    """Persist the terminal result before acknowledgement (lease-held)."""
    job = core.get_job(state_dir, request_id)
    use_token = token or job.get("owner_token")
    result_payload = {"output": output}
    if artifact:
        result_payload["artifact"] = artifact
    if token:
        # Public path: only the lease holder may complete; a stale
        # controller never writes success.
        try:
            done = core.complete(state_dir, request_id, token,
                                 json.dumps(result_payload, sort_keys=True))
        except core.OwnershipError as e:
            raise core.LeaseLostError(str(e))
        except core.TerminalError:
            return {"action": "noop-terminal", "status": core.get_job(state_dir, request_id)["status"]}
        return {"action": "completed", "status": done["status"]}
    if use_token:
        try:
            done = core.complete(state_dir, request_id, use_token,
                                 json.dumps(result_payload, sort_keys=True))
            return {"action": "completed", "status": done["status"]}
        except (core.OwnershipError, core.TerminalError, core.NotFoundError):
            pass
    # Offline helper paths without a live lease: durable terminal write
    # (still persists result + file before ack).
    head_commit = core._workspace_head(job.get("workspace"))
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        cur = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if cur is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        if cur["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            return {"action": "noop-terminal", "status": cur["status"]}
        now = core._utcnow()
        con.execute("UPDATE jobs SET status='succeeded', result_json=?, head_commit=?, updated_at=?"
                    " WHERE request_id=?",
                    (json.dumps({"ok": True, **result_payload}), head_commit, now, request_id))
        core._event(con, request_id, "completed", {"via": "controller"})
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
        core._mirror_result(state_dir, request_id,
                            json.dumps({"request_id": request_id, "status": "succeeded",
                                        "result": result_payload}))
        store.append_text(store.output_path_for(root, request_id), "[succeeded]\n")
    except OSError:
        pass
    return {"action": "completed", "status": "succeeded"}


def _handle_question_action(state_dir, request_id: str, envelope: dict,
                            run_cmd) -> dict:
    """Persist question -> Claude --resume exact planner session ->
    persist answer -> resume exact Luna task. Never forks a session.
    An answer already persisted (public ``answer`` or earlier callback)
    is used without asking the planner again.
    """
    qid = str(envelope.get("qid") or envelope.get("question_id") or "q1")
    prompt_q = str(envelope.get("prompt") or envelope.get("question") or
                   envelope.get("text") or "Planner input requested.")
    answer_text = None
    for q in core.list_questions(state_dir, request_id, only_pending=False):
        if q["qid"] == qid and q["status"] == "answered" and q.get("answer"):
            answer_text = str(q["answer"])
    if answer_text is None:
        cb = planner_callback(state_dir, request_id, qid, prompt_q, run_cmd=run_cmd)
        if cb.get("action") == "blocked":
            return cb
        for q in core.list_questions(state_dir, request_id, only_pending=False):
            if q["qid"] == qid:
                answer_text = q.get("answer") or ""
    r = resume_luna(state_dir, request_id, f"question {qid}: {prompt_q}\nanswer: {answer_text}",
                    run_cmd=run_cmd, label="PLANNER ANSWER")
    if r.get("action") == "blocked":
        return r
    return {"action": "question-answered-resumed", "qid": qid,
            "luna_action": r.get("luna_action")}


def _handle_implementation_action(state_dir, request_id: str, envelope: dict,
                                  run_cmd, use_owned_server: bool = False) -> dict:
    """One bounded implementation transition: OpenCode then Luna resume."""
    artifact = envelope.get("artifact")
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else None
    impl = run_implementation(state_dir, request_id, artifact=artifact,
                              payload=payload, run_cmd=run_cmd,
                              use_owned_server=use_owned_server)
    if impl.get("action") != "implementation_ok":
        # transferred_to_go or blocked: the next step retries on the new
        # route or stops; never fork a second writer here.
        return impl
    job = core.get_job(state_dir, request_id)
    evidence = _implementation_evidence(job, impl)
    r = resume_luna(state_dir, request_id, evidence, run_cmd=run_cmd,
                    label="IMPLEMENTATION RESULT")
    if r.get("action") == "blocked":
        return r
    return {"action": "implementation-resumed", "luna_action": r.get("luna_action")}


def step(state_dir, request_id: str, run_cmd=None, token: str | None = None,
         use_owned_server: bool = False) -> dict:
    """Advance exactly one useful bounded controller transition."""
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    if job["status"] in store.TERMINAL:
        return {"action": "noop-terminal", "status": job["status"]}
    if job["cancel_requested"]:
        return {"action": "cancelled", "status": job["status"]}
    if job["status"] == "blocked":
        return {"action": "blocked", "reason": job.get("block_reason")}
    if not job.get("codex_task_id") or not _load_controller_state(job).get("seq"):
        return dispatch(state_dir, request_id, run_cmd=run_cmd)
    last = _load_controller_state(job).get("last_action")
    if not isinstance(last, dict) or last.get("action") not in policy.VALID_ACTIONS:
        _mark_blocked(state_dir, request_id, "luna_missing_action: no saved action to continue")
        return {"action": "blocked", "reason": "luna_missing_action"}
    action_name = last.get("action")
    if action_name == "planner_question":
        return _handle_question_action(state_dir, request_id, last, run_cmd)
    if action_name == "implementation":
        return _handle_implementation_action(state_dir, request_id, last, run_cmd,
                                             use_owned_server=use_owned_server)
    if action_name == "completion":
        return _complete_job(state_dir, request_id, token,
                             str(last.get("output") or "done"),
                             last.get("artifact"))
    # review/unknown: durable block, never spin.
    _mark_blocked(state_dir, request_id, f"unsupported_luna_action: {action_name}")
    return {"action": "blocked", "reason": "unsupported_luna_action"}


def _advertise(state_dir: str, request_id: str, token: str) -> None:
    import datetime
    import os
    from pathlib import Path

    root = Path(state_dir)
    workers_dir = root / "workers"
    outputs_dir = root / "outputs"
    workers_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    outputs_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    pid = os.getpid()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    from .supervisor import process_start_identity
    payload = json.dumps({"token": token, "pid": pid, "updated": now,
                          "start": process_start_identity(pid)})
    ident = workers_dir / f"{request_id}.json"
    tmp = ident.with_name(ident.name + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, ident)
    try:
        os.chmod(ident, 0o600)
    except OSError:
        pass
    log = outputs_dir / f"{request_id}.log"
    try:
        if not log.exists():
            log.write_text("", encoding="utf-8")
            try:
                os.chmod(log, 0o600)
            except OSError:
                pass
        with open(log, "a", encoding="utf-8") as f:
            f.write(f"controller start request={request_id} pid={pid}\n")
        try:
            os.chmod(log, 0o600)
        except OSError:
            pass
    except OSError:
        pass


def run_controller_process(state_dir: str, request_id: str, token: str,
                           mode: str = "run-once", duration: float = 30.0) -> int:
    """Detached controller entry point (stdlib only, no secrets logged).

    ``run-once`` drives the bounded state machine with built-in
    adapters: at most ``MAX_LOOP_STEPS`` single-transition :func:`step`
    calls (dispatch -> question -> implementation -> completion),
    stopping on terminal/blocked-awaiting states. ``sleep`` preserves
    the offline lease seam for deterministic tests.
    """
    import time as _time

    if mode == "sleep":
        _advertise(state_dir, request_id, token)
        end = _time.time() + max(0.0, duration)
        while _time.time() < end:
            _advertise(state_dir, request_id, token)
            _time.sleep(0.2)
        return 0
    import os as _os
    from .supervisor import process_start_identity as _psi
    lock_fd = core.take_controller_lock(state_dir, request_id)
    if lock_fd is None:
        # Another controller holds the job; give back this launch's lease
        # (guarded on this token) so recover can reconcile it.
        core.release_controller(state_dir, request_id, token)
        return 0
    _LOCK["fd"] = lock_fd  # held until this process exits
    if not core.acknowledge_controller(state_dir, request_id, token, _os.getpid(),
                                       _psi(_os.getpid())):
        return 0  # superseded launch: another controller owns the job
    # Advertise only as the proven lease holder, after the lock.
    _advertise(state_dir, request_id, token)
    _LEASE["token"] = token
    # Bounded durable loop with built-in adapters (normal submit/start).
    # Every child spawn is file-backed, supervised, and recorded before
    # the spawn; a finished action is reused, never rerun.
    durable_run_cmd = core.make_durable_run_cmd(state_dir, request_id, token)
    continuing = ("dispatched", "question-answered-resumed",
                  "implementation-resumed", "transferred_to_go", "route_switched")
    for _ in range(MAX_LOOP_STEPS):
        try:
            res = step(state_dir, request_id, run_cmd=durable_run_cmd, token=token,
                       use_owned_server=True)
        except core.LeaseLostError:
            return 0  # another controller owns the job; touch nothing
        except Exception as e:  # noqa: BLE001 - persisted, never silent
            try:
                _mark_blocked(state_dir, request_id,
                              f"controller_error: {type(e).__name__}: {str(e)[:300]}")
            except Exception:
                pass
            break
        try:
            _advertise(state_dir, request_id, token)
        except Exception:
            pass
        if (res or {}).get("action") not in continuing:
            break
    else:
        try:
            job = core.get_job(state_dir, request_id)
            if job["status"] not in store.TERMINAL and job["status"] != "blocked":
                _mark_blocked(state_dir, request_id,
                              f"controller_step_budget_exhausted after {MAX_LOOP_STEPS} steps")
        except Exception:
            pass
    core.release_controller(state_dir, request_id, token)
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="runner.controller")
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--request-id", required=True)
    ap.add_argument("--token", default=None,
                    help="test seam; the launcher passes DURABLE_RUNNER_TOKEN")
    ap.add_argument("--mode", default="run-once")
    ap.add_argument("--duration", type=float, default=30.0)
    args = ap.parse_args(argv)
    import os
    token = args.token or os.environ.pop("DURABLE_RUNNER_TOKEN", "")
    if not token:
        ap.error("missing lease token")
    return run_controller_process(args.state_dir, args.request_id, token,
                                  mode=args.mode, duration=args.duration)


if __name__ == "__main__":
    raise SystemExit(main())
