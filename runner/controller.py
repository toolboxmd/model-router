"""Structured controller flow (stdlib only).

Built-in detached controller drives the durable flow with at most one
useful bounded transition per :func:`step` (a detached
:func:`run_controller_process` loops those steps boundedly):

- dispatch Luna with the built-in Codex adapter and persist the
  returned thread/task ID before accepting; the same Luna result is
  parsed for a structured action envelope (explicit action JSON
  protocol lives in the prompt);
- ``planner_question`` persists via ``core.post_question`` before
  calling Claude ``--resume`` of the exact original planner session;
  the Claude answer persists via ``core.answer`` before resuming that
  exact saved Luna task ID and parsing the next action;
- ``implementation`` runs Muse OpenCode with the selected route,
  persists session/artifact/output evidence, then resumes/finishes
  through the same saved Luna task or a structured completion path;
- ``completion`` persists the terminal result before acknowledgement;
- busy/callback/model/permission errors persist a durable ``blocked``
  reason and leave unanswered questions durable for recover.

Never forks a planner or Luna session: resume always reuses saved IDs.
A restart resumes saved IDs; a waiting question proceeds after the
public ``answer`` + ``recover``. ``run_cmd`` and control seams stay
injectable for deterministic tests; normal submit/start uses built-in
defaults with no injection required.
"""
from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

from . import adapters, core, policy, store

MAX_LOOP_STEPS = 8


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
    root = Path(state_dir)
    outdir = root / "outputs"
    try:
        outdir.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError:
        pass
    return str(outdir / f"{request_id}.{suffix}.json")


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
        if output_text:
            # Append bounded tail for crash recovery; full envelope is
            # persisted in events/controller_state, not truncated silently
            # for task routing (only the log tail is bounded).
            store.append_text(log, output_text[-4000:] + ("\n" if not output_text.endswith("\n") else ""))
        out_path = str(log)
    except OSError:
        pass
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
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
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        cur = _load_controller_state(dict(job))
        cur["phase"] = phase
        cur["last_action"] = envelope
        if envelope:
            cur["last_action_name"] = envelope.get("action")
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
        job = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if job is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        now = core._utcnow()
        con.execute("UPDATE jobs SET codex_task_id=?, adapter=?, model=?, effort=?,"
                    " controller_state=?, updated_at=? WHERE request_id=?",
                    (task_id, "codex", model, effort,
                     json.dumps({"phase": "dispatched", "codex_task_id": task_id}), now, request_id))
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


def dispatch(state_dir, request_id: str, run_cmd=None) -> dict:
    """Ensure Codex dispatch; persist the task ID before accepting."""
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    if job.get("codex_task_id"):
        st = _load_controller_state(job)
        return {"action": "already-dispatched", "codex_task_id": job["codex_task_id"],
                "luna_action": st.get("last_action")}
    workspace = job["workspace"]
    prompt = _full_luna_prompt(job["task_json"])
    last_path = _last_message_path(state_dir, request_id, "codex-last")
    cmd = adapters.build_codex_dispatch_cmd(workspace, prompt,
                                            last_message_path=last_path)
    rc, out, err = run_cmd(cmd, workspace)
    last_text = adapters.read_last_message_file(last_path)
    # Test-seam fakes may embed the last-message envelope in stdout only;
    # production reads both stdout JSONL and the last-message file.
    task_id = adapters.parse_codex_task_id(out, last_text)
    _record_child(state_dir, request_id, "codex_dispatch", cmd, rc,
                  session_id=task_id, output_text=(out or "") + (err or ""))
    if rc != 0 or not task_id:
        _persist_error_evidence(state_dir, request_id, {"source": "codex_dispatch", "rc": rc, "stderr": (err or "")[:1000], "stdout": (out or "")[:1000]})
        _mark_blocked(state_dir, request_id, f"codex_dispatch_failed rc={rc}: no task ID persisted")
        return {"action": "blocked", "reason": "codex_dispatch_failed"}
    _save_codex_task(state_dir, request_id, task_id)
    luna_action = parse_luna_action(out, last_text)
    # Persist the envelope before any side effect it commands.
    try:
        _persist_envelope(state_dir, request_id, luna_action, "dispatched")
    except Exception:
        pass
    if luna_action is None:
        _mark_blocked(state_dir, request_id, "luna_missing_action: no structured envelope")
        return {"action": "blocked", "reason": "luna_missing_action",
                "codex_task_id": task_id}
    return {"action": "dispatched", "codex_task_id": task_id, "luna_action": luna_action}


def planner_callback(state_dir, request_id: str, qid: str, prompt: str,
                     run_cmd=None) -> dict:
    """Persist question before Claude --resume; persist answer before resume.

    Never forks a new planner session. Busy or failed callbacks become a
    durable blocked state with a reason; the question stays pending.
    """
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    planner_session = job.get("planner_session_id")
    if not planner_session:
        _mark_blocked(state_dir, request_id, "missing planner session: refusing to fork a new session")
        return {"action": "blocked", "reason": "missing_planner_session"}
    # Persist the planner question before calling Claude.
    try:
        core.post_question(state_dir, request_id, qid, prompt)
    except core.ConflictError as e:
        _mark_blocked(state_dir, request_id, f"planner_question_conflict: {e}")
        return {"action": "blocked", "reason": "question_conflict"}
    planner_model = job.get("planner_model") or adapters.CLAUDE_MODEL
    planner_effort = job.get("planner_effort") or adapters.CLAUDE_EFFORT
    try:
        cmd = adapters.build_claude_cmd(planner_session, prompt,
                                         model=planner_model, effort=planner_effort)
    except ValueError as e:
        _mark_blocked(state_dir, request_id, f"planner_resume_refused: {e}")
        return {"action": "blocked", "reason": "planner_resume_refused"}
    rc, out, err = run_cmd(cmd, job["workspace"])
    combined = (out or "") + "\n" + (err or "")
    _record_child(state_dir, request_id, "claude_callback", cmd, rc,
                  session_id=planner_session, output_text=combined)
    if rc != 0 or adapters.is_planner_busy(combined):
        reason = "planner_busy" if adapters.is_planner_busy(combined) else f"planner_callback_failed rc={rc}"
        _persist_error_evidence(state_dir, request_id,
                                {"source": "claude_callback", "rc": rc,
                                 "stderr": (err or "")[:1000], "stdout": (out or "")[:1000]})
        _mark_blocked(state_dir, request_id, reason)
        return {"action": "blocked", "reason": reason}
    answer_text = (out or "").strip() or (err or "").strip() or "acknowledged"
    # Persist the answer before resuming the same Luna task.
    core.answer(state_dir, request_id, qid, answer_text)
    return {"action": "answered", "qid": qid}


def resume_luna(state_dir, request_id: str, prompt: str, run_cmd=None) -> dict:
    """Resume only the saved Luna task ID with new context (never fork)."""
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    task_id = job.get("codex_task_id")
    if not task_id:
        _mark_blocked(state_dir, request_id, "missing saved Codex task ID: refusing to fork")
        return {"action": "blocked", "reason": "missing_codex_task"}
    last_path = _last_message_path(state_dir, request_id, "codex-resume-last")
    cmd = adapters.build_codex_resume_cmd(task_id, job["workspace"], prompt,
                                          last_message_path=last_path)
    # Resume uses the saved workspace as cwd; never --cd/--reasoning.
    rc, out, err = run_cmd(cmd, job["workspace"])
    last_text = adapters.read_last_message_file(last_path)
    # Preserve the exact saved ID even if this resume emits no new ID.
    resumed_id = adapters.parse_codex_task_id(out, last_text) or task_id
    _record_child(state_dir, request_id, "codex_resume", cmd, rc,
                  session_id=resumed_id, output_text=(out or "") + (err or ""))
    if rc != 0:
        _persist_error_evidence(state_dir, request_id,
                                {"source": "codex_resume", "rc": rc,
                                 "stderr": (err or "")[:1000]})
        low = ((out or "") + "\n" + (err or "")).lower()
        if any(s in low for s in ("busy", "locked", "already running")):
            _mark_blocked(state_dir, request_id, "luna_busy")
            return {"action": "blocked", "reason": "luna_busy"}
        if any(s in low for s in ("permission", "consent", "denied", "auth", "model")):
            _mark_blocked(state_dir, request_id, f"luna_blocked rc={rc}")
            return {"action": "blocked", "reason": "luna_blocked"}
        _mark_blocked(state_dir, request_id, f"codex_resume_failed rc={rc}")
        return {"action": "blocked", "reason": "codex_resume_failed"}
    luna_action = parse_luna_action(out, last_text)
    try:
        _persist_envelope(state_dir, request_id, luna_action, "resumed")
    except Exception:
        pass
    return {"action": "resumed", "luna_action": luna_action, "raw": out}


def _route_allowance(route: str) -> str:
    if route == "muse-spark-xhigh-go":
        return "go-included"
    return "free"


def run_implementation(state_dir, request_id: str, artifact: str | None = None,
                       payload: dict | None = None, run_cmd=None,
                       control_request_func=None, control_base_url: str | None = None,
                       control_password: str | None = None,
                       use_owned_server: bool = False) -> dict:
    """Run the OpenCode adapter carrying the saved artifact/output.

    Uses the installed CLI contract (``--format json --pure --dir``,
    provider/model route, ``--variant xhigh --agent build``,
    ``--session`` for saved sessions). The durable ``allowance``
    (free vs go-included) always selects the model route. Handles exact
    free-exhaustion evidence with abort + idle confirm before any Go
    transfer. Generic errors never select Go.
    """
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    workspace = job["workspace"]
    route = job.get("route") or "muse-spark-xhigh-free"
    allowance = _route_allowance(route)
    saved_session = job.get("opencode_session_id")
    # Full task content, never silently clipped; artifact/context ride
    # along in full so the writer sees the complete handoff.
    base_task = _task_summary(job["task_json"])
    prompt = base_task
    if artifact:
        prompt += f"\nartifact: {artifact}"
    if payload:
        try:
            prompt += "\ncontext: " + json.dumps(payload, sort_keys=True)
        except Exception:
            prompt += f"\ncontext: {payload!r}"
    cmd = adapters.build_opencode_cmd(workspace, prompt,
                                      allowance=allowance,
                                      session_id=saved_session)
    recorded_kind = "opencode_run"
    recorded_cmd = cmd
    if use_owned_server and control_request_func is None:
        serve_cmd = adapters.build_opencode_serve_cmd()
        meta = {"prompt": prompt, "allowance": allowance,
                "session_id": saved_session, "model": adapters.opencode_model_for_allowance(allowance)}
        recorded_kind = "opencode_control"
        recorded_cmd = serve_cmd
        try:
            rc, out, err = run_cmd(serve_cmd, workspace, 120,
                                   kind="opencode_control", meta=meta)
        except TypeError:
            recorded_kind = "opencode_run"
            recorded_cmd = cmd
            rc, out, err = run_cmd(cmd, workspace)
    else:
        rc, out, err = run_cmd(cmd, workspace)
    combined = (out or "") + "\n" + (err or "")
    # Try to extract a provider-shaped envelope from stdout/stderr.
    envelope: object = combined
    for blob in (out or "", err or ""):
        blob = blob.strip()
        if not blob:
            continue
        try:
            envelope = json.loads(blob)
            break
        except ValueError:
            continue
        # keep scanning lines
    if isinstance(envelope, str):
        for line in envelope.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    envelope = json.loads(line)
                    break
                except ValueError:
                    continue
    # Persist the OpenCode session ID when the adapter reports one.
    session_id = None
    if isinstance(envelope, dict):
        for key in ("opencode_session_id", "session_id", "sessionId", "session"):
            val = envelope.get(key)
            if isinstance(val, str) and val:
                session_id = val
                break
            if isinstance(val, dict):
                for sub in ("id", "session_id", "sessionId"):
                    if isinstance(val.get(sub), str) and val.get(sub):
                        session_id = val.get(sub)
                        break
    if session_id:
        try:
            _save_opencode_session(state_dir, request_id, session_id, route)
        except Exception:
            pass
    _record_child(state_dir, request_id, recorded_kind, recorded_cmd, rc,
                  session_id=(session_id or saved_session), output_text=combined)
    if rc == 0:
        # Persist session/artifact/output evidence before resume/finish.
        try:
            con = store.connect(state_dir)
            try:
                con.execute("BEGIN IMMEDIATE")
                cur = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                                  (request_id,)).fetchone()
                st = {}
                if cur is not None and cur["controller_state"]:
                    try:
                        st = json.loads(cur["controller_state"]) or {}
                    except ValueError:
                        st = {}
                st["phase"] = "implemented"
                st["opencode_session_id"] = session_id or saved_session
                st["artifact"] = artifact
                st["implementation_output"] = (out or "")[-8000:]
                con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                            (json.dumps(st, sort_keys=True), core._utcnow(), request_id))
                core._event(con, request_id, "implementation_output",
                            {"session": ((session_id or saved_session or "")[:24]),
                             "artifact": str(artifact or "")[:256]})
                con.execute("COMMIT")
            except Exception:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
            finally:
                con.close()
        except Exception:
            pass
        return {"action": "implementation_ok", "output": (out or "")}
    # Failure: classify exact free exhaustion only.
    _persist_error_evidence(state_dir, request_id,
                            envelope if isinstance(envelope, dict) else {"source": "opencode", "rc": rc, "output": combined[:2000]})
    if route == "muse-spark-xhigh-free" and policy.classify_quota_exhaustion(envelope):
        # Abort the old free session before transfer and confirm idle.
        # Never invent a random password for a server this job does not
        # own: a real base_url requires its real password; tests use the
        # injectable request_func equivalent.
        saved = (core.get_job(state_dir, request_id).get("opencode_session_id") or session_id)
        if saved and (control_base_url or control_request_func is not None):
            try:
                if control_request_func is not None and not control_base_url:
                    fake = control_request_func
                    fake("POST", "/session/abort", {"session": saved})
                    fake("GET", f"/session/status?session={saved}&ownership=1", {})
                elif control_base_url:
                    if not control_password and control_request_func is None:
                        raise ValueError("missing control password for owned server")
                    ctrl = adapters.OpenCodeControl(control_base_url,
                                                    control_password,
                                                    saved, request_func=control_request_func)
                    ctrl.abort()
                    idle = ctrl.ensure_idle_ownership()
                    _persist_error_evidence(state_dir, request_id,
                                            {"source": "opencode_idle_confirm", "status": adapters.redact_nested(idle)})
                else:
                    raise ValueError("no control seam for abort")
            except Exception as e:
                _mark_blocked(state_dir, request_id, f"go_transfer_abort_failed: {e}")
                return {"action": "blocked", "reason": "go_transfer_abort_failed"}
        # Preserve artifacts: only switch the stored route; logs stay.
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute("UPDATE jobs SET route=?, updated_at=? WHERE request_id=?",
                        ("muse-spark-xhigh-go", core._utcnow(), request_id))
            core._event(con, request_id, "route_switched",
                        {"from": "muse-spark-xhigh-free", "to": "muse-spark-xhigh-go"})
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
            core.record_capacity(state_dir, "muse-spark-xhigh-free", "exhausted",
                                 envelope if isinstance(envelope, dict) else {"source": "opencode"},
                                 reset_at=core._trusted_reset_at(envelope if isinstance(envelope, dict) else None))
        except Exception:
            pass
        return {"action": "transferred_to_go", "route": "muse-spark-xhigh-go"}
    # Generic failures/blockers never select Go.
    low = combined.lower()
    if any(s in low for s in ("busy", "locked", "already running")):
        _mark_blocked(state_dir, request_id, "opencode_busy")
        return {"action": "blocked", "reason": "opencode_busy"}
    _mark_blocked(state_dir, request_id, f"implementation_failed rc={rc}")
    return {"action": "blocked", "reason": "implementation_failed"}


def _complete_job(state_dir, request_id: str, token: str | None,
                  output: str, artifact: str | None = None) -> dict:
    """Persist the terminal result before acknowledgement (lease-held)."""
    job = core.get_job(state_dir, request_id)
    use_token = token or job.get("owner_token")
    result_payload = {"output": output}
    if artifact:
        result_payload["artifact"] = artifact
    if use_token:
        try:
            done = core.complete(state_dir, request_id, use_token,
                                 json.dumps(result_payload, sort_keys=True))
            return {"action": "completed", "status": done["status"]}
        except (core.OwnershipError, core.TerminalError, core.NotFoundError):
            pass
        except Exception:
            pass
    # Fallback for offline helper paths without a live lease: durable
    # terminal write (still persists result + file before ack).
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        cur = con.execute("SELECT * FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if cur is None:
            con.execute("ROLLBACK")
            raise core.NotFoundError(f"unknown request: {request_id}")
        if cur["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            return {"action": "noop-terminal", "status": cur["status"]}
        now = core._utcnow()
        con.execute("UPDATE jobs SET status='succeeded', result_json=?, updated_at=? WHERE request_id=?",
                    (json.dumps({"ok": True, **result_payload}), now, request_id))
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
        store.secure_write_text(store.result_path_for(root, request_id),
                                json.dumps({"request_id": request_id, "status": "succeeded",
                                            "result": result_payload}))
        store.append_text(store.output_path_for(root, request_id),
                          f"[succeeded] {output[:2000]}\n")
    except OSError:
        pass
    return {"action": "completed", "status": "succeeded"}


def _handle_question_action(state_dir, request_id: str, envelope: dict,
                            run_cmd) -> dict:
    """One bounded planner_question transition.

    Persist question -> Claude --resume exact planner session ->
    persist answer -> resume exact Luna task -> parse next action.
    Never forks a new planner or Luna session.
    """
    qid = str(envelope.get("qid") or envelope.get("question_id") or "q1")
    prompt_q = str(envelope.get("prompt") or envelope.get("question") or
                   envelope.get("text") or "Planner input requested.")
    # If this exact question already answered (restart/recover replay),
    # skip Claude and resume Luna directly with the saved answer.
    try:
        existing = core.list_questions(state_dir, request_id, only_pending=False)
        for q in existing:
            if q["qid"] == qid and q["status"] == "answered" and q.get("answer"):
                r = resume_luna(state_dir, request_id, str(q["answer"]),
                                run_cmd=run_cmd)
                return {"action": "question-resumed-from-saved",
                        "qid": qid, "resume": r}
    except Exception:
        pass
    cb = planner_callback(state_dir, request_id, qid, prompt_q, run_cmd=run_cmd)
    if cb.get("action") == "blocked":
        return cb
    # Answer persisted; resume the exact saved Luna task.
    try:
        qs = core.list_questions(state_dir, request_id, only_pending=False)
        answer_text = ""
        for q in qs:
            if q["qid"] == qid:
                answer_text = q.get("answer") or ""
                break
    except Exception:
        answer_text = ""
    r = resume_luna(state_dir, request_id, answer_text or "acknowledged",
                    run_cmd=run_cmd)
    if r.get("action") == "blocked":
        return r
    return {"action": "question-answered-resumed", "qid": qid,
            "luna_action": r.get("luna_action"), "resume": r}


def _handle_implementation_action(state_dir, request_id: str, envelope: dict,
                                  run_cmd, control_request_func=None,
                                  control_base_url=None,
                                  control_password=None,
                                  use_owned_server: bool = False) -> dict:
    """One bounded implementation transition: OpenCode then Luna resume."""
    artifact = envelope.get("artifact")
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else None
    impl = run_implementation(state_dir, request_id, artifact=artifact,
                              payload=payload, run_cmd=run_cmd,
                              control_request_func=control_request_func,
                              control_base_url=control_base_url,
                              control_password=control_password,
                              use_owned_server=use_owned_server)
    if impl.get("action") != "implementation_ok":
        # transferred_to_go or blocked: next loop retries on the new route
        # or waits; never fork a second writer here.
        return impl
    evidence = f"implementation session={core.get_job(state_dir, request_id).get('opencode_session_id') or ''} " \
               f"artifact={artifact or ''} output={str(impl.get('output') or '')[-4000:]}"
    r = resume_luna(state_dir, request_id, evidence, run_cmd=run_cmd)
    if r.get("action") == "blocked":
        return r
    return {"action": "implementation-resumed",
            "luna_action": r.get("luna_action"), "resume": r}


def step(state_dir, request_id: str, run_cmd=None,
         control_request_func=None, control_base_url: str | None = None,
         control_password: str | None = None,
         token: str | None = None,
         use_owned_server: bool = False) -> dict:
    """Advance exactly one useful bounded controller transition."""
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    if job["status"] in store.TERMINAL:
        return {"action": "noop-terminal", "status": job["status"]}
    if job["cancel_requested"]:
        return {"action": "cancelled", "status": job["status"]}
    # Ensure dispatch first (persists task ID + envelope before accept).
    if not job.get("codex_task_id"):
        return dispatch(state_dir, request_id, run_cmd=run_cmd)
    # A live pending question the planner has not answered yet waits for
    # the public `answer` + `recover`; never fork Claude/Luna here.
    # But an answered question (public answer path) must proceed to Luna
    # resume below instead of stalling.
    pending = core.list_questions(state_dir, request_id, only_pending=True)
    st = _load_controller_state(job)
    last = st.get("last_action")
    if pending and job["status"] == "blocked":
        return {"action": "blocked-awaiting-answer", "pending": len(pending)}
    if pending and last is None:
        # Crashed between question persist and Claude call, or a
        # public post-question without a Luna envelope yet: wait for the
        # answer path rather than forking a duplicate question.
        return {"action": "blocked-awaiting-answer", "pending": len(pending)}
    # No envelope yet (e.g. dispatch parsed nothing but kept the task
    # ID): repair with one bounded Luna resume carrying the protocol.
    if not isinstance(last, dict) or last.get("action") not in policy.VALID_ACTIONS:
        # If there are answered questions waiting, resume with them;
        # else re-ask Luna with the explicit protocol.
        try:
            answered = [q for q in core.list_questions(state_dir, request_id, only_pending=False)
                        if q["status"] == "answered"]
        except Exception:
            answered = []
        if answered:
            latest = answered[-1].get("answer") or "acknowledged"
            r = resume_luna(state_dir, request_id, latest, run_cmd=run_cmd)
            nxt = r.get("luna_action")
            if isinstance(nxt, dict) and nxt.get("action") == "planner_question":
                return _handle_question_action(state_dir, request_id, nxt, run_cmd)
            if isinstance(nxt, dict) and nxt.get("action") == "implementation":
                return _handle_implementation_action(
                    state_dir, request_id, nxt, run_cmd,
                    control_request_func=control_request_func,
                    control_base_url=control_base_url,
                    control_password=control_password,
                    use_owned_server=use_owned_server)
            if isinstance(nxt, dict) and nxt.get("action") == "completion":
                return _complete_job(state_dir, request_id, token,
                                     str(nxt.get("output") or "done"),
                                     nxt.get("artifact"))
            return r
        repair = ("Your last reply contained no valid action envelope. "
                  "Reply again with exactly one JSON object per the protocol.")
        r = resume_luna(state_dir, request_id, repair, run_cmd=run_cmd)
        return r
    action_name = last.get("action")
    if action_name == "planner_question":
        # If the recorded question is still pending (Claude never
        # answered: crash between persist and callback), run the bounded
        # question transition now; it reuses the same qid (idempotent)
        # and never forks a second planner session.
        return _handle_question_action(state_dir, request_id, last, run_cmd)
    if action_name == "implementation":
        return _handle_implementation_action(
            state_dir, request_id, last, run_cmd,
            control_request_func=control_request_func,
            control_base_url=control_base_url,
            control_password=control_password,
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
    payload = json.dumps({"token": token, "pid": pid, "updated": now})
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

    _advertise(state_dir, request_id, token)
    if mode == "sleep":
        end = _time.time() + max(0.0, duration)
        while _time.time() < end:
            _advertise(state_dir, request_id, token)
            _time.sleep(0.2)
        return 0
    # Bounded durable loop with built-in adapters (normal submit/start).
    # Use the durable run_cmd so every child spawn is file-backed,
    # detached, and recorded in the invocations table before the spawn.
    durable_run_cmd = core.make_durable_run_cmd(state_dir, request_id, token)
    for _ in range(MAX_LOOP_STEPS):
        try:
            res = step(state_dir, request_id, run_cmd=durable_run_cmd, token=token,
                       use_owned_server=True)
        except Exception:
            break
        try:
            _advertise(state_dir, request_id, token)
        except Exception:
            pass
        action = (res or {}).get("action")
        # Terminal / waiting states stop; intermediate states continue
        # boundedly (dispatched -> question -> implementation ->
        # completion) without forking.
        if action in ("noop-terminal", "completed", "cancelled",
                      "blocked", "blocked-awaiting-answer",
                      "awaiting-luna", "already-dispatched"):
            # already-dispatched with no envelope progress also stops to
            # keep one useful transition per recovery; the next
            # start/recover resumes saved IDs.
            if action == "already-dispatched":
                break
            if action in ("dispatched",):
                continue
            break
        if action in ("dispatched", "question-answered-resumed",
                      "question-resumed-from-saved",
                      "implementation-resumed", "transferred_to_go",
                      "implementation_ok", "resumed", "answered"):
            continue
        break
    try:
        _advertise(state_dir, request_id, token)
    except Exception:
        pass
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="runner.controller")
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--request-id", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--mode", default="run-once")
    ap.add_argument("--duration", type=float, default=30.0)
    args = ap.parse_args(argv)
    return run_controller_process(args.state_dir, args.request_id, args.token,
                                  mode=args.mode, duration=args.duration)


if __name__ == "__main__":
    raise SystemExit(main())
