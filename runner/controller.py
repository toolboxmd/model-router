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
import signal
import subprocess
import time
from pathlib import Path

from . import adapters, core, harnesses, policy, store
from . import codex_native

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


def native_turn_meta(op: str, prompt: str, model: str | None, effort: str | None,
                      route: str, reason: str) -> dict:
    """Invocation meta for one native dispatcher turn (dispatch or resume).

    The prompt, policy model, and effort travel in meta (never in argv),
    so the driver argv stays stable across restarts and the action keeps
    one identity. Tests reuse this helper to compute the same key.
    """
    return {"stage": "dispatch", "route": route, "reason": reason,
            "prompt": prompt, "model": model, "effort": effort,
            codex_native.NATIVE_META_KEY: {"op": op}}


def _record_native_control_turn(state_dir, request_id: str, out) -> None:
    """Best-effort control-turn tracking from converted driver output.

    The native thread and turn ids stay authoritative; unrelated user
    turns never appear in driver output (the driver filters by turn id),
    so whatever parses here is the control turn. Never raises.
    """
    try:
        patch = {}
        turn_id = codex_native.turn_id_from_lines(out or "")
        if turn_id:
            patch["control_turn_id"] = turn_id
        thread_id = codex_native.thread_id_from_lines(out or "")
        if thread_id:
            patch["thread_id"] = thread_id
        if patch:
            codex_native.write_runtime(state_dir, request_id, patch)
    except Exception:
        pass


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
                      phase: str, raw_text: str | None = None) -> None:
    """Persist the Luna action envelope BEFORE its side effect.

    ``raw_text`` is the dispatcher text the envelope was extracted from
    (the OpenCode summary's last assistant message): its redacted tail
    travels in the ledger event so Agent Observer reads what Luna said
    without querying the private output files.
    """
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
            detail = {"action": str(envelope.get("action") or "unknown")[:64],
                      "phase": phase}
            if isinstance(raw_text, str) and raw_text.strip():
                try:
                    kept = adapters.redact_text(raw_text)
                except Exception:
                    kept = raw_text
                detail["assistant_text"] = " ".join(kept.split())[:2000]
            core._event(con, request_id, "luna_action", detail)
        else:
            missing = {"phase": phase}
            if isinstance(raw_text, str) and raw_text.strip():
                try:
                    kept = adapters.redact_text(raw_text)
                except Exception:
                    kept = raw_text
                missing["assistant_text"] = " ".join(kept.split())[:2000]
            core._event(con, request_id, "luna_action_missing", missing)
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
        if isinstance(raw_error, dict):
            raw_class = str(raw_error.get("class") or raw_error.get("code")
                            or raw_error.get("reason") or "error")[:200]
        else:
            raw_class = str(raw_error)[:200]
        # Free-text secrets must never reach the events table: key-based
        # redaction cannot see inside them, so mask secret shapes first.
        error_class = adapters.redact_text(raw_class)
        core._event(con, request_id, "provider_error_evidence", {"error_class": error_class})
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


def _block_runtime_missing(state_dir, request_id: str, kind: str,
                           cmd: list[str], meta: dict | None) -> dict | None:
    """Recoverable block when a turn provably never started for a missing runtime.

    Returns the blocked outcome when the action's newest non-abandoned
    row carries explicit never-started runtime-missing evidence, else
    None. The reason names the concrete cause and the next action (run
    recover on the installed runtime); the planner is never asked to
    restore cache directories. Rows without the explicit marker keep
    their existing handling, however empty their output is.
    """
    detail = core.runtime_missing_for_action(state_dir, request_id, kind, cmd, meta)
    if not detail:
        return None
    cause = detail.get("cause") or "old runtime path is gone; no supervisor or child started"
    _persist_error_evidence(state_dir, request_id,
                            {"source": kind, "rc": 127, "error": cause})
    reason = (f"runtime_missing: {cause}; run recover on the installed runtime "
              "(do not restore cache directories)")
    _mark_blocked(state_dir, request_id, reason)
    return {"action": "blocked", "reason": "runtime_missing"}


def _save_codex_task(state_dir, request_id: str, task_id: str,
                     model: str = adapters.CODEX_MODEL,
                     effort: str = adapters.CODEX_EFFORT,
                     adapter: str = "codex",
                     dispatch_route: str | None = None) -> dict:
    """Persist the dispatcher task and its active route in one transaction,
    so a crash between two writes cannot leave a session without its harness."""
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
        if dispatch_route is not None:
            st["dispatch_route"] = dispatch_route
        con.execute("UPDATE jobs SET codex_task_id=?, adapter=?, model=?, effort=?,"
                    " controller_state=?, updated_at=? WHERE request_id=?",
                    (task_id, adapter, model, effort,
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
                           model: str = adapters.OPENCODE_FREE_MODEL,
                           effort: str = adapters.OPENCODE_VARIANT) -> dict:
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


def _codex_probe_due(state_dir) -> bool:
    """True when a Codex usage probe is due before dispatch."""
    try:
        readings = core.list_readings(state_dir, pool="codex")
    except Exception:
        return True
    latest = None
    for r in readings or []:
        obs = r.get("observed_at")
        if isinstance(obs, str) and obs:
            if latest is None or obs > latest:
                latest = obs
    try:
        return core.probe_due_for("codex", latest)
    except Exception:
        return True


def _record_codex_probe_unknown(state_dir, route: str, reason: str,
                                shape=None) -> None:
    """Record unknown Codex readings for the probe windows, never raising.

    ``shape`` is the keys-only outline of a response that carried no rate
    record, so the parser can be fixed from the ledger.
    """
    try:
        observed = core._utcnow()
    except Exception:
        import datetime as _dt
        observed = _dt.datetime.now(_dt.timezone.utc).isoformat()
    try:
        shape_list = [str(k) for k in (shape or [])][:20]
    except Exception:
        shape_list = []
    for window in ("5h", "weekly"):
        try:
            detail = {"error": str(reason)[:300],
                      "probe": "codex-rate-limits"}
            if shape_list:
                detail["shape"] = shape_list
            core.note_probe_failure(state_dir, route, window,
                                    source="provider_reported",
                                    detail=detail,
                                    observed_at=observed)
        except Exception:
            continue


def _record_codex_probe_readings(state_dir, readings) -> None:
    """Store parsed Codex probe readings, never raising."""
    for r in readings or []:
        try:
            core.record_reading(state_dir, r["pool"], r["model"],
                                r["window"], r["used"], r["limit"],
                                r["reset_at"],
                                observed_at=r["observed_at"],
                                source=r["source"],
                                detail=r.get("detail"))
        except Exception:
            continue


def _default_codex_probe(state_dir, request_id: str, route: str,
                         query_fn=None) -> dict:
    """Best-effort Codex rate-limit probe before dispatch, never blocking.

    When due, reads ``account/rateLimits/read`` through the app-server
    first and stores the windows; a response with no rate record stores
    unknown with the raw shape (keys only) in the reading detail. The
    zero-cost session rollout is the fallback when the app-server cannot
    be read; with neither source the probe records unknown with its
    reason. Never raises: dispatch stays eligible on error evidence alone.
    ``query_fn`` overrides the app-server read (deterministic tests).
    """
    if not _codex_probe_due(state_dir):
        return {"action": "probe-skipped", "route": route}
    harness = harnesses.harness_named("codex")
    try:
        observed = core._utcnow()
    except Exception:
        import datetime as _dt
        observed = _dt.datetime.now(_dt.timezone.utc).isoformat()
    query_error = None
    try:
        query = query_fn or harness.query_app_server_rate_limits
        payload = query()
    except Exception as e:  # noqa: BLE001 - rollout fallback below
        payload = None
        query_error = f"{type(e).__name__}: {str(e)[:150]}"
    if payload is not None:
        try:
            readings = harness.probe_rate_limits(payload, observed)
        except Exception:
            readings = []
        if readings:
            _record_codex_probe_readings(state_dir, readings)
            return {"action": "probe-ok", "route": route,
                    "windows": sorted({r.get("window") for r in readings})}
        try:
            shape = harnesses.codex_payload_shape(payload)
        except Exception:
            shape = []
        _record_codex_probe_unknown(state_dir, route, "no rate record", shape)
        return {"action": "probe-unknown", "route": route}
    # Zero-cost fallback: the newest rollout file under the Codex home, when
    # one exists. Before the first dispatch there is usually none, which
    # records unknown like any other unreadable probe.
    try:
        try:
            home = policy._codex_home()
        except Exception:
            home = None
        candidates: list = []
        if home is not None:
            try:
                from pathlib import Path as _P
                root = _P(os.path.expanduser(str(home)))
                for pat in ("rollouts/**/*.jsonl", "rollouts/*.jsonl",
                            "sessions/**/*.jsonl", "*.jsonl"):
                    try:
                        candidates.extend([p for p in root.glob(pat) if p.is_file()])
                    except Exception:
                        continue
            except Exception:
                candidates = []
        candidates = sorted(
            (p for p in candidates if p.is_file()),
            key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
            reverse=True)[:3]
        for cand in candidates:
            try:
                readings = harness.read_rollout(str(cand))
            except Exception:
                continue
            if not readings:
                continue
            _record_codex_probe_readings(state_dir, readings)
            return {"action": "probe-ok", "route": route,
                    "windows": sorted({r.get("window") for r in readings})}
    except Exception as e:  # noqa: BLE001 - unknown below, never blocks
        _record_codex_probe_unknown(state_dir, route, f"probe failed: {type(e).__name__}")
        return {"action": "probe-unknown", "route": route}
    if query_error:
        _record_codex_probe_unknown(state_dir, route,
                                    f"probe unavailable: {query_error}")
    else:
        _record_codex_probe_unknown(state_dir, route, "probe unavailable: no rate record")
    return {"action": "probe-unknown", "route": route}


def _codex_auth_block(state_dir, request_id: str, rc, out, err) -> dict | None:
    """Blocked auth result when Codex text shows a missing/rejected login.

    Returns the blocked payload after persisting hard/auth evidence, or
    None when the output carries no auth marker. Auth never falls back to
    another route: no other route holds the same login.
    """
    try:
        reason = harnesses.harness_named("codex").auth_failure_reason(out, err)
    except Exception:
        reason = None
    if not reason:
        return None
    try:
        home = policy._codex_home()
        from pathlib import Path as _P
        missing = not (_P(os.path.expanduser(str(home))) / "auth.json").is_file()
    except Exception:
        missing = False
    if missing and "auth.json" not in reason.lower():
        reason = f"{reason} (auth.json missing in CODEX_HOME)"[:160]
    _persist_error_evidence(state_dir, request_id,
                            {"source": "codex_dispatch", "rc": rc,
                             "signal": "hard", "reason": "auth",
                             "error": reason,
                             "stderr": (err or "")[:1000],
                             "stdout": (out or "")[:1000]})
    _mark_blocked(state_dir, request_id, f"codex_auth_failed: {reason}")
    return {"action": "blocked", "reason": "codex_auth_failed"}


def _record_dispatch_reason(state_dir, request_id: str, prev: str,
                            target: str, reason: str) -> None:
    """Record why the dispatch moved routes, apart from the worker reason.

    The dispatch reason lands only on ``dispatch_route_reason``; the
    shared ``route_reason`` key stays untouched so the first worker
    invocation keeps its own reason (``initial`` until a worker move
    sets one). The ``route_switched`` event carries ``scope: dispatch``,
    so a later worker move overwriting ``route_reason`` never disguises
    a worker attempt with the dispatch cause. Never raises past the
    caller: a lost lease still yields to the owner.
    """
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        try:
            st = json.loads(row["controller_state"] or "{}") if row is not None else {}
        except ValueError:
            st = {}
        if not isinstance(st, dict):
            st = {}
        st["dispatch_route_reason"] = reason
        con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), core._utcnow(), request_id))
        core._event(con, request_id, "route_switched",
                    {"from": prev, "to": target, "reason": reason,
                      "scope": "dispatch",
                      "evidence": "codex_dispatch"})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _codex_failure_message(out, err, cmd) -> str:
    """Short redacted last provider message for a dispatch block reason."""
    try:
        msg = harnesses.harness_named("codex").last_provider_message(out, err, cmd)
    except Exception:
        msg = ""
    msg = " ".join((msg or "").split())
    return msg[:200] or "empty provider output"


def codex_model_mismatch_reason(requested_model, observed_model) -> str | None:
    """Block reason when the observed Codex model differs, else None.

    A mismatch is never a capacity signal: unknown on either side never
    blocks, and the reason names both models.
    """
    if isinstance(requested_model, str) and requested_model \
            and isinstance(observed_model, str) and observed_model:
        if requested_model != observed_model:
            return f"model_mismatch: requested={requested_model} observed={observed_model}"
    return None


def _latest_codex_invocation(state_dir, request_id: str, kind: str) -> dict | None:
    """The just-measured invocation for one Codex kind, or None.

    The latest row by id wins, so prior dispatch/resume rows never shadow
    the current turn. Fake run_cmd closures leave no invocation rows, which
    reads as unknown and never blocks. Database failures propagate so the
    caller blocks safely instead of passing as unknown.
    """
    invs = [i for i in core._list_invocations(state_dir, request_id)
            if i.get("kind") == kind]
    if not invs:
        return None
    try:
        return max(invs, key=lambda r: int(r.get("id") or 0))
    except (TypeError, ValueError):
        return invs[-1]


def check_codex_observed_model(state_dir, request_id: str, kind: str,
                               requested_route: str) -> tuple:
    """(observed_model, block_reason|None) for one measured Codex turn.

    Compares the invocation row's observed_model with
    ``policy.ROUTES[requested_route]['model']``. The requested route comes
    from the invocation when it names a policy route, else from the
    caller's controller state. Unknown on either side never blocks.
    Only expected missing rollout cases return unknown; database, policy,
    or implementation failures propagate so the caller blocks safely.
    """
    route_name = requested_route if requested_route in policy.ROUTES else None
    inv = _latest_codex_invocation(state_dir, request_id, kind)
    if inv is not None:
        inv_route = inv.get("requested_route")
        if isinstance(inv_route, str) and inv_route in policy.ROUTES:
            route_name = inv_route
    if not route_name:
        return None, None
    requested_model = policy.ROUTES[route_name].get("model")
    if not isinstance(requested_model, str) or not requested_model:
        return None, None
    observed = inv.get("observed_model") if isinstance(inv, dict) else None
    if not isinstance(observed, str) or not observed:
        return None, None
    return observed, codex_model_mismatch_reason(requested_model, observed)


def _block_codex_observation_failure(state_dir, request_id: str, kind: str,
                                     exc: Exception, task_id: str | None = None) -> dict:
    """Block safely on an unexpected observation failure, with evidence.

    Never retries, falls back, marks capacity, moves routes, or applies an
    envelope. Expected missing or unreadable rollout cases never reach
    here; they stay unknown.
    """
    try:
        detail = f"{type(exc).__name__}: {str(exc)[:150]}"
    except Exception:
        detail = "observation failed"
    reason = f"model_observation_failed: {kind} {detail}"[:300]
    try:
        _persist_error_evidence(state_dir, request_id,
                                {"source": kind, "error": detail[:1000]})
    except Exception:
        pass
    _mark_blocked(state_dir, request_id, reason)
    out = {"action": "blocked", "reason": reason}
    if task_id is not None:
        out["codex_task_id"] = task_id
    return out


def _assistant_text_quote(full) -> str:
    """First 200 characters of an OpenCode summary's assistant text.

    A missing envelope still blocks, but with what Luna actually said
    instead of a bare "no structured envelope". Redacted first (the
    text is model-authored and can echo secrets), then collapsed to one
    line; blank text reports as empty dispatcher output, never empty.
    """
    text = full.get("assistant_text") if isinstance(full, dict) else None
    if not isinstance(text, str) or not text.strip():
        return "empty dispatcher output"
    try:
        text = adapters.redact_text(text)
    except Exception:
        pass
    return " ".join(text.split())[:200] or "empty dispatcher output"


def _dispatch_fallback_routes(dispatch_route: str) -> list:
    """Owned-server dispatch routes after ``dispatch_route``, in policy order."""
    return [r for r in policy.stage_routes("dispatch") if r != dispatch_route
            and harnesses.route_uses_owned_server(r)]


def _codex_stall_marker(err) -> str | None:
    """The supervisor's stall marker in Codex stderr, or None.

    The supervisor ends a turn whose stream stays silent past the harness
    window with ``runner: stalled: ...`` on stderr (rc 4) and records the
    invocation as signal ``stalled``. The marker is the controller's
    evidence that this failure is a stall, checked before auth so command
    output quoting auth-pattern text can never win.
    """
    for line in (err or "").splitlines():
        if "runner: stalled" in line:
            return line.strip()
    return None


def _codex_dispatch_stalled(state_dir, request_id: str, dispatch_route: str,
                            prompt: str, run_cmd, out, err, cmd, rc) -> dict | None:
    """Lateral move when the supervisor ended a Codex dispatch turn silent.

    Records the stall (signal ``stalled``, never ``hard``) and dispatches
    on the next dispatch route, Luna on OpenCode Go, in the same step
    without retrying Codex. Returns None when the output carries no stall
    marker: the caller keeps its auth/exhausted/generic failure handling.
    """
    marker = _codex_stall_marker(err)
    if marker is None:
        return None
    try:
        detail = adapters.redact_text(marker)
    except Exception:
        detail = marker
    detail = " ".join(detail.split())[:200] or "no harness output past the silence window"
    _persist_error_evidence(state_dir, request_id,
                            {"source": "codex_dispatch", "rc": rc,
                             "signal": "stalled",
                             "evidence": {"source": "stream_silence",
                                          "detail": detail},
                             "stderr": (err or "")[-1000:],
                             "stdout": (out or "")[-1000:]})
    fallback = _dispatch_fallback_routes(dispatch_route)
    if not fallback:
        _mark_blocked(state_dir, request_id,
                      f"dispatch_stalled: no eligible route after stalled on {dispatch_route}")
        return {"action": "blocked", "reason": "dispatch_stalled"}
    _record_dispatch_reason(state_dir, request_id, dispatch_route,
                            fallback[0], "dispatch_stalled")
    return _dispatch_on_opencode(state_dir, request_id, fallback[0], prompt, run_cmd,
                                 reason="dispatch_stalled")


def _codex_resume_stalled(state_dir, request_id: str, rc, out, err) -> dict | None:
    """Blocked stalled result when the supervisor ended a Codex resume silent.

    Records the stall (signal ``stalled``, never ``hard``/``auth``) and
    blocks with a stalled reason a later resume can retry; a resume has no
    lateral route. Returns None without a stall marker.
    """
    marker = _codex_stall_marker(err)
    if marker is None:
        return None
    try:
        detail = adapters.redact_text(marker)
    except Exception:
        detail = marker
    detail = " ".join(detail.split())[:200] or "no harness output past the silence window"
    _persist_error_evidence(state_dir, request_id,
                            {"source": "codex_resume", "rc": rc,
                             "signal": "stalled",
                             "evidence": {"source": "stream_silence",
                                          "detail": detail},
                             "stderr": (err or "")[-1000:]})
    _mark_blocked(state_dir, request_id, f"codex_resume_stalled rc={rc}: {detail}")
    return {"action": "blocked", "reason": "codex_resume_stalled"}


def _dispatch_exhausted(state_dir, request_id: str, dispatch_route: str,
                        prompt: str, run_cmd, out, err, cmd, rc) -> dict | None:
    """Fallback when the Codex dispatch answers its usage-limit message.

    Classifies the refusal as ``exhausted`` with the carried reset time,
    marks the Codex pool exhausted until that reset in the capacity ledger
    (like the implementation stage), records the route reason, and
    dispatches on the next dispatch route, Luna on OpenCode Go, in the same
    step without retrying Codex. Returns None when the output carries no
    usage-limit shape: the caller keeps its generic failure.
    """
    codex_h = harnesses.harness_named("codex")
    try:
        signal, evidence = codex_h.dispatch_limit_signal(out, err, cmd, rc)
    except Exception:
        signal, evidence = None, None
    if signal != "exhausted" or not isinstance(evidence, dict):
        return None
    reset_at, source = core.reset_at_for_evidence(dispatch_route, evidence, "exhausted")
    try:
        core.record_capacity(state_dir, dispatch_route, "exhausted", evidence,
                             reset_at, reset_source=source)
    except Exception:
        pass
    _persist_error_evidence(state_dir, request_id,
                            {"source": "codex_dispatch", "rc": rc,
                             "signal": "exhausted", "evidence": evidence,
                             "reset_at": reset_at,
                             "stderr": (err or "")[:1000],
                             "stdout": (out or "")[:1000]})
    fallback = _dispatch_fallback_routes(dispatch_route)
    if not fallback:
        _mark_blocked(state_dir, request_id,
                      f"capacity_exhausted: no eligible route after exhausted on {dispatch_route}")
        return {"action": "blocked", "reason": "capacity_exhausted"}
    _record_dispatch_reason(state_dir, request_id, dispatch_route,
                            fallback[0], "dispatch_exhausted")
    return _dispatch_on_opencode(state_dir, request_id, fallback[0], prompt, run_cmd,
                                 reason="dispatch_exhausted")


def dispatch(state_dir, request_id: str, run_cmd=None, probe=None) -> dict:
    """Ensure Codex dispatch; persist the task ID before accepting.

    ``probe`` is an optional on-demand usage probe
    ``probe(state_dir, request_id, route)`` (for example Codex
    ``account/rateLimits/read`` before a dispatch). It runs best-effort:
    a failing or slow probe records unknown and never blocks the
    dispatch, which stays eligible on error evidence alone. When ``probe``
    is None the harness default runs when due.
    """
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
    dispatch_route = policy.stage_routes("dispatch")[0]
    if probe is not None:
        try:
            probe(state_dir, request_id, dispatch_route)
        except Exception as e:  # noqa: BLE001 - record unknown, never block
            try:
                _record_codex_probe_unknown(
                    state_dir, dispatch_route,
                    f"probe failed: {type(e).__name__}: {str(e)[:200]}")
            except Exception:
                pass
    else:
        try:
            _default_codex_probe(state_dir, request_id, dispatch_route)
        except Exception:
            pass  # probes never block routing
    # Preflight: a Codex pool the capacity memory knows as exhausted or
    # resting is skipped before any child starts; later jobs go straight to
    # Luna on OpenCode Go until the reset revalidates.
    try:
        _skip = core.exhausted_routes(state_dir) | core.degraded_routes(state_dir)
    except Exception:
        _skip = set()
    if dispatch_route in _skip:
        _exhausted_now = False
        try:
            _exhausted_now = dispatch_route in core.exhausted_routes(state_dir)
        except Exception:
            _exhausted_now = dispatch_route in _skip
        _pre_reason = "preflight_exhausted" if _exhausted_now else "preflight_degraded"
        _fallback = _dispatch_fallback_routes(dispatch_route)
        if not _fallback:
            _mark_blocked(state_dir, request_id,
                          f"capacity_exhausted: no eligible route before dispatch on {dispatch_route}")
            return {"action": "blocked", "reason": "capacity_exhausted"}
        _record_dispatch_reason(state_dir, request_id, dispatch_route,
                                _fallback[0], _pre_reason)
        return _dispatch_on_opencode(state_dir, request_id, _fallback[0], prompt, run_cmd,
                                     reason=_pre_reason)
    dispatch = policy.ROUTES[dispatch_route]
    if harnesses.route_uses_owned_server(dispatch_route):
        return _dispatch_on_opencode(state_dir, request_id, dispatch_route, prompt, run_cmd, "initial")
    # Native dispatcher transport (toolboxmd/model-router#86): one
    # job-owned app-server drives the turn through the native protocol.
    # The driver argv carries no prompt, port, or endpoint, so the action
    # keeps one identity across restarts; the turn's converted JSONL flows
    # through the same downstream handling as before.
    cmd = codex_native.python_driver_cmd()
    try:
        rc, out, err = run_cmd(cmd, workspace, None, kind="codex_dispatch",
                               meta=native_turn_meta(codex_native.OP_DISPATCH, prompt,
                                                     dispatch["model"], dispatch["variant"],
                                                     dispatch_route, "initial"))
    except Exception as e:
        # A failed measurement (including a failed observed_model write) or
        # any other turn failure must block safely with evidence, never let
        # a completion through as unknown.
        return _block_codex_observation_failure(
            state_dir, request_id, "codex_dispatch", e)
    codex_h = harnesses.harness_named("codex")
    task_id, _skind = codex_h.parse_session("codex_dispatch", out, err, job)
    _record_child(state_dir, request_id, "codex_dispatch", cmd, rc,
                  session_id=task_id, output_text=(out or "") + (err or ""))
    _record_native_control_turn(state_dir, request_id, out)
    if task_id:
        # The thread exists even when the turn failed: save it so recovery
        # resumes it instead of creating a replacement task.
        _save_codex_task(state_dir, request_id, task_id, model=dispatch["model"],
                         effort=dispatch["variant"], dispatch_route=dispatch_route)
    # Observed-model gate before any failure classification, auth or
    # usage-limit fallback, envelope, or route movement. A known
    # requested-versus-observed mismatch blocks with model_mismatch naming
    # both models, with no retry, fallback, capacity mark, or envelope.
    # Unknown never blocks, and only genuinely missing or unreadable
    # rollout cases stay unknown inside the rollout reader. Any other
    # observation failure blocks safely with evidence.
    # Failed turns gate alike: a mismatched failed
    # turn beats usage-limit and generic failure handling.
    try:
        _obs, _mm_reason = check_codex_observed_model(
            state_dir, request_id, "codex_dispatch", dispatch_route)
    except Exception as e:
        return _block_codex_observation_failure(
            state_dir, request_id, "codex_dispatch", e, task_id)
    if _mm_reason is not None:
        _mark_blocked(state_dir, request_id, _mm_reason)
        out_blocked = {"action": "blocked", "reason": _mm_reason}
        if task_id:
            out_blocked["codex_task_id"] = task_id
        return out_blocked
    # A turn that provably never started for a missing runtime blocks
    # recoverably instead of falling through to signal classification,
    # the OpenCode fallback, or a sticky dispatch failure. The check
    # recomputes the exact action identity, so it carries the same
    # native turn meta as the run above (prompt/model travel in meta).
    runtime_blocked = _block_runtime_missing(
        state_dir, request_id, "codex_dispatch", cmd,
        native_turn_meta(codex_native.OP_DISPATCH, prompt,
                         dispatch["model"], dispatch["variant"],
                         dispatch_route, "initial"))
    if runtime_blocked is not None:
        if task_id:
            runtime_blocked["codex_task_id"] = task_id
        return runtime_blocked
    if not task_id:
        stalled = _codex_dispatch_stalled(state_dir, request_id, dispatch_route,
                                          prompt, run_cmd, out, err, cmd, rc)
        if stalled is not None:
            return stalled
        auth_blocked = _codex_auth_block(state_dir, request_id, rc, out, err)
        if auth_blocked is not None:
            return auth_blocked
        exhausted = _dispatch_exhausted(state_dir, request_id, dispatch_route,
                                        prompt, run_cmd, out, err, cmd, rc)
        if exhausted is not None:
            return exhausted
        message = _codex_failure_message(out, err, cmd)
        _persist_error_evidence(state_dir, request_id, {"source": "codex_dispatch", "rc": rc, "message": message, "stderr": (err or "")[:1000], "stdout": (out or "")[:1000]})
        # The Codex dispatcher could not start a task at all: try the next
        # dispatch route once, the same model on OpenCode in plan mode.
        fallback = _dispatch_fallback_routes(dispatch_route)
        if fallback:
            _record_dispatch_reason(state_dir, request_id, dispatch_route,
                                    fallback[0], f"dispatch_fallback rc={rc}")
            return _dispatch_on_opencode(state_dir, request_id, fallback[0], prompt, run_cmd,
                                         reason=f"dispatch_fallback rc={rc}")
        _mark_blocked(state_dir, request_id, f"codex_dispatch_failed rc={rc}: no task ID persisted: {message}")
        return {"action": "blocked", "reason": "codex_dispatch_failed"}
    if not codex_h.turn_ok("codex_dispatch", rc, out, cmd):
        # A stall the supervisor ended for stream silence moves laterally
        # with signal stalled, before auth is even consulted: command
        # output quoting auth-pattern text is not error evidence.
        stalled = _codex_dispatch_stalled(state_dir, request_id, dispatch_route,
                                          prompt, run_cmd, out, err, cmd, rc)
        if stalled is not None:
            stalled["codex_task_id"] = task_id
            return stalled
        # A missing or rejected login blocks with its reason: no other
        # route holds the same login, so there is no fallback.
        auth_blocked = _codex_auth_block(state_dir, request_id, rc, out, err)
        if auth_blocked is not None:
            auth_blocked["codex_task_id"] = task_id
            return auth_blocked
        # A usage-limit refusal moves to Luna on OpenCode Go with the Codex
        # pool marked exhausted; it never retries Codex.
        exhausted = _dispatch_exhausted(state_dir, request_id, dispatch_route,
                                        prompt, run_cmd, out, err, cmd, rc)
        if exhausted is not None:
            return exhausted
        # A turn counts only with its completion event, as in recovery. A
        # failure with no recognized signal still blocks, but with the last
        # provider message in the reason, never a bare "turn not completed".
        message = _codex_failure_message(out, err, cmd)
        _persist_error_evidence(state_dir, request_id, {"source": "codex_dispatch", "rc": rc, "message": message, "stderr": (err or "")[:1000]})
        _mark_blocked(state_dir, request_id, f"codex_dispatch_failed rc={rc}: {message}")
        return {"action": "blocked", "reason": "codex_dispatch_failed", "codex_task_id": task_id}
    luna_action = codex_h.luna_action("codex_dispatch", out, cmd)
    # Persist the envelope before any side effect it commands.
    _persist_envelope(state_dir, request_id, luna_action, "dispatched")
    if luna_action is None:
        _mark_blocked(state_dir, request_id, "luna_missing_action: no structured envelope")
        return {"action": "blocked", "reason": "luna_missing_action",
                "codex_task_id": task_id}
    return {"action": "dispatched", "codex_task_id": task_id, "luna_action": luna_action}


def _dispatcher_route(job: dict) -> str:
    return _load_controller_state(job).get("dispatch_route") or policy.stage_routes("dispatch")[0]


def _reserve_dispatch_route(state_dir, request_id: str, route: str) -> bool:
    """Reserve a capped dispatch route in the same write transaction that
    records the reservation, counting running dispatch jobs inside the
    transaction. True when reserved, False when the cap is already used."""
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        if core._dispatch_route_full_locked(con, route, exclude=request_id):
            con.execute("ROLLBACK")
            return False
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        try:
            st = json.loads(row["controller_state"] or "{}") if row is not None else {}
        except ValueError:
            st = {}
        if not isinstance(st, dict):
            st = {}
        if st.get("dispatch_route") != route:
            st["dispatch_route"] = route
            con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                        (json.dumps(st, sort_keys=True), core._utcnow(), request_id))
            core._event(con, request_id, "dispatch_route_reserved", {"route": route})
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


def _dispatch_on_opencode(state_dir, request_id: str, route: str, prompt: str, run_cmd,
                          reason: str, session_id: str | None = None,
                          phase: str = "dispatched") -> dict:
    """Run one dispatcher turn on an OpenCode-hosted Luna in plan mode.

    The saved OpenCode session is the dispatcher task; a turn that reports
    another session, or no envelope, blocks like its Codex counterpart.
    A capped dispatch route (luna-go/max) is reserved atomically before any
    child starts; a full cap blocks without starting a dispatcher."""
    if session_id is None and policy.route_max_concurrent(route) is not None:
        if not _reserve_dispatch_route(state_dir, request_id, route):
            _mark_blocked(state_dir, request_id,
                          f"capacity_exhausted: dispatch route {route} at max_concurrent")
            return {"action": "blocked", "reason": "capacity_exhausted"}
    job = core.get_job(state_dir, request_id)
    oc_h = harnesses.harness_named("opencode")
    model, variant, agent = policy.opencode_route_params(route)
    cmd = adapters.build_opencode_serve_cmd()
    oc_meta = {"prompt": prompt, "allowance": policy.route_allowance(route),
               "model": model, "variant": variant, "agent": agent,
               "session_id": session_id, "seq": _load_controller_state(job).get("seq", 0),
               "stage": "dispatch", "route": route, "reason": reason}
    rc, out, err = run_cmd(cmd, job["workspace"], None, kind="opencode_control",
                           meta=oc_meta)
    full = oc_h.parse_report("opencode_control", out, cmd) or {}
    got_session, _skind = oc_h.parse_session("opencode_control", out, err, job)
    got_session = got_session or full.get("opencode_session_id")
    _record_child(state_dir, request_id, "opencode_control", cmd, rc, session_id=got_session,
                  output_text=json.dumps({k: full.get(k) for k in ("ok", "rc", "finish", "actual_model")},
                                         sort_keys=True))
    runtime_blocked = _block_runtime_missing(state_dir, request_id, "opencode_control",
                                             cmd, oc_meta)
    if runtime_blocked is not None:
        return runtime_blocked
    if not got_session:
        _persist_error_evidence(state_dir, request_id, {"source": route, "rc": rc, "error": full.get("error")})
        _mark_blocked(state_dir, request_id, f"dispatch_failed rc={rc}: no dispatcher session on {route}")
        return {"action": "blocked", "reason": "codex_dispatch_failed"}
    if session_id and not oc_h.identity_ok("opencode_control", got_session, session_id):
        _mark_blocked(state_dir, request_id,
                      f"luna_task_mismatch: {route} reported {str(got_session)[:24]}")
        return {"action": "blocked", "reason": "luna_task_mismatch"}
    if not session_id:
        _save_codex_task(state_dir, request_id, got_session, model=model, effort=variant or "default",
                         adapter="opencode", dispatch_route=route)
    if rc != 0 or not full.get("ok"):
        _persist_error_evidence(state_dir, request_id, {"source": route, "rc": rc, "error": full.get("error")})
        _mark_blocked(state_dir, request_id, f"codex_dispatch_failed rc={rc}: dispatcher turn on {route} failed")
        return {"action": "blocked", "reason": "codex_dispatch_failed"}
    luna_action = oc_h.luna_action("opencode_control", out, cmd)
    _persist_envelope(state_dir, request_id, luna_action, phase,
                      raw_text=full.get("assistant_text") if isinstance(full, dict) else None)
    if luna_action is None:
        quote = _assistant_text_quote(full)
        _mark_blocked(state_dir, request_id, f"luna_missing_action: {quote}")
        return {"action": "blocked", "reason": "luna_missing_action", "codex_task_id": got_session}
    return {"action": "dispatched" if phase == "dispatched" else "resumed",
            "codex_task_id": got_session, "luna_action": luna_action, "route": route}


def _own_invocation_pids(state_dir, request_id: str) -> set:
    pids = set()
    for inv in core._list_invocations(state_dir, request_id):
        if inv.get("state") in core.LIVE_INVOCATION_STATES:
            for k in ("pid", "supervisor_pid"):
                if inv.get(k):
                    pids.add(int(inv[k]))
    return pids


PLANNER_CALLBACK_KINDS = (
    harnesses.KIND_CLAUDE_CALLBACK, harnesses.KIND_CODEX_CALLBACK,
    harnesses.KIND_OPENCODE_CALLBACK, harnesses.KIND_GROK_CALLBACK,
)


def _planner_question_text(request_id: str, qid: str, prompt: str,
                           job: dict) -> str:
    """The callback prompt: stored handoff summary before the question."""
    question = ("The runner dispatcher for your submitted request "
                f"{request_id} asks (question {qid}):\n{prompt}\n\n"
                "Answer briefly with the decision only. Do not run tools.")
    summary = (job.get("handoff_summary") or "").strip()
    if summary:
        # The durable handoff summary travels before the question so the
        # resumed session answers from the ledger,
        # and a fresh fallback session can answer from this prompt alone.
        question = (f"HANDOFF SUMMARY for job {request_id}:\n{summary}\n\n" + question)
    return question


def _planner_meta(qid: str, question: str, route: str | None) -> dict:
    meta: dict = {"qid": qid, "stage": "planning",
                 "reason": "planner_question",
                 "prompt_sha256": hashlib.sha256(question.encode("utf-8")).hexdigest()}
    if route is not None:
        meta["route"] = route
    return meta


def _finish_planner_callback(state_dir, request_id: str, qid: str, kind: str,
                             cmd, rc: int, err: str,
                             answer: str | None, ok: bool, error: str | None,
                             reported_session: str | None,
                             planner_session: str | None,
                             check_identity: bool, meta: dict | None = None) -> dict:
    """Shared tail: stored-answer wins, then fail/mismatch blocks, else
    persist the answer. ``check_identity`` is false only for a fresh
    fallback session, which is never a resume. A callback that provably
    never started for a missing runtime blocks recoverably with its
    cause and next action instead of an opaque callback failure; the
    question stays pending for ``answer`` plus ``recover``."""
    _record_child(state_dir, request_id, kind, cmd, rc,
                  session_id=reported_session or planner_session,
                  output_text=(answer or "") + "\n" + (err or "")[-1000:])
    stored = [q for q in core.list_questions(state_dir, request_id, only_pending=False)
              if q["qid"] == qid and q["status"] == "answered"]
    mismatch = check_identity and reported_session != planner_session
    if stored and (rc != 0 or not ok or mismatch):
        # Answered publicly while the callback ran: the stored answer wins.
        return {"action": "answered", "qid": qid}
    runtime_blocked = _block_runtime_missing(state_dir, request_id, kind, cmd, meta)
    if runtime_blocked is not None:
        runtime_blocked["qid"] = qid
        return runtime_blocked
    if rc != 0 or not ok:
        reason = f"planner_callback_failed rc={rc}: {error or 'error'}"
        _persist_error_evidence(state_dir, request_id,
                                {"source": kind, "rc": rc,
                                 "error": error,
                                 "stderr": (err or "")[:1000]})
        _mark_blocked(state_dir, request_id, reason)
        return {"action": "blocked", "reason": "planner_callback_failed"}
    if mismatch:
        _persist_error_evidence(state_dir, request_id,
                                {"source": kind,
                                 "expected_session": planner_session,
                                 "reported_session": reported_session})
        _mark_blocked(state_dir, request_id,
                      "planner_session_mismatch: resumed result came from another session")
        return {"action": "blocked", "reason": "planner_session_mismatch"}
    # Persist the answer before resuming the same Luna task.
    try:
        core.answer(state_dir, request_id, qid, answer or "", lease_token=_LEASE["token"])
    except core.ConflictError:
        pass  # answered publicly meanwhile: the stored answer wins
    return {"action": "answered", "qid": qid}


def planner_callback(state_dir, request_id: str, qid: str, prompt: str,
                     run_cmd=None) -> dict:
    """Persist question before waking the planner; persist answer before resume.

    The planner wakes automatically in its own harness without human
    action: Claude, Codex, and OpenCode resume the exact saved session,
    while a harness without a usable resume path (such as Grok Build)
    answers from a fresh session seeded with the stored handoff summary.
    Never forks a new planner session except for that fresh fallback. A
    busy planner, a failed callback, or a resumed result from another
    session becomes a durable blocked state with a reason; the question
    stays pending for ``answer`` + recover.
    """
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    planner_session = job.get("planner_session_id")
    if not planner_session:
        _mark_blocked(state_dir, request_id, "missing planner session: refusing to fork a new session")
        return {"action": "blocked", "reason": "missing_planner_session"}
    harness_name = job.get("planner_harness") or "claude"
    if harness_name not in ("claude", "codex", "opencode", "grok"):
        _mark_blocked(state_dir, request_id,
                      f"planner_harness_unsupported: {harness_name!r} has no callback")
        return {"action": "blocked", "reason": "planner_harness_unsupported"}
    # Persist the planner question before waking the planner.
    try:
        core.post_question(state_dir, request_id, qid, prompt, lease_token=_LEASE["token"])
    except core.ConflictError as e:
        _mark_blocked(state_dir, request_id, f"planner_question_conflict: {e}")
        return {"action": "blocked", "reason": "question_conflict"}
    # An earlier controller may already have asked; that attempt is
    # adopted through its action key, so the busy checks do not apply.
    asked = any(i.get("kind") in PLANNER_CALLBACK_KINDS
                and json.loads(i.get("meta_json") or "{}").get("qid") == qid
                for i in core._list_invocations(state_dir, request_id)
                if i.get("state") != "abandoned")
    question = _planner_question_text(request_id, qid, prompt, job)
    workspace = job["workspace"]
    if harness_name == "codex":
        try:
            cmd = adapters.build_codex_planner_cmd(planner_session, question)
        except ValueError as e:
            _mark_blocked(state_dir, request_id, f"planner_resume_refused: {e}")
            return {"action": "blocked", "reason": "planner_resume_refused"}
        rc, out, err = run_cmd(cmd, workspace, None, kind=harnesses.KIND_CODEX_CALLBACK,
                               meta=_planner_meta(qid, question, "astra/max"))
        codex_h = harnesses.harness_named("codex")
        parsed = codex_h.parsed_planner_result(harnesses.KIND_CODEX_CALLBACK, out)
        return _finish_planner_callback(
            state_dir, request_id, qid, harnesses.KIND_CODEX_CALLBACK,
            cmd, rc, err, parsed.get("answer"), bool(parsed.get("ok")),
            parsed.get("error"), parsed.get("session_id"), planner_session,
            check_identity=True, meta=_planner_meta(qid, question, "astra/max"))
    if harness_name == "opencode":
        try:
            cmd = adapters.build_opencode_planner_cmd(planner_session, question, workspace)
        except ValueError as e:
            _mark_blocked(state_dir, request_id, f"planner_resume_refused: {e}")
            return {"action": "blocked", "reason": "planner_resume_refused"}
        rc, out, err = run_cmd(cmd, workspace, None, kind=harnesses.KIND_OPENCODE_CALLBACK,
                               meta=_planner_meta(qid, question, None))
        opencode_h = harnesses.harness_named("opencode")
        parsed = opencode_h.parsed_planner_result(harnesses.KIND_OPENCODE_CALLBACK, out)
        return _finish_planner_callback(
            state_dir, request_id, qid, harnesses.KIND_OPENCODE_CALLBACK,
            cmd, rc, err, parsed.get("answer"), bool(parsed.get("ok")),
            parsed.get("error"), parsed.get("session_id"), planner_session,
            check_identity=True, meta=_planner_meta(qid, question, None))
    if harness_name == "grok":
        # No usable resume path: a fresh Grok session answers from the
        # handoff summary carried in the question above. Its new session
        # ID is recorded as the answer's source, never as a resume.
        cmd, _env = adapters.build_grok_cmd(question, workspace)
        rc, out, err = run_cmd(cmd, workspace, None, kind=harnesses.KIND_GROK_CALLBACK,
                               meta=_planner_meta(qid, question, None))
        grok_h = harnesses.harness_named("grok")
        parsed = grok_h.parsed_planner_result(harnesses.KIND_GROK_CALLBACK, out)
        return _finish_planner_callback(
            state_dir, request_id, qid, harnesses.KIND_GROK_CALLBACK,
            cmd, rc, err, parsed.get("text"), bool(parsed.get("ok")),
            parsed.get("error"), parsed.get("session_id"), planner_session,
            check_identity=False, meta=_planner_meta(qid, question, None))
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
    try:
        cmd = adapters.build_claude_cmd(planner_session, question,
                                         model=planner_model, effort=planner_effort)
    except ValueError as e:
        _mark_blocked(state_dir, request_id, f"planner_resume_refused: {e}")
        return {"action": "blocked", "reason": "planner_resume_refused"}
    planner_route = "sonnet/medium" if planner_model == adapters.CLAUDE_LIVE_MODEL else "fable-5.1/max"
    rc, out, err = run_cmd(cmd, workspace, None, kind="claude_callback",
                           meta=_planner_meta(qid, question, planner_route))
    claude_h = harnesses.harness_named("claude")
    parsed = claude_h.parsed_result("claude_callback", out)
    return _finish_planner_callback(
        state_dir, request_id, qid, "claude_callback",
        cmd, rc, err, parsed.get("answer"), bool(parsed.get("ok")),
        parsed.get("error"), parsed.get("session_id"), planner_session,
        check_identity=True, meta=_planner_meta(qid, question, planner_route))


def astra_fallback_prompt(state_dir, request_id: str, qid: str, prompt: str) -> str:
    """Prompt for the Astra fallback planner in a fresh session, no resume.

    The Codex-harness Astra fallback never resumes the
    Claude planner session: it answers from the job's durable handoff
    summary carried before the dispatcher's question. Run it as a fresh
    Astra turn and persist the answer with ``answer`` plus ``recover``.
    """
    job = core.get_job(state_dir, request_id)
    summary = (job.get("handoff_summary") or "").strip() or "-"
    return (f"HANDOFF SUMMARY for job {request_id}:\n{summary}\n\n"
            f"The runner dispatcher asks (question {qid}):\n{prompt}\n\n"
            "Answer briefly with the decision only. Do not run tools.")


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
    active = _dispatcher_route(job)
    if harnesses.route_uses_owned_server(active):
        return _dispatch_on_opencode(state_dir, request_id, active, message, run_cmd,
                                     reason="resume", session_id=task_id, phase="resumed")
    dispatch = policy.ROUTES[active]
    # Native resume (toolboxmd/model-router#86): metadata-only attach to
    # the saved thread on the job-owned server, then a new control turn.
    # The saved thread id travels in the job row, never in argv.
    cmd = codex_native.python_driver_cmd()
    try:
        rc, out, err = run_cmd(cmd, job["workspace"], None, kind="codex_resume",
                               meta=native_turn_meta(codex_native.OP_RESUME, message,
                                                     dispatch["model"], dispatch["variant"],
                                                     active, "resume"))
    except Exception as e:
        return _block_codex_observation_failure(
            state_dir, request_id, "codex_resume", e)
    codex_h = harnesses.harness_named("codex")
    resumed_id, _skind = codex_h.parse_session("codex_resume", out, err, job)
    _record_child(state_dir, request_id, "codex_resume", cmd, rc,
                  session_id=resumed_id or task_id, output_text=(out or "") + (err or ""))
    _record_native_control_turn(state_dir, request_id, out)
    # Observed-model gate before any failure classification, envelope, or
    # route movement, like dispatch. A mismatched failed resume beats the
    # generic failure handling. Only genuinely missing or unreadable rollout
    # cases stay unknown inside the rollout reader; any other observation
    # failure blocks safely with evidence.
    try:
        _resume_route = active if active in policy.ROUTES else policy.stage_routes("dispatch")[0]
        _obs, _mm_reason = check_codex_observed_model(
            state_dir, request_id, "codex_resume", _resume_route)
    except Exception as e:
        return _block_codex_observation_failure(
            state_dir, request_id, "codex_resume", e)
    if _mm_reason is not None:
        _mark_blocked(state_dir, request_id, _mm_reason)
        return {"action": "blocked", "reason": _mm_reason}
    # A resume that provably never started for a missing runtime blocks
    # recoverably instead of the sticky turn-not-completed failure. The
    # check recomputes the exact action identity, so it carries the same
    # native turn meta as the run above.
    runtime_blocked = _block_runtime_missing(
        state_dir, request_id, "codex_resume", cmd,
        native_turn_meta(codex_native.OP_RESUME, message,
                         dispatch["model"], dispatch["variant"],
                         active, "resume"))
    if runtime_blocked is not None:
        return runtime_blocked
    if not codex_h.turn_ok("codex_resume", rc, out, cmd):
        # A stall the supervisor ended for stream silence stays stalled,
        # never auth: command output quoting auth-pattern text is not
        # error evidence.
        stalled = _codex_resume_stalled(state_dir, request_id, rc, out, err)
        if stalled is not None:
            return stalled
        # A missing or rejected login blocks with its reason like dispatch:
        # no other route holds the same login.
        auth_blocked = _codex_auth_block(state_dir, request_id, rc, out, err)
        if auth_blocked is not None:
            return auth_blocked
        # A resume that failed by exit code is a failed turn with its error
        # kept, whatever thread it named or did not reach. Exit code alone
        # is not a completed turn: the harness's own turn.completed record
        # decides, before any thread comparison.
        _persist_error_evidence(state_dir, request_id,
                                {"source": "codex_resume", "rc": rc,
                                 "stderr": (err or "")[:1000]})
        _mark_blocked(state_dir, request_id, f"codex_resume_failed rc={rc}: turn not completed")
        return {"action": "blocked", "reason": "codex_resume_failed"}
    if not codex_h.identity_ok("codex_resume", resumed_id, task_id):
        # A turn on another thread, completed or not, never replaces the saved task.
        _mark_blocked(state_dir, request_id,
                      f"luna_task_mismatch: resume reported {str(resumed_id or 'no thread')[:24]}")
        return {"action": "blocked", "reason": "luna_task_mismatch"}
    luna_action = codex_h.luna_action("codex_resume", out, cmd)
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
    "workspace. Run the task's proof command when it has one. Commit as "
    "you work; at the end push the branch and open exactly one PR without "
    "merging it, and report its URL. Do not start other agents or install "
    "anything. Finish with a short report of changed files, the PR URL, "
    "and proof results."
)


def _implementation_prompt(task_json: str, artifact, payload,
                           known_pr_url: str | None = None) -> str:
    prompt = f"TASK (complete):\n{_task_summary(task_json)}\n"
    if isinstance(payload, dict) and isinstance(payload.get("instructions"), str):
        prompt += f"\nDISPATCHER INSTRUCTIONS:\n{payload['instructions']}\n"
    if isinstance(known_pr_url, str) and known_pr_url.strip():
        prompt += (f"\nExisting PR for this job: {known_pr_url.strip()}\n"
                   "Update that PR; do not open another.\n")
    if artifact:
        prompt += f"\nartifact: {artifact}\n"
    if payload:
        try:
            prompt += "\ncontext: " + json.dumps(payload, sort_keys=True) + "\n"
        except Exception:
            prompt += f"\ncontext: {payload!r}\n"
    return prompt + "\n" + WORKER_RULES


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
                    {"session": str(fields.get("opencode_session_id")
                                     or fields.get("grok_session_id") or "")[:24],
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
    (state, route, evidence, reset_at, reset_source). The target's concurrency cap is
    reserved in the same write transaction that records the route: running
    jobs on the target are counted inside the transaction, so two
    controllers cannot both take a capped route."""
    evidence = evidence or {}
    turns = _turns_by_route(state_dir, request_id)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        row = con.execute("SELECT route, lane FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        prev = row["route"] if row is not None else None
        lane = row["lane"] if row is not None else None
        # Atomic reservation: a capped target already used by other running
        # jobs moves to the next eligible route in the same transaction.
        # Eligibility matches normal selection (lane membership, capacity
        # state, one-turn use, and cap); recovery reasons in the recovery
        # stage, never raises.
        if core._route_full_locked(con, target, exclude=request_id):
            nxt = core._next_capable_locked(con, target, lane, exclude=request_id,
                                            turns_by_route=turns)
            if nxt is None:
                # No free route: record the capacity mark, then block.
                if mark is not None:
                    state, marked_route, mark_evidence, reset_at, reset_source = mark
                    core._record_capacity_locked(con, marked_route, state,
                                                 mark_evidence, reset_at,
                                                 reset_source=reset_source)
                con.execute("COMMIT")
                _mark_blocked(state_dir, request_id,
                              f"capacity_exhausted: no eligible route for {target}")
                return {"action": "blocked", "reason": "capacity_exhausted"}
            target = nxt
            reason = reason + "_concurrent" if "concurrent" not in reason else reason
        model, variant = policy.worker_model_variant(target)
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
                      "scope": "worker",
                      "evidence": str(evidence.get("source") or evidence.get("class")
                                      or evidence.get("name") or "provider")[:64]})
        if mark is not None:
            state, marked_route, mark_evidence, reset_at, reset_source = mark
            core._record_capacity_locked(con, marked_route, state,
                                         mark_evidence, reset_at,
                                         reset_source=reset_source)
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


def _pool_target_in_lane(pool_target: str | None, route: str, lane: str | None) -> bool:
    """True when the same-model next-pool target stays in the job's lane."""
    if not pool_target:
        return False
    try:
        from_stage = policy.lane_of_route(route, lane)
        to_stage = policy.lane_of_route(pool_target, lane)
    except ValueError:
        return False
    if to_stage is None or from_stage is None:
        return False
    # Recovery pool moves stay in recovery; implementation pool moves stay
    # in the stored lane.
    return to_stage == from_stage


def _move_after_signal(state_dir, request_id: str, route: str, signal: str,
                       evidence: dict) -> dict:
    """Exhaustion: zero retries, the same model on the next pool, else the
    next family. Overload and stalled: the next family; the route rests for
    the documented cooldown. Moves stay inside the job's stored lane and skip
    one-turn routes already used. No eligible route blocks with a reason."""
    job = core.get_job(state_dir, request_id)
    lane = job.get("lane")
    turns = _turns_by_route(state_dir, request_id)
    exhausted = core.exhausted_routes(state_dir)
    degraded = core.degraded_routes(state_dir)
    if signal == "exhausted":
        exhausted.add(route)
        pool_target = policy.next_pool_route(route)
        if pool_target and (pool_target in exhausted or pool_target in degraded
                            or policy.one_turn_routes_used(pool_target, turns)
                            or not _pool_target_in_lane(pool_target, route, lane)):
            pool_target = None
        target = pool_target or policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "pool_move" if target and target == pool_target else "lateral"
        reset_at, source = core.reset_at_for_evidence(route, evidence, "exhausted")
        mark = ("exhausted", route, evidence, reset_at, source)
    else:
        degraded.add(route)
        target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "lateral"
        reset_at, source = core.reset_at_for_evidence(route, evidence, "degraded")
        mark = ("degraded", route, evidence, reset_at, source)
    if target is None:
        con = store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            _lease_guard(con, request_id)
            core._record_capacity_locked(con, mark[1], mark[0], mark[2], mark[3],
                                         reset_source=mark[4])
            con.execute("COMMIT")
        finally:
            con.close()
        _mark_blocked(state_dir, request_id,
                      f"capacity_exhausted: no eligible route after {signal} on {route}")
        return {"action": "blocked", "reason": "capacity_exhausted"}
    return _switch_route(state_dir, request_id, target, reason, evidence, mark=mark)


def _move_after_context(state_dir, request_id: str, route: str, evidence: dict) -> dict | None:
    """A context_length_exceeded turn moves to the next route in the job's
    lane with a strictly larger context window, skipping exhausted,
    degraded, already-used one-turn, and concurrency-full routes. No
    capacity mark: context pressure is not availability. None when no
    larger-context route is left: the caller then ends the turn as
    implementation_failed."""
    job = core.get_job(state_dir, request_id)
    lane = job.get("lane")
    turns = _turns_by_route(state_dir, request_id)
    exhausted = core.exhausted_routes(state_dir)
    degraded = core.degraded_routes(state_dir)
    try:
        stage = policy.lane_of_route(route, lane)
    except ValueError:
        return None
    target = None
    if stage is not None:
        size = policy.route_context_window(route)
        order = policy.stage_routes(stage)
        for cand in order[order.index(route) + 1:]:
            if cand in exhausted or cand in degraded:
                continue
            if policy.one_turn_routes_used(cand, turns):
                continue
            if policy.route_context_window(cand) <= size:
                continue
            if core.route_concurrency_full(state_dir, cand, exclude=request_id):
                continue
            target = cand
            break
    if target is None:
        return None
    return _switch_route(state_dir, request_id, target, "larger_context", evidence)


def _stalled_retry_state(state_dir, request_id: str, route: str) -> dict:
    st = _load_controller_state(core.get_job(state_dir, request_id)).get("stalled")
    if isinstance(st, dict) and st.get("route") == route:
        try:
            return {"route": route, "count": int(st.get("count") or 0),
                    "first": float(st.get("first") or 0)}
        except (TypeError, ValueError):
            pass
    return {"route": route, "count": 0, "first": 0.0}


def _stall_reset_evidence(evidence: dict) -> dict:
    """Stall evidence with the probe's structured answer hoisted.

    A stall can be exhaustion in disguise: the probe's provider detail
    (``resets_at``/``Retry-After``/reset text, status, message error) is
    copied alongside the stall fields so :func:`core.reset_at_for_evidence`
    reads the provider reset first and the assumed window only when none
    is present. The nested ``probe`` is kept for the ledger.
    """
    if not isinstance(evidence, dict):
        return evidence
    probe = evidence.get("probe")
    detail = None
    if isinstance(probe, dict):
        detail = probe.get("evidence") if isinstance(probe.get("evidence"), dict) else None
    if not isinstance(detail, dict):
        return evidence
    merged = dict(evidence)
    for key, val in detail.items():
        if key not in merged:
            merged[key] = val
    for key in ("status", "message_error_detail", "transport_evidence"):
        val = detail.get(key)
        if isinstance(val, dict) and "error" not in merged:
            merged["error"] = val
            break
    return merged


def _handle_stalled(state_dir, request_id: str, job: dict, seq: int, route: str,
                    full: dict, session_id: str | None, evidence: dict) -> dict:
    """Route a silent turn from its probe answer. A probe that found
    exhaustion moves pools and one that found overload moves family; an
    unknown probe retries the same route bounded by the overload window,
    then moves laterally with the route degraded. No hot loop (the retry
    count and window bound it, the step budgets bound the walk) and no
    duplicate writer (one turn at a time, same session reused)."""
    probe = evidence.get("probe") if isinstance(evidence.get("probe"), dict) else {}
    probe_signal = full.get("probe_signal") or probe.get("signal")
    stall_report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id,
                                      status="stalled", error=full.get("error"),
                                      run_proof=False,
                                      proof_skipped_reason="stalled turn: the suite is not run "
                                      "automatically for a silent turn; the dispatcher requests "
                                      "useful proof of a coherent candidate")
    _set_phase(state_dir, request_id, phase="stalled_moved",
               opencode_session_id=session_id, report_path=stall_report.get("report_path"))
    if not full.get("idle_confirmed"):
        _mark_blocked(state_dir, request_id,
                      "route_transfer_abort_failed: session not confirmed idle after stalled")
        return {"action": "blocked", "reason": "route_transfer_abort_failed",
                "report": stall_report}
    if probe_signal == "exhausted":
        moved = _move_after_signal(state_dir, request_id, route, "exhausted",
                                   _stall_reset_evidence(evidence))
        moved["report"] = stall_report
        return moved
    if probe_signal == "overloaded":
        moved = _move_after_signal(state_dir, request_id, route, "overloaded",
                                   _stall_reset_evidence(evidence))
        moved["report"] = stall_report
        return moved
    spec = policy.SIGNAL_CLASSES["stalled"]
    now = time.time()
    seen = _stalled_retry_state(state_dir, request_id, route)
    first = seen["first"]
    count = seen["count"]
    if not first or now - first > float(spec["window_secs"]):
        first, count = now, 0
    if count < int(spec["retries"]):
        _set_phase(state_dir, request_id,
                   stalled={"route": route, "count": count + 1, "first": first})
        return {"action": "stalled_retry", "route": route, "reason": "stalled_retry",
                "report": stall_report}
    moved = _move_after_signal(state_dir, request_id, route, "stalled", evidence)
    moved["report"] = stall_report
    return moved


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
        if (not target or target in exhausted or target in degraded
                or policy.one_turn_routes_used(target, turns)
                or not _pool_target_in_lane(target, route, lane)):
            target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "preflight_exhausted"
    elif route in degraded:
        target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "preflight_degraded"
    elif policy.one_turn_routes_used(route, turns):
        # A $15 model already had its one turn in this job: move on before dispatch.
        target = policy.next_family_route(route, exhausted, degraded, lane, turns)
        reason = "preflight_one_turn"
    elif core.route_concurrency_full(state_dir, route, exclude=request_id):
        # The route's max_concurrent is already used by running jobs: move
        # to the next eligible route in the lane (capacity state, one-turn
        # use, lane membership, and cap).
        target = core.next_capable_route(state_dir, route, lane, exclude=request_id,
                                         turns_by_route=turns)
        reason = "preflight_concurrent"
    else:
        return None
    if target is None:
        _mark_blocked(state_dir, request_id,
                      f"capacity_exhausted: no eligible route before dispatch on {route}")
        return {"action": "blocked", "reason": "capacity_exhausted"}
    return _switch_route(state_dir, request_id, target, reason,
                         {"source": "capacity_memory", "route": route})




def _record_planner_route_rejection(state_dir, request_id: str,
                                      requested: str, reason: str) -> None:
    """Persist why a dispatcher-requested route was not assigned.

    The job keeps its route; the rejection travels in the ledger so the
    dispatcher can direct an eligible route instead. Never raises past
    the caller: a lost lease still yields to the owner.
    """
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        core._event(con, request_id, "planner_route_rejected",
                    {"requested": requested, "reason": reason})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _apply_planner_directed_route(state_dir, request_id: str,
                                   envelope: dict) -> None:
    """Assign an explicit planner direction under the routing policy.

    A planner decision may direct a different approach or a stronger
    eligible agent through the dispatcher: when the implementation
    envelope carries ``directed_route`` naming a dispatcher-assignable
    route, the job moves there with reason ``planner_directed``. The
    ordinary envelope ``route`` field (and any legacy concrete route in
    an older example) is inert: the escalation ladder stays authoritative
    and a repeated default never undoes a correction or recovery move.
    Anything else (an unknown route, a planner-harness rung that runs in
    the planner session, or a route with no capacity left) is rejected
    with a ``planner_route_rejected`` event and the job keeps its route:
    explicit choices are never silently substituted and models never
    silently swapped. Correction and recovery stage routes are valid
    from any implementation lane; other routes must sit in the job lane.
    Returns True when the direction was assigned, False otherwise.
    """
    if not isinstance(envelope, dict):
        return False
    requested = envelope.get("directed_route")
    if not isinstance(requested, str) or not requested.strip():
        return False
    requested = requested.strip()
    job = core.get_job(state_dir, request_id)
    if requested == job.get("route"):
        return False
    if not policy.is_dispatcher_assignable(requested):
        _record_planner_route_rejection(
            state_dir, request_id, requested,
            f"route {requested!r} is not dispatcher-assignable: "
            "planner-harness rungs run in the planner session, and unknown "
            "routes never substitute a model")
        return False
    lane = job.get("lane")
    try:
        stage = policy.lane_of_route(requested, lane)
    except ValueError:
        stage = None
    if stage is None and requested not in policy.STAGES.get(
            "correction", {}).get("routes", ()) \
            and requested not in policy.STAGES.get(
                "recovery", {}).get("routes", ()):
        _record_planner_route_rejection(
            state_dir, request_id, requested,
            f"route {requested!r} is not in lane {lane!r}; keeping the job route")
        return False
    try:
        exhausted = core.exhausted_routes(state_dir)
        degraded = core.degraded_routes(state_dir)
    except Exception:
        exhausted, degraded = set(), set()
    if requested in exhausted or requested in degraded:
        _record_planner_route_rejection(
            state_dir, request_id, requested,
            f"route {requested!r} has no capacity left; keeping the job route")
        return False
    turns = _turns_by_route(state_dir, request_id)
    if policy.one_turn_routes_used(requested, turns):
        _record_planner_route_rejection(
            state_dir, request_id, requested,
            f"route {requested!r} already ran its one turn in this job; "
            "keeping the job route")
        return False
    if core.route_concurrency_full(state_dir, requested, exclude=request_id):
        _record_planner_route_rejection(
            state_dir, request_id, requested,
            f"route {requested!r} is at max_concurrent; keeping the job route")
        return False
    _switch_route(state_dir, request_id, requested, "planner_directed",
                  {"source": "dispatcher", "requested_route": requested})
    return True


def _turns_by_route(state_dir, request_id: str) -> dict:
    """Implementation turns already run per route, for the one-turn rule."""
    counts: dict = {}
    worker_kinds = harnesses.worker_control_kinds()
    for inv in core._list_invocations(state_dir, request_id):
        if inv.get("kind") not in worker_kinds or inv.get("state") == "abandoned":
            continue
        try:
            route = (json.loads(inv.get("meta_json") or "{}") or {}).get("route")
        except ValueError:
            route = None
        if route:
            counts[route] = counts.get(route, 0) + 1
    return counts


def run_implementation(state_dir, request_id: str, artifact: str | None = None,
                       payload: dict | None = None, run_cmd=None) -> dict:
    """Run one implementation turn for the current dispatcher action.

    The turn runs on the job route's harness through the harness seam:
    an owned ``opencode serve`` driven by the supervisor, or a headless
    ``grok -p`` worker process. Capacity signals move routes; other
    errors end the turn for the escalation ladder.
    """
    run_cmd = run_cmd or default_run_cmd
    job = core.get_job(state_dir, request_id)
    workspace = job["workspace"]
    route = job.get("route") or "muse-spark-xhigh-free"
    harness = harnesses.harness_named(policy.route_spec(route)["harness"])
    state = _load_controller_state(job)
    seq = state.get("seq", 0)
    route_reason = state.get("route_reason") or "initial"
    try:
        known_pr = core.known_pr_url(state_dir, request_id)
    except Exception:
        known_pr = None
    prompt = _implementation_prompt(job["task_json"], artifact, payload,
                                    known_pr_url=known_pr)
    def _turn_seq(inv):
        try:
            return (json.loads(inv.get("meta_json") or "{}") or {}).get("seq")
        except ValueError:
            return None
    prior_tries = [i for i in core._list_invocations(state_dir, request_id)
                   if i.get("kind") in harnesses.worker_control_kinds() and i.get("state") != "abandoned"
                   and _turn_seq(i) == seq]
    attempted = bool(prior_tries)
    if not attempted:
        move = _preflight_move(state_dir, request_id, route)
        if move is not None:
            return move
    # Bind a pending recovery decision to this actual attempt start, after
    # any preflight route move: a preflight move starts no worker
    # invocation, so it must not consume the pending link or emit a
    # recovery_next_attempt for a seq that never ran. The link carries the
    # post-move route, while the decision keeps its pre-move target.
    try:
        _post_move_job = core.get_job(state_dir, request_id)
        _post_move_route = _post_move_job.get("route") or route
    except Exception:
        _post_move_route = route
    _link_recovery_attempt(state_dir, request_id, seq, _post_move_route)
    # ``try`` numbers repeated worker turns for one dispatcher turn, so a
    # bounded same-route retry after a stall spawns a new attempt with its
    # own action identity instead of reusing the stalled turn's record.
    # Only stalled failures count: a live turn is adopted, a completed turn
    # is reused, and any other failure is returned as-is (never replayed),
    # all under the original identity.
    def _turn_stalled(inv):
        if inv.get("state") != "failed":
            return False
        try:
            res = json.loads(inv.get("result_json") or "null")
        except ValueError:
            return False
        if not isinstance(res, dict):
            return False
        if res.get("signal") == "stalled":
            return True
        env = res.get("envelope")
        return isinstance(env, dict) and env.get("signal") == "stalled"
    attempt = sum(1 for i in prior_tries if _turn_stalled(i))
    if harness.owned_server:
        return _run_opencode_turn(state_dir, request_id, job, workspace, route, prompt,
                                  seq, route_reason, run_cmd, artifact, attempt)
    if harness.headless_worker:
        return _run_grok_turn(state_dir, request_id, job, workspace, route, prompt,
                              seq, route_reason, run_cmd, artifact, attempt)
    raise core.RunnerError(f"route {route} runs on {harness.name}, which has no worker turn")


def _confirmed_stop_recoverable(state_dir, request_id: str,
                                  full: dict | None, rc: int | None) -> bool:
    """True when a stopped turn may return evidence to the dispatcher.

    A supervisor result (an OpenCode rc124 startup failure or an rc143
    confirmed stop) is recoverable only when every owned process group
    is proven dead: each recorded worker control invocation (abandoned
    never-started rows excepted) must read ``dead`` or ``finished``,
    never live, orphaned, unresolved, or unknown, and every recorded
    child and supervisor process group must be present and proven dead.
    A finished row alone proves the record
    was written, never that the group died: members that ignore SIGTERM
    can survive the supervisor's leader-only wait, and absent group
    identities prove nothing at all. Missing results, missing or
    unknown group identities, live groups, rc125 supervisor loss, and
    any ambiguous ownership stay sticky-blocked so no second writer
    starts behind a possible owner. Read-only: never kills anything.
    """
    if rc == 125:
        return False
    if not isinstance(full, dict) or not full:
        return False
    try:
        invs = [i for i in core._list_invocations(state_dir, request_id)
                if i.get("kind") in ("opencode_control", "grok_control")]
    except Exception:
        return False
    if not invs:
        return False
    try:
        for inv in invs:
            if inv.get("state") == "abandoned":
                continue
            try:
                own = core._invocation_ownership(inv)
            except Exception:
                return False
            if own not in ("dead", "finished"):
                return False
            for pgid_key in ("pgid", "supervisor_pgid"):
                pgid = inv.get(pgid_key)
                if pgid is None:
                    return False
                if core._is_pgid_alive(pgid):
                    return False
        if core._any_live_invocation(state_dir, request_id):
            return False
        if core._any_unresolved_invocation(state_dir, request_id):
            return False
    except Exception:
        return False
    return True


def _run_opencode_turn(state_dir, request_id, job, workspace, route, prompt,
                       seq, route_reason, run_cmd, artifact=None, attempt=0) -> dict:
    allowance = _route_allowance(route)
    model, variant, agent = policy.opencode_route_params(route)
    saved_session = job.get("opencode_session_id")
    cmd = adapters.build_opencode_serve_cmd()
    oc_meta = {"prompt": prompt, "allowance": allowance,
               "model": model, "variant": variant, "agent": agent,
               "session_id": saved_session, "seq": seq,
               "try": attempt,
               "stage": "implementation", "route": route,
               "reason": route_reason}
    rc, out, err = run_cmd(cmd, workspace, None, kind="opencode_control",
                           meta=oc_meta)
    # The result line comes from this invocation's own output file, read
    # through the harness seam.
    oc_h = harnesses.harness_named("opencode")
    full = oc_h.parse_report("opencode_control", out, cmd) or {}
    session_id = full.get("opencode_session_id") or saved_session
    _record_child(state_dir, request_id, "opencode_control", cmd, rc,
                  session_id=session_id,
                  output_text=json.dumps({k: full.get(k) for k in (
                      "ok", "rc", "quota", "finish", "actual_model")}, sort_keys=True))
    # A turn that provably never started for a missing runtime blocks
    # recoverably with its cause and next action, before capacity-signal
    # handling, the escalation ladder, or a sticky implementation failure
    # could count an unstarted action as an ordinary provider failure or
    # move its explicit route.
    runtime_blocked = _block_runtime_missing(state_dir, request_id, "opencode_control",
                                             cmd, oc_meta)
    if runtime_blocked is not None:
        return runtime_blocked
    if rc == 0 and full.get("ok"):
        report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id)
        _set_phase(state_dir, request_id, phase="implemented",
                   opencode_session_id=session_id, artifact=artifact,
                   report_path=report.get("report_path"),
                   implementation_output=adapters.redact_text(str(full.get("assistant_text") or ""))[-8000:])
        # A successful request revalidates the route's exhaustion marks the
        # same way a healthy probe does (provider marks only past reset).
        core.record_route_success(state_dir, route)
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
                             "idle_confirmed": full.get("idle_confirmed"),
                             "retry_next": full.get("last_retry_next"),
                             "retry_next_capped": full.get("last_retry_next_capped"),
                             "overload_retries": full.get("overload_retries"),
                             "probe_signal": full.get("probe_signal"),
                             "longest_silence_secs": full.get("longest_silence_secs")})
    if signal == "context" and isinstance(evidence, dict):
        # A capacity signal, not a hard failure: move to a larger-context
        # route, and only end the turn failed when none is left.
        moved = _move_after_context(state_dir, request_id, route, evidence)
        if moved is not None:
            ctx_report = _write_turn_report(state_dir, request_id, job, seq, route, full,
                                            session_id, status="context",
                                            error=full.get("error"),
                                            run_proof=False,
                                            proof_skipped_reason="context move: the suite is not run "
                                            "automatically for an incomplete turn; the dispatcher "
                                            "requests useful proof of a coherent candidate")
            _set_phase(state_dir, request_id, phase="context_moved",
                       opencode_session_id=session_id,
                       report_path=ctx_report.get("report_path"))
            moved["report"] = ctx_report
            return moved
    if signal == "stalled" and isinstance(evidence, dict):
        return _handle_stalled(state_dir, request_id, job, seq, route, full,
                               session_id, evidence)
    if signal in ("exhausted", "overloaded") and isinstance(evidence, dict):
        # Every worker turn leaves a report, including capacity moves, so
        # the dispatcher and the ledger keep the evidence. The suite is
        # not run automatically for a capacity turn: the candidate is
        # unfinished by definition, and the dispatcher requests useful
        # proof of a coherent candidate instead.
        move_report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id,
                                         status=signal, error=full.get("error"),
                                         run_proof=False,
                                         proof_skipped_reason=f"{signal} turn: the suite is not run "
                                         "automatically for a capacity turn; the dispatcher requests "
                                         "useful proof of a coherent candidate")
        _set_phase(state_dir, request_id, phase=f"{signal}_moved",
                   opencode_session_id=session_id, report_path=move_report.get("report_path"))
        if not full.get("idle_confirmed"):
            if route == "muse-spark-xhigh-free" and signal == "exhausted":
                _mark_blocked(state_dir, request_id,
                              "go_transfer_abort_failed: free session not confirmed idle")
                return {"action": "blocked", "reason": "go_transfer_abort_failed",
                        "report": move_report}
            _mark_blocked(state_dir, request_id,
                          f"route_transfer_abort_failed: session not confirmed idle after {signal}")
            return {"action": "blocked", "reason": "route_transfer_abort_failed",
                    "report": move_report}
        moved = _move_after_signal(state_dir, request_id, route, signal, evidence)
        moved["report"] = move_report
        return moved
    if not full or rc in (124, 143, 125) or full.get("rc") in (124, 143, 125):
        # No control result means the supervisor never finished its loop
        # (a crash, a stop, or supervisor loss); that is an ownership
        # matter, not a worker failure, and it blocks as before, but the
        # turn still leaves a report.
        # The suite is not run for the unfinished turn.
        # A supervisor result with every owned process group confirmed
        # dead (an OpenCode rc124 startup failure or an rc143 confirmed
        # stop) returns its evidence to the dispatcher instead: the turn
        # counts as failed on the ladder and stays recoverable. Missing
        # results, rc125, and ambiguous ownership keep the sticky block
        # so no second writer starts behind a possible owner.
        if _confirmed_stop_recoverable(state_dir, request_id, full, rc):
            stop_report = _write_turn_report(
                state_dir, request_id, job, seq, route, full, session_id,
                status="failed", error=full.get("error") or f"rc={rc}",
                run_proof=False, turn_rc=rc, harness_crash=True,
                proof_skipped_reason=f"incomplete turn rc={rc}: the suite is "
                "not run automatically for a stopped turn; the dispatcher "
                "requests useful proof of a coherent candidate")
            _set_phase(state_dir, request_id, phase="implementation_failed",
                       opencode_session_id=session_id, report_path=stop_report.get("report_path"))
            return {"action": "implementation_failed", "session": session_id, "rc": rc,
                    "error": full.get("error"), "report": stop_report,
                    "output": str(full.get("assistant_text") or "")}
        crash_report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id,
                                          status="failed", error=full.get("error") or f"rc={rc}",
                                          run_proof=False, turn_rc=rc, harness_crash=True,
                                          proof_skipped_reason=f"incomplete turn rc={rc}: the suite is "
                                          "not run automatically for a crashed turn; the dispatcher "
                                          "requests useful proof of a coherent candidate")
        _set_phase(state_dir, request_id, phase="implementation_failed",
                   opencode_session_id=session_id, report_path=crash_report.get("report_path"))
        _mark_blocked(state_dir, request_id,
                      f"implementation_failed rc={rc}: {str(full.get('error') or '')[:200]}")
        return {"action": "blocked", "reason": "implementation_failed", "report": crash_report}
    # A hard provider or worker error ends the turn as failed. The
    # dispatcher hears about it and the escalation ladder decides the
    # next route; nothing is retried here. The full suite is not run
    # automatically for the failed turn: the dispatcher requests useful
    # proof of a coherent candidate instead.
    report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id,
                                status="failed", error=full.get("error"),
                                run_proof=False,
                                proof_skipped_reason="failed turn: the suite is not run "
                                "automatically for a failed turn; the dispatcher requests "
                                "useful proof of a coherent candidate")
    _set_phase(state_dir, request_id, phase="implementation_failed",
               opencode_session_id=session_id, report_path=report.get("report_path"))
    return {"action": "implementation_failed", "session": session_id, "rc": rc,
            "error": full.get("error"), "report": report,
            "output": str(full.get("assistant_text") or "")}


def _run_grok_turn(state_dir, request_id, job, workspace, route, prompt,
                   seq, route_reason, run_cmd, artifact=None, attempt=0) -> dict:
    """One headless Grok Build worker turn for the current dispatcher action.

    The saved Grok session resumes when one exists; a turn that reports a
    different session never replaces it. Provider signals move routes
    through the same capacity memory as the owned-server path. The exited
    process is the turn boundary, so no abort or idle confirmation applies.
    """
    grok_h = harnesses.harness_named("grok")
    model, effort = policy.grok_route_params(route)
    saved_session = job.get("grok_session_id")
    # The live turn's kit arrives via the harness spawn_spec (GROK_HOME);
    # the spec env returned here is the manual-run equivalent.
    cmd, _grok_kit_env = adapters.build_grok_cmd(prompt, workspace, model, effort, saved_session)
    grok_meta = {"prompt": prompt, "model": model, "variant": effort,
                 "session_id": saved_session, "seq": seq,
                 "try": attempt,
                 "stage": "implementation", "route": route,
                 "reason": route_reason}
    rc, out, err = run_cmd(cmd, workspace, None, kind="grok_control",
                           meta=grok_meta)
    # The JSON result comes from this invocation's own output file, read
    # through the harness seam.
    full = grok_h.parse_report("grok_control", out, cmd) or {}
    got_session = full.get("grok_session_id")
    if saved_session and got_session \
            and not grok_h.identity_ok("grok_control", got_session, saved_session):
        # A turn on another session, completed or not, never replaces the saved one.
        _persist_error_evidence(state_dir, request_id,
                                {"source": "grok_control", "rc": rc,
                                 "expected_session": saved_session,
                                 "reported_session": got_session})
        _mark_blocked(state_dir, request_id,
                      f"luna_task_mismatch: grok turn reported {str(got_session)[:24]}")
        return {"action": "blocked", "reason": "luna_task_mismatch"}
    session_id = got_session or saved_session
    _record_child(state_dir, request_id, "grok_control", cmd, rc,
                  session_id=session_id,
                  output_text=json.dumps({k: full.get(k) for k in (
                      "ok", "finish", "signal", "error")}, sort_keys=True))
    # A turn that provably never started for a missing runtime blocks
    # recoverably with its cause and next action, before signal handling
    # or a sticky implementation failure could count an unstarted action
    # as an ordinary provider failure or move its explicit route.
    runtime_blocked = _block_runtime_missing(state_dir, request_id, "grok_control",
                                             cmd, grok_meta)
    if runtime_blocked is not None:
        return runtime_blocked
    if rc == 0 and full.get("ok"):
        report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id)
        _set_phase(state_dir, request_id, phase="implemented",
                   grok_session_id=session_id, artifact=artifact,
                   report_path=report.get("report_path"),
                   implementation_output=adapters.redact_text(str(full.get("assistant_text") or ""))[-8000:])
        # A successful request revalidates the route's exhaustion marks the
        # same way a healthy probe does (provider marks only past reset).
        core.record_route_success(state_dir, route)
        return {"action": "implementation_ok", "session": session_id,
                "output": str(full.get("assistant_text") or ""),
                "finish": full.get("finish"), "actual_model": full.get("actual_model"),
                "report": report}
    evidence = full.get("signal_evidence")
    signal = full.get("signal")
    _persist_error_evidence(state_dir, request_id,
                            {"source": "grok_control", "rc": rc,
                             "error": full.get("error"),
                             "signal": signal, "evidence": evidence,
                             "finish": full.get("finish"),
                             "stderr": (err or "")[:1000]})
    if signal in ("exhausted", "overloaded") and isinstance(evidence, dict):
        # Every worker turn leaves a report, including capacity moves, so
        # the dispatcher and the ledger keep the evidence. The suite is
        # not run automatically for a capacity turn.
        move_report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id,
                                         status=signal, error=full.get("error"),
                                         run_proof=False,
                                         proof_skipped_reason=f"{signal} turn: the suite is not run "
                                         "automatically for a capacity turn; the dispatcher requests "
                                         "useful proof of a coherent candidate")
        _set_phase(state_dir, request_id, phase=f"{signal}_moved",
                   grok_session_id=session_id, report_path=move_report.get("report_path"))
        moved = _move_after_signal(state_dir, request_id, route, signal, evidence)
        moved["report"] = move_report
        return moved
    if rc in (124, 143, 125):
        # The supervisor exited without a durable terminal result while the
        # turn never finished: supervisor loss (rc 125, the CLI child still
        # alive but unsupervised), a stop (143), or a legacy 124 record.
        # That is an ownership matter, not a worker failure, and it blocks
        # as before so no new attempt queues behind the unsupervised child;
        # the turn still leaves a report. No elapsed deadline is restored.
        # The suite is not run for the unfinished turn. A supervisor result
        # with every owned process group confirmed dead (rc124 startup
        # failure or rc143 confirmed stop) returns its evidence to the
        # dispatcher instead and stays recoverable; rc125 and ambiguous
        # ownership keep the sticky block.
        if _confirmed_stop_recoverable(state_dir, request_id, full, rc):
            stop_report = _write_turn_report(
                state_dir, request_id, job, seq, route, full, session_id,
                status="failed", error=full.get("error") or f"rc={rc}",
                run_proof=False, turn_rc=rc, harness_crash=True,
                proof_skipped_reason=f"incomplete turn rc={rc}: the suite is "
                "not run automatically for a stopped turn; the dispatcher "
                "requests useful proof of a coherent candidate")
            _set_phase(state_dir, request_id, phase="implementation_failed",
                       grok_session_id=session_id, report_path=stop_report.get("report_path"))
            return {"action": "implementation_failed", "session": session_id, "rc": rc,
                    "error": full.get("error"), "report": stop_report,
                    "output": str(full.get("assistant_text") or "")}
        crash_report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id,
                                          status="failed", error=full.get("error") or f"rc={rc}",
                                          run_proof=False, turn_rc=rc, harness_crash=True,
                                          proof_skipped_reason=f"incomplete turn rc={rc}: the suite is "
                                          "not run automatically for a crashed turn; the dispatcher "
                                          "requests useful proof of a coherent candidate")
        _set_phase(state_dir, request_id, phase="implementation_failed",
                   grok_session_id=session_id, report_path=crash_report.get("report_path"))
        _mark_blocked(state_dir, request_id,
                      f"implementation_failed rc={rc}: {str(full.get('error') or '')[:200]}")
        return {"action": "blocked", "reason": "implementation_failed", "report": crash_report}
    # A hard provider or worker error ends the turn as failed. The
    # dispatcher hears about it and the escalation ladder decides the
    # next route; nothing is retried here. The full suite is not run
    # automatically for the failed turn: the dispatcher requests useful
    # proof of a coherent candidate instead.
    report = _write_turn_report(state_dir, request_id, job, seq, route, full, session_id,
                                status="failed", error=full.get("error"),
                                run_proof=False,
                                proof_skipped_reason="failed turn: the suite is not run "
                                "automatically for a failed turn; the dispatcher requests "
                                "useful proof of a coherent candidate")
    _set_phase(state_dir, request_id, phase="implementation_failed",
               grok_session_id=session_id, report_path=report.get("report_path"))
    return {"action": "implementation_failed", "session": session_id, "rc": rc,
            "error": full.get("error"), "report": report,
            "output": str(full.get("assistant_text") or "")}



def _proof_command(task_json: str) -> str | None:
    """The task's own proof command, when the task JSON names one."""
    try:
        task = json.loads(task_json or "null")
    except ValueError:
        return None
    if isinstance(task, dict) and isinstance(task.get("proof"), str) and task["proof"].strip():
        return task["proof"].strip()
    return None


def _unquote_git_path(path: str) -> str:
    """Unquote a ``git status --porcelain`` path.

    Porcelain quotes paths with spaces or special bytes (for example
    ``?? "my file.txt"``) and reports renames as ``old -> new``. The
    changed-files list must hold the real path, never the arrow string
    or the surrounding quotes.
    """
    text = (path or "").strip()
    if " -> " in text:
        text = text.rsplit(" -> ", 1)[-1].strip()
    if len(text) >= 2 and text.startswith('"') and text.endswith('"'):
        inner = text[1:-1]
        out: list[str] = []
        i = 0
        while i < len(inner):
            if inner[i] == "\\" and i + 1 < len(inner):
                nxt = inner[i + 1]
                if nxt == "n":
                    out.append("\n")
                    i += 2
                    continue
                if nxt == "t":
                    out.append("\t")
                    i += 2
                    continue
                if nxt in ('"', "\\"):
                    out.append(nxt)
                    i += 2
                    continue
                if nxt in "01234567":
                    digits = ""
                    for c in inner[i + 1:i + 4]:
                        if c in "01234567":
                            digits += c
                        else:
                            break
                    if digits:
                        try:
                            out.append(chr(int(digits, 8)))
                            i += 1 + len(digits)
                            continue
                        except ValueError:
                            pass
                out.append(nxt)
                i += 2
                continue
            out.append(inner[i])
            i += 1
        text = "".join(out)
        try:
            if "\\" in inner:
                text = text.encode("latin1").decode("utf-8", errors="replace")
        except Exception:
            pass
    return text


def _git_changes(workspace: str) -> tuple[list[str], str, str | None]:
    """(changed files, diff text, note) for a Git workspace; a note when not Git.

    The diff covers staged and unstaged tracked changes (``git diff HEAD``)
    plus untracked files, so the report's changed-files list and its patch
    never disagree.
    """
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
        diff = subprocess.run(["git", "-C", workspace, "diff", "HEAD", "--"],
                              capture_output=True, text=True,
                              timeout=30, stdin=subprocess.DEVNULL).stdout
        # Untracked files are not in any diff: append them as new-file
        # patches so the patch stays complete.
        untracked = []
        for line in (status or "").splitlines():
            if line.startswith("??"):
                rel = _unquote_git_path(line[2:])
                if rel:
                    untracked.append(rel)
        for rel in untracked:
            try:
                shown = subprocess.run(["git", "-C", workspace, "diff", "--no-index", "--", "/dev/null", rel],
                                       capture_output=True, text=True, timeout=15,
                                       stdin=subprocess.DEVNULL, cwd=workspace).stdout
            except Exception:  # noqa: BLE001
                shown = ""
            if shown:
                diff += ("\n" if diff and not diff.endswith("\n") else "") + shown
            else:
                try:
                    body = Path(workspace, rel).read_text(encoding="utf-8", errors="replace")
                except OSError:
                    body = ""
                diff += f"\n--- /dev/null\n+++ b/{rel}\n{body}"
                if diff and not diff.endswith("\n"):
                    diff += "\n"
    except Exception as e:  # noqa: BLE001
        return [], "", f"git failed: {type(e).__name__}"
    files = [_unquote_git_path(line[3:]) for line in status.splitlines() if line.strip()]
    files = [f for f in files if f]
    return files, diff, None


def _run_proof_command(workspace: str, proof_cmd: str, timeout: int = 600,
                       state_dir: str | None = None,
                       request_id: str | None = None
                       ) -> tuple[int, str, str, str | None, str | None]:
    """Run one verification attempt in its own process group.

    Returns ``(rc, output, proof_class, started_at, ended_at)`` with the
    output redacted. A proof that runs past ``timeout`` has its whole
    process tree stopped (SIGKILL to the group, so no orphaned proof
    child can race a later writer) and classifies ``timeout`` (rc 124),
    apart from an executable-not-found ``not_found`` (rc 127). A main
    process that already exited while a background child holds the
    output pipe open is not a timeout: the group is reaped and the
    actual exit code classifies the attempt. After a normal exit any
    leftover group members are stopped as well. Other spawn failures
    classify ``error`` (rc 127); a zero exit is ``pass`` and any other
    exit is ``failed``. Timestamps are UTC ISO strings; unavailable
    values stay None, never invented. The proof group is recorded
    durably while it runs so cancel and recover block on unresolved
    proof ownership instead of treating the job as stopped.
    """
    started_at = core._utcnow()
    try:
        proc = subprocess.Popen(
            ["/bin/sh", "-c", proof_cmd], cwd=workspace,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, env=dict(os.environ),
            start_new_session=True, text=True)
    except FileNotFoundError as e:
        ended_at = core._utcnow()
        return (127, adapters.redact_text(
            f"proof command failed to run: {type(e).__name__}: {e}\n"),
            "not_found", started_at, ended_at)
    except OSError as e:
        ended_at = core._utcnow()
        return (127, adapters.redact_text(
            f"proof command failed to run: {type(e).__name__}: {e}\n"),
            "error", started_at, ended_at)
    if state_dir is not None and request_id is not None:
        try:
            proc_pgid = os.getpgid(proc.pid)
        except (ProcessLookupError, PermissionError, ValueError, OSError):
            proc_pgid = None
        core.record_proof_owner(state_dir, request_id, proc.pid, proc_pgid)
    try:
        try:
            out, _ = proc.communicate(timeout=timeout)
            rc = proc.returncode
            # The main process exited: reap any leftover group members
            # so no proof child races a later writer.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, ValueError, OSError):
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        except subprocess.TimeoutExpired:
            if proc.poll() is not None:
                # The main process already exited but a background child
                # holds the output pipe: reap the group and classify the
                # actual exit instead of reporting a false timeout.
                out = ""
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError, ValueError, OSError):
                    pass
                try:
                    out, _ = proc.communicate(timeout=10)
                except Exception:  # noqa: BLE001 - the tree is dead; keep what we have
                    pass
                rc = proc.returncode if proc.returncode is not None else 124
            else:
                out = ""
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError, ValueError, OSError):
                    pass
                try:
                    out, _ = proc.communicate(timeout=10)
                except Exception:  # noqa: BLE001 - the tree is dead; keep what we have
                    pass
                rc = 124
    finally:
        if state_dir is not None and request_id is not None:
            core.clear_proof_owner(state_dir, request_id)
    ended_at = core._utcnow()
    out = out or ""
    if rc == 0:
        proof_class = "pass"
    elif rc == 124:
        proof_class = "timeout"
    elif rc == 127:
        proof_class = "not_found"
    else:
        proof_class = "failed"
    return rc, out, proof_class, started_at, ended_at


def _write_turn_report(state_dir, request_id: str, job: dict, seq: int, route: str,
                       full: dict, session_id: str | None, status: str = "ok",
                       error=None, *, run_proof: bool = True,
                       proof_skipped_reason: str | None = None,
                       proof_timeout: int = 600,
                       turn_rc: int | None = None,
                       harness_crash: bool = False) -> dict:
    """Write report.json, proof.log, diff.patch, and worker.txt for one turn.

    The runner runs the task's own proof command and records the exit code;
    the worker's full text is kept in worker.txt (redacted) with only a
    redacted summary in the report. Harness-reported worker questions travel
    in the report's blockers (redacted), never as live questions; the
    implementation harness denies question permission today, so blockers are
    typically empty. Retries on the same seq use a suffixed
    directory so no turn's evidence is overwritten.

    Writing the failure report is separate from executing proof: pass
    ``run_proof=False`` for exhausted, stalled, crashed, or otherwise
    incomplete turns, where the full suite would run against an
    unfinished candidate. The skipped proof is recorded truthfully
    (``proof_class skipped`` with its reason); required final
    verification still refuses completion without a bound passing proof.
    Every report carries the turn's failure class, the proof attempt
    class with its timestamps, the harness signal where one exists, and
    the workspace HEAD the evidence was taken against, so Observer can
    measure failure-to-restart and recovery success.
    """
    root = store.ensure_state_dir(state_dir)
    base = store.job_dir_for(root, request_id) / f"turn-{seq}"
    turn_dir = base
    suffix = 1
    while (turn_dir / "report.json").exists():
        turn_dir = Path(str(base) + f"-{suffix}")
        suffix += 1
    turn_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    workspace = job["workspace"]
    worker_text = adapters.redact_text(str(full.get("assistant_text") or ""))
    store.secure_write_text(turn_dir / "worker.txt", worker_text)
    files, diff, note = _git_changes(workspace)
    store.secure_write_text(turn_dir / "diff.patch", diff if not note else f"# {note}\n")
    proof_cmd = _proof_command(job.get("task_json") or "")
    proof_rc = None
    proof_class = "none"
    proof_skipped = None
    proof_started_at = None
    proof_ended_at = None
    proof_log = turn_dir / "proof.log"
    if proof_cmd and run_proof:
        # The task's proof runs exactly as written: through the shell in
        # the workspace with the runner's environment, so `&&`, pipes,
        # and quoting behave as in the project's own docs.
        proof_rc, proof_out, proof_class, proof_started_at, proof_ended_at = \
            _run_proof_command(workspace, proof_cmd, timeout=proof_timeout,
                               state_dir=state_dir, request_id=request_id)
        header = f"$ {proof_cmd}\nexit {proof_rc}\n"
        store.secure_write_text(proof_log, adapters.redact_text(header + proof_out))
    elif proof_cmd:
        proof_class = "skipped"
        proof_skipped = proof_skipped_reason or \
            "proof deferred: the turn did not produce a verifiable candidate"
        store.secure_write_text(proof_log, adapters.redact_text(
            f"$ {proof_cmd}\nskipped: {proof_skipped}\n"))
    else:
        store.secure_write_text(proof_log, "# no proof command in the task\n")
    if proof_rc is not None and proof_rc != 0 and status == "ok":
        # The worker finished but the task's own proof failed: the turn
        # counts as failed on the ladder, so the report and the evidence
        # forwarded to Luna must say so too.
        status = "failed"
        if error is None:
            error = f"proof_failed rc={proof_rc}"
    signal_name = full.get("signal") if isinstance(full, dict) else None
    if not isinstance(signal_name, str):
        signal_name = None
    failure_class = None
    if status != "ok":
        failure_class = core.failure_class_for(
            signal=signal_name, proof_class=proof_class, rc=turn_rc)
        if failure_class == "timeout" and harness_crash and turn_rc == 124:
            # A supervisor-level rc124 with no proof outcome (an OpenCode
            # startup failure, a legacy 124 record) is infrastructure,
            # never a proof timeout: the suite never ran, so nothing
            # timed out. A proof that ran past its budget carries
            # proof_class timeout instead and stays timeout.
            failure_class = "infrastructure"
        if failure_class == "unknown" and (error or turn_rc not in (None, 0)):
            # The worker or harness errored without a capacity signal:
            # a vanished supervisor is infrastructure, a finished worker
            # that errored is implementation.
            failure_class = "infrastructure" if harness_crash else "implementation"
    am = full.get("actual_model") if isinstance(full.get("actual_model"), dict) else {}
    observed = (f"{am.get('providerID')}/{am.get('modelID')}" if am.get("providerID") and am.get("modelID") else None)
    observed_variant = am.get("variant") if isinstance(am.get("variant"), str) else None
    model, variant = policy.worker_model_variant(route)
    blockers_raw = full.get("blockers") if isinstance(full.get("blockers"), list) else []
    report = {
        "schema_version": store.SCHEMA_VERSION, "request_id": request_id, "seq": seq,
        "stage": "implementation", "route": route, "policy_version": policy.POLICY_VERSION,
        "model": model, "variant": variant, "observed_model": observed,
        "observed_variant": observed_variant, "session_id": session_id,
        "finish": full.get("finish"), "status": status,
        "error": adapters.redact_nested(error) if error else None,
        "failure_class": failure_class, "signal": signal_name,
        "changed_files": files, "workspace_note": note,
        "head_commit": core._workspace_head(workspace),
        "proof_command": proof_cmd, "proof_exit_code": proof_rc,
        "proof_class": proof_class, "proof_skipped": proof_skipped,
        "proof_started_at": proof_started_at, "proof_ended_at": proof_ended_at,
        "proof_log": str(proof_log), "diff": str(turn_dir / "diff.patch"),
        "worker_text": str(turn_dir / "worker.txt"), "worker_summary": worker_text[:1500],
        "blockers": adapters.redact_nested(blockers_raw),
        "tokens": full.get("usage"), "native_ids": full.get("native_ids"),
        "longest_silence_secs": full.get("longest_silence_secs"),
    }
    report_path = turn_dir / "report.json"
    store.secure_write_text(report_path, json.dumps(report, sort_keys=True, indent=1))
    report["report_path"] = str(report_path)
    # An executed proof is also a durable verification attempt row
    # (stage verification with its timestamps and class), so Observer
    # measures it from the existing invocation records. Skipped proofs
    # record no attempt: nothing ran, and the report says so truthfully.
    if proof_rc is not None:
        try:
            core.record_verification_attempt(
                state_dir, request_id, seq, route, proof_cmd, proof_rc,
                proof_class, proof_started_at, proof_ended_at,
                str(report_path), str(proof_log))
        except Exception:
            pass
    # Link the report to the invocation that produced it.
    kinds = harnesses.worker_control_kinds()
    placeholders = ",".join("?" for _ in kinds)
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(f"UPDATE invocations SET report_path=? WHERE request_id=? AND kind IN ({placeholders})"
                    f" AND invocation_id=(SELECT invocation_id FROM invocations WHERE request_id=?"
                    f" AND kind IN ({placeholders}) ORDER BY id DESC LIMIT 1)",
                    (str(report_path), request_id, *kinds, request_id, *kinds))
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
    """The dispatcher's resume message: paths and structured fields, not prose.

    Carries the turn's paths plus the structured measurements Luna needs
    (route, policy, models, variants, status, failure and proof classes,
    tokens, native identities). Worker prose and proof output stay in the
    files; only redacted summaries travel here, never raw tails. The
    guidance consumes the runner's exact-candidate evidence instead of
    requiring the read-only dispatcher to blindly repeat valid proof,
    and it preserves the single PR identity across correction and
    recovery.
    """
    report = impl.get("report") if isinstance(impl.get("report"), dict) else {}
    known_pr = None
    try:
        st = json.loads(job.get("controller_state") or "{}") or {}
        known_pr = st.get("pr_url") if isinstance(st, dict) else None
    except ValueError:
        known_pr = None
    fields = {
        "turn_status": report.get("status") or ("failed" if impl.get("action") == "implementation_failed" else "ok"),
        "turn_error": report.get("error"),
        "failure_class": report.get("failure_class"),
        "proof_class": report.get("proof_class"),
        "proof_skipped": report.get("proof_skipped"),
        "head_commit": report.get("head_commit"),
        "known_pr_url": known_pr,
        "route": report.get("route") or job.get("route"),
        "model": report.get("model") or job.get("model"),
        "variant": report.get("variant"),
        "observed_model": report.get("observed_model"),
        "observed_variant": report.get("observed_variant"),
        "policy_version": report.get("policy_version"),
        "stage": report.get("stage"),
        "seq": report.get("seq"),
        "session": impl.get("session") or report.get("session_id") or "",
        "finish": impl.get("finish") or report.get("finish") or "",
        "changed_files": report.get("changed_files") or [],
        "proof_command": report.get("proof_command"), "proof_exit_code": report.get("proof_exit_code"),
        "blockers": report.get("blockers") or [],
        "tokens": report.get("tokens"),
        "native_ids": report.get("native_ids"),
        "report": report.get("report_path"), "proof_log": report.get("proof_log"),
        "diff": report.get("diff"), "worker_text": report.get("worker_text"),
    }
    return (json.dumps(fields, sort_keys=True) + "\n"
            "Consume this exact-candidate evidence (report, proof log, diff) for the "
            "candidate commit above; do not rerun the full suite against it when its "
            "proof already passed. Request implementation again when the turn failed, "
            "when proof was skipped or is stale, or when the candidate changed; the "
            "worker updates the existing PR, never opens a second. Completion needs "
            "bound proof, an open PR on the current candidate, and required acceptance "
            "evidence.")


def _complete_job(state_dir, request_id: str, token: str | None,
                   output: str, artifact: str | None = None,
                   pr_url: str | None = None,
                   acceptance_evidence: str | None = None) -> dict:
    """Persist the terminal result before acknowledgement (lease-held)."""
    job = core.get_job(state_dir, request_id)
    use_token = token or job.get("owner_token")
    result_payload = {"output": output}
    if artifact:
        result_payload["artifact"] = artifact
    # A valid nonblank PR URL is preserved; otherwise record JSON null
    # (no PR was possible or none was supplied) rather than a fabricated
    # URL, matching the recovery result record.
    if isinstance(pr_url, str) and pr_url.strip():
        result_payload["pr_url"] = pr_url
    else:
        result_payload["pr_url"] = None
    # Acceptance evidence is durable in the result record as well as the
    # envelope, bound to the candidate commit the proof ran against:
    # worker exit zero, helper tests, and an open PR alone never
    # establish acceptance when the task names required evidence.
    if isinstance(acceptance_evidence, str) and acceptance_evidence.strip():
        result_payload["acceptance_evidence"] = acceptance_evidence.strip()
    else:
        result_payload["acceptance_evidence"] = None
    result_payload["head_commit"] = core._workspace_head(job.get("workspace"))
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


def _record_completion_refusal(state_dir, request_id: str, report: dict,
                               reason: str) -> None:
    """Persist one completion refusal and its evidence for the dispatcher.

    Stores ``completion_refused_seq`` in the controller state so a second
    completion on the same failed turn blocks, and emits a
    ``completion_refused`` ledger event (visible in ``status``) before the
    evidence goes back to Luna. Raises on persistence or lease loss so the
    caller fails loudly instead of resuming on an unrecorded refusal.
    """
    try:
        rep_seq = report.get("seq")
        rep_seq_i = int(rep_seq) if rep_seq is not None else None
    except (TypeError, ValueError):
        rep_seq_i = None
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        try:
            st = json.loads(row["controller_state"] or "{}") if row is not None else {}
        except ValueError:
            st = {}
        if not isinstance(st, dict):
            st = {}
        st["completion_refused_seq"] = rep_seq_i
        st["completion_refused_reason"] = reason
        st["completion_refused_report"] = str(
            report.get("report_path") or report.get("_path") or "")[:500]
        con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                    (json.dumps(st, sort_keys=True), core._utcnow(), request_id))
        core._event(con, request_id, "completion_refused",
                    {"reason": str(reason)[:200],
                     "proof_exit_code": report.get("proof_exit_code"),
                     "report": str(report.get("report_path") or "")[:500],
                     "seq": rep_seq_i})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _handle_completion_refusal(state_dir, request_id: str, envelope: dict,
                               report: dict, reason: str, run_cmd) -> dict:
    """Refuse a completion envelope while the last turn's proof failed.

    The first refusal hands the evidence back to the dispatcher once (a
    resume carrying the failed report's paths, status, and proof exit
    code, so the ladder can correct); a completion that insists on the
    same failed turn blocks with the refusal reason, visible in ``status``.
    An ordinary completion without an opened PR URL is refused through
    the same once-then-block pattern, with evidence naming the missing
    PR URL instead of a proof failure.
    """
    try:
        rep_seq = report.get("seq")
        rep_seq_i = int(rep_seq) if rep_seq is not None else None
    except (TypeError, ValueError):
        rep_seq_i = None
    st = _load_controller_state(core.get_job(state_dir, request_id))
    if "completion_refused_seq" in st and st.get("completion_refused_seq") == rep_seq_i:
        _mark_blocked(state_dir, request_id, reason)
        return {"action": "blocked", "reason": "completion_refused"}
    _record_completion_refusal(state_dir, request_id, report, reason)
    job = core.get_job(state_dir, request_id)
    try:
        _reqs = core.task_completion_requirements(job.get("task_json"))
    except Exception:
        _reqs = {}
    fields = {
        "completion_refused": reason,
        "required_acceptance": _reqs.get("acceptance"),
        "known_pr_url": core.known_pr_url(state_dir, request_id),
        "turn_status": report.get("status"),
        "turn_error": report.get("error"),
        "route": report.get("route") or job.get("route"),
        "proof_command": report.get("proof_command"),
        "proof_exit_code": report.get("proof_exit_code"),
        "report": report.get("report_path"),
        "proof_log": report.get("proof_log"),
        "diff": report.get("diff"),
        "worker_text": report.get("worker_text"),
        "seq": report.get("seq"),
    }
    evidence = (json.dumps(fields, sort_keys=True) + "\n" + (
        "Completion is refused: an ordinary job succeeds only with an "
        "opened PR URL. Push the branch, open the PR, and complete again "
        "with its pr_url; do not complete without one."
        if "missing pr_url" in reason else
        "Completion is refused: keep the job's single PR identity. Update the existing "
        "open PR to the candidate commit and complete again with its URL; do not open "
        "another."
        if ("duplicate PR" in reason or "completion_refused: pr_" in reason
                or "draft PR" in reason) else
        "Completion is refused: carry the required acceptance evidence in "
        "acceptance_evidence and complete again."
        if "acceptance" in reason else
        "Completion is refused: no bound proof covers the current candidate. Request "
        "implementation again for a coherent candidate; do not complete until the proof "
        "passes."
        if ("incomplete proof" in reason or "stale proof" in reason) else
        "Completion is refused: "
        "the last implementation turn's proof failed. Fix the work and "
        "request implementation again; do not complete until the proof passes."))
    r = resume_luna(state_dir, request_id, evidence, run_cmd=run_cmd,
                    label="COMPLETION REFUSED")
    if r.get("action") == "blocked":
        return r
    return {"action": "completion-refused-resumed", "luna_action": r.get("luna_action"),
            "reason": "completion_refused"}


def _handle_question_action(state_dir, request_id: str, envelope: dict,
                             run_cmd) -> dict:
    """Persist question -> wake the saved planner in its own harness ->
    persist answer -> resume exact Luna task. Never forks a session
    (except the fresh fallback for a harness without resume).
    An answer already persisted (public ``answer`` or earlier callback)
    is used without asking the planner again.
    """
    qid = str(envelope.get("qid") or envelope.get("question_id") or "q1")
    prompt_q = str(envelope.get("prompt") or envelope.get("question") or
                   envelope.get("text") or "Planner input requested.")
    answer_text = None
    for q in core.list_questions(state_dir, request_id, only_pending=False):
        if q["qid"] == qid and q["status"] == "answered" and q.get("answer"):
            if q.get("prompt") != prompt_q:
                _mark_blocked(state_dir, request_id,
                              f"planner_question_conflict: {qid} was answered for a different prompt; "
                              f"clear it with `questions --clear {qid}` or use a new qid")
                return {"action": "blocked", "reason": "question_conflict"}
            answer_text = str(q["answer"])
    if answer_text is None:
        cb = planner_callback(state_dir, request_id, qid, prompt_q, run_cmd=run_cmd)
        if cb.get("action") == "blocked":
            return cb
        for q in core.list_questions(state_dir, request_id, only_pending=False):
            if q["qid"] == qid:
                answer_text = q.get("answer") or ""
    # A planner answer to the exhaustion decision permits exactly one
    # explicitly authorized directed recovery attempt (an approach or an
    # eligible route relayed as directed_route). The answer directs; the
    # dispatcher assigns that work under the routing policy. The planner
    # never implements.
    try:
        st_now = _load_controller_state(core.get_job(state_dir, request_id))
    except Exception:
        st_now = {}
    if qid and qid == st_now.get("recovery_question_qid") \
            and isinstance(answer_text, str) and answer_text.strip() \
            and not st_now.get("planner_recovery_used"):
        try:
            _set_phase(state_dir, request_id,
                       planner_recovery_authorized=True)
        except Exception:
            pass
    r = resume_luna(state_dir, request_id, f"question {qid}: {prompt_q}\nanswer: {answer_text}",
                    run_cmd=run_cmd, label="PLANNER ANSWER")
    if r.get("action") == "blocked":
        return r
    return {"action": "question-answered-resumed", "qid": qid,
            "luna_action": r.get("luna_action")}


LADDER_RUNGS = ("initial", "correction", "correction_fresh", "recovery")


def eligible_directed_routes(state_dir, request_id: str) -> list[str]:
    """Genuinely eligible dispatcher routes for a planner direction.

    A route qualifies when the dispatcher may assign it (owned-server
    or headless-worker harness, never a planner-harness rung), it sits
    in the job lane or on the correction/recovery stage, and it has
    capacity (not exhausted or degraded, one-turn routes unused,
    concurrency cap free). The escalation evidence lists these so a
    planner direction names a route that can actually run; anything
    else is rejected with evidence, never substituted silently.
    """
    try:
        job = core.get_job(state_dir, request_id)
    except Exception:
        return []
    lane = job.get("lane")
    try:
        exhausted = core.exhausted_routes(state_dir)
        degraded = core.degraded_routes(state_dir)
    except Exception:
        exhausted, degraded = set(), set()
    try:
        turns = _turns_by_route(state_dir, request_id)
    except Exception:
        turns = {}
    ordered: list[str] = []
    for stage in ("implementation_default", "implementation_small",
                  "implementation_hard", "correction", "recovery"):
        try:
            routes = policy.stage_routes(stage)
        except ValueError:
            continue
        for route in routes:
            if route not in ordered:
                ordered.append(route)
    out: list[str] = []
    for route in ordered:
        if not policy.is_dispatcher_assignable(route):
            continue
        try:
            stage = policy.lane_of_route(route, lane)
        except ValueError:
            stage = None
        if stage is None and route not in policy.STAGES.get(
                "correction", {}).get("routes", ()) \
                and route not in policy.STAGES.get(
                    "recovery", {}).get("routes", ()):
            continue
        if route in exhausted or route in degraded:
            continue
        try:
            if policy.one_turn_routes_used(route, turns):
                continue
        except Exception:
            continue
        try:
            if core.route_concurrency_full(state_dir, route,
                                           exclude=request_id):
                continue
        except Exception:
            continue
        out.append(route)
    return out


def _escalate_recovery_to_planner(state_dir, request_id: str,
                                  token: str | None = None) -> dict:
    """Persist an exhaustion decision request and wake the planner.

    Posts the concrete decision (required choice, evidence, attempted
    remedies, eligible routes, recommendation) as a planner question
    through the existing planner-question path, preserving the job and
    Observer identities. The planner answers with direction (an approach
    or an eligible route relayed as ``directed_route``), which permits
    exactly one explicitly authorized recovery attempt; the planner
    never implements. Returns the step action for this transition.
    """
    reason = _recovery_exhausted_reason(state_dir, request_id)
    qid = "recovery-decision"
    try:
        core.post_question(state_dir, request_id, qid, reason,
                           lease_token=token)
    except core.ConflictError:
        _mark_blocked(state_dir, request_id, reason)
        return {"action": "blocked", "reason": "recovery_exhausted"}
    except (core.TerminalError, core.NotFoundError, ValueError) as e:
        _mark_blocked(state_dir, request_id,
                      f"recovery_exhausted: planner question unavailable "
                      f"({type(e).__name__}); {reason}"[:1500])
        return {"action": "blocked", "reason": "recovery_exhausted"}
    try:
        _persist_envelope(state_dir, request_id,
                          {"action": "planner_question", "qid": qid,
                           "prompt": reason},
                          "recovery-exhausted")
        _set_phase(state_dir, request_id, recovery_question_qid=qid)
    except core.LeaseLostError:
        raise
    except Exception:
        pass
    return {"action": "recovery-exhausted-question", "qid": qid,
            "reason": "recovery_exhausted"}


def _recovery_exhausted_reason(state_dir, request_id: str) -> str:
    """Concrete planner decision for an exhausted escalation.

    Retry exhaustion prompts a decision, never a request for the planner
    to implement: the reason carries the decision required, the evidence,
    the attempted remedies, and the dispatcher's recommendation. Routine
    mechanical recovery stays with the dispatcher; missing authority or
    a consequential scope or approach decision reaches the planner
    through this reason.
    """
    ctx = core.exhaustion_context(state_dir, request_id)
    tried = ", ".join(f"{route}x{n}" for route, n in ctx["routes_tried"]) \
        or "no turns recorded"
    ev = "; ".join(
        f"seq={r.get('seq')} {r.get('route')} {r.get('status')}/"
        f"{r.get('failure_class')} proof={r.get('proof_exit_code')} "
        f"{r.get('report')}" for r in ctx["reports"][-4:]) or "no turn reports"
    try:
        eligible = eligible_directed_routes(state_dir, request_id)
    except Exception:
        eligible = []
    eligible_txt = ", ".join(eligible) if eligible else \
        "none currently eligible (all dispatcher routes exhausted, degraded, or capped)"
    return ("recovery_exhausted: every recovery rung was already used; "
            "decision required: choose the next approach (an eligible route or a scope "
            "change) for the original objective; "
            f"evidence: {ev}; attempted: {tried}; "
            f"eligible dispatcher routes: {eligible_txt}; "
            "recommendation: answer with the direction (an approach or one eligible "
            "route the dispatcher relays as directed_route and assigns under the "
            "routing policy for exactly one authorized attempt); the planner does not implement")


def _escalation_exhausted_error(state_dir, request_id: str, failures: int,
                                reports: list) -> dict:
    """Terminal evidence for an exhausted escalation, for the planner to act on."""
    ctx = core.exhaustion_context(state_dir, request_id)
    tried = ", ".join(f"{route}x{n}" for route, n in ctx["routes_tried"]) \
        or "no turns recorded"
    return {
        "code": "ESCALATION_EXHAUSTED", "failures": failures, "reports": reports,
        "message": "initial turn, one correction, one fresh correction, and one escalation failed",
        "decision_required": "choose the next approach (an eligible route or a scope "
                             "change) for the original objective",
        "evidence": [f"seq={r.get('seq')} {r.get('route')} {r.get('status')}/"
                     f"{r.get('failure_class')} proof={r.get('proof_exit_code')} "
                     f"{r.get('report')}" for r in ctx["reports"][-6:]],
        "attempted": ("initial turn, one correction, one fresh correction, and one "
                      f"escalation; routes tried: {tried}"),
        "recommendation": "the planner returns its direction through the dispatcher "
                          "and the dispatcher assigns that work under the routing policy; "
                          "the planner does not implement",
    }


def _ladder(job: dict) -> dict:
    st = _load_controller_state(job)
    ladder = st.get("ladder") if isinstance(st.get("ladder"), dict) else {}
    out = {"failures": int(ladder.get("failures") or 0),
           "rung": ladder.get("rung") or "initial",
           "escalated": bool(ladder.get("escalated"))}
    if ladder.get("counted_seq") is not None:
        out["counted_seq"] = ladder.get("counted_seq")
    return out


def _clear_session(state_dir, request_id: str) -> None:
    """Forget the worker session on a rung change, on every worker harness.

    A fresh correction or escalation starts a new worker session; the old
    one is never resumed again.
    """
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        con.execute("UPDATE jobs SET opencode_session_id=NULL, grok_session_id=NULL, updated_at=?"
                    " WHERE request_id=?",
                    (core._utcnow(), request_id))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


def _apply_ladder(state_dir, request_id: str, token: str | None = None) -> dict | None:
    """Choose the rung for the next implementation turn from the failures so far.

    0 failures: the initial route. 1: a correction in the same worker
    session. 2: a fresh correction on the correction stage's route. 3: the
    single escalation to the recovery stage. 4 or more: the evidence
    returns to the planner through the existing planner-question path
    before any authorized directed attempt is used; only after that one
    explicitly authorized attempt is used does the job end failed with
    its evidence, which is the planner's to act on. A pool move inside
    the recovery stage is not a second escalation."""
    job = core.get_job(state_dir, request_id)
    ladder = _ladder(job)
    failures = ladder["failures"]
    if failures <= 0:
        return None
    if failures == 1:
        rung = "correction"
        target = None
    elif failures == 2:
        rung = "correction_fresh"
        target = policy.stage_routes("correction")[0]
    elif failures == 3:
        rung = "recovery"
        target = policy.next_recovery_route(None, _turns_by_route(state_dir, request_id))
        if target is None:
            # Every recovery rung was already used by this job: the
            # planner gets a concrete decision (evidence, attempted
            # remedies, eligible routes, recommendation) through the
            # existing planner-question path instead of a second
            # automatic rung, never a request for the planner to
            # implement. After the planner answers, exactly one
            # explicitly authorized directed attempt may run; a failure
            # after that attempt used ends the job terminally below.
            try:
                st_used = _load_controller_state(job).get(
                    "planner_recovery_used")
                st_flight = _load_controller_state(job).get(
                    "planner_recovery_in_flight")
            except Exception:
                st_used = False
                st_flight = False
            if st_flight and not st_used:
                # The planner already answered and its one authorized
                # attempt is still in flight through a capacity move:
                # never re-post the question nor fail before it runs.
                return None
            if st_used:
                reports = sorted(str(p) for p in (store.job_dir_for(store.ensure_state_dir(state_dir), request_id)
                                                  .glob("turn-*/report.json")))
                error = _escalation_exhausted_error(state_dir, request_id, failures, reports)
                if token:
                    try:
                        core.fail(state_dir, request_id, token, error)
                    except core.OwnershipError as e:
                        raise core.LeaseLostError(str(e))
                else:
                    _fail_offline(state_dir, request_id, error)
                return {"action": "failed", "reason": "escalation_exhausted", "reports": reports}
            return _escalate_recovery_to_planner(state_dir, request_id, token)
    else:
        # Normal failures at 4 or more still owe the planner a concrete
        # decision before any authorized directed attempt is used: the
        # usual path is one recovery escalation that fails, not three
        # exhausted rungs. Only after the planner's one explicitly
        # authorized attempt is used does exhaustion end terminally.
        try:
            st_used = _load_controller_state(job).get("planner_recovery_used")
            st_flight = _load_controller_state(job).get("planner_recovery_in_flight")
        except Exception:
            st_used = False
            st_flight = False
        if st_flight and not st_used:
            # The planner already answered and its one authorized attempt
            # is still in flight through a preflight or capacity move:
            # never re-post the question nor fail before the actual
            # directed worker attempt runs.
            return None
        if not st_used:
            return _escalate_recovery_to_planner(state_dir, request_id, token)
        reports = sorted(str(p) for p in (store.job_dir_for(store.ensure_state_dir(state_dir), request_id)
                                          .glob("turn-*/report.json")))
        error = _escalation_exhausted_error(state_dir, request_id, failures, reports)
        if token:
            try:
                core.fail(state_dir, request_id, token, error)
            except core.OwnershipError as e:
                raise core.LeaseLostError(str(e))
        else:
            _fail_offline(state_dir, request_id, error)
        return {"action": "failed", "reason": "escalation_exhausted", "reports": reports}
    if rung == ladder["rung"]:
        return None
    if target and target != job.get("route"):
        _clear_session(state_dir, request_id)
        _switch_route(state_dir, request_id, target,
                      "escalation" if rung == "recovery" else "correction",
                      {"source": "ladder", "failures": failures})
    _set_phase(state_dir, request_id, ladder={"failures": failures, "rung": rung,
                                              "escalated": ladder["escalated"] or rung == "recovery",
                                              **({"counted_seq": ladder["counted_seq"]}
                                                 if ladder.get("counted_seq") is not None else {})})
    # Link the failed attempt onward: Observer joins this decision to the
    # next attempt's report and invocation rows by seq, and measures
    # failure-to-restart from failed_at to the next attempt start. The
    # actual next seq is linked when the next worker invocation starts
    # (see _link_recovery_attempt): planner questions, refusals, and
    # completion envelopes may consume seq numbers first, so the decision
    # itself never guesses it.
    core.record_recovery_decision(
        state_dir, request_id, failures, rung, target,
        ladder.get("counted_seq"),
        "escalation" if rung == "recovery" else "correction",
        lease_token=token)
    try:
        _set_phase(state_dir, request_id,
                   pending_recovery={"failed_seq": ladder.get("counted_seq")})
    except core.LeaseLostError:
        raise
    except Exception:
        pass
    return None


def _link_recovery_attempt(state_dir, request_id: str, seq: int | None,
                           route: str | None = None) -> None:
    """Bind a pending recovery decision to the worker attempt now starting.

    Emits ``recovery_next_attempt`` with the failed seq and the actual
    next seq, then clears the pending link. Never raises past the
    caller: a missed link leaves the decision event itself, never a
    fabricated seq.
    """
    try:
        job = core.get_job(state_dir, request_id)
    except Exception:
        return
    st = _load_controller_state(job)
    pending = st.get("pending_recovery")
    if not isinstance(pending, dict):
        return
    try:
        failed_seq = pending.get("failed_seq")
        failed_i = int(failed_seq) if failed_seq is not None else None
    except (TypeError, ValueError):
        failed_i = None
    try:
        next_i = int(seq) if seq is not None else None
    except (TypeError, ValueError):
        next_i = None
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        try:
            cur = json.loads(row["controller_state"] or "{}") if row is not None else {}
        except ValueError:
            cur = {}
        if not isinstance(cur, dict):
            cur = {}
        if not isinstance(cur.get("pending_recovery"), dict):
            con.execute("ROLLBACK")
            return
        cur.pop("pending_recovery", None)
        cur["last_recovery_link"] = {"failed_seq": failed_i,
                                     "next_seq": next_i,
                                     "route": route,
                                     "linked_at": core._utcnow()}
        con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                    (json.dumps(cur, sort_keys=True), core._utcnow(), request_id))
        core._event(con, request_id, "recovery_next_attempt",
                    {"failed_seq": failed_i, "next_seq": next_i,
                     "route": route})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def _fail_offline(state_dir, request_id: str, error: dict) -> None:
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        cur = con.execute("SELECT status FROM jobs WHERE request_id=?", (request_id,)).fetchone()
        if cur is None or cur["status"] in store.TERMINAL:
            con.execute("ROLLBACK")
            return
        con.execute("UPDATE jobs SET status='failed', error_class=?, result_json=?, updated_at=?"
                    " WHERE request_id=?",
                    ("escalation_exhausted", json.dumps({"ok": False, "error": error}), core._utcnow(),
                     request_id))
        core._event(con, request_id, "failed", {"reason": "escalation_exhausted"})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    core._mirror_result(state_dir, request_id, json.dumps({"request_id": request_id, "status": "failed",
                                                          "result": {"ok": False, "error": error}}))


def _record_turn_outcome(state_dir, request_id: str, impl: dict) -> None:
    """A turn counts as failed when the worker or provider errored or the
    task's proof failed. Success does not reset the ladder. Counting is
    idempotent per dispatcher turn (seq): a recovered controller that
    reuses the same implementation invocation must not count the same
    failure twice and skip the correction rung. When the controller state
    lost its seq, the turn's own report and invocation metadata supply it;
    without any seq the turn still counts (no dedup key exists)."""
    report = impl.get("report") if isinstance(impl.get("report"), dict) else {}
    proof_rc = report.get("proof_exit_code")
    failed = impl.get("action") == "implementation_failed" or (proof_rc is not None and proof_rc != 0)
    # Preserve the eventual result link for a bound recovery attempt:
    # Observer joins recovery_next_attempt to this outcome by seq.
    try:
        _record_recovery_result(state_dir, request_id, report, failed)
    except Exception:
        pass
    if not failed:
        return
    job = core.get_job(state_dir, request_id)
    seq = _load_controller_state(job).get("seq")
    if seq is None and isinstance(report.get("seq"), int):
        seq = report.get("seq")
    if seq is None:
        seq = _latest_invocation_seq(state_dir, request_id)
    ladder = _ladder(job)
    if seq is not None and ladder.get("counted_seq") == seq:
        return
    ladder["failures"] += 1
    if seq is not None:
        ladder["counted_seq"] = seq
    _set_phase(state_dir, request_id, ladder=ladder)


def _record_recovery_result(state_dir, request_id: str, report: dict,
                              failed: bool) -> None:
    """Emit ``recovery_attempt_result`` for a bound recovery attempt.

    Joins the earlier ``recovery_next_attempt`` link to this attempt's
    outcome (ok or failed) by seq, so the eventual result is preserved
    on the ledger instead of inferred. No-op without a stored link for
    this report's seq. Never raises past the caller.
    """
    try:
        rep_seq = report.get("seq")
        next_i = int(rep_seq) if rep_seq is not None else None
    except (TypeError, ValueError):
        return
    if next_i is None:
        return
    try:
        job = core.get_job(state_dir, request_id)
    except Exception:
        return
    link = _load_controller_state(job).get("last_recovery_link")
    if not isinstance(link, dict):
        return
    try:
        linked = int(link.get("next_seq")) if link.get("next_seq") is not None else None
    except (TypeError, ValueError):
        return
    if linked != next_i:
        return
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        _lease_guard(con, request_id)
        core._event(con, request_id, "recovery_attempt_result",
                    {"failed_seq": link.get("failed_seq"),
                     "next_seq": next_i,
                     "outcome": "failed" if failed else "ok"})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def _latest_invocation_seq(state_dir, request_id: str) -> int | None:
    """Newest worker-turn invocation seq, for a controller whose own state
    lost its seq but whose turn already started."""
    worker_kinds = harnesses.worker_control_kinds()
    try:
        invs = core._list_invocations(state_dir, request_id)
    except Exception:
        return None
    for inv in reversed(invs):
        if inv.get("kind") not in worker_kinds:
            continue
        try:
            meta = json.loads(inv.get("meta_json") or "{}")
        except ValueError:
            continue
        seq = meta.get("seq")
        if isinstance(seq, int):
            return seq
    return None


def _consume_planner_authorized_attempt(state_dir, request_id: str,
                                          envelope: dict,
                                          token: str | None = None) -> bool:
    """Run one explicitly authorized planner-directed recovery attempt.

    After recovery exhaustion the planner's answer permits exactly one
    additional bounded dispatcher-owned attempt: an approach-only answer
    runs once on the current route, while an eligible ``directed_route``
    switches there with reason ``planner_directed``. The authorization
    stays reserved (``planner_recovery_in_flight``) through preflight,
    pool, and other non-terminal capacity or routing transitions, and is
    consumed (``planner_recovery_used``) only when the authorized
    dispatcher-owned attempt reaches a genuine ``implementation_ok`` or
    ``implementation_failed`` outcome, including a real verification
    failure after the worker turn. A preflight move, pool transfer,
    stalled retry, or other non-result transition never consumes it.
    Records the recovery decision once, linking the failed attempt
    onward; continuing the same authorized attempt after a capacity move
    never re-posts the planner question nor records a second decision.
    Returns True when the caller should run the turn directly instead of
    the ordinary ladder path. A missing authorization, a used
    authorization, or a rejected route returns False so the ladder path
    applies unchanged: no unlimited retries.
    """
    try:
        job_now = core.get_job(state_dir, request_id)
        st = _load_controller_state(job_now)
    except Exception:
        return False
    if not st.get("planner_recovery_authorized") or st.get("planner_recovery_used"):
        return False
    if not isinstance(envelope, dict):
        return False
    if st.get("planner_recovery_in_flight"):
        # Continuing the same authorized attempt after a preflight or
        # capacity move: the decision is already recorded and the pending
        # link is still reserved for the actual worker invocation. Run
        # directly without a second decision or question.
        return True
    directed = envelope.get("directed_route")
    current_route = job_now.get("route")
    if not isinstance(directed, str) or not directed.strip():
        # Approach-only direction: one authorized attempt on the current
        # route, without re-posting the recovery question.
        target = current_route
    elif directed.strip() == current_route:
        # An explicit direction naming the current route needs no switch
        # but still consumes the single authorization.
        target = current_route
    else:
        assigned = _apply_planner_directed_route(state_dir, request_id, envelope)
        if not assigned:
            return False
        target = directed.strip()
    try:
        ladder = _ladder(core.get_job(state_dir, request_id))
    except Exception:
        ladder = {"failures": 0, "rung": "initial", "escalated": False}
    try:
        _set_phase(state_dir, request_id, planner_recovery_in_flight=True,
                   ladder={**ladder, "rung": "recovery_directed",
                           "escalated": True})
    except core.LeaseLostError:
        raise
    except Exception:
        pass
    try:
        core.record_recovery_decision(
            state_dir, request_id, int(ladder.get("failures") or 0),
            "recovery_directed", target,
            ladder.get("counted_seq"), "planner_directed",
            lease_token=token)
    except core.LeaseLostError:
        raise
    except Exception:
        pass
    try:
        _set_phase(state_dir, request_id,
                   pending_recovery={"failed_seq": ladder.get("counted_seq")})
    except Exception:
        pass
    return True


def _handle_implementation_action(state_dir, request_id: str, envelope: dict,
                                  run_cmd, token: str | None = None) -> dict:
    """One bounded implementation transition: worker turn then Luna resume.

    The escalation ladder chooses the rung first and stays authoritative:
    an ordinary envelope route never moves the job. An explicit planner
    direction relayed as ``directed_route`` is assigned under the routing
    policy (or rejected with evidence) before the turn runs. After
    recovery exhaustion the planner's answer permits exactly one
    explicitly authorized directed attempt; the submitted candidate stays
    dispatcher-owned throughout.
    """
    artifact = envelope.get("artifact")
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else None
    if _consume_planner_authorized_attempt(state_dir, request_id, envelope,
                                           token):
        impl = run_implementation(state_dir, request_id, artifact=artifact,
                                   payload=payload, run_cmd=run_cmd)
        if impl.get("action") not in ("implementation_ok", "implementation_failed"):
            # A preflight route move, pool transfer, stalled retry, or
            # other non-result transition: the authorization stays in
            # flight for the actual directed attempt, with the same job
            # and Observer identities and no second planner question or
            # recovery decision. The next step continues the same attempt.
            return impl
        # The authorized dispatcher-owned attempt reached a genuine
        # result (including a real verification failure after the worker
        # turn): consume the single use now, never before.
        try:
            _set_phase(state_dir, request_id, planner_recovery_used=True,
                       planner_recovery_in_flight=False)
        except core.LeaseLostError:
            raise
        except Exception:
            pass
        _record_turn_outcome(state_dir, request_id, impl)
        rep = impl.get("report") if isinstance(impl.get("report"), dict) else {}
        proof_rc = rep.get("proof_exit_code")
        failed = impl.get("action") == "implementation_failed" or (
            proof_rc is not None and proof_rc != 0)
        if not failed:
            # A successful directed attempt continues to Luna with its
            # exact-candidate evidence; only a failure after the one
            # authorized attempt ends terminally with evidence.
            job = core.get_job(state_dir, request_id)
            evidence = _implementation_evidence(job, impl)
            r = resume_luna(state_dir, request_id, evidence, run_cmd=run_cmd,
                            label="IMPLEMENTATION RESULT")
            if r.get("action") == "blocked":
                return r
            return {"action": "implementation-resumed", "luna_action": r.get("luna_action")}
        exhausted = _apply_ladder(state_dir, request_id, token)
        if exhausted is not None:
            return exhausted
        job = core.get_job(state_dir, request_id)
        evidence = _implementation_evidence(job, impl)
        r = resume_luna(state_dir, request_id, evidence, run_cmd=run_cmd,
                        label="IMPLEMENTATION RESULT")
        if r.get("action") == "blocked":
            return r
        return {"action": "implementation-resumed", "luna_action": r.get("luna_action")}
    ended = _apply_ladder(state_dir, request_id, token)
    if ended is not None:
        return ended
    _apply_planner_directed_route(state_dir, request_id, envelope)
    impl = run_implementation(state_dir, request_id, artifact=artifact,
                               payload=payload, run_cmd=run_cmd)
    if impl.get("action") not in ("implementation_ok", "implementation_failed"):
        # transferred_to_go, route_switched, stalled_retry, or blocked: the
        # next step retries on the same or the new route, or stops; never
        # fork a second writer here.
        return impl
    _record_turn_outcome(state_dir, request_id, impl)
    # The recovery turn just failed: the job is exhausted. End it here
    # with its evidence for the planner instead of resuming Luna, whose
    # completion envelope must never succeed an exhausted escalation.
    exhausted = _apply_ladder(state_dir, request_id, token)
    if exhausted is not None:
        return exhausted
    job = core.get_job(state_dir, request_id)
    evidence = _implementation_evidence(job, impl)
    r = resume_luna(state_dir, request_id, evidence, run_cmd=run_cmd,
                    label="IMPLEMENTATION RESULT")
    if r.get("action") == "blocked":
        return r
    return {"action": "implementation-resumed", "luna_action": r.get("luna_action")}


def step(state_dir, request_id: str, run_cmd=None, token: str | None = None) -> dict:
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
        return _handle_implementation_action(state_dir, request_id, last, run_cmd, token=token)
    if action_name == "completion":
        # A job can never succeed while its last implementation turn's
        # proof failed: refuse the envelope, hand the evidence back once,
        # and block if the dispatcher insists. The bound proof must cover
        # the current candidate (no skipped or stale proof), named
        # acceptance evidence must travel in the envelope, and an ordinary
        # completion in a workspace with an origin push remote needs an
        # opened PR URL that exists, is open, and points at the candidate
        # commit (a draft only when the task explicitly allows it).
        # Without a remote it succeeds as before, experiment and replay
        # jobs keep their behavior. One job owns one PR: a different URL
        # than the preserved identity refuses as a duplicate.
        _latest = core.latest_turn_report(state_dir, request_id)
        _refusal = core.completion_refusal_reason(_latest)
        if _refusal is None:
            _refusal = core.incomplete_proof_reason(
                job.get("task_json"), _latest, job.get("workspace"))
        if _refusal is None:
            _refusal = core.incomplete_acceptance_reason(
                job.get("task_json"), last)
        if _refusal is None:
            _refusal = core.missing_pr_url_reason(
                job.get("job_kind"), last, job.get("workspace"))
        if _refusal is None and core.pr_gate_applies(job.get("job_kind"),
                                                     job.get("workspace")):
            _pr = last.get("pr_url") if isinstance(last, dict) else None
            if isinstance(_pr, str) and _pr.strip():
                _refusal = core.duplicate_pr_reason(
                    core.known_pr_url(state_dir, request_id), last)
                if _refusal is None:
                    # Verify live before preserving: an invalid first URL
                    # never becomes the job's identity, so a corrected URL
                    # is accepted instead of refused as a duplicate.
                    _head = _latest.get("head_commit") \
                        if isinstance(_latest, dict) else None
                    _refusal = core.verify_pr_for_completion(
                        job.get("workspace"), _pr.strip(), _head,
                        job.get("task_json"))
                    if _refusal is None:
                        core.record_known_pr(state_dir, request_id, _pr.strip(),
                                             lease_token=_LEASE["token"])
        if _refusal is not None:
            return _handle_completion_refusal(state_dir, request_id, last,
                                              _latest if isinstance(_latest, dict) else {},
                                              _refusal, run_cmd)
        return _complete_job(state_dir, request_id, token,
                             str(last.get("output") or "done"),
                             last.get("artifact"), last.get("pr_url"),
                             last.get("acceptance_evidence"))
    # review/unknown: durable block, never spin.
    _mark_blocked(state_dir, request_id, f"unsupported_luna_action: {action_name}")
    return {"action": "blocked", "reason": "unsupported_luna_action"}


def _count_step(state_dir, request_id: str) -> int:
    """Steps taken by every launch of this job; the per-launch budget can
    be renewed by `recover`, the job budget cannot."""
    job = core.get_job(state_dir, request_id)
    st = _load_controller_state(job)
    total = int(st.get("steps_total") or 0) + 1
    # Persist before stepping: a lost write must surface as a controller
    # error (which blocks) rather than silently resetting the cumulative
    # per-job budget across recovery. Lease loss still yields to the owner.
    _set_phase(state_dir, request_id, steps_total=total)
    return total


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


def run_controller_process(state_dir: str, request_id: str, token: str) -> int:
    """Detached controller entry point (stdlib only, no secrets logged).

    ``run-once`` drives the bounded state machine with built-in
    adapters: at most ``MAX_LOOP_STEPS`` single-transition :func:`step`
    calls (dispatch -> question -> implementation -> completion),
    stopping on terminal/blocked-awaiting states.
    """
    import time as _time

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
                  "implementation-resumed", "completion-refused-resumed",
                  "transferred_to_go", "route_switched",
                  "stalled_retry")
    for _ in range(MAX_LOOP_STEPS):
        try:
            total = _count_step(state_dir, request_id)
            if total > core.MAX_JOB_STEPS:
                _mark_blocked(state_dir, request_id,
                              f"job_step_budget_exhausted after {total} steps across launches")
                break
            res = step(state_dir, request_id, run_cmd=durable_run_cmd, token=token)
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
    args = ap.parse_args(argv)
    import os
    token = args.token or os.environ.pop("DURABLE_RUNNER_TOKEN", "")
    if not token:
        ap.error("missing lease token")
    return run_controller_process(args.state_dir, args.request_id, token)


if __name__ == "__main__":
    raise SystemExit(main())
