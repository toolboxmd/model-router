"""Independent durable child supervisor. Stdlib only.

Owns spawn for one invocation and survives controller death. Persists
pid/pgid/process-start after Popen, waits for the child, then writes
rc/session/result to the invocation row. Never logs credentials.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def _utcnow() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def process_start_identity(pid: int | None) -> str | None:
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
        ident = (out or "").strip()
        return ident or None
    except Exception:
        return None


def _pgid_for_pid(pid: int | None) -> int | None:
    if pid is None:
        return None
    try:
        return os.getpgid(int(pid))
    except (ProcessLookupError, OSError, ValueError):
        return None


def _read_paths(stdout_path: str, stderr_path: str) -> tuple[str, str]:
    out = err = ""
    try:
        out = Path(stdout_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    try:
        err = Path(stderr_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        pass
    return out, err


def supervise_invocation(state_dir: str, request_id: str, invocation_id: str) -> int:
    from . import adapters, core, store

    con = store.connect(state_dir)
    try:
        row = con.execute(
            "SELECT * FROM invocations WHERE invocation_id=?", (invocation_id,)
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return 2
    inv = dict(row)
    if inv.get("request_id") != request_id:
        return 2
    try:
        cmd = json.loads(inv["cmd_json"] or "[]")
    except ValueError:
        cmd = []
    if not isinstance(cmd, list) or not cmd:
        _finish(state_dir, invocation_id, request_id, 127, "failed", None, None,
                extra_result={"error": "missing command"})
        return 127
    stdout_path = inv["stdout_path"]
    stderr_path = inv["stderr_path"]
    workspace = inv.get("workspace")
    timeout = inv.get("timeout_secs")
    try:
        timeout_f = float(timeout) if timeout is not None else 120.0
    except (TypeError, ValueError):
        timeout_f = 120.0
    meta = {}
    if inv.get("meta_json"):
        try:
            meta = json.loads(inv["meta_json"]) or {}
        except ValueError:
            meta = {}
    kind = inv.get("kind") or "unknown"
    env = adapters.child_harness_env()
    extra_env = meta.get("env") if isinstance(meta.get("env"), dict) else {}
    # Only pass through non-secret test PATH/fixture keys plus any
    # already-present child environment. Passwords stay in this process
    # memory / child env and are never written back to meta/DB.
    for k, v in extra_env.items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        lk = k.lower()
        if any(s in lk for s in ("password", "secret", "token", "api_key", "credential")):
            env[k] = v
            continue
        env[k] = v
    generated_password = None
    if kind in ("opencode_serve", "opencode_control"):
        generated_password = adapters.generate_control_password()
        env["OPENCODE_SERVER_PASSWORD"] = generated_password

    try:
        out_f = open(stdout_path, "a", encoding="utf-8")
        err_f = open(stderr_path, "a", encoding="utf-8")
        proc = subprocess.Popen(
            [str(c) for c in cmd],
            cwd=workspace or None,
            stdout=out_f, stderr=err_f,
            stdin=subprocess.DEVNULL,
            start_new_session=True, close_fds=True,
            env=env,
        )
    except OSError as e:
        try:
            out_f.close()
        except Exception:
            pass
        try:
            err_f.close()
        except Exception:
            pass
        _finish(state_dir, invocation_id, request_id, 127, "failed", None, None,
                extra_result={"error": f"spawn failed: {e}"})
        return 127

    pid = proc.pid
    pgid = _pgid_for_pid(pid)
    start_id = process_start_identity(pid)
    sup_pid = os.getpid()
    try:
        sup_pgid = os.getpgid(sup_pid)
    except OSError:
        sup_pgid = None
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "UPDATE invocations SET pid=?, pgid=?, process_start=?,"
            " supervisor_pid=?, supervisor_pgid=?, supervisor_start=? WHERE invocation_id=?",
            (pid, pgid, start_id, sup_pid, sup_pgid,
             process_start_identity(sup_pid), invocation_id),
        )
        core._event(con, request_id, "invocation_spawned",
                    {"invocation_id": invocation_id[:16], "kind": kind,
                     "pid": pid, "pgid": pgid})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        try:
            proc.terminate()
        except Exception:
            pass
        raise
    finally:
        con.close()

    captured_session = None
    captured_kind = None
    deadline = time.monotonic() + max(1.0, timeout_f)
    job = None
    try:
        job = core.get_job(state_dir, request_id)
    except Exception:
        job = None
    control_result = None
    try:
        if kind == "opencode_control":
            try:
                control_result, captured_session, captured_kind = _drive_opencode_control(
                    state_dir, request_id, invocation_id, proc,
                    stdout_path, stderr_path, generated_password, meta, job,
                    deadline, workspace)
            except Exception as e:  # noqa: BLE001 - recorded, never silent
                control_result = {"ok": False, "rc": 1,
                                  "error": f"control failed: {type(e).__name__}: {str(e)[:300]}"}
            rc = 0 if (control_result or {}).get("ok") else int((control_result or {}).get("rc") or 1)
            try:
                os.killpg(int(pgid or pid), 15)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                proc.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(int(pgid or pid), 9)
                except Exception:
                    pass
        else:
            while True:
                rc = proc.poll()
                stdout_acc, stderr_acc = _read_paths(stdout_path, stderr_path)
                if captured_session is None:
                    sid, skind = core._parse_session_from_output(
                        kind, stdout_acc, stderr_acc, job)
                    if sid:
                        captured_session, captured_kind = sid, skind
                        _persist_session(state_dir, request_id, invocation_id,
                                         sid, skind, job)
                if rc is not None:
                    break
                if time.monotonic() > deadline:
                    try:
                        os.killpg(int(pgid or pid), 9)
                    except Exception:
                        try:
                            proc.kill()
                        except Exception:
                            pass
                    try:
                        rc = proc.wait(timeout=5)
                    except Exception:
                        rc = 124
                    break
                time.sleep(0.05)
    finally:
        try:
            out_f.close()
        except Exception:
            pass
        try:
            err_f.close()
        except Exception:
            pass

    stdout_text, stderr_text = _read_paths(stdout_path, stderr_path)
    if captured_session is None:
        sid, skind = core._parse_session_from_output(
            kind, stdout_text, stderr_text, job)
        if sid:
            captured_session, captured_kind = sid, skind
            _persist_session(state_dir, request_id, invocation_id, sid, skind, job)

    envelope = control_result
    if envelope is None and kind in ("codex_dispatch", "codex_resume"):
        envelope = adapters.parse_codex_agent_envelope(
            stdout_text, adapters.read_last_message_file(
                adapters.last_message_path_from_cmd(cmd)))

    result = {
        "rc": rc,
        "session_id": captured_session,
        "session_kind": captured_kind,
        "envelope": envelope,
    }
    state = "completed" if rc == 0 else "failed"
    _finish(state_dir, invocation_id, request_id, rc, state,
            captured_session, captured_kind, extra_result=result)

    if kind in ("opencode_serve", "opencode_control") and generated_password:
        # Password stays out of DB/logs; drop local reference.
        generated_password = None
    return 0 if rc == 0 else int(rc or 1)


def _drive_opencode_control(state_dir, request_id, invocation_id, proc,
                            stdout_path, stderr_path, password, meta, job,
                            deadline, workspace):
    """Drive one implementation turn on the owned ``opencode serve``.

    Save the session before the model request, prompt asynchronously,
    and observe only that session. Free exhaustion requires trusted
    server evidence; the session is aborted and confirmed idle before
    the result allows a Go transfer.
    """
    from . import adapters
    meta = meta if isinstance(meta, dict) else {}
    base_url = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return {"ok": False, "rc": proc.returncode or 1,
                    "error": "opencode serve exited before emitting a URL"}, None, None
        out, err = _read_paths(stdout_path, stderr_path)
        base_url = adapters.parse_serve_url(out + "\n" + err)
        if base_url:
            break
        time.sleep(0.05)
    if not base_url:
        return {"ok": False, "rc": 124, "error": "opencode serve did not emit a localhost URL"}, None, None
    client = adapters.OpenCodeClient(base_url, password, directory=workspace)
    health_err = None
    while time.monotonic() < deadline:
        try:
            client.health()
            health_err = None
            break
        except Exception as e:  # noqa: BLE001
            health_err = f"{type(e).__name__}: {str(e)[:200]}"
            time.sleep(0.2)
    if health_err:
        return {"ok": False, "rc": 1, "error": f"opencode health failed: {health_err}"}, None, None
    saved = meta.get("session_id") or None
    if not saved:
        created = client.create_session(title=f"model-router runner {request_id}")
        saved = client.session_id_from(created)
        if not saved:
            return {"ok": False, "rc": 1,
                    "error": "opencode session create returned no id"}, None, None
    _persist_session(state_dir, request_id, invocation_id, saved,
                     "opencode_session_id", job)
    allowance = meta.get("allowance") or "free"
    model = meta.get("model") or adapters.opencode_model_for_allowance(allowance)
    baseline = set()
    for m in client.messages(saved):
        info = m.get("info") if isinstance(m, dict) else None
        if isinstance(info, dict) and info.get("id"):
            baseline.add(info["id"])
    client.prompt_async(saved, meta.get("prompt") or "", model=model)
    result = {"opencode_session_id": saved, "model": model,
              "variant": adapters.OPENCODE_VARIANT, "agent": adapters.OPENCODE_AGENT,
              "ok": False}
    prompted_at = time.monotonic()
    seen_active = False

    def abort_and_confirm():
        try:
            client.abort(saved)
        except Exception as e:  # noqa: BLE001
            result["abort_error"] = f"{type(e).__name__}: {str(e)[:200]}"
        try:
            idle = client.wait_idle(saved, timeout=30.0)
        except Exception as e:  # noqa: BLE001
            idle = {"idle": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}
        result["idle_confirmed"] = bool(idle.get("idle"))
        result["idle_status"] = adapters.redact_nested(idle.get("status"))

    while True:
        if proc.poll() is not None:
            result.update(rc=1, error="opencode serve exited during the turn")
            break
        status = client.session_status(saved)
        evidence = adapters.trusted_free_exhaustion(status=status)
        if evidence:
            result.update(rc=3, quota=True, free_exhaustion_evidence=evidence)
            abort_and_confirm()
            break
        typ = status.get("type")
        if typ in ("busy", "retry"):
            seen_active = True
            if typ == "retry":
                result["last_retry"] = adapters.redact_nested(status)
        else:
            new = adapters.assistant_messages_after(client.messages(saved), baseline)
            if new:
                last = new[-1]
                info = last.get("info") or {}
                if (info.get("time") or {}).get("completed") or info.get("error"):
                    err = info.get("error")
                    evidence = adapters.trusted_free_exhaustion(message_error=err)
                    texts = [adapters.message_text(m) for m in new]
                    result["assistant_text"] = next(
                        (t for t in reversed(texts) if t.strip()), "")[-8000:]
                    result["finish"] = info.get("finish")
                    result["assistant_messages"] = len(new)
                    result["actual_model"] = {"providerID": info.get("providerID"),
                                              "modelID": info.get("modelID"),
                                              "variant": info.get("variant")}
                    if evidence:
                        # The session already ended idle with this error.
                        result.update(rc=3, quota=True, free_exhaustion_evidence=evidence,
                                      idle_confirmed=True)
                    elif err:
                        result.update(rc=1, error=adapters.redact_nested(err))
                    else:
                        result.update(rc=0, ok=True)
                    break
            elif not seen_active and time.monotonic() - prompted_at > 60:
                result.update(rc=1, error="prompt was not accepted within 60 seconds")
                abort_and_confirm()
                break
        if time.monotonic() > deadline:
            result.update(rc=124, error="implementation turn timed out")
            abort_and_confirm()
            break
        time.sleep(1.0)
    summary = {k: result.get(k) for k in (
        "opencode_session_id", "ok", "rc", "quota", "idle_confirmed", "finish",
        "actual_model", "error", "free_exhaustion_evidence")}
    try:
        with open(stdout_path, "a", encoding="utf-8") as f:
            f.write("\nRUNNER_RESULT " + json.dumps(summary, sort_keys=True) + "\n")
    except OSError:
        pass
    return result, saved, "opencode_session_id"


def _persist_session(state_dir, request_id, invocation_id, sid, skind, job) -> None:
    from . import adapters, core, store
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(
            "UPDATE invocations SET session_id=?, session_kind=? WHERE invocation_id=?",
            (sid, skind, invocation_id),
        )
        if skind == "codex_task_id":
            con.execute(
                "UPDATE jobs SET codex_task_id=?, adapter='codex', model=?, effort=?, updated_at=? WHERE request_id=?",
                (sid, adapters.CODEX_MODEL, adapters.CODEX_EFFORT, core._utcnow(), request_id),
            )
        elif skind == "opencode_session_id":
            row = con.execute("SELECT route FROM jobs WHERE request_id=?",
                              (request_id,)).fetchone()
            route = (row["route"] if row is not None else None) or "muse-spark-xhigh-free"
            con.execute(
                "UPDATE jobs SET opencode_session_id=?, adapter='opencode', model=?, effort=?, updated_at=? WHERE request_id=?",
                (sid, adapters.opencode_model_for_route(route), adapters.OPENCODE_VARIANT,
                 core._utcnow(), request_id),
            )
        elif skind == "planner_session_id":
            con.execute(
                "UPDATE jobs SET planner_session_id=?, updated_at=? WHERE request_id=?",
                (sid, core._utcnow(), request_id),
            )
        core._event(con, request_id, "invocation_session_captured",
                    {"invocation_id": invocation_id[:16],
                     "session_kind": skind, "session": sid[:24]})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def _finish(state_dir, invocation_id, request_id, rc, state,
            session_id, session_kind, extra_result=None) -> None:
    from . import core, store
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        payload = extra_result if isinstance(extra_result, dict) else {"rc": rc}
        con.execute(
            "UPDATE invocations SET rc=?, state=?, ended_at=?, result_json=?,"
            " session_id=COALESCE(?, session_id), session_kind=COALESCE(?, session_kind)"
            " WHERE invocation_id=?",
            (rc, state, core._utcnow(), json.dumps(payload, sort_keys=True),
             session_id, session_kind, invocation_id),
        )
        core._event(con, request_id, "invocation_finished",
                    {"invocation_id": invocation_id[:16], "rc": rc, "state": state})
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="runner.supervisor")
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--request-id", required=True)
    ap.add_argument("--invocation-id", required=True)
    args = ap.parse_args(argv)
    return supervise_invocation(args.state_dir, args.request_id, args.invocation_id)


if __name__ == "__main__":
    raise SystemExit(main())
