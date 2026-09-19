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


def _prepare_spawn(inv: dict, adapters):
    try:
        cmd = json.loads(inv["cmd_json"] or "[]")
    except ValueError:
        cmd = []
    if not isinstance(cmd, list) or not cmd:
        return None
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
    generated_password = None
    if kind in ("opencode_serve", "opencode_control"):
        generated_password = adapters.generate_control_password()
        env["OPENCODE_SERVER_PASSWORD"] = generated_password
    return (cmd, inv["stdout_path"], inv["stderr_path"], inv.get("workspace"), timeout_f,
            meta, kind, env, generated_password)


def child_record_path(stdout_path: str) -> str:
    base = str(stdout_path)
    return (base[:-len(".stdout")] if base.endswith(".stdout") else base) + ".child.json"


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


# Termination requests are deferred until the child's PID is committed,
# then honored by stopping the child, so no child can run unrecorded.
_STOP = {"requested": False}


def _defer_stop(signum, frame):  # noqa: ARG001
    _STOP["requested"] = True


def supervise_invocation(state_dir: str, request_id: str, invocation_id: str) -> int:
    import signal as _signal
    from . import adapters, core, store

    for sig in (_signal.SIGTERM, _signal.SIGINT, _signal.SIGHUP):
        _signal.signal(sig, _defer_stop)

    # Claim the row before any spawn, under the write lock. A cancelled
    # job, a lost lease, or a row already closed never gets a child.
    my_pid = os.getpid()
    con = store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT * FROM invocations WHERE invocation_id=?", (invocation_id,)
        ).fetchone()
        job_row = con.execute("SELECT owner_token, cancel_requested, status FROM jobs"
                              " WHERE request_id=?", (request_id,)).fetchone()
        if row is None or row["request_id"] != request_id or job_row is None:
            con.execute("ROLLBACK")
            return 2
        refuse = None
        if _STOP["requested"]:
            refuse = "termination requested before spawn"
        elif row["state"] != "running" or row["supervisor_pid"] not in (None, my_pid):
            refuse = "invocation already closed or claimed"
        elif job_row["cancel_requested"] or job_row["status"] in store.TERMINAL:
            refuse = "job cancelled or terminal before spawn"
        elif job_row["owner_token"] != row["owner_token"]:
            refuse = "controller lost the lease before spawn"
        if refuse:
            if row["state"] in ("running", "cancelling") and row["pid"] is None \
                    and row["supervisor_pid"] in (None, my_pid):
                con.execute("UPDATE invocations SET state='abandoned', rc=125, ended_at=?, consumed_at=?,"
                            " result_json=? WHERE invocation_id=?",
                            (core._utcnow(), core._utcnow(), json.dumps({"error": refuse}),
                             invocation_id))
                core._event(con, request_id, "invocation_refused",
                            {"invocation_id": invocation_id[:16], "reason": refuse})
                con.execute("COMMIT")
            else:
                con.execute("ROLLBACK")
            return 3
        try:
            my_pgid = os.getpgid(my_pid)
        except OSError:
            my_pgid = None
        con.execute("UPDATE invocations SET supervisor_pid=?, supervisor_pgid=?, supervisor_start=?"
                    " WHERE invocation_id=?",
                    (my_pid, my_pgid, process_start_identity(my_pid), invocation_id))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()
    inv = dict(row)
    try:
        prepared = _prepare_spawn(inv, adapters)
    except Exception as e:  # noqa: BLE001 - a claimed row is always closed
        _finish(state_dir, invocation_id, request_id, 127, "failed", None, None,
                extra_result={"error": f"prepare failed: {type(e).__name__}: {str(e)[:200]}"})
        return 127
    if prepared is None:
        _finish(state_dir, invocation_id, request_id, 127, "failed", None, None,
                extra_result={"error": "missing command"})
        return 127
    cmd, stdout_path, stderr_path, workspace, timeout_f, meta, kind, env, generated_password = prepared

    if _STOP["requested"]:
        _finish(state_dir, invocation_id, request_id, 143, "failed", None, None,
                extra_result={"error": "terminated before spawn"})
        return 143
    side = os.path.abspath(child_record_path(stdout_path))  # the child chdirs first

    def _child_setup():
        # Runs in the child before exec: every child that can run a model
        # has a side record, so a dead supervisor without one never
        # started a child.
        me = os.getpid()
        fd = os.open(side, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, ('{"pid": %d, "pgid": %d}' % (me, me)).encode("ascii"))
        finally:
            os.close(fd)

    out_f = err_f = None
    try:
        out_f = open(stdout_path, "a", encoding="utf-8")
        err_f = open(stderr_path, "a", encoding="utf-8")
        proc = subprocess.Popen(
            [str(c).replace("\x00", "") for c in cmd],
            cwd=workspace or None,
            stdout=out_f, stderr=err_f,
            stdin=subprocess.DEVNULL,
            start_new_session=True, close_fds=True,
            env=env, preexec_fn=_child_setup,
        )
    except Exception as e:  # noqa: BLE001
        for f in (out_f, err_f):
            try:
                if f is not None:
                    f.close()
            except Exception:
                pass
        _finish(state_dir, invocation_id, request_id, 127, "failed", None, None,
                extra_result={"error": f"spawn failed: {type(e).__name__}: {str(e)[:200]}"})
        return 127

    pid = proc.pid
    pgid = pid  # start_new_session makes the child its own group leader
    # The child wrote its side record before exec; add its start identity
    # before the database commit.
    try:
        store.secure_write_text(Path(side), json.dumps({"pid": pid, "pgid": pgid}))
    except OSError:
        pass
    start_id = process_start_identity(pid)
    try:
        store.secure_write_text(Path(side), json.dumps({"pid": pid, "pgid": pgid, "start": start_id}))
    except OSError:
        pass
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
    except Exception as e:  # noqa: BLE001
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        # The child cannot be recorded: stop its whole group, then close the
        # row as failed so recovery never sees an unrecorded child.
        try:
            os.killpg(pgid, _signal.SIGKILL)
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
        con.close()
        _finish(state_dir, invocation_id, request_id, 1, "failed", None, None,
                extra_result={"error": f"child record failed: {type(e).__name__}"})
        return 1
    finally:
        try:
            con.close()
        except Exception:
            pass

    def stop_child():
        try:
            os.killpg(int(pgid or pid), _signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

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
            stop_sent = False
            while True:
                if _STOP["requested"] and not stop_sent:
                    stop_child()
                    stop_sent = True
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
        if _STOP["requested"]:
            return {"ok": False, "rc": 143, "error": "terminated during server startup"}, None, None
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
        if _STOP["requested"]:
            return {"ok": False, "rc": 143, "error": "terminated during server startup"}, None, None
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
    # Variant and agent come from the route through the controller's meta;
    # a missing key keeps the legacy Muse defaults, an explicit None omits the
    # variant so the provider default applies.
    variant = meta["variant"] if "variant" in meta else adapters.OPENCODE_VARIANT
    agent = meta.get("agent") or adapters.OPENCODE_AGENT
    baseline = set()
    for m in client.messages(saved):
        info = m.get("info") if isinstance(m, dict) else None
        if isinstance(info, dict) and info.get("id"):
            baseline.add(info["id"])
    if _STOP["requested"]:
        return {"ok": False, "rc": 143, "error": "terminated before the prompt",
                "opencode_session_id": saved}, saved, "opencode_session_id"
    client.prompt_async(saved, meta.get("prompt") or "", model=model, variant=variant, agent=agent)
    result = {"opencode_session_id": saved, "model": model,
              "variant": variant, "agent": agent,
              "ok": False}
    prompted_at = time.monotonic()
    seen_active = False
    from . import policy as _policy
    overload_spec = _policy.SIGNAL_CLASSES["overloaded"]
    overload_first = None
    overload_attempts = 0

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
        if _STOP["requested"]:
            result.update(rc=143, error="terminated by cancellation or timeout")
            abort_and_confirm()
            break
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
                cls = _policy.classify_signal(status)
                if cls == "exhausted":
                    # Exhaustion on a paid pool (for example GoUsageLimitError):
                    # zero retries, abort, confirm idle, let the controller move pools.
                    result.update(rc=3, quota=True, signal="exhausted",
                                  signal_evidence=adapters.redact_nested(status),
                                  error="provider allowance exhausted")
                    abort_and_confirm()
                    break
                if cls == "overloaded":
                    # The provider retries on its own; allow the policy's bounded
                    # attempts and window, then abort and move to another family.
                    overload_first = overload_first or time.monotonic()
                    try:
                        reported = int(status.get("attempt") or 0)
                    except (TypeError, ValueError):
                        reported = 0
                    overload_attempts = max(overload_attempts + 1, reported)
                    if overload_attempts > overload_spec["retries"] \
                            or time.monotonic() - overload_first > overload_spec["window_secs"]:
                        result.update(rc=4, signal="overloaded",
                                      signal_evidence=adapters.redact_nested(status),
                                      error="provider overloaded")
                        abort_and_confirm()
                        break
                elif cls == "hard":
                    result.update(rc=1, signal="hard",
                                  signal_evidence=adapters.redact_nested(status),
                                  error="hard provider error")
                    abort_and_confirm()
                    break
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
                    # Counters verbatim per assistant message, never folded,
                    # plus the native message identities for deduplication.
                    all_msgs = client.messages(saved)
                    user_ids = [m["info"].get("id") for m in all_msgs
                                if isinstance(m, dict) and isinstance(m.get("info"), dict)
                                and m["info"].get("role") == "user"
                                and m["info"].get("id") not in baseline]
                    result["usage"] = {"source": "opencode", "messages": [
                        {"id": (m.get("info") or {}).get("id"),
                         "tokens": (m.get("info") or {}).get("tokens"),
                         "cost": (m.get("info") or {}).get("cost")} for m in new]}
                    result["native_ids"] = {"session_id": saved,
                                            "assistant_message_ids": [(m.get("info") or {}).get("id") for m in new],
                                            "user_message_ids": user_ids}
                    cls = _policy.classify_signal(err) if err else None
                    if evidence:
                        # The session already ended idle with this error.
                        result.update(rc=3, quota=True, signal="exhausted",
                                      free_exhaustion_evidence=evidence,
                                      signal_evidence=evidence, idle_confirmed=True)
                    elif cls == "exhausted":
                        result.update(rc=3, quota=True, signal="exhausted",
                                      signal_evidence=adapters.redact_nested(err),
                                      error=adapters.redact_nested(err), idle_confirmed=True)
                    elif cls == "overloaded":
                        result.update(rc=4, signal="overloaded",
                                      signal_evidence=adapters.redact_nested(err),
                                      error=adapters.redact_nested(err), idle_confirmed=True)
                    elif err:
                        result.update(rc=1, signal=cls, error=adapters.redact_nested(err))
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
        "actual_model", "error", "free_exhaustion_evidence", "signal", "signal_evidence",
        "usage", "native_ids", "assistant_text",
        "assistant_messages", "model", "variant", "agent")}
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
        kind_row = con.execute("SELECT kind FROM invocations WHERE invocation_id=?",
                               (invocation_id,)).fetchone()
        inv_kind = kind_row["kind"] if kind_row is not None else None
        if skind == "codex_task_id" and inv_kind == "codex_dispatch":
            # Only the dispatch turn names the Luna task, and never twice.
            # A resume reporting another thread is recorded on its own row.
            con.execute(
                "UPDATE jobs SET codex_task_id=COALESCE(codex_task_id, ?), adapter='codex',"
                " model=?, effort=?, updated_at=? WHERE request_id=?",
                (sid, adapters.CODEX_MODEL, adapters.CODEX_EFFORT, core._utcnow(), request_id),
            )
        elif skind == "opencode_session_id":
            row = con.execute("SELECT route FROM jobs WHERE request_id=?",
                              (request_id,)).fetchone()
            route = (row["route"] if row is not None else None) or "muse-spark-xhigh-free"
            r_model, r_variant, _r_agent = adapters.opencode_route_params(route)
            con.execute(
                "UPDATE jobs SET opencode_session_id=?, adapter='opencode', model=?, effort=?, updated_at=? WHERE request_id=?",
                (sid, r_model, r_variant or "default", core._utcnow(), request_id),
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
