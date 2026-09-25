"""Per-turn native Codex driver (toolboxmd/model-router#86). Stdlib only.

Runs as a supervised child (one process per dispatcher turn) against the
job-owned native app-server from ``runner/codex_native.py``. The server
itself persists across turns in its own process group; this driver only
performs one control turn, prints converted CLI-like JSONL to stdout, and
exits. The supervisor's existing stdout polling, stall detection, timeout,
and cancellation apply unchanged.

Environment (injected by the harness spawn spec for native turns):
  MR_NATIVE_STATE_DIR, MR_NATIVE_REQUEST_ID, MR_NATIVE_INVOCATION_ID.

Exit codes: 0 turn completed, 1 failed/refused/busy, 143 terminated.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_STOP = {"requested": False, "interrupted": False}


def _on_term(signum, frame):  # noqa: ARG001
    _STOP["requested"] = True


def _emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _fail(message: str) -> int:
    _emit({"type": "error", "message": message[:1000]})
    sys.stderr.write(message[:2000] + "\n")
    sys.stderr.flush()
    return 1


def _rpc_fail(prefix: str, exc: Exception) -> int:
    """Failure line keeping the server's RPC code for signal classification.

    Usage-limit codes (UsageLimitExceeded / RateLimitExceeded) must survive
    as structured evidence so the controller marks the pool exhausted; auth
    text rides verbatim on stderr for the existing auth markers.
    """
    line: dict = {"type": "error", "message": f"{prefix}: {exc}"[:1000]}
    code = getattr(exc, "rpc_code", None)
    if code is not None:
        line["code"] = code
    _emit(line)
    sys.stderr.write(f"{prefix}: {exc}"[:2000] + "\n")
    sys.stderr.flush()
    return 1


def _load_context():
    from runner import core as _core
    from runner import store as _store

    state_dir = os.environ.get("MR_NATIVE_STATE_DIR") or ""
    request_id = os.environ.get("MR_NATIVE_REQUEST_ID") or ""
    invocation_id = os.environ.get("MR_NATIVE_INVOCATION_ID") or ""
    if not state_dir or not request_id or not invocation_id:
        raise RuntimeError("native driver: missing MR_NATIVE_* environment")
    job = _core.get_job(state_dir, request_id)
    invs = [i for i in _core._list_invocations(state_dir, request_id)
            if i.get("invocation_id") == invocation_id]
    if not invs:
        raise RuntimeError("native driver: unknown invocation")
    inv = invs[0]
    try:
        meta = json.loads(inv.get("meta_json") or "{}")
    except ValueError:
        meta = {}
    if not isinstance(meta, dict):
        meta = {}
    return state_dir, request_id, invocation_id, job, inv, meta


def _cancel_requested(state_dir, request_id: str) -> bool:
    try:
        from runner import core as _core

        job = _core.get_job(state_dir, request_id)
        return bool(job.get("cancel_requested"))
    except Exception:
        return False


def _active_turn_from_resume(resumed: dict, thread_id: str):
    """(active_turn_id|None, status_text) from a thread/resume result."""
    thread = resumed.get("thread") if isinstance(resumed, dict) else None
    if not isinstance(thread, dict):
        return None, "unknown"
    status = thread.get("status")
    if isinstance(status, dict) and status.get("type") == "idle":
        return None, "idle"
    if not isinstance(status, dict) or status.get("type") != "active":
        return None, "unknown"
    turns = thread.get("turns")
    if isinstance(turns, list):
        for turn in reversed(turns):
            if isinstance(turn, dict) and turn.get("status") == "inProgress" \
                    and isinstance(turn.get("id"), str) and turn["id"]:
                return turn["id"], "active"
        return None, "active-no-turn"
    return None, "active"


def _await_idle(client, thread_id, state_dir, request_id, recover_turn=None):
    """Wait for foreign work; only rejoin our own interrupted invocation."""
    while True:
        if _STOP["requested"] or _cancel_requested(state_dir, request_id):
            raise RuntimeError("native driver cancelled while waiting")
        # A newly created native thread has no persisted turns yet; asking
        # for history there fails even though its idle metadata is readable.
        snapshot = client.thread_read(thread_id, include_turns=False)
        active, status = _active_turn_from_resume(snapshot, thread_id)
        if status == "idle":
            return None
        if status == "unknown":
            raise RuntimeError("native thread status is unknown")
        if recover_turn and active is None:
            active, status = _active_turn_from_resume(client.thread_read(thread_id), thread_id)
        if active is not None and active == recover_turn:
            return active
        while True:
            if _STOP["requested"] or _cancel_requested(state_dir, request_id):
                raise RuntimeError("native driver cancelled while waiting")
            event = client.next_event()
            if event is None:
                continue
            _emit({"type": "native.activity", "method": "waiting-for-native-turn"})
            if event.get("method") in ("turn/completed", "thread/status/changed"):
                break


def main() -> int:
    from runner import adapters as _adapters
    from runner import codex_native as _native

    try:
        state_dir, request_id, _inv_id, job, _inv, meta = _load_context()
    except Exception as e:  # noqa: BLE001 - the supervisor records rc/stderr
        sys.stderr.write(f"native driver setup failed: {e}\n")
        return 1
    native_meta = meta.get(_native.NATIVE_META_KEY)
    op = native_meta.get("op") if isinstance(native_meta, dict) else None
    prompt = meta.get("prompt") or ""
    model = meta.get("model") or _adapters.CODEX_MODEL
    effort = meta.get("effort") or _adapters.CODEX_EFFORT
    workspace = job.get("workspace") or os.getcwd()
    if op not in (_native.OP_DISPATCH, _native.OP_RESUME):
        return _fail(f"native driver: unknown op {op!r}")
    if not prompt:
        return _fail("native driver: missing prompt")

    try:
        runtime = _native.ensure_server(state_dir, request_id, workspace)
    except Exception as e:  # noqa: BLE001 - server failure degrades downstream
        return _fail(f"native_server_failed: {type(e).__name__}: {str(e)[:300]}")
    endpoint = runtime.get("endpoint")
    if not isinstance(endpoint, str) or not endpoint:
        return _fail("native_server_failed: no endpoint")

    try:
        client = _native.NativeClient.connect(endpoint, role="dispatcher")
    except Exception as e:  # noqa: BLE001
        return _fail(f"native connect failed: {type(e).__name__}: {str(e)[:300]}")

    thread_id: str | None = None
    turn_id: str | None = None
    try:
        thread_id = runtime.get("thread_id") or job.get("codex_task_id")
        if op == _native.OP_DISPATCH and not thread_id:
            try:
                thread = client.thread_start(workspace, model)
            except Exception as e:  # noqa: BLE001
                return _rpc_fail("native thread/start failed", e)
            thread_id = thread.get("id")
            if not isinstance(thread_id, str) or not thread_id:
                return _fail("native thread/start returned no thread id")
            _native.write_runtime(state_dir, request_id, {"thread_id": thread_id})
            _emit({"type": "thread.started", "thread_id": thread_id})
        else:
            if not isinstance(thread_id, str) or not thread_id:
                return _fail("native resume: missing saved thread id")
            try:
                resumed = client.thread_resume(thread_id)
            except Exception as e:  # noqa: BLE001
                return _rpc_fail("native thread/resume failed", e)
            _emit({"type": "thread.started", "thread_id": thread_id})
        recover_turn = (runtime.get("control_turn_id")
                        if runtime.get("control_invocation_id") == _inv_id else None)
        turn_id = _await_idle(client, thread_id, state_dir, request_id, recover_turn)
        if turn_id is None:
            try:
                turn = client.turn_start(thread_id, prompt, effort)
            except Exception as e:  # noqa: BLE001
                return _rpc_fail("native turn/start failed", e)
            turn_id = turn.get("id")
        if not isinstance(turn_id, str) or not turn_id:
            return _fail("native turn/start returned no turn id")
        _emit({"type": "turn.started", "turn_id": turn_id,
               "thread_id": thread_id})
        try:
            _native.write_runtime(state_dir, request_id,
                                  {"thread_id": thread_id,
                                   "control_turn_id": turn_id,
                                   "control_invocation_id": _inv_id,
                                   "endpoint": endpoint})
        except Exception:
            client.turn_interrupt(thread_id, turn_id)
            raise

        token_events: list = []
        last_cancel_check = time.monotonic()
        while True:
            if _STOP["requested"]:
                try:
                    client.turn_interrupt(thread_id, turn_id)
                except Exception:
                    pass
                sys.stderr.write("native driver terminated\n")
                return 143
            now = time.monotonic()
            if now - last_cancel_check > 5.0:
                last_cancel_check = now
                if _cancel_requested(state_dir, request_id):
                    try:
                        client.turn_interrupt(thread_id, turn_id)
                    except Exception:
                        pass
                    return _fail("native driver cancelled")
            event = client.next_event(timeout=_native.TURN_POLL_SECS)
            if event is None:
                continue
            if event.get("id") is not None:
                # Late RPC answer for another call; the request path
                # already moved on. Ignore, never treat as turn output.
                continue
            if event.get("method") == "thread/tokenUsage/updated":
                if event.get("params", {}).get("turnId") == turn_id:
                    token_events.append(event)
                continue
            for line in _native.convert_event(event, control_turn_id=turn_id):
                if line.get("type") == "turn.completed":
                    usage = _native.usage_from_token_events(token_events)
                    if usage is not None:
                        line = dict(line, usage={"source": "codex", **usage})
                    _emit(line)
                    try:
                        client.close()
                    except Exception:
                        pass
                    return 0
                if line.get("type") == "error":
                    _emit(line)
                    sys.stderr.write(str(line.get("message") or "turn failed")[:1000] + "\n")
                    return 1
                _emit(line)
    except Exception as e:
        return _rpc_fail("native control turn failed", e)
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    for _sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(_sig, _on_term)
    raise SystemExit(main())
