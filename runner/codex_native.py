"""Native Codex dispatcher transport (toolboxmd/model-router#86). Stdlib only.

One job-owned ``codex app-server --listen ws://127.0.0.1:PORT`` serves every
dispatcher turn of the job, so a second native client (T3) can discover the
active thread, metadata-only attach, observe, steer the control turn,
disconnect, and reconnect without terminating the agent or changing
permissions. The proven protocol (Codex 0.156.1, ``/tmp/t3-fleet-spec/probe/
shared.mjs``) is: ``initialize`` with ``experimentalApi`` true, then
``thread/start`` + ``turn/start`` on the control client; observers use
``thread/loaded/list``, ``thread/resume`` with ``excludeTurns`` true and no
settings changes, and ``turn/steer`` with the control turn id.

The transport converts native events to the existing CLI JSONL shapes
(``thread.started``, ``turn.completed``, ``item.completed`` with
``agent_message``), so envelope handling, capacity signals, stall detection,
cancellation, recovery, ownership, and one-writer guarantees stay unchanged.
Only loopback endpoints are allowed; no credentials exist on this transport
and none are published (the endpoint is runtime metadata, never a secret).
There is no Router inbox, chat API, transcript parser, dashboard, model
policy change, remote exposure, or persistent host service.
"""
from __future__ import annotations

import base64
from collections import deque
import hashlib
import json
import os
import select as _select
import socket as _socket
import struct as _struct
import subprocess
import sys
import time
import urllib.parse

# Proven client identity prefix; the per-client suffix names the role.
CLIENT_NAME = "model-router"
CLIENT_VERSION = "1.0.0"

# Loopback hosts only. Anything else is refused before any spawn or connect.
LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")

# Dispatcher sandbox: read-only, never ask (probe-exact thread settings).
NATIVE_APPROVAL_POLICY = "never"
NATIVE_SANDBOX = "read-only"

# RPC answer budget per request (the probe used 30s).
RPC_TIMEOUT_SECS = 30.0
# Server readiness budget after spawn.
SERVER_READY_TIMEOUT_SECS = 30.0
# Short poll quantum for the turn loop (signal/cancel responsiveness).
TURN_POLL_SECS = 1.0

# Kind flag carried in invocation meta for native dispatcher turns.
NATIVE_META_KEY = "native"
OP_DISPATCH = "dispatch"
OP_RESUME = "resume"


class NativeError(RuntimeError):
    pass


class NativeRpcError(NativeError):
    """A JSON-RPC error answer, keeping the server's code and message."""

    def __init__(self, method: str, code, message: str):
        self.rpc_method = method
        self.rpc_code = code
        self.rpc_message = message
        super().__init__(f"native {method} refused: {str(message)[:300]}")


def validate_endpoint(url: str) -> tuple[str, int]:
    """(host, port) for a loopback WebSocket endpoint, or raise.

    Only ``ws://`` loopback URLs without userinfo are allowed: no remote
    exposure, and credentials can never hide in the published endpoint.
    """
    try:
        parts = urllib.parse.urlparse(url or "")
    except ValueError as e:
        raise NativeError(f"invalid native endpoint: {e}") from e
    if parts.scheme != "ws":
        raise NativeError(f"native endpoint must be ws://, got {url!r}")
    if parts.username or parts.password:
        raise NativeError("native endpoint must carry no credentials")
    host = (parts.hostname or "").lower()
    if host not in LOOPBACK_HOSTS:
        raise NativeError(f"native endpoint allows loopback only, got {host!r}")
    try:
        port = parts.port
    except ValueError as e:
        raise NativeError(f"invalid native endpoint port: {e}") from e
    if not port:
        raise NativeError(f"native endpoint needs a port, got {url!r}")
    return host, port


def reserve_port() -> int:
    """An ephemeral loopback port for one app-server listen address."""
    s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])
    finally:
        s.close()


def server_cmd(port: int) -> list[str]:
    """Job-owned native app-server argv on a loopback port."""
    return ["codex", "app-server", "--listen", f"ws://127.0.0.1:{port}"]


def server_kit_dir(state_dir, request_id: str):
    """The single dispatcher kit dir backing the job's native server."""
    from . import kits as _kits

    return _kits.kit_dir_for(str(state_dir), str(request_id), "native", "dispatcher")


def server_env(server_kit) -> dict:
    """Isolated Codex environment for the job server.

    The kit links the user's login and shares the job's sessions directory,
    so every thread the server owns is this job's own. Nothing is inherited
    from ambient Codex configuration beyond what the kit carries.
    """
    from . import adapters as _adapters
    from . import kits as _kits

    env = _adapters.child_harness_env()
    env.update(_kits.codex_kit_env(server_kit))
    return env


def server_log_path(state_dir, request_id: str) -> str:
    from . import store as _store

    root = _store.ensure_state_dir(state_dir)
    return str(root / "outputs" / f"{request_id}.native-server.log")


# ---------------------------------------------------------------------------
# Minimal stdlib WebSocket client (RFC 6455, text frames).
# ---------------------------------------------------------------------------

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_accept_key(key: str) -> str:
    digest = hashlib.sha1((key + _WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


class NativeWs:
    """One WebSocket connection to a native app-server. Blocking, stdlib."""

    def __init__(self, sock: _socket.socket):
        self._sock = sock
        self._buf = b""
        self._fragments = None
        self.closed = False

    @classmethod
    def connect(cls, host: str, port: int, timeout: float = 10.0) -> "NativeWs":
        sock = _socket.create_connection((host, port), timeout=timeout)
        try:
            sock.settimeout(timeout)
            key = base64.b64encode(os.urandom(16)).decode("ascii")
            req = (
                f"GET / HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\n"
                f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n\r\n"
            )
            sock.sendall(req.encode("ascii"))
            raw = b""
            while b"\r\n\r\n" not in raw:
                chunk = sock.recv(4096)
                if not chunk:
                    raise NativeError("native handshake: connection closed")
                raw += chunk
            header, remainder = raw.split(b"\r\n\r\n", 1)
            head = header.decode("latin-1")
            status = head.split("\r\n", 1)[0]
            if status.split()[1:2] != ["101"]:
                raise NativeError(f"native handshake refused: {status[:120]}")
            lowered = head.lower()
            if "upgrade" not in lowered or "websocket" not in lowered:
                raise NativeError("native handshake: not a websocket upgrade")
            headers = dict(line.split(":", 1) for line in head.split("\r\n")[1:] if ":" in line)
            accept = next((v.strip() for k, v in headers.items()
                           if k.lower() == "sec-websocket-accept"), None)
            if accept != _ws_accept_key(key):
                raise NativeError("native handshake: invalid accept key")
            ws = cls(sock)
            ws._buf = remainder
            return ws
        except Exception:
            try:
                sock.close()
            except OSError:
                pass
            raise

    def send_text(self, text: str) -> None:
        data = text.encode("utf-8")
        header = bytes([0x81])
        n = len(data)
        if n < 126:
            header += bytes([0x80 | n])
        elif n < (1 << 16):
            header += bytes([0x80 | 126]) + _struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + _struct.pack(">Q", n)
        mask = os.urandom(4)
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        try:
            self._sock.sendall(header + masked)
        except OSError as e:
            raise NativeError(f"native send failed: {e}") from e

    def recv_message(self, timeout: float) -> dict | None:
        """Read a message without discarding partial frames across poll deadlines."""
        deadline = time.monotonic() + max(0, timeout)
        while True:
            frame = None
            if len(self._buf) >= 2:
                first, second = self._buf[:2]
                offset = 2
                length = second & 0x7f
                size = 2 if length == 126 else 8 if length == 127 else 0
                if len(self._buf) >= offset + size:
                    if size:
                        length = int.from_bytes(self._buf[offset:offset + size], "big")
                        offset += size
                    if length > 64 * 1024 * 1024:
                        raise NativeError("native WebSocket frame is too large")
                    if second & 0x80:
                        raise NativeError("native server frames must not be masked")
                    if len(self._buf) >= offset + length:
                        frame = (first, self._buf[offset:offset + length])
                        self._buf = self._buf[offset + length:]
            if frame is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                ready, _, _ = _select.select([self._sock], [], [], remaining)
                if not ready:
                    return None
                chunk = self._sock.recv(65536)
                if not chunk:
                    raise NativeError("native connection closed")
                self._buf += chunk
                continue
            first, payload = frame
            opcode, final = first & 0x0f, bool(first & 0x80)
            if first & 0x70:
                raise NativeError("unsupported WebSocket extension")
            if opcode >= 8:
                if not final or len(payload) > 125:
                    raise NativeError("invalid WebSocket control frame")
                if opcode == 8:
                    self.closed = True
                    return {"close": True}
                if opcode == 9:
                    self._pong(payload)
                continue
            if opcode == 1 and self._fragments is None:
                self._fragments = bytearray()
            elif opcode != 0 or self._fragments is None:
                raise NativeError("unexpected WebSocket continuation or binary message")
            self._fragments.extend(payload)
            if len(self._fragments) > 64 * 1024 * 1024:
                raise NativeError("native WebSocket message is too large")
            if final:
                payload = bytes(self._fragments)
                self._fragments = None
                try:
                    return {"text": payload.decode("utf-8")}
                except UnicodeDecodeError as e:
                    raise NativeError("native non-text message") from e

    def _pong(self, payload: bytes) -> None:
        header = bytes([0x8A, 0x80 | len(payload)]) + os.urandom(4)
        mask = header[-4:]
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        try:
            self._sock.sendall(header + masked)
        except OSError:
            pass

    def close(self) -> None:
        if self.closed:
            try:
                self._sock.close()
            except OSError:
                pass
            return
        self.closed = True
        try:
            self._sock.shutdown(_socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass


class NativeClient:
    """JSON-RPC over one native WebSocket (proven Codex 0.156.1 protocol)."""

    def __init__(self, ws: NativeWs, role: str = "dispatcher"):
        self.ws = ws
        self.role = role
        self._seq = 0
        self._pending = deque()

    @classmethod
    def connect(cls, endpoint: str, role: str = "dispatcher",
                timeout: float = 10.0) -> "NativeClient":
        host, port = validate_endpoint(endpoint)
        ws = NativeWs.connect(host, port, timeout=timeout)
        client = cls(ws, role=role)
        try:
            client.initialize(timeout=timeout)
        except Exception:
            ws.close()
            raise
        return client

    def _next_id(self) -> int:
        self._seq += 1
        return self._seq

    def request(self, method: str, params: dict | None = None,
                timeout: float = RPC_TIMEOUT_SECS) -> dict:
        """Send one RPC and wait for its ``result`` (raises on error)."""
        rid = self._next_id()
        payload: dict = {"id": rid, "method": method}
        if params is not None:
            payload["params"] = params
        self.ws.send_text(json.dumps(payload))
        deadline = time.monotonic() + max(1.0, float(timeout))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise NativeError(f"native {method}: read timed out")
            msg = self.ws.recv_message(timeout=min(remaining, TURN_POLL_SECS))
            if msg is None:
                continue
            if "text" not in msg:
                if msg.get("close"):
                    raise NativeError(f"native {method}: connection closed")
                continue
            try:
                obj = json.loads(msg["text"])
            except ValueError:
                continue
            if not isinstance(obj, dict) or obj.get("id") != rid:
                if isinstance(obj, dict) and "method" in obj:
                    self._pending.append(obj)
                continue
            if isinstance(obj.get("error"), dict):
                err = obj["error"]
                raise NativeRpcError(
                    method, err.get("code"),
                    str(err.get("message") or err)[:500])
            if "result" not in obj:
                raise NativeError(f"native {method}: no result")
            result = obj["result"]
            return result if isinstance(result, dict) else {"value": result}

    def next_event(self, timeout: float = TURN_POLL_SECS) -> dict | None:
        if self._pending:
            return self._pending.popleft()
        msg = self.ws.recv_message(timeout)
        if msg is None or "ping" in msg:
            return None
        if msg.get("close"):
            raise NativeError("native connection closed")
        try:
            event = json.loads(msg["text"])
        except (KeyError, ValueError):
            raise NativeError("native server sent invalid JSON") from None
        return event if isinstance(event, dict) else None

    def notify(self, method: str, params: dict | None = None) -> None:
        payload: dict = {"method": method}
        if params is not None:
            payload["params"] = params
        self.ws.send_text(json.dumps(payload))

    def initialize(self, timeout: float = RPC_TIMEOUT_SECS) -> dict:
        result = self.request(
            "initialize",
            {"clientInfo": {"name": f"{CLIENT_NAME}_{self.role}",
                            "version": CLIENT_VERSION},
             "capabilities": {"experimentalApi": True}}, timeout=timeout)
        # Probe-exact handshake: bare initialized notification, no params.
        self.ws.send_text(json.dumps({"method": "initialized"}))
        return result

    # -- dispatcher turns -------------------------------------------------
    def thread_start(self, cwd: str, model: str) -> dict:
        return self.request("thread/start",
                            thread_start_params(cwd, model))["thread"]

    def turn_start(self, thread_id: str, text: str, effort: str) -> dict:
        return self.request(
            "turn/start",
            turn_start_params(thread_id, text, effort))["turn"]

    # -- observer / resume (metadata-only, never changes settings) --------
    def thread_loaded_list(self) -> dict:
        return self.request("thread/loaded/list", {})

    def thread_resume(self, thread_id: str) -> dict:
        return self.request("thread/resume", resume_params(thread_id))

    def turn_steer(self, thread_id: str, expected_turn_id: str,
                   text: str) -> dict:
        return self.request("turn/steer",
                            steer_params(thread_id, expected_turn_id, text))

    def thread_read(self, thread_id: str, include_turns: bool = True) -> dict:
        return self.request("thread/read", dict(read_params(thread_id), includeTurns=include_turns))

    def turn_interrupt(self, thread_id: str, turn_id: str) -> None:
        try:
            self.request("turn/interrupt",
                         interrupt_params(thread_id, turn_id),
                         timeout=10.0)
        except NativeError:
            # Best effort: the turn may already be done; the caller owns
            # the outcome either way.
            pass

    def close(self) -> None:
        try:
            self.ws.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Proven protocol params (unit-tested pure constructors; the client above
# and the focused regression tests share them, so transport drift fails
# loudly instead of forking a second contract).
# ---------------------------------------------------------------------------

def text_input(text: str) -> list[dict]:
    return [{"type": "text", "text": text, "text_elements": []}]


def thread_start_params(cwd: str, model: str) -> dict:
    """First turn's thread: policy model, read-only sandbox, never ask."""
    if not cwd:
        raise ValueError("missing cwd for thread/start")
    if not model:
        raise ValueError("missing model for thread/start")
    return {"cwd": cwd, "model": model,
            "approvalPolicy": NATIVE_APPROVAL_POLICY,
            "sandbox": NATIVE_SANDBOX}


def turn_start_params(thread_id: str, text: str, effort: str) -> dict:
    """One control turn on the saved thread: input plus policy effort."""
    if not thread_id:
        raise ValueError("missing thread id for turn/start")
    if not text:
        raise ValueError("missing input for turn/start")
    return {"threadId": thread_id, "input": text_input(text),
            "effort": effort or "max"}


def resume_params(thread_id: str) -> dict:
    """Metadata-only attach: thread id plus excludeTurns, never settings.

    No model, sandbox, approval, or permission field may ride along: an
    observer (T3 or a restarted controller) must not change what the
    agent may do by attaching.
    """
    if not thread_id:
        raise ValueError("missing thread id for thread/resume")
    return {"threadId": thread_id, "excludeTurns": True}


def steer_params(thread_id: str, expected_turn_id: str, text: str) -> dict:
    if not thread_id:
        raise ValueError("missing thread id for turn/steer")
    if not expected_turn_id:
        raise ValueError("missing expected turn id for turn/steer")
    if not text:
        raise ValueError("missing input for turn/steer")
    return {"threadId": thread_id, "expectedTurnId": expected_turn_id,
            "input": text_input(text)}


def read_params(thread_id: str) -> dict:
    if not thread_id:
        raise ValueError("missing thread id for thread/read")
    return {"threadId": thread_id, "includeTurns": True}


def interrupt_params(thread_id: str, turn_id: str) -> dict:
    if not thread_id:
        raise ValueError("missing thread id for turn/interrupt")
    if not turn_id:
        raise ValueError("missing turn id for turn/interrupt")
    return {"threadId": thread_id, "turnId": turn_id}


# ---------------------------------------------------------------------------
# Native event conversion to the existing CLI JSONL shapes.
# ---------------------------------------------------------------------------

def _agent_text(item: dict) -> str | None:
    if not isinstance(item, dict):
        return None
    if item.get("type") != "agentMessage":
        return None
    text = item.get("text")
    return text if isinstance(text, str) else None


def convert_event(event: dict, control_turn_id: str | None = None) -> list[dict]:
    """CLI-like JSONL dicts for one native event, filtered to our turn.

    Events from unrelated turns (a user turn started while the dispatcher
    was idle) convert to nothing: side communication can never become a
    routing envelope. Native activity is retained without copying tool
    output into assistant messages, so the supervisor can detect stalls.
    """
    if not isinstance(event, dict):
        return []
    method = event.get("method")
    params = event.get("params") if isinstance(event.get("params"), dict) else {}
    if method == "thread/started":
        thread = params.get("thread") if isinstance(params.get("thread"), dict) else {}
        tid = thread.get("id")
        if isinstance(tid, str) and tid:
            return [{"type": "thread.started", "thread_id": tid}]
        return []
    if method in ("turn/started",):
        turn = params.get("turn") if isinstance(params.get("turn"), dict) else {}
        tid = turn.get("id")
        if isinstance(tid, str) and tid and (control_turn_id is None or tid == control_turn_id):
            return [{"type": "turn.started", "turn_id": tid,
                     "thread_id": params.get("threadId")}]
        return []
    if method == "turn/completed":
        turn = params.get("turn") if isinstance(params.get("turn"), dict) else {}
        tid = turn.get("id")
        if not isinstance(tid, str) or not tid:
            return []
        if control_turn_id is not None and tid != control_turn_id:
            return []
        if turn.get("status") not in (None, "completed"):
            err = turn.get("error") or {"message": f"native turn {turn.get('status')}"}
            return [{"type": "error", "message": json.dumps(err),
                     "code": err.get("codexErrorInfo", err.get("code")) if isinstance(err, dict) else None}]
        return [{"type": "turn.completed", "turn_id": tid,
                 "thread_id": params.get("threadId")}]
    if method == "turn/failed":
        turn = params.get("turn") if isinstance(params.get("turn"), dict) else {}
        tid = turn.get("id") if isinstance(turn, dict) else None
        if control_turn_id is not None and tid != control_turn_id:
            return []
        err = params.get("error") or (turn.get("error") if isinstance(turn, dict) else None)
        line: dict = {"type": "error",
                      "message": str(err)[:1000] if err else "turn failed"}
        if isinstance(err, dict):
            for key in ("code", "name"):
                if err.get(key) is not None:
                    line["code"] = err[key]
                    break
        return [line]
    if method == "item/completed":
        item = params.get("item")
        if not isinstance(item, dict):
            return []
        if control_turn_id is not None and params.get("turnId") != control_turn_id:
            return []
        text = _agent_text(item)
        if text is None:
            return [{"type": "native.activity", "method": method}]
        return [{"type": "item.completed",
                 "item": {"type": "agent_message", "text": text,
                          "id": item.get("id")}}]
    if params.get("turnId") == control_turn_id and control_turn_id is not None:
        if method == "error":
            # Retriable native errors are activity until the native turn
            # reports its terminal status. Preserve the provider evidence.
            err = params.get("error") or {}
            return [{"type": "native.activity" if params.get("willRetry") else "error",
                     "message": json.dumps(err)}]
        if isinstance(method, str) and method.startswith("item/"):
            return [{"type": "native.activity", "method": method}]
    return []


def usage_from_token_events(events: list[dict]) -> dict | None:
    """Sum unique request usage updates from this control turn only."""
    totals = {}
    seen = set()
    for event in events or []:
        if not isinstance(event, dict) or event.get("method") != "thread/tokenUsage/updated":
            continue
        params = event.get("params")
        if not isinstance(params, dict):
            continue
        usage = params.get("tokenUsage")
        if not isinstance(usage, dict) or not isinstance(usage.get("last"), dict):
            continue
        identity = json.dumps([params.get("turnId"), usage.get("total")], sort_keys=True)
        if identity in seen:
            continue
        seen.add(identity)
        for key, value in usage["last"].items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                totals[key] = totals.get(key, 0) + value
    if not totals:
        return None
    out: dict = {}
    mapping = (("inputTokens", "input_tokens"), ("outputTokens", "output_tokens"),
               ("totalTokens", "total_tokens"),
               ("cachedInputTokens", "cached_input_tokens"),
               ("cacheWriteInputTokens", "cache_write_input_tokens"),
               ("reasoningOutputTokens", "reasoning_output_tokens"))
    for src, dst in mapping:
        try:
            if totals.get(src) is not None:
                out[dst] = totals[src]
        except (TypeError, ValueError):
            continue
    return out or None


def thread_id_from_lines(stdout: str) -> str | None:
    """First ``thread.started`` id in converted JSONL (never another id)."""
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict) and obj.get("type") == "thread.started" \
                and isinstance(obj.get("thread_id"), str) and obj["thread_id"]:
            return obj["thread_id"]
    return None


def turn_id_from_lines(stdout: str) -> str | None:
    """First ``turn.started`` id in converted JSONL (the control turn)."""
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict) and obj.get("type") == "turn.started" \
                and isinstance(obj.get("turn_id"), str) and obj["turn_id"]:
            return obj["turn_id"]
    return None


# ---------------------------------------------------------------------------
# Per-job server lifecycle + runtime metadata (existing ownership patterns).
# ---------------------------------------------------------------------------

def read_runtime(job: dict) -> dict:
    """The job's native runtime record (endpoint, thread, control turn)."""
    try:
        state = json.loads((job or {}).get("controller_state") or "{}")
    except ValueError:
        return {}
    native = state.get("native")
    return dict(native) if isinstance(native, dict) else {}


def write_runtime(state_dir, request_id: str, patch: dict) -> dict:
    """Persist ownership before callers proceed; never hide a failed write."""
    from . import core as _core, store as _store
    con = _store.connect(state_dir)
    try:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute("SELECT controller_state FROM jobs WHERE request_id=?",
                          (request_id,)).fetchone()
        if row is None:
            raise NativeError("native runtime has no owning job")
        state = json.loads(row["controller_state"] or "{}")
        native = dict(state.get("native") or {})
        native.update(patch)
        state["native"] = native
        con.execute("UPDATE jobs SET controller_state=?, updated_at=? WHERE request_id=?",
                    (json.dumps(state, sort_keys=True), _core._utcnow(), request_id))
        con.commit()
        return native
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _process_start(pid: int | None) -> str | None:
    try:
        from .supervisor import process_start_identity
    except ImportError:
        return None
    try:
        return process_start_identity(pid)
    except Exception:
        return None


def _pid_alive(pid) -> bool:
    try:
        from . import core as _core
        return bool(_core._is_pid_alive(pid))
    except Exception:
        return False


def _endpoint_allowed(endpoint: str) -> bool:
    """True when the recorded endpoint is still a publishable loopback URL."""
    try:
        validate_endpoint(endpoint)
        return True
    except NativeError:
        return False


def server_reachable(endpoint: str, timeout: float = 3.0) -> bool:
    """True when a native handshake answers on the endpoint (no turn)."""
    try:
        host, port = validate_endpoint(endpoint)
        client = NativeClient.connect(endpoint, role="health", timeout=timeout)
    except Exception:
        return False
    try:
        client.close()
    except Exception:
        pass
    return True


def _recorded_server_alive(runtime: dict) -> bool:
    pid = runtime.get("pid")
    if not pid or not _pid_alive(pid):
        return False
    recorded = runtime.get("start")
    if recorded:
        return _process_start(pid) == recorded
    return False


def codex_version() -> str | None:
    try:
        out = subprocess.run(["codex", "--version"], capture_output=True,
                             text=True, timeout=5,
                             stdin=subprocess.DEVNULL).stdout
    except Exception:
        return None
    first = (out or "").strip().splitlines()
    return first[0][:80] if first else None


def ensure_server(state_dir, request_id: str, workspace: str) -> dict:
    """Reuse the owned server or reserve its launch without a duplicate."""
    import fcntl
    from . import core, store

    runtime = read_runtime(core.get_job(state_dir, request_id))
    if runtime.get("pid") and _pid_alive(runtime["pid"]):
        endpoint = runtime.get("endpoint")
        if (_recorded_server_alive(runtime) and isinstance(endpoint, str)
                and _endpoint_allowed(endpoint) and server_reachable(endpoint)):
            return runtime
        raise NativeError("native server is alive but unreachable or ownership is unproved; refusing replacement")
    # The server inherits this lock. It remains held if the launching
    # driver dies before saving the PID, so recovery cannot create a
    # second backend in that crash window.
    lock_path = store.ensure_state_dir(state_dir) / f"{request_id}.native.lock"
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise NativeError("native server launch is still owned; refusing replacement") from None
        return _launch_server(state_dir, request_id, workspace, fd)
    finally:
        os.close(fd)


def _launch_server(state_dir, request_id: str, workspace: str, lock_fd: int) -> dict:
    """Reuse or start the job-owned native app-server. Returns runtime.

    Reuse needs all three: the recorded PID is alive with its recorded
    start identity, and a native handshake answers on the recorded
    endpoint. Otherwise a fresh loopback server starts on the job's
    dispatcher kit (shared sessions dir, user login link) and the runtime
    is replaced. Raises NativeError when no server can be provided.
    """
    from . import core as _core
    from . import kits as _kits
    from . import store as _store

    job = _core.get_job(state_dir, request_id)
    runtime = read_runtime(job)
    endpoint = runtime.get("endpoint")
    if runtime.get("pid") and _pid_alive(runtime["pid"]):
        if (_recorded_server_alive(runtime) and isinstance(endpoint, str)
                and _endpoint_allowed(endpoint) and server_reachable(endpoint)):
            return runtime
        raise NativeError("native server is alive but unreachable or ownership is unproved; refusing replacement")
    kit_dir = server_kit_dir(state_dir, request_id)
    _kits.materialize_codex_kit("dispatcher", kit_dir)
    shared = _kits.codex_sessions_dir_for(str(state_dir), request_id)
    _kits.link_codex_sessions(kit_dir, shared)
    thread_id = runtime.get("thread_id") or job.get("codex_task_id")
    if isinstance(thread_id, str) and thread_id:
        try:
            _kits.adopt_codex_thread(str(state_dir), request_id, thread_id, shared)
        except Exception:
            pass
    port = reserve_port()
    cmd = server_cmd(port)
    env = server_env(kit_dir)
    log_path = server_log_path(state_dir, request_id)
    try:
        log_f = open(log_path, "a", encoding="utf-8")
    except OSError as e:
        raise NativeError(f"native server log unavailable: {e}") from e
    try:
        os.chmod(log_path, 0o600)
    except OSError:
        pass
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL, stdout=log_f, stderr=log_f,
            start_new_session=True, close_fds=True, pass_fds=(lock_fd,), env=env,
            cwd=workspace or None)
    except FileNotFoundError as e:
        raise NativeError(f"native server unavailable: {e}") from e
    except OSError as e:
        raise NativeError(f"native server spawn failed: {e}") from e
    finally:
        try:
            log_f.close()
        except OSError:
            pass
    endpoint = f"ws://127.0.0.1:{port}"
    try:
        identity = _process_start(proc.pid)
        if not identity:
            raise NativeError("native server process identity is unavailable")
        runtime = write_runtime(state_dir, request_id, {
            "endpoint": endpoint, "pid": proc.pid, "pgid": proc.pid,
            "start": identity, "codex_version": codex_version(),
        })
    except Exception:
        # We own this Popen object even when the durable write failed.
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        raise
    deadline = time.monotonic() + SERVER_READY_TIMEOUT_SECS
    ready = False
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise NativeError(f"native server exited rc={proc.returncode} before ready")
        if server_reachable(endpoint, timeout=3.0):
            ready = True
            break
        time.sleep(0.2)
    if not ready:
        stop_server(state_dir, request_id, "startup-failed")
        raise NativeError("native server did not answer on its endpoint")
    try:
        con = _store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            _core._event(con, request_id, "native_server_started",
                         {"endpoint": endpoint})
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
    return runtime


def _launch_unowned(state_dir, request_id: str) -> bool:
    """A held launch lock can outlive the driver's durable PID write."""
    import fcntl
    from pathlib import Path

    path = Path(state_dir) / f"{request_id}.native.lock"
    try:
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    except FileNotFoundError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(fd)


def stop_server(state_dir, request_id: str, reason: str = "terminal") -> bool:
    """Stop the recorded server only when it is provably ours.

    A PID whose start identity does not match the recorded one is never
    signalled (it now belongs to another process). Never raises: terminal
    and cancel paths must not fail on cleanup. Returns True when nothing
    owned remains.
    """
    from . import core as _core
    from . import store as _store

    try:
        job = _core.get_job(state_dir, request_id)
    except Exception:
        return False
    runtime = read_runtime(job)
    pid = runtime.get("pid")
    if not pid:
        return not _launch_unowned(state_dir, request_id)
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    recorded = runtime.get("start")
    try:
        current = _process_start(pid)
    except Exception:
        current = None
    if not _pid_alive(pid):
        write_runtime(state_dir, request_id, {"pid": None, "pgid": None,
                                              "endpoint": None})
        return not _launch_unowned(state_dir, request_id)
    if recorded and current is not None and current != recorded:
        # Not ours anymore: drop the record, touch nothing.
        write_runtime(state_dir, request_id, {"pid": None, "pgid": None,
                                              "endpoint": None})
        return not _launch_unowned(state_dir, request_id)
    if not recorded or current is None:
        # Ownership unprovable: never signal, report not stopped.
        return False
    try:
        import signal as _signal
        pgid = runtime.get("pgid") or pid
        try:
            os.killpg(int(pgid), _signal.SIGTERM)
        except Exception:
            try:
                os.kill(int(pid), _signal.SIGTERM)
            except Exception:
                pass
        end = time.monotonic() + 5.0
        while time.monotonic() < end and _pid_alive(pid):
            time.sleep(0.1)
        if _pid_alive(pid):
            try:
                os.killpg(int(pgid), _signal.SIGKILL)
            except Exception:
                try:
                    os.kill(int(pid), _signal.SIGKILL)
                except Exception:
                    pass
            end = time.monotonic() + 5.0
            while time.monotonic() < end and _pid_alive(pid):
                time.sleep(0.1)
        dead = not _pid_alive(pid)
    except Exception:
        dead = False
    if dead:
        write_runtime(state_dir, request_id, {"pid": None, "pgid": None,
                                              "endpoint": None})
    try:
        con = _store.connect(state_dir)
        try:
            con.execute("BEGIN IMMEDIATE")
            _core._event(con, request_id,
                         "native_server_stopped" if dead else "native_server_stop_failed",
                         {"reason": str(reason)[:64]})
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
    return dead and not _launch_unowned(state_dir, request_id)


def stop_for_job(state_dir, request_id: str, reason: str) -> bool:
    """Confirm cleanup before callers release the job's workspace."""
    try:
        return stop_server(state_dir, request_id, reason)
    except Exception:
        return False


def job_runtime_has_server(state_dir, request_id: str) -> bool:
    from . import core as _core

    try:
        job = _core.get_job(state_dir, request_id)
    except Exception:
        return False
    runtime = read_runtime(job)
    return bool(runtime.get("endpoint") or runtime.get("pid"))


def status_runtime(job: dict) -> dict:
    """Publishable native runtime metadata: endpoint, never credentials.

    There is no password on this transport; the endpoint is loopback-only
    by construction. ``server`` reports the last known liveness without
    starting anything.
    """
    runtime = read_runtime(job)
    endpoint = runtime.get("endpoint")
    if not isinstance(endpoint, str) or not endpoint:
        if runtime.get("thread_id") or runtime.get("control_turn_id"):
            return {"native": {"server": "absent",
                               "thread_id": runtime.get("thread_id"),
                               "control_turn_id": runtime.get("control_turn_id")}}
        return {"native": {"server": "absent"}}
    if not _endpoint_allowed(endpoint):
        return {"native": {"server": "absent"}}
    alive = _recorded_server_alive(runtime)
    out: dict = {"server": "running" if alive else "unreachable",
                 "endpoint": endpoint}
    for key in ("thread_id", "control_turn_id", "codex_version"):
        if runtime.get(key):
            out[key] = runtime[key]
    return {"native": out}


def shared_rollout_paths(state_dir, request_id: str, thread_id: str) -> list:
    """Rollout files for one thread in the job's shared sessions dir."""
    from pathlib import Path as _Path

    from . import kits as _kits

    if not isinstance(thread_id, str) or not thread_id or "/" in thread_id \
            or "\\" in thread_id or ".." in thread_id or "\x00" in thread_id:
        return []
    shared = _kits.codex_sessions_dir_for(str(state_dir), request_id)
    try:
        if not _Path(shared).is_dir():
            return []
        found = sorted(_Path(shared).rglob(f"rollout-*-{thread_id}.jsonl"))
    except OSError:
        return []
    return [p for p in found if p.is_file()]


def thread_active_turn(endpoint: str, thread_id: str,
                       timeout: float = 10.0) -> dict | None:
    """The thread's active turn via metadata-only resume, or None if idle.

    Uses ``thread/resume`` with ``excludeTurns`` true and no settings
    changes, so observing never disturbs the agent. Returns the active
    turn dict, or None when the thread is idle or unreadable.
    """
    try:
        client = NativeClient.connect(endpoint, role="observer", timeout=timeout)
    except Exception:
        return None
    try:
        resumed = client.thread_resume(thread_id)
        thread = resumed.get("thread") if isinstance(resumed, dict) else None
        if not isinstance(thread, dict):
            return None
        status = thread.get("status")
        if isinstance(status, dict) and status.get("type") == "active":
            turns = thread.get("turns")
            if isinstance(turns, list):
                for turn in reversed(turns):
                    if isinstance(turn, dict) and turn.get("status") == "inProgress":
                        return turn
            return {"id": None, "status": "active"}
        return None
    except Exception:
        return None
    finally:
        try:
            client.close()
        except Exception:
            pass


def interrupt_control_turn(state_dir, request_id: str, timeout: float = 10.0) -> bool:
    """Best-effort ``turn/interrupt`` for the recorded control turn."""
    from . import core as _core

    try:
        job = _core.get_job(state_dir, request_id)
    except Exception:
        return False
    runtime = read_runtime(job)
    endpoint = runtime.get("endpoint")
    thread_id = runtime.get("thread_id") or job.get("codex_task_id")
    turn_id = runtime.get("control_turn_id")
    if not (isinstance(endpoint, str) and endpoint and thread_id and turn_id):
        return False
    try:
        client = NativeClient.connect(endpoint, role="dispatcher", timeout=timeout)
    except Exception:
        return False
    try:
        client.turn_interrupt(thread_id, turn_id)
        return True
    except Exception:
        return False
    finally:
        try:
            client.close()
        except Exception:
            pass


def recover_server(state_dir, request_id: str) -> dict:
    """Best-effort native reconcile for recover. Never raises or blocks.

    A live recorded server is reused untouched. A dead record keeps the
    thread id (resume-from-disk on the next turn) but drops the endpoint
    and pids. Jobs without native runtime are untouched.
    """
    from . import core as _core

    try:
        job = _core.get_job(state_dir, request_id)
    except Exception:
        return {"action": "noop"}
    runtime = read_runtime(job)
    if not runtime:
        return {"action": "noop"}
    if job.get("status") in ("succeeded", "failed", "cancelled"):
        stop_for_job(state_dir, request_id, "recover-terminal")
        return {"action": "native-stopped-terminal"}
    if runtime.get("pid") and _pid_alive(runtime["pid"]):
        if _recorded_server_alive(runtime):
            return {"action": "native-reused"}
        return {"action": "native-ownership-unknown"}
    endpoint = runtime.get("endpoint")
    if isinstance(endpoint, str) and endpoint and _recorded_server_alive(runtime) \
            and server_reachable(endpoint):
        return {"action": "native-reused"}
    if runtime.get("pid") or runtime.get("endpoint"):
        write_runtime(state_dir, request_id, {"pid": None, "pgid": None,
                                              "endpoint": None})
        return {"action": "native-cleared-dead"}
    return {"action": "noop"}


def python_driver_cmd() -> list[str]:
    """Argv for the per-turn native driver (stable across restarts)."""
    from pathlib import Path as _Path

    script = _Path(__file__).resolve().parent / "codex_native_turn.py"
    return [sys.executable, str(script)]
