"""Stdlib-only adapter defaults and ephemeral OpenCode control seam.

Built-in defaults (no executor/callback commands required):

- Codex dispatcher: existing ``codex`` CLI as
  ``codex exec --json --output-last-message PATH --model gpt-5.6-luna
  -c model_reasoning_effort="max" --sandbox workspace-write --cd WS``.
  The returned thread/task ID is persisted before the dispatch counts
  as accepted. Resume is ``codex exec resume ID --json`` with the saved
  workspace as cwd (never ``--cd``/``--reasoning`` on resume).
- Claude planner: existing ``claude`` CLI with ``--resume`` of the exact
  saved planner session. Production default is Fable 5.1 max; the
  explicit bounded live-test override is Claude Sonnet 5 medium. Never
  creates a new planner session implicitly.
- Implementation: existing ``opencode`` CLI as
  ``opencode run --format json --pure --dir WS
  --model opencode/muse-spark-1.3-contributor-free --variant xhigh
  --agent build`` free-first, ``opencode-go/muse-spark-1.3-contributor``
  only after exact free exhaustion. ``--session`` resumes the saved
  OpenCode session. Command injection stays available as a test seam
  but is not required in normal submit/start usage.

Control seam: an ephemeral per-job ``opencode serve --pure
--hostname 127.0.0.1 --port 0`` process owned by the controller with a
fresh in-memory password that is never logged or persisted (or an
injectable ``request_func`` equivalent for deterministic tests). Only
the saved OpenCode session status is observed. Never invent a random
password for a server the controller does not own.
"""
from __future__ import annotations

import json
import os
import secrets
import subprocess
import tempfile
import urllib.parse
import urllib.request

CODEX_BIN = "codex"
CODEX_MODEL = "gpt-5.6-luna"
CODEX_EFFORT = "max"
CODEX_REASONING_CONFIG = 'model_reasoning_effort="max"'
CODEX_SANDBOX = "workspace-write"

CLAUDE_BIN = "claude"
# Production planner default: Fable 5.1 max. Sonnet medium is only the
# explicit bounded live-test override (see fixtures/LIVE_RECIPE.md).
CLAUDE_MODEL = "fable-5.1"
CLAUDE_EFFORT = "max"
CLAUDE_LIVE_MODEL = "claude-sonnet-5"
CLAUDE_LIVE_EFFORT = "medium"

OPENCODE_BIN = "opencode"
OPENCODE_FREE_MODEL = "opencode/muse-spark-1.3-contributor-free"
OPENCODE_GO_MODEL = "opencode-go/muse-spark-1.3-contributor"
OPENCODE_VARIANT = "xhigh"
OPENCODE_AGENT = "build"
# Back-compat aliases: old single-model/effort names now map to the
# free route. New code uses FREE/GO models + variant.
OPENCODE_MODEL = OPENCODE_FREE_MODEL
OPENCODE_EFFORT = OPENCODE_VARIANT

# Explicit structured-action protocol embedded in every Luna prompt.
# Luna must reply with exactly one JSON envelope as its final message.
LUNA_ACTION_PROTOCOL = (
    "REPLY PROTOCOL (required): emit exactly one JSON object as your final "
    "message, on its own line, with one of these shapes:\n"
    '{"action":"planner_question","qid":"q1","prompt":"<question for the human planner>"}\n'
    '{"action":"implementation","artifact":"<path or empty>","payload":{},"route":"muse-spark-xhigh-free"}\n'
    '{"action":"completion","output":"<final result text>","artifact":"<path or empty>"}\n'
    "Rules: exactly one envelope; valid JSON; action must be one of "
    "planner_question, implementation, completion; never invent a new "
    "planner or Codex session ID."
)

BUSY_MARKERS = (
    "session is busy",
    "already running",
    "session busy",
    "locked by another",
    "busy",
)


def default_last_message_path() -> str:
    """Fallback output-last-message path when the caller has no state dir."""
    return os.path.join(tempfile.gettempdir(), "codex-last-message.json")


def build_codex_dispatch_cmd(workspace: str, prompt: str,
                             model: str = CODEX_MODEL,
                             effort: str = CODEX_EFFORT,
                             last_message_path: str | None = None) -> list[str]:
    """Default Codex dispatch using the proved CLI contract.

    ``codex exec --json --output-last-message PATH --model gpt-5.6-luna
    -c model_reasoning_effort="max" --sandbox workspace-write --cd WS``.
    Never uses nonexistent ``--reasoning``.
    """
    if not workspace:
        raise ValueError("missing workspace for codex dispatch")
    if effort != CODEX_EFFORT:
        # Effort is encoded via -c model_reasoning_effort; keep the
        # parameter for seam compat but always emit the proved form.
        pass
    out_path = last_message_path or default_last_message_path()
    return [CODEX_BIN, "exec",
            "--json",
            "--output-last-message", out_path,
            "--model", model,
            "-c", CODEX_REASONING_CONFIG,
            "--sandbox", CODEX_SANDBOX,
            "--cd", workspace,
            prompt]


def build_codex_resume_cmd(task_id: str, workspace: str | None = None,
                           prompt: str = "",
                           last_message_path: str | None = None) -> list[str]:
    """Resume only the saved Codex thread/task ID (never fork).

    ``codex exec resume ID --json`` with the saved workspace as cwd.
    Never emits ``--cd`` or ``--reasoning`` on resume. ``workspace`` is
    kept as an optional cwd hint for seam compat and is never placed on
    the command line.
    """
    if not task_id:
        raise ValueError("missing saved Codex task ID for resume")
    # Back-compat: old callers passed (task_id, workspace, prompt).
    # If the second positional looks like prompt text and no third arg
    # was given, treat it as the prompt. A workspace path hint is
    # otherwise ignored for argv (it becomes cwd at runtime).
    actual_prompt = prompt
    if workspace is not None and not prompt:
        # Heuristic: if workspace contains whitespace/newlines it is
        # almost certainly prompt text, not a path.
        if any(ch in workspace for ch in ("\n", "?", "!", " ")):
            actual_prompt = workspace
            workspace = None
    cmd = [CODEX_BIN, "exec", "resume", task_id, "--json"]
    if last_message_path:
        cmd += ["--output-last-message", last_message_path]
    if actual_prompt:
        cmd += [actual_prompt]
    return cmd


def _codex_id_from_obj(obj: object) -> str | None:
    if isinstance(obj, dict):
        # thread.started / thread envelope first (exact ID preserved).
        for key in ("thread_id", "threadId", "thread-id"):
            val = obj.get(key)
            if isinstance(val, str) and val:
                return val
        typ = obj.get("type")
        if isinstance(typ, str) and "thread" in typ.lower():
            for key in ("id", "thread_id", "session_id", "sessionId"):
                val = obj.get(key)
                if isinstance(val, str) and val:
                    return val
            data = obj.get("thread")
            if isinstance(data, dict):
                for key in ("id", "thread_id", "session_id"):
                    val = data.get(key)
                    if isinstance(val, str) and val:
                        return val
        for key in ("task_id", "taskId", "session_id", "sessionId",
                    "id", "codex_task_id", "conversation_id",
                    "conversationId"):
            val = obj.get(key)
            if isinstance(val, str) and val:
                return val
        data = obj.get("data")
        if isinstance(data, dict):
            found = _codex_id_from_obj(data)
            if found:
                return found
        # Completed agent message envelope may nest the ID.
        for key in ("thread", "session", "conversation"):
            nested = obj.get(key)
            if isinstance(nested, dict):
                found = _codex_id_from_obj(nested)
                if found:
                    return found
    return None


def parse_codex_task_id(output_text: str,
                        last_message_text: str | None = None) -> str | None:
    """Extract a Codex thread/task ID from JSONL stdout + last-message file.

    Prefers ``thread.started`` / ``thread_id`` events, then any
    task/session/conversation ID, preserving the exact string. Falls
    back to the output-last-message envelope when stdout has no ID.
    """
    for blob in (output_text or "", last_message_text or ""):
        if not blob or not blob.strip():
            continue
        text = blob.strip()
        # Whole-payload JSON first.
        try:
            obj = json.loads(text)
            found = _codex_id_from_obj(obj)
            if found:
                return found
            if isinstance(obj, dict):
                for key in ("result", "data", "final", "message"):
                    nested = obj.get(key)
                    if isinstance(nested, dict):
                        found = _codex_id_from_obj(nested)
                        if found:
                            return found
                    if isinstance(nested, str):
                        try:
                            inner = json.loads(nested)
                        except ValueError:
                            continue
                        found = _codex_id_from_obj(inner)
                        if found:
                            return found
        except ValueError:
            pass
        # JSON-lines scan: first thread.started ID wins, else last ID wins.
        first_thread_id = None
        last_id = None
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
            typ = str(obj.get("type") or "")
            found = _codex_id_from_obj(obj)
            if found:
                last_id = found
                if "thread" in typ.lower() and "start" in typ.lower():
                    if first_thread_id is None:
                        first_thread_id = found
        if first_thread_id:
            return first_thread_id
        if last_id:
            return last_id
    return None


def read_last_message_file(path: str | None) -> str | None:
    """Read an output-last-message file when present (None otherwise)."""
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except (FileNotFoundError, OSError, ValueError):
        return None


def parse_luna_envelope_from_texts(*blobs: str | None) -> dict | None:
    """Parse the structured action envelope from stdout + last-message.

    The envelope is the JSON object with a valid ``action`` emitted as
    the completed agent message / output-last-message payload, including
    Codex ``item.completed`` ``agent_message.text``.
    """
    from . import policy as _policy

    def _from_obj(obj, _depth=0):
        if _depth > 8 or obj is None:
            return None
        if isinstance(obj, dict):
            if obj.get("action") in _policy.VALID_ACTIONS:
                return obj
            # Codex item.completed agent_message text, plus common wrappers.
            for key in ("text", "result", "data", "final", "message",
                        "last_message", "lastMessage", "output", "response",
                        "item", "payload", "content"):
                if key in obj:
                    found = _from_obj(obj.get(key), _depth + 1)
                    if found:
                        return found
            for v in obj.values():
                found = _from_obj(v, _depth + 1)
                if found:
                    return found
            return None
        if isinstance(obj, str):
            raw = obj.strip()
            if not raw:
                return None
            try:
                return _from_obj(json.loads(raw), _depth + 1)
            except ValueError:
                return None
        if isinstance(obj, (list, tuple)):
            found = None
            for v in obj:
                got = _from_obj(v, _depth + 1)
                if got:
                    found = got
            return found
        return None

    found = None
    for blob in blobs:
        if not blob or not str(blob).strip():
            continue
        text = blob.strip()
        got = _from_obj(text)
        if got:
            found = got
        for line in text.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            got = _from_obj(obj)
            if got:
                found = got
    return found


def build_claude_cmd(planner_session_id: str, prompt: str,
                     model: str = CLAUDE_MODEL,
                     effort: str = CLAUDE_EFFORT) -> list[str]:
    """Default planner callback: --resume the exact saved session.

    Never creates a new planner session implicitly; missing session is
    a caller error so recover can persist a durable blocked reason.
    """
    if not planner_session_id:
        raise ValueError("missing planner session ID: refusing to fork a new session")
    return [CLAUDE_BIN, "--resume", planner_session_id,
            "--model", model, "--effort", effort,
            "-p", prompt]


def opencode_model_for_allowance(allowance: str) -> str:
    """Map a durable allowance to the installed provider/model route."""
    if allowance == "free":
        return OPENCODE_FREE_MODEL
    if allowance == "go-included":
        return OPENCODE_GO_MODEL
    raise ValueError(f"unsupported allowance: {allowance!r}")


def build_opencode_cmd(workspace: str, prompt: str,
                       model: str | None = None,
                       effort: str | None = None,
                       allowance: str = "free",
                       variant: str = OPENCODE_VARIANT,
                       agent: str = OPENCODE_AGENT,
                       session_id: str | None = None) -> list[str]:
    """Default implementation command using the installed CLI contract.

    ``opencode run --format json --pure --dir WS
    --model opencode/muse-spark-1.3-contributor-free --variant xhigh
    --agent build <prompt>`` free-first; Go uses
    ``opencode-go/muse-spark-1.3-contributor`` only after exact free
    exhaustion. ``allowance`` always selects the model route unless an
    explicit non-default ``model`` is given. ``session_id`` adds
    ``--session`` to resume the saved OpenCode session. Never emits
    nonexistent ``--effort`` or ``--cd``.
    """
    if not workspace:
        raise ValueError("missing workspace for opencode run")
    if allowance not in ("free", "go-included"):
        raise ValueError(f"unsupported allowance: {allowance!r}")
    # Back-compat: old callers passed model=muse-spark-... and
    # effort=xhigh positionally. Map them onto the provider-shaped route.
    if effort is not None and effort != OPENCODE_VARIANT:
        # Old --effort values map to --variant; only xhigh is supported.
        if effort in ("xhigh", "max", "high", "medium", "low"):
            variant = effort if effort == "xhigh" else OPENCODE_VARIANT
        else:
            raise ValueError(f"unsupported variant: {effort!r}")
    if model is None or model in (OPENCODE_MODEL, "muse-spark-1.3-contributor",
                                  OPENCODE_FREE_MODEL, OPENCODE_GO_MODEL):
        # Allowance drives the route for default/legacy model spellings.
        resolved = opencode_model_for_allowance(allowance)
        # An explicit Go model spelling always wins (never downgrade).
        if model == OPENCODE_GO_MODEL:
            resolved = OPENCODE_GO_MODEL
        model = resolved
    elif allowance == "go-included" and "opencode-go/" not in model:
        raise ValueError(
            f"allowance go-included requires an opencode-go/* model, got {model!r}")
    cmd = [OPENCODE_BIN, "run",
           "--format", "json",
           "--pure",
           "--dir", workspace,
           "--model", model,
           "--variant", variant,
           "--agent", agent]
    if session_id:
        cmd += ["--session", session_id]
    cmd += [prompt]
    return cmd


def build_luna_prompt(task_json_text: str, extra: str = "") -> str:
    """Full task content plus the explicit action JSON protocol.

    The complete canonical task JSON is always included; never silently
    clipped. ``extra`` carries resumed answers / implementation evidence.
    """
    body = task_json_text or ""
    prompt = f"TASK (complete, do not truncate):\n{body}\n\n{LUNA_ACTION_PROTOCOL}"
    if extra:
        prompt += f"\n\nCONTEXT:\n{extra}"
    return prompt


def is_planner_busy(output_text: str) -> bool:
    low = (output_text or "").lower()
    return any(m in low for m in BUSY_MARKERS)


def redact_nested(obj):
    """Recursively redact secret-bearing keys; never log credentials."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            lk = str(k).lower()
            if any(s in lk for s in ("secret", "token", "password", "api_key",
                                     "apikey", "credential", "auth", "authorization",
                                     "cookie", "set-cookie")):
                out[k] = "<redacted>"
            else:
                out[k] = redact_nested(v)
        return out
    if isinstance(obj, list):
        return [redact_nested(v) for v in obj]
    if isinstance(obj, str) and len(obj) > 4000:
        return obj[:4000] + "...<truncated>"
    return obj


def generate_control_password(nbytes: int = 24) -> str:
    """Random localhost control password held in process, never logged."""
    return secrets.token_hex(nbytes)


def build_opencode_serve_cmd(hostname: str = "127.0.0.1", port: str | int = 0) -> list[str]:
    """Owned ephemeral serve argv. Password is environment-only, never argv."""
    if hostname not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("ephemeral serve allows localhost only")
    return [OPENCODE_BIN, "serve", "--pure", "--hostname", hostname, "--port", str(port)]


def parse_serve_url(output_text: str) -> str | None:
    """Parse a loopback URL from serve stdout. None if absent."""
    import re as _re
    if not output_text:
        return None
    m = _re.search(r"https?://(?:127\.0\.0\.1|localhost|\[::1\]|::1):\d+", output_text)
    if not m:
        return None
    url = m.group(0)
    _ensure_localhost(url)
    return url


class OpenCodeClient:
    """Authenticated loopback client. Password stays in memory."""

    def __init__(self, base_url: str, password: str | None, request_func=None):
        if not base_url:
            raise ValueError("missing control base_url")
        _ensure_localhost(base_url)
        if request_func is None and not password:
            raise ValueError(
                "missing control password: refusing to invent one for "
                "a server this job does not own")
        self._base_url = base_url.rstrip("/")
        self._password = password or ""
        self._request_func = request_func

    def _http(self, method: str, path: str, body: dict | None = None) -> dict:
        if self._request_func is not None:
            return dict(self._request_func(method, path, body or {}))
        url = self._base_url + path
        data = None
        headers = {"Authorization": "Bearer " + self._password,
                   "Content-Type": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
            raw = resp.read().decode("utf-8", errors="replace")
        try:
            return json.loads(raw) if raw.strip() else {}
        except ValueError:
            return {"raw": raw}

    def create_session(self) -> dict:
        return self._http("POST", "/session", {})

    def session_id_from(self, payload: dict) -> str | None:
        if not isinstance(payload, dict):
            return None
        for key in ("id", "session_id", "sessionId"):
            val = payload.get(key)
            if isinstance(val, str) and val:
                return val
        sess = payload.get("session")
        if isinstance(sess, dict):
            for key in ("id", "session_id", "sessionId"):
                val = sess.get(key)
                if isinstance(val, str) and val:
                    return val
        if isinstance(sess, str) and sess:
            return sess
        return None

    def prompt(self, session_id: str, text: str, model: str | None = None,
               variant: str | None = None) -> dict:
        if not session_id:
            raise ValueError("missing saved OpenCode session ID")
        body = {"session": session_id, "parts": [{"type": "text", "text": text}]}
        if model:
            body["model"] = model
        if variant:
            body["variant"] = variant
        return self._http("POST", f"/session/{urllib.parse.quote(session_id)}/prompt", body)

    def session_status(self, session_id: str) -> dict:
        return OpenCodeControl(self._base_url, self._password, session_id,
                               request_func=self._request_func).session_status()

    def abort(self, session_id: str) -> dict:
        return OpenCodeControl(self._base_url, self._password, session_id,
                               request_func=self._request_func).abort()

    def ensure_idle_ownership(self, session_id: str) -> dict:
        return OpenCodeControl(self._base_url, self._password, session_id,
                               request_func=self._request_func).ensure_idle_ownership()


def _ensure_localhost(base_url: str) -> urllib.parse.ParseResult:
    parts = urllib.parse.urlparse(base_url)
    host = (parts.hostname or "").lower()
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("opencode control seam allows localhost only")
    if parts.scheme not in ("http", "https"):
        raise ValueError("opencode control seam requires http(s)")
    return parts


class OpenCodeControl:
    """Ephemeral per-job OpenCode server/control handle (stdlib only).

    ``password`` stays in process memory and is never written to the DB,
    logs, or events. Only ``session_id`` (the saved OpenCode session) is
    ever observed. ``request_func`` is an injectable equivalent with
    signature ``(method, path, body) -> dict`` for deterministic tests;
    when it is provided no real password is required and no random
    password is invented for a server the controller does not own.
    """

    def __init__(self, base_url: str, password: str | None, session_id: str,
                 request_func=None):
        if not base_url:
            raise ValueError("missing control base_url")
        if not session_id:
            raise ValueError("missing saved OpenCode session ID")
        _ensure_localhost(base_url)
        if request_func is None and not password:
            raise ValueError(
                "missing control password: refusing to invent one for "
                "a server this job does not own")
        self._base_url = base_url.rstrip("/")
        self._password = password or ""
        self._session_id = session_id
        self._request_func = request_func

    @property
    def session_id(self) -> str:
        return self._session_id

    def _http(self, method: str, path: str, body: dict | None = None) -> dict:
        if self._request_func is not None:
            return dict(self._request_func(method, path, body or {}))
        url = self._base_url + path
        data = None
        headers = {"Authorization": "Bearer " + self._password,
                   "Content-Type": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
            raw = resp.read().decode("utf-8", errors="replace")
        try:
            return json.loads(raw) if raw.strip() else {}
        except ValueError:
            return {"raw": raw}

    def session_status(self) -> dict:
        """Observe only the saved session ID (never list other sessions)."""
        qs = urllib.parse.urlencode({"session": self._session_id})
        return self._http("GET", f"/session/status?{qs}", None)

    def abort(self) -> dict:
        """Abort the saved free session before any Go transfer."""
        return self._http("POST", "/session/abort", {"session": self._session_id})

    def ensure_idle_ownership(self) -> dict:
        """Confirm ownership/idle state for the saved session after abort."""
        return self._http("GET", "/session/status?" + urllib.parse.urlencode(
            {"session": self._session_id, "ownership": "1"}), None)


def fetch_session_status(base_url: str, password: str | None, session_id: str,
                         request_func=None) -> dict:
    """Fetch status for exactly one saved session ID (localhost only)."""
    return OpenCodeControl(base_url, password, session_id,
                           request_func=request_func).session_status()


class EphemeralOpenCodeServe:
    """Own an ephemeral per-job ``opencode serve`` process (stdlib only).

    Spawns ``opencode serve --pure --hostname 127.0.0.1 --port 0`` with a
    fresh in-memory ``OPENCODE_SERVER_PASSWORD``. Parses the emitted URL
    (stdout ``http://127.0.0.1:PORT``). Never installs a service, never
    logs the password, never touches unrelated servers. Caller must call
    :meth:`close` (terminates the owned child only).
    """

    def __init__(self, proc: "subprocess.Popen[str]", base_url: str,
                 password: str):
        self._proc = proc
        self._base_url = base_url
        self._password = password

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def pid(self) -> int | None:
        try:
            return self._proc.pid
        except Exception:
            return None

    def control_for(self, session_id: str,
                    request_func=None) -> "OpenCodeControl":
        return OpenCodeControl(self._base_url, self._password, session_id,
                               request_func=request_func)

    def close(self) -> None:
        try:
            self._proc.terminate()
        except Exception:
            pass
        try:
            self._proc.wait(timeout=5)
        except Exception:
            try:
                self._proc.kill()
            except Exception:
                pass


def start_ephemeral_serve(password: str | None = None,
                          hostname: str = "127.0.0.1",
                          timeout: float = 15.0) -> EphemeralOpenCodeServe:
    """Start an owned ephemeral ``opencode serve`` on localhost:0."""
    if hostname not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("ephemeral serve allows localhost only")
    pwd = password or generate_control_password()
    env = dict(os.environ)
    env["OPENCODE_SERVER_PASSWORD"] = pwd
    proc = subprocess.Popen(
        [OPENCODE_BIN, "serve", "--pure", "--hostname", hostname,
         "--port", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, env=env, start_new_session=True, close_fds=True,
    )
    import re as _re
    import time as _time
    url_re = _re.compile(r"https?://(?:127\.0\.0\.1|localhost|::1):\d+")
    end = _time.time() + max(1.0, timeout)
    buf = ""
    base_url: str | None = None
    while _time.time() < end:
        if proc.poll() is not None:
            break
        import select as _select
        try:
            import fcntl as _fcntl
            fd = proc.stdout.fileno() if proc.stdout else -1
            if fd >= 0:
                flags = _fcntl.fcntl(fd, _fcntl.F_GETFL)
                _fcntl.fcntl(fd, _fcntl.F_SETFL, flags | os.O_NONBLOCK)
        except Exception:
            pass
        try:
            chunk = proc.stdout.read(1024) if proc.stdout else ""
        except Exception:
            chunk = ""
        if chunk:
            buf += chunk
            m = url_re.search(buf)
            if m:
                base_url = m.group(0)
                break
        else:
            _time.sleep(0.05)
    if base_url is None:
        try:
            proc.terminate()
        except Exception:
            pass
        raise RuntimeError("ephemeral opencode serve did not emit a localhost URL")
    _ensure_localhost(base_url)
    return EphemeralOpenCodeServe(proc, base_url, pwd)
