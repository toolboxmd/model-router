"""Stdlib-only harness adapters. See RUNNER.md for the full contract.

- Codex dispatcher: ``codex exec --json`` with Luna max and a read-only
  sandbox; resume names the saved thread, model, effort, and sandbox.
- Claude planner: ``claude --resume SID --output-format json --tools ""``
  on the exact saved session; the result must come from that session.
- Implementation: an owned ``opencode serve`` per turn, driven through
  :class:`OpenCodeClient`. ``build_opencode_cmd`` (``opencode run``) is kept
  as a deterministic test seam only.
"""
from __future__ import annotations

import json
import os
import secrets
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request

CODEX_BIN = "codex"
CODEX_MODEL = "gpt-5.6-luna"
CODEX_EFFORT = "max"
CODEX_REASONING_CONFIG = 'model_reasoning_effort="max"'
# The dispatcher coordinates and verifies; it never edits. Codex enforces
# this with its OS sandbox. Implementation belongs to the Muse worker.
CODEX_SANDBOX = "read-only"

CLAUDE_BIN = "claude"
# Production planner default: Fable 5.1 max. Sonnet medium is only the
# explicit bounded live-test override.
CLAUDE_MODEL = "claude-fable-5-1"
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
    "ROLE: you are the dispatcher for this runner job. Your sandbox is "
    "read-only: do not edit files. Ask the saved planner with "
    "planner_question when a decision belongs to the planner. Request code "
    "changes with the implementation action; put complete worker "
    "instructions in payload.instructions. The runner sends them to the "
    "implementation worker and returns its result to you. Inspect the "
    "workspace and run the task's proof before reporting completion. Do not "
    "start other agents or models yourself.\n"
    "REPLY PROTOCOL (required): emit exactly one JSON object as your final "
    "message, on its own line, with one of these shapes:\n"
    '{"action":"planner_question","qid":"q1","prompt":"<question for the human planner>"}\n'
    '{"action":"implementation","artifact":"<path or empty>","payload":{"instructions":"<complete worker instructions>"},"route":"muse-spark-xhigh-free"}\n'
    '{"action":"completion","output":"<final result text>","artifact":"<path or empty>"}\n'
    "Rules: exactly one envelope; valid JSON; action must be one of "
    "planner_question, implementation, completion; use a new qid for each "
    "new question; never invent a new planner or Codex session ID."
)


def build_luna_followup(kind: str, body: str) -> str:
    """Frame a resumed-turn message for the saved Luna task."""
    return f"{kind}:\n{body}\n\n{LUNA_ACTION_PROTOCOL}"


BUSY_MARKERS = (
    "session is busy",
    "already running",
    "session busy",
    "locked by another",
    "busy",
)


# Session-bound variables a parent Claude Code or Codex process exports to
# its tools. A detached child harness must start as its own session, so
# these never pass through. Auth and home locations are kept.
_KEEP_HARNESS_ENV = {"CODEX_HOME", "CLAUDE_CONFIG_DIR"}


def child_harness_env(base: dict | None = None) -> dict:
    env = dict(os.environ if base is None else base)
    for key in list(env):
        if key in _KEEP_HARNESS_ENV:
            continue
        if key in ("CLAUDECODE", "CLAUDE_PID", "CLAUDE_EFFORT") \
                or key.startswith("CLAUDE_CODE_") or key.startswith("CODEX_"):
            env.pop(key, None)
    return env


def default_last_message_path() -> str:
    """Fallback output-last-message path when the caller has no state dir."""
    return os.path.join(tempfile.gettempdir(), "codex-last-message.json")


def build_codex_dispatch_cmd(workspace: str, prompt: str,
                             model: str = CODEX_MODEL,
                             effort: str = CODEX_EFFORT,
                             last_message_path: str | None = None) -> list[str]:
    """Default Codex dispatch using the proved CLI contract.

    ``codex exec --json --output-last-message PATH --model gpt-5.6-luna
    -c model_reasoning_effort="max" --sandbox read-only --cd WS``.
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


def build_codex_resume_cmd(task_id: str, prompt: str = "",
                           last_message_path: str | None = None,
                           model: str = CODEX_MODEL) -> list[str]:
    """Resume only the saved Codex thread/task ID (never fork).

    ``codex exec resume ID --json -m gpt-5.6-luna -c
    model_reasoning_effort="max" -c sandbox_mode="read-only"`` with the
    saved workspace as cwd. Model, effort, and sandbox are explicit on
    every resume; ``resume`` has no ``--cd`` or ``--sandbox`` flag.
    """
    if not task_id:
        raise ValueError("missing saved Codex task ID for resume")
    cmd = [CODEX_BIN, "exec", "resume", task_id, "--json",
           "-m", model, "-c", CODEX_REASONING_CONFIG,
           "-c", f'sandbox_mode="{CODEX_SANDBOX}"']
    if last_message_path:
        cmd += ["--output-last-message", last_message_path]
    if prompt:
        cmd += [prompt]
    return cmd


def _codex_id_from_obj(obj: object) -> str | None:
    """Thread ID from ``thread.started`` or an explicit ``thread_id`` key."""
    if not isinstance(obj, dict):
        return None
    for key in ("thread_id", "threadId"):
        val = obj.get(key)
        if isinstance(val, str) and val:
            return val
    if obj.get("type") == "thread.started":
        val = obj.get("id")
        if isinstance(val, str) and val:
            return val
    return None


def parse_codex_task_id(output_text: str,
                        last_message_text: str | None = None) -> str | None:
    """Extract the Codex thread ID from ``codex exec --json`` output.

    The first ``thread.started`` event wins. Item, message, and other
    IDs are never treated as the task ID. ``last_message_text`` is kept
    for call compatibility and is not an ID source.
    """
    for line in (output_text or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        found = _codex_id_from_obj(obj)
        if found:
            return found
    return None


def last_message_path_from_cmd(cmd) -> str | None:
    if not isinstance(cmd, list):
        return None
    for flag in ("--output-last-message", "-o"):
        if flag in cmd:
            i = cmd.index(flag)
            if i + 1 < len(cmd):
                return str(cmd[i + 1])
    return None


def _envelope_from_message_text(text: str) -> dict | None:
    from . import policy as _policy
    text = (text or "").strip()
    if not text:
        return None
    candidates = [text] + [l.strip() for l in reversed(text.splitlines())]
    if text.startswith("```"):
        candidates.append(text.strip("`").partition("\n")[2])
    for cand in candidates:
        if not cand.startswith("{"):
            continue
        try:
            obj = json.loads(cand)
        except ValueError:
            continue
        if isinstance(obj, dict) and obj.get("action") in _policy.VALID_ACTIONS:
            return obj
    return None


def parse_codex_agent_envelope(output_text: str,
                               last_message_text: str | None = None) -> dict | None:
    """Parse Luna's action from its own final agent message only.

    Uses the last ``item.completed`` ``agent_message`` text in the JSONL
    stream, else the ``--output-last-message`` file. Command output and
    other items can contain arbitrary JSON and are never parsed.
    """
    last_text = None
    for line in (output_text or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if not isinstance(obj, dict) or obj.get("type") != "item.completed":
            continue
        item = obj.get("item")
        if isinstance(item, dict) and item.get("type") == "agent_message" \
                and isinstance(item.get("text"), str):
            last_text = item["text"]
    if last_text is not None:
        found = _envelope_from_message_text(last_text)
        if found:
            return found
    return _envelope_from_message_text(last_message_text or "")


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
    JSON output lets the runner verify the resumed session identity.
    """
    if not planner_session_id:
        raise ValueError("missing planner session ID: refusing to fork a new session")
    # The callback asks for a decision only: no tools, so a resumed
    # planner turn cannot take new actions on the runner's behalf.
    return [CLAUDE_BIN, "--resume", planner_session_id,
            "--model", model, "--effort", effort,
            "--output-format", "json", "--tools", "",
            "-p", prompt]


def parse_claude_result(stdout: str) -> dict:
    """Parse ``claude -p --output-format json``.

    Returns ``{ok, answer, session_id, error}``. Only a ``result`` object
    with ``is_error`` false and a non-empty string result is an answer.
    """
    obj = None
    text = (stdout or "").strip()
    if text:
        try:
            obj = json.loads(text)
        except ValueError:
            for line in reversed(text.splitlines()):
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    cand = json.loads(line)
                except ValueError:
                    continue
                if isinstance(cand, dict) and cand.get("type") == "result":
                    obj = cand
                    break
    if not isinstance(obj, dict) or obj.get("type") != "result":
        return {"ok": False, "answer": None, "session_id": None,
                "error": "planner output is not a Claude JSON result"}
    sid = obj.get("session_id") if isinstance(obj.get("session_id"), str) else None
    result = obj.get("result")
    if obj.get("is_error") or obj.get("subtype") not in (None, "success"):
        kind = obj.get("subtype") if obj.get("subtype") not in (None, "success") else "is_error"
        status = obj.get("api_error_status")
        return {"ok": False, "answer": None, "session_id": sid,
                "error": f"planner result error: {str(kind)[:80]}"
                         + (f" (API status {status})" if status else ""),
                "detail": str(result or "")[:500]}
    if not isinstance(result, str) or not result.strip():
        return {"ok": False, "answer": None, "session_id": sid,
                "error": "planner result is empty"}
    return {"ok": True, "answer": result.strip(), "session_id": sid, "error": None}


def planner_session_in_use(planner_session_id: str, exclude_pids=()) -> list[int]:
    """PIDs of running ``claude`` processes naming this session in argv.

    Detects a planner started with ``--session-id``/``--resume``. An
    interactive session opened without the ID in argv is not visible.
    """
    if not planner_session_id:
        return []
    try:
        out = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True,
                             text=True, timeout=5).stdout
    except Exception:
        return []
    found = []
    for line in out.splitlines():
        line = line.strip()
        pid_s, _, command = line.partition(" ")
        try:
            pid = int(pid_s)
        except ValueError:
            continue
        if pid in exclude_pids or pid == os.getpid():
            continue
        argv = command.split()
        # Native binary, or an interpreter/shell wrapper running it.
        if not any(os.path.basename(a) == CLAUDE_BIN for a in argv[:3]):
            continue
        if planner_session_id in argv:
            found.append(pid)
    return found


def opencode_model_for_route(route: str | None) -> str:
    return OPENCODE_GO_MODEL if route == "muse-spark-xhigh-go" else OPENCODE_FREE_MODEL


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


def _ensure_localhost(base_url: str) -> urllib.parse.ParseResult:
    parts = urllib.parse.urlparse(base_url)
    host = (parts.hostname or "").lower()
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("opencode control seam allows localhost only")
    if parts.scheme not in ("http", "https"):
        raise ValueError("opencode control seam requires http(s)")
    return parts


def split_model(model: str) -> dict:
    """``provider/model`` -> the server's ``{providerID, modelID}`` object."""
    provider, sep, model_id = (model or "").partition("/")
    if not sep or not provider or not model_id:
        raise ValueError(f"model must be provider/model, got {model!r}")
    return {"providerID": provider, "modelID": model_id}


# Harness write boundary for the owned implementation session. Session
# rules are appended to the build agent's rules. Anything outside the
# served directory is denied; subagents, interactive questions, doom-loop
# prompts, and web access are denied so a headless session cannot block
# on an approval or recurse into other workers. Bash is not OS-confined
# by OpenCode; the live proof checks the fixture diff separately.
SESSION_PERMISSION_RULES = (
    {"permission": "external_directory", "pattern": "*", "action": "deny"},
    {"permission": "task", "pattern": "*", "action": "deny"},
    {"permission": "question", "pattern": "*", "action": "deny"},
    {"permission": "doom_loop", "pattern": "*", "action": "deny"},
    {"permission": "webfetch", "pattern": "*", "action": "deny"},
    {"permission": "websearch", "pattern": "*", "action": "deny"},
)


class OpenCodeHTTPError(RuntimeError):
    def __init__(self, status: int, body: str):
        super().__init__(f"opencode server HTTP {status}")
        self.status = status
        self.body = (body or "")[:2000]


class OpenCodeClient:
    """Client for an owned ``opencode serve`` (API of OpenCode 1.18.31).

    Basic auth ``opencode:<password>``; the password stays in memory.
    Every call is scoped with ``directory=<workspace>``. Only the saved
    session is observed or aborted. ``request_func(method, path, body)``
    is the deterministic test seam; ``path`` includes the query string.
    """

    USERNAME = "opencode"

    def __init__(self, base_url: str, password: str | None,
                 directory: str | None = None, request_func=None):
        if not base_url:
            raise ValueError("missing control base_url")
        _ensure_localhost(base_url)
        if request_func is None and not password:
            raise ValueError(
                "missing control password: refusing to invent one for "
                "a server this job does not own")
        self._base_url = base_url.rstrip("/")
        self._password = password or ""
        self._directory = directory
        self._request_func = request_func

    def _path(self, path: str, query: dict | None = None) -> str:
        q = dict(query or {})
        if self._directory:
            q["directory"] = self._directory
        return path + ("?" + urllib.parse.urlencode(q) if q else "")

    def _http(self, method: str, path: str, body=None, query: dict | None = None):
        full = self._path(path, query)
        if self._request_func is not None:
            return self._request_func(method, full, body)
        import base64
        token = base64.b64encode(
            f"{self.USERNAME}:{self._password}".encode("utf-8")).decode("ascii")
        headers = {"Authorization": "Basic " + token}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self._base_url + full, data=data,
                                     headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            try:
                detail = e.read().decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            raise OpenCodeHTTPError(e.code, detail) from None
        if not raw.strip():
            return {}
        try:
            return json.loads(raw)
        except ValueError:
            return {"raw": raw[:2000]}

    def health(self):
        return self._http("GET", "/global/health")

    def create_session(self, title: str = "model-router runner",
                       permission=SESSION_PERMISSION_RULES) -> dict:
        body = {"title": title}
        if permission:
            body["permission"] = [dict(r) for r in permission]
        got = self._http("POST", "/session", body)
        return got if isinstance(got, dict) else {}

    @staticmethod
    def session_id_from(payload) -> str | None:
        if isinstance(payload, dict):
            val = payload.get("id")
            if isinstance(val, str) and val.startswith("ses"):
                return val
        return None

    def prompt_async(self, session_id: str, text: str, model: str,
                     variant: str | None = OPENCODE_VARIANT,
                     agent: str | None = OPENCODE_AGENT):
        if not session_id:
            raise ValueError("missing saved OpenCode session ID")
        body = {"parts": [{"type": "text", "text": text}],
                "model": split_model(model)}
        if agent:
            body["agent"] = agent
        if variant:
            body["variant"] = variant
        return self._http("POST",
                          f"/session/{urllib.parse.quote(session_id)}/prompt_async",
                          body)

    def status_map(self) -> dict:
        got = self._http("GET", "/session/status")
        return got if isinstance(got, dict) else {}

    def session_status(self, session_id: str) -> dict:
        """Status of the saved session only. Absent means idle."""
        entry = self.status_map().get(session_id)
        if isinstance(entry, dict) and entry.get("type"):
            return entry
        return {"type": "idle"}

    def abort(self, session_id: str):
        return self._http("POST", f"/session/{urllib.parse.quote(session_id)}/abort")

    def messages(self, session_id: str) -> list:
        got = self._http("GET", f"/session/{urllib.parse.quote(session_id)}/message")
        return got if isinstance(got, list) else []

    def wait_idle(self, session_id: str, timeout: float = 30.0,
                  interval: float = 0.5) -> dict:
        """Poll until the saved session is idle. Returns the last status."""
        import time as _time
        end = _time.monotonic() + max(0.0, timeout)
        last = {"type": "unknown"}
        while True:
            last = self.session_status(session_id)
            if last.get("type") == "idle":
                return {"idle": True, "status": last}
            if _time.monotonic() >= end:
                return {"idle": False, "status": last}
            _time.sleep(interval)


def trusted_free_exhaustion(status: dict | None = None,
                            message_error: dict | None = None) -> dict | None:
    """Provider evidence for free-allowance exhaustion, or None.

    Only two server-generated sources count: the owned session's retry
    status with reason ``free_tier_limit`` from provider ``opencode``, or
    an assistant ``APIError`` whose provider ``responseBody`` names
    ``FreeUsageLimitError`` (OpenCode's own rule). Model-authored text,
    generic 429, and Go limits never count.
    """
    if isinstance(status, dict) and status.get("type") == "retry":
        action = status.get("action")
        if isinstance(action, dict) and action.get("reason") == "free_tier_limit" \
                and action.get("provider") == "opencode":
            return {"source": "session_status", "type": "retry",
                    "reason": "free_tier_limit", "provider": "opencode",
                    "attempt": status.get("attempt"), "next": status.get("next"),
                    "message": str(status.get("message") or "")[:500]}
    if isinstance(message_error, dict) and message_error.get("name") == "APIError":
        data = message_error.get("data") if isinstance(message_error.get("data"), dict) else {}
        body = data.get("responseBody")
        if isinstance(body, str) and "FreeUsageLimitError" in body:
            return {"source": "assistant_error", "name": "APIError",
                    "statusCode": data.get("statusCode"),
                    "responseBody": body[:2000]}
    return None


def assistant_messages_after(messages: list, baseline_ids: set) -> list:
    out = []
    for m in messages or []:
        info = m.get("info") if isinstance(m, dict) else None
        if not isinstance(info, dict) or info.get("id") in baseline_ids:
            continue
        if info.get("role") == "assistant":
            out.append(m)
    return out


def message_text(message: dict) -> str:
    parts = message.get("parts") if isinstance(message, dict) else None
    texts = []
    for p in parts or []:
        if isinstance(p, dict) and p.get("type") == "text" and isinstance(p.get("text"), str):
            texts.append(p["text"])
    return "\n".join(texts)
