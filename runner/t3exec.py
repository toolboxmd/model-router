"""T3 execution path for the durable local runner. Stdlib only.

Every job names a planner T3 thread (``submit --planner-t3-thread``):
dispatcher and worker invocations run as T3 child threads of that
planner thread, and questions and the terminal job state are posted
into the planner thread as messages (#106, #110).

Wire reference: toolboxmd/t3code ``packages/contracts`` —
``POST /api/orchestration/dispatch`` carries a client orchestration
command (``thread.create``, ``thread.turn.start``, ...), and
``GET /api/orchestration/threads/:threadId`` returns the thread
detail snapshot (messages, activities, session, latestTurn).

Child identity: a child id follows the fork convention
``sub.<parent>.<suffix>`` (toolboxmd/t3code#8) and the ``thread.create``
payload additionally carries ``parentThreadId``, so a server that honors
the field keeps working without a runner change. A job's threads form a
tree (#113): the dispatcher thread is a child of the planner thread, and
worker, correction and recovery threads are children of the dispatcher
thread. Every child's first message names the job and links the planner
thread.

Liveness is activity-based, never a fixed silence window: a turn is
healthy while assistant tokens stream (message ``updatedAt`` advances)
or a ``tool.*`` / ``task.*`` call is still open. Silence with no running tool past
``T3_SILENCE_SECS`` (≈ a minute) is flagged and probed immediately
with a fresh snapshot read; explicit provider errors act at once. A
long test run holds a running tool activity, so it is never mistaken
for a stall.

Restart: after a T3 server restart with
``continueThreadsAfterServerUpdate`` on, interrupted Claude, Codex and
OpenCode turns continue on the same thread; Grok turns are re-sent by
the router (see :func:`post_restart_action`).

Safety: this module never stops, restarts, or reconfigures a T3
server. It only creates child threads, starts turns, reads snapshots,
and posts messages. The user's own T3 instance (port 3773,
``~/.t3/userdata``) is never touched by tests: tests use a fake
orchestration server on a free port (see ``tests/test_issue106.py``).
"""
from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from . import policy

# The user's own daily T3. Operational rule (toolboxmd/model-router#106):
# tests and live verification run against an isolated T3 server with a
# /tmp home and a free port; this address is never stopped or
# reconfigured by the runner (nothing here stops any server at all).
T3_USER_PORT = 3773
T3_DEFAULT_URL = f"http://127.0.0.1:{T3_USER_PORT}"
# The fork's Prism snapshot endpoint (toolboxmd/t3code#19).
PRISM_SNAPSHOT_PATH = "/api/prism/snapshot"

# Planner harness name for T3-hosted planners. Stored in jobs.planner_harness
# alongside claude/codex/opencode/grok; selecting it is not required to use
# the T3 path (naming a planner T3 thread is), but it records where the
# planner session lives.
T3_PLANNER_HARNESS = "t3"

# Activity-based liveness: silence with no running tool is flagged past
# this window (about a minute) and probed immediately with a fresh read.
T3_SILENCE_SECS = 60.0
# Poll interval while watching a T3 turn.
T3_POLL_SECS = 2.0

# A running activity of these kinds keeps a turn healthy regardless of
# message silence (a long test run is a running tool, never a stall).
TOOL_ACTIVITY_PREFIXES = ("tool.", "task.")

# Reasoning-effort option id per T3 driver, as each adapter reads it
# (CodexAdapter/GrokAdapter ``reasoningEffort``, ClaudeAdapter ``effort``,
# OpenCodeAdapter ``variant``). A route's driver follows its instance id.
T3_EFFORT_OPTION = {
    "codex": "reasoningEffort",
    "grok": "reasoningEffort",
    "claudeAgent": "effort",
    "opencode": "variant",
}

# Dispatcher turns coordinate; worker turns implement. The dispatcher
# stays in the default interaction mode: T3's Codex plan mode asks for
# ``<proposed_plan>`` blocks and user-input requests, which would bury the
# dispatcher envelope or wait on a person. ``auto`` lets the provider's
# own reviewer approve the dispatcher's read commands.
T3_RUNTIME_MODES = {
    "dispatch": {"runtimeMode": "auto", "interactionMode": "default"},
    "implementation": {"runtimeMode": "full-access", "interactionMode": "default"},
    "correction": {"runtimeMode": "full-access", "interactionMode": "default"},
    "recovery": {"runtimeMode": "full-access", "interactionMode": "default"},
}

# Error text -> provider signal. Structured evidence only where the
# snapshot carries it (session.lastError, error-tone activities); model
# text is never classified.
_EXHAUSTED_MARKERS = (
    "usage limit", "usage_limit", "usagelimit", "free_tier_limit",
    "freeusagelimiterror", "gousagelimiterror", "insufficient_quota",
    "quota exceeded", "quota_exceeded", "out of credit", "credits exhausted",
)
_OVERLOADED_MARKERS = (
    "overloaded", "overloaded_error", "rate_limit", "ratelimit",
    "rate limit exceeded", "rate_limit_exceeded", "account_rate_limit",
    "http 503", "http 529", " 503 ", " 529 ",
)
_HARD_MARKERS = (
    "unauthorized", "authentication", "missing bearer", "missing authentication",
    "invalid api key", "auth failed", "autherror", "forbidden", " 401 ", " 403 ",
)


class T3Error(Exception):
    """A T3 orchestration call failed (unreachable server, auth, bad payload)."""


# ---------------------------------------------------------------------------
# Job mode and thread identity
# ---------------------------------------------------------------------------

def is_t3_job(job: dict | None) -> bool:
    """True when the job names a planner T3 thread (every job since #110)."""
    tid = (job or {}).get("planner_t3_thread")
    return isinstance(tid, str) and bool(tid.strip())


def validate_thread_id(thread_id: str) -> str:
    """A planner/child T3 thread id, stripped. Raises ValueError when bad."""
    tid = (thread_id or "").strip()
    if not tid:
        raise ValueError("missing T3 thread id")
    if len(tid) > 256:
        raise ValueError(f"T3 thread id too long: {tid[:64]!r}...")
    for ch in tid:
        if not (ch.isalnum() or ch in "._-:"):
            raise ValueError(f"invalid T3 thread id: {tid!r}")
    if ".." in tid or tid.startswith((".", "-", ":")) or tid.endswith("."):
        raise ValueError(f"invalid T3 thread id: {tid!r}")
    return tid


def child_thread_id(parent_thread_id: str, suffix: str | None = None) -> str:
    """Child id for the spike convention ``sub.<parent>.<suffix>``.

    Used until the fork parent link lands (toolboxmd/t3code#8); the
    ``thread.create`` payload also carries ``parentThreadId`` so a fork
    server honors the link either way.
    """
    parent = validate_thread_id(parent_thread_id)
    suf = suffix or secrets.token_hex(4)
    suf = "".join(c for c in suf if c.isalnum() or c in "_-")[:32] or secrets.token_hex(4)
    return f"sub.{parent}.{suf}"


def parent_of_child(child_thread_id_: str) -> str | None:
    """The planner thread id from a spike-convention child id, else None."""
    tid = (child_thread_id_ or "").strip()
    if not tid.startswith("sub."):
        return None
    rest = tid[len("sub."):]
    if "." not in rest:
        return None
    parent, _suffix = rest.rsplit(".", 1)
    try:
        validate_thread_id(parent)
    except ValueError:
        return None
    return parent


# ---------------------------------------------------------------------------
# Server and token discovery
# ---------------------------------------------------------------------------

def discover_server_url(explicit: str | None = None) -> str:
    """T3 server URL: explicit submit flag wins, then T3_SERVER_URL, then default."""
    for cand in (explicit, os.environ.get("T3_SERVER_URL")):
        if isinstance(cand, str) and cand.strip():
            return cand.strip().rstrip("/")
    return T3_DEFAULT_URL


def _issue_token_via_cli(timeout_secs: float = 15.0) -> str | None:
    """Bearer token via ``t3 auth session issue``; None when unavailable.

    The CLI runs against the T3 home in T3CODE_HOME (``T3_HOME`` is
    accepted as an alias; default ``~/.t3``), so an isolated verification
    server with a /tmp home issues its own token without touching the
    user's home. ``--token-only`` prints the raw
    token; anything else is ignored.
    """
    binary = os.environ.get("T3_BIN") or shutil.which("t3")
    if not binary:
        return None
    home = os.environ.get("T3CODE_HOME") or os.environ.get("T3_HOME")
    cmd = [binary, "auth", "session", "issue", "--token-only"]
    env = dict(os.environ)
    if home:
        env["T3CODE_HOME"] = home
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout_secs, stdin=subprocess.DEVNULL,
                              env=env)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    token = (proc.stdout or "").strip().splitlines()
    token = [ln.strip() for ln in token if ln.strip()]
    if not token:
        return None
    return token[-1]


def discover_token(explicit: str | None = None) -> str:
    """Bearer token: explicit value wins, then T3_SERVER_TOKEN, then the CLI.

    Raises T3Error when no token is available: the caller blocks with the
    reason, so a broken T3 setup is always visible.
    """
    for cand in (explicit, os.environ.get("T3_SERVER_TOKEN")):
        if isinstance(cand, str) and cand.strip():
            return cand.strip()
    token = _issue_token_via_cli()
    if token:
        return token
    raise T3Error("no T3 bearer token: set T3_SERVER_TOKEN or provide "
                  "`t3 auth session issue` (T3_BIN/T3CODE_HOME honored)")


# ---------------------------------------------------------------------------
# Route mapping: policy route -> T3 model selection
# ---------------------------------------------------------------------------

def route_instance_id(route: str) -> str:
    """T3 provider instance id for a policy or snapshot route."""
    return policy.route_spec(route)["instance"]


def route_driver(route: str) -> str:
    """T3 driver kind for a route: the known driver its instance id names."""
    instance = route_instance_id(route)
    return next((d for d in T3_EFFORT_OPTION if instance.startswith(d)), instance)


def route_model_id(route: str) -> str:
    """T3 model slug for a route (OpenCode keeps ``<provider>/<model>``,
    which is what separates Zen free from Go)."""
    model = policy.route_spec(route).get("model") or ""
    if not model:
        raise ValueError(f"route {route!r} carries no model")
    return model


def route_effort(route: str) -> str | None:
    """Reasoning effort for a route, or None for the provider default."""
    return policy.route_spec(route).get("effort")


def route_model_selection(route: str, role: str | None = None) -> dict:
    """T3 ``ModelSelection`` for a route: instance, model, effort.

    Options use the canonical ``[{id, value}]`` array with the option id
    the route's T3 adapter reads; OpenCode turns also carry their agent
    (``plan`` for the dispatcher, ``build`` otherwise). Unknown routes
    raise (never a silent substitution).
    """
    driver = route_driver(route)
    selection: dict = {"instanceId": route_instance_id(route),
                       "model": route_model_id(route)}
    options: list[dict] = []
    effort = route_effort(route)
    if effort:
        options.append({"id": T3_EFFORT_OPTION.get(driver, "effort"), "value": effort})
    if driver == "opencode":
        is_dispatch = (role or policy.route_spec(route).get("role")) == "dispatch"
        options.append({"id": "agent", "value": "plan" if is_dispatch else "build"})
    if options:
        selection["options"] = options
    return selection


def runtime_modes(role: str) -> dict:
    """T3 runtime/interaction modes for a turn role (dispatch is plan-mode)."""
    return dict(T3_RUNTIME_MODES.get(role, T3_RUNTIME_MODES["implementation"]))


# ---------------------------------------------------------------------------
# Command builders (POST /api/orchestration/dispatch payloads)
# ---------------------------------------------------------------------------

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(8)}"


def child_create_command(child_id: str, parent_thread_id: str, project_id: str,
                         title: str, route: str, role: str = "implementation") -> dict:
    """``thread.create`` for a job child: route selection plus parent link.

    ``parentThreadId`` is forward-compatibility for the fork link
    (toolboxmd/t3code#8); until it lands the ``sub.<parent>.<suffix>``
    id is the link.
    """
    modes = runtime_modes(role)
    return {
        "type": "thread.create",
        "commandId": _new_id("cmd"),
        "threadId": validate_thread_id(child_id),
        "projectId": project_id,
        "title": title[:200] or f"model-router {child_id}",
        "modelSelection": route_model_selection(route, role),
        "runtimeMode": modes["runtimeMode"],
        "interactionMode": modes["interactionMode"],
        "branch": None,
        "worktreePath": None,
        "createdAt": _utcnow_iso(),
        "parentThreadId": validate_thread_id(parent_thread_id),
    }


def turn_start_command(thread_id: str, text: str, route: str | None = None,
                       role: str = "implementation",
                       title_seed: str | None = None,
                       message_id: str | None = None) -> dict:
    """``thread.turn.start`` carrying one user message on an existing thread.

    On an existing thread T3 keeps the thread's own runtime and
    interaction modes (the decider ignores the command's), so a post into
    the planner thread never changes how the planner runs.
    """
    cmd: dict = {
        "type": "thread.turn.start",
        "commandId": _new_id("cmd"),
        "threadId": validate_thread_id(thread_id),
        "message": {
            "messageId": message_id or _new_id("msg"),
            "role": "user",
            "text": text,
            "attachments": [],
        },
        "runtimeMode": runtime_modes(role)["runtimeMode"],
        "interactionMode": runtime_modes(role)["interactionMode"],
        "createdAt": _utcnow_iso(),
    }
    if route is not None:
        cmd["modelSelection"] = route_model_selection(route, role)
    if title_seed:
        cmd["titleSeed"] = title_seed[:200]
    return cmd


def child_first_message(request_id: str, kind_label: str, planner_thread_id: str,
                        route: str, prompt: str) -> str:
    """First message on a job child: names the job and links the planner thread."""
    return (
        f"[model-router job {request_id} {kind_label} on route {route}; "
        f"planner thread {planner_thread_id}]\n\n"
        f"This turn belongs to model-router job {request_id}. "
        f"Report back in this thread; the planner follows from thread {planner_thread_id}.\n\n"
        f"{prompt}"
    )


def terminal_text(request_id: str, status: str, pr_url: str | None,
                  reason: str | None, summary: str | None = None) -> str:
    """Terminal job message posted into the planner thread.

    Deferred to the controller's canonical terminal-report text so the
    T3 post can never drift from the harness-callback wording.
    """
    from . import controller as _controller

    return _controller._terminal_report_text(request_id, status, pr_url,
                                             reason, summary)


# ---------------------------------------------------------------------------
# HTTP client (stdlib)
# ---------------------------------------------------------------------------

class T3Client:
    """Minimal T3 orchestration client: dispatch, thread snapshot."""

    def __init__(self, server_url: str, token: str, timeout_secs: float = 15.0):
        self.server_url = (server_url or "").rstrip("/") or T3_DEFAULT_URL
        self.token = token
        self.timeout_secs = timeout_secs

    def _request(self, method: str, path: str,
                 payload: dict | None = None) -> dict:
        url = f"{self.server_url}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.token}")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_secs) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            try:
                detail = e.read().decode("utf-8", errors="replace")[:500]
            except Exception:
                detail = ""
            raise T3Error(f"T3 {method} {path} failed: HTTP {e.code} {detail}") from e
        except OSError as e:
            raise T3Error(f"T3 {method} {path} unreachable at {self.server_url}: {e}") from e
        try:
            obj = json.loads(body) if body.strip() else {}
        except ValueError as e:
            raise T3Error(f"T3 {method} {path} returned non-JSON") from e
        return obj if isinstance(obj, dict) else {"value": obj}

    def dispatch(self, command: dict) -> dict:
        """POST one orchestration command; returns the dispatch result."""
        if not isinstance(command, dict) or not command.get("type"):
            raise ValueError("T3 dispatch needs a command with a type")
        return self._request("POST", "/api/orchestration/dispatch", command)

    def thread_snapshot(self, thread_id: str) -> dict:
        """GET the thread detail snapshot (messages, activities, session)."""
        tid = validate_thread_id(thread_id)
        return self._request("GET", f"/api/orchestration/threads/{tid}")

    def post_message(self, thread_id: str, text: str,
                     route: str | None = None, role: str = "implementation",
                     title_seed: str | None = None,
                     message_id: str | None = None) -> dict:
        """Post one user message as a turn on an existing thread."""
        return self.dispatch(turn_start_command(thread_id, text, route, role,
                                                title_seed, message_id))

    def prism_snapshot(self, project_id: str | None = None) -> dict:
        """GET the Prism provider snapshot (models, usage windows, roles)."""
        path = PRISM_SNAPSHOT_PATH
        if project_id:
            path += "?projectId=" + urllib.parse.quote(project_id, safe="")
        return self._request("GET", path)

    def create_child(self, child_id: str, parent_thread_id: str,
                     project_id: str, title: str, route: str,
                     role: str = "implementation") -> dict:
        """Create a job child thread linked to the planner thread."""
        return self.dispatch(child_create_command(child_id, parent_thread_id,
                                                  project_id, title, route, role))


def client_for_job(job: dict, server_url: str | None = None,
                   token: str | None = None) -> T3Client:
    """Client for a T3 job: stored server URL wins, else discovery."""
    stored = (job or {}).get("t3_server_url")
    url = discover_server_url(server_url or (stored if isinstance(stored, str) else None))
    return T3Client(url, discover_token(token))


# ---------------------------------------------------------------------------
# Snapshot reading and activity-based liveness
# ---------------------------------------------------------------------------

def _parse_ts(value) -> float | None:
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def snapshot_thread(snapshot: dict) -> dict:
    """The thread object inside a detail snapshot (or {})."""
    if not isinstance(snapshot, dict):
        return {}
    thread = snapshot.get("thread")
    return thread if isinstance(thread, dict) else {}


def latest_assistant_text(snapshot: dict, turn_id: str | None = None) -> str:
    """Text of the last assistant message (of ``turn_id`` when given)."""
    thread = snapshot_thread(snapshot)
    texts = [m for m in (thread.get("messages") or [])
             if isinstance(m, dict) and m.get("role") == "assistant"
             and (turn_id is None or m.get("turnId") == turn_id)]
    if not texts:
        return ""
    last = texts[-1]
    return last.get("text") if isinstance(last.get("text"), str) else ""


_TOOL_CLOSE_KINDS = ("tool.completed", "tool.denied", "task.completed")


def _activity_order(act: dict) -> tuple:
    seq = act.get("sequence")
    return (_parse_ts(act.get("createdAt")) or 0.0,
            seq if isinstance(seq, int) else 0)


_WAIT_OPEN = ("approval.requested", "user-input.requested")
_WAIT_CLOSE = ("approval.resolved", "user-input.resolved")


def pending_waits(snapshot: dict, turn_id: str | None = None) -> list[dict]:
    """Unresolved approval / user-input requests of the turn.

    A tool held behind an approval nobody grants is not running: the
    turn is waiting, so silence counts against it like any other.
    """
    thread = snapshot_thread(snapshot)
    acts = [a for a in (thread.get("activities") or [])
            if isinstance(a, dict) and a.get("kind") in _WAIT_OPEN + _WAIT_CLOSE
            and not (turn_id and isinstance(a.get("turnId"), str)
                     and a.get("turnId") != turn_id)]
    acts.sort(key=_activity_order)
    open_waits: dict = {}
    for i, act in enumerate(acts):
        payload = act.get("payload") if isinstance(act.get("payload"), dict) else {}
        rid = payload.get("requestId")
        key = rid if isinstance(rid, str) and rid else f"anon-{i}"
        if act["kind"] in _WAIT_OPEN:
            open_waits[key] = act
        elif isinstance(rid, str) and rid:
            open_waits.pop(rid, None)
        elif open_waits:
            open_waits.pop(next(iter(open_waits)))
    return list(open_waits.values())


def running_tool_activities(snapshot: dict, turn_id: str | None = None) -> list[dict]:
    """``tool.*`` / ``task.*`` activities still running in the turn.

    T3 activities are an append-only log: ``tool.started`` (or
    ``tool.updated`` / ``tool.progress``) opens a call keyed by
    ``payload.toolCallId`` until its ``tool.completed`` / ``tool.denied``;
    ``task.started`` / ``task.progress`` / ``task.updated`` open a task
    keyed by ``payload.taskId`` until ``task.completed``. Only calls still
    open count: a long test run holds its tool open and is never a stall,
    while a finished tool no longer keeps a silent model alive. Scoped to
    the active turn when known.
    """
    thread = snapshot_thread(snapshot)
    acts = []
    for act in (thread.get("activities") or []):
        if not isinstance(act, dict):
            continue
        kind = act.get("kind") if isinstance(act.get("kind"), str) else ""
        if not kind.startswith(TOOL_ACTIVITY_PREFIXES):
            continue
        if turn_id and isinstance(act.get("turnId"), str) and act.get("turnId") != turn_id:
            continue
        acts.append(act)
    acts.sort(key=_activity_order)
    open_calls: dict = {}
    anon = 0
    for act in acts:
        kind = act["kind"]
        payload = act.get("payload") if isinstance(act.get("payload"), dict) else {}
        family = "task" if kind.startswith("task.") else "tool"
        ident = payload.get("taskId") if family == "task" else payload.get("toolCallId")
        if kind in _TOOL_CLOSE_KINDS:
            if isinstance(ident, str) and ident:
                open_calls.pop((family, ident), None)
            else:
                # An anonymous close ends the oldest anonymous open call.
                for key in list(open_calls):
                    if key[0] == family and key[1].startswith("anon-"):
                        open_calls.pop(key)
                        break
            continue
        if isinstance(ident, str) and ident:
            open_calls[(family, ident)] = act
        elif kind in ("tool.started", "task.started"):
            anon += 1
            open_calls[(family, f"anon-{anon}")] = act
    return list(open_calls.values())


def streaming_messages(snapshot: dict) -> list[dict]:
    """Assistant messages still streaming (tokens flowing)."""
    thread = snapshot_thread(snapshot)
    return [m for m in (thread.get("messages") or [])
            if isinstance(m, dict) and m.get("role") == "assistant"
            and m.get("streaming") is True]


def last_activity_ts(snapshot: dict) -> float | None:
    """Newest message/activity timestamp in the snapshot, if any."""
    thread = snapshot_thread(snapshot)
    best: float | None = None
    for m in (thread.get("messages") or []):
        if not isinstance(m, dict):
            continue
        ts = _parse_ts(m.get("updatedAt")) or _parse_ts(m.get("createdAt"))
        if ts is not None and (best is None or ts > best):
            best = ts
    for act in (thread.get("activities") or []):
        if not isinstance(act, dict):
            continue
        ts = _parse_ts(act.get("createdAt"))
        if ts is not None and (best is None or ts > best):
            best = ts
    return best


def classify_provider_error(text: str | None) -> str | None:
    """Provider signal from error text: exhausted/overloaded/hard/None.

    Reads structured error evidence only (session.lastError, error-tone
    activities); callers must never pass model-authored text.
    """
    if not isinstance(text, str) or not text.strip():
        return None
    low = text.strip().lower()
    if any(m in low for m in _EXHAUSTED_MARKERS):
        return "exhausted"
    if any(m in low for m in _OVERLOADED_MARKERS):
        return "overloaded"
    if any(m in low for m in _HARD_MARKERS):
        return "hard"
    return None


def evidence_dict(text: str | None) -> dict:
    """Structured evidence for a provider error message: the message, plus
    the first JSON object it embeds (OpenCode's retry status carries the
    reset as ``next`` in epoch milliseconds) under ``detail``."""
    msg = (text or "")[:2000]
    out: dict = {"message": msg[:500]}
    start = msg.find("{")
    while start != -1:
        try:
            obj, _end = json.JSONDecoder().raw_decode(msg[start:])
        except ValueError:
            start = msg.find("{", start + 1)
            continue
        if isinstance(obj, dict):
            out["detail"] = obj
        break
    return out


def error_evidence(snapshot: dict, turn_id: str | None = None) -> str | None:
    """Explicit provider error for the active turn, or None.

    Session ``error`` status acts at once (with ``lastError`` when
    present); a ``lastError`` left from an earlier turn on a ready or
    running session does not. Error-tone activities of the active turn
    naming a provider failure count too. Assistant text never counts.
    """
    thread = snapshot_thread(snapshot)
    session = thread.get("session")
    if isinstance(session, dict) and session.get("status") == "error":
        last = session.get("lastError")
        if isinstance(last, str) and last.strip():
            return last.strip()
        return "provider turn error"
    for act in (thread.get("activities") or []):
        if not isinstance(act, dict) or act.get("tone") != "error":
            continue
        if turn_id and act.get("turnId") != turn_id:
            continue
        summary = act.get("summary")
        if isinstance(summary, str) and summary.strip() \
                and classify_provider_error(summary) is not None:
            return summary.strip()
    return None


def evaluate_turn(snapshot: dict, *, now: float,
                  last_known_activity: float | None = None,
                  silence_secs: float = T3_SILENCE_SECS) -> dict:
    """Activity-based liveness of one T3 turn.

    Returns ``state`` ``running`` (tokens streaming or a tool running),
    ``completed`` (with ``assistant_text``), ``error`` (with ``reason``),
    ``interrupted`` (server restart cut the turn), or ``stalled``
    (silence past ``silence_secs`` with no running tool, with
    ``silence`` age and ``last_part`` evidence). ``last_activity_ts``
    carries the newest observed activity for the caller's next poll.
    """
    thread = snapshot_thread(snapshot)
    latest = thread.get("latestTurn")
    latest_state = latest.get("state") if isinstance(latest, dict) else None
    active_turn = latest.get("turnId") if isinstance(latest, dict) else None
    if not isinstance(active_turn, str):
        active_turn = None

    err = error_evidence(snapshot, active_turn)
    if err is not None:
        return {"state": "error", "reason": err,
                "signal": classify_provider_error(err),
                "last_activity_ts": last_activity_ts(snapshot) or last_known_activity}

    if latest_state == "completed":
        text = latest_assistant_text(snapshot, active_turn) if active_turn \
            else ""
        return {"state": "completed",
                "assistant_text": text or latest_assistant_text(snapshot),
                "last_activity_ts": last_activity_ts(snapshot) or last_known_activity}
    if latest_state == "error":
        session = thread.get("session")
        last = session.get("lastError") if isinstance(session, dict) else None
        reason = last.strip() if isinstance(last, str) and last.strip() \
            else "provider turn error"
        return {"state": "error", "reason": reason,
                "signal": classify_provider_error(reason),
                "last_activity_ts": last_activity_ts(snapshot) or last_known_activity}
    if latest_state == "interrupted":
        return {"state": "interrupted",
                "last_activity_ts": last_activity_ts(snapshot) or last_known_activity}

    # Silence runs from the newest of: any message or activity, the turn's
    # own request/start (so a resumed thread's older history never counts
    # against a fresh turn, and a turn stuck before its first token still
    # stalls), and the caller's last known activity.
    candidates = [last_activity_ts(snapshot), last_known_activity]
    if isinstance(latest, dict):
        candidates += [_parse_ts(latest.get("requestedAt")),
                       _parse_ts(latest.get("startedAt"))]
    known = [c for c in candidates if c is not None]
    anchor = max(known) if known else None
    # A running tool keeps the turn healthy however long it takes. Tokens
    # streaming count through the message's advancing ``updatedAt``: a
    # message stuck in ``streaming`` with no new tokens goes silent like
    # any other turn.
    tools = running_tool_activities(snapshot, active_turn)
    if tools and not pending_waits(snapshot, active_turn):
        return {"state": "running", "reason": "tool running",
                "running_tools": len(tools),
                "last_activity_ts": anchor}
    if anchor is None:
        # Nothing observed yet: the turn just started, not a stall.
        return {"state": "running", "reason": "awaiting first activity",
                "last_activity_ts": None}
    silence = max(0.0, now - anchor)
    if silence >= silence_secs:
        return {"state": "stalled", "silence": silence,
                "last_part": _last_part_kind(snapshot),
                "last_activity_ts": anchor}
    return {"state": "running", "reason": "within silence window",
            "silence": silence, "last_activity_ts": anchor}


def _last_part_kind(snapshot: dict) -> str | None:
    thread = snapshot_thread(snapshot)
    messages = [m for m in (thread.get("messages") or []) if isinstance(m, dict)]
    if messages:
        return f"message:{messages[-1].get('role') or 'unknown'}"
    activities = [a for a in (thread.get("activities") or []) if isinstance(a, dict)]
    if activities:
        return f"activity:{activities[-1].get('kind') or 'unknown'}"
    return None


# ---------------------------------------------------------------------------
# Turn watching, restart, and full turns
# ---------------------------------------------------------------------------

def latest_turn_id(snapshot: dict) -> str | None:
    """The thread's latest turn id, or None."""
    latest = snapshot_thread(snapshot).get("latestTurn")
    tid = latest.get("turnId") if isinstance(latest, dict) else None
    return tid if isinstance(tid, str) and tid else None


def _awaiting_new_turn(snapshot: dict, prior_turn_id: str | None) -> bool:
    latest = snapshot_thread(snapshot).get("latestTurn")
    if not isinstance(latest, dict):
        return True
    return prior_turn_id is not None and latest_turn_id(snapshot) == prior_turn_id


def _session_error_since(snapshot: dict, since: float) -> str | None:
    """Session error recorded at or after ``since`` (epoch secs), or None."""
    session = snapshot_thread(snapshot).get("session")
    if not isinstance(session, dict) or session.get("status") != "error":
        return None
    updated = _parse_ts(session.get("updatedAt"))
    if updated is not None and updated < since:
        return None
    last = session.get("lastError")
    return last.strip() if isinstance(last, str) and last.strip() \
        else "provider turn error"


def watch_turn(client: T3Client, thread_id: str, *,
               now_fn=None, sleep_fn=None,
               poll_secs: float = T3_POLL_SECS,
               silence_secs: float = T3_SILENCE_SECS,
               timeout_secs: float | None = None,
               on_snapshot=None,
               prior_turn_id: str | None = None,
               await_new_turn: bool = False) -> dict:
    """Poll a T3 turn to its end. Returns the terminal evaluation.

    A stalled read is probed immediately with a fresh snapshot before it
    counts: a turn that produced activity between the two reads stays
    running. ``timeout_secs`` bounds only the watch (tests and operator
    tools); production watches carry no elapsed deadline (no per-turn
    deadline since #88): pass None to wait as
    long as the turn stays active. Explicit provider errors return at
    once.
    """
    now_fn = now_fn or time.time
    sleep_fn = sleep_fn or time.sleep
    started = now_fn()
    deadline = (started + timeout_secs) if timeout_secs else None
    last_activity: float | None = None
    probation: dict | None = None
    while True:
        now = now_fn()
        if deadline is not None and now >= deadline:
            return {"state": "timeout", "reason": "watch timed out",
                    "last_activity_ts": last_activity}
        try:
            snapshot = client.thread_snapshot(thread_id)
        except T3Error as e:
            return {"state": "error", "reason": f"t3 snapshot failed: {e}",
                    "signal": None, "last_activity_ts": last_activity}
        if on_snapshot is not None:
            try:
                on_snapshot(snapshot)
            except Exception:
                pass
        if await_new_turn and _awaiting_new_turn(snapshot, prior_turn_id):
            latest = snapshot_thread(snapshot).get("latestTurn")
            if isinstance(latest, dict) and latest.get("state") == "running":
                # The thread is still busy with an earlier turn (a planner
                # mid-answer): T3 queues the message behind it. Busy is not
                # silence; the silence window starts once that turn ends.
                started = now
                sleep_fn(poll_secs)
                continue
            # The message was accepted but no turn has adopted it yet; an
            # earlier turn's completed state must never answer for it. A
            # provider that refuses to start the turn (unknown instance,
            # auth) errors the session with no turn at all: that acts at
            # once. A turn that never starts goes silent like any other.
            refused = _session_error_since(snapshot, started - 2.0)
            if refused is not None:
                return {"state": "error", "reason": refused,
                        "signal": classify_provider_error(refused),
                        "last_activity_ts": last_activity}
            if now - started >= silence_secs:
                return {"state": "stalled", "silence": now - started,
                        "last_part": "turn never started", "probed": True,
                        "last_activity_ts": last_activity}
            sleep_fn(poll_secs)
            continue
        ev = evaluate_turn(snapshot, now=now,
                           last_known_activity=last_activity,
                           silence_secs=silence_secs)
        last_activity = ev.get("last_activity_ts", last_activity)
        state = ev.get("state")
        if state in ("completed", "error", "interrupted"):
            return ev
        if state == "stalled":
            if probation is None:
                # Flagged: probe immediately with a fresh read before it counts.
                probation = ev
                try:
                    snapshot = client.thread_snapshot(thread_id)
                except T3Error as e:
                    return {"state": "error",
                            "reason": f"t3 probe failed: {e}",
                            "signal": None, "last_activity_ts": last_activity}
                if on_snapshot is not None:
                    try:
                        on_snapshot(snapshot)
                    except Exception:
                        pass
                second = evaluate_turn(snapshot, now=now_fn(),
                                       last_known_activity=last_activity,
                                       silence_secs=silence_secs)
                last_activity = second.get("last_activity_ts", last_activity)
                if second.get("state") == "running":
                    probation = None
                    ev = second
                else:
                    second["probed"] = True
                    return second
            else:
                ev["probed"] = True
                return ev
        else:
            probation = None
        sleep_fn(poll_secs)


def post_restart_action(harness_name: str | None, snapshot: dict) -> dict:
    """What the router does with a T3 turn after a server restart.

    With ``continueThreadsAfterServerUpdate`` on, interrupted Claude,
    Codex and OpenCode turns continue on the same thread (re-issue the
    turn); Grok turns are re-sent by the router with the same message.
    A still-running turn is simply watched on.
    """
    ev_state = evaluate_turn(snapshot, now=time.time(),
                             silence_secs=T3_SILENCE_SECS).get("state")
    if ev_state not in ("interrupted", "error", "stalled"):
        return {"action": "continue", "reason": f"turn {ev_state} after restart"}
    if (harness_name or "") == "grok":
        return {"action": "resend",
                "reason": "grok turns are re-sent by the router after a restart"}
    return {"action": "continue",
            "reason": f"{harness_name or 'unknown'} turn continues after restart"}


def post_and_watch(client: T3Client, thread_id: str, text: str, *,
                   route: str | None = None, role: str = "implementation",
                   watch_kwargs: dict | None = None) -> dict:
    """Post one message on an existing thread and watch the turn it starts.

    Reads the latest turn id first so a completed earlier turn on the same
    thread (a resumed dispatcher, a continued worker) never answers for
    the new message.
    """
    prior = latest_turn_id(client.thread_snapshot(thread_id))
    client.post_message(thread_id, text, route=route, role=role)
    kwargs = dict(watch_kwargs or {})
    kwargs.setdefault("prior_turn_id", prior)
    kwargs.setdefault("await_new_turn", True)
    return watch_turn(client, thread_id, **kwargs)


def project_id_for_thread(client: T3Client, thread_id: str,
                          override: str | None = None) -> str:
    """T3 project hosting a thread: explicit override wins, else the snapshot."""
    if isinstance(override, str) and override.strip():
        return override.strip()
    snapshot = client.thread_snapshot(thread_id)
    project = snapshot_thread(snapshot).get("projectId")
    if isinstance(project, str) and project.strip():
        return project.strip()
    raise T3Error(f"T3 thread {thread_id} carries no project id")


def run_t3_turn(client: T3Client, *, request_id: str, kind_label: str,
                parent_thread_id: str, project_id: str, route: str,
                role: str, prompt: str, title: str,
                child_suffix: str | None = None,
                existing_thread_id: str | None = None,
                watch_kwargs: dict | None = None,
                planner_thread_id: str | None = None) -> dict:
    """Run one job turn as a T3 child thread: create, start, watch.

    ``parent_thread_id`` is the thread the child is created under (the
    planner for a dispatcher, the dispatcher for a worker);
    ``planner_thread_id`` is the planner thread the first message links,
    defaulting to the parent. ``existing_thread_id`` adopts a turn a
    previous controller already created (crash recovery) instead of
    starting a second writer. Returns the watch outcome plus
    ``thread_id`` and the child identity.
    """
    planner = validate_thread_id(planner_thread_id or parent_thread_id)
    if existing_thread_id is not None:
        thread_id = validate_thread_id(existing_thread_id)
        adopted = True
    else:
        thread_id = child_thread_id(parent_thread_id, child_suffix)
        client.create_child(thread_id, parent_thread_id, project_id,
                            title, route, role)
        client.post_message(thread_id,
                            child_first_message(request_id, kind_label,
                                                planner, route, prompt),
                            route=route, role=role, title_seed=title)
        adopted = False
    kwargs = dict(watch_kwargs or {})
    if not adopted:
        kwargs.setdefault("await_new_turn", True)
    outcome = watch_turn(client, thread_id, **kwargs)
    outcome["thread_id"] = thread_id
    outcome["adopted"] = adopted
    outcome["parent_thread_id"] = validate_thread_id(parent_thread_id)
    outcome["planner_thread_id"] = planner
    outcome["route"] = route
    return outcome
