"""Harness adapters behind one interface. Stdlib only.

One class per harness. The supervisor and the core never branch on an
invocation kind: they ask the harness registered for that kind. A stage
declares the capabilities it needs and the router refuses a route whose
harness lacks one.
"""
from __future__ import annotations

import json
import os
import subprocess

from . import adapters, policy

# Invocation kinds are labels on ledger rows; each maps to one harness.
KIND_CODEX_DISPATCH = "codex_dispatch"
KIND_CODEX_RESUME = "codex_resume"
KIND_CLAUDE_CALLBACK = "claude_callback"
KIND_OPENCODE_CONTROL = "opencode_control"
KIND_OPENCODE_SERVE = "opencode_serve"


class Harness:
    """The seam. Subclasses override what their harness reports."""

    name = "harness"
    kinds: tuple = ()
    capabilities: frozenset = frozenset()
    owned_server = False  # a server process the runner owns per turn
    binary = ""
    timeouts: dict = {}
    stages: dict = {}
    session_kind = None

    # -- spawn -------------------------------------------------------
    def spawn_spec(self, inv: dict, env: dict) -> tuple[dict, str | None]:
        """(environment for the child, generated secret or None)."""
        return env, None

    def drives(self, kind: str) -> bool:
        return False

    def drive(self, *args, **kwargs):
        raise NotImplementedError

    # -- reading the harness's own records ----------------------------
    def parse_session(self, kind: str, stdout: str, stderr: str, job: dict | None) -> tuple:
        return None, None

    def parse_report(self, kind: str, stdout: str, cmd) -> dict | None:
        return None

    def classify_signal(self, evidence):
        return policy.classify_signal(evidence)

    def measure(self, kind: str, stdout: str, stderr: str, meta: dict | None) -> tuple:
        # (usage, observed_model, observed_variant, native_ids). Usage is
        # verbatim under a source label; model and variant stay separate so
        # the ledger never folds the variant into the model name.
        return None, None, None, {}

    def infer_rc(self, kind: str, stdout: str, cmd=None) -> int:
        """Exit code to assume when the child and its supervisor died."""
        return 1

    def turn_ok(self, kind: str, rc, stdout: str, cmd=None) -> bool:
        return rc == 0

    def luna_action(self, kind: str, stdout: str, cmd=None) -> dict | None:
        """Luna dispatcher envelope from this harness's own records.

        Codex returns its agent envelope (stdout or the last-message file);
        OpenCode extracts it from the owned-server summary's assistant text.
        The controller and recovery call this instead of parsing adapters
        directly, so harness specifics stay in this module."""
        return None

    def identity_ok(self, kind: str, sid, saved) -> bool:
        return True

    def planner_answer(self, kind: str, stdout: str, meta: dict, job: dict) -> tuple | None:
        return None

    def callback_failure_reason(self, kind: str, rc, stdout: str, job: dict) -> str | None:
        return None

    def record_session(self, con, request_id: str, sid: str, skind: str, kind: str,
                       job: dict | None, meta: dict | None, now: str) -> None:
        return None

    def is_dispatch(self, kind: str) -> bool:
        return self.stages.get(kind) == "dispatch"

    def stage_for(self, kind: str) -> str | None:
        return self.stages.get(kind)

    def timeout_for(self, kind: str) -> int:
        return int(self.timeouts.get(kind, 1800))

    def default_route(self, kind: str, job: dict | None) -> str | None:
        return None

    _version_cache: dict = {}

    def version(self) -> str | None:
        if self.binary not in Harness._version_cache:
            try:
                out = subprocess.run([self.binary, "--version"], capture_output=True, text=True,
                                     timeout=5, stdin=subprocess.DEVNULL).stdout
                first = (out or "").strip().splitlines()
                Harness._version_cache[self.binary] = first[0][:80] if first else None
            except Exception:
                Harness._version_cache[self.binary] = None
        return Harness._version_cache[self.binary]


def _jsonl(stdout: str):
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict):
            yield obj


class CodexCLI(Harness):
    name = "codex"
    kinds = (KIND_CODEX_DISPATCH, KIND_CODEX_RESUME)
    capabilities = frozenset({"read_only", "session_resume", "structured_output"})
    binary = adapters.CODEX_BIN
    timeouts = {KIND_CODEX_DISPATCH: 1800, KIND_CODEX_RESUME: 1800}
    stages = {KIND_CODEX_DISPATCH: "dispatch", KIND_CODEX_RESUME: "dispatch"}
    session_kind = "codex_task_id"

    def parse_session(self, kind, stdout, stderr, job):
        sid = adapters.parse_codex_task_id(stdout, stderr)
        return (sid, "codex_task_id") if sid else (None, None)

    def parse_report(self, kind, stdout, cmd):
        return adapters.parse_codex_agent_envelope(
            stdout, adapters.read_last_message_file(adapters.last_message_path_from_cmd(cmd)))

    def measure(self, kind, stdout, stderr, meta):
        usage = None
        ids = {"thread_id": None, "turn_ids": [], "agent_message_ids": []}
        for obj in _jsonl(stdout):
            typ = obj.get("type")
            if typ == "thread.started":
                ids["thread_id"] = obj.get("thread_id") or obj.get("id")
            elif typ == "turn.completed":
                usage = {"source": "codex", **(obj.get("usage") or {})}
                if obj.get("turn_id"):
                    ids["turn_ids"].append(obj["turn_id"])
            elif typ == "item.completed":
                item = obj.get("item") or {}
                if item.get("type") == "agent_message" and item.get("id"):
                    ids["agent_message_ids"].append(item["id"])
        return usage, None, None, ids

    def infer_rc(self, kind, stdout, cmd=None):
        last_text = None
        if cmd is not None:
            try:
                last_text = adapters.read_last_message_file(
                    adapters.last_message_path_from_cmd(cmd))
            except Exception:
                last_text = None
        envelope = adapters.parse_codex_agent_envelope(stdout, last_text)
        if isinstance(envelope, dict) and "turn.completed" in (stdout or ""):
            return 0
        # Recovery fallback: the turn's action exists only in the
        # output-last-message file (stdout was lost with the supervisor).
        if last_text:
            try:
                file_only = adapters.parse_codex_agent_envelope("", last_text)
            except Exception:
                file_only = None
            if isinstance(file_only, dict):
                return 0
        return 1

    def turn_ok(self, kind, rc, stdout, cmd=None):
        if rc != 0:
            return False
        if "turn.completed" in (stdout or ""):
            return True
        # Same file fallback as infer_rc: a completed turn whose stdout was
        # lost still counts when its last-message file holds the envelope.
        if cmd is not None:
            try:
                last_text = adapters.read_last_message_file(
                    adapters.last_message_path_from_cmd(cmd))
            except Exception:
                last_text = None
            if last_text:
                try:
                    return isinstance(
                        adapters.parse_codex_agent_envelope("", last_text), dict)
                except Exception:
                    return False
        return False

    def luna_action(self, kind, stdout, cmd=None):
        return self.parse_report(kind, stdout, cmd)

    def identity_ok(self, kind, sid, saved):
        return bool(sid) and (saved is None or sid == saved)

    def record_session(self, con, request_id, sid, skind, kind, job, meta, now):
        if skind == "codex_task_id" and kind == KIND_CODEX_DISPATCH:
            con.execute(
                "UPDATE jobs SET codex_task_id=COALESCE(codex_task_id, ?), adapter='codex',"
                " model=?, effort=?, updated_at=? WHERE request_id=?",
                (sid, adapters.CODEX_MODEL, adapters.CODEX_EFFORT, now, request_id))

    def default_route(self, kind, job):
        return policy.stage_routes("dispatch")[0]


class ClaudeCLI(Harness):
    name = "claude"
    kinds = (KIND_CLAUDE_CALLBACK,)
    # The callback runs with no tools, so it cannot write: read-only holds.
    capabilities = frozenset({"read_only", "session_resume", "structured_output"})
    binary = adapters.CLAUDE_BIN
    timeouts = {KIND_CLAUDE_CALLBACK: 900}
    stages = {KIND_CLAUDE_CALLBACK: "planning"}
    session_kind = "planner_session_id"

    def parse_session(self, kind, stdout, stderr, job):
        sid = (job or {}).get("planner_session_id")
        return (sid, "planner_session_id") if sid else (None, None)

    @staticmethod
    def _result(stdout):
        text = (stdout or "").strip()
        try:
            obj = json.loads(text) if text else None
        except ValueError:
            obj = None
            for line in reversed(text.splitlines()):
                try:
                    cand = json.loads(line.strip())
                except ValueError:
                    continue
                if isinstance(cand, dict) and cand.get("type") == "result":
                    obj = cand
                    break
        return obj if isinstance(obj, dict) else None

    def measure(self, kind, stdout, stderr, meta):
        meta = meta or {}
        usage = None
        ids = {}
        obj = self._result(stdout)
        if obj is not None:
            usage = {"source": "claude", **(obj.get("usage") or {})}
            for key in ("total_cost_usd", "duration_ms", "num_turns"):
                if obj.get(key) is not None:
                    usage[key] = obj.get(key)
            ids = {"session_id": obj.get("session_id"), "uuid": obj.get("uuid")}
        if meta.get("prompt_sha256"):
            ids["prompt_sha256"] = meta["prompt_sha256"]
        observed = obj.get("model") if obj and isinstance(obj.get("model"), str) else None
        return usage, observed, None, ids

    def infer_rc(self, kind, stdout, cmd=None):
        return 0 if adapters.parse_claude_result(stdout).get("ok") else 1

    def parsed_result(self, kind, stdout):
        """Structured Claude result via the seam, so callers never parse
        the harness output directly."""
        return adapters.parse_claude_result(stdout)

    def planner_answer(self, kind, stdout, meta, job):
        parsed = adapters.parse_claude_result(stdout)
        qid = (meta or {}).get("qid")
        if parsed.get("ok") and qid and parsed.get("session_id") == (job or {}).get("planner_session_id"):
            return qid, parsed["answer"]
        return None

    def callback_failure_reason(self, kind, rc, stdout, job):
        parsed = adapters.parse_claude_result(stdout)
        if rc != 0 or not parsed.get("ok"):
            return f"planner_callback_failed rc={rc} (consumed by recovery)"
        if parsed.get("session_id") != (job or {}).get("planner_session_id"):
            return "planner_session_mismatch (consumed by recovery)"
        return None

    def default_route(self, kind, job):
        model = (job or {}).get("planner_model")
        return "sonnet/medium" if model == adapters.CLAUDE_LIVE_MODEL else policy.stage_routes("planning")[0]


class OpenCodeServer(Harness):
    name = "opencode"
    kinds = (KIND_OPENCODE_CONTROL, KIND_OPENCODE_SERVE)
    capabilities = frozenset({"workspace_write", "session_resume", "structured_output", "read_only"})
    owned_server = True
    binary = adapters.OPENCODE_BIN
    timeouts = {KIND_OPENCODE_CONTROL: 1800, KIND_OPENCODE_SERVE: 1800}
    stages = {KIND_OPENCODE_CONTROL: "implementation", KIND_OPENCODE_SERVE: "implementation"}
    session_kind = "opencode_session_id"

    def spawn_spec(self, inv, env):
        password = adapters.generate_control_password()
        env = dict(env)
        env["OPENCODE_SERVER_PASSWORD"] = password
        return env, password

    def drives(self, kind):
        return kind == KIND_OPENCODE_CONTROL

    def drive(self, *args, **kwargs):
        from .supervisor import _drive_opencode_control
        return _drive_opencode_control(*args, **kwargs)

    def parse_session(self, kind, stdout, stderr, job):
        # The owned server reports its session in this invocation's own
        # RUNNER_RESULT line; stderr carries no session. A missing or
        # unparsable line means no session, never a match.
        for line in (stdout or "").splitlines():
            if not line.startswith("RUNNER_RESULT "):
                continue
            try:
                summary = json.loads(line[len("RUNNER_RESULT "):])
            except ValueError:
                continue
            if isinstance(summary, dict):
                sid = summary.get("opencode_session_id")
                if isinstance(sid, str) and sid:
                    return sid, "opencode_session_id"
        return None, None

    def identity_ok(self, kind, sid, saved) -> bool:
        # A dispatch turn counts only from the saved dispatcher task: a
        # missing session or a forked session never applies, on the live
        # path and in recovery alike.
        return bool(sid) and (saved is None or sid == saved)

    def luna_action(self, kind, stdout, cmd=None):
        summary = self.parse_report(kind, stdout, cmd) or {}
        text = summary.get("assistant_text") if isinstance(summary, dict) else None
        if not isinstance(text, str) or not text.strip():
            return None
        return adapters.parse_luna_envelope_from_texts(text)

    def parse_report(self, kind, stdout, cmd):
        summary = {}
        for line in (stdout or "").splitlines():
            if line.startswith("RUNNER_RESULT "):
                try:
                    summary = json.loads(line[len("RUNNER_RESULT "):])
                except ValueError:
                    continue
        return summary if isinstance(summary, dict) and summary else None

    def measure(self, kind, stdout, stderr, meta):
        summary = self.parse_report(kind, stdout, None) or {}
        usage = summary.get("usage") if isinstance(summary.get("usage"), dict) else None
        ids = summary.get("native_ids") if isinstance(summary.get("native_ids"), dict) else {}
        am = summary.get("actual_model") if isinstance(summary.get("actual_model"), dict) else None
        observed = None
        variant = None
        if am and am.get("providerID") and am.get("modelID"):
            observed = f"{am['providerID']}/{am['modelID']}"
            variant = am.get("variant") if isinstance(am.get("variant"), str) else None
        return usage, observed, variant, ids

    def classify_signal(self, evidence):
        """Harness-aware classification. Transport HTTP 503/529 from the
        owned server counts as overload even when no retry dictionary
        arrived; everything else follows the policy."""
        try:
            if isinstance(evidence, adapters.OpenCodeHTTPError):
                if evidence.status in policy.SIGNAL_CLASSES["overloaded"].get("status_codes", ()):
                    return "overloaded"
                return policy.classify_signal(
                    {"name": "APIError",
                     "data": {"statusCode": evidence.status,
                              "responseBody": evidence.body or ""}})
        except Exception:
            pass
        return policy.classify_signal(evidence)

    @staticmethod
    def capped_retry_next(status) -> tuple[float, float]:
        """(raw next, capped next) from a session retry status. The cap is
        the policy's ``next_cap_secs``; an unreadable ``next`` is 0."""
        cap = float(policy.SIGNAL_CLASSES["overloaded"].get("next_cap_secs", 20))
        raw = status.get("next") if isinstance(status, dict) else None
        try:
            val = float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            val = 0.0
        if val <= 0:
            return 0.0, 0.0
        return val, min(val, cap)

    def record_session(self, con, request_id, sid, skind, kind, job, meta, now):
        if skind != "opencode_session_id":
            return
        meta = meta or {}
        if meta.get("stage") == "dispatch":
            # An OpenCode-hosted dispatcher: its session is the dispatcher task.
            con.execute(
                "UPDATE jobs SET codex_task_id=COALESCE(codex_task_id, ?), adapter='opencode',"
                " updated_at=? WHERE request_id=?", (sid, now, request_id))
            return
        route = (job or {}).get("route") or "muse-spark-xhigh-free"
        model, variant, _agent = policy.opencode_route_params(route)
        con.execute(
            "UPDATE jobs SET opencode_session_id=COALESCE(opencode_session_id, ?), adapter='opencode',"
            " model=?, effort=?, updated_at=? WHERE request_id=?",
            (sid, model, variant or "default", now, request_id))

    def default_route(self, kind, job):
        return (job or {}).get("route")


HARNESSES = {h.name: h for h in (CodexCLI(), ClaudeCLI(), OpenCodeServer())}
KIND_TO_HARNESS = {kind: h for h in HARNESSES.values() for kind in h.kinds}
INVOCATION_KINDS = tuple(KIND_TO_HARNESS)


def harness_for(kind: str | None) -> Harness:
    h = KIND_TO_HARNESS.get(kind or "")
    return h if h is not None else Harness()


def harness_named(name: str) -> Harness:
    if name not in HARNESSES:
        raise ValueError(f"unknown harness: {name!r}")
    return HARNESSES[name]


def kind_for_cmd(cmd: list) -> str:
    """Infer the invocation kind from a built-in adapter command."""
    if not cmd:
        return "unknown"
    name = os.path.basename(str(cmd[0])).lower() if cmd[0] else ""
    args = [str(a) for a in cmd[1:]]
    if name == "codex":
        return KIND_CODEX_RESUME if "resume" in args else KIND_CODEX_DISPATCH
    if name == "claude":
        return KIND_CLAUDE_CALLBACK
    if name == "opencode":
        return KIND_OPENCODE_SERVE if "serve" in args else KIND_OPENCODE_CONTROL
    return "unknown"


def route_uses_owned_server(route: str) -> bool:
    """True when the route's harness runs on an owned server process.

    Dispatch and resume ask the registry, never the policy's harness
    string, so a stage swaps harness by policy row.
    """
    spec = policy.ROUTES.get(route or "")
    if not isinstance(spec, dict):
        return False
    try:
        return bool(harness_named(spec.get("harness") or "").owned_server)
    except ValueError:
        return False


def route_capability_blocker(route: str, stage: str | None) -> str | None:
    """Precise reason when a stage needs a capability the route's harness lacks."""
    if not stage or stage not in policy.STAGES:
        return None
    spec = policy.ROUTES.get(route)
    if spec is None:
        return f"unsupported route: {route!r}"
    harness = harness_named(spec["harness"])
    missing = [c for c in policy.STAGES[stage].get("capabilities", []) if c not in harness.capabilities]
    if missing:
        return f"route {route} on {harness.name} lacks {', '.join(missing)} required by stage {stage}"
    return None
