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

from pathlib import Path as _KitPath

# Invocation kinds are labels on ledger rows; each maps to one harness.
KIND_CODEX_DISPATCH = "codex_dispatch"
KIND_CODEX_RESUME = "codex_resume"
KIND_CLAUDE_CALLBACK = "claude_callback"
KIND_OPENCODE_CONTROL = "opencode_control"
KIND_OPENCODE_SERVE = "opencode_serve"
KIND_GROK_CONTROL = "grok_control"


class StreamActivity:
    """Last-activity tracking for one turn, behind the harness seam.

    ``note(now)`` records stream activity (a new part, a part update, a
    new JSON line); ``silence(now)`` is seconds since that activity;
    ``longest`` is the longest observed silence, recorded on the
    invocation so the window is tuned on data. Times use a monotonic
    clock supplied by the caller, so deterministic tests can drive it.
    """

    def __init__(self, now: float):
        self.started = now
        self.last = now
        self.longest = 0.0
        self.events = 0
        self.last_part_type = None
        self.part_sig = None
        self.cli_mark = None

    def note(self, now: float) -> None:
        if now < self.last:
            return
        gap = now - self.last
        if gap > self.longest:
            self.longest = gap
        self.last = now
        self.events += 1

    def silence(self, now: float) -> float:
        return max(0.0, now - self.last)


class Harness:
    """The seam. Subclasses override what their harness reports."""

    name = "harness"
    kinds: tuple = ()
    capabilities: frozenset = frozenset()
    owned_server = False  # a server process the runner owns per turn
    headless_worker = False  # one headless CLI process per worker turn
    binary = ""
    timeouts: dict = {}
    stages: dict = {}
    session_kind = None

    # -- spawn -------------------------------------------------------
    def spawn_spec(self, inv: dict, env: dict) -> tuple[dict, str | None]:
        """(environment for the child, generated secret or None)."""
        return env, None

    def action_key(self, kind: str, cmd: list, meta: dict | None) -> str:
        """Identity of one logical side effect, stable across controllers.

        Volatile values such as a saved session learned by an earlier
        attempt are excluded so a restarted controller finds the same key.
        ``try`` numbers repeated worker turns for one dispatcher turn (seq),
        so a bounded same-route retry after a stall is a new attempt, never a
        silent reuse and never a duplicate writer. Only stalled failures take a
        new number; every other outcome reuses its original identity.
        """
        import hashlib as _hashlib
        import json as _json
        m = dict(meta or {})
        stable = {k: m.get(k) for k in ("prompt", "model", "allowance", "seq", "qid", "try")}
        blob = _json.dumps([kind, list(cmd), stable], sort_keys=True)
        return _hashlib.sha256(blob.encode("utf-8")).hexdigest()

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

    # -- stream activity (stall detection) ---------------------------
    def stall_window_secs(self, meta=None) -> float:
        """Silence window for this turn in seconds: a per-invocation
        ``stall_secs`` override wins, then the ``RUNNER_STALL_SECS``
        environment override (deterministic drills only; unset in
        production), else the policy default (whose session-database
        evidence lives beside it in the policy)."""
        try:
            override = (meta or {}).get("stall_secs")
            if override is not None and float(override) > 0:
                return float(override)
        except (TypeError, ValueError):
            pass
        try:
            import os as _os
            env_override = _os.environ.get("RUNNER_STALL_SECS")
            if env_override is not None and float(env_override) > 0:
                return float(env_override)
        except (TypeError, ValueError):
            pass
        return float(policy.STALL_SILENCE_SECS)

    def new_activity_tracker(self, now=None) -> StreamActivity:
        import time as _time
        return StreamActivity(now if now is not None else _time.monotonic())

    def note_cli_output(self, tracker: StreamActivity, stdout: str, stderr: str,
                        now: float) -> tuple[bool, dict]:
        """(changed, detail) from JSON-line stream growth. Any new output
        line counts as activity; the supervisor ends a turn whose stream
        stays silent past the window while the child lives."""
        blob = (stdout or "") + "\n" + (stderr or "")
        lines = blob.count("\n")
        mark = (lines, len(blob))
        prev = tracker.cli_mark
        tracker.cli_mark = mark
        if prev is None:
            changed = lines > 1 or len(blob) > 1
        else:
            changed = mark != prev
        if changed:
            tracker.note(now)
        return changed, {"lines": lines, "bytes": len(blob)}

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


def _kit_meta(inv) -> dict:
    try:
        meta = json.loads((inv or {}).get("meta_json") or "{}")
    except ValueError:
        return {}
    return meta if isinstance(meta, dict) else {}


def _kit_state_dir(inv):
    try:
        p = _KitPath(str((inv or {}).get("stdout_path") or ""))
        if p.parent.name == "outputs":
            return p.parent.parent
    except Exception:
        pass
    return None


def _kit_name_for_inv(inv, default: str | None = None) -> str | None:
    meta = _kit_meta(inv)
    route = meta.get("route")
    if isinstance(route, str) and route in policy.ROUTES:
        try:
            return policy.kit_name_for_route(route)
        except ValueError:
            pass
    stage = meta.get("stage")
    if stage == "dispatch":
        return "dispatcher"
    if stage in ("implementation", "correction", "recovery"):
        try:
            return policy.kit_name_for_route(route) if isinstance(route, str) and route in policy.ROUTES else default
        except ValueError:
            return default
    return default


class CodexCLI(Harness):
    name = "codex"
    kinds = (KIND_CODEX_DISPATCH, KIND_CODEX_RESUME)
    capabilities = frozenset({"read_only", "session_resume", "structured_output"})
    binary = adapters.CODEX_BIN
    timeouts = {KIND_CODEX_DISPATCH: 1800, KIND_CODEX_RESUME: 1800}
    stages = {KIND_CODEX_DISPATCH: "dispatch", KIND_CODEX_RESUME: "dispatch"}
    session_kind = "codex_task_id"

    def spawn_spec(self, inv, env):
        """Codex sessions run on the route's kit via an isolated CODEX_HOME.

        The kit directory is generated from policy (see ``runner/kits.py``);
        nothing is inherited from the user's Codex configuration.
        """
        from . import kits as _kits

        env = dict(env)
        kit_name = _kit_name_for_inv(inv, default="dispatcher")
        state_dir = _kit_state_dir(inv)
        if kit_name is not None and state_dir is not None:
            request_id = str((inv or {}).get("request_id") or "job")
            invocation_id = str((inv or {}).get("invocation_id") or "inv")
            kit_dir = _kits.kit_dir_for(str(state_dir), request_id, invocation_id, kit_name)
            _kits.materialize_codex_kit(kit_name, kit_dir)
            env.update(_kits.codex_kit_env(kit_dir))
        return env, None

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

    def spawn_spec(self, inv, env):
        """The planner keeps the user's own session: never isolate it.

        ``claude_callback`` resumes the saved planner session with the
        user's ``CLAUDE_CONFIG_DIR`` untouched. Kit equivalents for other
        Claude sessions (if any) travel in an isolated ``CLAUDE_CONFIG_DIR``
        built by ``runner/kits.py``; use ``kits.materialize_claude_kit`` and
        ``kits.claude_kit_env`` for those manual runs.
        """
        return dict(env), None

    @staticmethod
    def kit_spec_for_manual_run(kit_name: str, dest) -> dict:
        """Kit equivalent for a manual Claude session (not the planner)."""
        from . import kits as _kits

        kit_dir = _kits.materialize_claude_kit(kit_name, dest)
        return _kits.claude_kit_env(kit_dir)

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
        """Owned server runs on the route's kit, never ``--pure``.

        The configuration directory is generated from policy
        (``runner/kits.py``): ``OPENCODE_CONFIG_DIR``, ``XDG_CONFIG_HOME``
        (shadow), and ``OPENCODE_CONFIG`` point at it, so the kit's
        plugins, skills, and MCP subset are allowed and nothing is
        inherited from the user's configuration.
        """
        from . import kits as _kits

        password = adapters.generate_control_password()
        env = dict(env)
        env["OPENCODE_SERVER_PASSWORD"] = password
        kit_name = _kit_name_for_inv(inv, default="worker")
        state_dir = _kit_state_dir(inv)
        if kit_name is not None and state_dir is not None:
            meta = _kit_meta(inv)
            route = meta.get("route")
            route_or_none = route if isinstance(route, str) and route in policy.ROUTES else None
            request_id = str((inv or {}).get("request_id") or "job")
            invocation_id = str((inv or {}).get("invocation_id") or "inv")
            kit_dir = _kits.kit_dir_for(str(state_dir), request_id, invocation_id, kit_name)
            _kits.materialize_opencode_kit(kit_name, kit_dir, route=route_or_none)
            env.update(_kits.opencode_kit_env(kit_dir))
        return env, password

    @staticmethod
    def grok_kit_spec_for_manual_run(kit_name: str, dest) -> dict:
        """Kit equivalent for a Grok Build session (``GROK_HOME``)."""
        from . import kits as _kits

        kit_dir = _kits.materialize_grok_kit(kit_name, dest)
        return _kits.grok_kit_env(kit_dir)

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
    def _tool_running(part: dict) -> bool:
        """True when a tool part reports a running process. The part counts
        as activity while its process is alive; the per-turn timeout stays
        the outer budget for a tool that never finishes."""
        for key in ("state", "status"):
            val = part.get(key)
            if isinstance(val, str) and val.lower() == "running":
                return True
            if isinstance(val, dict):
                nested = val.get("status") or val.get("state")
                if isinstance(nested, str) and nested.lower() == "running":
                    return True
        return False

    def part_activity(self, messages, baseline_ids) -> tuple[list, str | None, bool]:
        """(signature, last_part_type, running_tool) for assistant messages
        after the baseline. Every part creation or update (text, reasoning,
        or tool) changes the signature; a tool part in running state also
        reports liveness. Text changes count as activity through the part
        signature, but text is never classified: only the signal tables
        decide capacity signals."""
        sig: list = []
        last_type: str | None = None
        running = False
        base = baseline_ids or set()
        for m in messages or []:
            info = m.get("info") if isinstance(m, dict) else None
            if not isinstance(info, dict) or info.get("id") in base \
                    or info.get("role") != "assistant":
                continue
            for p in m.get("parts") or []:
                if not isinstance(p, dict):
                    continue
                ptype = p.get("type") if isinstance(p.get("type"), str) else None
                last_type = ptype or last_type
                try:
                    sig.append((info.get("id"), ptype,
                                json.dumps(p, sort_keys=True, default=str)))
                except (TypeError, ValueError):
                    sig.append((info.get("id"), ptype, str(ptype)))
                if ptype == "tool" and self._tool_running(p):
                    running = True
        return sig, last_type, running

    def note_part_activity(self, tracker: StreamActivity, messages, baseline_ids,
                           now: float) -> tuple[bool, dict]:
        """(changed, detail) from message-part polling at a short interval.
        A changed part signature, or a tool part in running state, counts as
        activity and refreshes the last-activity timestamp."""
        sig, last_type, running = self.part_activity(messages, baseline_ids)
        prev = tracker.part_sig
        tracker.part_sig = sig
        if last_type is not None:
            tracker.last_part_type = last_type
        if running:
            changed = True
        elif prev is None:
            changed = bool(sig)
        else:
            changed = sig != prev
        if changed:
            tracker.note(now)
        return changed, {"last_part_type": tracker.last_part_type,
                         "parts": len(sig), "running_tool": running}

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


class GrokBuildCLI(Harness):
    """Native Grok Build CLI on the xAI subscription (``grok -p`` headless).

    One process per worker turn: headless single prompt in the workspace
    with the route's model and effort as JSON, resumed by saved session id.
    No owned server, so the generic supervisor path (spawn, wait, kill on
    timeout) applies; there is no session to abort and confirm idle, the
    process exit is the turn boundary. ``grok usage`` style counters are
    not read: usage is recorded only where the turn's own JSON reports it.
    """

    name = "grok"
    kinds = (KIND_GROK_CONTROL,)
    capabilities = frozenset({"workspace_write", "session_resume", "structured_output"})
    owned_server = False
    headless_worker = True
    binary = adapters.GROK_BIN
    timeouts = {KIND_GROK_CONTROL: 1800}
    stages = {KIND_GROK_CONTROL: "implementation"}
    session_kind = "grok_session_id"

    def spawn_spec(self, inv, env):
        """Grok sessions run on the route's kit via an isolated GROK_HOME.

        The kit directory is generated from policy (see ``runner/kits.py``);
        nothing is inherited from the user's Grok configuration.
        """
        from . import kits as _kits

        env = dict(env)
        kit_name = _kit_name_for_inv(inv, default="worker")
        state_dir = _kit_state_dir(inv)
        if kit_name is not None and state_dir is not None:
            request_id = str((inv or {}).get("request_id") or "job")
            invocation_id = str((inv or {}).get("invocation_id") or "inv")
            kit_dir = _kits.kit_dir_for(str(state_dir), request_id, invocation_id, kit_name)
            _kits.materialize_grok_kit(kit_name, kit_dir)
            env.update(_kits.grok_kit_env(kit_dir))
        return env, None

    def parse_session(self, kind, stdout, stderr, job):
        # The headless JSON result carries the session; anything else
        # (empty output, plain text, an error object) means no session,
        # never a match.
        parsed = adapters.parse_grok_result(stdout)
        sid = parsed.get("session_id")
        return (sid, "grok_session_id") if sid else (None, None)

    def parse_report(self, kind, stdout, cmd):
        """Normalized turn summary for the controller's report path.

        Success carries the worker text, the stop reason, and any
        reported counters; failure carries the provider error object as
        signal evidence with its signal class. Model-authored text is
        never signal evidence: only the JSON error object counts.
        """
        parsed = adapters.parse_grok_result(stdout)
        sid = parsed.get("session_id")
        usage, observed, _variant, ids = self.measure(kind, stdout, stderr="", meta=None)
        if parsed.get("ok"):
            actual = None
            if observed and "/" in observed:
                provider, _, model_id = observed.partition("/")
                actual = {"providerID": provider, "modelID": model_id, "variant": None}
            return {"grok_session_id": sid, "ok": True,
                    "assistant_text": parsed.get("text") or "",
                    "finish": parsed.get("stop_reason") or "end_turn",
                    "actual_model": actual, "usage": usage, "native_ids": ids,
                    "blockers": [], "error": None,
                    "signal": None, "signal_evidence": None}
        evidence: dict = {"source": "grok",
                          "message": str(parsed.get("error") or "")[:1000]}
        detail = parsed.get("detail")
        if isinstance(detail, dict):
            for key in ("status", "statusCode", "code"):
                if detail.get(key) is not None:
                    evidence[key] = detail[key]
            nested = detail.get("message")
            if isinstance(nested, str) and nested != evidence["message"]:
                evidence["detail_message"] = nested[:1000]
        return {"grok_session_id": sid, "ok": False,
                "assistant_text": "", "finish": parsed.get("stop_reason"),
                "actual_model": None, "usage": usage, "native_ids": ids,
                "blockers": [], "error": evidence["message"] or "grok turn failed",
                "signal": self.classify_signal(evidence),
                "signal_evidence": evidence}

    def measure(self, kind, stdout, stderr, meta):
        """(usage, observed_model, observed_variant, native_ids).

        Counters verbatim under the ``grok`` source label, only where the
        turn's own JSON reports them; unreported usage stays None. The
        native CLI serves the xAI subscription, so a reported model id is
        recorded under provider ``xai``. Elapsed time comes from the
        invocation row itself.
        """
        parsed = adapters.parse_grok_result(stdout)
        counters: dict = {}
        if isinstance(parsed.get("usage"), dict):
            counters.update(parsed["usage"])
        if isinstance(parsed.get("num_turns"), int):
            counters["num_turns"] = parsed["num_turns"]
        usage = {"source": "grok", **counters} if counters else None
        observed = None
        if isinstance(parsed.get("model"), str) and parsed["model"]:
            observed = f"xai/{parsed['model']}"
        ids: dict = {}
        if parsed.get("session_id"):
            ids["session_id"] = parsed["session_id"]
        return usage, observed, None, ids

    def classify_signal(self, evidence):
        """xAI provider errors into the policy's signal classes.

        Standard policy shapes (retry reasons, APIError bodies, status
        codes) classify first; then the Grok JSON error object's message
        and status: quota and credit exhaustion, rate-limit and overload,
        and hard errors (context, auth, region, consent). Only the error
        object counts, never worker text.
        """
        cls = policy.classify_signal(evidence)
        if cls is not None:
            return cls
        if not isinstance(evidence, dict):
            return None
        blob = ""
        for key in ("message", "error", "detail_message"):
            val = evidence.get(key)
            if isinstance(val, str):
                blob += " " + val.lower()
        detail = evidence.get("detail")
        if isinstance(detail, dict):
            for key in ("message", "type", "code"):
                val = detail.get(key)
                if isinstance(val, str):
                    blob += " " + val.lower()
        code = evidence.get("status", evidence.get("statusCode", evidence.get("code")))
        try:
            code = int(code) if code is not None and str(code).strip() else None
        except (TypeError, ValueError):
            code = None
        if code in policy.SIGNAL_CLASSES["overloaded"].get("status_codes", ()):
            return "overloaded"
        hard_markers = ("context_length_exceeded", "context length exceeded",
                        "maximum context", "authentication", "unauthorized",
                        "invalid api key", "invalid_api_key", "forbidden",
                        "region not supported", "region_not_supported", "consent",
                        "datapolicy", "data policy")
        if any(m in blob for m in hard_markers):
            return "hard"
        exhausted_markers = ("insufficient_quota", "insufficient quota",
                             "quota exceeded", "quota_exceeded", "quota exhausted",
                             "out of credit", "out of credits", "no credits",
                             "credit exhausted", "credits exhausted",
                             "billing", "subscription expired", "plan quota",
                             "usage limit exceeded")
        if any(m in blob for m in exhausted_markers):
            return "exhausted"
        overloaded_markers = ("rate_limit", "rate limit", "ratelimit",
                              "rate limited", "ratelimiterror", "overloaded",
                              "overload", "too many requests", "too_many_requests",
                              "server busy", "temporarily unavailable", "try again later",
                              "429", "503", "529")
        if code == 429 or any(m in blob for m in overloaded_markers):
            return "overloaded"
        if code in (401, 403):
            return "hard"
        return None

    def infer_rc(self, kind, stdout, cmd=None):
        return 0 if adapters.parse_grok_result(stdout).get("ok") else 1

    def turn_ok(self, kind, rc, stdout, cmd=None):
        return rc == 0 and bool(adapters.parse_grok_result(stdout).get("ok"))

    def identity_ok(self, kind, sid, saved) -> bool:
        # A worker turn counts only from the saved Grok session: a missing
        # session or a forked session never applies, on the live path and
        # in recovery alike.
        return bool(sid) and (saved is None or sid == saved)

    def record_session(self, con, request_id, sid, skind, kind, job, meta, now):
        if skind != "grok_session_id":
            return
        try:
            model, effort = policy.grok_route_params((job or {}).get("route"))
        except ValueError:
            model, effort = adapters.GROK_MODEL, adapters.GROK_EFFORT
        con.execute(
            "UPDATE jobs SET grok_session_id=COALESCE(grok_session_id, ?), adapter='grok',"
            " model=?, effort=?, updated_at=? WHERE request_id=?",
            (sid, model, effort or "default", now, request_id))

    def default_route(self, kind, job):
        return (job or {}).get("route")

    def action_key(self, kind, cmd, meta):
        """A resumed turn is the same logical turn: the ``--resume``
        session learned after the first attempt started is volatile, so a
        restarted controller reuses the finished attempt instead of
        starting a second writer."""
        stable_cmd = list(cmd or [])
        if "--resume" in stable_cmd:
            i = stable_cmd.index("--resume")
            del stable_cmd[i:i + 2]
        return super().action_key(kind, stable_cmd, meta)


HARNESSES = {h.name: h for h in (CodexCLI(), ClaudeCLI(), OpenCodeServer(), GrokBuildCLI())}
KIND_TO_HARNESS = {kind: h for h in HARNESSES.values() for kind in h.kinds}
INVOCATION_KINDS = tuple(KIND_TO_HARNESS)


def kinds_for_stage(stage: str | None) -> list[str]:
    """Invocation kinds whose harness serves ``stage`` (worker turns share
    the implementation stage across harnesses)."""
    if not stage:
        return []
    return [kind for kind, h in KIND_TO_HARNESS.items() if h.stage_for(kind) == stage]


def worker_control_kinds() -> list[str]:
    """Kinds that run implementation worker turns, on any harness."""
    return kinds_for_stage("implementation")


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
    if name == "grok":
        return KIND_GROK_CONTROL
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
