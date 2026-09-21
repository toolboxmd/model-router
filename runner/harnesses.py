"""Harness adapters behind one interface. Stdlib only.

One class per harness. The supervisor and the core never branch on an
invocation kind: they ask the harness registered for that kind. A stage
declares the capabilities it needs and the router refuses a route whose
harness lacks one.
"""
from __future__ import annotations

import datetime
import json
import os
import re
import subprocess
import time

from . import adapters, policy

from pathlib import Path as _KitPath

# Invocation kinds are labels on ledger rows; each maps to one harness.
KIND_CODEX_DISPATCH = "codex_dispatch"
KIND_CODEX_RESUME = "codex_resume"
KIND_CLAUDE_CALLBACK = "claude_callback"
KIND_CLAUDE_COMPACT = "claude_compact"
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

    def probe_interval_secs(self) -> float:
        """Minimum seconds between proactive usage probes on this harness
        (Codex 300, Claude 180; the policy owns the values)."""
        return float(policy.PROBE_INTERVAL_SECS.get(self.name, 300))

    def probe_due(self, observed_at: str | None, now_ts: float | None = None) -> bool:
        """True when a usage probe is due: never probed or older than the
        harness interval. Probes never block routing: a due probe runs
        best-effort and a failure records unknown."""
        now = now_ts if now_ts is not None else time.time()
        if not observed_at:
            return True
        try:
            obs = datetime.datetime.fromisoformat(
                str(observed_at).replace("Z", "+00:00"))
            if obs.tzinfo is None:
                obs = obs.replace(tzinfo=datetime.timezone.utc)
            obs_ts = obs.timestamp()
        except (ValueError, TypeError, OverflowError):
            return True
        return (now - obs_ts) >= self.probe_interval_secs()

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

    # A missing or rejected Codex login surfaces as transport text, not as
    # structured provider evidence: live 2026-09-21 showed stderr
    # "failed to connect to websocket: HTTP error: 401 Unauthorized" with
    # stdout "Missing bearer or basic authentication in header". Both
    # classify hard with reason auth (policy hard covers 401/403 and auth
    # names for structured shapes; this covers the CLI text shapes).
    CODEX_AUTH_TEXT_MARKERS = (
        "missing bearer",
        "missing authentication",
        "bearer or basic authentication",
        "authentication in header",
        "auth failed",
        "invalid api key",
        "unauthorized",
    )
    # Bare "401" never matches by substring: counts, ports, and ids like
    # "4010" or "14012" are not auth failures. Only a standalone 401 token
    # (e.g. "HTTP error: 401 Unauthorized") counts, via word boundaries.
    CODEX_AUTH_CODE_RE = re.compile(r"\b401\b")

    # A usage-limit refusal arrives as the turn's terminal provider message
    # (live 2026-09-21: an agent_message "You've hit your usage limit. ...
    # try again at Sep 22nd, 2026 9:51 AM." with rc=1 and no
    # turn.completed), or as an error item naming UsageLimitExceeded /
    # RateLimitExceeded with resets_at. Both classify exhausted with the
    # carried reset time. The phrasing rule needs "usage limit" plus a
    # provider corroborator (retry wording, reset wording, the Codex usage
    # URL, or credit wording), so a bare mention never counts. Consulted
    # only for failed dispatch turns, never for completed ones.
    CODEX_USAGE_LIMIT_CODES = ("UsageLimitExceeded", "RateLimitExceeded")
    CODEX_USAGE_LIMIT_PHRASE_RE = re.compile(r"(?i)usage[\s_-]?limit")
    CODEX_USAGE_LIMIT_CORROBORATORS = (
        "try again",
        "resets at",
        "reset at",
        "resets in",
        "reset in",
        "chatgpt.com/codex",
        "purchase more credits",
        "purchase credits",
        "rate limit",
        "ratelimit",
        "quota exceeded",
        "quota_exceeded",
        "credits exhausted",
        "out of credit",
    )

    def _usage_limit_text_hit(self, text: str | None) -> str | None:
        """The verbatim usage-limit message in ``text``, or None.

        Matches an exact limit code, or "usage limit" with a provider
        corroborator in the same text. Returns the stripped text.
        """
        if not isinstance(text, str) or not text.strip():
            return None
        if any(code in text for code in self.CODEX_USAGE_LIMIT_CODES):
            return text.strip()
        if self.CODEX_USAGE_LIMIT_PHRASE_RE.search(text) is not None:
            low = text.lower()
            if any(c in low for c in self.CODEX_USAGE_LIMIT_CORROBORATORS):
                return text.strip()
        return None

    @staticmethod
    def _error_dicts_from_jsonl(stdout: str | None):
        """Candidate error dicts from a Codex JSON event stream.

        Yields each top-level object plus its nested ``item``, ``error``,
        and ``data`` dicts one level down, so UsageLimitExceeded /
        RateLimitExceeded items are found whatever wrapper carries them.
        """
        for obj in _jsonl(stdout or ""):
            yield obj
            for key in ("item", "error", "data"):
                nested = obj.get(key)
                if isinstance(nested, dict):
                    yield nested

    @staticmethod
    def _resets_from_dict(blob: dict):
        """(raw resets value, window) from explicit reset fields, or (None, None)."""
        if not isinstance(blob, dict):
            return None, None
        raw = None
        for key in ("resets_at", "reset_at", "resetsAt", "resetAt",
                    "provider_reset_at", "quota_reset_at"):
            if blob.get(key) is not None:
                raw = blob.get(key)
                break
        window = blob.get("window")
        if not isinstance(window, str) or not window:
            window = None
        return raw, window

    def _agent_message_texts(self, stdout: str | None) -> list[str]:
        """Agent message texts from the JSON stream, oldest first."""
        texts: list[str] = []
        for obj in _jsonl(stdout or ""):
            if not isinstance(obj, dict) or obj.get("type") != "item.completed":
                continue
            item = obj.get("item")
            if isinstance(item, dict) and item.get("type") == "agent_message" \
                    and isinstance(item.get("text"), str) and item["text"].strip():
                texts.append(item["text"])
        return texts

    def usage_limit_evidence(self, stdout: str | None, stderr: str | None,
                             cmd=None) -> dict | None:
        """Exhaustion evidence from a Codex usage-limit refusal, or None.

        Scans error items (UsageLimitExceeded / RateLimitExceeded with
        resets_at) first, then the turn's agent messages, the
        output-last-message file, and finally raw stdout/stderr text for
        the provider's usage-limit message. The evidence records the
        verbatim message with its code, raw resets value, and window;
        reset parsing stays with the policy. None when no limit shape is
        present: the caller keeps its generic dispatch failure.
        """
        for blob in self._error_dicts_from_jsonl(stdout):
            code = None
            for key in ("code", "name", "type", "class"):
                val = blob.get(key)
                if isinstance(val, str) and val in self.CODEX_USAGE_LIMIT_CODES:
                    code = val
                    break
            message = None
            for key in ("message", "text", "error"):
                val = blob.get(key)
                if isinstance(val, str) and val.strip():
                    message = val.strip()
                    break
            if code is None:
                # Structured error objects only: an agent message merely
                # mentioning limits is model-authored text and never counts
                # here (it is re-checked with corroborators below).
                if not isinstance(blob, dict) or blob.get("type") != "error":
                    continue
                if message is None \
                        or self.CODEX_USAGE_LIMIT_PHRASE_RE.search(message) is None:
                    continue
            raw_reset, window = self._resets_from_dict(blob)
            if raw_reset is None:
                for key in ("item", "error", "data"):
                    nested = blob.get(key)
                    if isinstance(nested, dict):
                        raw_reset, window = self._resets_from_dict(nested)
                        if raw_reset is not None:
                            break
            try:
                verbatim = adapters.redact_text(str(message or code))
            except Exception:
                verbatim = str(message or code)
            evidence: dict = {"source": "codex",
                              "message": verbatim[:1000]}
            if code is not None:
                evidence["code"] = code
            if raw_reset is not None:
                evidence["resets_at"] = raw_reset
            if window is not None:
                evidence["window"] = window
            return evidence
        candidates: list[str] = []
        candidates.extend(self._agent_message_texts(stdout))
        if cmd is not None:
            try:
                file_text = adapters.read_last_message_file(
                    adapters.last_message_path_from_cmd(cmd))
            except Exception:
                file_text = None
            if isinstance(file_text, str) and file_text.strip():
                candidates.append(file_text)
        for blob in (stdout or "", stderr or ""):
            for line in blob.splitlines():
                if line.strip():
                    candidates.append(line)
        for text in candidates:
            hit = self._usage_limit_text_hit(text)
            if hit is not None:
                try:
                    verbatim = adapters.redact_text(hit)
                except Exception:
                    verbatim = hit
                return {"source": "codex", "message": verbatim[:1000]}
        return None

    def dispatch_limit_signal(self, stdout: str | None, stderr: str | None,
                              cmd=None, rc=None) -> tuple:
        """(signal, evidence) for a failed Codex dispatch turn.

        A usage-limit refusal (message or error item with resets_at) is
        ``exhausted`` with the verbatim message as evidence; anything else
        is (None, None) and the caller keeps its generic failure. Only
        structured limit shapes count: auth text stays ``hard`` via
        ``classify_signal`` and is checked before this.
        """
        try:
            evidence = self.usage_limit_evidence(stdout, stderr, cmd)
        except Exception:
            evidence = None
        if not isinstance(evidence, dict):
            return None, None
        try:
            if policy.classify_signal({"name": evidence.get("code")}) == "exhausted":
                return "exhausted", evidence
        except Exception:
            pass
        if evidence.get("code") in self.CODEX_USAGE_LIMIT_CODES:
            return "exhausted", evidence
        return "exhausted", evidence

    def last_provider_message(self, stdout: str | None, stderr: str | None,
                              cmd=None, limit: int = 200) -> str:
        """Short redacted last provider message for a dispatch block reason.

        Prefers the turn's last agent message, then the output-last-message
        file, then the last non-empty stdout/stderr line. Never empty:
        falls back to "empty provider output".
        """
        texts = self._agent_message_texts(stdout)
        if texts:
            cand = texts[-1]
        else:
            cand = ""
            if cmd is not None:
                try:
                    cand = adapters.read_last_message_file(
                        adapters.last_message_path_from_cmd(cmd)) or ""
                except Exception:
                    cand = ""
            if not (isinstance(cand, str) and cand.strip()):
                lines = ((stdout or "") + "\n" + (stderr or "")).splitlines()
                cand = ""
                for line in reversed(lines):
                    if line.strip():
                        cand = line
                        break
        try:
            cand = adapters.redact_text(cand or "")
        except Exception:
            cand = cand or ""
        cand = " ".join(cand.split())
        return cand[:limit] if cand else "empty provider output"

    def auth_failure_reason(self, stdout: str | None, stderr: str | None) -> str | None:
        """Short redacted auth reason from Codex CLI text, or None.

        Scans stderr then stdout line by line for a missing-auth marker
        or a standalone 401 token (word-boundary match, so "4010" or
        "14012" never count). Returns the first matching line truncated
        to 120 chars (redacted), never a secret value. None when no auth
        marker is present: the caller keeps its generic dispatch failure.
        """
        blobs = [stderr or "", stdout or ""]
        for blob in blobs:
            for line in blob.splitlines():
                low = line.strip().lower()
                if not low:
                    continue
                if (any(m in low for m in self.CODEX_AUTH_TEXT_MARKERS)
                        or self.CODEX_AUTH_CODE_RE.search(low) is not None):
                    try:
                        reason = adapters.redact_text(line.strip())
                    except Exception:
                        reason = line.strip()
                    reason = " ".join(reason.split())
                    return reason[:120] or "codex authentication failed"
        return None

    def classify_signal(self, evidence):
        """Codex signals: structured shapes via policy, 401 text as hard."""
        try:
            cls = policy.classify_signal(evidence)
        except Exception:
            cls = None
        if cls is not None:
            return cls
        if isinstance(evidence, dict):
            blobs = []
            for key in ("message", "error", "stderr", "stdout", "text"):
                val = evidence.get(key)
                if isinstance(val, str) and val.strip():
                    blobs.append(val)
            nested = evidence.get("detail") if isinstance(evidence.get("detail"), dict) else None
            if isinstance(nested, dict):
                for key in ("message", "text"):
                    val = nested.get(key)
                    if isinstance(val, str) and val.strip():
                        blobs.append(val)
            for blob in blobs:
                if self.auth_failure_reason("", blob) is not None:
                    return "hard"
        return None

    def probe_rate_limits(self, payload, observed_at: str,
                          plan: str | None = None) -> list[dict]:
        """Readings from ``account/rateLimits/read`` (every 300 seconds,
        or on demand before a dispatch). Raises on misshapen payloads so
        the caller records unknown."""
        readings = parse_codex_rate_limits(payload, observed_at, plan)
        if not readings:
            raise ValueError("codex rate-limits payload carried no window")
        return readings

    def query_app_server_rate_limits(self, timeout_secs: float = 15.0) -> dict:
        """Raw ``account/rateLimits/read`` payload through the app-server.

        Spawns ``codex app-server`` on stdio (default transport), runs the
        initialize handshake, and reads the rate-limits snapshot. The kit
        login is resolved through the policy home (``CODEX_HOME`` is set
        for the child, so isolated test homes stay isolated). Raises with
        the exact failure (missing binary, timeout, auth error, RPC error)
        so the caller records why the probe could not read. Stdlib only.
        """
        return query_codex_app_server_rate_limits(timeout_secs=timeout_secs)

    def read_rollout(self, path, observed_at: str | None = None) -> list[dict]:
        """Zero-cost readings from the session's own rollout file."""
        readings = read_codex_rollout_reading(path, observed_at)
        if not readings:
            raise ValueError("codex rollout carried no rate record")
        return readings


class ClaudeCLI(Harness):
    name = "claude"
    kinds = (KIND_CLAUDE_CALLBACK, KIND_CLAUDE_COMPACT)
    # The callback runs with no tools, so it cannot write: read-only holds.
    # Compaction rewrites the planner's own transcript summary, which is
    # the harness's session maintenance, not workspace work: read-only
    # still holds for the runner's stage capabilities.
    capabilities = frozenset({"read_only", "session_resume", "structured_output"})
    binary = adapters.CLAUDE_BIN
    timeouts = {KIND_CLAUDE_CALLBACK: 900, KIND_CLAUDE_COMPACT: 900}
    stages = {KIND_CLAUDE_CALLBACK: "planning", KIND_CLAUDE_COMPACT: "planning"}
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
        if kind == KIND_CLAUDE_COMPACT:
            return 0 if adapters.parse_claude_compact_result(stdout).get("ok") else 1
        return 0 if adapters.parse_claude_result(stdout).get("ok") else 1

    def parsed_result(self, kind, stdout):
        """Structured Claude result via the seam, so callers never parse
        the harness output directly."""
        if kind == KIND_CLAUDE_COMPACT:
            return adapters.parse_claude_compact_result(stdout)
        return adapters.parse_claude_result(stdout)

    def parsed_compact_result(self, kind, stdout):
        """Structured Claude compact result via the seam."""
        return adapters.parse_claude_compact_result(stdout)

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

    def compact_failure_reason(self, kind, rc, stdout, job):
        """Recorded compact failure; never blocks the job."""
        parsed = adapters.parse_claude_compact_result(stdout)
        if rc != 0 or not parsed.get("ok"):
            return f"planner_compact_failed rc={rc}"
        if parsed.get("session_id") not in (None, (job or {}).get("planner_session_id")):
            return "planner_compact_session_mismatch"
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

    def probe_oauth_usage(self, payload: dict, observed_at: str,
                          model: str | None = None) -> list[dict]:
        """Readings from the OAuth usage endpoint or the free statusline
        feed (every 180 seconds at most). Raises when no window parses so
        the caller records unknown."""
        readings = parse_claude_oauth_usage(payload, observed_at, model)
        if not readings:
            raise ValueError("claude usage payload carried no window")
        return readings

    def probe_statusline(self, payload: dict, observed_at: str,
                         model: str | None = None) -> list[dict]:
        """The statusline stdin JSON carries the OAuth fields for free."""
        return self.probe_oauth_usage(payload, observed_at, model)

    def probe_usage_text(self, text: str, observed_at: str,
                         model: str | None = None) -> list[dict]:
        """Readings from ``claude -p \"/usage\"`` at zero quota cost."""
        readings = parse_claude_usage_text(text, observed_at, model)
        if not readings:
            raise ValueError("claude /usage text carried no window")
        return readings

    def read_transcript(self, path, observed_at: str | None = None) -> list[dict]:
        """Zero-cost readings from the session's own transcript file."""
        readings = read_claude_transcript_reading(path, observed_at)
        if not readings:
            raise ValueError("claude transcript carried no quota record")
        return readings


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
        (``runner/kits.py``): ``OPENCODE_CONFIG_DIR`` and
        ``OPENCODE_CONFIG`` point at it, so the kit's plugins, skills,
        and MCP subset are allowed and nothing is inherited from the
        user's OpenCode configuration. ``XDG_CONFIG_HOME`` is left
        alone, so the server inherits the runner's user environment for
        ``gh``, git, and every other XDG-aware tool.
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
        """Dispatcher envelope from the owned-server summary's text.

        The summary's ``assistant_text`` is the turn's last assistant
        message verbatim (the live shape is two messages: prose, then
        the envelope): the last complete JSON object in it is the
        envelope, tolerating code fences and surrounding prose. The raw
        text stays in the summary on disk and in the ledger event; only
        a validated action envelope returns.
        """
        summary = self.parse_report(kind, stdout, cmd) or {}
        text = summary.get("assistant_text") if isinstance(summary, dict) else None
        if not isinstance(text, str) or not text.strip():
            return None
        return adapters.parse_opencode_dispatcher_envelope(text)

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

    def probe_go_costs(self, cost_rows, model: str, observed_at: str,
                       pool: str = "go", now_ts: float | None = None) -> list[dict]:
        """Measured Go readings from local-database cost rows against the
        policy tier windows. Raises when the tier is unknown so the
        caller records unknown."""
        try:
            tier = policy.GO_MONTHLY_LIMIT_USD[model.split("/", 1)[1]]
        except (KeyError, IndexError, AttributeError):
            raise ValueError(f"go probe: no monthly tier for {model!r}")
        return probe_opencode_go_readings(cost_rows, tier, model,
                                          observed_at, pool, now_ts)

    def probe_zen_free(self, request_epochs, window: str, observed_at: str,
                       cap: float | None = None,
                       now_ts: float | None = None) -> dict:
        """Measured Zen free request count against the assumed cap."""
        return probe_zen_free_reading(request_epochs, window, observed_at,
                                      cap, now_ts)

    @staticmethod
    def read_cost_rows(db_path, model: str | None = None) -> list[dict]:
        """Best-effort cost rows from the OpenCode local database."""
        return read_opencode_cost_rows(db_path, model)


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
        signal = self.classify_signal(evidence)
        if signal == "exhausted":
            # Grok names no window and no reset for the weekly limit: the
            # exhaustion is assumed weekly with error-driven cool-off, so
            # the assumed-window rule and its re-probe schedule apply.
            evidence["window"] = "weekly"
        return {"grok_session_id": sid, "ok": False,
                "assistant_text": "", "finish": parsed.get("stop_reason"),
                "actual_model": None, "usage": usage, "native_ids": ids,
                "blockers": [], "error": evidence["message"] or "grok turn failed",
                "signal": signal,
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
                             "weekly limit", "weekly_limit", "monthly limit",
                             "monthly_limit", "allowance exceeded",
                             "allowance_exceeded",
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
        if code == 402:
            # Payment/quota refusal on the xAI subscription: exhausted,
            # never a hard auth error.
            return "exhausted"
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

    def probe_billing(self, payload: dict, observed_at: str,
                      model: str | None = None) -> list[dict]:
        """Monthly Grok allowance readings from the internal billing call
        (provider_reported): one per xai-pool Grok route model, so both the
        native Build route and OpenCode's xAI provider skip together. No
        5-hour or weekly window exists: weekly stays assumed with
        error-driven cool-off. Raises when the payload carries no allowance
        so the caller records unknown."""
        readings = parse_grok_billing(payload, observed_at, model)
        if not readings:
            raise ValueError("grok billing payload carried no allowance")
        return readings

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


# ---------------------------------------------------------------------------
# Usage probes (toolboxmd/model-router#34). Each probe returns Reading dicts
# (pool, model, window, used, limit, reset_at, observed_at, source) for
# core.record_reading, or raises on failure so the caller records unknown
# and the route stays eligible on error evidence alone. Live execution
# (subprocess CLIs, token reads, database paths) stays with the operator
# and the release task; these parsers are the deterministic seam.
# ---------------------------------------------------------------------------

def _probe_reading(pool: str, model: str, window: str, used, limit,
                   reset_at: str | None, observed_at: str,
                   source: str, detail: dict | None = None) -> dict:
    if source not in policy.READING_SOURCES:
        raise ValueError(f"invalid reading source: {source!r}")
    return {"pool": pool, "model": model, "window": window, "used": used,
            "limit": limit, "reset_at": reset_at, "observed_at": observed_at,
            "source": source, "detail": detail or {}}


def _pool_route_models(pool: str) -> list[str]:
    """Distinct route models on a pool, in policy order.

    Account-level probes (Codex rate limits, Claude usage, Grok monthly
    billing) report subscription windows, not per-model counters, so each
    window fans out to every route model on its pool. Exact-match joins in
    ``core`` then see the reading on every route that draws on the pool.
    """
    seen: list[str] = []
    for spec in policy.ROUTES.values():
        if spec.get("pool") == pool:
            model = spec.get("model")
            if isinstance(model, str) and model and model not in seen:
                seen.append(model)
    return seen


def _minutes_to_window(mins) -> str | None:
    try:
        mins = int(float(mins))
    except (TypeError, ValueError):
        return None
    if mins <= 0:
        return None
    if 240 <= mins <= 360:
        return "5h"
    if 9000 <= mins <= 11000:
        return "weekly"
    if 40000 <= mins <= 47000:
        return "monthly"
    # Unrecognized provider durations are skipped by the caller: storing an
    # arbitrary '<n>min' window would pollute the ledger and feed preflight
    # on a window the policy never defined.
    return None


def query_codex_app_server_rate_limits(timeout_secs: float = 15.0) -> dict:
    """Raw ``account/rateLimits/read`` result through ``codex app-server``.

    JSON-RPC over stdio (newline-delimited JSON, no ``jsonrpc`` header on
    the wire): ``initialize`` with client metadata, the ``initialized``
    notification, then ``account/rateLimits/read``. Returns the ``result``
    object. Raises on a missing binary, a timeout, an auth refusal, or an
    RPC error, naming the cause, so the caller records exactly why the
    probe could not read. The child inherits the caller's environment with
    ``CODEX_HOME`` pointed at the policy home, so the user's login is used
    in production and isolated homes stay isolated in tests.
    """
    import select as _select

    binary = adapters.CODEX_BIN
    try:
        home = policy._codex_home()
    except Exception:
        home = None
    env = dict(os.environ)
    if home is not None:
        try:
            env["CODEX_HOME"] = os.path.expanduser(str(home))
        except Exception:
            pass
    try:
        timeout = max(1.0, float(timeout_secs))
    except (TypeError, ValueError):
        timeout = 15.0
    # Capability sniff: the deterministic fake CLIs in this suite answer
    # `--version` as fake-harness and speak only the exec contract, never
    # app-server. Spawning app-server against one would run a fake dispatch
    # (polluting call counts and hanging), so a non-zero version exit or a
    # fake version string skips the live read and the caller falls back to
    # the rollout or unknown. The sniff itself is safe: every fake answers
    # `--version` without running its body.
    try:
        ver = subprocess.run(
            [binary, "--version"], capture_output=True, text=True,
            timeout=5, stdin=subprocess.DEVNULL, env=env)
    except FileNotFoundError as e:
        raise RuntimeError(f"codex app-server unavailable: {e}") from e
    except (OSError, subprocess.TimeoutExpired) as e:
        raise RuntimeError(f"codex app-server unavailable: {e}") from e
    ver_out = ((ver.stdout or "") + "\n" + (ver.stderr or "")).strip()
    if ver.returncode != 0 or "fake" in ver_out.lower():
        raise RuntimeError(
            f"codex app-server not served by this binary "
            f"(version: {ver_out[:80] or 'unknown'})")
    deadline = time.time() + timeout
    try:
        proc = subprocess.Popen(
            [binary, "app-server"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1, env=env)
    except FileNotFoundError as e:
        raise RuntimeError(f"codex app-server unavailable: {e}") from e
    except OSError as e:
        raise RuntimeError(f"codex app-server spawn failed: {e}") from e

    def _remaining() -> float:
        return max(0.1, deadline - time.time())

    def _send(obj: dict) -> None:
        try:
            proc.stdin.write(json.dumps(obj) + "\n")
            proc.stdin.flush()
        except (BrokenPipeError, OSError, ValueError) as e:
            raise RuntimeError(f"codex app-server write failed: {e}") from e

    def _read_id(want_id) -> dict:
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"codex app-server exited rc={proc.returncode} before answering")
            try:
                ready, _, _ = _select.select([proc.stdout], [], [], _remaining())
            except (OSError, ValueError) as e:
                raise RuntimeError(f"codex app-server read failed: {e}") from e
            if not ready:
                break
            line = proc.stdout.readline()
            if not line:
                break
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict) or obj.get("id") != want_id:
                continue
            return obj
        raise RuntimeError("codex app-server read timed out")

    try:
        _send({"id": 0, "method": "initialize",
               "params": {"clientInfo": {"name": "model-router",
                                         "title": "Model Router",
                                         "version": "1.0.0"}}})
        init = _read_id(0)
        if isinstance(init.get("error"), dict):
            raise RuntimeError(
                f"codex app-server initialize refused: "
                f"{str(init['error'].get('message') or init['error'])[:200]}")
        if "result" not in init:
            raise RuntimeError("codex app-server initialize returned no result")
        _send({"method": "initialized", "params": {}})
        _send({"id": 1, "method": "account/rateLimits/read"})
        resp = _read_id(1)
        if isinstance(resp.get("error"), dict):
            raise RuntimeError(
                f"codex account/rateLimits/read refused: "
                f"{str(resp['error'].get('message') or resp['error'])[:200]}")
        result = resp.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("codex account/rateLimits/read returned no result")
        return result
    finally:
        for stream in (proc.stdin, proc.stdout):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def codex_payload_shape(payload) -> list[str]:
    """Keys-only outline of a rate-limits payload for the ledger.

    Top-level keys plus one level of nested keys under ``rateLimits``,
    ``rate_limits``, and ``result`` (``parent.child``). Values never
    travel: the shape lets the parser be fixed from the ledger when a
    response carries no rate record.
    """
    shape: set[str] = set()
    if isinstance(payload, dict):
        for key in payload:
            shape.add(str(key))
        for parent in ("rateLimits", "rate_limits", "result"):
            nested = payload.get(parent)
            if isinstance(nested, dict):
                for key in nested:
                    shape.add(f"{parent}.{str(key)}")
    elif isinstance(payload, list):
        shape.add(f"list[{len(payload)}]")
    else:
        shape.add(type(payload).__name__)
    return sorted(shape)


def parse_codex_rate_limits(payload, observed_at: str, plan: str | None = None) -> list[dict]:
    """Readings from Codex ``account/rateLimits/read``.

    Each limit window carries ``usedPercent``, ``windowDurationMins``,
    ``resetsAt`` (epoch or ISO), plus the plan type. The app-server answers
    with ``rateLimits`` holding ``primary``/``secondary`` limit records (a
    JSON-RPC ``result`` wrapper is unwrapped); older shapes carry a
    ``windows``/``rate_limits`` list. Limits are account-level, so every
    window fans out to one provider_reported Reading per codex-pool route
    model; exact-match joins in ``core`` then see it on every Codex route.
    An entry naming a route model already in the policy stays on that
    single model. Unrecognized window durations are skipped. Empty or
    misshapen payloads yield no readings: the caller records unknown with
    the payload shape.
    """
    if isinstance(payload, dict):
        if isinstance(payload.get("result"), dict):
            payload = payload["result"]
        plan = (plan or payload.get("plan") or payload.get("plan_type")
                or payload.get("planType"))
        raw = payload.get("windows")
        if raw is None:
            for key in ("rate_limits", "rateLimits"):
                cand = payload.get(key)
                if isinstance(cand, list):
                    raw = cand
                    break
                if isinstance(cand, dict):
                    recs = [v for v in cand.values() if isinstance(v, dict)]
                    if recs:
                        raw = recs
                        break
                    if "usedPercent" in cand or "used_percent" in cand:
                        raw = [cand]
                        break
            if raw is None:
                if "usedPercent" in payload or "used_percent" in payload:
                    raw = [payload]
                else:
                    raw = []
        windows = raw
    elif isinstance(payload, list):
        windows = payload
    else:
        return []
    now_ts = time.time()
    try:
        now_ts = datetime.datetime.fromisoformat(
            str(observed_at).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, OverflowError):
        pass
    pool_models = _pool_route_models("codex")
    out: list[dict] = []
    for entry in windows:
        if not isinstance(entry, dict):
            continue
        used = entry.get("usedPercent", entry.get("used_percent"))
        mins = entry.get("windowDurationMins", entry.get("window_minutes"))
        reset = entry.get("resetsAt", entry.get("resets_at",
                         entry.get("reset_at", entry.get("resetAt"))))
        window = _minutes_to_window(mins)
        if window is None or used is None:
            continue
        try:
            if isinstance(used, bool):
                continue
            used_f = float(used)
        except (TypeError, ValueError):
            continue
        reset_iso = policy._coerce_reset_moment(reset, now_ts)
        entry_model = entry.get("model")
        if isinstance(entry_model, str) and entry_model in pool_models:
            models = [entry_model]
        else:
            models = pool_models
        for model in models:
            out.append(_probe_reading(
                "codex", str(model), window, used_f, 100.0, reset_iso,
                observed_at, "provider_reported",
                {"window_minutes": mins,
                 "plan": entry.get("plan_type", plan)}))
    return out


def read_codex_rollout_reading(path, observed_at: str | None = None) -> list[dict]:
    """Zero-cost Codex readings from the session's own rollout file.

    Rollouts carry ``rate_limits.primary`` (``used_percent``,
    ``window_minutes``, ``resets_at``, ``plan_type``). The latest such
    record fans out to one provider_reported Reading per codex-pool route
    model with the file's timestamp as observed_at: no probe call, no quota
    cost. Unrecognized window durations yield no readings. Missing files or
    records yield no readings.
    """
    try:
        text = open(str(path), encoding="utf-8").read()
    except OSError:
        return []
    if observed_at is None:
        try:
            mtime = os.path.getmtime(str(path))
            observed_at = datetime.datetime.fromtimestamp(
                mtime, datetime.timezone.utc).isoformat()
        except OSError:
            observed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    now_ts = time.time()
    try:
        now_ts = datetime.datetime.fromisoformat(
            observed_at.replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, OverflowError):
        pass
    last: dict | None = None
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
        primary = (obj.get("rate_limits") or {}).get("primary") \
            if isinstance(obj.get("rate_limits"), dict) else None
        if primary is None and isinstance(obj.get("primary"), dict):
            primary = obj.get("primary")
        if isinstance(primary, dict) and primary.get("used_percent") is not None:
            last = primary
    if last is None:
        return []
    window = _minutes_to_window(last.get("window_minutes"))
    if window is None:
        return []
    try:
        used_f = float(last["used_percent"])
        if isinstance(last["used_percent"], bool):
            return []
    except (TypeError, ValueError, KeyError):
        return []
    reset_iso = policy._coerce_reset_moment(last.get("resets_at"), now_ts)
    pool_models = _pool_route_models("codex")
    entry_model = last.get("model")
    if isinstance(entry_model, str) and entry_model in pool_models:
        models = [entry_model]
    else:
        models = pool_models
    return [_probe_reading("codex", str(model), window, used_f, 100.0,
                           reset_iso, observed_at, "provider_reported",
                           {"via": "rollout"})
            for model in models]


_CLAUDE_WINDOW_NAMES = {"five_hour": "5h", "five-hour": "5h", "5h": "5h",
                        "seven_day": "weekly", "seven-day": "weekly",
                        "weekly": "weekly", "session": "session",
                        "monthly": "monthly"}


def parse_claude_oauth_usage(payload: dict, observed_at: str,
                             model: str | None = None) -> list[dict]:
    """Readings from the Claude OAuth usage endpoint.

    Structured ``five_hour`` and ``seven_day`` entries with ``utilization``
    and ``resets_at`` (needs the login token and the CLI user agent; 180
    seconds minimum between calls). The statusline stdin JSON carries the
    same fields for free while a session runs and parses through this
    same function. Usage is subscription-level, so every window fans out to
    one Reading per claude-pool route model unless the caller names a route
    model already in the policy.
    """
    if not isinstance(payload, dict):
        return []
    now_ts = time.time()
    try:
        now_ts = datetime.datetime.fromisoformat(
            str(observed_at).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, OverflowError):
        pass
    pool_models = _pool_route_models("claude")
    explicit = model or payload.get("model")
    if isinstance(explicit, str) and explicit in pool_models:
        models = [explicit]
    else:
        models = pool_models
    out: list[dict] = []
    for key, window in _CLAUDE_WINDOW_NAMES.items():
        entry = payload.get(key)
        if not isinstance(entry, dict):
            continue
        used = entry.get("utilization", entry.get("used_percent",
                 entry.get("usedPercent")))
        if used is None or isinstance(used, bool):
            continue
        try:
            used_f = float(used)
        except (TypeError, ValueError):
            continue
        reset_iso = policy._coerce_reset_moment(
            entry.get("resets_at", entry.get("resetsAt")), now_ts)
        for route_model in models:
            out.append(_probe_reading(
                "claude", str(route_model),
                window, used_f, 100.0, reset_iso, observed_at,
                "provider_reported", {"endpoint": "oauth"}))
    return out


_USAGE_PERCENT_RE = re.compile(
    r"(?i)(session|five[\s_-]?hour|5[\s_-]?hour|5h|seven[\s_-]?day|weekly|week|monthly|month)"
    r"[^\n%]{0,80}?(\d+(?:\.\d+)?)\s*%")
_ISO_MOMENT_RE = re.compile(
    r"20\d\d-\d\d-\d\d[T ]\d\d:\d\d(?::\d\d)?(?:Z|[+-]\d\d:?\d\d)?")


def _claude_text_window(label: str) -> str:
    low = label.lower().replace("_", " ").replace("-", " ")
    if "session" in low:
        return "session"
    if "five" in low or "5h" in low or "5hour" in low or "5 hour" in low:
        return "5h"
    if "seven" in low or "week" in low:
        return "weekly"
    if "month" in low:
        return "monthly"
    return "unknown"


def parse_claude_usage_text(text: str, observed_at: str,
                            model: str | None = None) -> list[dict]:
    """Readings from ``claude -p \"/usage\" --output-format json``.

    The command costs zero quota; its human-readable text names the
    session and weekly percentages with reset times. Each labeled
    percentage fans out to one provider_reported Reading per claude-pool
    route model (unless the caller names one), paired in order with the ISO
    reset moments in the same text (a window without a moment keeps reset
    None and the assumed rule applies on errors). Unparseable text yields
    no readings: the caller records unknown.
    """
    if not isinstance(text, str) or not text.strip():
        return []
    found = _USAGE_PERCENT_RE.findall(text)
    moments = _ISO_MOMENT_RE.findall(text)
    now_ts = time.time()
    try:
        now_ts = datetime.datetime.fromisoformat(
            str(observed_at).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, OverflowError):
        pass
    pool_models = _pool_route_models("claude")
    if isinstance(model, str) and model in pool_models:
        models = [model]
    else:
        models = pool_models
    out: list[dict] = []
    for i, (label, pct) in enumerate(found):
        window = _claude_text_window(label)
        if window == "unknown":
            continue
        try:
            used_f = float(pct)
        except (TypeError, ValueError):
            continue
        reset_iso = None
        if i < len(moments):
            reset_iso = policy._coerce_reset_moment(moments[i], now_ts)
        for route_model in models:
            out.append(_probe_reading(
                "claude", str(route_model), window, used_f, 100.0,
                reset_iso, observed_at, "provider_reported",
                {"via": "cli-/usage"}))
    return out


def read_claude_transcript_reading(path, observed_at: str | None = None) -> list[dict]:
    """Zero-cost Claude readings from the session's own transcript file.

    Transcripts carry ``quotaLimits`` (``rateLimitType`` five_hour,
    ``resetsAt``). The latest such record fans out to one provider_reported
    Reading per claude-pool route model with the file's timestamp as
    observed_at. Unknown rate-limit types yield no readings.
    """
    try:
        text = open(str(path), encoding="utf-8").read()
    except OSError:
        return []
    if observed_at is None:
        try:
            mtime = os.path.getmtime(str(path))
            observed_at = datetime.datetime.fromtimestamp(
                mtime, datetime.timezone.utc).isoformat()
        except OSError:
            observed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    now_ts = time.time()
    try:
        now_ts = datetime.datetime.fromisoformat(
            observed_at.replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, OverflowError):
        pass
    last: dict | None = None
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
        quota = obj.get("quotaLimits") or obj.get("quota_limits")
        if isinstance(quota, dict):
            last = quota
        elif isinstance(quota, list) and quota \
                and isinstance(quota[0], dict):
            last = quota[0]
    if last is None:
        return []
    rate_type = str(last.get("rateLimitType") or last.get("rate_limit_type") or "")
    window = _CLAUDE_WINDOW_NAMES.get(rate_type)
    if window is None:
        return []
    used = last.get("utilization", last.get("used_percent",
             last.get("usedPercent")))
    used_f = None
    limit_f = None
    if used is not None and not isinstance(used, bool):
        try:
            used_f = float(used)
            limit_f = 100.0
        except (TypeError, ValueError):
            used_f, limit_f = None, None
    reset_iso = policy._coerce_reset_moment(
        last.get("resetsAt", last.get("resets_at")), now_ts)
    pool_models = _pool_route_models("claude")
    entry_model = last.get("model")
    if isinstance(entry_model, str) and entry_model in pool_models:
        models = [entry_model]
    else:
        models = pool_models
    return [_probe_reading("claude", str(route_model), window, used_f, limit_f,
                           reset_iso, observed_at, "provider_reported",
                           {"via": "transcript"})
            for route_model in models]


GO_WINDOW_SECS = {"5h": 5 * 3600, "weekly": 7 * 24 * 3600,
                  "monthly": 30 * 24 * 3600}


def probe_opencode_go_readings(cost_rows, tier_usd: float, model: str,
                               observed_at: str, pool: str = "go",
                               now_ts: float | None = None) -> list[dict]:
    """Measured Go readings from the local database's cost per model.

    Go limits are dollar limits, so rolling cost sums over the 5-hour
    (20 percent), weekly (50 percent), and monthly (100 percent) windows
    are comparable. Each window becomes one measured Reading; rolling
    windows name no exact reset, so reset stays None and error evidence
    governs exhaustion. ``cost_rows`` are ``{"cost": float, "ts": epoch}``
    dicts (the model match already applied by the caller); tier_usd is
    the policy's monthly dollar limit for the model.
    """
    try:
        tier = float(tier_usd)
        if tier <= 0:
            return []
    except (TypeError, ValueError):
        return []
    now = now_ts if now_ts is not None else time.time()
    costs: list[tuple[float, float]] = []
    for row in cost_rows or []:
        if isinstance(row, dict):
            cost, ts = row.get("cost"), row.get("ts")
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            cost, ts = row[0], row[1]
        else:
            continue
        try:
            if isinstance(cost, bool) or isinstance(ts, bool):
                continue
            costs.append((float(cost), float(ts)))
        except (TypeError, ValueError):
            continue
    out: list[dict] = []
    for window, share in policy.WINDOWS.items():
        secs = GO_WINDOW_SECS[window]
        used = round(sum(c for c, ts in costs if now - ts <= secs), 6)
        out.append(_probe_reading(
            pool, str(model), window, used, round(tier * float(share), 6),
            None, observed_at, "measured",
            {"tier_usd": tier, "share": float(share),
             "window_secs": secs, "samples": len(costs)}))
    return out


def read_opencode_cost_rows(db_path, model: str | None = None) -> list[dict]:
    """Best-effort cost rows from the OpenCode local session database.

    Reads per-assistant-message cost records ``{"cost": float, "ts": epoch,
    "model": str}`` for probe_opencode_go_readings. The local schema is
    versioned by OpenCode, so known table and column shapes are tried in
    order; anything unreadable yields no rows and the caller records
    unknown instead of blocking routing.
    """
    import sqlite3 as _sqlite

    try:
        con = _sqlite.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    except Exception:
        return []
    rows: list[dict] = []
    try:
        try:
            tables = {r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        except Exception:
            return []
        for table in ("message", "messages", "assistant_message",
                      "assistant_messages", "session_message"):
            if table not in tables:
                continue
            try:
                cols = {r[1] for r in con.execute(
                    f"PRAGMA table_info({table})").fetchall()}
            except Exception:
                continue
            cost_col = next((c for c in ("cost", "costUsd", "cost_usd",
                                         "total_cost") if c in cols), None)
            ts_col = next((c for c in ("created", "createdAt", "created_at",
                                       "timestamp", "ts", "time") if c in cols), None)
            model_col = next((c for c in ("model", "modelID", "model_id",
                                          "providerModel") if c in cols), None)
            if cost_col is None or ts_col is None:
                continue
            select = f"SELECT {cost_col} AS cost, {ts_col} AS ts"
            if model_col is not None:
                select += f", {model_col} AS model"
            select += f" FROM {table}"
            try:
                for rec in con.execute(select).fetchall():
                    try:
                        cost = float(rec[0])
                    except (TypeError, ValueError):
                        continue
                    ts = rec[1]
                    try:
                        ts_f = float(ts)
                    except (TypeError, ValueError):
                        try:
                            ts_f = datetime.datetime.fromisoformat(
                                str(ts).replace("Z", "+00:00")).timestamp()
                        except (ValueError, TypeError, OverflowError):
                            continue
                    got_model = rec[2] if len(rec) > 2 else None
                    if model is not None and got_model not in (None, model):
                        continue
                    rows.append({"cost": cost, "ts": ts_f,
                                 "model": got_model})
                if rows:
                    return rows
            except Exception:
                continue
        return rows
    finally:
        try:
            con.close()
        except Exception:
            pass


def probe_zen_free_reading(request_epochs, window: str, observed_at: str,
                           cap: float | None = None,
                           now_ts: float | None = None) -> dict:
    """Measured Zen free reading: request counts against an assumed cap.

    Zen names no allowance, so the window count is measured and the cap
    is assumed (flagged in the detail); the source stays measured because
    the count itself is observed.
    """
    secs = GO_WINDOW_SECS.get(window)
    if secs is None:
        raise ValueError(f"unsupported zen window: {window!r}")
    now = now_ts if now_ts is not None else time.time()
    count = 0
    for ts in request_epochs or []:
        try:
            if isinstance(ts, bool):
                continue
            if now - float(ts) <= secs:
                count += 1
        except (TypeError, ValueError):
            continue
    limit = float(cap) if cap is not None else float(policy.ZEN_FREE_ASSUMED_REQUESTS)
    return _probe_reading("zen-free",
                          "opencode/muse-spark-1.3-contributor-free",
                          window, float(count), limit, None, observed_at,
                          "measured",
                          {"assumed_cap": cap is None,
                           "window_secs": secs})


def parse_grok_billing(payload: dict, observed_at: str,
                       model: str | None = None) -> list[dict]:
    """Monthly Grok allowance readings from the internal billing call.

    The allowance data behind the TUI modal (``creditUsagePercent``,
    ``includedUsed``, ``monthlyLimit``, ``billingPeriodStart`` and
    ``billingPeriodEnd``) fans out to one provider_reported monthly Reading
    per xai-pool Grok route model (native Build and OpenCode's xAI
    provider share the xAI subscription; the Go Grok route keeps its own
    Go-dollar readings and is never overwritten here). Internal and may
    change; failure yields no readings so the caller records unknown. No
    5-hour or weekly window and no weekly reset exist: weekly stays assumed
    with error-driven cool-off.
    """
    if not isinstance(payload, dict):
        return []
    now_ts = time.time()
    try:
        now_ts = datetime.datetime.fromisoformat(
            str(observed_at).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError, OverflowError):
        pass
    used = payload.get("creditUsagePercent", payload.get("credit_usage_percent"))
    limit: float | None = 100.0
    if used is not None and not isinstance(used, bool):
        try:
            used_f = float(used)
        except (TypeError, ValueError):
            return []
    else:
        taken = payload.get("includedUsed", payload.get("included_used"))
        cap = payload.get("monthlyLimit", payload.get("monthly_limit"))
        try:
            if taken is None or cap is None or float(cap) <= 0:
                return []
            used_f, limit = float(taken), float(cap)
        except (TypeError, ValueError):
            return []
    reset_iso = policy._coerce_reset_moment(
        payload.get("billingPeriodEnd", payload.get("billing_period_end")),
        now_ts)
    pool_models = _pool_route_models("xai")
    if isinstance(model, str) and model in pool_models:
        models = [model]
    else:
        models = pool_models
    return [_probe_reading("xai", str(route_model), "monthly", used_f, limit,
                           reset_iso, observed_at, "provider_reported",
                           {"via": "billing"})
            for route_model in models]


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
        if any(str(a).strip().startswith("/compact") for a in args):
            return KIND_CLAUDE_COMPACT
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
