"""Dispatcher prompt, envelope parsing, and redaction. Stdlib only.

The dispatcher (Luna) runs as a T3 thread; this module builds its
prompts, extracts its action envelope from the assistant text, and
redacts secrets before anything reaches the ledger. See RUNNER.md.
"""
from __future__ import annotations

import json

# Explicit structured-action protocol embedded in every Luna prompt.
# Luna must reply with exactly one JSON envelope as its final message.
LUNA_ACTION_PROTOCOL = (
    "ROLE: you are the dispatcher for this runner job. Your sandbox is "
    "read-only: do not edit files. The submitted candidate stays yours: "
    "assign implementation, debugging, test execution, and mechanical "
    "recovery yourself with the implementation action; put complete worker "
    "instructions in payload.instructions. The runner sends them to the "
    "implementation worker and returns its result to you with its evidence. "
    "Ask the saved planner with planner_question only when a decision belongs "
    "to the planner (missing authority, or a consequential scope or approach "
    "choice): carry the decision required, the evidence, attempted remedies, "
    "and your recommendation, then assign the planner's direction under the "
    "routing policy (name an eligible policy route only to direct a stronger "
    "agent; the runner never substitutes silently). A failure never authorizes "
    "the planner to implement. "
    "The worker commits as it works, then pushes the branch and updates the "
    "existing PR without merging (one PR per job, never a second), "
    "and reports its URL. Consume the runner's exact-candidate proof evidence "
    "bound to the current candidate commit; request new proof only when that "
    "evidence is missing, stale, or for a different candidate. Confirm the "
    "branch is pushed with one open PR pointing at the current candidate, "
    "with required acceptance evidence, before reporting completion. When "
    "the workspace has no origin push remote, no PR is possible: complete "
    "without a pr_url instead of asking the planner. Do not "
    "start other agents or models yourself.\n"
    "REPLY PROTOCOL (required): emit exactly one JSON object as your final "
    "message, on its own line, with one of these shapes:\n"
    '{"action":"planner_question","qid":"q1","prompt":"<question for the human planner>"}\n'
    '{"action":"implementation","artifact":"<path or empty>","payload":{"instructions":"<complete worker instructions>","proof":"<the target project\'s documented proof command>"}}\n'
    '{"action":"completion","output":"<final result text>","artifact":"<path or empty>","pr_url":"<opened PR URL>","acceptance_evidence":"<required acceptance evidence when the task names it>"}\n'
    "On the first implementation action, set payload.proof to the target "
    "project's own documented proof command (the one you give the worker); "
    "the runner binds it once and runs it after every worker turn. The "
    "task's proof wins when it names one.\n"
    "The escalation ladder owns the worker route: an ordinary implementation "
    "envelope carries no route field and never changes the route. Relay an "
    "explicit planner direction only with the optional field "
    '"directed_route" (an eligible policy route the planner chose for this '
    "job); the runner assigns it under the routing policy or rejects it "
    "with evidence, never substituting silently. "
    "Rules: exactly one envelope; valid JSON; action must be one of "
    "planner_question, implementation, completion; use a new qid for each "
    "new question; completion carries the opened PR URL and required acceptance "
    "evidence; never invent a new planner or Codex session ID."
)


def build_luna_followup(kind: str, body: str) -> str:
    """Frame a resumed-turn message for the saved Luna task."""
    return f"{kind}:\n{body}\n\n{LUNA_ACTION_PROTOCOL}"


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


def redact_nested(obj):
    """Recursively redact secret-bearing keys and free-text secret shapes.

    Key-based redaction cannot see inside free-text values (for example
    ``class=password=hunter2``), so every string also passes through
    :func:`redact_text` before it is persisted or forwarded.
    """
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
    if isinstance(obj, str):
        masked = redact_text(obj)
        if len(masked) > 4000:
            return masked[:4000] + "...<truncated>"
        return masked
    return obj


def redact_text(text: str) -> str:
    """Best-effort redaction of credential-like substrings in free text.

    Worker prose and proof output are model- or command-authored and can
    echo secrets. Structured key redaction cannot see inside them, so mask
    common secret shapes before persisting or forwarding the text.
    """
    import re as _re

    if not isinstance(text, str) or not text:
        return text
    redacted = text
    # k=v style secrets: password=..., secret: ..., api_key=..., token=...
    redacted = _re.sub(
        r"(?i)(password|secret|passwd|api[_-]?key|apikey|credential|auth[_-]?token|access[_-]?token"
        r"|authorization|cookie|set-cookie)\s*[:=]\s*\S+",
        r"\1=<redacted>", redacted)
    redacted = _re.sub(r"(?i)Bearer\s+\S+", "Bearer <redacted>", redacted)
    redacted = _re.sub(r"(?i)Basic\s+[A-Za-z0-9+/=]{8,}", "Basic <redacted>", redacted)
    redacted = _re.sub(r"sk-[A-Za-z0-9\-_]{8,}", "sk-<redacted>", redacted)
    redacted = _re.sub(r"gh[pousr]_[A-Za-z0-9]{8,}", "gh-<redacted>", redacted)
    redacted = _re.sub(r"xox[bap]-" r"[A-Za-z0-9\-]+", "xox-<redacted>", redacted)
    redacted = _re.sub(r"AKIA[0-9A-Z]{16}", "AKIA<redacted>", redacted)
    redacted = _re.sub(r"-----BEGIN [^-]*PRIVATE KEY-----", "-----BEGIN <redacted> PRIVATE KEY-----", redacted)
    return redacted

