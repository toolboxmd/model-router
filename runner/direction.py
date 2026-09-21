"""Project Direction supply through the harness seam. Stdlib only.

Every role session's input carries the current Project Direction of its
workspace. The installed AgentsMD loader (``project-direction`` on PATH,
the ``project-direction hook`` for the host) owns the block; the runner
attaches it verbatim only where no hook supplies it, and records the
supply mechanism everywhere.

Supply mechanisms (Agent Observer ``supply``), decided from the route's
kit on the owned OpenCode server:

- ``runner``: the owned OpenCode server with a kit that does not name
  the ``agentsmd-project-direction`` plugin. The runner ran the installed
  loader for the job's workspace and prepended its verbatim block (plus
  the core instruction link and the project's own AGENTS.md when present)
  to the session input.
- ``hook``: a host whose own hook supplies direction (Codex, Claude,
  Grok, plus any owned-OpenCode kit that names the
  ``agentsmd-project-direction`` plugin). The runner still ran the loader
  once per invocation to learn the block hash for the ledger but did not
  duplicate the block in the prompt.
- ``none``: the loader was missing or failed. The invocation records the
  reason, the worker prompt names VISION.md, MISSION.md, and OBJECTIVE.md
  to read, and the job continues.

Every loader call from the runner uses a unique ``session_id``
(``model-router-<request>-<invocation>-<uuid>``) so the loader's
per-session hook cache never suppresses a block; the loader is still
called exactly once per invocation.

The loader is never fabricated: absence or failure is recorded, never
invented. ``status`` carries the loader payload status (``ready``,
``potentially_stale``, ``uninitialized``, ``not_in_repository``,
``read_required``) or the gap reason when none.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

BLOCK_START = "<<<AGENTSMD_PROJECT_DIRECTION_V1>>>"
BLOCK_END = "<<<END_AGENTSMD_PROJECT_DIRECTION_V1>>>"

REQUIRED_FILES = ("VISION.md", "MISSION.md", "OBJECTIVE.md")

# Hosts whose own hook supplies direction; the runner records hook and
# does not duplicate the block. On the owned OpenCode server the kit
# decides: a kit naming the agentsmd-project-direction plugin is a hook
# host (the plugin's SessionStart transform supplies direction); a kit
# without it gets the runner's verbatim injection.
HOOK_HOSTS = frozenset({"codex", "claude", "grok"})
RUNNER_HOSTS = frozenset({"opencode"})

# Kit plugin that makes an owned-OpenCode kit a hook host.
DIRECTION_PLUGIN = "agentsmd-project-direction"


def _plugins_for_kit_name(kit_name: str | None) -> list:
    """Plugin names for a kit role, or [] when unknown. Never raises."""
    if not isinstance(kit_name, str) or not kit_name:
        return []
    try:
        from . import policy as _policy
        kit = _policy.KITS.get(kit_name)
        if isinstance(kit, dict) and isinstance(kit.get("plugins"), list):
            return [p for p in kit["plugins"] if isinstance(p, str)]
    except Exception:
        pass
    return []


def kit_has_direction_plugin(kit_name: str | None = None,
                             kit_plugins: list | None = None) -> bool:
    """True when the kit carries the direction hook plugin. Never raises."""
    try:
        if kit_plugins is not None:
            return DIRECTION_PLUGIN in list(kit_plugins or [])
        return DIRECTION_PLUGIN in _plugins_for_kit_name(kit_name)
    except Exception:
        return False

LOADER_BIN_NAME = "project-direction"
LOADER_ENV = "MODEL_ROUTER_PROJECT_DIRECTION_BIN"
LOADER_TIMEOUT_SECS = 10


def find_loader() -> str | None:
    """Path of the installed AgentsMD loader, or None when missing."""
    override = os.environ.get(LOADER_ENV)
    if override and override.strip():
        cand = os.path.expanduser(override.strip())
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
        # An explicit override naming a missing file is missing, never a
        # silent fallback to another loader.
        if os.path.isfile(cand):
            return cand
        return None
    found = shutil.which(LOADER_BIN_NAME)
    if found:
        return found
    # Fall back to the loader beside the installed canonical source: each
    # host home holds an AGENTS.md link to the canonical checkout, whose
    # bin/project-direction is the loader.
    for home_env in ("MODEL_ROUTER_OPENCODE_HOME", "MODEL_ROUTER_CODEX_HOME",
                     "MODEL_ROUTER_CLAUDE_HOME", "MODEL_ROUTER_GROK_HOME",
                     "CODEX_HOME", "CLAUDE_CONFIG_DIR", "GROK_HOME"):
        val = os.environ.get(home_env)
        if not val:
            continue
        try:
            link = Path(os.path.expanduser(val)) / "AGENTS.md"
            if link.is_symlink():
                raw = os.readlink(link)
                base = Path(raw) if os.path.isabs(raw) else (link.parent / raw)
                src_dir = base.parent
                cand = src_dir / "bin" / "project-direction"
                if cand.is_file():
                    return str(cand)
        except OSError:
            continue
    # Last resort: well-known homes.
    for cand in (Path.home() / ".agents" / "bin" / "project-direction",
                 Path.home() / ".codex" / "bin" / "project-direction"):
        try:
            if cand.is_file():
                return str(cand)
        except OSError:
            continue
    return None


def _extract_block(stdout: str) -> str | None:
    """Verbatim direction block (delimiters included) from loader output."""
    if not stdout or BLOCK_START not in stdout or BLOCK_END not in stdout:
        return None
    start = stdout.index(BLOCK_START)
    end = stdout.index(BLOCK_END, start) + len(BLOCK_END)
    return stdout[start:end]


def _payload_from_block(block: str) -> dict | None:
    try:
        inner = block[len(BLOCK_START):-len(BLOCK_END)].strip()
        obj = json.loads(inner)
    except (ValueError, IndexError):
        return None
    return obj if isinstance(obj, dict) else None


def _payload_from_hook_json(stdout: str) -> dict | None:
    """Loader hook JSON carries the block in additionalContext."""
    try:
        obj = json.loads((stdout or "").strip())
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    hook = obj.get("hookSpecificOutput") if isinstance(obj.get("hookSpecificOutput"), dict) else None
    ctx = (hook or {}).get("additionalContext")
    if not isinstance(ctx, str) or BLOCK_START not in ctx:
        return None
    return _payload_from_block(_extract_block(ctx) or "")


def block_hash(block: str | None) -> str | None:
    if not block:
        return None
    return hashlib.sha256(block.encode("utf-8")).hexdigest()


def project_agentsmd(workspace: str | None) -> tuple[str | None, str | None]:
    """(path, content) of the workspace's own AGENTS.md when present."""
    if not workspace:
        return None, None
    try:
        cand = Path(workspace) / "AGENTS.md"
        if cand.is_file() and not cand.is_dir():
            return str(cand), cand.read_text(encoding="utf-8")
    except OSError:
        pass
    return None, None


def core_link_text(payload: dict | None) -> str | None:
    """Human-readable core instruction link from the loader payload."""
    if not isinstance(payload, dict):
        return None
    instr = payload.get("instructions")
    if not isinstance(instr, dict):
        return None
    target = instr.get("resolved_target") or instr.get("link_target") or instr.get("target")
    sha = instr.get("sha256") or instr.get("target_sha256")
    if isinstance(target, str) and target:
        if isinstance(sha, str) and sha:
            return f"{target} (sha256:{sha[:16]}...)"
        return target
    return None


def load_direction(workspace: str, host: str = "opencode",
                   loader: str | None = None,
                   timeout: float = LOADER_TIMEOUT_SECS,
                   session_id: str | None = None) -> dict:
    """Run the installed loader for ``workspace``. Never raises.

    ``session_id`` is the loader hook session id. Every call uses a unique
    one (``model-router-<request>-<invocation>-<uuid>`` from the caller,
    else a fresh ``model-router-<uuid>`` here) so the loader's per-session
    hook cache never suppresses a block. The loader is still called exactly
    once per invocation by the caller.

    Returns ``{ok, block, payload, status, files, reason, loader,
    session_id}``. ``ok`` is True only when a verbatim block with a
    parseable payload was read; otherwise ``block`` is None and ``reason``
    names the gap (``loader missing``, ``loader failed ...``, ``no block
    in loader output``).
    """
    if not isinstance(session_id, str) or not session_id.strip():
        try:
            import secrets as _secrets
            session_id = f"model-router-{_secrets.token_hex(4)}"
        except Exception:
            session_id = "model-router-fallback"
    else:
        session_id = session_id.strip()
    exe = loader or find_loader()
    if not exe:
        return {"ok": False, "block": None, "payload": None, "status": "gap",
                "files": [], "reason": "loader missing: project-direction not on PATH",
                "loader": None, "session_id": session_id}
    stdin_obj = {"cwd": str(workspace), "hook_event_name": "UserPromptSubmit",
                 "session_id": session_id}
    try:
        proc = subprocess.run(
            [exe, "--host", host, "hook"],
            input=json.dumps(stdin_obj),
            capture_output=True, text=True, timeout=timeout,
            cwd=str(workspace) if workspace and os.path.isdir(str(workspace)) else None,
            env=dict(os.environ))
    except FileNotFoundError:
        return {"ok": False, "block": None, "payload": None, "status": "gap",
                "files": [], "reason": "loader missing: project-direction not executable",
                "loader": exe, "session_id": session_id}
    except subprocess.TimeoutExpired:
        return {"ok": False, "block": None, "payload": None, "status": "gap",
                "files": [], "reason": "loader failed: timeout",
                "loader": exe, "session_id": session_id}
    except OSError as e:
        return {"ok": False, "block": None, "payload": None, "status": "gap",
                "files": [], "reason": f"loader failed: {type(e).__name__}",
                "loader": exe, "session_id": session_id}
    out = proc.stdout or ""
    if proc.returncode != 0:
        # A failing loader may still print a block on stdout; prefer it
        # when present, else record the failure.
        block = _extract_block(out)
        if block and _payload_from_block(block) is not None:
            payload = _payload_from_block(block)
            return {"ok": True, "block": block, "payload": payload,
                    "status": str((payload or {}).get("status") or "ready"),
                    "files": (payload or {}).get("files") if isinstance((payload or {}).get("files"), list) else [],
                    "reason": None, "loader": exe, "session_id": session_id}
        err = (proc.stderr or "").strip().splitlines()
        detail = err[-1][:200] if err else f"exit {proc.returncode}"
        return {"ok": False, "block": None, "payload": None, "status": "gap",
                "files": [], "reason": f"loader failed: {detail}",
                "loader": exe, "session_id": session_id}
    block = _extract_block(out)
    payload = _payload_from_block(block) if block else None
    if payload is None:
        payload = _payload_from_hook_json(out)
        if payload is not None:
            # Re-extract the verbatim block from the hook JSON context.
            try:
                ctx = json.loads(out.strip())["hookSpecificOutput"]["additionalContext"]
                block = _extract_block(ctx)
            except (ValueError, KeyError, TypeError):
                block = None
    if not block or payload is None:
        return {"ok": False, "block": None, "payload": None, "status": "gap",
                "files": [], "reason": "loader failed: no direction block in loader output",
                "loader": exe, "session_id": session_id}
    status = str(payload.get("status") or "ready")
    files = payload.get("files") if isinstance(payload.get("files"), list) else []
    return {"ok": True, "block": block, "payload": payload, "status": status,
            "files": files, "reason": None, "loader": exe, "session_id": session_id}


def supply_for_harness(harness: str | None, direction_ok: bool,
                       kit_name: str | None = None,
                       kit_plugins: list | None = None) -> str:
    """``runner``, ``hook``, or ``none`` for a harness, loader outcome, and kit.

    Codex, Claude, and Grok are always hook hosts when the loader read.
    On the owned OpenCode server the route's kit decides: a kit naming the
    ``agentsmd-project-direction`` plugin is a hook host (supply ``hook``,
    hash recorded, no block prepended); a kit without it gets the runner
    injection (supply ``runner``). ``none`` means the loader failed or was
    absent. ``kit_plugins`` overrides the policy lookup for ``kit_name``
    when given (deterministic tests for a kit without the plugin).
    """
    if not direction_ok:
        return "none"
    if (harness or "") in HOOK_HOSTS:
        return "hook"
    if (harness or "") in RUNNER_HOSTS:
        if kit_plugins is not None:
            try:
                if DIRECTION_PLUGIN in list(kit_plugins or []):
                    return "hook"
            except Exception:
                pass
            return "runner"
        if kit_has_direction_plugin(kit_name):
            return "hook"
        return "runner"
    return "runner"


def kit_for_invocation(route: str | None, stage: str | None) -> tuple[str | None, str | None, list]:
    """(kit name, kit hash, skills) for an invocation. Never raises."""
    try:
        from . import policy as _policy
        kit_name: str | None = None
        if isinstance(route, str) and route in _policy.ROUTES:
            try:
                kit_name = _policy.kit_name_for_route(route)
            except ValueError:
                kit_name = None
        if kit_name is None and isinstance(stage, str):
            stage = stage.strip() or None
            if stage == "dispatch":
                kit_name = "dispatcher"
            elif stage == "implementation":
                kit_name = "worker"
            elif stage == "planning":
                kit_name = "planner"
            elif stage in ("correction",):
                kit_name = "correction"
            elif stage in ("recovery",):
                kit_name = "recovery"
            elif stage in ("review",):
                kit_name = "reviewer"
        if kit_name is None:
            return None, None, []
        kit = _policy.kit_for_role(kit_name)
        skills = list(kit.get("skills", [])) if isinstance(kit.get("skills"), list) else []
        return kit_name, _policy.kit_hash(kit), skills
    except Exception:
        return None, None, []


def fallback_text(reason: str | None = None) -> str:
    """Worker gap text naming the three files to read (never fabricated)."""
    why = f" ({reason})" if reason else ""
    names = ", ".join(REQUIRED_FILES[:-1]) + f", and {REQUIRED_FILES[-1]}"
    return (f"Project Direction is unavailable{why}: read {names} "
            "in the workspace in full before work; "
            "status shows the gap.")


def session_input(original: str, workspace: str | None, harness: str | None,
                  direction: dict | None, kit_name: str | None = None,
                  kit_plugins: list | None = None) -> tuple[str, dict]:
    """(prompt to send, supply record) for one role session.

    - ``runner`` (owned OpenCode server with a kit that does not name the
      direction plugin, loader ok): verbatim block plus the core
      instruction link and the project's own AGENTS.md when present, then
      the original prompt.
    - ``hook`` (Codex, Claude, Grok, or an owned-OpenCode kit naming the
      direction plugin, loader ok): the original prompt unchanged; the
      host hook supplies the block. The hash still comes from the runner's
      own loader read.
    - ``none`` (loader missing/failed): the fallback naming the three
      files plus the original prompt on the owned server; unchanged
      elsewhere. The job continues.
    """
    direction = direction or {"ok": False, "block": None, "status": "gap",
                              "reason": "loader missing"}
    ok = bool(direction.get("ok") and direction.get("block"))
    supply = supply_for_harness(harness, ok, kit_name, kit_plugins)
    base = original or ""
    if supply == "hook":
        return base, {"supply": supply,
                      "block_hash": block_hash(direction.get("block")),
                      "status": str(direction.get("status") or "ready"),
                      "reason": None}
    if supply == "runner":
        block = direction.get("block") or ""
        digest = block_hash(block)
        link = core_link_text(direction.get("payload"))
        ag_path, ag_content = project_agentsmd(workspace)
        parts = [f"PROJECT DIRECTION (status={direction.get('status')}, hash={digest}):",
                 block]
        if link:
            parts.append(f"Core instructions: {link}")
        if ag_path and ag_content:
            parts.append(f"Project AGENTS.md ({ag_path}):\n{ag_content.rstrip()}")
        parts.append("END PROJECT DIRECTION.")
        return "\n\n".join(parts) + "\n\n" + base, {
            "supply": supply, "block_hash": digest,
            "status": str(direction.get("status") or "ready"), "reason": None}
    reason = str(direction.get("reason") or "loader missing")
    status = str(direction.get("status") or "gap")
    if (harness or "") in RUNNER_HOSTS:
        return fallback_text(reason) + "\n\n" + base, {
            "supply": "none", "block_hash": None, "status": status,
            "reason": reason}
    return base, {"supply": "none", "block_hash": None, "status": status,
                  "reason": reason}
