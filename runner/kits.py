"""Role-kit materialization. Stdlib only.

Policy names the kits (``runner/policy.py``); this module builds the
per-launch configuration directories the harnesses spawn on. Every
directory is generated from the route's kit with nothing inherited from
the user's own configuration:

- OpenCode: ``OPENCODE_CONFIG_DIR`` + ``XDG_CONFIG_HOME`` (shadow) +
  ``OPENCODE_CONFIG`` point at the generated directory, which holds
  ``kit.json``, ``AGENTS.md``, ``opencode.json`` (the kit's MCP subset),
  ``skills/`` and ``plugins/`` equal to the kit.
- Codex: ``CODEX_HOME`` points at the generated directory, which holds
  ``kit.json``, ``AGENTS.md``, ``config.toml`` (kit lists plus the installed
  ``[mcp_servers.NAME]`` sections named by the kit), ``mcp.json`` (the kit's
  MCP subset), and ``skills/`` plus ``plugins/`` equal to the kit.
- Claude: ``CLAUDE_CONFIG_DIR`` points at the generated directory, which
  holds ``kit.json``, ``CLAUDE.md``, ``settings.json`` (kit skills, plugins,
  MCP list, permission set), ``mcp.json`` (the kit's MCP subset), and
  ``skills/`` plus ``plugins/`` equal to the kit.
- Grok: ``GROK_HOME`` points at the generated directory, which holds
  ``kit.json``, ``AGENTS.md``, ``config.toml`` (kit lists), ``mcp.json``
  (the kit's MCP subset), and ``skills/`` plus ``plugins/`` equal to the kit.

The planner keeps the user's own session: the Claude harness never
isolates ``claude_callback``.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from . import policy

OPENCODE_KIT_ENV_VARS = ("OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME", "OPENCODE_CONFIG")
CODEX_KIT_ENV_VAR = "CODEX_HOME"
CLAUDE_KIT_ENV_VAR = "CLAUDE_CONFIG_DIR"
GROK_KIT_ENV_VAR = "GROK_HOME"


def kit_dir_for(state_dir, request_id: str, invocation_id: str, kit_name: str) -> Path:
    """Generated config directory for one invocation (under the state dir)."""
    safe_req = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(request_id))[:64] or "job"
    safe_inv = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(invocation_id))[:32] or "inv"
    return Path(state_dir) / "kits" / f"{safe_req}.{safe_inv}.{kit_name}"


def _find_skill_source(name: str) -> Path | None:
    for root in policy._skill_search_roots():
        for cand in (root / name, root / f"{name}.md"):
            try:
                if cand.is_dir() or cand.is_file():
                    return cand
            except OSError:
                continue
    return None


def _find_plugin_source(name: str) -> Path | None:
    for root in (policy._opencode_home() / "plugins", policy._codex_home() / "plugins",
                 policy._claude_home() / "plugins", policy._grok_home() / "plugins"):
        for cand in (root / name, root / f"{name}.js"):
            try:
                if cand.is_dir() or cand.is_file():
                    return cand
            except OSError:
                continue
    return None


def _installed_opencode_config() -> dict:
    for candidate in (policy._opencode_home() / "opencode.json",
                      policy._opencode_home() / "opencode.jsonc"):
        try:
            raw = candidate.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        if isinstance(obj, dict):
            return obj
    return {}


def _link_or_copy(src: Path, dest: Path) -> None:
    dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        if dest.is_symlink() or dest.exists():
            try:
                if dest.is_symlink() or dest.is_file():
                    dest.unlink()
                elif dest.is_dir():
                    import shutil
                    shutil.rmtree(dest)
            except OSError:
                pass
        os.symlink(str(src), str(dest))
        return
    except OSError:
        pass
    if src.is_dir():
        import shutil
        shutil.copytree(str(src), str(dest), symlinks=False)
    else:
        data = src.read_bytes()
        dest.write_bytes(data)
    try:
        os.chmod(dest, 0o600)
    except OSError:
        pass


def _write_agentsmd_link(dest: Path) -> None:
    installed = policy._opencode_home() / "AGENTS.md"
    target = None
    try:
        if installed.is_symlink():
            raw = os.readlink(installed)
            # A relative readlink target resolves against the installed
            # link's parent, not against the kit dir: join it first so the
            # generated kit link points at the same file instead of dangling.
            if not os.path.isabs(raw):
                raw = str(installed.parent / raw)
            target = raw
    except OSError:
        target = None
    if target:
        try:
            if dest.is_symlink() or dest.exists():
                dest.unlink()
        except OSError:
            pass
        try:
            os.symlink(target, str(dest))
            return
        except OSError:
            pass
    try:
        if installed.is_file():
            _link_or_copy(installed, dest)
            return
    except OSError:
        pass
    dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    dest.write_text("# AGENTS.md\n# Generated kit link: canonical AgentsMD was not installed.\n",
                    encoding="utf-8")
    try:
        os.chmod(dest, 0o600)
    except OSError:
        pass


def _secure_write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _opencode_mcp_subset(kit_mcp: list) -> dict:
    """Installed opencode.json MCP entries named by the kit (possibly empty)."""
    installed = _installed_opencode_config()
    installed_mcp = installed.get("mcp") if isinstance(installed.get("mcp"), dict) else {}
    return {name: installed_mcp[name] for name in kit_mcp if name in installed_mcp}


def _installed_codex_mcp_blocks() -> dict:
    """Installed Codex ``[mcp_servers.NAME]`` sections, by server name."""
    try:
        raw = (policy._codex_home() / "config.toml").read_text(encoding="utf-8")
    except OSError:
        return {}
    blocks: dict[str, list[str]] = {}
    current: str | None = None
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("[mcp_servers."):
            rest = stripped[len("[mcp_servers."):]
            name = rest.split("]")[0].strip().strip('"').strip("'")
            current = name or None
            if current and current not in blocks:
                blocks[current] = [line]
            elif current:
                blocks[current].append(line)
        elif stripped.startswith("[") and stripped.endswith("]"):
            current = None
        elif current is not None:
            blocks[current].append(line)
    return {k: "\n".join(v) + "\n" for k, v in blocks.items()}


def _materialize_skills_and_plugins(kit_name: str, kit: dict, dest: Path) -> None:
    """Link the kit's skills and plugins into the kit dir; raise when missing."""
    for skill in kit.get("skills", []):
        src = _find_skill_source(skill)
        if src is None:
            raise ValueError(f"kit {kit_name}: skill {skill!r} is not installed")
        _link_or_copy(src, dest / "skills" / src.name)
    for plugin in kit.get("plugins", []):
        src = _find_plugin_source(plugin)
        if src is None:
            raise ValueError(f"kit {kit_name}: plugin {plugin!r} is not installed")
        _link_or_copy(src, dest / "plugins" / src.name)


def materialize_opencode_kit(kit_name: str, dest, route: str | None = None) -> Path:
    """Build the owned-server config directory for a kit. Returns the dir."""
    kit = policy.kit_for_role(kit_name)
    dest = Path(dest)
    dest.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(dest, 0o700)
    except OSError:
        pass
    permissions = [dict(r) for r in policy.session_permissions(route)] if route else []
    _secure_write_json(dest / "kit.json", {
        "kit": kit_name, "hash": policy.kit_hash(kit), "route": route,
        "instructions": kit.get("instructions"),
        "skills": list(kit.get("skills", [])),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
        "permission_set": kit.get("permission_set"),
        "permissions": permissions,
        "agentsmd": bool(kit.get("agentsmd")),
    })
    installed = _installed_opencode_config()
    installed_mcp = installed.get("mcp") if isinstance(installed.get("mcp"), dict) else {}
    subset = {name: installed_mcp[name] for name in kit.get("mcp", []) if name in installed_mcp}
    _secure_write_json(dest / "opencode.json", {
        "$schema": "https://opencode.ai/config.json",
        "mcp": subset,
        "plugin": list(kit.get("plugins", [])),
        "skills": list(kit.get("skills", [])),
    })
    for skill in kit.get("skills", []):
        src = _find_skill_source(skill)
        if src is None:
            raise ValueError(f"kit {kit_name}: skill {skill!r} is not installed")
        _link_or_copy(src, dest / "skills" / src.name)
    for plugin in kit.get("plugins", []):
        src = _find_plugin_source(plugin)
        if src is None:
            raise ValueError(f"kit {kit_name}: plugin {plugin!r} is not installed")
        _link_or_copy(src, dest / "plugins" / src.name)
    _write_agentsmd_link(dest / "AGENTS.md")
    # XDG shadow: an empty home so the real ~/.config/opencode is never read.
    xdg = dest / "xdg-shadow"
    xdg.mkdir(mode=0o700, parents=True, exist_ok=True)
    return dest


def materialize_codex_kit(kit_name: str, dest) -> Path:
    """Build an isolated CODEX_HOME for a kit. Returns the dir.

    ``skills/`` and ``plugins/`` equal the kit; ``config.toml`` carries the
    kit's skill/plugin/MCP lists plus the installed ``[mcp_servers.NAME]``
    sections named by the kit (none when the kit names none); ``mcp.json``
    holds the installed opencode MCP subset named by the kit so a kit that
    names a paid MCP server keeps it as data on every harness.
    """
    kit = policy.kit_for_role(kit_name)
    dest = Path(dest)
    dest.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(dest, 0o700)
    except OSError:
        pass
    _secure_write_json(dest / "kit.json", {
        "kit": kit_name, "hash": policy.kit_hash(kit),
        "skills": list(kit.get("skills", [])),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
        "permission_set": kit.get("permission_set"),
        "agentsmd": bool(kit.get("agentsmd")),
    })
    _materialize_skills_and_plugins(kit_name, kit, dest)
    _secure_write_json(dest / "mcp.json", {"mcp": _opencode_mcp_subset(kit.get("mcp", []))})
    installed_blocks = _installed_codex_mcp_blocks()
    wanted = list(kit.get("mcp", []))
    kept = [n for n in wanted if n in installed_blocks]
    lines = [
        f"# Generated from policy kit {kit_name} ({policy.kit_hash(kit)}); do not edit.",
        f"# kit skills: {', '.join(kit.get('skills', [])) or 'none'}",
        f"# kit plugins: {', '.join(kit.get('plugins', [])) or 'none'}",
        f"# kit mcp: {', '.join(wanted) or 'none'}",
        "",
    ]
    for name in kept:
        lines.append(installed_blocks[name].rstrip("\n"))
        lines.append("")
    (dest / "config.toml").write_text("\n".join(lines), encoding="utf-8")
    try:
        os.chmod(dest / "config.toml", 0o600)
    except OSError:
        pass
    _write_agentsmd_link(dest / "AGENTS.md")
    return dest


def materialize_claude_kit(kit_name: str, dest) -> Path:
    """Build an isolated CLAUDE_CONFIG_DIR for a kit. Returns the dir.

    ``skills/`` and ``plugins/`` equal the kit; ``settings.json`` records the
    kit's skills, plugins, MCP list, and permission set; ``mcp.json`` holds
    the installed opencode MCP subset named by the kit (empty when none).
    """
    kit = policy.kit_for_role(kit_name)
    dest = Path(dest)
    dest.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(dest, 0o700)
    except OSError:
        pass
    _secure_write_json(dest / "kit.json", {
        "kit": kit_name, "hash": policy.kit_hash(kit),
        "skills": list(kit.get("skills", [])),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
        "permission_set": kit.get("permission_set"),
        "agentsmd": bool(kit.get("agentsmd")),
    })
    _materialize_skills_and_plugins(kit_name, kit, dest)
    _secure_write_json(dest / "mcp.json", {"mcp": _opencode_mcp_subset(kit.get("mcp", []))})
    _secure_write_json(dest / "settings.json", {
        "kit": kit_name, "kitHash": policy.kit_hash(kit),
        "permissions": kit.get("permission_set"),
        "skills": list(kit.get("skills", [])),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
    })
    _write_agentsmd_link(dest / "CLAUDE.md")
    return dest


def materialize_grok_kit(kit_name: str, dest) -> Path:
    """Build an isolated GROK_HOME for a kit. Returns the dir.

    ``skills/`` and ``plugins/`` equal the kit; ``config.toml`` lists the
    kit's skills, plugins, and MCP servers; ``mcp.json`` holds the installed
    opencode MCP subset named by the kit (empty when none).
    """
    kit = policy.kit_for_role(kit_name)
    dest = Path(dest)
    dest.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(dest, 0o700)
    except OSError:
        pass
    _secure_write_json(dest / "kit.json", {
        "kit": kit_name, "hash": policy.kit_hash(kit),
        "skills": list(kit.get("skills", [])),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
        "permission_set": kit.get("permission_set"),
        "agentsmd": bool(kit.get("agentsmd")),
    })
    _materialize_skills_and_plugins(kit_name, kit, dest)
    _secure_write_json(dest / "mcp.json", {"mcp": _opencode_mcp_subset(kit.get("mcp", []))})
    (dest / "config.toml").write_text(
        f"# Generated from policy kit {kit_name} ({policy.kit_hash(kit)}); do not edit.\n"
        f"# kit skills: {', '.join(kit.get('skills', [])) or 'none'}\n"
        f"# kit plugins: {', '.join(kit.get('plugins', [])) or 'none'}\n"
        f"# kit mcp: {', '.join(kit.get('mcp', [])) or 'none'}\n",
        encoding="utf-8")
    try:
        os.chmod(dest / "config.toml", 0o600)
    except OSError:
        pass
    _write_agentsmd_link(dest / "AGENTS.md")
    return dest


def opencode_kit_env(kit_dir: Path) -> dict:
    """Environment overrides launching the owned server on a kit dir."""
    kit_dir = str(kit_dir)
    return {
        "OPENCODE_CONFIG_DIR": kit_dir,
        "XDG_CONFIG_HOME": str(Path(kit_dir) / "xdg-shadow"),
        "OPENCODE_CONFIG": str(Path(kit_dir) / "opencode.json"),
    }


def codex_kit_env(kit_dir: Path) -> dict:
    return {CODEX_KIT_ENV_VAR: str(kit_dir)}


def claude_kit_env(kit_dir: Path) -> dict:
    return {CLAUDE_KIT_ENV_VAR: str(kit_dir)}


def grok_kit_env(kit_dir: Path) -> dict:
    return {GROK_KIT_ENV_VAR: str(kit_dir)}
