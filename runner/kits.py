"""Role-kit materialization. Stdlib only.

Policy names the kits (``runner/policy.py``); this module builds the
per-launch configuration directories the harnesses spawn on. Every
directory is generated from the route's kit with nothing inherited from
the user's own configuration:

- OpenCode: ``OPENCODE_CONFIG_DIR`` + ``OPENCODE_CONFIG`` point at
  the generated directory, which holds ``kit.json``, ``AGENTS.md``,
  ``opencode.json`` (the kit's MCP subset), ``skills/`` and ``plugins/``
  equal to the kit, plus ``xdg-mirror/`` (one symlink per entry of the
  user's ``~/.config`` except ``opencode``). ``XDG_CONFIG_HOME`` points
  at ``xdg-mirror/`` so the owned server's shell keeps the user's
  environment (``gh``, global git config, and other XDG-aware tools)
  while OpenCode scans no user skills; ``OPENCODE_DISABLE_EXTERNAL_SKILLS=1``
  disables the ``~/.claude`` and ``~/.agents`` skill roots. ``OPENCODE_CONFIG_DIR``
  alone keeps the user's ``~/.config/opencode`` out.
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

import hashlib
import json
import os
import shutil
import sqlite3
import stat as _stat
from pathlib import Path

from . import policy

OPENCODE_KIT_ENV_VARS = ("OPENCODE_CONFIG_DIR", "OPENCODE_CONFIG",
                           "XDG_CONFIG_HOME", "OPENCODE_DISABLE_EXTERNAL_SKILLS",
                           "OPENCODE_DISABLE_PROJECT_CONFIG")
CODEX_KIT_ENV_VAR = "CODEX_HOME"
CLAUDE_KIT_ENV_VAR = "CLAUDE_CONFIG_DIR"
GROK_KIT_ENV_VAR = "GROK_HOME"

# Per-kit XDG mirror holding the user's environment without user skills.
XDG_MIRROR_DIRNAME = "xdg-mirror"
OPENCODE_DISABLE_EXTERNAL_SKILLS_ENV = "OPENCODE_DISABLE_EXTERNAL_SKILLS"
# OpenCode's own built-in skill, invocable on every owned-server session
# beside the kit's skills; recorded in skills_loaded as observed.
OPENCODE_BUILTIN_SKILLS = ("customize-opencode",)

# Config-directory logins shared by link, never copied. Codex keeps the
# user's login as ``auth.json`` directly under ``CODEX_HOME``; the Grok kit
# tries the same ``auth.json`` filename convention when the Grok home keeps
# its login in its config directory, and records missing otherwise.
# Only filenames travel in kit.json and the ledger; secret values never do.
CODEX_AUTH_FILES = ("auth.json",)
GROK_AUTH_FILES = ("auth.json",)


def kit_dir_for(state_dir, request_id: str, invocation_id: str, kit_name: str) -> Path:
    """Generated config directory for one invocation (under the state dir)."""
    safe_req = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(request_id))[:64] or "job"
    safe_inv = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(invocation_id))[:32] or "inv"
    return Path(state_dir) / "kits" / f"{safe_req}.{safe_inv}.{kit_name}"


def _user_config_dir() -> Path:
    """User config directory the XDG mirror reflects.

    ``$XDG_CONFIG_HOME`` when the runner itself runs under one, else
    ``~/.config``. Read from the process environment at materialization
    time, so each turn mirrors the current user environment.
    """
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if isinstance(xdg, str) and xdg.strip():
        return Path(os.path.expanduser(xdg.strip()))
    return Path.home() / ".config"


def materialize_xdg_mirror(dest) -> Path:
    """Build ``xdg-mirror/`` in a kit dir; rebuilt per invocation.

    One symlink per entry of the user's config directory except
    ``opencode``. Entries added to ``~/.config`` later appear on the next
    turn; removed entries disappear. Never raises for a missing source:
    the mirror is then empty (the shell keeps working, only with fewer
    XDG entries).
    """
    dest = Path(dest)
    mirror = dest / XDG_MIRROR_DIRNAME
    mirror.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(mirror, 0o700)
    except OSError:
        pass
    source = _user_config_dir()
    try:
        src_real = os.path.realpath(str(source))
        mir_real = os.path.realpath(str(mirror))
        if src_real == mir_real or src_real.startswith(mir_real + os.sep):
            return mirror
    except Exception:
        pass
    wanted: dict[str, Path] = {}
    try:
        if source.is_dir():
            for entry in source.iterdir():
                try:
                    name = entry.name
                except Exception:
                    continue
                if not isinstance(name, str) or not name or name in (".", ".."):
                    continue
                if "/" in name:
                    continue
                if name == "opencode":
                    continue
                wanted[name] = entry
    except OSError:
        wanted = {}
    try:
        for child in list(mirror.iterdir()):
            try:
                if child.name not in wanted:
                    if child.is_symlink() or child.is_file():
                        child.unlink()
                    elif child.is_dir():
                        import shutil
                        shutil.rmtree(child)
                    else:
                        try:
                            child.unlink()
                        except OSError:
                            pass
            except OSError:
                continue
    except OSError:
        pass
    for name, src in wanted.items():
        target = mirror / name
        try:
            if target.is_symlink():
                try:
                    if os.path.realpath(str(target)) == os.path.realpath(str(src)):
                        continue
                except OSError:
                    pass
                target.unlink()
            elif target.exists():
                try:
                    if target.is_dir():
                        import shutil
                        shutil.rmtree(str(target))
                    else:
                        target.unlink()
                except OSError:
                    continue
            os.symlink(str(src), str(target))
        except OSError:
            continue
    return mirror


def _skill_names_in_dir(skills_dir: Path) -> set[str]:
    """Skill names from ``skills/*/SKILL.md`` plus file-shaped skills."""
    names: set[str] = set()
    try:
        if not skills_dir.is_dir():
            return names
        for child in skills_dir.iterdir():
            try:
                if child.is_dir() and not child.is_symlink():
                    try:
                        if (child / "SKILL.md").is_file():
                            names.add(child.name)
                    except OSError:
                        continue
                elif child.is_symlink():
                    try:
                        real = Path(os.path.realpath(str(child)))
                        if real.is_dir():
                            if (real / "SKILL.md").is_file() or (child / "SKILL.md").is_file():
                                names.add(child.name)
                        elif real.is_file():
                            if child.name.endswith(".md"):
                                stem = child.name[:-3]
                                if stem and stem != "SKILL":
                                    names.add(stem)
                            else:
                                names.add(child.name)
                        else:
                            if (child / "SKILL.md").is_file():
                                names.add(child.name)
                    except OSError:
                        continue
                elif child.is_file():
                    if child.name.endswith(".md"):
                        stem = child.name[:-3]
                        if stem and stem != "SKILL":
                            names.add(stem)
                    else:
                        names.add(child.name)
            except OSError:
                continue
    except OSError:
        pass
    return names


def observed_opencode_skills(kit_dir) -> list[str]:
    """Observed invocable skills for an owned-server kit dir.

    The kit directory's ``skills/*/SKILL.md`` names plus OpenCode's
    built-in ``customize-opencode``. Sorted; never raises.
    """
    try:
        names = _skill_names_in_dir(Path(kit_dir) / "skills")
    except Exception:
        names = set()
    for builtin in OPENCODE_BUILTIN_SKILLS:
        names.add(builtin)
    return sorted(names)


def observed_skills_for_kit_dir(kit_dir) -> list[str] | None:
    """Observed skills from a materialized kit dir, or None when absent.

    OpenCode kits (holding ``opencode.json``) include the built-in;
    other kits list only their ``skills/`` directories. None when the
    kit dir holds no skills directory at all.
    """
    try:
        kit_p = Path(kit_dir)
    except Exception:
        return None
    try:
        skills_dir = kit_p / "skills"
        if not skills_dir.is_dir():
            return None
    except OSError:
        return None
    try:
        if (kit_p / "opencode.json").is_file():
            return observed_opencode_skills(kit_p)
        return sorted(_skill_names_in_dir(skills_dir))
    except Exception:
        return None


def observed_skills_for_invocation(state_dir, request_id: str,
                                   invocation_id: str) -> list[str] | None:
    """Observed skills from an invocation's materialized kit dir, if any."""
    try:
        dirs = kit_dirs_for_invocation(state_dir, request_id, invocation_id or "")
    except Exception:
        return None
    for cand in dirs or []:
        try:
            kit_file = Path(cand) / "kit.json"
            if kit_file.is_file():
                try:
                    obj = json.loads(kit_file.read_text(encoding="utf-8"))
                except ValueError:
                    obj = None
                if isinstance(obj, dict) and isinstance(obj.get("skills_loaded"), list) \
                        and all(isinstance(v, str) for v in obj["skills_loaded"]):
                    return sorted(set(obj["skills_loaded"]))
        except OSError:
            pass
        try:
            observed = observed_skills_for_kit_dir(cand)
        except Exception:
            observed = None
        if observed is not None:
            return observed
    return None


def _link_login_files(home, filenames, dest: Path) -> dict:
    """Share a config-directory login by symlink, never by copy.

    Each ``filename`` present as a regular file under ``home`` is linked
    into ``dest`` (an existing link to the same file is kept; a stale link
    or copy is replaced). Missing files are recorded as missing so the
    ledger shows the gap without secrets. Returns
    ``{"linked": [...], "missing": [...]}`` with filenames only; no secret
    values ever leave the user's home.
    """
    linked: list[str] = []
    missing: list[str] = []
    try:
        home_p = Path(os.path.expanduser(str(home)))
    except Exception:
        return {"linked": linked, "missing": list(filenames)}
    for name in filenames:
        if not isinstance(name, str) or not name or "/" in name or name in (".", ".."):
            continue
        try:
            src = home_p / name
        except Exception:
            missing.append(name)
            continue
        try:
            is_file = src.is_file() and not src.is_dir()
        except OSError:
            is_file = False
        if not is_file:
            missing.append(name)
            try:
                stale = dest / name
                if stale.is_symlink():
                    stale.unlink()
            except OSError:
                pass
            continue
        target = dest / name
        try:
            if target.is_symlink():
                try:
                    if os.path.realpath(target) == os.path.realpath(src):
                        linked.append(name)
                        continue
                except OSError:
                    pass
                target.unlink()
            elif target.exists():
                try:
                    if target.is_dir():
                        import shutil
                        shutil.rmtree(target)
                    else:
                        target.unlink()
                except OSError:
                    pass
            os.symlink(str(src), str(target))
            linked.append(name)
        except OSError:
            missing.append(name)
    return {"linked": linked, "missing": missing}


def _codex_auth_link(dest: Path) -> dict:
    """Link the user's Codex login into a kit dir, honoring overrides."""
    try:
        home = policy._codex_home()
    except Exception:
        return {"linked": [], "missing": list(CODEX_AUTH_FILES)}
    return _link_login_files(home, CODEX_AUTH_FILES, dest)


def _grok_auth_link(dest: Path) -> dict:
    """Link the user's Grok login into a kit dir when it uses one."""
    try:
        home = policy._grok_home()
    except Exception:
        return {"linked": [], "missing": list(GROK_AUTH_FILES)}
    return _link_login_files(home, GROK_AUTH_FILES, dest)


def _safe_request(request_id: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_"
                   for c in str(request_id))[:64] or "job"


def codex_sessions_dir_for(state_dir, request_id: str) -> Path:
    """One Codex sessions directory per job, shared by every Codex kit of the
    job, so a resume finds the rollout its dispatch wrote. Each invocation
    still gets its own kit directory; only ``sessions/`` is shared. The name
    carries a hash of the full request id, so two ids that sanitize alike
    (``a/b`` and ``a_b``) never share a directory."""
    digest = hashlib.sha256(str(request_id).encode("utf-8")).hexdigest()[:16]
    return Path(state_dir) / "codex-sessions" / f"{_safe_request(request_id)[:40]}-{digest}"


def _job_invocation_ids(state_dir, request_id: str) -> list[str]:
    """Invocation ids the ledger records for exactly this request id."""
    db = Path(state_dir) / "jobs.db"
    if not db.is_file():
        return []
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5.0)
        try:
            return [row[0] for row in con.execute(
                "SELECT invocation_id FROM invocations WHERE request_id=?",
                (str(request_id),))]
        finally:
            con.close()
    except sqlite3.Error:
        return []


def link_codex_sessions(kit_dir, shared) -> Path:
    """Point ``<kit_dir>/sessions`` at the job's shared sessions directory.

    A kit that already holds real rollouts (materialized before sessions
    were shared) keeps them: they are copied into the shared directory
    before the link replaces the directory.
    """
    kit_dir = Path(kit_dir)
    shared = Path(shared)
    shared.mkdir(mode=0o700, parents=True, exist_ok=True)
    link = kit_dir / "sessions"
    if link.is_symlink():
        if link.resolve() == shared.resolve():
            return link
        link.unlink()
    elif link.is_dir():
        shutil.copytree(link, shared, dirs_exist_ok=True)
        shutil.rmtree(link)
    link.symlink_to(shared, target_is_directory=True)
    return link


def adopt_codex_thread(state_dir, request_id: str, thread_id: str, shared) -> Path | None:
    """Make a thread's rollout available in the shared sessions directory.

    A job dispatched before sessions were shared wrote its rollout into the
    dispatch invocation's own kit; copy it so the resume finds it. The
    earlier kit keeps its copy as evidence. Returns the rollout path, or
    None when no kit of the job holds the thread.
    """
    if not thread_id:
        return None
    shared = Path(shared)
    pattern = f"rollout-*-{thread_id}.jsonl"
    for path in shared.rglob(pattern):
        return path
    # Only kits of this job's own invocations: a kit directory name is
    # derived from the sanitized request id, which other ids can share.
    kits = []
    for invocation_id in _job_invocation_ids(state_dir, request_id):
        kits.extend(kit_dirs_for_invocation(state_dir, request_id, invocation_id))
    for kit in sorted(set(kits)):
        sessions = kit / "sessions"
        if sessions.is_symlink() or not sessions.is_dir():
            continue
        for path in sorted(sessions.rglob(pattern)):
            dest = shared / path.relative_to(sessions)
            dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copy2(path, dest)
            return dest
    return None


def kit_dirs_for_invocation(state_dir, request_id: str, invocation_id: str) -> list[Path]:
    """Kit directories materialized for one invocation, if any.

    Only a genuinely absent kits root (or a kits root that is not a
    directory) returns an empty list. Every other filesystem failure
    (permission, I/O, ...) propagates so the observed-model/controller
    path blocks safely instead of treating the observation as unknown.
    ``Path.is_dir`` suppresses filesystem errors, so this uses
    ``stat``/``iterdir`` seams that preserve unexpected errors.
    """
    def _safe(value: str, limit: int) -> str:
        return "".join(c if c.isalnum() or c in ("-", "_") else "_"
                       for c in str(value))[:limit] or "job"
    root = Path(state_dir) / "kits"
    prefix = f"{_safe(request_id, 64)}.{_safe(invocation_id, 32)}."
    try:
        root_st = root.stat()
    except FileNotFoundError:
        return []
    except NotADirectoryError:
        return []
    if not _stat.S_ISDIR(root_st.st_mode):
        return []
    try:
        entries = list(root.iterdir())
    except FileNotFoundError:
        return []
    except NotADirectoryError:
        return []
    out = []
    for cand in entries:
        if not cand.name.startswith(prefix):
            continue
        try:
            cand_st = cand.stat()
        except FileNotFoundError:
            continue
        except NotADirectoryError:
            continue
        if not _stat.S_ISDIR(cand_st.st_mode):
            continue
        out.append(cand)
    return sorted(out)


def kit_contents_for_ledger(kit_dir) -> dict | None:
    """Kit contents from a materialized kit.json without secrets.

    Returns ``{"kit", "hash", "skills", "plugins", "mcp", "auth"}`` with
    filenames and status only, plus ``skills_loaded`` when the kit
    recorded its observed skills at materialization; secret file contents
    are never read. None when the kit.json is missing or unparsable.
    """
    try:
        raw = (Path(kit_dir) / "kit.json").read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        obj = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    auth = obj.get("auth")
    if not isinstance(auth, dict):
        auth = {"linked": [], "missing": []}
    def _names(value) -> list:
        return [v for v in (value or []) if isinstance(v, str)][:32]
    out = {
        "kit": obj.get("kit") if isinstance(obj.get("kit"), str) else None,
        "hash": obj.get("hash") if isinstance(obj.get("hash"), str) else None,
        "skills": _names(obj.get("skills")),
        "plugins": _names(obj.get("plugins")),
        "mcp": _names(obj.get("mcp")),
        "auth": {"linked": _names(auth.get("linked")),
                 "missing": _names(auth.get("missing"))},
    }
    if isinstance(obj.get("skills_loaded"), list) \
            and all(isinstance(v, str) for v in obj["skills_loaded"]):
        out["skills_loaded"] = _names(obj.get("skills_loaded"))
    return out


def _find_skill_source(name: str) -> Path | None:
    return policy.skill_source(name)


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
        os.chmod(dest, 0o700 if src.is_dir() else 0o600)
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
    try:
        sources = policy.kit_skill_sources(kit)
    except (OSError, ValueError) as exc:
        raise ValueError(f"kit {kit_name}: {exc}") from exc
    if "project-direction" in kit.get("skills", []) and "project-direction" not in sources:
        # A reused kit may still hold the former separately invocable skill.
        for name in ("project-direction", "project-direction.md"):
            stale = dest / "skills" / name
            if stale.is_symlink() or stale.is_file():
                stale.unlink()
            elif stale.is_dir():
                import shutil
                shutil.rmtree(stale)
    for src in sources.values():
        _link_or_copy(src, dest / "skills" / src.name)
    for plugin in kit.get("plugins", []):
        src = _find_plugin_source(plugin)
        if src is None:
            raise ValueError(f"kit {kit_name}: plugin {plugin!r} is not installed")
        _link_or_copy(src, dest / "plugins" / src.name)


def materialize_opencode_kit(kit_name: str, dest, route: str | None = None) -> Path:
    """Build the owned-server config directory for a kit. Returns the dir.

    Besides the kit files, builds ``xdg-mirror/`` (one symlink per entry
    of the user's config directory except ``opencode``, rebuilt on every
    call) and records the observed ``skills_loaded`` (the kit dir's
    ``skills/*/SKILL.md`` names plus OpenCode's built-in) in ``kit.json``.
    """
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
        "skills": list(policy.kit_skill_sources(kit)),
    })
    _materialize_skills_and_plugins(kit_name, kit, dest)
    _write_agentsmd_link(dest / "AGENTS.md")
    materialize_xdg_mirror(dest)
    try:
        loaded = observed_opencode_skills(dest)
    except Exception:
        loaded = sorted(set(list(kit.get("skills", [])) + list(OPENCODE_BUILTIN_SKILLS)))
    try:
        raw = json.loads((dest / "kit.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raw = {}
    if isinstance(raw, dict):
        raw["skills_loaded"] = list(loaded)
        raw["xdg_mirror"] = XDG_MIRROR_DIRNAME
        _secure_write_json(dest / "kit.json", raw)
    return dest


def materialize_codex_kit(kit_name: str, dest) -> Path:
    """Build an isolated CODEX_HOME for a kit. Returns the dir.

    ``skills/`` and ``plugins/`` equal the kit; ``config.toml`` carries the
    kit's skill/plugin/MCP lists plus the installed ``[mcp_servers.NAME]``
    sections named by the kit (none when the kit names none); ``mcp.json``
    holds the installed opencode MCP subset named by the kit so a kit that
    names a paid MCP server keeps it as data on every harness. The user's
    ``auth.json`` login is shared by symlink from the user's Codex home
    (honoring ``MODEL_ROUTER_CODEX_HOME``/``CODEX_HOME`` overrides) so the
    dispatcher authenticates; ``kit.json`` records the linked filenames
    without secrets.
    """
    kit = policy.kit_for_role(kit_name)
    dest = Path(dest)
    dest.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(dest, 0o700)
    except OSError:
        pass
    auth = _codex_auth_link(dest)
    _secure_write_json(dest / "kit.json", {
        "kit": kit_name, "hash": policy.kit_hash(kit),
        "skills": list(kit.get("skills", [])),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
        "permission_set": kit.get("permission_set"),
        "agentsmd": bool(kit.get("agentsmd")),
        "auth": {"linked": list(auth.get("linked", [])),
                 "missing": list(auth.get("missing", []))},
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
        "skills": list(policy.kit_skill_sources(kit)),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
    })
    _write_agentsmd_link(dest / "CLAUDE.md")
    return dest


def materialize_grok_kit(kit_name: str, dest) -> Path:
    """Build an isolated GROK_HOME for a kit. Returns the dir.

    ``skills/`` and ``plugins/`` equal the kit; ``config.toml`` lists the
    kit's skills, plugins, and MCP servers; ``mcp.json`` holds the installed
    opencode MCP subset named by the kit (empty when none). When the Grok
    home keeps its login as ``auth.json`` in its config directory, that
    file is shared by symlink (honoring ``MODEL_ROUTER_GROK_HOME``/``GROK_HOME``
    overrides, recorded as missing otherwise);
    ``kit.json`` records the linked filenames without secrets.
    """
    kit = policy.kit_for_role(kit_name)
    dest = Path(dest)
    dest.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(dest, 0o700)
    except OSError:
        pass
    auth = _grok_auth_link(dest)
    _secure_write_json(dest / "kit.json", {
        "kit": kit_name, "hash": policy.kit_hash(kit),
        "skills": list(kit.get("skills", [])),
        "plugins": list(kit.get("plugins", [])),
        "mcp": list(kit.get("mcp", [])),
        "permission_set": kit.get("permission_set"),
        "agentsmd": bool(kit.get("agentsmd")),
        "auth": {"linked": list(auth.get("linked", [])),
                 "missing": list(auth.get("missing", []))},
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
    """Environment overrides launching the owned server on a kit dir.

    ``OPENCODE_CONFIG_DIR`` and ``OPENCODE_CONFIG`` point at the kit;
    ``XDG_CONFIG_HOME`` points at the kit's ``xdg-mirror/`` so the shell
    keeps the user's environment (``gh``, git, XDG-aware tools) while
    OpenCode scans no user skills; ``OPENCODE_DISABLE_EXTERNAL_SKILLS=1``
    disables the ``~/.claude`` and ``~/.agents`` skill roots. Project-local
    config and Skills are disabled too; otherwise OpenCode merges the target
    workspace's MCP and Skills into this kit despite the isolated config dir.
    """
    kit_dir = str(kit_dir)
    return {
        "OPENCODE_CONFIG_DIR": kit_dir,
        "OPENCODE_CONFIG": str(Path(kit_dir) / "opencode.json"),
        "XDG_CONFIG_HOME": str(Path(kit_dir) / XDG_MIRROR_DIRNAME),
        OPENCODE_DISABLE_EXTERNAL_SKILLS_ENV: "1",
        "OPENCODE_DISABLE_PROJECT_CONFIG": "1",
    }


def codex_kit_env(kit_dir: Path) -> dict:
    return {CODEX_KIT_ENV_VAR: str(kit_dir)}


def claude_kit_env(kit_dir: Path) -> dict:
    return {CLAUDE_KIT_ENV_VAR: str(kit_dir)}


def grok_kit_env(kit_dir: Path) -> dict:
    return {GROK_KIT_ENV_VAR: str(kit_dir)}
