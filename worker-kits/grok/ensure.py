#!/usr/bin/env python3
"""Ensure the Grok worker host carries AgentsMD (link + hook). Stdlib only.

The Grok Build worker turns run on the operator's Grok host, whose
configuration directory must deliver Project Direction: an ``AGENTS.md``
link to the canonical AgentsMD checkout and the ``project-direction``
hook. This script ensures both, idempotently, without ever overwriting
user-owned files unless ``--replace`` is given:

    python3 worker-kits/grok/ensure.py --source /path/to/agentsmd [--home ~/.grok]
    python3 worker-kits/grok/ensure.py --source /path/to/agentsmd --check

``--check`` only reports (exit 0 when current, 2 when anything is
missing or divergent). The hook document is byte-identical to the one
managed by AgentsMD's own ``bin/agentsmd-grok-hook``, so either installer
reports the other's file as owned-current.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
from pathlib import Path

HOOK_TIMEOUT_SECONDS = 15
HOOK_INSTALLER = "agentsmd-grok-hook"


def hook_document(source: str) -> dict:
    return {
        "agentsmd": {"installer": HOOK_INSTALLER, "source": source},
        "hooks": {
            "PreToolUse": [
                {
                    "hooks": [
                        {
                            "type": "command",
                            "command": f'"{source}/bin/project-direction" hook',
                            "timeout": HOOK_TIMEOUT_SECONDS,
                        }
                    ]
                }
            ]
        },
    }


def hook_bytes(source: str) -> bytes:
    return (json.dumps(hook_document(source), indent=2, sort_keys=True) + "\n").encode()


def _backup(target: Path) -> Path:
    backup_dir = target.parent / "agentsmd-backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    candidate = backup_dir / f"{target.name}.{stamp}.bak"
    counter = 1
    while os.path.lexists(candidate):
        candidate = backup_dir / f"{target.name}.{stamp}.{counter}.bak"
        counter += 1
    if target.is_symlink():
        candidate.symlink_to(os.readlink(target))
        target.unlink()
    else:
        target.rename(candidate)
    return candidate


def ensure_link(home: Path, source: Path, replace: bool) -> dict:
    """Ensure <home>/AGENTS.md links at the canonical AgentsMD file."""
    link = home / "AGENTS.md"
    want = source / "AGENTS.md"
    if os.path.lexists(link):
        if link.is_symlink():
            try:
                same = link.resolve() == want.resolve()
            except OSError:
                same = False
            if same:
                return {"path": str(link), "status": "owned-current", "target": str(want)}
            if not replace:
                return {"path": str(link), "status": "divergent",
                        "target": os.readlink(link),
                        "error": "link points elsewhere; re-run with --replace"}
            backup = _backup(link)
            link.symlink_to(want)
            return {"path": str(link), "status": "replaced", "target": str(want),
                    "backup": str(backup)}
        return {"path": str(link), "status": "user-owned",
                "error": "a regular file or directory owns this path; refusing to replace"}
    home.mkdir(parents=True, exist_ok=True)
    link.symlink_to(want)
    return {"path": str(link), "status": "created", "target": str(want)}


def ensure_hook(home: Path, source: str, replace: bool) -> dict:
    """Ensure <home>/hooks/agentsmd.json holds the Project Direction hook."""
    target = home / "hooks" / "agentsmd.json"
    expected = hook_bytes(source)
    if os.path.lexists(target):
        if target.is_symlink() or not target.is_file():
            return {"path": str(target), "status": "other-path",
                    "error": "not a regular file; refusing to replace"}
        raw = target.read_bytes()
        if raw == expected:
            return {"path": str(target), "status": "owned-current"}
        if not replace:
            return {"path": str(target), "status": "divergent",
                    "error": "hook file differs; re-run with --replace"}
        backup = _backup(target)
        target.write_bytes(expected)
        return {"path": str(target), "status": "replaced", "backup": str(backup)}
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(expected)
    return {"path": str(target), "status": "created"}


def ensure(source: Path, home: Path, replace: bool, check: bool) -> tuple[dict, int]:
    if not source.is_dir() or not (source / "AGENTS.md").is_file():
        return {"error": f"source has no AGENTS.md: {source}"}, 1
    if not (source / "bin" / "project-direction").exists():
        return {"error": f"source has no bin/project-direction: {source}"}, 1
    if check:
        # --check never writes: classify only.
        link_path = home / "AGENTS.md"
        want = source / "AGENTS.md"
        if os.path.lexists(link_path) and link_path.is_symlink():
            try:
                current = link_path.resolve() == want.resolve()
            except OSError:
                current = False
            link = {"path": str(link_path),
                    "status": "owned-current" if current else "divergent",
                    "target": os.readlink(link_path)}
        elif os.path.lexists(link_path):
            link = {"path": str(link_path), "status": "user-owned"}
        else:
            link = {"path": str(link_path), "status": "missing"}
        hook_path = home / "hooks" / "agentsmd.json"
        if hook_path.is_file() and not hook_path.is_symlink() \
                and hook_path.read_bytes() == hook_bytes(str(source)):
            hook = {"path": str(hook_path), "status": "owned-current"}
        elif os.path.lexists(hook_path):
            hook = {"path": str(hook_path), "status": "divergent"}
        else:
            hook = {"path": str(hook_path), "status": "missing"}
        report = {"source": str(source), "home": str(home),
                  "link": link, "hook": hook}
        current_all = link["status"] == "owned-current" and hook["status"] == "owned-current"
        return report, 0 if current_all else 2
    link = ensure_link(home, source, replace)
    hook = ensure_hook(home, str(source), replace)
    report = {"source": str(source), "home": str(home), "link": link, "hook": hook}
    bad = [p for p in (link, hook) if p.get("status") in ("user-owned", "other-path")
           or "error" in p]
    return report, 1 if bad else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ensure.py",
                                 description="Ensure the Grok host carries the AgentsMD link and hook.")
    ap.add_argument("--source", required=True, help="canonical AgentsMD checkout directory")
    ap.add_argument("--home", default="~/.grok", help="Grok configuration directory")
    ap.add_argument("--check", action="store_true", help="report only; never write")
    ap.add_argument("--replace", action="store_true",
                    help="re-point a divergent link and back up then replace a divergent hook file")
    args = ap.parse_args(argv)
    source = Path(os.path.expanduser(args.source))
    try:
        source = source.resolve()
    except OSError:
        pass
    report, rc = ensure(source, Path(os.path.expanduser(args.home)), args.replace, args.check)
    print(json.dumps(report, indent=2, sort_keys=True))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
