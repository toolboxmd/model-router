"""Preserve evidence named by a stale T3 worker before interrupting it."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import tempfile

from . import adapters, store, t3exec


TEXT_LIMIT = 64 * 1024
ARTIFACT_LIMIT = 16
# Explicit file references, including Markdown links, backticks and $TMPDIR.
_PATH = re.compile(r"(?:https?://|\$\{TMPDIR\}|\$TMPDIR|/|\./|[\w.-]+/)[^\s`\"'<>|;()]+")
_TEXT_SUFFIXES = (".txt", ".log", ".md", ".json", ".patch", ".diff")


def _references(text: str) -> list[str]:
    # Preserve spaces and bare filenames when explicitly quoted or linked.
    quoted = re.findall(r"`([^`\n]+)`|\[[^\]]*\]\(<?([^\n)]+?)>?\)", text)
    refs = [a or b for a, b in quoted
            if Path(a or b).suffix.lower() in _TEXT_SUFFIXES]
    refs += [m.group().rstrip(".,:]}\\") for m in _PATH.finditer(text)]
    return list(dict.fromkeys(refs))


def capture(snapshot: dict, text: str, workspace: str, directory: Path) -> dict:
    """Retain redacted text and bounded named text artifacts in the job store.

    Never execute worker text or fetch URLs. Only regular text files inside
    the workspace or host temporary directories are read. Keep a reference
    and explicit reason for every artifact that cannot be copied.
    """
    thread = t3exec.snapshot_thread(snapshot)
    turn = t3exec.latest_turn_id(snapshot)
    messages = [m.get("text", "") for m in thread.get("messages", [])
                if isinstance(m, dict) and m.get("role") == "assistant"
                and (not turn or m.get("turnId") == turn)
                and isinstance(m.get("text"), str)]
    text = "\n\n".join(messages) or text
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    text = adapters.redact_text(text)
    text_path = directory / "worker.txt"
    store.secure_write_text(text_path, text)
    # Tool output can name an artifact before the assistant mentions it.
    activities = [a for a in thread.get("activities", [])
                  if isinstance(a, dict) and (not turn or a.get("turnId") == turn)]
    sources = text + "\n" + json.dumps(activities, ensure_ascii=False)
    refs = _references(sources)
    roots = [Path(workspace).resolve(), Path(tempfile.gettempdir()).resolve(), Path("/tmp").resolve()]
    artifacts = []
    copied = 0
    for ref in refs:
        # URL paths are references, not local files to fetch.
        if "://" in ref or ref.startswith("//"):
            artifacts.append({"source": adapters.redact_text(ref), "note": "URL reference only"})
            continue
        expanded = ref.replace("${TMPDIR}", tempfile.gettempdir()).replace("$TMPDIR", tempfile.gettempdir())
        path = Path(expanded)
        if not path.is_absolute():
            path = roots[0] / path
        item = {"source": adapters.redact_text(ref)}
        try:
            path = path.resolve()
            if not any(path.is_relative_to(root) for root in roots):
                item["note"] = "outside workspace and temporary directories; reference only"
            elif path.suffix.lower() not in _TEXT_SUFFIXES:
                item["note"] = "not a text artifact; reference only"
            elif copied >= ARTIFACT_LIMIT:
                item["note"] = "artifact copy limit reached; reference only"
            else:
                fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
                with os.fdopen(fd, "rb") as source:
                    if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                        raise ValueError("not a regular file")
                    data = source.read(TEXT_LIMIT + 1)
                if b"\x00" in data:
                    raise ValueError("not a text file")
                contents = data[:TEXT_LIMIT].decode("utf-8", errors="replace")
                if path.suffix.lower() == ".json":
                    try:
                        contents = json.dumps(adapters.redact_nested(json.loads(contents)), indent=2)
                    except ValueError:
                        pass
                contents = adapters.redact_text(contents)
                saved = directory / f"artifact-{copied + 1}.txt"
                store.secure_write_text(saved, contents)
                item.update(saved=str(saved), excerpt=contents[:4000],
                            truncated=len(data) > TEXT_LIMIT or len(contents) > 4000)
                copied += 1
        except (OSError, ValueError) as e:
            item["note"] = adapters.redact_text(f"unavailable: {e}")
        artifacts.append(item)
    return {"worker_text": str(text_path), "text": text[:8000],
            "text_truncated": len(text) > 8000, "artifacts": artifacts}


def report(records: list[dict]) -> str:
    """Evidence carried across retries into the eventual terminal report."""
    if not records:
        return ""
    lines = ["", "SALVAGED STALE WORKER EVIDENCE (partial, not a completion verdict):"]
    for rec in records:
        lines += [f"Thread: {rec.get('thread_id')} | Route: {rec.get('route')}",
                  f"Evidence: {rec.get('path')}"]
        salvage = rec.get("salvage") or {}
        if salvage.get("text"):
            lines.append(salvage["text"])
        if salvage.get("text_truncated"):
            lines.append(f"[Text excerpt; full text: {salvage['worker_text']}]")
        for item in salvage.get("artifacts", []):
            lines.append(f"Artifact: {item['source']}")
            if item.get("saved"):
                lines += [f"Saved: {item['saved']}", item["excerpt"]]
                if item.get("truncated"):
                    lines.append("[Artifact excerpt; see saved copy]")
            else:
                lines.append(item.get("note", "reference only"))
        if rec.get("salvage_error"):
            lines.append(f"Salvage incomplete: {rec['salvage_error']}")
    return "\n".join(lines)
