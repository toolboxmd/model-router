"""SQLite + file-backed durable store. Stdlib only.

Layout under <state_dir>:
  jobs.db                    SQLite, WAL, 0600
  outputs/<request_id>.log   file-backed worker output, 0600, append-only
  outputs/<request_id>.result.json  terminal result mirror, 0600
  workers/<request_id>.json  live worker advertised identity, 0600
                             {"token":..., "pid":..., "updated": iso-ts}
Secrets are never written to logs/config by this module; callers must
not place credentials in task metadata. Task bodies are stored for
recovery but never emitted to stdout logs.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS jobs (
  request_id TEXT PRIMARY KEY,
  task_json TEXT NOT NULL,
  task_hash TEXT NOT NULL,
  workspace TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  planner_session_id TEXT NOT NULL,
  executor_session_id TEXT NOT NULL,
  output_path TEXT NOT NULL,
  status TEXT NOT NULL,
  route TEXT NOT NULL,
  result_json TEXT,
  error_class TEXT,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  timeout_secs INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  block_reason TEXT,
  owner_token TEXT,
  owner_pid INTEGER,
  codex_task_id TEXT,
  opencode_session_id TEXT,
  adapter TEXT,
  model TEXT,
  effort TEXT,
  planner_model TEXT,
  planner_effort TEXT,
  controller_state TEXT,
  last_error_json TEXT,
  planner_cwd TEXT,
  owner_start TEXT,
  lane TEXT
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS launches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  start_token TEXT NOT NULL UNIQUE,
  pid INTEGER,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  ack_at TEXT,
  UNIQUE(request_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS questions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL,
  qid TEXT NOT NULL,
  prompt TEXT NOT NULL,
  status TEXT NOT NULL,
  answer TEXT,
  created_at TEXT NOT NULL,
  answered_at TEXT,
  UNIQUE(request_id, qid)
);
CREATE TABLE IF NOT EXISTS child_calls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  kind TEXT NOT NULL,
  cmd_json TEXT NOT NULL,
  rc INTEGER,
  session_id TEXT,
  output_path TEXT
);
CREATE TABLE IF NOT EXISTS invocations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  invocation_id TEXT NOT NULL UNIQUE,
  request_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  cmd_json TEXT NOT NULL,
  workspace TEXT NOT NULL,
  owner_token TEXT NOT NULL,
  pid INTEGER,
  pgid INTEGER,
  process_start TEXT,
  supervisor_pid INTEGER,
  supervisor_pgid INTEGER,
  stdout_path TEXT NOT NULL,
  stderr_path TEXT NOT NULL,
  started_at TEXT NOT NULL,
  state TEXT NOT NULL,
  rc INTEGER,
  session_id TEXT,
  session_kind TEXT,
  task_json TEXT,
  ended_at TEXT,
  result_json TEXT,
  consumed_at TEXT,
  timeout_secs INTEGER,
  meta_json TEXT,
  supervisor_start TEXT,
  action_key TEXT
);
CREATE TABLE IF NOT EXISTS capacity (
  route TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  evidence_json TEXT,
  reset_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_launches_req ON launches(request_id);
CREATE INDEX IF NOT EXISTS idx_events_req ON events(request_id);
CREATE INDEX IF NOT EXISTS idx_questions_req ON questions(request_id);
CREATE INDEX IF NOT EXISTS idx_child_req ON child_calls(request_id);
CREATE INDEX IF NOT EXISTS idx_invocations_request ON invocations(request_id);
CREATE INDEX IF NOT EXISTS idx_invocations_state ON invocations(state);
"""

TERMINAL = ("succeeded", "failed", "cancelled")
ACTIVE_WORKSPACE_STATUSES = ("pending", "running", "question_pending", "blocked", "cancelling")


def ensure_state_dir(state_dir: str | os.PathLike) -> Path:
    # Absolute, because children run in their workspace and every recorded
    # path must stay valid from any working directory.
    root = Path(os.path.abspath(os.path.expanduser(str(state_dir))))
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(root, 0o700)
    except OSError:
        pass
    for sub in ("outputs", "workers"):
        p = root / sub
        p.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(p, 0o700)
        except OSError:
            pass
    # Secure the database file itself if it exists.
    db = root / "jobs.db"
    if db.exists():
        try:
            os.chmod(db, 0o600)
        except OSError:
            pass
    return root


def _add_column(con: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    """Add a migration column; a concurrent first connection may have added
    it a moment earlier, which is not an error."""
    try:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise


def connect(state_dir: str | os.PathLike) -> sqlite3.Connection:
    root = ensure_state_dir(state_dir)
    db = root / "jobs.db"
    first = not db.exists()
    con = sqlite3.connect(str(db), timeout=10.0, isolation_level=None)
    con.row_factory = sqlite3.Row
    # Concurrent first connections may race on WAL setup; retry briefly.
    last_err = None
    for _ in range(20):
        try:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA foreign_keys=ON;")
            con.executescript(SCHEMA)
            break
        except sqlite3.OperationalError as e:
            last_err = e
            if "locked" not in str(e).lower():
                raise
            time.sleep(0.05)
    else:
        raise last_err or sqlite3.OperationalError("database locked during setup")
    # Lightweight migration for DBs created before owner lease columns.
    cols = {r["name"] for r in con.execute("PRAGMA table_info(jobs)").fetchall()}
    if "owner_token" not in cols:
        _add_column(con, "jobs", "owner_token", "TEXT")
    if "owner_pid" not in cols:
        _add_column(con, "jobs", "owner_pid", "INTEGER")
    for _col, _ddl in (
        ("codex_task_id", "TEXT"),
        ("opencode_session_id", "TEXT"),
        ("adapter", "TEXT"),
        ("model", "TEXT"),
        ("effort", "TEXT"),
        ("planner_model", "TEXT"),
        ("planner_effort", "TEXT"),
        ("controller_state", "TEXT"),
        ("last_error_json", "TEXT"),
        ("planner_cwd", "TEXT"),
        ("owner_start", "TEXT"),
        ("lane", "TEXT"),
    ):
        if _col not in cols:
            _add_column(con, "jobs", _col, _ddl)
    inv_cols = {r["name"] for r in con.execute("PRAGMA table_info(invocations)").fetchall()}
    for _col, _ddl in (
        ("process_start", "TEXT"),
        ("supervisor_pid", "INTEGER"),
        ("supervisor_pgid", "INTEGER"),
        ("result_json", "TEXT"),
        ("consumed_at", "TEXT"),
        ("timeout_secs", "INTEGER"),
        ("meta_json", "TEXT"),
        ("supervisor_start", "TEXT"),
        ("action_key", "TEXT"),
    ):
        if _col not in inv_cols:
            _add_column(con, "invocations", _col, _ddl)
    if first:
        try:
            os.chmod(db, 0o600)
        except OSError:
            pass
    else:
        try:
            if (db.stat().st_mode & 0o777) != 0o600:
                os.chmod(db, 0o600)
        except OSError:
            pass
    return con


def secure_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def secure_write_text(path: Path, text: str) -> None:
    secure_write(path, text.encode("utf-8"))


def append_text(path: Path, text: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.exists():
        secure_write_text(path, "")
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def output_path_for(root: Path, request_id: str) -> Path:
    return root / "outputs" / f"{request_id}.log"


def result_path_for(root: Path, request_id: str) -> Path:
    return root / "outputs" / f"{request_id}.result.json"


def worker_identity_path(root: Path, request_id: str) -> Path:
    return root / "workers" / f"{request_id}.json"


def read_worker_identity(root: Path, request_id: str) -> dict | None:
    p = worker_identity_path(root, request_id)
    try:
        raw = p.read_bytes().decode("utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


def write_worker_identity(root: Path, request_id: str, token: str, pid: int, updated: str) -> None:
    secure_write_text(
        worker_identity_path(root, request_id),
        json.dumps({"token": token, "pid": pid, "updated": updated}),
    )


_SECRET_KEY_PARTS = ("secret", "token", "password", "api_key", "apikey", "credential", "auth")


def redact_for_log(obj):
    """Recursively strip secret-bearing keys so logs never carry credentials."""
    if isinstance(obj, dict):
        return {k: ("<redacted>" if any(s in str(k).lower() for s in _SECRET_KEY_PARTS)
                    else redact_for_log(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact_for_log(v) for v in obj]
    return obj
