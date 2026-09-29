#!/usr/bin/env python3
"""Compare dispatcher modes (#146): Luna dispatcher thread vs the planner.

Stdlib only, read-only. Reads the runner ledger (``jobs.db``) for each
terminal job's mode and timings, and Agent Observer's ledger
(``observer.db``, after ``agent-observer sync``) for the tokens the
dispatching agent spent:

- ``luna``: every response in the job's dispatcher child thread session.
- ``planner``: the planner session's responses inside each decision
  window, from ``planner_dispatch_posted`` to ``planner_dispatch_reply``.

Both modes also count the planner's responses while it answered the
job's questions (``question_posted`` to ``answer_persisted``). Tokens
stay grouped by counter semantics: Observer's totals are never added
across harnesses. Terminal reports reach the planner in both modes and
are not counted.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import sys
from datetime import datetime

HARNESS_OF_PROVIDER = {"claudeAgent": "claude", "codex": "codex",
                       "opencode": "opencode", "grok": "grok"}
TERMINAL = ("succeeded", "failed", "blocked", "cancelled")


def _epoch(ts: str | None) -> float | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def _session_key(obs: sqlite3.Connection | None, thread_id: str | None) -> str | None:
    if obs is None or not thread_id:
        return None
    row = obs.execute("SELECT provider, native_session FROM t3_threads WHERE thread_id=?",
                      (thread_id,)).fetchone()
    if row is None:
        return None
    harness = HARNESS_OF_PROVIDER.get(row["provider"], row["provider"])
    return f"{harness}:{row['native_session']}"


def _tokens(obs, session_key, windows=None) -> dict | None:
    """Total tokens by counter semantics; None when the session is unknown."""
    if obs is None or session_key is None:
        return None
    rows = obs.execute("SELECT ts, semantics, total_tokens FROM responses"
                       " WHERE session_key=? AND is_overlap=0", (session_key,)).fetchall()
    out: dict = {}
    for r in rows:
        if windows is not None and not any(
                r["ts"] is not None and a <= r["ts"] <= b for a, b in windows):
            continue
        if r["total_tokens"] is None:
            continue
        key = r["semantics"] or "unknown"
        out[key] = out.get(key, 0) + int(r["total_tokens"])
    return out


def _add(a: dict | None, b: dict | None) -> dict | None:
    if a is None and b is None:
        return None
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[k] = out.get(k, 0) + v
    return out


def _windows(events, start_kind, end_kind, key_field) -> list[tuple[float, float]]:
    opened, spans = {}, []
    for ev in events:
        payload = ev["payload"]
        key = payload.get(key_field)
        if ev["kind"] == start_kind and key not in opened:
            opened[key] = ev["t"]
        elif ev["kind"] == end_kind and key in opened:
            spans.append((opened.pop(key), ev["t"]))
    return spans


def job_rows(runner: sqlite3.Connection, obs: sqlite3.Connection | None,
             since: str | None = None) -> list[dict]:
    cols = {r["name"] for r in runner.execute("PRAGMA table_info(jobs)")}
    mode_col = "dispatcher" if "dispatcher" in cols else "NULL AS dispatcher"
    query = (f"SELECT request_id, status, created_at, planner_t3_thread, {mode_col}"
             " FROM jobs WHERE status IN (?,?,?,?)")
    args: list = list(TERMINAL)
    if since:
        query += " AND created_at >= ?"
        args.append(since)
    out = []
    for job in runner.execute(query + " ORDER BY created_at", args).fetchall():
        rid = job["request_id"]
        events = []
        for e in runner.execute("SELECT ts, kind, payload_json FROM events"
                                " WHERE request_id=? ORDER BY id", (rid,)):
            try:
                payload = json.loads(e["payload_json"] or "{}")
            except ValueError:
                payload = {}
            events.append({"t": _epoch(e["ts"]), "kind": e["kind"],
                           "payload": payload if isinstance(payload, dict) else {}})
        mode = job["dispatcher"] or "luna"
        submitted = next((e["t"] for e in events if e["kind"] == "submitted"),
                         _epoch(job["created_at"]))
        first_worker = next((e["t"] for e in events if e["kind"] == "t3_thread"
                             and str(e["payload"].get("slot", "")).startswith("impl_")), None)
        approved = next((e["t"] for e in events if e["kind"] == "review_verdict"
                         and e["payload"].get("verdict") == "approve"), None)
        planner_key = _session_key(obs, job["planner_t3_thread"])
        questions = _tokens(obs, planner_key,
                            _windows(events, "question_posted", "answer_persisted", "qid"))
        if mode == "planner":
            decisions = _windows(events, "planner_dispatch_posted",
                                 "planner_dispatch_reply", "key")
            dispatch = _tokens(obs, planner_key, decisions)
            decision_secs = round(sum(b - a for a, b in decisions), 3)
        else:
            dispatch = None
            for tid in {e["payload"].get("thread_id") for e in events
                        if e["kind"] == "t3_thread" and e["payload"].get("slot") == "dispatch"}:
                dispatch = _add(dispatch, _tokens(obs, _session_key(obs, tid)))
            decision_secs = None
        out.append({
            "request_id": rid, "dispatcher": mode, "status": job["status"],
            "secs_to_first_worker": (round(first_worker - submitted, 3)
                                     if first_worker and submitted else None),
            "secs_to_reviewed_pr": (round(approved - submitted, 3)
                                    if approved and submitted else None),
            "planner_decision_secs": decision_secs,
            "dispatch_tokens": dispatch,
            "planner_question_tokens": questions,
        })
    return out


def _median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def summarize(rows: list[dict]) -> dict:
    """Per mode: sample size, success rate, medians, token sums by semantics."""
    out = {}
    for mode in sorted({r["dispatcher"] for r in rows}):
        group = [r for r in rows if r["dispatcher"] == mode]
        tokens, measured = {}, 0
        for r in group:
            combined = _add(r["dispatch_tokens"], r["planner_question_tokens"])
            if combined is not None:
                measured += 1
                tokens = _add(tokens, combined)
        out[mode] = {
            "jobs": len(group),
            "succeeded": sum(r["status"] == "succeeded" for r in group),
            "success_rate": round(sum(r["status"] == "succeeded" for r in group)
                                  / len(group), 3),
            "median_secs_to_first_worker": _median(r["secs_to_first_worker"] for r in group),
            "median_secs_to_reviewed_pr": _median(r["secs_to_reviewed_pr"] for r in group),
            "jobs_with_tokens": measured,
            "dispatch_and_question_tokens": tokens,
        }
    return out


def main(argv=None) -> int:
    state = (os.environ.get("DURABLE_RUNNER_STATE_DIR")
             or os.path.expanduser("~/.local/share/durable-runner"))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--runner-db", default=os.path.join(state, "jobs.db"))
    ap.add_argument("--observer-db", default=os.environ.get("AGENT_OBSERVER_DB") or
                    os.path.expanduser("~/.local/state/agent-observer/observer.db"))
    ap.add_argument("--since", default=None, help="ISO date: jobs created on or after")
    ap.add_argument("--jobs", action="store_true", help="also print one row per job")
    args = ap.parse_args(argv)
    runner = _connect(args.runner_db)
    obs = _connect(args.observer_db) if os.path.exists(args.observer_db) else None
    rows = job_rows(runner, obs, args.since)
    report = {"modes": summarize(rows),
              "observer": args.observer_db if obs is not None else None}
    if args.jobs:
        report["jobs"] = rows
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
