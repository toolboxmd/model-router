"""Documented public CLI for the durable local runner. Stdlib only.

Public operations (built-in defaults, no executor/callback commands):
  submit     persist a stable request ID + prepared task before ack
  start      launch a detached controller with built-in adapters
  status     show job, launches, questions, and output tail
  questions  list pending (persisted) questions
  answer     persist an answer before ack
  cancel     persist cancellation before ack
  recover    reconcile durable records with live ownership (resume saved IDs)
  capacity   show or clear remembered route capacity

State:
  --state-dir <dir> (or DURABLE_RUNNER_STATE_DIR). Created 0700; files 0600.
  SQLite WAL + file-backed output. No secrets in logs/config.

Examples:
  python -m runner --state-dir /tmp/rr submit --request-id r1 \\
    --task '{"goal":"fix typo"}' --workspace /tmp/ws --planner-session claude-1 --no-start
  python -m runner --state-dir /tmp/rr submit --request-id r1 \\
    --task '{"goal":"fix typo"}' --workspace /tmp/ws --planner-session claude-1 --start
  python -m runner --state-dir /tmp/rr start --request-id r1
  python -m runner --state-dir /tmp/rr status --request-id r1
  python -m runner --state-dir /tmp/rr recover --all
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import core, policy


def _state_dir(args) -> str:
    d = args.state_dir or os.environ.get("DURABLE_RUNNER_STATE_DIR") or os.path.expanduser("~/.local/share/durable-runner")
    return os.path.abspath(os.path.expanduser(d))


def _out(obj, code=0) -> int:
    print(json.dumps(obj, indent=2, sort_keys=True))
    return code


def _err(msg, code=1) -> int:
    print(json.dumps({"error": msg}), file=sys.stderr)
    print(json.dumps({"error": msg}))
    return code


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="runner",
                                 description="Durable local task runner.")
    ap.add_argument("--state-dir", default=None, help="private state directory (0700)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("submit", help="persist a prepared task before ack")
    p.add_argument("--request-id", required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--task", default=None, help="JSON task object or plain text")
    g.add_argument("--task-file", default=None, help="file holding JSON task")
    p.add_argument("--workspace", required=True)
    p.add_argument("--planner-session", required=True)
    lane_or_route = p.add_mutually_exclusive_group()
    lane_or_route.add_argument("--route", default=None,
                               help=f"implementation route: {', '.join(policy.implementation_routes())}"
                                    " (default: the lane's sticky home)"
                                    )
    lane_or_route.add_argument("--lane", default=None, choices=sorted(policy.LANE_ALIASES),
                               help="implementation lane: default, small, hard; the job starts on "
                                    "its sticky home; critical is planner-executed and rejected")
    p.add_argument("--policy", default=policy.POLICY_ID)
    p.add_argument("--max-attempts", type=int, default=5,
                   help="controller launches per job (default 5: five 12-step launches cover the 48-step job budget)")
    p.add_argument("--timeout-secs", type=int, default=None)
    p.add_argument("--planner-model", default=None,
                   help="planner model (default: policy planning route; live-test override claude-sonnet-5)")
    p.add_argument("--planner-effort", default=None,
                   help="planner effort (default: policy planning route; live-test override medium)")
    p.add_argument("--job-kind", default="ordinary", choices=("ordinary", "experiment", "replay"),
                   help="ordinary work, an experiment, or a replay of an earlier request")
    p.add_argument("--replay-of", default=None, help="request id this replay repeats")
    p.add_argument("--planner-harness", default="claude", choices=("claude", "codex", "opencode", "grok"),
                   help="harness that hosts the planner session (its session id goes in --planner-session)")
    p.add_argument("--handoff-summary", default=None,
                   help="durable handoff summary stored on the job (default: derived from the task packet)")
    p.add_argument("--handoff-summary-file", default=None,
                   help="file holding the handoff summary")
    p.add_argument("--start", action="store_true",
                   help="launch detached controller with built-in adapters after persist")
    p.add_argument("--no-start", action="store_true",
                   help="test-only: persist without launching so tests stay offline")

    p = sub.add_parser("start", help="launch a detached controller with built-in adapters")
    p.add_argument("--request-id", required=True)

    p = sub.add_parser("status", help="show job status")
    p.add_argument("--request-id", required=True)

    p = sub.add_parser("result", help="print the terminal result, including the completion report")
    p.add_argument("--request-id", required=True)

    p = sub.add_parser("questions", help="list pending questions")
    p.add_argument("--request-id", required=True)
    qg = p.add_mutually_exclusive_group()
    qg.add_argument("--all", action="store_true", help="include answered")
    qg.add_argument("--clear", default=None, metavar="QID",
                    help="operator action: forget a stored question (clears planner_question_conflict)")

    p = sub.add_parser("answer", help="persist an answer before ack")
    p.add_argument("--request-id", required=True)
    p.add_argument("--qid", required=True)
    p.add_argument("--answer", required=True)

    p = sub.add_parser("cancel", help="persist cancellation before ack")
    p.add_argument("--request-id", required=True)

    p = sub.add_parser("recover", help="reconcile records with live ownership")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--request-id", default=None)
    g.add_argument("--all", action="store_true")

    p = sub.add_parser("capacity", help="show remembered route capacity; --clear forgets one")
    p.add_argument("--clear", default=None, metavar="ROUTE",
                   help="operator action after checking the provider allowance")

    args = ap.parse_args(argv)
    sd = _state_dir(args)
    try:
        if args.cmd == "submit":
            task_raw = args.task
            if args.task_file:
                with open(args.task_file, encoding="utf-8") as f:
                    task_raw = f.read()
            try:
                task = json.loads(task_raw)
            except ValueError:
                task = task_raw
            handoff_summary = args.handoff_summary
            if args.handoff_summary_file:
                with open(args.handoff_summary_file, encoding="utf-8") as f:
                    handoff_summary = f.read()
            # Persist first; launch only when explicitly requested with
            # built-in adapters. --no-start wins so deterministic tests
            # stay offline. Default (neither flag) only persists, which
            # preserves the baseline offline behavior.
            if getattr(args, "start", False) and not getattr(args, "no_start", False):
                job = core.submit_and_start(sd, args.request_id, task, args.workspace,
                                            args.planner_session, route=args.route,
                                            policy_id=args.policy, max_attempts=args.max_attempts,
                                            timeout_secs=args.timeout_secs,
                                            planner_model=args.planner_model,
                                            planner_effort=args.planner_effort,
                                            lane=args.lane, job_kind=args.job_kind,
                                            replay_of=args.replay_of,
                                            planner_harness=args.planner_harness,
                                            handoff_summary=handoff_summary)
            else:
                job = core.submit(sd, args.request_id, task, args.workspace,
                                  args.planner_session, route=args.route,
                                  policy_id=args.policy, max_attempts=args.max_attempts,
                                  timeout_secs=args.timeout_secs,
                                  planner_model=args.planner_model,
                                  planner_effort=args.planner_effort,
                                  lane=args.lane, job_kind=args.job_kind,
                                  replay_of=args.replay_of,
                                  planner_harness=args.planner_harness,
                                  handoff_summary=handoff_summary)
            return _out({"acknowledged": True, "request_id": job["request_id"],
                         "status": job["status"], "route": job["route"],
                         "policy": job["policy_id"]})
        if args.cmd == "start":
            info = core.start_controller(sd, args.request_id)
            redacted = dict(info)
            redacted["token"] = "<redacted>"
            return _out({"launched": True, **redacted})
        if args.cmd == "capacity":
            if args.clear:
                return _out(core.clear_capacity(sd, args.clear))
            return _out({"capacity": core.list_capacity(sd),
                         "readings": core.list_readings(sd)})
        if args.cmd == "result":
            return _out(core.result_view(sd, args.request_id))
        if args.cmd == "status":
            return _out(core.status_view(sd, args.request_id))
        if args.cmd == "questions":
            if args.clear:
                return _out(core.clear_question(sd, args.request_id, args.clear))
            qs = core.list_questions(sd, args.request_id, only_pending=not args.all)
            return _out({"request_id": args.request_id, "questions": qs})
        if args.cmd == "answer":
            q = core.answer(sd, args.request_id, args.qid, args.answer)
            return _out({"acknowledged": True, "qid": q["qid"], "status": q["status"]})
        if args.cmd == "cancel":
            job = core.cancel(sd, args.request_id)
            return _out({"acknowledged": True, "request_id": job["request_id"],
                         "status": job["status"]})
        if args.cmd == "recover":
            if args.all:
                return _out({"recovered": core.recover_all(sd)})
            return _out(core.recover_one(sd, args.request_id))
    except (core.NotFoundError, core.ConflictError, core.WorkspaceConflictError,
            core.TerminalError, core.BlockedError, core.OwnershipError,
            core.RunnerError, ValueError) as e:
        return _err(str(e), 2 if isinstance(e, (core.ConflictError, core.WorkspaceConflictError)) else 1)
    return _err("unknown command", 1)


if __name__ == "__main__":
    raise SystemExit(main())
