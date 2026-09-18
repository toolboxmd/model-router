"""Detached fake worker. Stdlib only.

The worker advertises its start-token identity in
<state_dir>/workers/<request_id>.json and appends to
<state_dir>/outputs/<request_id>.log. Ownership is proven by matching
that advertised token against the DB lease; PID aliveness alone never
proves ownership.

Modes (deterministic, no live model calls):
  sleep      advertise then heartbeat for --duration seconds
  partial    append partial output then exit without result (death sim)
  exit-quick advertise then exit immediately (crash-before-ack sim)
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path


def _utcnow() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run_worker(state_dir: str, request_id: str, token: str, mode: str = "sleep",
               duration: float = 30.0, text: str = "") -> int:
    root = Path(state_dir)
    workers_dir = root / "workers"
    outputs_dir = root / "outputs"
    workers_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    outputs_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    ident = workers_dir / f"{request_id}.json"
    log = outputs_dir / f"{request_id}.log"
    pid = os.getpid()

    from .supervisor import process_start_identity
    start = process_start_identity(pid)

    def advertise() -> None:
        payload = json.dumps({"token": token, "pid": pid, "updated": _utcnow(), "start": start})
        tmp = ident.with_name(ident.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        os.replace(tmp, ident)
        try:
            os.chmod(ident, 0o600)
        except OSError:
            pass

    def append(line: str) -> None:
        if not log.exists():
            with open(log, "w", encoding="utf-8") as f:
                f.write("")
            try:
                os.chmod(log, 0o600)
            except OSError:
                pass
        with open(log, "a", encoding="utf-8") as f:
            f.write(line)
        try:
            os.chmod(log, 0o600)
        except OSError:
            pass

    advertise()
    append(f"worker start request={request_id} pid={pid}\n")
    if text:
        append(text if text.endswith("\n") else text + "\n")

    if mode == "exit-quick":
        return 0
    if mode == "partial":
        append("partial output chunk 1\n")
        advertise()
        return 0
    # sleep / heartbeat mode
    end = time.time() + max(0.0, duration)
    while time.time() < end:
        advertise()
        time.sleep(0.2)
    append("worker heartbeat end\n")
    advertise()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="runner.worker")
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--request-id", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--mode", default="sleep")
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--text", default="")
    args = ap.parse_args(argv)
    return run_worker(args.state_dir, args.request_id, args.token,
                      mode=args.mode, duration=args.duration, text=args.text)


if __name__ == "__main__":
    raise SystemExit(main())
