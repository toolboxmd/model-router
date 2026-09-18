"""Public-path durable loop proof (deterministic, stdlib only).

Uses deterministic fake executable transports in a temporary PATH
(fake ``codex``, ``claude``, ``opencode``) and calls the public CLI
``submit --start``, waits for the detached controller, then asserts the
full durable flow. Never calls live CLIs. Separate fake state/fixture;
existing helper tests are preserved.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import core, store  # noqa: E402

PY = sys.executable

THREAD_ID = "thread-loop-001"
PLANNER_SID = "claude-planner-loop-001"
OC_SESSION = "oc-loop-001"
QID = "q-loop-1"

FAKE_CODEX = """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
state = Path(os.environ["DURABLE_FAKE_STATE"])
state.mkdir(parents=True, exist_ok=True)
log = state / "codex.log"
count_f = state / "codex_resume_count"
argv = sys.argv[1:]
with open(log, "a", encoding="utf-8") as f:
    f.write(json.dumps(argv) + "\\n")
def last_msg_path(args):
    if "--output-last-message" in args:
        i = args.index("--output-last-message")
        if i + 1 < len(args):
            return args[i + 1]
    return None
# Contract guards (fail loudly so the controller marks blocked).
if argv and argv[0] == "exec" and len(argv) > 1 and argv[1] == "resume":
    rid = argv[2] if len(argv) > 2 else ""
    assert rid == "%s", f"resume must use exact saved ID, got {rid!r}"
    assert "--json" in argv, "resume must pass --json"
    assert "--cd" not in argv, "resume must not use --cd"
    assert "--reasoning" not in argv, "resume must not use --reasoning"
    n = 0
    if count_f.exists():
        try:
            n = int(count_f.read_text().strip() or "0")
        except ValueError:
            n = 0
    n += 1
    count_f.write_text(str(n))
    tid = "%s"
    if n == 1:
        env = {"thread_id": tid, "action": "implementation",
               "artifact": "outputs/fix.txt", "payload": {}, "route": "muse-spark-xhigh-free"}
    else:
        env = {"thread_id": tid, "action": "completion",
               "output": "Loop proof complete", "artifact": "outputs/fix.txt"}
    lp = last_msg_path(argv)
    if lp:
        Path(lp).parent.mkdir(parents=True, exist_ok=True)
        Path(lp).write_text(json.dumps(env), encoding="utf-8")
    print(json.dumps({"type": "thread.started", "thread_id": tid}))
    print(json.dumps(env))
else:
    # Fresh dispatch contract.
    assert "--json" in argv, "dispatch must pass --json"
    assert "--output-last-message" in argv, "dispatch must pass --output-last-message"
    assert "--model" in argv and "%s" in argv, "dispatch model"
    blob = " ".join(argv)
    assert "model_reasoning_effort" in blob and "max" in blob, "dispatch reasoning effort"
    assert "--sandbox" in argv and "workspace-write" in argv, "dispatch sandbox"
    assert "--cd" in argv, "dispatch must pass --cd"
    assert "--reasoning" not in argv, "dispatch must not use --reasoning"
    tid = "%s"
    env = {"thread_id": tid, "action": "planner_question",
           "qid": "%s", "prompt": "Confirm the fix?"}
    lp = last_msg_path(argv)
    if lp:
        Path(lp).parent.mkdir(parents=True, exist_ok=True)
        Path(lp).write_text(json.dumps(env), encoding="utf-8")
    print(json.dumps({"type": "thread.started", "thread_id": tid}))
    print(json.dumps(env))
""" % (THREAD_ID, THREAD_ID, "gpt-5.6-luna", THREAD_ID, QID)

FAKE_CLAUDE = """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
state = Path(os.environ["DURABLE_FAKE_STATE"])
state.mkdir(parents=True, exist_ok=True)
log = state / "claude.log"
argv = sys.argv[1:]
with open(log, "a", encoding="utf-8") as f:
    f.write(json.dumps(argv) + "\\n")
assert "--resume" in argv, "claude must use --resume"
i = argv.index("--resume")
sid = argv[i + 1] if i + 1 < len(argv) else ""
assert sid == "%s", f"must resume exact planner session, got {sid!r}"
assert "--fork-session" not in argv, "must never fork planner"
print("Approved as written.")
""" % PLANNER_SID

FAKE_OPENCODE = """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
state = Path(os.environ["DURABLE_FAKE_STATE"])
state.mkdir(parents=True, exist_ok=True)
log = state / "opencode.log"
argv = sys.argv[1:]
with open(log, "a", encoding="utf-8") as f:
    f.write(json.dumps(argv) + "\\n")
assert argv and argv[0] == "run", f"must use opencode run, got {argv!r}"
assert "--format" in argv and "json" in argv, "must use --format json"
assert "--pure" in argv, "must use --pure"
assert "--dir" in argv, "must use --dir"
assert "--model" in argv and "opencode/muse-spark-1.3-contributor-free" in argv, f"free model, got {argv!r}"
assert "--variant" in argv and "xhigh" in argv, "must use --variant xhigh"
assert "--agent" in argv and "build" in argv, "must use --agent build"
assert "--effort" not in argv, "must not use --effort"
assert "--cd" not in argv, "must not use --cd"
print(json.dumps({"opencode_session_id": "%s", "ok": True}))
""" % OC_SESSION


def kill_pid(pid):
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        return


class TestPublicLoopProof(unittest.TestCase):
    def test_submit_start_drives_full_durable_loop(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        fake_bin = base / "fakebin"
        fake_bin.mkdir()
        fake_state = base / "fakestate"
        fake_state.mkdir()
        for name, body in (("codex", FAKE_CODEX), ("claude", FAKE_CLAUDE),
                           ("opencode", FAKE_OPENCODE)):
            p = fake_bin / name
            p.write_text(body, encoding="utf-8")
            os.chmod(p, 0o755)
        env = dict(os.environ)
        env["PATH"] = str(fake_bin) + os.pathsep + env.get("PATH", "")
        env["DURABLE_FAKE_STATE"] = str(fake_state)

        req = "loop-proof-001"
        task = json.dumps({"goal": "Loop proof fix", "scope": "one file"})
        cmd = [PY, "-m", "runner", "--state-dir", sd, "submit",
               "--request-id", req, "--task", task,
               "--workspace", str(ws), "--planner-session", PLANNER_SID,
               "--start"]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=20,
                           cwd=str(ROOT), env=env)
        self.assertEqual(p.returncode, 0, p.stderr[-2000:])
        out = json.loads(p.stdout)
        self.assertTrue(out.get("acknowledged"))

        # Wait for the detached controller (built-in defaults, fake PATH).
        deadline = time.time() + 15.0
        job = None
        while time.time() < deadline:
            try:
                job = core.get_job(sd, req)
            except Exception:
                time.sleep(0.1)
                continue
            if job["status"] == "succeeded":
                break
            if job["status"] in ("failed", "cancelled", "blocked"):
                break
            time.sleep(0.2)
        job = core.get_job(sd, req)
        self.assertEqual(job["status"], "succeeded",
                         f"job={job.get('status')} block={job.get('block_reason')} err={job.get('last_error_json')}")
        self.assertEqual(job["codex_task_id"], THREAD_ID)

        def read_log(name):
            f = fake_state / name
            if not f.exists():
                return []
            return [json.loads(line) for line in f.read_text().splitlines() if line.strip()]

        codex_calls = read_log("codex.log")
        claude_calls = read_log("claude.log")
        oc_calls = read_log("opencode.log")

        # One Luna fresh dispatch (exec without resume) + exact-ID resumes.
        dispatches = [c for c in codex_calls
                      if len(c) > 1 and c[0] == "exec" and c[1] != "resume"]
        resumes = [c for c in codex_calls
                   if len(c) > 1 and c[0] == "exec" and c[1] == "resume"]
        self.assertEqual(len(dispatches), 1, codex_calls)
        self.assertGreaterEqual(len(resumes), 1, codex_calls)
        for r in resumes:
            self.assertIn(THREAD_ID, r)
        # No duplicate dispatch after the loop.
        self.assertEqual(len(codex_calls), len(dispatches) + len(resumes))

        # One Claude --resume of the exact original planner ID, never fork.
        self.assertEqual(len(claude_calls), 1, claude_calls)
        self.assertIn("--resume", claude_calls[0])
        self.assertIn(PLANNER_SID, claude_calls[0])

        # One Muse invocation with free model + xhigh + --dir.
        self.assertEqual(len(oc_calls), 1, oc_calls)
        oc = oc_calls[0]
        self.assertIn("opencode/muse-spark-1.3-contributor-free", oc)
        self.assertIn("xhigh", oc)
        self.assertIn("--dir", oc)
        self.assertIn(str(ws), oc)

        # Durable question then answer then completion.
        qs = core.list_questions(sd, req, only_pending=False)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0]["qid"], QID)
        self.assertEqual(qs[0]["status"], "answered")
        self.assertIn("Approved", qs[0]["answer"] or "")
        self.assertEqual(job["opencode_session_id"], OC_SESSION)
        # Controller state + child records preserve session IDs.
        self.assertTrue(job.get("controller_state"))
        con = store.connect(sd)
        try:
            kids = con.execute(
                "SELECT kind, session_id FROM child_calls WHERE request_id=?",
                (req,)).fetchall()
        finally:
            con.close()
        kinds = [r["kind"] for r in kids]
        self.assertIn("codex_dispatch", kinds)
        self.assertIn("codex_resume", kinds)
        self.assertIn("claude_callback", kinds)
        self.assertIn("opencode_run", kinds)

        # Recover/restart must not fork a second planner session or writer.
        n_claude_before = len(claude_calls)
        n_codex_before = len(codex_calls)
        rc, _, _ = self._cli(sd, "recover", "--request-id", req, env=env)
        self.assertEqual(rc, 0)
        time.sleep(0.5)
        codex_after = read_log("codex.log")
        claude_after = read_log("claude.log")
        self.assertEqual(len(claude_after), n_claude_before,
                         "recover must never fork a second planner session")
        self.assertEqual(len(codex_after), n_codex_before,
                         "recover must never start a duplicate writer")
        job2 = core.get_job(sd, req)
        self.assertEqual(job2["status"], "succeeded")
        self.assertEqual(job2["codex_task_id"], THREAD_ID)
        # Terminal jobs refuse resurrection.
        p2 = subprocess.run(
            [PY, "-m", "runner", "--state-dir", sd, "start",
             "--request-id", req],
            capture_output=True, text=True, timeout=20, cwd=str(ROOT), env=env)
        self.assertNotEqual(p2.returncode, 0)

    def _cli(self, sd, *args, env):
        cmd = [PY, "-m", "runner", "--state-dir", sd, *args]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=20,
                           cwd=str(ROOT), env=env)
        try:
            out = json.loads(p.stdout) if p.stdout.strip() else {}
        except ValueError:
            out = {"raw": p.stdout}
        return p.returncode, out, p.stderr


if __name__ == "__main__":
    unittest.main(verbosity=2)
