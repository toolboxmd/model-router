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
from runner.core import _is_pid_alive  # noqa: E402

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

FAKE_OPENCODE = r'''#!/usr/bin/env python3
import json, os, sys, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
state = Path(os.environ["DURABLE_FAKE_STATE"])
state.mkdir(parents=True, exist_ok=True)
log = state / "opencode.log"
argv = sys.argv[1:]
with open(log, "a", encoding="utf-8") as f:
    f.write(json.dumps(argv) + "\n")
SESSION = "''' + OC_SESSION + r'''"
PASSWORD = os.environ.get("OPENCODE_SERVER_PASSWORD") or ""

if argv and argv[0] == "serve":
    assert "--pure" in argv, "serve must use --pure"
    assert "--hostname" in argv and "127.0.0.1" in argv, "serve must bind loopback"
    assert "--port" in argv, "serve must pass --port"
    assert PASSWORD, "serve password must arrive via environment, not argv"
    assert "password" not in " ".join(argv).lower()
    class H(BaseHTTPRequestHandler):
        def _auth(self):
            got = self.headers.get("Authorization") or ""
            return got == "Bearer " + PASSWORD
        def _json(self, code, obj):
            data = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def do_GET(self):
            if not self._auth():
                return self._json(401, {"error": "unauthorized"})
            parsed = urlparse(self.path)
            if parsed.path == "/session/status":
                qs = parse_qs(parsed.query)
                sid = (qs.get("session") or [SESSION])[0]
                self._json(200, {"session": sid, "status": "idle"})
                return
            self._json(404, {"error": "missing"})
        def do_POST(self):
            if not self._auth():
                return self._json(401, {"error": "unauthorized"})
            length = int(self.headers.get("Content-Length") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8") or "{}")
            except ValueError:
                body = {}
            path = urlparse(self.path).path
            if path == "/session":
                self._json(200, {"id": SESSION})
                return
            if path.endswith("/prompt"):
                (state / "opencode-prompt.jsonl").open("a").write(json.dumps(body) + "\n")
                self._json(200, {"ok": True, "session": SESSION})
                return
            if path == "/session/abort":
                self._json(200, {"ok": True})
                return
            self._json(404, {"error": "missing"})
        def log_message(self, *args):
            return
    srv = HTTPServer(("127.0.0.1", 0), H)
    port = srv.server_address[1]
    print(f"listening on http://127.0.0.1:{port}", flush=True)
    try:
        srv.serve_forever(poll_interval=0.05)
    except KeyboardInterrupt:
        pass
    sys.exit(0)

assert argv and argv[0] == "run", f"must use opencode run or serve, got {argv!r}"
assert "--format" in argv and "json" in argv, "must use --format json"
assert "--pure" in argv, "must use --pure"
assert "--dir" in argv, "must use --dir"
assert "--model" in argv and "opencode/muse-spark-1.3-contributor-free" in argv, f"free model, got {argv!r}"
assert "--variant" in argv and "xhigh" in argv, "must use --variant xhigh"
assert "--agent" in argv and "build" in argv, "must use --agent build"
assert "--effort" not in argv, "must not use --effort"
assert "--cd" not in argv, "must not use --cd"
print(json.dumps({"opencode_session_id": SESSION, "ok": True}))
'''


def kill_pid(pid):
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        return


class TestPublicLoopProof(unittest.TestCase):
    def test_submit_start_drives_full_durable_loop(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
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

        # Owned ephemeral OpenCode serve (public path) or run (fallback).
        self.assertGreaterEqual(len(oc_calls), 1, oc_calls)
        serve_calls = [c for c in oc_calls if c and c[0] == "serve"]
        run_calls = [c for c in oc_calls if c and c[0] == "run"]
        self.assertTrue(serve_calls or run_calls, oc_calls)
        if serve_calls:
            sc = serve_calls[0]
            self.assertIn("--pure", sc)
            self.assertIn("127.0.0.1", sc)
            self.assertIn("--port", sc)
            self.assertNotIn("password", " ".join(sc).lower())
            prompt_log = fake_state / "opencode-prompt.jsonl"
            self.assertTrue(prompt_log.exists(), "owned server must prompt the saved session")
            prompts = [json.loads(l) for l in prompt_log.read_text().splitlines() if l.strip()]
            self.assertTrue(prompts)
            self.assertIn("opencode/muse-spark-1.3-contributor-free",
                          json.dumps(prompts))
        else:
            oc = run_calls[0]
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
        self.assertTrue("opencode_run" in kinds or "opencode_control" in kinds
                        or "opencode_serve" in kinds, kinds)

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

    def test_controller_death_child_lives_no_duplicate_codex(self):
        """Regression: killing only the controller must not orphan the writer.

        A durable child invocation survives controller death. recover() must
        detect the live process group and captured thread ID, and start() must
        refuse to fork a second Codex. This is the exact unsafe ownership bug
        reproduced by reproduce-controller-crash.py.
        """
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        fake_bin = base / "fakebin"
        fake_bin.mkdir()
        fake_state = base / "fakestate"
        fake_state.mkdir()
        calls = fake_state / "codex-calls.jsonl"
        thread_id = "crash-repro-thread-001"

        fake_codex = '''#!/usr/bin/env python3
import json, os, time
from pathlib import Path
p = Path(os.environ["CRASH_REPRO_CALLS"])
with p.open("a") as f:
    f.write(json.dumps({"pid": os.getpid(), "pgid": os.getpgid(0),
                        "args": __import__("sys").argv[1:]}) + "\\n")
print(json.dumps({"type": "thread.started", "thread_id": "%s"}), flush=True)
time.sleep(60)
''' % thread_id
        p = fake_bin / "codex"
        p.write_text(fake_codex, encoding="utf-8")
        os.chmod(p, 0o755)
        env = dict(os.environ)
        env["PATH"] = str(fake_bin) + os.pathsep + env.get("PATH", "")
        env["CRASH_REPRO_CALLS"] = str(calls)
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        def rows():
            if not calls.exists():
                return []
            return [json.loads(line) for line in calls.read_text().splitlines() if line.strip()]

        def wait_calls(count, deadline=10.0):
            end = time.time() + deadline
            while time.time() < end:
                if len(rows()) >= count:
                    return True
                time.sleep(0.05)
            return False

        def kill_pgid(pgid):
            try:
                os.killpg(int(pgid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass

        def wait_pgid_dead(pgid, deadline=5.0):
            end = time.time() + deadline
            while time.time() < end:
                try:
                    os.killpg(int(pgid), 0)
                except ProcessLookupError:
                    return True
                except (PermissionError, OSError):
                    return False
                time.sleep(0.05)
            return False

        req = "crash-repro"
        rc, out, err = self._cli(
            sd, "submit", "--request-id", req,
            "--task", json.dumps({"goal": "offline crash reproduction"}),
            "--workspace", str(ws), "--planner-session", "original-planner",
            "--planner-model", "claude-sonnet-5", "--planner-effort", "medium",
            "--start", env=env)
        self.assertEqual(rc, 0, err[-2000:])
        self.assertTrue(out.get("acknowledged"))
        self.assertTrue(wait_calls(1), "first fake Codex did not launch")

        # Kill only the controller lease holder, not the fake Codex child.
        job = core.get_job(sd, req)
        controller_pid = job["owner_pid"]
        self.assertIsNotNone(controller_pid)
        try:
            os.kill(int(controller_pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        time.sleep(0.3)

        first = rows()[0]
        self.assertTrue(_is_pid_alive(first["pid"]), "first fake Codex child must survive controller death")

        # Recover must NOT declare the worker gone and clear the lease.
        rc, rec, err = self._cli(sd, "recover", "--request-id", req, env=env)
        self.assertEqual(rc, 0, err[-2000:])
        self.assertNotEqual(rec.get("action"), "worker-dead-cleared",
                            "recover must not clear a live child invocation")
        self.assertIn(rec.get("action"),
                      ("adopted-live-invocation", "blocked-claimed-live-invocation"))

        # start() must refuse to fork a second writer.
        rc, start_out, err = self._cli(sd, "start", "--request-id", req, env=env)
        self.assertNotEqual(rc, 0, "start must refuse duplicate launch while child lives")
        self.assertFalse(wait_calls(2, deadline=3.0),
                         "a second fake Codex must not start")

        # The captured thread ID must be durable.
        job = core.get_job(sd, req)
        self.assertEqual(job.get("codex_task_id"), thread_id)

        # Cleanup: stop the surviving fake Codex and any controller remnants.
        for r in rows():
            kill_pgid(r["pgid"])
        for r in rows():
            wait_pgid_dead(r["pgid"])
        try:
            os.kill(int(controller_pid), 0)
        except ProcessLookupError:
            pass
        else:
            try:
                os.kill(int(controller_pid), signal.SIGKILL)
            except Exception:
                pass

    def test_cancel_while_child_lives_blocks_replacement_until_dead(self):
        """Regression: cancel must stop the child before releasing the workspace.

        Cancelling while a fake Codex invocation is still running must signal
        the child process group, wait for it to die, and only then mark the job
        terminal. A same-workspace replacement must not start until the first
        writer is gone, so two fake Codex processes are never alive together.
        """
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        fake_bin = base / "fakebin"
        fake_bin.mkdir()
        fake_state = base / "fakestate"
        fake_state.mkdir()
        calls = fake_state / "codex-calls.jsonl"
        thread_id = "cancel-repro-thread-001"

        fake_codex = '''#!/usr/bin/env python3
import json, os, time
from pathlib import Path
p = Path(os.environ["CRASH_REPRO_CALLS"])
with p.open("a") as f:
    f.write(json.dumps({"pid": os.getpid(), "pgid": os.getpgid(0),
                        "args": __import__("sys").argv[1:]}) + "\\n")
print(json.dumps({"type": "thread.started", "thread_id": "%s"}), flush=True)
time.sleep(60)
''' % thread_id
        p = fake_bin / "codex"
        p.write_text(fake_codex, encoding="utf-8")
        os.chmod(p, 0o755)
        env = dict(os.environ)
        env["PATH"] = str(fake_bin) + os.pathsep + env.get("PATH", "")
        env["CRASH_REPRO_CALLS"] = str(calls)
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        def rows():
            if not calls.exists():
                return []
            return [json.loads(line) for line in calls.read_text().splitlines() if line.strip()]

        def wait_calls(count, deadline=15.0):
            end = time.time() + deadline
            while time.time() < end:
                if len(rows()) >= count:
                    return True
                time.sleep(0.05)
            return False

        def is_pid_alive(pid):
            try:
                os.kill(int(pid), 0)
            except ProcessLookupError:
                return False
            except (PermissionError, OSError):
                return True
            return True

        def kill_pgid(pgid):
            try:
                os.killpg(int(pgid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass

        def wait_pgid_dead(pgid, deadline=5.0):
            end = time.time() + deadline
            while time.time() < end:
                try:
                    os.killpg(int(pgid), 0)
                except ProcessLookupError:
                    return True
                except (PermissionError, OSError):
                    return False
                time.sleep(0.05)
            return False

        req = "cancel-repro"
        rc, out, err = self._cli(
            sd, "submit", "--request-id", req,
            "--task", json.dumps({"goal": "cancellation reproduction"}),
            "--workspace", str(ws), "--planner-session", "original-planner",
            "--planner-model", "claude-sonnet-5", "--planner-effort", "medium",
            "--start", env=env)
        self.assertEqual(rc, 0, err[-2000:])
        self.assertTrue(out.get("acknowledged"))
        self.assertTrue(wait_calls(1), "first fake Codex did not launch")
        first_pid = rows()[0]["pid"]
        first_pgid = rows()[0]["pgid"]

        # Cancel while the fake Codex child is still running.
        rc, out, err = self._cli(sd, "cancel", "--request-id", req, env=env)
        self.assertEqual(rc, 0, err[-2000:])
        # cancel must wait for the child process group to stop before returning.
        self.assertFalse(is_pid_alive(first_pid),
                         "cancel must stop the fake Codex child before returning")
        job = core.get_job(sd, req)
        self.assertEqual(job["status"], "cancelled",
                         "cancel must only mark terminal after child is dead")

        # A same-workspace replacement may now start; the first writer is gone.
        rc, out, err = self._cli(
            sd, "submit", "--request-id", "replacement",
            "--task", json.dumps({"goal": "replacement after cancel"}),
            "--workspace", str(ws), "--planner-session", "replacement-planner",
            "--planner-model", "claude-sonnet-5", "--planner-effort", "medium",
            "--start", env=env)
        self.assertEqual(rc, 0, err[-2000:])
        self.assertTrue(wait_calls(2), "replacement fake Codex did not launch")

        # Prove no duplicate writer: the first child was dead before the second started.
        second_pid = rows()[1]["pid"]
        self.assertNotEqual(first_pid, second_pid)
        # (The cleanup below stops the replacement; the assertion above already
        # proves the first died before this point because cancel waited.)

        # Cleanup.
        for r in rows():
            kill_pgid(r["pgid"])
        for r in rows():
            wait_pgid_dead(r["pgid"])

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
