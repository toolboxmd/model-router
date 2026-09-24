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
from tests.fakes import VERSION_GUARD  # noqa: E402

from runner import core, harnesses, store  # noqa: E402
from runner.core import _is_pid_alive  # noqa: E402

PY = sys.executable

THREAD_ID = "thread-loop-001"
PLANNER_SID = "claude-planner-loop-001"
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
    assert argv[argv.index("-m") + 1] == "gpt-5.6-luna", "resume must name Luna"
    assert 'model_reasoning_effort="max"' in argv, "resume must pass max effort"
    assert 'sandbox_mode="read-only"' in argv, "resume must stay read-only"
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
    print(json.dumps({"type": "turn.completed"}))
else:
    # Fresh dispatch contract.
    assert "--json" in argv, "dispatch must pass --json"
    assert "--output-last-message" in argv, "dispatch must pass --output-last-message"
    assert "--model" in argv and "%s" in argv, "dispatch model"
    blob = " ".join(argv)
    assert "model_reasoning_effort" in blob and "max" in blob, "dispatch reasoning effort"
    assert "--sandbox" in argv and "read-only" in argv, "dispatch sandbox"
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
    print(json.dumps({"type": "turn.completed"}))
""" % (THREAD_ID, THREAD_ID, "gpt-5.6-luna", THREAD_ID, QID)

from tests.fakes import FAKE_CLAUDE as _FC, FAKE_OPENCODE as _FO  # noqa: E402

FAKE_CLAUDE = "#!" + PY + "\n" + _FC
FAKE_OPENCODE = "#!" + PY + "\n" + _FO


def wait_for_dead(pid, secs=15.0):
    end = time.time() + secs
    while time.time() < end:
        if not _is_pid_alive(pid):
            return True
        time.sleep(0.05)
    return False


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
            shebang, _, rest = body.partition("\n")
            p.write_text(shebang + "\n" + VERSION_GUARD + rest, encoding="utf-8")
            os.chmod(p, 0o755)
        env = dict(os.environ)
        env["PATH"] = str(fake_bin) + os.pathsep + env.get("PATH", "")
        env["DURABLE_FAKE_STATE"] = str(fake_state)
        env["FAKE_STATE"] = str(fake_state)
        env["FAKE_OC_WRITE"] = "fix.txt"

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
        claude_calls = [c["argv"] for c in read_log("claude.log")]
        oc_calls = read_log("opencode.log")

        # One Luna fresh dispatch (exec without resume) + exact-ID resumes.
        dispatches = [c for c in codex_calls
                      if len(c) > 1 and c[0] == "exec" and c[1] != "resume"]
        resumes = [c for c in codex_calls
                   if len(c) > 1 and c[0] == "exec" and c[1] == "resume"]
        self.assertEqual(len(dispatches), 1, codex_calls)
        self.assertEqual(len(resumes), 2, codex_calls)
        for r in resumes:
            self.assertIn(THREAD_ID, r)
        self.assertEqual(len(codex_calls), len(dispatches) + len(resumes))

        # One Claude --resume of the exact original planner ID, run in
        # the planner's directory (default: the workspace). Submit never
        # touches the planner session: the only Claude turn is a callback.
        self.assertEqual(len(claude_calls), 1, claude_calls)
        for c in claude_calls:
            self.assertEqual(harnesses.kind_for_cmd(["claude"] + list(c)),
                             "claude_callback")
            self.assertIn("--resume", c)
            self.assertIn(PLANNER_SID, c)
        self.assertIn("HANDOFF SUMMARY", " ".join(str(a) for a in claude_calls[0]))
        self.assertIn(req, " ".join(str(a) for a in claude_calls[0]))
        self.assertEqual(read_log("claude.log")[0]["cwd"], os.path.realpath(ws))

        # Owned ephemeral OpenCode server with the real API contract.
        self.assertEqual([c[0] for c in oc_calls], ["serve"], oc_calls)
        self.assertNotIn("password", " ".join(oc_calls[0]).lower())
        self.assertEqual((fake_state / "pwd-in-argv").read_text(), "no")
        reqs = read_log("opencode-requests.jsonl")
        self.assertTrue(reqs and all(r["auth_ok"] for r in reqs), reqs)
        creates = [r for r in reqs if r["method"] == "POST" and r["path"] == "/session"]
        self.assertEqual(len(creates), 1)
        rules = {(x["permission"], x["action"]) for x in creates[0]["body"]["permission"]}
        # Full access by default; question and task stay denied.
        self.assertIn(("external_directory", "allow"), rules)
        self.assertIn(("webfetch", "allow"), rules)
        self.assertIn(("websearch", "allow"), rules)
        self.assertIn(("doom_loop", "allow"), rules)
        self.assertIn(("task", "deny"), rules)
        self.assertIn(("question", "deny"), rules)
        prompts = [r for r in reqs if r["path"].endswith("/prompt_async")]
        self.assertEqual(len(prompts), 1)
        self.assertEqual(prompts[0]["body"]["model"],
                         {"providerID": "opencode", "modelID": "muse-spark-1.3-contributor-free"})
        self.assertEqual(prompts[0]["body"]["variant"], "xhigh")
        self.assertEqual(prompts[0]["body"]["agent"], "build")
        self.assertTrue(all(r["query"].get("directory") == job["workspace"] for r in reqs
                            if r["path"] != "/global/health"), reqs)
        self.assertTrue((ws / "fix.txt").exists())

        # Durable question then answer then completion.
        qs = core.list_questions(sd, req, only_pending=False)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0]["qid"], QID)
        self.assertEqual(qs[0]["status"], "answered")
        self.assertIn("Approved", qs[0]["answer"] or "")
        self.assertTrue(job["opencode_session_id"].startswith("ses_"))
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
        self.assertIn("opencode_control", kinds)
        # Every model child ran under a supervisor and was collected once.
        invs = core._list_invocations(sd, req)
        self.assertEqual(sorted(i["kind"] for i in invs),
                         ["claude_callback", "codex_dispatch",
                          "codex_resume", "codex_resume", "opencode_control"])
        self.assertTrue(all(i["state"] == "completed" and i["consumed_at"] for i in invs))

        # Recover/restart must not fork a second planner session or writer.
        n_claude_before = len(read_log("claude.log"))
        n_codex_before = len(codex_calls)
        rc, _, _ = self._cli(sd, "recover", "--request-id", req, env=env)
        self.assertEqual(rc, 0)
        time.sleep(0.5)
        codex_after = read_log("codex.log")
        claude_after = read_log("claude.log")  # noqa: F841
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
import sys as _vs
if _vs.argv[1:2] == ['--version']:
    print('fake-harness 0.0.0'); raise SystemExit(0)
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
        self.assertEqual(rec.get("action"), "adopted-live-invocation")
        # The adopting controller waits on the same action; it never
        # launches a second Codex task.

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
        adopter = core.get_job(sd, req).get("owner_pid")
        if adopter:
            self.assertTrue(wait_for_dead(adopter), "adopting controller must stop after the child fails")
        self.assertFalse(wait_calls(2, deadline=1.0),
                         "a failed dispatch must not be replayed as a new task")
        self.assertEqual(core.get_job(sd, req)["status"], "blocked")

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
import sys as _vs
if _vs.argv[1:2] == ['--version']:
    print('fake-harness 0.0.0'); raise SystemExit(0)
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
