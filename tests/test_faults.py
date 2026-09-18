"""Public-path subprocess faults, capacity, and owned OpenCode serve.

Deterministic fakes only. No live model CLIs.
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

from runner import core, policy, store  # noqa: E402
from runner.core import _is_pid_alive  # noqa: E402

PY = sys.executable


def cli(state_dir, *args, env=None, timeout=25):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(ROOT), env=env)
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(fn, secs=12.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if fn():
            return True
        time.sleep(0.05)
    return False


def kill_pid(pid):
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        return


def alive(pid):
    return _is_pid_alive(pid)


class TestCompletedChildBeforeRecover(unittest.TestCase):
    def test_recover_consumes_finished_child_exactly_once(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        calls = base / "calls.jsonl"
        release = base / "finish-child"
        fake = bindir / "codex"
        fake.write_text("#!" + PY + "\n" + r"""
import json, os, pathlib, sys, time
p = pathlib.Path(os.environ["REPRO_CALLS"])
n = 1 + (len(p.read_text().splitlines()) if p.exists() else 0)
with p.open("a") as f:
    f.write(json.dumps({"pid": os.getpid(), "pgid": os.getpgid(0), "n": n}) + "\n")
print(json.dumps({"type": "thread.started", "thread_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"}), flush=True)
end = time.monotonic() + 20
while n == 1 and not pathlib.Path(os.environ["REPRO_RELEASE"]).exists() and time.monotonic() < end:
    time.sleep(0.05)
a = {"action": "completion", "output": "FIRST_COMPLETION" if n == 1 else "REPLAYED_COMPLETION", "artifact": ""}
if "--output-last-message" in sys.argv:
    pathlib.Path(sys.argv[sys.argv.index("--output-last-message") + 1]).write_text(json.dumps(a))
print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(a)}}), flush=True)
print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 0, "output_tokens": 0}}), flush=True)
""")
        fake.chmod(0o700)
        env = dict(os.environ)
        env["PATH"] = str(bindir) + os.pathsep + env.get("PATH", "")
        env["REPRO_CALLS"] = str(calls)
        env["REPRO_RELEASE"] = str(release)
        env["PYTHONDONTWRITEBYTECODE"] = "1"

        rc, out, err = cli(sd, "submit", "--request-id", "completed-child",
                           "--task", '{"goal":"offline completion fault"}',
                           "--workspace", str(ws), "--planner-session", "fake-planner",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: calls.exists() and core.get_job(sd, "completed-child").get("codex_task_id")))
        first = core.get_job(sd, "completed-child")
        controller_pid = first["owner_pid"]
        self.assertIsNotNone(controller_pid)
        os.kill(int(controller_pid), signal.SIGKILL)
        release.write_text("finish")
        rows = lambda: [json.loads(l) for l in calls.read_text().splitlines()] if calls.exists() else []
        self.assertTrue(wait_for(lambda: rows() and not alive(rows()[0]["pid"])))
        rc, rec, err = cli(sd, "recover", "--request-id", "completed-child", env=env)
        self.assertEqual(rc, 0, err)
        job = core.get_job(sd, "completed-child")
        self.assertEqual(job["status"], "succeeded", rec)
        self.assertIn("FIRST_COMPLETION", job.get("result_json") or "")
        self.assertNotIn("REPLAYED_COMPLETION", job.get("result_json") or "")
        rc, start_out, err = cli(sd, "start", "--request-id", "completed-child", env=env)
        self.assertNotEqual(rc, 0)
        self.assertEqual(len(rows()), 1, rows())


class TestNullPidWindow(unittest.TestCase):
    def test_null_pid_is_unresolved_not_death(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "null-pid", {"goal": "t"}, str(ws), "planner-1")
        root = store.ensure_state_dir(sd)
        outp = root / "outputs" / "null-pid.inv.stdout"
        errp = root / "outputs" / "null-pid.inv.stderr"
        store.secure_write_text(outp, "")
        store.secure_write_text(errp, "")
        con = store.connect(sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json, workspace,"
                " owner_token, pid, pgid, stdout_path, stderr_path, started_at, state, task_json)"
                " VALUES(?,?,?,?,?,?,NULL,NULL,?,?,?,'running',?)",
                ("deadbeefdeadbeef", "null-pid", "codex_dispatch", json.dumps(["codex", "exec"]),
                 str(ws), "token", str(outp), str(errp), core._utcnow(), "{}"),
            )
        finally:
            con.close()
        rec = core.recover_one(sd, "null-pid")
        self.assertEqual(rec.get("action"), "blocked-unresolved-invocation")
        self.assertEqual(core.get_job(sd, "null-pid")["status"], "blocked")
        with self.assertRaises((core.OwnershipError, core.BlockedError)):
            core.start_controller(sd, "null-pid", spawn=lambda cmd: 1)


class TestAnswerRecoverContinues(unittest.TestCase):
    def test_answer_recover_reaches_completion_after_controller_kill(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        fake_state = base / "fakestate"
        fake_state.mkdir()
        thread = "answer-recover-thread"
        planner = "planner-ar-1"
        (bindir / "codex").write_text("#!" + PY + "\n" + r"""
import json, os, sys, time
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
log = st / "codex.log"
argv = sys.argv[1:]
log.open("a").write(json.dumps(argv) + "\n")
tid = os.environ["THREAD_ID"]
count_f = st / "n"
n = int(count_f.read_text()) if count_f.exists() else 0
n += 1
count_f.write_text(str(n))
if argv[:2] == ["exec", "resume"]:
    env = {"thread_id": tid, "action": "implementation", "artifact": "a.txt", "payload": {}}
    if n >= 3:
        env = {"thread_id": tid, "action": "completion", "output": "ANSWER_RECOVER_DONE"}
else:
    env = {"thread_id": tid, "action": "planner_question", "qid": "q-ar", "prompt": "Confirm?"}
    time.sleep(0.4)
lp = None
if "--output-last-message" in argv:
    lp = argv[argv.index("--output-last-message") + 1]
    Path(lp).write_text(json.dumps(env))
print(json.dumps({"type": "thread.started", "thread_id": tid}), flush=True)
print(json.dumps(env), flush=True)
""")
        (bindir / "codex").chmod(0o700)
        (bindir / "claude").write_text(
            "#!" + PY + "\nimport sys,time\ntime.sleep(0.2)\nsys.exit(1)\n")
        (bindir / "claude").chmod(0o700)
        (bindir / "opencode").write_text("#!" + PY + "\n" + r"""
import json, os, sys, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
from pathlib import Path
argv = sys.argv[1:]
Path(os.environ["FAKE_STATE"], "opencode.log").open("a").write(json.dumps(argv)+"\n")
if argv and argv[0] == "serve":
    pwd = os.environ.get("OPENCODE_SERVER_PASSWORD") or ""
    class H(BaseHTTPRequestHandler):
        def _json(self, code, obj):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def do_GET(self):
            self._json(200, {"session": "oc-ar", "status": "idle"})
        def do_POST(self):
            path = urlparse(self.path).path
            if path == "/session":
                return self._json(200, {"id": "oc-ar"})
            self._json(200, {"ok": True, "session": "oc-ar"})
        def log_message(self, *a):
            return
    srv = HTTPServer(("127.0.0.1", 0), H)
    print("http://127.0.0.1:%s" % srv.server_address[1], flush=True)
    srv.serve_forever(poll_interval=0.05)
print(json.dumps({"opencode_session_id": "oc-ar", "ok": True}))
""")
        (bindir / "opencode").chmod(0o700)
        env = dict(os.environ)
        env["PATH"] = str(bindir) + os.pathsep + env.get("PATH", "")
        env["FAKE_STATE"] = str(fake_state)
        env["THREAD_ID"] = thread
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        rc, out, err = cli(sd, "submit", "--request-id", "ar1",
                           "--task", '{"goal":"answer recover"}',
                           "--workspace", str(ws), "--planner-session", planner,
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: bool(core.list_questions(sd, "ar1")), 15))
        job = core.get_job(sd, "ar1")
        kill_pid(job["owner_pid"])
        # Wait until callback children exit. Do not kill an unresolved
        # spawn window: that freezes NULL pid ownership.
        self.assertTrue(wait_for(lambda: all(
            core._invocation_ownership(i) not in ("live", "unresolved")
            for i in core._list_invocations(sd, "ar1")), 10))
        rc, _, err = cli(sd, "answer", "--request-id", "ar1", "--qid", "q-ar",
                         "--answer", "Approved.", env=env)
        self.assertEqual(rc, 0, err)
        rc, rec, err = cli(sd, "recover", "--request-id", "ar1", env=env)
        self.assertEqual(rc, 0, err)
        self.assertIn(rec.get("action"), ("resumed-controller", "consumed-completion"))
        self.assertTrue(wait_for(lambda: core.get_job(sd, "ar1")["status"] in
                                 ("succeeded", "failed", "blocked"), 20))
        job = core.get_job(sd, "ar1")
        self.assertEqual(job["status"], "succeeded", job.get("block_reason"))
        self.assertEqual(job["codex_task_id"], thread)
        self.assertIn("ANSWER_RECOVER_DONE", job.get("result_json") or "")


class TestCapacityPolicy(unittest.TestCase):
    def test_exhausted_route_not_retried_across_jobs(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "c1", {"g": 1}, str(ws), "p1")
        core.record_capacity(sd, "muse-spark-xhigh-free", "exhausted",
                             {"class": "FreeUsageLimitError"})
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(sd))
        nxt, blocker = core.select_implementation_route(
            sd, "muse-spark-xhigh-free",
            {"class": "FreeUsageLimitError"})
        self.assertEqual(nxt, "muse-spark-xhigh-go")
        self.assertIsNone(blocker)
        # Duplicate recover must not reset capacity memory.
        core.recover_one(sd, "c1")
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(sd))
        core.submit(sd, "c2", {"g": 2}, str(Path(tmp.name) / "ws2"), "p1")
        nxt2, blocker2 = policy.next_capacity_route(
            "muse-spark-xhigh-free", core.exhausted_routes(sd))
        self.assertEqual(nxt2, "muse-spark-xhigh-go")
        self.assertIsNone(blocker2)
        # Unavailable next eligible route is a precise blocker, not operational.
        nxt3, blocker3 = policy.next_capacity_route("muse-spark-xhigh-go", set())
        self.assertEqual(nxt3, "go-deepseek-v4.1-flash")
        self.assertTrue(blocker3)
        self.assertIn("no live-exercised adapter", blocker3)
        self.assertFalse(policy.is_operational("go-deepseek-v4.1-flash"))
        self.assertFalse(policy.is_operational("terra/max"))
        self.assertFalse(policy.is_operational("kimi-k2.7-code"))
        self.assertFalse(policy.is_operational("grok-4.6/medium"))
        self.assertIsNone(policy.route_blocker("muse-spark-xhigh-free"))
        # Unknown reset stays unknown; no invented daily reset.
        core.record_capacity(sd, "muse-spark-xhigh-go", "exhausted",
                             {"class": "GoUsageLimitError"}, reset_at=None)
        row = None
        con = store.connect(sd)
        try:
            row = dict(con.execute("SELECT * FROM capacity WHERE route=?",
                                   ("muse-spark-xhigh-go",)).fetchone())
        finally:
            con.close()
        self.assertIsNone(row["reset_at"])
        self.assertEqual(core._trusted_reset_at({"message": "try tomorrow"}), None)


class TestOwnedOpenCodeServe(unittest.TestCase):
    def test_fake_serve_creates_session_without_leaking_password(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        fake_state = base / "fakestate"
        fake_state.mkdir()
        (bindir / "opencode").write_text("#!" + PY + "\n" + r"""
import json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
from pathlib import Path
argv = sys.argv[1:]
st = Path(os.environ["FAKE_STATE"])
st.mkdir(exist_ok=True)
(st / "argv.jsonl").open("a").write(json.dumps(argv) + "\n")
assert argv[0] == "serve"
assert "password" not in " ".join(argv).lower()
pwd = os.environ["OPENCODE_SERVER_PASSWORD"]
(st / "pwd-in-argv").write_text("yes" if pwd in " ".join(argv) else "no")
class H(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        auth = self.headers.get("Authorization") or ""
        if auth != "Bearer " + pwd:
            return self._json(401, {"error": "unauthorized"})
        self._json(200, {"session": "oc-owned", "status": "idle",
                         "action": {"reason": "free_tier_limit", "provider": "opencode"}})
    def do_POST(self):
        auth = self.headers.get("Authorization") or ""
        if auth != "Bearer " + pwd:
            return self._json(401, {"error": "unauthorized"})
        path = urlparse(self.path).path
        if path == "/session":
            return self._json(200, {"id": "oc-owned"})
        if path == "/session/abort":
            (st / "aborted").write_text("1")
            return self._json(200, {"ok": True})
        return self._json(200, {"ok": True})
    def log_message(self, *a):
        return
srv = HTTPServer(("127.0.0.1", 0), H)
print("http://127.0.0.1:%s" % srv.server_address[1], flush=True)
srv.serve_forever(poll_interval=0.05)
""")
        (bindir / "opencode").chmod(0o700)
        old_path = os.environ.get("PATH", "")
        os.environ["PATH"] = str(bindir) + os.pathsep + old_path
        os.environ["FAKE_STATE"] = str(fake_state)
        self.addCleanup(lambda: os.environ.__setitem__("PATH", old_path))
        core.submit(sd, "oc1", {"goal": "owned serve"}, str(ws), "planner")
        job = core.get_job(sd, "oc1")
        run = core.make_durable_run_cmd(sd, "oc1", job.get("owner_token") or "tok")
        from runner import adapters
        rc, out, err = run(adapters.build_opencode_serve_cmd(), str(ws), 20,
                           kind="opencode_control",
                           meta={"prompt": "implement", "allowance": "free"})
        blob = out + err
        self.assertNotIn("OPENCODE_SERVER_PASSWORD", blob)
        con = store.connect(sd)
        try:
            db = json.dumps([dict(r) for r in con.execute("SELECT * FROM invocations").fetchall()])
            ev = json.dumps([dict(r) for r in con.execute("SELECT * FROM events").fetchall()])
        finally:
            con.close()
        self.assertNotIn("OPENCODE_SERVER_PASSWORD", db + ev)
        self.assertNotIn("Bearer ", db + ev)
        self.assertIn("oc-owned", (core.get_job(sd, "oc1").get("opencode_session_id") or "") + blob)
        argv = json.loads((fake_state / "argv.jsonl").read_text().splitlines()[0])
        self.assertNotIn("password", " ".join(argv).lower())
        self.assertEqual((fake_state / "pwd-in-argv").read_text().strip(), "no")
        for inv in core._list_invocations(sd, "oc1"):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass


class TestCancelTimeoutOwnChildren(unittest.TestCase):
    def test_timeout_stops_child_group(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        calls = base / "calls.jsonl"
        (bindir / "codex").write_text("#!" + PY + "\n" + r"""
import json, os, time
from pathlib import Path
p = Path(os.environ["CALLS"])
p.open("a").write(json.dumps({"pid": os.getpid(), "pgid": os.getpgid(0)}) + "\n")
print(json.dumps({"type":"thread.started","thread_id":"to-thread"}), flush=True)
time.sleep(60)
""")
        (bindir / "codex").chmod(0o700)
        env = dict(os.environ)
        env["PATH"] = str(bindir) + os.pathsep + env.get("PATH", "")
        env["CALLS"] = str(calls)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        rc, out, err = cli(sd, "submit", "--request-id", "to1",
                           "--task", '{"goal":"timeout"}', "--workspace", str(ws),
                           "--planner-session", "p", "--timeout-secs", "1",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: calls.exists()))
        child = json.loads(calls.read_text().splitlines()[0])
        time.sleep(1.2)
        rc, rec, err = cli(sd, "recover", "--request-id", "to1", env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: not alive(child["pid"]), secs=8))
        job = core.get_job(sd, "to1")
        self.assertIn(job["status"], ("failed", "blocked", "cancelling"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
