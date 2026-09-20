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

from runner import controller, core, policy, store  # noqa: E402
from tests.fakes import FAKE_CLAUDE, FAKE_OPENCODE, VERSION_GUARD, write_fake  # noqa: E402
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
        fake.write_text("#!" + PY + "\n" + VERSION_GUARD + r"""
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
        (bindir / "codex").write_text("#!" + PY + "\n" + VERSION_GUARD + r"""
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
print(json.dumps({"type": "turn.completed"}), flush=True)
""")
        (bindir / "codex").chmod(0o700)
        (bindir / "claude").write_text(
            "#!" + PY + "\nimport sys,time\ntime.sleep(0.2)\nsys.exit(1)\n")
        (bindir / "claude").chmod(0o700)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
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
        free_status = {"type": "retry", "attempt": 1, "message": "m", "next": 1,
                       "action": {"reason": "free_tier_limit", "provider": "opencode",
                                  "title": "t", "message": "m", "label": "l"}}
        core.record_capacity(sd, "muse-spark-xhigh-free", "exhausted", free_status)
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(sd))
        nxt, blocker = core.select_implementation_route(
            sd, "muse-spark-xhigh-free", free_status)
        self.assertEqual(nxt, "muse-spark-xhigh-go")
        self.assertIsNone(blocker)
        # The stored lane decides where a shared route continues.
        self.assertEqual(core.select_implementation_route(sd, "muse-spark-xhigh-go", free_status,
                                                          lane="implementation_hard"),
                         ("glm-5.3-go", None))
        # Duplicate recover must not reset capacity memory.
        core.recover_one(sd, "c1")
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(sd))
        (Path(tmp.name) / "ws2").mkdir()
        core.submit(sd, "c2", {"g": 2}, str(Path(tmp.name) / "ws2"), "p1")
        nxt2, blocker2 = policy.next_capacity_route(
            "muse-spark-xhigh-free", core.exhausted_routes(sd))
        self.assertEqual(nxt2, "muse-spark-xhigh-go")
        self.assertIsNone(blocker2)
        # After Go Muse the default lane continues on another family; every
        # policy route has an adapter, so there is no blocker, and the lane
        # ends with (None, None) instead of an inoperable placeholder.
        nxt3, blocker3 = policy.next_capacity_route("muse-spark-xhigh-go", set())
        self.assertEqual(nxt3, "glm-5.3-flash-go")
        self.assertIsNone(blocker3)
        self.assertEqual(policy.next_capacity_route("glm-5.1-go", set()), (None, None))
        self.assertTrue(policy.is_operational("glm-5.1-go"))
        for gone in ("go-deepseek-v4.1-flash", "kimi-k2.7-code", "grok-4.6/medium",
                     "kimi-k3-go"):
            self.assertFalse(policy.is_operational(gone))
            self.assertIn("unsupported route", policy.route_blocker(gone))
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
    """Owned ``opencode serve`` against a fake of the real server API."""

    def _setup(self, mode, timeout_secs=None):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        self.sd = str(base / "state")
        self.ws = base / "ws"
        self.ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        self.fake_state = base / "fakestate"
        self.fake_state.mkdir()
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_OC_MODE")}

        def restore():
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.addCleanup(restore)
        os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
        os.environ["FAKE_STATE"] = str(self.fake_state)
        os.environ["FAKE_OC_MODE"] = mode
        core.submit(self.sd, "oc1", {"goal": "owned serve"}, str(self.ws), "planner")
        con = store.connect(self.sd)  # hold the lease as a controller would
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='oc1'")
        finally:
            con.close()
        self.addCleanup(self._kill_groups)
        run = core.make_durable_run_cmd(self.sd, "oc1", "tok")

        def timed_run(cmd, cwd=None, timeout=None, **kw):
            return run(cmd, cwd, timeout_secs or timeout, **kw)
        return timed_run

    def test_route_params_flow_from_policy_to_owned_server(self):
        run = self._setup("ok")
        for route, expect_model, expect_variant in (
                ("muse-spark-xhigh-free", {"providerID": "opencode", "modelID": "muse-spark-1.3-contributor-free"}, "xhigh"),
                ("muse-spark-xhigh-go", {"providerID": "opencode-go", "modelID": "muse-spark-1.3-contributor"}, "xhigh"),
                ("glm-5.3-flash-go", {"providerID": "opencode-go", "modelID": "glm-5.3-flash"}, None),
                ("kimi-k2.7-code-go", {"providerID": "opencode-go", "modelID": "kimi-k2.7-code"}, None),
                ("grok-4.6-go", {"providerID": "opencode-go", "modelID": "grok-4.6"}, "medium"),
                ("grok-4.6-xai", {"providerID": "xai", "modelID": "grok-4.6"}, "medium")):
            con = store.connect(self.sd)
            try:
                con.execute("UPDATE jobs SET route=?, status='running' WHERE request_id='oc1'", (route,))
            finally:
                con.close()
            res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
            self.assertEqual(res["action"], "implementation_ok", route)
            body = [r for r in self._requests() if r["path"].endswith("/prompt_async")][-1]["body"]
            self.assertEqual(body["model"], expect_model)
            self.assertEqual(body.get("variant"), expect_variant)
            self.assertEqual(body["agent"], "build")
            job = core.get_job(self.sd, "oc1")
            self.assertEqual(job["model"], policy.opencode_route_params(route)[0])
            self.assertEqual(job["effort"], expect_variant or "default")
        self._no_secret_leak()

    def _go_mode(self, mode):
        prev = os.environ.get("FAKE_OC_MODE_GO")
        os.environ["FAKE_OC_MODE_GO"] = mode

        def restore():
            if prev is None:
                os.environ.pop("FAKE_OC_MODE_GO", None)
            else:
                os.environ["FAKE_OC_MODE_GO"] = prev
        self.addCleanup(restore)

    def _set_route(self, route):
        # Keep the job's stored lane consistent with the route, as submit does.
        lane = policy.lane_of_route(route)
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET route=?, lane=?, status='running' WHERE request_id='oc1'", (route, lane))
        finally:
            con.close()

    def _insert_completed_turn(self, inv_id, seq=0):
        root = store.ensure_state_dir(self.sd)
        stdout = root / "outputs" / f"{inv_id}.stdout"
        stderr = root / "outputs" / f"{inv_id}.stderr"
        store.secure_write_text(stdout, "")
        store.secure_write_text(stderr, "")
        con = store.connect(self.sd)
        try:
            con.execute(
                "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,owner_token,"
                "pid,pgid,process_start,stdout_path,stderr_path,started_at,state,timeout_secs,"
                "action_key,meta_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (inv_id, "oc1", "opencode_control", json.dumps(["true"]), str(self.ws), "tok",
                 None, None, None, str(stdout), str(stderr), core._utcnow(), "completed", 5,
                 None, json.dumps({"seq": seq})),
            )
        finally:
            con.close()

    def test_one_turn_route_is_refused_a_second_turn(self):
        run = self._setup("ok")
        self._set_route("glm-5.3-go")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='oc1'",
                        (json.dumps({"seq": 1, "last_action": {"action": "implementation"}}),))
        finally:
            con.close()
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        # The one-turn mark now yields a lateral move before dispatch, not a block.
        self.assertEqual((res2["action"], res2["reason"], res2["route"]),
                         ("route_switched", "preflight_one_turn", "deepseek-v4-pro-go"))
        prompts = [r for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual([p["body"]["model"]["modelID"] for p in prompts], ["glm-5.3"])
    def _capacity(self, route):
        rows = [r for r in core.list_capacity(self.sd) if r["route"] == route]
        return rows[0] if rows else None

    def test_overload_moves_to_next_family_within_60s(self):
        run = self._setup("overloaded")
        started = time.monotonic()
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        elapsed = time.monotonic() - started
        self.assertEqual(res["action"], "route_switched")
        self.assertEqual(res["reason"], "lateral")
        self.assertLess(elapsed, 60.0)
        job = core.get_job(self.sd, "oc1")
        # Same family (Go Muse) is skipped; the next family in the default lane is GLM 5.3 Flash.
        self.assertEqual(job["route"], "glm-5.3-flash-go")
        self.assertTrue((self.fake_state / "aborted").exists())
        self.assertIn('"signal": "overloaded"', job["last_error_json"])
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        cap = self._capacity("muse-spark-xhigh-free")
        self.assertEqual((cap["state"], cap["pool"], cap["window"]), ("degraded", "zen-free", "cooldown"))
        self.assertTrue(cap["reset_at"])
        # The next turn runs on GLM 5.3 Flash in the same saved session.
        session = job["opencode_session_id"]
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        prompts = [r["body"]["model"] for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual([m["providerID"] for m in prompts], ["opencode", "opencode-go"])
        self.assertEqual(prompts[-1]["modelID"], "glm-5.3-flash")
        self.assertEqual(core.get_job(self.sd, "oc1")["opencode_session_id"], session)
        self._no_secret_leak()

    def test_go_exhaustion_moves_the_same_model_to_xai(self):
        run = self._setup("ok")
        self._go_mode("go_limit")
        self._set_route("grok-4.6-go")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "pool_move"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "grok-4.6-xai")
        self.assertIn("grok-4.6-go", core.exhausted_routes(self.sd))
        cap = self._capacity("grok-4.6-go")
        self.assertEqual((cap["state"], cap["pool"], cap["model"]), ("exhausted", "go", "opencode-go/grok-4.6"))
        self.assertIn("GoUsageLimitError", cap["evidence_json"])
        # No provider reset: the 5-hour default is assumed and flagged, so
        # preflight skips the route until it passes instead of forever.
        self.assertIsNotNone(cap["reset_at"])
        self.assertEqual(cap["reset_source"], "assumed")
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        prompts = [r["body"]["model"] for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual([m["providerID"] for m in prompts], ["opencode-go", "xai"])
        self._no_secret_leak()

    def test_exhaustion_without_next_route_blocks_with_reason(self):
        run = self._setup("ok")
        self._go_mode("go_limit")
        self._set_route("glm-5.1-go")  # last route of the default lane, no next pool
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res["action"], res["reason"]), ("blocked", "capacity_exhausted"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("capacity_exhausted", job["block_reason"])
        self.assertEqual(job["route"], "glm-5.1-go")
        self.assertIn("glm-5.1-go", core.exhausted_routes(self.sd))

    def test_hard_lane_moves_stay_in_the_hard_lane_and_respect_one_turn(self):
        run = self._setup("overloaded")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET lane='implementation_hard', status='running' WHERE request_id='oc1'")
        finally:
            con.close()
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        # Overloaded free Muse in the hard lane skips the Muse family and
        # moves to GLM 5.3, not GLM 5.3 Flash (which is a different lane).
        self.assertEqual((res["action"], res["route"]), ("route_switched", "glm-5.3-go"))
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        # The dispatcher asks for another turn (new seq). A second turn on the
        # one-turn route is refused before dispatch: the job moves to the next
        # family in the hard lane without contacting the provider.
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='oc1'",
                        (json.dumps({"seq": 1, "last_action": {"action": "implementation"}}),))
        finally:
            con.close()
        res3 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res3["action"], res3["reason"], res3["route"]),
                         ("route_switched", "preflight_one_turn", "deepseek-v4-pro-go"))
        prompts = [r["body"]["model"]["modelID"] for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual(prompts.count("glm-5.3"), 1)

    def test_preflight_skips_degraded_and_exhausted_routes(self):
        run = self._setup("ok")
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "degraded",
                             {"source": "test", "class": "overloaded"}, reset_at=core.degraded_until())
        core.record_capacity(self.sd, "muse-spark-xhigh-go", "exhausted", {"source": "test"})
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "preflight_degraded"))
        self.assertEqual(core.get_job(self.sd, "oc1")["route"], "glm-5.3-flash-go")
        self.assertEqual(self._requests(), [])  # nothing was dispatched to the resting routes
        # Cooldown in the past means the route is eligible again.
        core.record_capacity(self.sd, "glm-5.3-flash-go", "degraded", {"source": "test"},
                             reset_at="2000-01-01T00:00:00+00:00")
        self.assertNotIn("glm-5.3-go", core.degraded_routes(self.sd))
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        self._no_secret_leak()

    def test_turn_report_and_invocation_measurements(self):
        run = self._setup("ok")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET task_json=?, status='running' WHERE request_id='oc1'",
                        (json.dumps({"goal": "owned serve", "proof": "python3 -c \"print('proof-ran')\""}),))
        finally:
            con.close()
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        report = res["report"]
        turn_dir = Path(report["report_path"]).parent
        self.assertEqual(turn_dir.name, "turn-0")
        for name in ("report.json", "proof.log", "diff.patch", "worker.txt"):
            self.assertTrue((turn_dir / name).exists(), name)
            self.assertEqual((turn_dir / name).stat().st_mode & 0o777, 0o600, name)
        on_disk = json.loads((turn_dir / "report.json").read_text())
        self.assertEqual((on_disk["proof_command"], on_disk["proof_exit_code"]),
                         ("python3 -c \"print('proof-ran')\"", 0))
        self.assertIn("proof-ran", (turn_dir / "proof.log").read_text())
        self.assertEqual(on_disk["observed_model"], "opencode/muse-spark-1.3-contributor-free")
        self.assertEqual(on_disk["observed_variant"], "xhigh")
        self.assertEqual(on_disk["tokens"]["source"], "opencode")
        self.assertEqual(on_disk["tokens"]["messages"][0]["tokens"]["cache"]["read"], 300)
        self.assertTrue(on_disk["native_ids"]["assistant_message_ids"])
        self.assertEqual(on_disk["workspace_note"], "workspace is not a git checkout")
        self.assertIn("IMPLEMENTED by fake worker", on_disk["worker_summary"])
        self.assertIn("IMPLEMENTED by fake worker", (turn_dir / "worker.txt").read_text())
        # The dispatcher gets paths and structured fields, not the prose.
        evidence = controller._implementation_evidence(core.get_job(self.sd, "oc1"), res)
        self.assertIn(report["report_path"], evidence)
        self.assertIn('"proof_exit_code": 0', evidence)
        self.assertIn('"policy_version":', evidence)
        self.assertIn('"observed_variant": "xhigh"', evidence)
        self.assertIn('"tokens":', evidence)
        self.assertIn('"native_ids":', evidence)
        self.assertNotIn("proof log tail:", evidence)
        self.assertNotIn("worker summary:", evidence)
        self.assertNotIn("IMPLEMENTED by fake worker", evidence)
        # The invocation row carries the measurements Agent Observer needs.
        inv = [i for i in core._list_invocations(self.sd, "oc1") if i["kind"] == "opencode_control"][-1]
        self.assertEqual((inv["stage"], inv["requested_route"], inv["policy_version"], inv["reason"]),
                         ("implementation", "muse-spark-xhigh-free", policy.POLICY_VERSION, "initial"))
        self.assertEqual(inv["terminal_class"], "completed")
        self.assertGreater(inv["elapsed_secs"], 0)
        self.assertEqual(inv["observed_model"], "opencode/muse-spark-1.3-contributor-free")
        self.assertEqual(inv["observed_variant"], "xhigh")
        self.assertEqual(json.loads(inv["usage_json"])["source"], "opencode")
        self.assertEqual(json.loads(inv["native_ids_json"])["session_id"], inv["session_id"])
        self.assertEqual(inv["report_path"], report["report_path"])
        self.assertEqual(inv["schema_version"], store.SCHEMA_VERSION)
        view = core.status_view(self.sd, "oc1")
        m = view["job"]["measurements"][-1]
        self.assertEqual((m["stage"], m["terminal_class"], m["observed_model"], m["observed_variant"]),
                         ("implementation", "completed", inv["observed_model"], "xhigh"))
        self.assertTrue(m["native_ids"]["assistant_message_ids"])
        self.assertEqual(m["schema_version"], store.SCHEMA_VERSION)
        self.assertEqual(core.result_view(self.sd, "oc1")["reports"], [report["report_path"]])
        self._no_secret_leak()

    def test_hard_error_ends_the_turn_failed_without_blocking(self):
        run = self._setup("hard_error")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_failed")
        job = core.get_job(self.sd, "oc1")
        self.assertNotEqual(job["status"], "blocked")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        report = json.loads(Path(res["report"]["report_path"]).read_text())
        self.assertEqual(report["status"], "failed")
        self.assertIn("DataPolicyError", json.dumps(report["error"]))
        inv = [i for i in core._list_invocations(self.sd, "oc1") if i["kind"] == "opencode_control"][-1]
        self.assertEqual(inv["terminal_class"], "hard_error")
        controller._record_turn_outcome(self.sd, "oc1", res)
        self.assertEqual(controller._ladder(core.get_job(self.sd, "oc1"))["failures"], 1)
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        self.assertNotIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))

    def test_dispatch_falls_back_to_luna_on_opencode_plan_mode(self):
        run = self._setup("ok")
        bindir = Path(os.environ["PATH"].split(os.pathsep)[0])
        write_fake(bindir, "codex", "import sys\nprint('{\"type\":\"error\",\"message\":\"usage limit reached\"}')\nsys.exit(1)\n", PY)
        res = controller.dispatch(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res["route"], "luna-go/max")
        self.assertEqual(res["luna_action"]["action"], "completion")
        job = core.get_job(self.sd, "oc1")
        self.assertTrue(str(job["codex_task_id"]).startswith("ses"))
        self.assertEqual(job["adapter"], "opencode")
        self.assertEqual(controller._load_controller_state(job)["dispatch_route"], "luna-go/max")
        prompts = [r["body"] for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual(prompts[-1]["model"], {"providerID": "opencode-go", "modelID": "gpt-5.6-luna"})
        self.assertEqual(prompts[-1]["agent"], "plan")
        invs = core._list_invocations(self.sd, "oc1")
        self.assertEqual([i["stage"] for i in invs], ["dispatch", "dispatch"])
        self.assertEqual(invs[-1]["requested_route"], "luna-go/max")
        # A resume goes back to the same OpenCode dispatcher session.
        res2 = controller.resume_luna(self.sd, "oc1", "context", run_cmd=run)
        self.assertEqual((res2["action"], res2["codex_task_id"]), ("resumed", job["codex_task_id"]))
        self.assertEqual(len([r for r in self._requests() if r["path"].endswith("/prompt_async")]), 2)
        # The completion envelope ends the job through the normal step.
        done = controller.step(self.sd, "oc1", run_cmd=run)
        self.assertEqual(done["action"], "completed")
        self.assertEqual(core.get_job(self.sd, "oc1")["status"], "succeeded")
        self._no_secret_leak()

    def _kill_groups(self):
        for inv in core._list_invocations(self.sd, "oc1"):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass

    def _requests(self):
        f = self.fake_state / "opencode-requests.jsonl"
        return [json.loads(l) for l in f.read_text().splitlines()] if f.exists() else []

    def _no_secret_leak(self):
        con = store.connect(self.sd)
        try:
            db = json.dumps([dict(r) for r in con.execute("SELECT * FROM invocations").fetchall()])
            db += json.dumps([dict(r) for r in con.execute("SELECT * FROM events").fetchall()])
            db += json.dumps([dict(r) for r in con.execute("SELECT * FROM jobs").fetchall()])
        finally:
            con.close()
        self.assertNotIn("OPENCODE_SERVER_PASSWORD", db)
        self.assertNotIn("Basic ", db)
        self.assertEqual((self.fake_state / "pwd-in-argv").read_text(), "no")

    def test_success_saves_session_before_prompt_and_stops_server(self):
        run = self._setup("ok")
        res = controller.run_implementation(self.sd, "oc1", artifact="a.txt",
                                            run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok", core.get_job(self.sd, "oc1"))
        self.assertIn("IMPLEMENTED", res["output"])
        job = core.get_job(self.sd, "oc1")
        self.assertTrue(job["opencode_session_id"].startswith("ses_"))
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        reqs = self._requests()
        order = [r["path"].rsplit("/", 1)[-1] for r in reqs if r["method"] == "POST"]
        self.assertEqual(order, ["session", "prompt_async"])
        self.assertTrue(all(r["auth_ok"] for r in reqs))
        events = [e["kind"] for e in core.status_view(self.sd, "oc1")["recent_events"]]
        self.assertIn("invocation_session_captured", events)
        inv = core._list_invocations(self.sd, "oc1")[0]
        self.assertTrue(wait_for(lambda: not core._is_pgid_alive(inv["pgid"]), 10))
        self._no_secret_leak()

    def test_implementation_carries_saved_artifact_output(self):
        run = self._setup("ok")
        res = controller.run_implementation(self.sd, "oc1", artifact="outputs/fix.txt",
                                            payload={"output": "prior output"}, run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        prompt = [r for r in self._requests()
                  if r["path"].endswith("/prompt_async")][-1]["body"]["parts"][0]["text"]
        self.assertIn("outputs/fix.txt", prompt)
        self.assertIn("prior output", prompt)

    def test_provider_api_error_transfers_and_preserves_artifacts(self):
        run = self._setup("api_free_error")
        log = Path(self.sd) / "outputs" / "oc1.log"
        log.write_text("prior artifact chunk\n", encoding="utf-8")
        res = controller.run_implementation(self.sd, "oc1", artifact="outputs/fix.txt",
                                            run_cmd=run)
        self.assertEqual(res["action"], "transferred_to_go")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-go")
        self.assertTrue(job["opencode_session_id"].startswith("ses_"))
        self.assertIn("prior artifact chunk", log.read_text(encoding="utf-8"))
        self.assertIn("FreeUsageLimitError", job["last_error_json"])

    def test_capacity_memory_never_overrides_an_attempted_turn(self):
        run = self._setup("ok")
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "exhausted",
                             {"class": "FreeUsageLimitError"})
        self._insert_completed_turn("i-free")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertTrue(job["opencode_session_id"].startswith("ses_"))
        self.assertEqual(len([r for r in self._requests()
                              if r["path"].endswith("/prompt_async")]), 1)

    def test_free_limit_status_aborts_confirms_idle_then_go(self):
        run = self._setup("free_limit")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "transferred_to_go")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-go")
        self.assertTrue((self.fake_state / "aborted").exists())
        self.assertIn("free_tier_limit", job["last_error_json"])
        self.assertIn('"idle_confirmed": true', job["last_error_json"])
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        # Same saved session continues on Go with the Go model.
        free_session = job["opencode_session_id"]
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        prompts = [r for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual([p["body"]["model"]["providerID"] for p in prompts],
                         ["opencode", "opencode-go"])
        self.assertEqual(core.get_job(self.sd, "oc1")["opencode_session_id"], free_session)
        self._no_secret_leak()

    def test_provider_api_error_body_is_trusted_free_evidence(self):
        run = self._setup("api_free_error")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "transferred_to_go")

    def test_generic_rate_limit_never_transfers(self):
        # A 429 rate limit is overload, not exhaustion: the free route is never
        # marked exhausted and Go Muse is never selected; the job moves to the
        # next family after the provider's own retries.
        run = self._setup("rate_limit")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "lateral"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "glm-5.3-flash-go")
        self.assertNotEqual(job["route"], "muse-spark-xhigh-go")
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))

    def test_model_authored_text_is_not_provider_evidence(self):
        run = self._setup("model_text")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertNotEqual(job["status"], "succeeded")
        self.assertIsNone(controller._load_controller_state(job).get("last_action"))

    def test_hung_turn_times_out_with_abort(self):
        run = self._setup("hang", timeout_secs=3)
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "blocked")
        self.assertTrue((self.fake_state / "aborted").exists())
        job = core.get_job(self.sd, "oc1")
        self.assertIn("timed out", job["block_reason"] + job["last_error_json"])
        self.assertEqual(job["route"], "muse-spark-xhigh-free")


FAKE_LUNA = r"""
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
argv = sys.argv[1:]
with open(st / "codex.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
tid = "impl-crash-thread"
n_f = st / "codex_n"
n = int(n_f.read_text()) + 1 if n_f.exists() else 1
n_f.write_text(str(n))
ask = os.environ.get("FAKE_LUNA_ASK") == "1"
impl = {"action": "implementation", "artifact": "fix.txt",
        "payload": {"instructions": "write fix.txt"}}
if argv[:2] != ["exec", "resume"]:
    env = {"action": "planner_question", "qid": "q1", "prompt": "Which order?"} if ask else impl
elif ask and n == 2:
    env = impl
else:
    env = {"action": "completion", "output": "IMPL_CRASH_DONE"}
lp = argv[argv.index("--output-last-message") + 1]
Path(lp).write_text(json.dumps(env))
for obj in ({"type": "thread.started", "thread_id": tid},
            {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(env)}},
            {"type": "turn.completed", "usage": {}}):
    print(json.dumps(obj), flush=True)
"""


class TestImplementationBoundaryFaults(unittest.TestCase):
    def _setup(self, delay, extra_env=None, wait_kind="opencode_control"):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        self.sd = str(base / "state")
        self.ws = base / "ws"
        self.ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        self.fs = base / "fakestate"
        self.fs.mkdir()
        write_fake(bindir, "codex", FAKE_LUNA, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        env = dict(os.environ)
        env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""),
                   FAKE_STATE=str(self.fs), FAKE_OC_DELAY=str(delay),
                   FAKE_OC_WRITE="fix.txt", PYTHONDONTWRITEBYTECODE="1")
        env.update(extra_env or {})
        self.env = env
        self.addCleanup(self._cleanup)
        rc, out, err = cli(self.sd, "submit", "--request-id", "ib1",
                           "--task", '{"goal":"implementation boundary"}',
                           "--workspace", str(self.ws), "--planner-session", "p-ib",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: any(
            i["kind"] == wait_kind and i.get("pid")
            and (wait_kind != "opencode_control"
                 or core.get_job(self.sd, "ib1").get("opencode_session_id"))
            for i in core._list_invocations(self.sd, "ib1")), 15))
        return [i for i in core._list_invocations(self.sd, "ib1")
                if i["kind"] == wait_kind][0]

    def _cleanup(self):
        job = core.get_job(self.sd, "ib1")
        if job.get("owner_pid"):
            kill_pid(job["owner_pid"])
        for inv in core._list_invocations(self.sd, "ib1"):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass

    def _lines(self, name):
        f = self.fs / name
        return f.read_text().splitlines() if f.exists() else []

    def test_controller_killed_during_implementation_adopts_without_second_worker(self):
        inv = self._setup(delay=3.0)
        kill_pid(core.get_job(self.sd, "ib1")["owner_pid"])
        rc, rec, err = cli(self.sd, "recover", "--request-id", "ib1", env=self.env)
        self.assertEqual(rc, 0, err)
        self.assertEqual(rec.get("action"), "adopted-live-invocation", rec)
        self.assertTrue(rec.get("pid"))
        self.assertTrue(wait_for(lambda: core.get_job(self.sd, "ib1")["status"]
                                 in ("succeeded", "failed", "blocked"), 25))
        job = core.get_job(self.sd, "ib1")
        self.assertEqual(job["status"], "succeeded", job.get("block_reason"))
        self.assertIn("IMPL_CRASH_DONE", job["result_json"])
        self.assertEqual(len(self._lines("opencode.log")), 1, "one owned server only")
        prompts = [l for l in self._lines("opencode-requests.jsonl") if "prompt_async" in l]
        self.assertEqual(len(prompts), 1, "one implementation turn only")
        self.assertEqual(len(self._lines("codex.log")), 2, "dispatch plus one resume")
        kinds = [i["kind"] for i in core._list_invocations(self.sd, "ib1")]
        self.assertEqual(kinds.count("opencode_control"), 1)
        reused = [e for e in core.status_view(self.sd, "ib1")["recent_events"]
                  if e["kind"] == "invocation_reused"]
        self.assertTrue(reused, "the adopting controller reused the finished turn")
        self.assertEqual(job["codex_task_id"], "impl-crash-thread")
        self.assertFalse(core._is_pgid_alive(inv["pgid"]))

    def test_controller_killed_during_planner_callback_asks_once(self):
        self._setup(delay=0.2, extra_env={"FAKE_LUNA_ASK": "1", "FAKE_CLAUDE_DELAY": "2",
                                          "FAKE_CLAUDE_ANSWER": "Descending."},
                    wait_kind="claude_callback")
        kill_pid(core.get_job(self.sd, "ib1")["owner_pid"])
        rc, rec, err = cli(self.sd, "recover", "--request-id", "ib1", env=self.env)
        self.assertEqual(rc, 0, err)
        self.assertEqual(rec.get("action"), "adopted-live-invocation", rec)
        self.assertTrue(wait_for(lambda: core.get_job(self.sd, "ib1")["status"]
                                 in ("succeeded", "failed", "blocked"), 25))
        job = core.get_job(self.sd, "ib1")
        self.assertEqual(job["status"], "succeeded", job.get("block_reason"))
        self.assertEqual(len(self._lines("claude.log")), 1, "the planner is asked once")
        qs = core.list_questions(self.sd, "ib1", only_pending=False)
        self.assertEqual([(q["qid"], q["status"], q["answer"]) for q in qs],
                         [("q1", "answered", "Descending.")])
        resumes = [json.loads(l) for l in self._lines("codex.log") if '"resume"' in l]
        self.assertEqual(len(resumes), 2)
        self.assertIn("Descending.", resumes[0][-1])

    def test_supervisor_killed_stops_orphaned_server_and_blocks(self):
        inv = self._setup(delay=30.0)
        kill_pid(core.get_job(self.sd, "ib1")["owner_pid"])
        os.kill(int(inv["supervisor_pid"]), signal.SIGKILL)
        self.assertTrue(wait_for(lambda: not alive(inv["supervisor_pid"]), 5))
        self.assertTrue(core._is_pgid_alive(inv["pgid"]), "server outlives its supervisor")
        cur = [i for i in core._list_invocations(self.sd, "ib1") if i["invocation_id"] == inv["invocation_id"]][0]
        self.assertEqual(core._invocation_ownership(cur), "orphaned")
        rc, out, err = cli(self.sd, "start", "--request-id", "ib1", env=self.env)
        self.assertNotEqual(rc, 0, "an orphaned server blocks a new controller")
        rc, rec, err = cli(self.sd, "recover", "--request-id", "ib1", env=self.env)
        self.assertEqual(rc, 0, err)
        self.assertFalse(core._is_pgid_alive(inv["pgid"]), "recover stops the orphaned server")
        self.assertTrue(wait_for(lambda: core.get_job(self.sd, "ib1")["status"]
                                 in ("succeeded", "failed", "blocked"), 15))
        job = core.get_job(self.sd, "ib1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("implementation_failed", job["block_reason"])
        self.assertEqual(len(self._lines("opencode.log")), 1, "the interrupted turn is not replayed")


class TestIssue13PublicCLIDrills(unittest.TestCase):
    """Issue #13 drills through the public CLI (submit --start).

    Overload leaves within 60s with the 20s `next` cap enforced and the
    evidence recorded; exhaustion moves the same model to the Go pool;
    transport HTTP 503/529 degrades laterally. Capacity keys on pool,
    model, and window.
    """

    def _start_cli_job(self, oc_mode, extra_env=None, request_id="issue13-cli"):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        bindir = base / "bin"
        bindir.mkdir()
        fs = base / "fakestate"
        fs.mkdir()
        write_fake(bindir, "codex", FAKE_LUNA, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        env = dict(os.environ)
        env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""),
                   FAKE_STATE=str(fs), FAKE_OC_DELAY="0.2",
                   FAKE_OC_WRITE="fix.txt", PYTHONDONTWRITEBYTECODE="1")
        env.update(extra_env or {})
        rc, out, err = cli(sd, "submit", "--request-id", request_id,
                           "--task", '{"goal":"issue13 cli drill"}',
                           "--workspace", str(ws), "--planner-session", "p-issue13",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        return tmp, base, sd, ws, fs, env, request_id

    def _cleanup_cli_job(self, sd, request_id):
        try:
            job = core.get_job(sd, request_id)
        except Exception:
            return
        if job.get("owner_pid"):
            kill_pid(job["owner_pid"])
        for inv in core._list_invocations(sd, request_id):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass

    def _requests(self, fs):
        f = fs / "opencode-requests.jsonl"
        return [json.loads(l) for l in f.read_text().splitlines()] if f.exists() else []

    def test_cli_overload_leaves_within_60s_with_capped_next(self):
        tmp, base, sd, ws, fs, env, rid = self._start_cli_job(
            "overloaded", {"FAKE_OC_MODE": "overloaded", "FAKE_OC_NEXT": "999"},
            request_id="issue13-overload-cap")
        self.addCleanup(lambda: self._cleanup_cli_job(sd, rid))
        started = time.monotonic()
        self.assertTrue(wait_for(
            lambda: core.get_job(sd, rid)["status"] == "succeeded", 25),
            core.get_job(sd, rid))
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 60.0, "overloaded route left within 60s via public CLI")
        job = core.get_job(sd, rid)
        self.assertEqual(job["route"], "glm-5.3-flash-go", job)
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(sd))
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(sd))
        # Retry counts and the 20s cap are recorded by the CLI-driven turn.
        last = json.loads(job["last_error_json"] or "{}")
        self.assertEqual(last.get("signal"), "overloaded")
        self.assertLessEqual(float(last.get("retry_next_capped") or 0), 20.0)
        self.assertEqual(float(last.get("retry_next_capped") or 0), 20.0)
        self.assertLessEqual(int(last.get("overload_retries") or 0), 3)
        self.assertTrue(last.get("idle_confirmed"))
        # CLI-recorded evidence: capacity lists the degraded route with pool/model/window.
        rc, cap_out, err = cli(sd, "capacity", env=env)
        self.assertEqual(rc, 0, err)
        rows = [r for r in cap_out.get("capacity", []) if r["route"] == "muse-spark-xhigh-free"]
        self.assertTrue(rows)
        self.assertEqual(rows[0]["state"], "degraded")
        self.assertEqual((rows[0]["pool"], rows[0]["window"]), ("zen-free", "cooldown"))
        self.assertTrue(rows[0]["reset_at"])
        rc, st_out, err = cli(sd, "status", "--request-id", rid, env=env)
        self.assertEqual(rc, 0, err)
        self.assertEqual(st_out["job"]["route"], "glm-5.3-flash-go")
        # Same model is never retried on the same route: one free prompt, then Go.
        prompts = [r for r in self._requests(fs) if r["path"].endswith("/prompt_async")]
        free_prompts = [p for p in prompts if p["body"]["model"]["providerID"] == "opencode"]
        self.assertEqual(len(free_prompts), 1, prompts)
        self.assertTrue((ws / "fix.txt").exists())

    def test_cli_transport_503_degrades_laterally(self):
        tmp, base, sd, ws, fs, env, rid = self._start_cli_job(
            "transport_503", {"FAKE_OC_MODE": "transport_503"},
            request_id="issue13-transport-503")
        self.addCleanup(lambda: self._cleanup_cli_job(sd, rid))
        started = time.monotonic()
        self.assertTrue(wait_for(
            lambda: core.get_job(sd, rid)["status"] == "succeeded", 25),
            core.get_job(sd, rid))
        self.assertLess(time.monotonic() - started, 60.0)
        job = core.get_job(sd, rid)
        self.assertEqual(job["route"], "glm-5.3-flash-go", job)
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(sd))
        last = json.loads(job["last_error_json"] or "{}")
        self.assertEqual(last.get("signal"), "overloaded")
        self.assertEqual((last.get("evidence") or {}).get("source"), "transport")
        self.assertEqual((last.get("evidence") or {}).get("status"), 503)
        rc, cap_out, err = cli(sd, "capacity", env=env)
        self.assertEqual(rc, 0, err)
        rows = [r for r in cap_out.get("capacity", []) if r["route"] == "muse-spark-xhigh-free"]
        self.assertTrue(rows and rows[0]["state"] == "degraded")

    def test_cli_exhaustion_moves_to_go_with_evidence(self):
        tmp, base, sd, ws, fs, env, rid = self._start_cli_job(
            "free_limit", {"FAKE_OC_MODE": "free_limit"},
            request_id="issue13-exhaust-go")
        self.addCleanup(lambda: self._cleanup_cli_job(sd, rid))
        started = time.monotonic()
        self.assertTrue(wait_for(
            lambda: core.get_job(sd, rid)["status"] == "succeeded", 25),
            core.get_job(sd, rid))
        self.assertLess(time.monotonic() - started, 60.0)
        job = core.get_job(sd, rid)
        self.assertEqual(job["route"], "muse-spark-xhigh-go", job)
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(sd))
        self.assertIn("free_tier_limit", job["last_error_json"])
        self.assertIn('"idle_confirmed": true', job["last_error_json"])
        rc, cap_out, err = cli(sd, "capacity", env=env)
        self.assertEqual(rc, 0, err)
        rows = [r for r in cap_out.get("capacity", []) if r["route"] == "muse-spark-xhigh-free"]
        self.assertTrue(rows and rows[0]["state"] == "exhausted")
        prompts = [r for r in self._requests(fs) if r["path"].endswith("/prompt_async")]
        self.assertEqual([p["body"]["model"]["providerID"] for p in prompts],
                         ["opencode", "opencode-go"])
        self.assertTrue((ws / "fix.txt").exists())

    def test_capacity_keys_on_pool_model_window(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "capwin", {"g": 1}, str(ws), "p1")
        core.record_capacity(sd, "grok-4.6-go", "exhausted", {"class": "GoUsageLimitError"})
        rows = [r for r in core.list_capacity(sd) if r["route"] == "grok-4.6-go"]
        self.assertEqual({r["window"] for r in rows}, {"5h", "weekly", "monthly"})
        self.assertTrue(all(r["pool"] == "go" and r["model"] == "opencode-go/grok-4.6" for r in rows))
        self.assertIn("grok-4.6-go", core.exhausted_routes(sd))
        # A degraded mark on another window coexists; clearing forgets all windows.
        core.record_capacity(sd, "muse-spark-xhigh-free", "degraded",
                             {"source": "test"}, reset_at=core.degraded_until())
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(sd))
        self.assertIn("grok-4.6-go", core.exhausted_routes(sd))
        rc, cap_out, err = cli(sd, "capacity", env=dict(os.environ))
        self.assertEqual(rc, 0, err)
        self.assertGreaterEqual(len(cap_out.get("capacity", [])), 4)
        core.clear_capacity(sd, "grok-4.6-go")
        self.assertNotIn("grok-4.6-go", core.exhausted_routes(sd))
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(sd))

    def test_exhaustion_preflight_skips_degraded_next_pool(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        core.submit(sd, "pre1", {"g": 1}, str(ws), "p1")
        core.record_capacity(sd, "muse-spark-xhigh-free", "exhausted", {"class": "FreeUsageLimitError"})
        core.record_capacity(sd, "muse-spark-xhigh-go", "degraded",
                             {"source": "test"}, reset_at=core.degraded_until())
        move = controller._preflight_move(sd, "pre1", "muse-spark-xhigh-free")
        self.assertIsNotNone(move)
        self.assertEqual((move["reason"], move["route"]),
                         ("preflight_exhausted", "glm-5.3-flash-go"))
        # The lateral signal path skips a degraded next-pool route as well.
        ws2 = Path(tmp.name) / "ws2"
        ws2.mkdir(exist_ok=True)
        core.submit(sd, "pre2", {"g": 1}, str(ws2), "p1")
        res = controller._move_after_signal(sd, "pre2", "muse-spark-xhigh-free",
                                            "exhausted", {"class": "FreeUsageLimitError"})
        self.assertEqual((res["reason"], res["route"]), ("lateral", "glm-5.3-flash-go"))

    def test_docs_and_skill_list_all_overload_signals(self):
        root = Path(__file__).resolve().parents[1]
        runner_doc = (root / "RUNNER.md").read_text()
        skill = (root / "skills" / "model-routing" / "references" / "codex.md").read_text()
        for name in ("overloaded_error", "rate_limit_exceeded", "RateLimitError",
                     "overloaded", "rate_limit", "account_rate_limit", "503", "529"):
            self.assertIn(name, runner_doc, name)
            self.assertIn(name, skill, name)
        self.assertIn("`next` capped at 20 seconds", runner_doc)
        self.assertIn("`next` capped at", skill)
        self.assertIn("implementation_failed", runner_doc)
        self.assertIn("implementation_failed", skill)


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
        (bindir / "codex").write_text("#!" + PY + "\n" + VERSION_GUARD + r"""
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


class TestIssue14ReportContract(unittest.TestCase):
    """Issue #14: every turn leaves redacted reports, diffs, and measurements."""

    def _setup_owned(self, mode, task=None):
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
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_OC_MODE")}
        def restore():
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.addCleanup(restore)
        os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
        os.environ["FAKE_STATE"] = str(fake_state)
        os.environ["FAKE_OC_MODE"] = mode
        core.submit(sd, "oc1", task or {"goal": "owned serve"}, str(ws), "planner")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='oc1'")
        finally:
            con.close()
        self.sd = sd
        self.ws = ws
        self.fake_state = fake_state
        self.addCleanup(self._kill_owned)
        run = core.make_durable_run_cmd(sd, "oc1", "tok")
        def timed_run(cmd, cwd=None, timeout=None, **kw):
            return run(cmd, cwd, timeout, **kw)
        return timed_run

    def _kill_owned(self):
        try:
            for inv in core._list_invocations(self.sd, "oc1"):
                for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                    if pg:
                        try:
                            os.killpg(int(pg), signal.SIGKILL)
                        except Exception:
                            pass
        except Exception:
            pass

    def _report_files(self, report):
        turn_dir = Path(report["report_path"]).parent
        for name in ("report.json", "proof.log", "diff.patch", "worker.txt"):
            p = turn_dir / name
            self.assertTrue(p.exists(), name)
            self.assertEqual(p.stat().st_mode & 0o777, 0o600, name)
        return turn_dir

    def test_failed_turn_writes_redacted_report(self):
        run = self._setup_owned("hard_error")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_failed")
        turn_dir = self._report_files(res["report"])
        on_disk = json.loads((turn_dir / "report.json").read_text())
        self.assertEqual(on_disk["status"], "failed")
        self.assertEqual(on_disk["observed_variant"], "xhigh")
        inv = [i for i in core._list_invocations(self.sd, "oc1") if i["kind"] == "opencode_control"][-1]
        self.assertEqual(inv["terminal_class"], "hard_error")
        self.assertEqual(inv["observed_variant"], "xhigh")

    def test_quota_and_overload_turns_write_reports_without_overwrite(self):
        run = self._setup_owned("free_limit")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertIn(res["action"], ("transferred_to_go", "route_switched"))
        self.assertIn("report", res)
        first_dir = self._report_files(res["report"])
        first_report = json.loads((first_dir / "report.json").read_text())
        self.assertIn(first_report["status"], ("exhausted", "failed"))
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        second_dir = self._report_files(res2["report"])
        self.assertNotEqual(str(first_dir), str(second_dir))
        reports = core.result_view(self.sd, "oc1")["reports"]
        self.assertEqual(len(reports), 2)
        self.assertIn(res["report"]["report_path"], reports)
        self.assertIn(res2["report"]["report_path"], reports)

    def test_crash_turn_writes_report(self):
        self._setup_owned("ok")
        def stub(cmd, cwd=None, timeout=None, **kw):
            return 124, "", ""
        res = controller.run_implementation(self.sd, "oc1", run_cmd=stub)
        self.assertEqual(res["action"], "blocked")
        self.assertIn("report", res)
        self._report_files(res["report"])
        on_disk = json.loads(Path(res["report"]["report_path"]).read_text())
        self.assertEqual(on_disk["status"], "failed")

    def test_git_diff_includes_staged_and_untracked(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x",
                   GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@x")
        subprocess.run(["git", "init", "-q", str(ws)], check=True, env=env)
        (ws / "tracked.txt").write_text("base\n")
        subprocess.run(["git", "-C", str(ws), "add", "tracked.txt"], check=True, env=env)
        subprocess.run(["git", "-C", str(ws), "commit", "-q", "-m", "base"], check=True, env=env)
        (ws / "tracked.txt").write_text("unstaged\n")
        (ws / "staged.txt").write_text("staged\n")
        subprocess.run(["git", "-C", str(ws), "add", "staged.txt"], check=True, env=env)
        (ws / "new.txt").write_text("untracked-body\n")
        files, diff, note = controller._git_changes(str(ws))
        self.assertIsNone(note)
        self.assertIn("tracked.txt", files)
        self.assertIn("staged.txt", files)
        self.assertIn("new.txt", files)
        self.assertIn("unstaged", diff)
        self.assertIn("staged", diff)
        self.assertIn("untracked-body", diff)

    def test_worker_txt_is_full_and_redacted(self):
        self._setup_owned("ok")
        job = core.get_job(self.sd, "oc1")
        long_text = "X" * 20000 + " api_key=SECRET123 Bearer abcdefgh12345678"
        full = {"assistant_text": long_text, "usage": {"source": "opencode"},
                "native_ids": {"session_id": "s"}, "actual_model": {"providerID": "opencode", "modelID": "m", "variant": "xhigh"}}
        report = controller._write_turn_report(self.sd, "oc1", job, 9, "muse-spark-xhigh-free", full, "ses_1")
        body = Path(report["worker_text"]).read_text()
        self.assertEqual(len(body), len(controller.adapters.redact_text(long_text)))
        self.assertNotIn("SECRET123", body)
        self.assertNotIn("abcdefgh12345678", body)
        self.assertIn("<redacted>", body)
        self.assertGreater(len(body), 8000)
        self.assertNotIn("SECRET123", report["worker_summary"])
        proof = Path(report["proof_log"]).read_text()
        self.assertNotIn("SECRET123", proof)

    def test_proof_output_is_redacted(self):
        run = self._setup_owned("ok")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET task_json=? WHERE request_id='oc1'",
                        (json.dumps({"goal": "g", "proof": "python3 -c \"print('password=hunter2-secret')\""}),))
        finally:
            con.close()
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        proof = Path(res["report"]["proof_log"]).read_text()
        self.assertNotIn("hunter2-secret", proof)
        self.assertIn("<redacted>", proof)

    def test_evidence_has_fields_without_prose(self):
        run = self._setup_owned("ok")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET task_json=? WHERE request_id='oc1'",
                        (json.dumps({"goal": "g", "proof": "python3 -c \"print(40+2)\""}),))
        finally:
            con.close()
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        evidence = controller._implementation_evidence(core.get_job(self.sd, "oc1"), res)
        for key in ("policy_version", "variant", "observed_variant", "tokens", "native_ids",
                    "turn_status", "report", "proof_log", "diff", "worker_text"):
            self.assertIn(f'"{key}"', evidence)
        self.assertNotIn("proof log tail:", evidence)
        self.assertNotIn("worker summary:", evidence)
        self.assertNotIn("IMPLEMENTED", evidence)
        # Proof output (42) stays in the file, not in the dispatcher message.
        self.assertNotIn("\n42\n", evidence)

    def test_measurements_expose_variant_native_ids_schema(self):
        run = self._setup_owned("ok")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        view = core.status_view(self.sd, "oc1")
        m = view["job"]["measurements"][-1]
        self.assertEqual(m["observed_model"], "opencode/muse-spark-1.3-contributor-free")
        self.assertEqual(m["observed_variant"], "xhigh")
        self.assertTrue(m["native_ids"]["assistant_message_ids"])
        self.assertEqual(m["schema_version"], store.SCHEMA_VERSION)
        self.assertGreater(m["elapsed_secs"], 0)
        rview = core.result_view(self.sd, "oc1")
        self.assertEqual(rview["measurements"][-1]["observed_variant"], "xhigh")

    def test_planner_harness_codex_rejected_and_blocked(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        with self.assertRaises(ValueError):
            core.submit(sd, "bad", {"g": 1}, str(ws), "p", planner_harness="codex")
        rc, _, _ = cli(sd, "submit", "--request-id", "bad2", "--task", '{"g":1}',
                       "--workspace", str(ws), "--planner-session", "p",
                       "--planner-harness", "codex", "--no-start")
        self.assertNotEqual(rc, 0)
        # Legacy row naming codex must not execute Claude.
        core.submit(sd, "legacy", {"g": 1}, str(ws), "p")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET planner_harness='codex' WHERE request_id='legacy'")
        finally:
            con.close()
        out = controller.planner_callback(sd, "legacy", "q1", "prompt?")
        self.assertEqual(out["reason"], "planner_harness_unsupported")
        self.assertIn("planner_harness_unsupported", core.get_job(sd, "legacy")["block_reason"])

    def test_supervisor_spawn_failure_is_measured(self):
        self._setup_owned("ok")
        import subprocess as _sp
        real_popen = _sp.Popen
        def boom(*a, **k):
            raise OSError("no fork")
        _sp.Popen = boom
        try:
            rc, _, _ = core._durable_run(self.sd, "oc1", "tok", "codex_dispatch", ["codex", "exec"], cwd=str(self.ws), meta={"stage": "dispatch", "route": "luna/max"})
        finally:
            _sp.Popen = real_popen
        self.assertEqual(rc, 127)
        inv = core._list_invocations(self.sd, "oc1")[-1]
        self.assertEqual(inv["state"], "failed")
        self.assertEqual(inv["rc"], 127)
        self.assertIsNotNone(inv["elapsed_secs"])
        self.assertEqual(inv["terminal_class"], "failed")
        self.assertEqual(inv["schema_version"], store.SCHEMA_VERSION)

    def test_public_cli_job_kinds_links_and_status(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        ws2 = Path(tmp.name) / "ws2"
        ws2.mkdir()
        task = json.dumps({"goal": "g", "links": [{"rel": "spec", "href": "https://example.com/s"}]})
        rc, out, err = cli(sd, "submit", "--request-id", "jk1", "--task", task,
                           "--workspace", str(ws), "--planner-session", "p",
                           "--job-kind", "experiment", "--no-start")
        self.assertEqual(rc, 0, err)
        rc, st, err = cli(sd, "status", "--request-id", "jk1")
        self.assertEqual(rc, 0, err)
        self.assertEqual(st["job"]["job_kind"], "experiment")
        self.assertIn("links", core.get_job(sd, "jk1")["task_json"])
        rc, out, err = cli(sd, "submit", "--request-id", "jk2", "--task", '{"goal":"g"}',
                           "--workspace", str(ws2), "--planner-session", "p",
                           "--job-kind", "replay", "--replay-of", "jk1", "--no-start")
        self.assertEqual(rc, 0, err)
        rc, st, err = cli(sd, "status", "--request-id", "jk2", env=None)
        self.assertEqual(rc, 0, err)
        self.assertEqual((st["job"]["job_kind"], st["job"]["replay_of"]), ("replay", "jk1"))
        rc, res, err = cli(sd, "result", "--request-id", "jk1")
        self.assertEqual(rc, 0, err)
        self.assertEqual(res["job_kind"], "experiment")


if __name__ == "__main__":
    unittest.main(verbosity=2)
