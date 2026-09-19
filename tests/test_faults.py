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
                         ("kimi-k3-go", None))
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
        self.assertEqual(nxt3, "glm-5.3-go")
        self.assertIsNone(blocker3)
        self.assertEqual(policy.next_capacity_route("glm-5.3-go", set()), (None, None))
        self.assertTrue(policy.is_operational("glm-5.3-go"))
        for gone in ("go-deepseek-v4.1-flash", "terra/max", "kimi-k2.7-code", "grok-4.6/medium",
                     "astra/medium", "opus-5/high"):
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
            res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
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

    def test_one_turn_route_is_refused_a_second_turn(self):
        run = self._setup("ok")
        self._set_route("kimi-k3-go")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual(res["action"], "implementation_ok")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='oc1'",
                        (json.dumps({"seq": 1, "last_action": {"action": "implementation"}}),))
        finally:
            con.close()
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        # The one-turn mark now yields a lateral move before dispatch, not a block.
        self.assertEqual((res2["action"], res2["reason"], res2["route"]),
                         ("route_switched", "preflight_one_turn", "deepseek-v4-pro-go"))
        prompts = [r for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual(len(prompts), 1)
    def _capacity(self, route):
        rows = [r for r in core.list_capacity(self.sd) if r["route"] == route]
        return rows[0] if rows else None

    def test_overload_moves_to_next_family_within_60s(self):
        run = self._setup("overloaded")
        started = time.monotonic()
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        elapsed = time.monotonic() - started
        self.assertEqual(res["action"], "route_switched")
        self.assertEqual(res["reason"], "lateral")
        self.assertLess(elapsed, 60.0)
        job = core.get_job(self.sd, "oc1")
        # Same family (Go Muse) is skipped; the next family in the default lane is GLM.
        self.assertEqual(job["route"], "glm-5.3-go")
        self.assertTrue((self.fake_state / "aborted").exists())
        self.assertIn('"signal": "overloaded"', job["last_error_json"])
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        cap = self._capacity("muse-spark-xhigh-free")
        self.assertEqual((cap["state"], cap["pool"], cap["window"]), ("degraded", "zen-free", "cooldown"))
        self.assertTrue(cap["reset_at"])
        # The next turn runs on GLM in the same saved session.
        session = job["opencode_session_id"]
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual(res2["action"], "implementation_ok")
        prompts = [r["body"]["model"] for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual([m["providerID"] for m in prompts], ["opencode", "opencode-go"])
        self.assertEqual(prompts[-1]["modelID"], "glm-5.3")
        self.assertEqual(core.get_job(self.sd, "oc1")["opencode_session_id"], session)
        self._no_secret_leak()

    def test_go_exhaustion_moves_the_same_model_to_xai(self):
        run = self._setup("ok")
        self._go_mode("go_limit")
        self._set_route("grok-4.6-go")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "pool_move"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "grok-4.6-xai")
        self.assertIn("grok-4.6-go", core.exhausted_routes(self.sd))
        cap = self._capacity("grok-4.6-go")
        self.assertEqual((cap["state"], cap["pool"], cap["model"]), ("exhausted", "go", "opencode-go/grok-4.6"))
        self.assertIn("GoUsageLimitError", cap["evidence_json"])
        self.assertIsNone(cap["reset_at"])  # no invented reset
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual(res2["action"], "implementation_ok")
        prompts = [r["body"]["model"] for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual([m["providerID"] for m in prompts], ["opencode-go", "xai"])
        self._no_secret_leak()

    def test_exhaustion_without_next_route_blocks_with_reason(self):
        run = self._setup("ok")
        self._go_mode("go_limit")
        self._set_route("glm-5.3-go")  # last route of the default lane, no next pool
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual((res["action"], res["reason"]), ("blocked", "capacity_exhausted"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["status"], "blocked")
        self.assertIn("capacity_exhausted", job["block_reason"])
        self.assertEqual(job["route"], "glm-5.3-go")
        self.assertIn("glm-5.3-go", core.exhausted_routes(self.sd))

    def test_hard_lane_moves_stay_in_the_hard_lane_and_respect_one_turn(self):
        run = self._setup("overloaded")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET lane='implementation_hard', status='running' WHERE request_id='oc1'")
        finally:
            con.close()
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        # Overloaded free Muse in the hard lane moves to Kimi K3, not to GLM.
        self.assertEqual((res["action"], res["route"]), ("route_switched", "kimi-k3-go"))
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
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
        res3 = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual((res3["action"], res3["reason"], res3["route"]),
                         ("route_switched", "preflight_one_turn", "deepseek-v4-pro-go"))
        prompts = [r["body"]["model"]["modelID"] for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual(prompts.count("kimi-k3"), 1)

    def test_preflight_skips_degraded_and_exhausted_routes(self):
        run = self._setup("ok")
        core.record_capacity(self.sd, "muse-spark-xhigh-free", "degraded",
                             {"source": "test", "class": "overloaded"}, reset_at=core.degraded_until())
        core.record_capacity(self.sd, "muse-spark-xhigh-go", "exhausted", {"source": "test"})
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "preflight_degraded"))
        self.assertEqual(core.get_job(self.sd, "oc1")["route"], "glm-5.3-go")
        self.assertEqual(self._requests(), [])  # nothing was dispatched to the resting routes
        # Cooldown in the past means the route is eligible again.
        core.record_capacity(self.sd, "glm-5.3-go", "degraded", {"source": "test"},
                             reset_at="2000-01-01T00:00:00+00:00")
        self.assertNotIn("glm-5.3-go", core.degraded_routes(self.sd))
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
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
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
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
        self.assertEqual(on_disk["tokens"]["source"], "opencode")
        self.assertEqual(on_disk["tokens"]["messages"][0]["tokens"]["cache"]["read"], 300)
        self.assertTrue(on_disk["native_ids"]["assistant_message_ids"])
        self.assertEqual(on_disk["workspace_note"], "workspace is not a git checkout")
        self.assertIn("IMPLEMENTED by fake worker", on_disk["worker_summary"])
        # The dispatcher gets paths and fields, not the prose.
        evidence = controller._implementation_evidence(core.get_job(self.sd, "oc1"), res)
        self.assertIn(report["report_path"], evidence)
        self.assertIn('"proof_exit_code": 0', evidence)
        self.assertIn("proof-ran", evidence)
        # The invocation row carries the measurements Agent Observer needs.
        inv = [i for i in core._list_invocations(self.sd, "oc1") if i["kind"] == "opencode_control"][-1]
        self.assertEqual((inv["stage"], inv["requested_route"], inv["policy_version"], inv["reason"]),
                         ("implementation", "muse-spark-xhigh-free", policy.POLICY_VERSION, "initial"))
        self.assertEqual(inv["terminal_class"], "completed")
        self.assertGreater(inv["elapsed_secs"], 0)
        self.assertEqual(inv["observed_model"], "opencode/muse-spark-1.3-contributor-free xhigh")
        self.assertEqual(json.loads(inv["usage_json"])["source"], "opencode")
        self.assertEqual(json.loads(inv["native_ids_json"])["session_id"], inv["session_id"])
        self.assertEqual(inv["report_path"], report["report_path"])
        self.assertEqual(inv["schema_version"], store.SCHEMA_VERSION)
        view = core.status_view(self.sd, "oc1")
        m = view["job"]["measurements"][-1]
        self.assertEqual((m["stage"], m["terminal_class"], m["observed_model"]),
                         ("implementation", "completed", inv["observed_model"]))
        self.assertEqual(core.result_view(self.sd, "oc1")["reports"], [report["report_path"]])
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
                                            run_cmd=run, use_owned_server=True)
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

    def test_free_limit_status_aborts_confirms_idle_then_go(self):
        run = self._setup("free_limit")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual(res["action"], "transferred_to_go")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-go")
        self.assertTrue((self.fake_state / "aborted").exists())
        self.assertIn("free_tier_limit", job["last_error_json"])
        self.assertIn('"idle_confirmed": true', job["last_error_json"])
        self.assertIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        # Same saved session continues on Go with the Go model.
        free_session = job["opencode_session_id"]
        res2 = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual(res2["action"], "implementation_ok")
        prompts = [r for r in self._requests() if r["path"].endswith("/prompt_async")]
        self.assertEqual([p["body"]["model"]["providerID"] for p in prompts],
                         ["opencode", "opencode-go"])
        self.assertEqual(core.get_job(self.sd, "oc1")["opencode_session_id"], free_session)
        self._no_secret_leak()

    def test_provider_api_error_body_is_trusted_free_evidence(self):
        run = self._setup("api_free_error")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual(res["action"], "transferred_to_go")

    def test_generic_rate_limit_never_transfers(self):
        # A 429 rate limit is overload, not exhaustion: the free route is never
        # marked exhausted and Go Muse is never selected; the job moves to the
        # next family after the provider's own retries.
        run = self._setup("rate_limit")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "lateral"))
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "glm-5.3-go")
        self.assertNotEqual(job["route"], "muse-spark-xhigh-go")
        self.assertNotIn("muse-spark-xhigh-free", core.exhausted_routes(self.sd))
        self.assertIn("muse-spark-xhigh-free", core.degraded_routes(self.sd))

    def test_model_authored_text_is_not_provider_evidence(self):
        run = self._setup("model_text")
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
        self.assertEqual(res["action"], "implementation_ok")
        job = core.get_job(self.sd, "oc1")
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        self.assertNotEqual(job["status"], "succeeded")
        self.assertIsNone(controller._load_controller_state(job).get("last_action"))

    def test_hung_turn_times_out_with_abort(self):
        run = self._setup("hang", timeout_secs=3)
        res = controller.run_implementation(self.sd, "oc1", run_cmd=run, use_owned_server=True)
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
