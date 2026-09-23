"""Grok Build CLI harness for the xAI pool (Issue #31).

Deterministic fakes only. No live model CLIs. Covers the harness seam
(spawn spec, session/report parsing, signal classification, measurement),
the policy routes (Build first, OpenCode xAI as next_pool fallback,
recovery and the hard lane), the worker kit, and fake-``grok`` drills
through the public CLI: success, exhaustion, overload, timeout, and
controller death with resume of the saved session and no second writer.
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

from runner import adapters, controller, core, harnesses, policy, store  # noqa: E402
from tests.fakes import FAKE_GROK, FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable

FAKE_CODEX_DISPATCH = r'''
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
st.mkdir(parents=True, exist_ok=True)
argv = sys.argv[1:]
with open(st / "codex.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
print(json.dumps({"type": "thread.started", "thread_id": "thr-grok-e2e-1"}), flush=True)
if "resume" in argv:
    a = {"action": "completion", "output": "E2E_DONE", "artifact": ""}
else:
    a = {"action": "implementation", "artifact": "",
         "payload": {"instructions": "do the thing"}}
if "--output-last-message" in argv:
    Path(argv[argv.index("--output-last-message") + 1]).write_text(json.dumps(a))
print(json.dumps({"type": "item.completed",
                  "item": {"type": "agent_message", "text": json.dumps(a)}}), flush=True)
print(json.dumps({"type": "turn.completed",
                  "usage": {"input_tokens": 1, "output_tokens": 1}}), flush=True)
'''


def cli(state_dir, *args, env=None, timeout=30):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(ROOT), env=env)
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(fn, secs=15.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def kill_pid(pid):
    try:
        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        return


def grok_calls(fake_state):
    f = Path(fake_state) / "grok.log"
    if not f.exists():
        return []
    return [json.loads(l) for l in f.read_text().splitlines() if l.strip()]


def worker_calls(fake_state):
    return [c for c in grok_calls(fake_state) if "-p" in c["argv"] or "--prompt-file" in c["argv"]]


class GrokCmd(unittest.TestCase):
    def test_spawn_spec_is_headless_single_prompt(self):
        cmd, env = adapters.build_grok_cmd("do it", "/tmp/ws", "grok-4.6", "medium")
        self.assertEqual(env, {})
        self.assertEqual(cmd[0], "grok")
        self.assertIn("-p", cmd)
        self.assertEqual(cmd[cmd.index("-p") + 1], "do it")
        self.assertIn("--verbatim", cmd)
        self.assertEqual(cmd[cmd.index("--cwd") + 1], "/tmp/ws")
        self.assertEqual(cmd[cmd.index("-m") + 1], "grok-4.6")
        self.assertEqual(cmd[cmd.index("--effort") + 1], "medium")
        self.assertIn("--always-approve", cmd)
        self.assertIn("--disable-web-search", cmd)
        self.assertIn("--no-subagents", cmd)
        self.assertEqual(cmd[cmd.index("--output-format") + 1], "json")
        self.assertNotIn("--resume", cmd)

    def test_resume_names_the_saved_session(self):
        cmd, _env = adapters.build_grok_cmd("do it", "/tmp/ws", "grok-4.6", "medium",
                                            "ses_saved_1")
        self.assertEqual(cmd[cmd.index("--resume") + 1], "ses_saved_1")

    def test_kit_env_points_at_config_dir(self):
        cmd, env = adapters.build_grok_cmd("do it", "/tmp/ws", "grok-4.6", "medium",
                                           config_dir="/tmp/grok-kit")
        self.assertEqual(cmd[cmd.index("--cwd") + 1], "/tmp/ws")
        self.assertEqual(env.get("GROK_HOME"), "/tmp/grok-kit")

    def test_missing_fields_refuse(self):
        for kw in ({"prompt": ""}, {"workspace": ""}, {"model": ""}):
            base = {"prompt": "p", "workspace": "/tmp/ws", "model": "grok-4.6"}
            base.update(kw)
            with self.assertRaises(ValueError):
                adapters.build_grok_cmd(**base)

    def test_harness_defaults_come_from_policy(self):
        self.assertEqual(adapters.GROK_MODEL, policy.ROUTES["grok-4.6-build"]["model"])
        self.assertEqual(adapters.GROK_EFFORT, policy.ROUTES["grok-4.6-build"]["variant"])
        self.assertEqual(adapters.grok_route_params("grok-4.6-build"), ("grok-4.6", "medium"))
        with self.assertRaises(ValueError):
            adapters.grok_route_params("grok-4.6-xai")
        with self.assertRaises(ValueError):
            adapters.grok_route_params("bogus")


class GrokParse(unittest.TestCase):
    def test_success(self):
        out = json.dumps({"text": "did it", "stopReason": "end_turn",
                          "sessionId": "ses_1", "num_turns": 2, "model": "grok-4.6",
                          "usage": {"input_tokens": 5}}) + "\n"
        parsed = adapters.parse_grok_result("noise line\n" + out)
        self.assertTrue(parsed["ok"])
        self.assertEqual((parsed["text"], parsed["session_id"], parsed["stop_reason"]),
                         ("did it", "ses_1", "end_turn"))
        self.assertEqual(parsed["num_turns"], 2)
        self.assertEqual(parsed["usage"], {"input_tokens": 5})

    def test_error_object_is_never_worker_text(self):
        parsed = adapters.parse_grok_result(
            json.dumps({"type": "error", "message": "xAI rate limit exceeded"}))
        self.assertFalse(parsed["ok"])
        self.assertIsNone(parsed["text"])
        self.assertIn("rate limit", parsed["error"])

    def test_empty_and_malformed_output_fail(self):
        for blob in ("", "plain text, no json\n", '{"text": "no session"}'):
            parsed = adapters.parse_grok_result(blob)
            if blob.startswith("{"):
                self.assertIsNone(parsed["session_id"])
            else:
                self.assertFalse(parsed["ok"])

    def test_incomplete_stop_reason_is_not_ok(self):
        for stop in ("max_tokens", "max_turn_requests", "cancelled", "refusal"):
            parsed = adapters.parse_grok_result(json.dumps(
                {"text": "partial", "stopReason": stop, "sessionId": "ses_1"}))
            self.assertFalse(parsed["ok"], stop)
            self.assertEqual(parsed["stop_reason"], stop)
            self.assertEqual(parsed["text"], "partial")


class GrokHarness(unittest.TestCase):
    def setUp(self):
        self.h = harnesses.harness_named("grok")

    def test_registry_and_capabilities(self):
        self.assertIs(harnesses.harness_for("grok_control"), self.h)
        self.assertEqual(self.h.session_kind, "grok_session_id")
        self.assertFalse(self.h.owned_server)
        self.assertTrue(self.h.headless_worker)
        for route, stage in (("grok-4.6-build", "implementation_hard"),
                             ("grok-4.6-build", "recovery"),
                             ("grok-4.6-xai", "implementation_hard"),
                             ("grok-4.6-xai", "recovery")):
            self.assertIsNone(harnesses.route_capability_blocker(route, stage), (route, stage))
        self.assertEqual(harnesses.kind_for_cmd(["grok", "-p", "x"]), "grok_control")

    def test_session_and_report_parsing(self):
        out = json.dumps({"text": "IMPLEMENTED", "stopReason": "end_turn",
                          "sessionId": "ses_abc", "num_turns": 1,
                          "model": "grok-4.6",
                          "usage": {"input_tokens": 60, "output_tokens": 12}}) + "\n"
        self.assertEqual(self.h.parse_session("grok_control", out, "", None),
                         ("ses_abc", "grok_session_id"))
        self.assertEqual(self.h.parse_session("grok_control", "nothing\n", "", None),
                         (None, None))
        report = self.h.parse_report("grok_control", out, ["grok"])
        self.assertTrue(report["ok"])
        self.assertEqual(report["grok_session_id"], "ses_abc")
        self.assertEqual(report["assistant_text"], "IMPLEMENTED")
        self.assertEqual(report["finish"], "end_turn")
        self.assertEqual(report["actual_model"],
                         {"providerID": "xai", "modelID": "grok-4.6", "variant": None})
        self.assertEqual(report["usage"]["source"], "grok")
        self.assertEqual(report["usage"]["input_tokens"], 60)
        self.assertEqual(report["native_ids"], {"session_id": "ses_abc"})
        self.assertEqual(report["blockers"], [])
        self.assertIsNone(report["signal"])

    def test_error_report_carries_signal_evidence(self):
        out = json.dumps({"type": "error",
                          "message": "xAI subscription quota exceeded: insufficient_quota"})
        report = self.h.parse_report("grok_control", out, ["grok"])
        self.assertFalse(report["ok"])
        self.assertEqual(report["signal"], "exhausted")
        self.assertEqual(report["signal_evidence"]["source"], "grok")
        self.assertIn("insufficient_quota", report["error"])
        self.assertIsNone(report["usage"])  # unreported usage stays unknown

    def test_signal_classification(self):
        h = self.h
        self.assertEqual(h.classify_signal({"source": "grok",
                                            "message": "quota exceeded: insufficient_quota"}),
                         "exhausted")
        self.assertEqual(h.classify_signal({"source": "grok",
                                            "message": "RateLimitError, retry later"}),
                         "overloaded")
        self.assertEqual(h.classify_signal({"source": "grok", "message": "HTTP 503"}),
                         "overloaded")
        self.assertEqual(h.classify_signal({"source": "grok", "status": 503,
                                            "message": "busy"}), "overloaded")
        self.assertEqual(h.classify_signal({"source": "grok",
                                            "message": "context_length_exceeded"}), "hard")
        self.assertEqual(h.classify_signal({"source": "grok", "status": 401,
                                            "message": "unauthorized"}), "hard")
        self.assertIsNone(h.classify_signal({"source": "grok", "message": "all good"}))
        self.assertIsNone(h.classify_signal("worker prose is never evidence"))
        # Standard policy shapes still classify through the seam.
        self.assertEqual(h.classify_signal(
            {"type": "retry", "action": {"reason": "free_tier_limit"}}), "exhausted")

    def test_measure_reports_usage_only_where_reported(self):
        out = json.dumps({"text": "t", "stopReason": "end_turn",
                          "sessionId": "ses_m", "num_turns": 3}) + "\n"
        usage, observed, variant, ids = self.h.measure("grok_control", out, "", None)
        self.assertEqual(usage, {"source": "grok", "num_turns": 3})
        self.assertIsNone(observed)
        self.assertIsNone(variant)
        self.assertEqual(ids, {"session_id": "ses_m"})
        usage2, _, _, _ = self.h.measure("grok_control", "no json\n", "", None)
        self.assertIsNone(usage2)

    def test_turn_ok_identity_and_infer(self):
        ok = json.dumps({"text": "t", "stopReason": "end_turn", "sessionId": "s"}) + "\n"
        self.assertTrue(self.h.turn_ok("grok_control", 0, ok, ["grok"]))
        self.assertFalse(self.h.turn_ok("grok_control", 1, ok, ["grok"]))
        self.assertFalse(self.h.turn_ok("grok_control", 0, "nope\n", ["grok"]))
        self.assertEqual(self.h.infer_rc("grok_control", ok, ["grok"]), 0)
        self.assertEqual(self.h.infer_rc("grok_control", "nope\n", ["grok"]), 1)
        self.assertTrue(self.h.identity_ok("grok_control", "s", "s"))
        self.assertTrue(self.h.identity_ok("grok_control", "s", None))
        self.assertFalse(self.h.identity_ok("grok_control", "other", "s"))
        self.assertFalse(self.h.identity_ok("grok_control", None, "s"))


class GrokPolicy(unittest.TestCase):
    def test_build_route_is_native_first_with_opencode_fallback(self):
        spec = policy.route_spec("grok-4.6-build")
        self.assertEqual((spec["harness"], spec["pool"], spec["model"], spec["variant"]),
                         ("grok", "xai", "grok-4.6", "medium"))
        self.assertEqual(spec["family"], "grok")
        self.assertEqual(spec["next_pool"], "grok-4.6-xai")
        fallback = policy.route_spec("grok-4.6-xai")
        self.assertEqual((fallback["harness"], fallback["pool"], fallback["model"]),
                         ("opencode", "xai", "xai/grok-4.6"))
        self.assertIsNone(policy.next_pool_route("grok-4.6-xai"))
        self.assertEqual(policy.route_allowance("grok-4.6-build"), "xai-subscription")
        self.assertEqual(policy.route_pool("grok-4.6-build"), "xai")

    def test_worker_model_variant(self):
        self.assertEqual(policy.worker_model_variant("grok-4.6-build"), ("grok-4.6", "medium"))
        self.assertEqual(policy.worker_model_variant("grok-4.6-xai"),
                         ("xai/grok-4.6", "medium"))
        with self.assertRaises(ValueError):
            policy.worker_model_variant("luna/max")

    def test_recovery_and_hard_lane_use_both_xai_routes(self):
        self.assertEqual(policy.STAGES["recovery"]["routes"],
                         ["grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"])
        self.assertEqual(policy.STAGES["implementation_hard"]["routes"][-3:],
                         ["grok-4.6-go", "grok-4.6-build", "grok-4.6-xai"])
        # Dual-listed Grok rungs reason in recovery once the stored lane is
        # consulted: recovery moves stay in recovery from any lane, and the
        # hard tail order matches the recovery order rung for rung.
        self.assertEqual(policy.lane_of_route("grok-4.6-build", "hard"),
                         "recovery")
        self.assertEqual(policy.lane_of_route("grok-4.6-build", "recovery"),
                         "recovery")
        self.assertEqual(policy.lane_of_route("grok-4.6-build"), "implementation_hard")
        self.assertEqual(policy.lane_of_route("grok-4.6-xai", "hard"), "recovery")
        self.assertEqual(policy.next_pool_route("grok-4.6-build"), "grok-4.6-xai")

    def test_skill_table_shows_the_native_route(self):
        rendered = policy.render_skill_table()
        self.assertIn("`grok-4.6-build`", rendered)
        self.assertIn("`grok-4.6-build`, `grok-4.6-xai`", rendered)
        on_disk = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        self.assertEqual(on_disk, rendered)


class GrokBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.saved_env = {k: os.environ.get(k) for k in
                          ("PATH", "FAKE_STATE", "FAKE_GROK_MODE", "FAKE_GROK_DELAY",
                           "FAKE_GROK_WRITE", "FAKE_GROK_RELEASE", "FAKE_OC_MODE",
                           "PYTHONDONTWRITEBYTECODE")}
        self.addCleanup(self._restore_env)

    def _restore_env(self):
        for k, v in self.saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _bindir(self):
        bindir = self.base / "bin"
        bindir.mkdir(exist_ok=True)
        return bindir

    def _env(self, **extra):
        env = dict(os.environ)
        env["PATH"] = str(self._bindir()) + os.pathsep + env.get("PATH", "")
        env["FAKE_STATE"] = str(self.base / "fakestate")
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env.update(extra)
        return env

    def _submit_build(self, rid="g1"):
        return core.submit(self.sd, rid, {"goal": "grok turn"}, str(self.ws),
                           "planner-1", route="grok-4.6-build", lane="hard")

    def _kill_groups(self, rid):
        for inv in core._list_invocations(self.sd, rid):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass
        try:
            job = core.get_job(self.sd, rid)
            if job.get("owner_pid"):
                kill_pid(job["owner_pid"])
        except Exception:
            pass


def _grok_ok(sid="ses_grok_1", text="IMPLEMENTED by grok"):
    return 0, json.dumps({"text": text, "stopReason": "end_turn", "sessionId": sid,
                          "num_turns": 1, "model": "grok-4.6",
                          "usage": {"input_tokens": 60, "output_tokens": 12}}) + "\n", ""


def _grok_error(message):
    return 1, json.dumps({"type": "error", "message": message}) + "\n", ""


class GrokControllerInjected(GrokBase):
    def test_success_writes_report(self):
        self._submit_build()
        res = controller.run_implementation(self.sd, "g1", run_cmd=_inj(_grok_ok()))
        self.assertEqual(res["action"], "implementation_ok")
        self.assertEqual(res["session"], "ses_grok_1")
        report = res["report"]
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["route"], "grok-4.6-build")
        self.assertEqual((report["model"], report["variant"]), ("grok-4.6", "medium"))
        self.assertEqual(report["observed_model"], "xai/grok-4.6")
        self.assertEqual(report["tokens"]["source"], "grok")
        self.assertEqual(report["native_ids"], {"session_id": "ses_grok_1"})
        on_disk = json.loads(Path(report["report_path"]).read_text())
        self.assertEqual(on_disk["session_id"], "ses_grok_1")
        job = core.get_job(self.sd, "g1")
        self.assertEqual(json.loads(job["controller_state"])["grok_session_id"], "ses_grok_1")
        # The worker turn is not a dispatcher turn: no Luna envelope is consumed.
        self.assertEqual(job["status"], "pending")

    def test_resume_uses_the_saved_session(self):
        self._submit_build()
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET grok_session_id='ses_saved_9' WHERE request_id='g1'")
        finally:
            con.close()
        seen = {}

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            seen["cmd"] = cmd
            self.assertEqual(kind, "grok_control")
            self.assertEqual(meta["route"], "grok-4.6-build")
            return _grok_ok("ses_saved_9")

        res = controller.run_implementation(self.sd, "g1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        cmd = seen["cmd"]
        self.assertEqual(cmd[cmd.index("--resume") + 1], "ses_saved_9")
        self.assertEqual(cmd[cmd.index("--cwd") + 1],
                         core.get_job(self.sd, "g1")["workspace"])

    def test_forked_session_never_replaces_the_saved_one(self):
        self._submit_build()
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET grok_session_id='ses_saved_9' WHERE request_id='g1'")
        finally:
            con.close()
        res = controller.run_implementation(self.sd, "g1", run_cmd=_inj(_grok_ok("ses_other")))
        self.assertEqual(res["action"], "blocked")
        self.assertEqual(res["reason"], "luna_task_mismatch")
        job = core.get_job(self.sd, "g1")
        self.assertEqual(job["status"], "blocked")
        self.assertEqual(job["grok_session_id"], "ses_saved_9")

    def test_exhaustion_pool_moves_to_the_opencode_fallback(self):
        self._submit_build()
        res = controller.run_implementation(
            self.sd, "g1",
            run_cmd=_inj(_grok_error("xAI subscription quota exceeded: insufficient_quota")))
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "pool_move"))
        self.assertEqual(res["route"], "grok-4.6-xai")
        job = core.get_job(self.sd, "g1")
        self.assertEqual((job["route"], job["model"]), ("grok-4.6-xai", "xai/grok-4.6"))
        self.assertIn("grok-4.6-build", core.exhausted_routes(self.sd))
        cap = [r for r in core.list_capacity(self.sd) if r["route"] == "grok-4.6-build"][0]
        self.assertEqual((cap["state"], cap["pool"], cap["model"]),
                         ("exhausted", "xai", "grok-4.6"))
        self.assertIn("insufficient_quota", cap["evidence_json"])
        self.assertEqual(res["report"]["status"], "exhausted")

    def test_exhaustion_without_fallback_blocks(self):
        self._submit_build()
        core.record_capacity(self.sd, "grok-4.6-xai", "exhausted", {"source": "test"})
        res = controller.run_implementation(
            self.sd, "g1",
            run_cmd=_inj(_grok_error("quota exceeded: insufficient_quota")))
        self.assertEqual((res["action"], res["reason"]), ("blocked", "capacity_exhausted"))

    def test_overload_moves_laterally_or_blocks_at_the_last_rung(self):
        self._submit_build()
        res = controller.run_implementation(
            self.sd, "g1",
            run_cmd=_inj(_grok_error("xAI rate limit exceeded: RateLimitError")))
        # Recovery ignores the family filter (same model across pools), so
        # overload on the native route falls back to OpenCode's xAI provider.
        self.assertEqual((res["action"], res["reason"]), ("route_switched", "lateral"))
        self.assertEqual(res["route"], "grok-4.6-xai")
        self.assertIn("grok-4.6-build", core.degraded_routes(self.sd))
        self.assertEqual(res["report"]["status"], "overloaded")
        job = core.get_job(self.sd, "g1")
        self.assertEqual((job["route"], job["model"]), ("grok-4.6-xai", "xai/grok-4.6"))
        # Overload on the last recovery rung ends the lane.
        res = controller._move_after_signal(self.sd, "g1", "grok-4.6-xai", "overloaded",
                                            {"name": "RateLimitError"})
        self.assertEqual((res["action"], res["reason"]), ("blocked", "capacity_exhausted"))

    def test_hard_error_ends_the_turn_failed(self):
        self._submit_build()
        res = controller.run_implementation(
            self.sd, "g1",
            run_cmd=_inj(_grok_error("context_length_exceeded: maximum context exceeded")))
        self.assertEqual(res["action"], "implementation_failed")
        job = core.get_job(self.sd, "g1")
        self.assertNotEqual(job["status"], "blocked")
        self.assertEqual(job["route"], "grok-4.6-build")
        self.assertEqual(json.loads(Path(res["report"]["report_path"]).read_text())["status"],
                         "failed")
        # The injected path records child calls, not invocations; the
        # durable measurements live in GrokDurable.
        controller._record_turn_outcome(self.sd, "g1", res)
        self.assertEqual(controller._ladder(core.get_job(self.sd, "g1"))["failures"], 1)
        self.assertNotIn("grok-4.6-build", core.exhausted_routes(self.sd))
        self.assertNotIn("grok-4.6-build", core.degraded_routes(self.sd))

    def test_timeout_blocks_with_a_report(self):
        self._submit_build()
        res = controller.run_implementation(self.sd, "g1", run_cmd=_inj((124, "", "")))
        self.assertEqual(res["action"], "blocked")
        self.assertEqual(res["reason"], "implementation_failed")
        self.assertEqual(res["report"]["status"], "failed")

    def test_preflight_skips_an_exhausted_build_without_a_child(self):
        self._submit_build()
        core.record_capacity(self.sd, "grok-4.6-build", "exhausted", {"source": "test"})

        def nope(*a, **k):
            raise AssertionError("exhausted route must not spawn")

        res = controller.run_implementation(self.sd, "g1", run_cmd=nope)
        self.assertEqual((res["action"], res["reason"], res["route"]),
                         ("route_switched", "preflight_exhausted", "grok-4.6-xai"))


def _inj(result):
    def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
        return result
    return run


class GrokDurable(GrokBase):
    def _setup(self, mode="ok", rid="gd1"):
        write_fake(self._bindir(), "grok", FAKE_GROK, PY)
        for k, v in self._env(FAKE_GROK_MODE=mode).items():
            os.environ[k] = v
        core.submit(self.sd, rid, {"goal": "durable grok"}, str(self.ws),
                    "planner-1", route="grok-4.6-build", lane="hard")
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id=?", (rid,))
        finally:
            con.close()
        self.addCleanup(self._kill_groups, rid)
        return core.make_durable_run_cmd(self.sd, rid, "tok")

    def test_success_persists_session_and_measurements(self):
        run = self._setup("ok")
        res = controller.run_implementation(self.sd, "gd1", run_cmd=run)
        self.assertEqual(res["action"], "implementation_ok")
        job = core.get_job(self.sd, "gd1")
        sid = job["grok_session_id"]
        self.assertTrue(sid.startswith("ses_grok_"))
        self.assertEqual(job["adapter"], "grok")
        self.assertEqual(res["report"]["native_ids"], {"session_id": sid})
        inv = [i for i in core._list_invocations(self.sd, "gd1")
               if i["kind"] == "grok_control"][-1]
        self.assertEqual(inv["session_kind"], "grok_session_id")
        self.assertEqual(inv["terminal_class"], "completed")
        self.assertGreater(inv["elapsed_secs"], 0)
        self.assertEqual(inv["observed_model"], "xai/grok-4.6")
        self.assertEqual(json.loads(inv["usage_json"])["source"], "grok")
        self.assertEqual(json.loads(inv["native_ids_json"]), {"session_id": sid})
        self.assertEqual(inv["report_path"], res["report"]["report_path"])

    def test_second_turn_resumes_the_saved_session(self):
        run = self._setup("ok")
        res1 = controller.run_implementation(self.sd, "gd1", run_cmd=run)
        sid = core.get_job(self.sd, "gd1")["grok_session_id"]
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET controller_state=? WHERE request_id='gd1'",
                        (json.dumps({"seq": 1}),))
        finally:
            con.close()
        res2 = controller.run_implementation(self.sd, "gd1", run_cmd=run)
        self.assertEqual(res2["action"], "implementation_ok")
        self.assertEqual(core.get_job(self.sd, "gd1")["grok_session_id"], sid)
        calls = worker_calls(str(self.base / "fakestate"))
        self.assertEqual(len(calls), 2)
        second = calls[1]["argv"]
        self.assertEqual(second[second.index("--resume") + 1], sid)
        self.assertTrue((store.job_dir_for(store.ensure_state_dir(self.sd), "gd1")
                         / "turn-1" / "report.json").exists())

    def test_hung_worker_is_killed_and_recorded_before_the_timeout(self):
        # The default 180-second window is clamped strictly below the
        # 2-second turn timeout (toolboxmd/model-router#73), so a silent
        # hang is killed and recorded as stalled before the deadline
        # instead of running to it.
        run = self._setup("hang")
        cmd, _env = adapters.build_grok_cmd("hang on", str(self.ws), "grok-4.6", "medium")
        rc, out, err = run(cmd, str(self.ws), 2, kind="grok_control",
                           meta={"stage": "implementation", "route": "grok-4.6-build",
                                 "reason": "test", "seq": 0})
        self.assertEqual(rc, 4)
        inv = [i for i in core._list_invocations(self.sd, "gd1")
               if i["kind"] == "grok_control"][-1]
        self.assertEqual(inv["terminal_class"], "stalled")
        result = json.loads(inv["result_json"] or "{}")
        self.assertEqual(result.get("signal"), "stalled")
        self.assertLess(result["signal_evidence"]["window_secs"], 2)
        self.assertGreater(inv["elapsed_secs"], 0)
        # The turn's own measured duration stays inside the outer budget.
        self.assertLess(inv["elapsed_secs"], 2)

    def test_finished_action_reuses_without_a_second_writer(self):
        run = self._setup("ok")
        cmd, _env = adapters.build_grok_cmd("same turn", str(self.ws), "grok-4.6", "medium")
        meta = {"stage": "implementation", "route": "grok-4.6-build",
                "reason": "test", "seq": 0, "prompt": "same turn"}
        rc1, out1, _ = run(cmd, str(self.ws), 30, kind="grok_control", meta=meta)
        self.assertEqual(rc1, 0)
        rc2, out2, _ = run(cmd, str(self.ws), 30, kind="grok_control", meta=meta)
        self.assertEqual((rc2, out2), (rc1, out1))
        self.assertEqual(len(worker_calls(str(self.base / "fakestate"))), 1)


class GrokPublicCLI(GrokBase):
    def _write_fakes(self, grok_mode="ok", oc_mode="ok", extra_env=None):
        write_fake(self._bindir(), "grok", FAKE_GROK, PY)
        write_fake(self._bindir(), "codex", FAKE_CODEX_DISPATCH, PY)
        write_fake(self._bindir(), "opencode", FAKE_OPENCODE, PY)
        env = self._env(FAKE_GROK_MODE=grok_mode, FAKE_OC_MODE=oc_mode)
        if extra_env:
            env.update(extra_env)
        return env

    def _cleanup(self, sd, rid):
        try:
            job = core.get_job(sd, rid)
            if job.get("owner_pid"):
                kill_pid(job["owner_pid"])
        except Exception:
            pass
        try:
            for inv in core._list_invocations(sd, rid):
                for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                    if pg:
                        try:
                            os.killpg(int(pg), signal.SIGKILL)
                        except Exception:
                            pass
        except Exception:
            pass

    def test_success_end_to_end(self):
        env = self._write_fakes("ok")
        rid = "grok-e2e-ok"
        self.addCleanup(self._cleanup, self.sd, rid)
        rc, out, err = cli(self.sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"e2e grok success"}',
                           "--workspace", str(self.ws), "--planner-session", "p-e2e",
                           "--route", "grok-4.6-build",
                           "--start", env=env, timeout=60)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: core.get_job(self.sd, rid)["status"] == "succeeded", 60),
                        core.get_job(self.sd, rid))
        job = core.get_job(self.sd, rid)
        self.assertIn("E2E_DONE", job.get("result_json") or "")
        self.assertEqual(job["route"], "grok-4.6-build")
        self.assertTrue((job["grok_session_id"] or "").startswith("ses_grok_"))
        calls = worker_calls(str(self.base / "fakestate"))
        self.assertEqual(len(calls), 1)
        argv = calls[0]["argv"]
        for flag in ("-p", "--verbatim", "--cwd", "-m", "--effort",
                     "--always-approve", "--disable-web-search", "--no-subagents",
                     "--output-format"):
            self.assertIn(flag, argv)
        self.assertEqual(argv[argv.index("-m") + 1], "grok-4.6")
        report = json.loads(sorted(
            (store.job_dir_for(store.ensure_state_dir(self.sd), rid)).glob("turn-*/report.json"),
            key=str)[0].read_text())
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["tokens"]["source"], "grok")
        self.assertEqual(report["native_ids"], {"session_id": job["grok_session_id"]})
        inv = [i for i in core._list_invocations(self.sd, rid)
               if i["kind"] == "grok_control"][-1]
        self.assertEqual(inv["terminal_class"], "completed")

    def test_exhaustion_falls_back_to_the_opencode_xai_provider(self):
        env = self._write_fakes("exhaustion", "ok")
        rid = "grok-e2e-poolfallback"
        self.addCleanup(self._cleanup, self.sd, rid)
        rc, out, err = cli(self.sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"e2e grok exhaustion"}',
                           "--workspace", str(self.ws), "--planner-session", "p-e2e",
                           "--route", "grok-4.6-build",
                           "--start", env=env, timeout=60)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: core.get_job(self.sd, rid)["status"] == "succeeded", 60),
                        core.get_job(self.sd, rid))
        job = core.get_job(self.sd, rid)
        # The native turn exhausted; the same model on OpenCode's xAI
        # provider finished the work. That pool move is not an escalation.
        self.assertEqual(job["route"], "grok-4.6-xai")
        self.assertIn("grok-4.6-build", core.exhausted_routes(self.sd))
        self.assertEqual(len(worker_calls(str(self.base / "fakestate"))), 1)
        reqs = [json.loads(l) for l in
                (self.base / "fakestate" / "opencode-requests.jsonl").read_text().splitlines()]
        models = [r["body"]["model"] for r in reqs if r["path"].endswith("/prompt_async")]
        self.assertIn({"providerID": "xai", "modelID": "grok-4.6"}, models)

    def test_overload_blocks_at_the_last_rung(self):
        env = self._write_fakes("overload", "ok")
        rid = "grok-e2e-overload"
        self.addCleanup(self._cleanup, self.sd, rid)
        rc, out, err = cli(self.sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"e2e grok overload"}',
                           "--workspace", str(self.ws), "--planner-session", "p-e2e",
                           "--route", "grok-4.6-build",
                           "--start", env=env, timeout=60)
        self.assertEqual(rc, 0, err)
        # Overload on the native route is a lateral recovery move to the
        # OpenCode xAI fallback, which then finishes the work. Blocking at
        # the last rung is covered by the injected last-rung test.
        self.assertTrue(wait_for(lambda: core.get_job(self.sd, rid)["status"] == "succeeded", 60),
                        core.get_job(self.sd, rid))
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["route"], "grok-4.6-xai")
        self.assertIn("E2E_DONE", job.get("result_json") or "")
        self.assertIn("grok-4.6-build", core.degraded_routes(self.sd))
        self.assertEqual(len(worker_calls(str(self.base / "fakestate"))), 1)
        reqs = [json.loads(l) for l in
                (self.base / "fakestate" / "opencode-requests.jsonl").read_text().splitlines()]
        models = [r["body"]["model"] for r in reqs if r["path"].endswith("/prompt_async")]
        self.assertIn({"providerID": "xai", "modelID": "grok-4.6"}, models)

    def test_controller_death_resumes_without_a_second_writer(self):
        release = self.base / "grok-release"
        env = self._write_fakes("hold", "ok",
                                extra_env={"FAKE_GROK_RELEASE": str(release)})
        rid = "grok-e2e-death"
        self.addCleanup(self._cleanup, self.sd, rid)
        rc, out, err = cli(self.sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"e2e grok death"}',
                           "--workspace", str(self.ws), "--planner-session", "p-e2e",
                           "--route", "grok-4.6-build",
                           "--start", env=env, timeout=60)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(
            lambda: worker_calls(str(self.base / "fakestate")), 30),
            "the grok worker never started")
        job = core.get_job(self.sd, rid)
        self.assertIsNotNone(job.get("owner_pid"))
        kill_pid(job["owner_pid"])  # the controller dies mid-turn
        release.write_text("finish")
        self.assertTrue(wait_for(
            lambda: [i for i in core._list_invocations(self.sd, rid)
                     if i["state"] not in ("running", "cancelling")], 30),
            "the orphaned turn never finished")
        rc, rec, err = cli(self.sd, "recover", "--request-id", rid, env=env, timeout=60)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(lambda: core.get_job(self.sd, rid)["status"] == "succeeded", 60),
                        core.get_job(self.sd, rid))
        job = core.get_job(self.sd, rid)
        self.assertIn("E2E_DONE", job.get("result_json") or "")
        self.assertTrue((job["grok_session_id"] or "").startswith("ses_grok_"))
        self.assertEqual(len(worker_calls(str(self.base / "fakestate"))), 1,
                         "recovery must adopt the finished turn, never copy it")


class WorkerKit(unittest.TestCase):
    KIT = ROOT / "worker-kits" / "grok" / "ensure.py"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.src = self.base / "agentsmd"
        (self.src / "bin").mkdir(parents=True)
        (self.src / "AGENTS.md").write_text("# agents\n")
        (self.src / "bin" / "project-direction").write_text("#!/bin/sh\n")
        self.src = self.src.resolve()  # macOS /tmp aliases must match --source
        self.home = self.base / "grok-home"

    def _run(self, *args):
        p = subprocess.run([PY, str(self.KIT), *args], capture_output=True, text=True,
                           timeout=20, cwd=str(ROOT))
        try:
            out = json.loads(p.stdout) if p.stdout.strip() else {}
        except ValueError:
            out = {"raw": p.stdout}
        return p.returncode, out, p.stderr

    def test_ensure_creates_link_and_hook_and_is_idempotent(self):
        rc, out, err = self._run("--source", str(self.src), "--home", str(self.home))
        self.assertEqual(rc, 0, err)
        self.assertEqual(out["link"]["status"], "created")
        self.assertEqual(out["hook"]["status"], "created")
        self.assertEqual(os.readlink(self.home / "AGENTS.md"), str(self.src / "AGENTS.md"))
        hook = json.loads((self.home / "hooks" / "agentsmd.json").read_text())
        self.assertEqual(hook["agentsmd"]["source"], str(self.src))
        self.assertIn("project-direction", json.dumps(hook["hooks"]))
        rc, out, err = self._run("--source", str(self.src), "--home", str(self.home))
        self.assertEqual(rc, 0, err)
        self.assertEqual((out["link"]["status"], out["hook"]["status"]),
                         ("owned-current", "owned-current"))
        rc, out, err = self._run("--source", str(self.src), "--home", str(self.home),
                                 "--check")
        self.assertEqual(rc, 0, err)

    def test_check_reports_missing_and_divergence(self):
        rc, out, _ = self._run("--source", str(self.src), "--home", str(self.home), "--check")
        self.assertEqual(rc, 2)
        self.assertEqual((out["link"]["status"], out["hook"]["status"]),
                         ("missing", "missing"))
        (self.home / "hooks").mkdir(parents=True)
        (self.home / "hooks" / "agentsmd.json").write_text("{}\n")
        rc, out, _ = self._run("--source", str(self.src), "--home", str(self.home), "--check")
        self.assertEqual(rc, 2)
        self.assertEqual(out["hook"]["status"], "divergent")

    def test_divergent_hook_needs_replace_and_is_backed_up(self):
        self._run("--source", str(self.src), "--home", str(self.home))
        (self.home / "hooks" / "agentsmd.json").write_text("{}\n")
        rc, out, _ = self._run("--source", str(self.src), "--home", str(self.home))
        self.assertEqual(rc, 1)
        self.assertIn("error", out["hook"])
        self.assertEqual((self.home / "hooks" / "agentsmd.json").read_text(), "{}\n")
        rc, out, _ = self._run("--source", str(self.src), "--home", str(self.home),
                               "--replace")
        self.assertEqual(rc, 0)
        self.assertEqual(out["hook"]["status"], "replaced")
        self.assertTrue(Path(out["hook"]["backup"]).exists())
        hook = json.loads((self.home / "hooks" / "agentsmd.json").read_text())
        self.assertEqual(hook["agentsmd"]["installer"], "agentsmd-grok-hook")

    def test_user_owned_link_target_is_never_overwritten(self):
        self.home.mkdir(parents=True)
        (self.home / "AGENTS.md").write_text("my own notes\n")
        rc, out, _ = self._run("--source", str(self.src), "--home", str(self.home))
        self.assertEqual(rc, 1)
        self.assertEqual(out["link"]["status"], "user-owned")
        self.assertEqual((self.home / "AGENTS.md").read_text(), "my own notes\n")

    def test_hook_document_matches_the_agentsmd_installer_shape(self):
        self._run("--source", str(self.src), "--home", str(self.home))
        hook = json.loads((self.home / "hooks" / "agentsmd.json").read_text())
        self.assertEqual(set(hook), {"agentsmd", "hooks"})
        self.assertEqual(hook["hooks"]["PreToolUse"][0]["hooks"][0]["type"], "command")
        self.assertEqual(hook["hooks"]["PreToolUse"][0]["hooks"][0]["command"],
                         f'"{self.src}/bin/project-direction" hook')


class GrokStore(unittest.TestCase):
    def test_jobs_carry_a_grok_session(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        ws = Path(tmp.name) / "ws"
        ws.mkdir()
        job = core.submit(sd, "gs1", {"g": 1}, str(ws), "p1",
                          route="grok-4.6-build", lane="hard")
        self.assertIn("grok_session_id", job)
        self.assertIsNone(job["grok_session_id"])
        self.assertEqual((job["model"], job["effort"]), ("grok-4.6", "medium"))


if __name__ == "__main__":
    unittest.main()
