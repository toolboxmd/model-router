"""Issue #88: no elapsed-time termination of active agent turns or jobs.

Deterministic only. No live model CLIs, no 30-minute waits: small legacy
timeout values (2-3 seconds) stand in for the removed 1800/900-second caps,
and short real subprocesses prove active work continues past them.

Before #88 these drills failed: the supervisor killed the active turn with
rc 124 at the deadline, the owned-server drive reported
"implementation turn timed out", and recovery drained a live job whose age
passed ``timeout_secs``. After #88 the legacy values are recorded but never
enforced: active turns complete, recovery adopts live work, and only
explicit cancellation, real failures, and genuine stream silence end turns.

Unknown-ownership safeguards stay covered by
``test_review_fixes.TestRoundFour.test_unknown_identity_of_the_lease_holder_is_not_adopted``;
legacy timeout-drain finalization (``cancel_requested=2``) by
``test_review_fixes.TestRoundFour.test_timeout_origin_is_kept_after_a_pending_drain``;
and explicit-cancel precedence by
``test_review_fixes.TestRoundThree.test_cancellation_takes_precedence_over_timeout``.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import adapters, controller, core, harnesses, store  # noqa: E402
from tests.fakes import FAKE_GROK, FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable


def _tmp(testcase):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    testcase.addCleanup(tmp.cleanup)
    base = Path(tmp.name)
    sd = str(base / "state")
    ws = base / "ws"
    ws.mkdir()
    return sd, ws, base


def _kill_groups(sd, rid):
    try:
        invs = core._list_invocations(sd, rid)
    except Exception:
        return
    for inv in invs:
        for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
            if pg:
                try:
                    os.killpg(int(pg), signal.SIGKILL)
                except Exception:
                    pass


def _wait_for(fn, secs=15.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if fn():
            return True
        time.sleep(0.05)
    return False


class TestNoDefaultDeadline(unittest.TestCase):
    def test_timeout_for_returns_none_for_every_kind(self):
        seen = set()
        for harness in (harnesses.harness_named(n) for n in ("codex", "claude", "opencode", "grok")):
            for kind in harness.kinds:
                seen.add(kind)
                self.assertIsNone(harness.timeout_for(kind), kind)
        # Every historical per-turn cap kind is covered: nothing falls back
        # to a fixed deadline anymore.
        self.assertTrue({"codex_dispatch", "codex_resume", "claude_callback",
                         "opencode_control", "grok_control"} <= seen)

    def test_no_harness_carries_a_timeout_table(self):
        for name in ("codex", "claude", "opencode", "grok"):
            self.assertFalse(hasattr(harnesses.harness_named(name), "timeouts"), name)

    def test_stall_window_ignores_the_legacy_timeout_slot(self):
        h = harnesses.harness_named("codex")
        self.assertEqual(h.effective_stall_window_secs({}, 1800), 300.0)
        self.assertEqual(h.effective_stall_window_secs({}, 2), 300.0)
        self.assertEqual(h.effective_stall_window_secs({}, None), 300.0)


class TestActiveCliTurnOutlivesLegacyTimeout(unittest.TestCase):
    """Generic supervisor path (codex/claude/grok CLIs): steady stream
    activity for ~7s with a legacy 3s timeout completes rc 0."""

    def test_active_turn_completes_past_the_legacy_cap(self):
        sd, ws, base = _tmp(self)
        bindir = base / "bin"
        bindir.mkdir()
        script = bindir / "codex"
        script.write_text(
            "#!" + PY + "\n"
            "import json, sys, time\n"
            "for i in range(24):\n"
            "    print(json.dumps({'type': 'item.completed', 'i': i}), flush=True)\n"
            "    time.sleep(0.3)\n")
        script.chmod(0o700)
        core.submit(sd, "nc1", {"g": 1}, str(ws), "p", timeout_secs=3)
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='nc1'")
        finally:
            con.close()
        self.addCleanup(_kill_groups, sd, "nc1")
        started = time.monotonic()
        rc, out, err = core._durable_run(
            sd, "nc1", "tok", "codex_dispatch", [str(script)],
            cwd=str(ws), timeout=3,
            meta={"stage": "dispatch", "route": "luna/max", "reason": "test",
                  "stall_secs": 30})
        elapsed = time.monotonic() - started
        self.assertEqual(rc, 0, err)
        self.assertGreater(elapsed, 3.0)
        inv = core._list_invocations(sd, "nc1")[-1]
        result = json.loads(inv["result_json"] or "{}")
        self.assertIsNone(result.get("signal"))
        self.assertEqual(inv["terminal_class"], "completed")
        self.assertGreater(inv["elapsed_secs"], 3.0)

    def test_genuine_silence_still_stalls(self):
        sd, ws, base = _tmp(self)
        bindir = base / "bin"
        bindir.mkdir()
        script = bindir / "codex"
        script.write_text("#!" + PY + "\nimport time\ntime.sleep(60)\n")
        script.chmod(0o700)
        core.submit(sd, "nc2", {"g": 1}, str(ws), "p")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='nc2'")
        finally:
            con.close()
        self.addCleanup(_kill_groups, sd, "nc2")
        rc, out, err = core._durable_run(
            sd, "nc2", "tok", "codex_dispatch", [str(script)],
            cwd=str(ws), timeout=30,
            meta={"stage": "dispatch", "route": "luna/max", "reason": "test",
                  "stall_secs": 1})
        self.assertEqual(rc, 4)
        inv = core._list_invocations(sd, "nc2")[-1]
        result = json.loads(inv["result_json"] or "{}")
        self.assertEqual(result.get("signal"), "stalled")
        self.assertEqual(inv["terminal_class"], "stalled")
        self.assertIn("stalled", err)


class TestActiveOwnedServerTurnOutlivesLegacyTimeout(unittest.TestCase):
    """Owned-server drive path: a 5s busy worker turn with a legacy 2s
    invocation timeout reaches implementation_ok instead of
    "implementation turn timed out"."""

    def test_busy_worker_completes_past_the_legacy_cap(self):
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
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_OC_MODE",
                                                "FAKE_OC_DELAY")}
        try:
            os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
            os.environ["FAKE_STATE"] = str(fake_state)
            os.environ["FAKE_OC_MODE"] = "ok"
            os.environ["FAKE_OC_DELAY"] = "5"
            core.submit(sd, "oc88", {"goal": "slow worker"}, str(ws), "planner")
            con = store.connect(sd)
            try:
                con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='oc88'")
            finally:
                con.close()
            self.addCleanup(_kill_groups, sd, "oc88")
            run = core.make_durable_run_cmd(sd, "oc88", "tok")

            def timed_run(cmd, cwd=None, timeout=None, **kw):
                return run(cmd, cwd, 2, **kw)  # legacy per-turn value, now ignored

            started = time.monotonic()
            res = controller.run_implementation(sd, "oc88", run_cmd=timed_run)
            elapsed = time.monotonic() - started
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.assertEqual(res["action"], "implementation_ok", res)
        self.assertGreater(elapsed, 2.0)
        inv = [i for i in core._list_invocations(sd, "oc88")
               if i["kind"] == "opencode_control"][-1]
        self.assertEqual(inv["terminal_class"], "completed")


class _LiveChildBase(unittest.TestCase):
    KIND = "claude_callback"  # generic path; never a dispatch row, so recover adopts without spawning
    META = {"stage": "planning", "reason": "test"}

    def _start_live_sleep(self, rid, timeout):
        run = core.make_durable_run_cmd(self.sd, rid, "tok")
        box = {}

        def target():
            box["res"] = run(["sleep", "30"], str(self.ws), timeout,
                             kind=self.KIND, meta=dict(self.META))
        t = threading.Thread(target=target, daemon=True)
        t.start()
        self.addCleanup(t.join, 20)
        self.assertTrue(_wait_for(lambda: any(
            i.get("pid") for i in core._list_invocations(self.sd, rid))),
            "supervisor never recorded the child")
        inv = core._list_invocations(self.sd, rid)[-1]
        return inv

    def _child_alive(self, rid):
        inv = core._list_invocations(self.sd, rid)[-1]
        try:
            os.kill(int(inv["pid"]), 0)
        except Exception:
            return False
        return True


class TestRecoveryIgnoresLegacyJobTimeout(_LiveChildBase):
    """A live job whose age passed its legacy ``timeout_secs`` is adopted,
    never drained: recover returns adopted-live-invocation with the child
    still running."""

    def test_recover_adopts_instead_of_timing_out(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.sd = str(Path(tmp.name) / "state")
        self.ws = Path(tmp.name) / "ws"
        self.ws.mkdir()
        core.submit(self.sd, "rj1", {"g": 1}, str(self.ws), "p", timeout_secs=1)
        con = store.connect(self.sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok',"
                        " created_at='2000-01-01T00:00:00+00:00' WHERE request_id='rj1'")
        finally:
            con.close()
        self.addCleanup(_kill_groups, self.sd, "rj1")
        self._start_live_sleep("rj1", timeout=1)
        out = core.recover_one(self.sd, "rj1")
        self.assertNotEqual(out.get("action"), "timeout")
        self.assertTrue(self._child_alive("rj1"),
                        "recovery must not drain a live job for its age")
        job = core.get_job(self.sd, "rj1")
        self.assertNotIn(job["status"], ("failed", "cancelled"))
        self.assertNotEqual(job.get("error_class"), "timeout")
        # Explicit cancellation still stops the same live child.
        core.cancel(self.sd, "rj1")
        self.assertTrue(_wait_for(lambda: not self._child_alive("rj1"), secs=15))
        self.assertEqual(core.get_job(self.sd, "rj1")["status"], "cancelled")


class TestLegacyTimeoutRecordedButUnenforced(unittest.TestCase):
    def test_passed_timeout_is_stored_and_readable(self):
        sd, ws, base = _tmp(self)
        core.submit(sd, "lr1", {"g": 1}, str(ws), "p", timeout_secs=60)
        self.assertEqual(core.get_job(sd, "lr1")["timeout_secs"], 60)
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='lr1'")
        finally:
            con.close()
        self.addCleanup(_kill_groups, sd, "lr1")
        rc, out, err = core._durable_run(
            sd, "lr1", "tok", "codex_dispatch", ["true"],
            cwd=str(ws), timeout=2,
            meta={"stage": "dispatch", "route": "luna/max", "reason": "test"})
        self.assertEqual(rc, 0, err)
        inv = core._list_invocations(sd, "lr1")[-1]
        self.assertEqual(inv["timeout_secs"], 2)

    def test_default_invocation_stores_no_deadline(self):
        sd, ws, base = _tmp(self)
        core.submit(sd, "lr2", {"g": 1}, str(ws), "p")
        self.assertIsNone(core.get_job(sd, "lr2")["timeout_secs"])
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='lr2'")
        finally:
            con.close()
        self.addCleanup(_kill_groups, sd, "lr2")
        rc, out, err = core._durable_run(
            sd, "lr2", "tok", "codex_dispatch", ["true"],
            cwd=str(ws), timeout=None,
            meta={"stage": "dispatch", "route": "luna/max", "reason": "test"})
        self.assertEqual(rc, 0, err)
        inv = core._list_invocations(sd, "lr2")[-1]
        self.assertIsNone(inv["timeout_secs"])


class TestIdleIncompleteTurnEndsOnStall(unittest.TestCase):
    """Review finding 1: an OpenCode turn that goes idle after showing
    activity, without a terminal assistant result, must end through the
    existing stream-silence stall machinery, never poll forever. No
    elapsed deadline is restored: genuine silence past the policy window
    is the only time-based end, and live activity keeps the turn alive."""

    def _run_mode(self, mode):
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
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_OC_MODE",
                                                "FAKE_OC_DELAY", "RUNNER_STALL_SECS")}
        try:
            os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
            os.environ["FAKE_STATE"] = str(fake_state)
            os.environ["FAKE_OC_MODE"] = mode
            # Small deterministic window for the supervisor subprocess
            # (production uses the 180s policy default); the old 1800s
            # per-turn cap is never waited out.
            os.environ["RUNNER_STALL_SECS"] = "2"
            core.submit(sd, "oc-idle", {"goal": "idle drill"}, str(ws), "planner")
            con = store.connect(sd)
            try:
                con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='oc-idle'")
            finally:
                con.close()
            self.addCleanup(_kill_groups, sd, "oc-idle")
            run = core.make_durable_run_cmd(sd, "oc-idle", "tok")
            started = time.monotonic()
            res = controller.run_implementation(sd, "oc-idle", run_cmd=run)
            elapsed = time.monotonic() - started
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        return sd, res, elapsed

    def _assert_stalled(self, sd, res, elapsed):
        # Terminates on the stall machinery, far short of any old budget.
        self.assertLess(elapsed, 30.0, res)
        self.assertGreaterEqual(elapsed, 2.0, res)
        self.assertEqual(res["action"], "stalled_retry", res)
        last = json.loads(core.get_job(sd, "oc-idle")["last_error_json"] or "{}")
        self.assertEqual(last.get("signal"), "stalled", last)
        ev = last.get("evidence") or {}
        self.assertEqual(ev.get("source"), "stream_silence", ev)
        self.assertTrue(ev.get("idle_without_terminal_result"), ev)
        self.assertTrue(last.get("idle_confirmed"), last)
        self.assertIsNotNone(last.get("longest_silence_secs"), last)
        report = json.loads(Path(res["report"]["report_path"]).read_text())
        self.assertEqual(report["status"], "stalled", report)
        inv = [i for i in core._list_invocations(sd, "oc-idle")
               if i["kind"] == "opencode_control"][-1]
        self.assertEqual(inv["terminal_class"], "stalled", inv)
        self.assertIsNotNone(inv["longest_silence_secs"], inv)

    def test_idle_with_incomplete_message_stalls(self):
        sd, res, elapsed = self._run_mode("idle_incomplete")
        self._assert_stalled(sd, res, elapsed)

    def test_idle_with_no_new_message_stalls(self):
        sd, res, elapsed = self._run_mode("idle_empty")
        self._assert_stalled(sd, res, elapsed)


class TestGrokSupervisorLossIsOwnership(unittest.TestCase):
    """Review finding 2: a Grok turn whose supervisor exits without a
    durable terminal result while the CLI child is still alive
    (``core._durable_run`` rc 125) stays in ownership handling like the
    124/143 paths. The dispatcher blocks instead of escalating a new
    attempt behind the unsupervised child; no duplicate writer spawns,
    no unknown process is killed, explicit cancellation still works."""

    def test_rc125_blocks_like_rc124_without_a_second_writer(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        core.submit(sd, "g125", {"goal": "supervisor loss drill"}, str(ws),
                    "planner-1", route="grok-4.6-build", lane="hard")
        calls = []

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            calls.append((cmd, kind))
            # Faithful supervisor-loss shape: no durable result, rc 125,
            # empty output so the harness parses no report.
            return 125, "", ""

        # An unrelated live process stands in for every unknown process:
        # the ownership block must not kill it.
        other = None
        try:
            import subprocess as _sp
            other = _sp.Popen(["sleep", "30"])
            res = controller.run_implementation(sd, "g125", run_cmd=run)
        finally:
            other_alive = (other is not None and other.poll() is None)
            if other is not None:
                other.terminate()
                try:
                    other.wait(timeout=5)
                except Exception:
                    try:
                        other.kill()
                    except Exception:
                        pass
        self.assertTrue(other_alive, "ownership handling must not kill unknown processes")
        self.assertEqual(len(calls), 1, "no duplicate writer may spawn")
        self.assertEqual(res["action"], "blocked", res)
        self.assertEqual(res["reason"], "implementation_failed", res)
        self.assertEqual(res["report"]["status"], "failed", res)
        self.assertTrue(Path(res["report"]["report_path"]).exists())
        job = core.get_job(sd, "g125")
        self.assertEqual(job["status"], "blocked", job)
        # Explicit cancellation still works after the ownership block.
        core.cancel(sd, "g125")
        self.assertEqual(core.get_job(sd, "g125")["status"], "cancelled")

    def test_supervisor_killed_with_live_child_returns_125_and_blocks(self):
        """The reviewed trigger, faithfully: a real ``core._durable_run``
        loses its supervisor (SIGKILL on the recorded supervisor pid only)
        while the owned fake-Grok CLI child stays live. The durable run
        must return rc 125 with the child still alive, and passing that
        authentic result through the controller must block as ownership:
        one worker spawn ever, no escalation, the owned child live until
        targeted cleanup, explicit cancel safe, unknown processes alive."""
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
        write_fake(bindir, "grok", FAKE_GROK, PY)
        saved = {k: os.environ.get(k) for k in ("PATH", "FAKE_STATE", "FAKE_GROK_MODE",
                                                "FAKE_GROK_DELAY", "FAKE_GROK_RELEASE",
                                                "PYTHONDONTWRITEBYTECODE")}
        other = None
        child_pid = None
        try:
            os.environ["PATH"] = str(bindir) + os.pathsep + (saved["PATH"] or "")
            os.environ["FAKE_STATE"] = str(fake_state)
            # Hold mode waits (up to 120s) for a release file that never
            # comes, so the owned CLI child outlives its supervisor.
            os.environ["FAKE_GROK_MODE"] = "hold"
            os.environ.pop("FAKE_GROK_RELEASE", None)
            os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
            core.submit(sd, "ghold", {"goal": "supervisor loss drill"}, str(ws),
                        "planner-1", route="grok-4.6-build", lane="hard")
            con = store.connect(sd)
            try:
                con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='ghold'")
            finally:
                con.close()
            cmd, _env = adapters.build_grok_cmd("hold the turn", str(ws),
                                                "grok-4.6", "medium")
            run = core.make_durable_run_cmd(sd, "ghold", "tok")
            box = {}

            def target():
                box["res"] = run(cmd, str(ws), 30, kind="grok_control",
                                 meta={"stage": "implementation", "route": "grok-4.6-build",
                                       "reason": "test", "seq": 0, "prompt": "hold the turn"})

            t = threading.Thread(target=target, daemon=True)
            t.start()
            self.addCleanup(t.join, 20)

            def _identities():
                invs = core._list_invocations(sd, "ghold")
                return invs and invs[-1].get("pid") and invs[-1].get("supervisor_pid")

            self.assertTrue(_wait_for(_identities, secs=20),
                            "supervisor never recorded the owned child")
            inv = core._list_invocations(sd, "ghold")[-1]
            sup_pid = int(inv["supervisor_pid"])
            child_pid = int(inv["pid"])
            for pid in (sup_pid, child_pid):
                try:
                    os.kill(pid, 0)
                except Exception as e:  # noqa: BLE001
                    self.fail("supervisor and child must both be alive before the kill: %r" % e)
            log = fake_state / "grok.log"

            def _worker_spawns():
                if not log.exists():
                    return 0
                n = 0
                for line in log.read_text().splitlines():
                    if not line.strip():
                        continue
                    try:
                        argv = json.loads(line)["argv"]
                    except ValueError:
                        continue
                    if "-p" in argv or "--prompt-file" in argv:
                        n += 1
                return n

            # The owned worker must have started (and logged its spawn)
            # before its supervisor dies: otherwise the drill could read
            # the log before a loaded machine finishes child startup.
            self.assertTrue(_wait_for(lambda: _worker_spawns() == 1, secs=20),
                            "owned worker never started")
            # Kill only the supervisor by its exact recorded pid: never a
            # group, never an unrelated identity.
            os.kill(sup_pid, signal.SIGKILL)
            t.join(20)
            self.assertFalse(t.is_alive(), "_durable_run must return after losing its supervisor")
            rc, out, err = box["res"]
            self.assertEqual(rc, 125, (out, err))
            # The owned child is still live and still owned: the reviewed
            # trigger condition, proven against the live row.
            try:
                os.kill(child_pid, 0)
            except Exception as e:  # noqa: BLE001
                self.fail("owned CLI child must stay live after its supervisor dies: %r" % e)
            fresh = [i for i in core._list_invocations(sd, "ghold")
                     if i["invocation_id"] == inv["invocation_id"]][0]
            self.assertEqual(core._invocation_ownership(fresh), "live", fresh)
            self.assertEqual(_worker_spawns(), 1, "exactly one worker spawn, never a copy")
            # An unrelated live process must survive the whole sequence.
            import subprocess as _sp
            other = _sp.Popen(["sleep", "30"])
            # Pass the authentic supervisor-loss result through the
            # controller: it must block as ownership, not escalate.
            calls = []

            def feed(cmd2, cwd=None, timeout=None, kind=None, meta=None):
                calls.append((cmd2, kind))
                return box["res"]

            res = controller.run_implementation(sd, "ghold", run_cmd=feed)
            self.assertEqual(other.poll(), None,
                             "ownership handling must not signal unknown processes")
            self.assertEqual(len(calls), 1, "no duplicate writer or escalation may start")
            self.assertEqual(res["action"], "blocked", res)
            self.assertEqual(res["reason"], "implementation_failed", res)
            self.assertEqual(res["report"]["status"], "failed", res)
            self.assertEqual(core.get_job(sd, "ghold")["status"], "blocked")
            # Explicit cancellation still works after the ownership block.
            core.cancel(sd, "ghold")
            self.assertEqual(core.get_job(sd, "ghold")["status"], "cancelled")
            self.assertEqual(other.poll(), None, "cancel must not signal unknown processes")
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            # Targeted cleanup of the owned child by exact recorded pid.
            if child_pid is not None:
                try:
                    os.kill(child_pid, signal.SIGKILL)
                except Exception:
                    pass
                end = time.monotonic() + 10.0
                while time.monotonic() < end:
                    try:
                        os.kill(child_pid, 0)
                    except Exception:
                        break
                    time.sleep(0.05)
            if other is not None and other.poll() is None:
                other.terminate()
                try:
                    other.wait(timeout=5)
                except Exception:
                    try:
                        other.kill()
                    except Exception:
                        pass
        # The owned child is gone and the stranger survived to its own teardown.
        if child_pid is not None:
            try:
                os.kill(child_pid, 0)
                self.fail("owned child must be reaped by targeted cleanup")
            except Exception:
                pass

    def test_rc125_matches_rc124_and_rc1_still_fails(self):
        for rc, action in ((124, "blocked"), (125, "blocked")):
            tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
            self.addCleanup(tmp.cleanup)
            base = Path(tmp.name)
            sd = str(base / "state")
            ws = base / "ws"
            ws.mkdir()
            core.submit(sd, "g%d" % rc, {"goal": "rc drill"}, str(ws),
                        "planner-1", route="grok-4.6-build", lane="hard")

            def run(cmd, cwd=None, timeout=None, kind=None, meta=None, _rc=rc):
                return _rc, "", ""

            res = controller.run_implementation(sd, "g%d" % rc, run_cmd=run)
            self.assertEqual(res["action"], action, (rc, res))
        # A genuine worker failure (rc 1 with an error report) still ends
        # the turn failed for the escalation ladder, never blocked.
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        core.submit(sd, "g1", {"goal": "rc drill"}, str(ws),
                    "planner-1", route="grok-4.6-build", lane="hard")

        def fail(cmd, cwd=None, timeout=None, kind=None, meta=None):
            return 1, json.dumps({"type": "error",
                                  "message": "context_length_exceeded: blown"}) + "\n", ""

        res = controller.run_implementation(sd, "g1", run_cmd=fail)
        self.assertEqual(res["action"], "implementation_failed", res)


if __name__ == "__main__":
    unittest.main(verbosity=2)
