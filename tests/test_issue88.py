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

from runner import controller, core, harnesses, store  # noqa: E402
from tests.fakes import FAKE_OPENCODE, write_fake  # noqa: E402

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
