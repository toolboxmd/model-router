"""Issue 61: record the observed Codex model, block on a model switch.

Deterministic only, stdlib only, no live model CLIs. Fake rollout files
live in the job's shared sessions directory (since #71 a kit's sessions/
links to <state>/codex-sessions/<id>-<hash>); fake run_cmd closures create
a real invocation row, write the rollout through the shared layout, and run
the real _measure_invocation path, so controller checks exercise the
rollout-to-controller behavior.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import controller, core, harnesses, kits as kitmod, policy, store  # noqa: E402


def codex_out(thread, env):
    lines = [{"type": "thread.started", "thread_id": thread},
             {"type": "item.completed", "item": {"id": "item_0", "type": "agent_message",
                                                 "text": json.dumps(env)}},
             {"type": "turn.completed", "usage": {}}]
    return "\n".join(json.dumps(x) for x in lines) + "\n"


def codex_failed_out(thread, text="You've hit your usage limit. Try again at Sep 22nd, 2026 9:51 AM. See https://chatgpt.com/codex"):
    lines = [{"type": "thread.started", "thread_id": thread},
             {"type": "item.completed", "item": {"id": "item_0", "type": "agent_message",
                                                 "text": text}}]
    return "\n".join(json.dumps(x) for x in lines) + "\n"


def rollout_text(rows):
    return "\n".join(json.dumps(r) for r in rows) + "\n"


def write_rollout(state_dir, request_id, invocation_id, thread_id, rows,
                  subdir="sess-01", fname=None):
    """Write a fake rollout through the #71 shared sessions layout."""
    kit_dir = kitmod.kit_dir_for(state_dir, request_id, invocation_id, "dispatcher")
    kit_dir.mkdir(parents=True, exist_ok=True)
    shared = kitmod.codex_sessions_dir_for(state_dir, request_id)
    kitmod.link_codex_sessions(kit_dir, shared)
    sess = Path(shared) / subdir
    sess.mkdir(parents=True, exist_ok=True)
    name = fname or f"rollout-2026-09-23-{thread_id}.jsonl"
    path = sess / name
    path.write_text(rollout_text(rows), encoding="utf-8")
    return path


def insert_invocation(state_dir, request_id, invocation_id, kind, thread_id,
                      requested_route="luna/max", stdout_text=None):
    """Insert an invocation row without seeding observed_model.

    The caller writes the rollout first (or not, for missing-rollout
    cases) and runs the real _measure_invocation afterwards, so observed
    always comes from the rollout.
    """
    root = store.ensure_state_dir(state_dir)
    out_path = root / "outputs" / f"{request_id}.{invocation_id}.stdout"
    err_path = root / "outputs" / f"{request_id}.{invocation_id}.stderr"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stdout = stdout_text if stdout_text is not None else codex_out(
        thread_id, {"action": "completion", "output": "x"})
    out_path.write_text(stdout, encoding="utf-8")
    err_path.write_text("", encoding="utf-8")
    con = store.connect(state_dir)
    try:
        con.execute(
            "INSERT INTO invocations(invocation_id, request_id, kind, cmd_json,"
            " workspace, owner_token, stdout_path, stderr_path, started_at,"
            " state, meta_json, stage, requested_route, policy_version, reason,"
            " observed_model, native_ids_json, schema_version)"
            " VALUES(?,?,?,?,?,?,?,?,?,'finished',?,?,?,?,?,?,?,?)",
            (invocation_id, request_id, kind, json.dumps(["codex", "exec"]),
             "/tmp/ws", "tok", str(out_path), str(err_path), core._utcnow(),
             json.dumps({"stage": "dispatch", "route": requested_route,
                         "reason": "initial"}),
             "dispatch", requested_route, policy.POLICY_VERSION, "initial",
             None,
             json.dumps({"thread_id": thread_id}) if thread_id else None,
             store.SCHEMA_VERSION))
        con.commit()
    finally:
        con.close()
    return stdout


class RolloutParsing(unittest.TestCase):
    def test_last_turn_context_wins(self):
        text = rollout_text([
            {"type": "turn_context", "payload": {"model": "gpt-5.5"}},
            {"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
        ])
        self.assertEqual(harnesses.parse_codex_rollout_model_text(text), "gpt-5.6-luna")

    def test_last_turn_context_missing_model_stays_unknown(self):
        # An earlier turn_context never backfills a later one without a model.
        text = rollout_text([
            {"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
            {"type": "turn_context", "payload": {}},
        ])
        self.assertIsNone(harnesses.parse_codex_rollout_model_text(text))

    def test_final_turn_context_missing_payload_clears_earlier(self):
        # A final turn_context without a dictionary payload is authoritative:
        # it clears an earlier model instead of returning it.
        text = rollout_text([
            {"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
            {"type": "turn_context"},
        ])
        self.assertIsNone(harnesses.parse_codex_rollout_model_text(text))

    def test_final_turn_context_non_dict_payload_clears_earlier(self):
        for bad in ("oops", None, [], 123):
            text = rollout_text([
                {"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
                {"type": "turn_context", "payload": bad},
            ])
            self.assertIsNone(
                harnesses.parse_codex_rollout_model_text(text),
                f"payload={bad!r} must clear to unknown")

    def test_final_turn_context_missing_or_empty_model_clears_earlier(self):
        for rows in (
            [{"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
             {"type": "turn_context", "payload": {"model": ""}}],
            [{"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
             {"type": "turn_context", "payload": {"model": None}}],
            [{"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
             {"type": "turn_context", "payload": {"model": 123}}],
        ):
            self.assertIsNone(harnesses.parse_codex_rollout_model_text(
                rollout_text(rows)))

    def test_later_valid_turn_context_reestablishes_after_clear(self):
        text = rollout_text([
            {"type": "turn_context", "payload": {"model": "gpt-5.5"}},
            {"type": "turn_context", "payload": {}},
            {"type": "turn_context", "payload": {"model": "gpt-5.6-luna"}},
        ])
        self.assertEqual(harnesses.parse_codex_rollout_model_text(text), "gpt-5.6-luna")

    def test_thread_settings_applied_is_ignored_stays_unknown(self):
        # Only the last turn_context model is observed; thread_settings
        # never supplies a model and stays unknown.
        text = rollout_text([
            {"type": "event_msg", "payload": {"type": "thread_settings_applied",
                                              "thread_settings": {"model": "gpt-5.6-luna"}}},
        ])
        self.assertIsNone(harnesses.parse_codex_rollout_model_text(text))
        text2 = rollout_text([
            {"type": "thread_settings_applied",
             "payload": {"thread_settings": {"model": "gpt-5.5"}}},
        ])
        self.assertIsNone(harnesses.parse_codex_rollout_model_text(text2))
        self.assertIsNone(harnesses.parse_codex_rollout_model_text(""))
        self.assertIsNone(harnesses.parse_codex_rollout_model_text(
            rollout_text([{"type": "noise", "payload": {}}])))

    def test_missing_or_unreadable_rollout_stays_unknown(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        sd = str(Path(tmp.name) / "state")
        self.assertIsNone(harnesses.codex_observed_model(sd, "r1", "i1", "thr-missing"))
        self.assertIsNone(harnesses.codex_observed_model(sd, "r1", "i1", ""))
        self.assertIsNone(harnesses.codex_observed_model(sd, "r1", "i1", None))
        self.assertIsNone(harnesses.codex_observed_model(sd, "r1", "i1", "../escape"))


class MeasureObserved(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()

    def test_matching_model_recorded_via_measure_invocation(self):
        rid, iid, thr = "m61-match-1", "inv-match-01", "thr-match-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        requested = policy.ROUTES["luna/max"]["model"]
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": requested}},
        ])
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr)
        core._measure_invocation(self.sd, rid, iid)
        rows = [i for i in core._list_invocations(self.sd, rid) if i["invocation_id"] == iid]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["observed_model"], requested)
        native = json.loads(rows[0]["native_ids_json"] or "{}")
        self.assertEqual(native.get("thread_id"), thr)

    def test_missing_rollout_leaves_unknown(self):
        rid, iid, thr = "m61-miss-1", "inv-miss-01", "thr-miss-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr)
        core._measure_invocation(self.sd, rid, iid)
        rows = [i for i in core._list_invocations(self.sd, rid) if i["invocation_id"] == iid]
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["observed_model"])
        native = json.loads(rows[0]["native_ids_json"] or "{}")
        self.assertEqual(native.get("thread_id"), thr)

    def test_measure_without_context_never_guesses(self):
        out = codex_out("thr-guess-1", {"action": "completion", "output": "x"})
        usage, observed, variant, ids = core.measure_output(
            "codex_dispatch", out, "", {"stage": "dispatch", "route": "luna/max"})
        self.assertIsNone(observed)
        self.assertEqual(ids.get("thread_id"), "thr-guess-1")

    def test_resume_kind_records_matching_model(self):
        rid, iid, thr = "m61-resume-1", "inv-res-01", "thr-res-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        requested = policy.ROUTES["luna/max"]["model"]
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": requested}},
        ])
        insert_invocation(self.sd, rid, iid, "codex_resume", thr)
        core._measure_invocation(self.sd, rid, iid)
        rows = [i for i in core._list_invocations(self.sd, rid) if i["invocation_id"] == iid]
        self.assertEqual(rows[0]["observed_model"], requested)


class MismatchBlocking(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.requested = policy.ROUTES["luna/max"]["model"]
        self.assertEqual(self.requested, "gpt-5.6-luna")
        self._iid_seq = 0

    def _next_iid(self, prefix="inv"):
        self._iid_seq += 1
        return f"{prefix}-{self._iid_seq:04d}"

    def _run_with_rollout(self, request_id, thread, rollout_rows, rc=0,
                          env=None, out_text=None, kind_expected="codex_dispatch"):
        """Fake run_cmd that measures the real rollout path.

        Creates a fresh invocation row for the turn, writes the rollout
        through the shared sessions layout, runs the real measurement, and
        returns the turn output. rollout_rows None means no rollout file
        (missing-rollout unknown); otherwise the file holds those rows.
        """
        env = env or {"action": "completion", "output": "DONE"}

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            run.calls.append((list(cmd), kind, dict(meta or {})))
            use_kind = kind or kind_expected
            route = (meta or {}).get("route") or "luna/max"
            iid = self._next_iid("inv")
            if rollout_rows is not None:
                write_rollout(self.sd, request_id, iid, thread, rollout_rows)
            out = out_text if out_text is not None else codex_out(thread, env)
            insert_invocation(self.sd, request_id, iid, use_kind, thread,
                              requested_route=route, stdout_text=out)
            core._measure_invocation(self.sd, request_id, iid)
            err = ""
            return rc, out, err

        run.calls = []
        return run

    def test_dispatch_match_permits_turn(self):
        rid, thr = "m61-d-ok", "thr-d-ok"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        run = self._run_with_rollout(rid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        job = core.get_job(self.sd, rid)
        self.assertNotEqual(job["status"], "blocked")
        # The measured row carries the observed model from the rollout.
        invs = [i for i in core._list_invocations(self.sd, rid)
                if i.get("kind") == "codex_dispatch"]
        self.assertEqual(len(invs), 1)
        self.assertEqual(invs[0]["observed_model"], self.requested)

    def test_dispatch_mismatch_blocks_with_reason_and_no_fallback(self):
        rid, thr = "m61-d-bad", "thr-d-bad"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        observed = "gpt-5.5"
        self.assertNotEqual(observed, self.requested)
        run = self._run_with_rollout(rid, thr, [
            {"type": "turn_context", "payload": {"model": observed}},
        ])
        res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        self.assertEqual(res["action"], "blocked")
        self.assertTrue(res["reason"].startswith("model_mismatch"),
                        res["reason"])
        self.assertIn(f"requested={self.requested}", res["reason"])
        self.assertIn(f"observed={observed}", res["reason"])
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue((job["block_reason"] or "").startswith("model_mismatch"))
        # No retry or fallback: exactly one child call, no route move, no
        # capacity mark.
        self.assertEqual(len(run.calls), 1)
        self.assertEqual(run.calls[0][1], "codex_dispatch")
        self.assertEqual(core.exhausted_routes(self.sd), set())
        st = json.loads(job.get("controller_state") or "{}")
        self.assertNotIn("route_reason", st)

    def test_dispatch_unknown_never_blocks(self):
        rid, thr = "m61-d-unknown", "thr-d-unknown"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        # Rollout file without a usable model stays unknown via the real path.
        run = self._run_with_rollout(rid, thr, [
            {"type": "turn_context", "payload": {}},
        ])
        res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")

    def test_dispatch_failed_turn_mismatch_beats_usage_limit_fallback(self):
        rid, thr = "m61-d-fail-bad", "thr-d-fail-bad"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        observed = "gpt-5.5"
        failed_out = codex_failed_out(thr)
        run = self._run_with_rollout(rid, thr, [
            {"type": "turn_context", "payload": {"model": observed}},
        ], rc=1, out_text=failed_out)
        res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        self.assertEqual(res["action"], "blocked")
        self.assertTrue(res["reason"].startswith("model_mismatch"), res["reason"])
        self.assertIn(f"requested={self.requested}", res["reason"])
        self.assertIn(f"observed={observed}", res["reason"])
        # No usage-limit fallback: exactly one call, still codex_dispatch,
        # no capacity mark and no route move.
        self.assertEqual(len(run.calls), 1)
        self.assertEqual(run.calls[0][1], "codex_dispatch")
        self.assertEqual(core.exhausted_routes(self.sd), set())
        job = core.get_job(self.sd, rid)
        st = json.loads(job.get("controller_state") or "{}")
        self.assertNotIn("route_reason", st)

    def test_resume_match_mismatch_unknown_via_rollouts(self):
        # Match permits.
        rid = "m61-r-ok"
        ws_ok = str(self.base / "ws-r-ok")
        import os as _os
        _os.makedirs(ws_ok, exist_ok=True)
        thr = "thr-r-ok"
        core.submit(self.sd, rid, {"goal": "t"}, ws_ok, "claude-1")
        disp = self._run_with_rollout(rid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        dres = controller.dispatch(self.sd, rid, run_cmd=disp, probe=lambda *a: None)
        self.assertEqual(dres["action"], "dispatched")

        def run_ok(cmd, cwd=None, timeout=None, kind=None, meta=None):
            iid = self._next_iid("inv")
            write_rollout(self.sd, rid, iid, thr, [
                {"type": "turn_context", "payload": {"model": self.requested}},
            ])
            out = codex_out(thr, {"action": "completion", "output": "done"})
            route = (meta or {}).get("route") or "luna/max"
            insert_invocation(self.sd, rid, iid, "codex_resume", thr,
                              requested_route=route, stdout_text=out)
            core._measure_invocation(self.sd, rid, iid)
            return 0, out, ""

        res = controller.resume_luna(self.sd, rid, "ctx", run_cmd=run_ok)
        self.assertEqual(res["action"], "resumed")
        # Mismatch blocks.
        rid2 = "m61-r-bad"
        ws_bad = str(self.base / "ws-r-bad")
        _os.makedirs(ws_bad, exist_ok=True)
        thr2 = "thr-r-bad"
        core.submit(self.sd, rid2, {"goal": "t"}, ws_bad, "claude-1")
        disp2 = self._run_with_rollout(rid2, thr2, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        dres2 = controller.dispatch(self.sd, rid2, run_cmd=disp2, probe=lambda *a: None)
        self.assertEqual(dres2["action"], "dispatched")
        observed = "gpt-5.5"
        calls = []

        def run_bad(cmd, cwd=None, timeout=None, kind=None, meta=None):
            calls.append(kind)
            iid = self._next_iid("inv")
            write_rollout(self.sd, rid2, iid, thr2, [
                {"type": "turn_context", "payload": {"model": observed}},
            ])
            out = codex_out(thr2, {"action": "completion", "output": "done"})
            route = (meta or {}).get("route") or "luna/max"
            insert_invocation(self.sd, rid2, iid, "codex_resume", thr2,
                              requested_route=route, stdout_text=out)
            core._measure_invocation(self.sd, rid2, iid)
            return 0, out, ""

        res2 = controller.resume_luna(self.sd, rid2, "ctx", run_cmd=run_bad)
        self.assertEqual(res2["action"], "blocked")
        self.assertTrue(res2["reason"].startswith("model_mismatch"), res2["reason"])
        self.assertIn(f"requested={self.requested}", res2["reason"])
        self.assertIn(f"observed={observed}", res2["reason"])
        job2 = core.get_job(self.sd, rid2)
        self.assertEqual(job2["status"], "blocked")
        self.assertEqual(calls, ["codex_resume"])

    def test_resume_failed_turn_mismatch_beats_failure_fallback(self):
        rid = "m61-r-fail-bad"
        import os as _os
        ws = str(self.base / "ws-r-fail")
        _os.makedirs(ws, exist_ok=True)
        thr = "thr-r-fail-bad"
        core.submit(self.sd, rid, {"goal": "t"}, ws, "claude-1")
        disp = self._run_with_rollout(rid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        dres = controller.dispatch(self.sd, rid, run_cmd=disp, probe=lambda *a: None)
        self.assertEqual(dres["action"], "dispatched")
        observed = "gpt-5.5"
        failed_out = codex_failed_out(thr, "boom failed turn")

        def run_bad(cmd, cwd=None, timeout=None, kind=None, meta=None):
            iid = self._next_iid("inv")
            write_rollout(self.sd, rid, iid, thr, [
                {"type": "turn_context", "payload": {"model": observed}},
            ])
            route = (meta or {}).get("route") or "luna/max"
            insert_invocation(self.sd, rid, iid, "codex_resume", thr,
                              requested_route=route, stdout_text=failed_out)
            core._measure_invocation(self.sd, rid, iid)
            return 1, failed_out, ""

        res = controller.resume_luna(self.sd, rid, "ctx", run_cmd=run_bad)
        self.assertEqual(res["action"], "blocked")
        self.assertTrue(res["reason"].startswith("model_mismatch"), res["reason"])

    def test_resume_unknown_never_blocks(self):
        rid = "m61-r-unknown"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        disp = self._run_with_rollout(rid, "thr-r-u", [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        dres = controller.dispatch(self.sd, rid, run_cmd=disp, probe=lambda *a: None)
        self.assertEqual(dres["action"], "dispatched")

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            iid = self._next_iid("inv")
            write_rollout(self.sd, rid, iid, "thr-r-u", [
                {"type": "turn_context", "payload": {}},
            ])
            out = codex_out("thr-r-u", {"action": "completion", "output": "done"})
            route = (meta or {}).get("route") or "luna/max"
            insert_invocation(self.sd, rid, iid, "codex_resume", "thr-r-u",
                              requested_route=route, stdout_text=out)
            core._measure_invocation(self.sd, rid, iid)
            return 0, out, ""

        res = controller.resume_luna(self.sd, rid, "ctx", run_cmd=run)
        self.assertEqual(res["action"], "resumed")

    def test_helper_latest_row_wins(self):
        rid = "m61-latest"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        write_rollout(self.sd, rid, "inv-old", "thr-old", [
            {"type": "turn_context", "payload": {"model": "gpt-5.5"}},
        ])
        insert_invocation(self.sd, rid, "inv-old", "codex_dispatch", "thr-old")
        core._measure_invocation(self.sd, rid, "inv-old")
        write_rollout(self.sd, rid, "inv-new", "thr-new", [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        insert_invocation(self.sd, rid, "inv-new", "codex_dispatch", "thr-new")
        core._measure_invocation(self.sd, rid, "inv-new")
        observed, reason = controller.check_codex_observed_model(
            self.sd, rid, "codex_dispatch", "luna/max")
        self.assertEqual(observed, self.requested)
        self.assertIsNone(reason)

    def test_unexpected_observation_failure_blocks_safely(self):
        rid = "m61-unexp"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            return 0, codex_out("thr-unexp", {"action": "completion", "output": "x"}), ""

        orig = core._list_invocations

        def boom(*a, **k):
            raise RuntimeError("boom-observation")

        core._list_invocations = boom
        try:
            res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        finally:
            core._list_invocations = orig
        self.assertEqual(res["action"], "blocked")
        self.assertTrue(res["reason"].startswith("model_observation_failed"), res["reason"])
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked")


class RecoveryBlocking(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.requested = policy.ROUTES["luna/max"]["model"]

    def test_recovery_mismatched_completed_envelope_blocked_before_success(self):
        rid, iid, thr = "m61-rec-bad", "inv-rec-01", "thr-rec-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        controller._save_codex_task(self.sd, rid, thr, model=self.requested,
                                    effort="high", dispatch_route="luna/max")
        observed = "gpt-5.5"
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": observed}},
        ])
        stdout = codex_out(thr, {"action": "completion", "output": "DONE"})
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                          stdout_text=stdout)
        applied = core.consume_finished_invocations(self.sd, rid)
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue((job["block_reason"] or "").startswith("model_mismatch"),
                        job["block_reason"])
        self.assertIn(f"requested={self.requested}", job["block_reason"])
        self.assertIn(f"observed={observed}", job["block_reason"])
        # No completion: the envelope was never applied.
        self.assertNotEqual(job["status"], "succeeded")
        invs = [i for i in core._list_invocations(self.sd, rid)
                if i["invocation_id"] == iid]
        self.assertEqual(invs[0]["observed_model"], observed)
        self.assertTrue(any(a.get("action") == "blocked" for a in applied)
                        or job["status"] == "blocked")

    def test_recovery_matching_completed_envelope_succeeds(self):
        rid, iid, thr = "m61-rec-ok", "inv-rec-02", "thr-rec-02"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        controller._save_codex_task(self.sd, rid, thr, model=self.requested,
                                    effort="high", dispatch_route="luna/max")
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        stdout = codex_out(thr, {"action": "completion", "output": "DONE"})
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                          stdout_text=stdout)
        core.consume_finished_invocations(self.sd, rid)
        job = core.get_job(self.sd, rid)
        # Matching model lets the completed envelope apply; the job must not
        # be stuck blocked by the model gate.
        self.assertFalse((job.get("block_reason") or "").startswith("model_mismatch"))
        self.assertEqual(job["status"], "succeeded")


class ObservationFailureBlocking(unittest.TestCase):
    """Unexpected observation failures block safely via fake rollout files.

    Each path creates its invocation and observed model through a real
    rollout file (never by seeding observed_model directly), then induces
    an unexpected failure at the real observation seam (kit lookup) and
    proves the job blocks with model_observation_failed and no completion
    envelope is applied.
    """

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.requested = policy.ROUTES["luna/max"]["model"]
        self._iid_seq = 0

    def _next_iid(self, prefix="inv"):
        self._iid_seq += 1
        return f"{prefix}-{self._iid_seq:04d}"

    def test_dispatch_observation_failure_blocks_with_no_envelope(self):
        rid, thr = "m61-obs-d", "thr-obs-d"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        orig = kitmod.kit_dirs_for_invocation

        def boom(*a, **k):
            raise RuntimeError("boom-kit-lookup")

        kitmod.kit_dirs_for_invocation = boom
        try:
            calls = []

            def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
                calls.append(kind)
                iid = self._next_iid("inv")
                write_rollout(self.sd, rid, iid, thr, [
                    {"type": "turn_context", "payload": {"model": self.requested}},
                ])
                out = codex_out(thr, {"action": "completion", "output": "DONE"})
                route = (meta or {}).get("route") or "luna/max"
                insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                                  requested_route=route, stdout_text=out)
                # Real measurement seam: kit lookup fails unexpectedly.
                core._measure_invocation(self.sd, rid, iid)
                return 0, out, ""

            res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        finally:
            kitmod.kit_dirs_for_invocation = orig
        self.assertEqual(res["action"], "blocked")
        self.assertTrue(res["reason"].startswith("model_observation_failed"), res["reason"])
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue((job["block_reason"] or "").startswith("model_observation_failed"))
        self.assertNotEqual(job["status"], "succeeded")
        st = json.loads(job.get("controller_state") or "{}")
        last = st.get("last_action") if isinstance(st, dict) else None
        self.assertFalse(isinstance(last, dict) and last.get("action") == "completion", st)
        self.assertEqual(core.exhausted_routes(self.sd), set())
        self.assertNotIn("route_reason", st)
        self.assertEqual(calls, ["codex_dispatch"])

    def test_resume_observation_failure_blocks_with_no_envelope(self):
        import os as _os
        rid = "m61-obs-r"
        ws = str(self.base / "ws-obs-r")
        _os.makedirs(ws, exist_ok=True)
        thr = "thr-obs-r"
        core.submit(self.sd, rid, {"goal": "t"}, ws, "claude-1")

        def disp_run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            iid = self._next_iid("inv")
            write_rollout(self.sd, rid, iid, thr, [
                {"type": "turn_context", "payload": {"model": self.requested}},
            ])
            out = codex_out(thr, {"action": "completion", "output": "DONE"})
            route = (meta or {}).get("route") or "luna/max"
            insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                              requested_route=route, stdout_text=out)
            core._measure_invocation(self.sd, rid, iid)
            return 0, out, ""

        dres = controller.dispatch(self.sd, rid, run_cmd=disp_run, probe=lambda *a: None)
        self.assertEqual(dres["action"], "dispatched")
        orig = kitmod.kit_dirs_for_invocation

        def boom(*a, **k):
            raise RuntimeError("boom-kit-lookup-resume")

        kitmod.kit_dirs_for_invocation = boom
        try:
            def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
                iid = self._next_iid("inv")
                write_rollout(self.sd, rid, iid, thr, [
                    {"type": "turn_context", "payload": {"model": self.requested}},
                ])
                out = codex_out(thr, {"action": "completion", "output": "done"})
                route = (meta or {}).get("route") or "luna/max"
                insert_invocation(self.sd, rid, iid, "codex_resume", thr,
                                  requested_route=route, stdout_text=out)
                core._measure_invocation(self.sd, rid, iid)
                return 0, out, ""

            res = controller.resume_luna(self.sd, rid, "ctx", run_cmd=run)
        finally:
            kitmod.kit_dirs_for_invocation = orig
        self.assertEqual(res["action"], "blocked")
        self.assertTrue(res["reason"].startswith("model_observation_failed"), res["reason"])
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue((job["block_reason"] or "").startswith("model_observation_failed"))

    def test_recovery_observation_failure_blocks_before_success(self):
        rid, iid, thr = "m61-obs-rec", "inv-obs-rec-01", "thr-obs-rec"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        controller._save_codex_task(self.sd, rid, thr, model=self.requested,
                                    effort="high", dispatch_route="luna/max")
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        stdout = codex_out(thr, {"action": "completion", "output": "DONE"})
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                          stdout_text=stdout)
        orig = kitmod.kit_dirs_for_invocation

        def boom(*a, **k):
            raise RuntimeError("boom-kit-lookup-recovery")

        kitmod.kit_dirs_for_invocation = boom
        try:
            core.consume_finished_invocations(self.sd, rid)
        finally:
            kitmod.kit_dirs_for_invocation = orig
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue((job["block_reason"] or "").startswith("model_observation_failed"),
                        job["block_reason"])
        self.assertNotEqual(job["status"], "succeeded")


class MeasurementDurability(unittest.TestCase):
    """A failed observed_model write never reads as unknown.

    The invocation and observed model come from a fake rollout file (never
    by seeding observed_model directly). The failure is induced at the real
    measurement seam (the database write), then the controller path must
    block before any completion envelope.
    """

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.requested = policy.ROUTES["luna/max"]["model"]

    def test_observed_model_write_failure_blocks_before_envelope(self):
        import sqlite3 as _sqlite3
        rid = "m61-dur-1"
        thr = "thr-dur-1"
        iid = "inv-dur-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr)
        orig_connect = store.connect

        class _FailingCon:
            """Proxy that fails only UPDATEs writing observed_model."""

            def __init__(self, real):
                object.__setattr__(self, "_real", real)

            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str) and sql.strip().upper().startswith("UPDATE") \
                        and "invocations" in sql and "observed_model" in sql:
                    raise _sqlite3.OperationalError("injected observed_model write failure")
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(object.__getattribute__(self, "_real"), name)

        def failing_connect(state_dir):
            return _FailingCon(orig_connect(state_dir))

        store.connect = failing_connect
        try:
            with self.assertRaises(_sqlite3.OperationalError):
                core._measure_invocation(self.sd, rid, iid)
            rid2 = "m61-dur-ctrl"
            import os as _os2
            ws2 = str(self.base / "ws-dur-ctrl")
            _os2.makedirs(ws2, exist_ok=True)
            core.submit(self.sd, rid2, {"goal": "t"}, ws2, "claude-1")
            seq = {"n": 0}

            def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
                seq["n"] += 1
                iid2 = f"inv-dur-ctrl-{seq['n']:02d}"
                write_rollout(self.sd, rid2, iid2, thr, [
                    {"type": "turn_context", "payload": {"model": self.requested}},
                ])
                out = codex_out(thr, {"action": "completion", "output": "DONE"})
                route = (meta or {}).get("route") or "luna/max"
                insert_invocation(self.sd, rid2, iid2, "codex_dispatch", thr,
                                  requested_route=route, stdout_text=out)
                core._measure_invocation(self.sd, rid2, iid2)
                return 0, out, ""

            res = controller.dispatch(self.sd, rid2, run_cmd=run, probe=lambda *a: None)
        finally:
            store.connect = orig_connect
        self.assertEqual(res["action"], "blocked")
        self.assertTrue(res["reason"].startswith("model_observation_failed"), res["reason"])
        job = core.get_job(self.sd, rid2)
        self.assertEqual(job["status"], "blocked")
        self.assertTrue((job["block_reason"] or "").startswith("model_observation_failed"))
        self.assertNotEqual(job["status"], "succeeded")
        st = json.loads(job.get("controller_state") or "{}")
        last = st.get("last_action") if isinstance(st, dict) else None
        self.assertFalse(isinstance(last, dict) and last.get("action") == "completion", st)


class RecoveryMeasurementFailure(unittest.TestCase):
    """Recovery measures before committing completion, fail safe.

    A completed matching Codex envelope with an injected measurement
    database failure (sqlite3.OperationalError at the observed_model
    write seam) must block durably, never succeed, never apply the
    completion envelope, and record a useful reason/event.
    """

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.requested = policy.ROUTES["luna/max"]["model"]

    def test_recovery_measurement_db_failure_blocks_before_success(self):
        import sqlite3 as _sqlite3
        rid, iid, thr = "m61-rec-meas-fail", "inv-rec-meas-01", "thr-rec-meas-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        controller._save_codex_task(self.sd, rid, thr, model=self.requested,
                                    effort="high", dispatch_route="luna/max")
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        stdout = codex_out(thr, {"action": "completion", "output": "DONE"})
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                          stdout_text=stdout)
        orig_connect = store.connect

        class _FailingCon:
            """Proxy failing only UPDATEs writing observed_model."""

            def __init__(self, real):
                object.__setattr__(self, "_real", real)

            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str) and sql.strip().upper().startswith("UPDATE") \
                        and "invocations" in sql and "observed_model" in sql:
                    raise _sqlite3.OperationalError(
                        "injected observed_model write failure")
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(object.__getattribute__(self, "_real"), name)

        def failing_connect(state_dir):
            return _FailingCon(orig_connect(state_dir))

        store.connect = failing_connect
        try:
            applied = core.consume_finished_invocations(self.sd, rid)
        finally:
            store.connect = orig_connect
        job = core.get_job(self.sd, rid)
        self.assertNotEqual(job["status"], "succeeded", job)
        self.assertEqual(job["status"], "blocked", job)
        reason = job.get("block_reason") or ""
        self.assertTrue(reason.startswith("model_observation_failed"), reason)
        self.assertIn("OperationalError", reason, reason)
        st = json.loads(job.get("controller_state") or "{}")
        last = st.get("last_action") if isinstance(st, dict) else None
        self.assertFalse(isinstance(last, dict) and last.get("action") == "completion", st)
        self.assertTrue(any(a.get("action") == "blocked" for a in applied),
                        applied)
        con = store.connect(self.sd)
        try:
            rows = [dict(r) for r in con.execute(
                "SELECT kind, payload_json FROM events WHERE request_id=? ORDER BY id",
                (rid,)).fetchall()]
        finally:
            con.close()
        blocked = [r for r in rows if r["kind"] == "blocked"]
        self.assertTrue(blocked, rows)
        self.assertIn("model_observation_failed",
                      (blocked[-1]["payload_json"] or ""), rows)

    def test_recovery_post_measurement_write_failure_leaves_no_completion_trace(self):
        """Only the full measurement write fails: no completion may commit.

        The fault allows the candidate's old partial observed-model gate
        write (no longest_silence_secs/tools_json) to succeed and fails
        only the distinctive full-measurement persistence statement. On
        the candidate this exposes a committed completed trace via the
        post-commit repeat; on the fix the job blocks before any
        envelope with no completed event, no controller_state envelope,
        and no successful result_json.
        """
        import sqlite3 as _sqlite3
        rid, iid, thr = "m61-rec-post-meas", "inv-rec-post-01", "thr-rec-post-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        controller._save_codex_task(self.sd, rid, thr, model=self.requested,
                                    effort="high", dispatch_route="luna/max")
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        stdout = codex_out(thr, {"action": "completion", "output": "DONE"})
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                          stdout_text=stdout)
        orig_connect = store.connect

        class _FailingCon:
            """Proxy failing only the full-measurement persistence write."""

            def __init__(self, real):
                object.__setattr__(self, "_real", real)

            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str) and sql.strip().upper().startswith("UPDATE") \
                        and "invocations" in sql \
                        and ("longest_silence_secs" in sql or "tools_json" in sql):
                    raise _sqlite3.OperationalError(
                        "injected post-measurement write failure")
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(object.__getattribute__(self, "_real"), name)

        def failing_connect(state_dir):
            return _FailingCon(orig_connect(state_dir))

        store.connect = failing_connect
        try:
            applied = core.consume_finished_invocations(self.sd, rid)
        finally:
            store.connect = orig_connect
        job = core.get_job(self.sd, rid)
        self.assertNotEqual(job["status"], "succeeded", job)
        self.assertEqual(job["status"], "blocked", job)
        reason = job.get("block_reason") or ""
        self.assertTrue(reason.startswith("model_observation_failed"), reason)
        self.assertIn("OperationalError", reason, reason)
        self.assertTrue(any(a.get("action") == "blocked" for a in applied),
                        applied)
        con = store.connect(self.sd)
        try:
            rows = [dict(r) for r in con.execute(
                "SELECT kind, payload_json FROM events WHERE request_id=? ORDER BY id",
                (rid,)).fetchall()]
        finally:
            con.close()
        kinds = [r["kind"] for r in rows]
        self.assertNotIn("completed", kinds, rows)
        self.assertNotIn("luna_action", kinds, rows)
        blocked = [r for r in rows if r["kind"] == "blocked"]
        self.assertTrue(blocked, rows)
        self.assertIn("model_observation_failed",
                      (blocked[-1]["payload_json"] or ""), rows)
        st = json.loads(job.get("controller_state") or "{}")
        last = st.get("last_action") if isinstance(st, dict) else None
        self.assertFalse(isinstance(last, dict) and last.get("action") == "completion", st)
        self.assertNotIn("completion", json.dumps(st, sort_keys=True), st)
        raw_result = job.get("result_json")
        if raw_result:
            try:
                parsed = json.loads(raw_result)
            except ValueError:
                parsed = None
            self.assertFalse(isinstance(parsed, dict) and parsed.get("ok") is True,
                             raw_result)
        else:
            self.assertTrue(raw_result is None or raw_result == "", raw_result)


class KitDirsErrorPropagation(unittest.TestCase):
    """kit_dirs_for_invocation suppresses only missing-directory cases."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.requested = policy.ROUTES["luna/max"]["model"]

    def test_missing_root_returns_empty(self):
        self.assertEqual(kitmod.kit_dirs_for_invocation(
            str(self.base / "no-state"), "r1", "i1"), [])

    def test_non_directory_root_returns_empty(self):
        kits = self.base / "kits-state"
        kits.mkdir()
        (kits / "kits").write_text("not-a-dir", encoding="utf-8")
        self.assertEqual(kitmod.kit_dirs_for_invocation(
            str(kits), "r1", "i1"), [])

    def test_permission_error_at_listing_propagates(self):
        from unittest import mock as _mock
        rid, iid = "rk1", "ik1"
        root = Path(self.sd) / "kits"
        root.mkdir(parents=True, exist_ok=True)
        (root / f"{rid}.{iid}.dispatcher").mkdir(exist_ok=True)
        real_iterdir = Path.iterdir

        def boom(self_path):
            if str(self_path).endswith("kits"):
                raise PermissionError("injected kits listing denial")
            return real_iterdir(self_path)

        with _mock.patch.object(Path, "iterdir", boom):
            with self.assertRaises(PermissionError):
                kitmod.kit_dirs_for_invocation(self.sd, rid, iid)
        # A missing directory still returns empty, never raises.
        self.assertEqual(kitmod.kit_dirs_for_invocation(
            str(self.base / "no-state-2"), "r1", "i1"), [])

    def test_io_error_at_listing_propagates(self):
        from unittest import mock as _mock
        rid, iid = "rk2", "ik2"
        root = Path(self.sd) / "kits"
        root.mkdir(parents=True, exist_ok=True)
        real_iterdir = Path.iterdir

        def boom(self_path):
            if str(self_path).endswith("kits"):
                raise OSError(5, "injected kits I/O failure")
            return real_iterdir(self_path)

        with _mock.patch.object(Path, "iterdir", boom):
            with self.assertRaises(OSError):
                kitmod.kit_dirs_for_invocation(self.sd, rid, iid)

    def test_recovery_kit_listing_permission_blocks_safely(self):
        from unittest import mock as _mock
        rid, iid, thr = "m61-kit-perm-rec", "inv-kit-perm-01", "thr-kit-perm-01"
        core.submit(self.sd, rid, {"goal": "t"}, str(self.ws), "claude-1")
        controller._save_codex_task(self.sd, rid, thr, model=self.requested,
                                    effort="high", dispatch_route="luna/max")
        write_rollout(self.sd, rid, iid, thr, [
            {"type": "turn_context", "payload": {"model": self.requested}},
        ])
        stdout = codex_out(thr, {"action": "completion", "output": "DONE"})
        insert_invocation(self.sd, rid, iid, "codex_dispatch", thr,
                          stdout_text=stdout)
        real_iterdir = Path.iterdir

        def boom(self_path):
            if str(self_path).endswith("kits"):
                raise PermissionError("injected kits listing denial")
            return real_iterdir(self_path)

        with _mock.patch.object(Path, "iterdir", boom):
            applied = core.consume_finished_invocations(self.sd, rid)
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["status"], "blocked", job)
        self.assertNotEqual(job["status"], "succeeded", job)
        reason = job.get("block_reason") or ""
        self.assertTrue(reason.startswith("model_observation_failed"), reason)
        st = json.loads(job.get("controller_state") or "{}")
        last = st.get("last_action") if isinstance(st, dict) else None
        self.assertFalse(isinstance(last, dict) and last.get("action") == "completion", st)
        self.assertTrue(any(a.get("action") == "blocked" for a in applied),
                        applied)


if __name__ == "__main__":
    unittest.main()


class SharedSessionsLayout(unittest.TestCase):
    """#71 layout: a kit's sessions/ is a symlink to the job's shared directory."""

    def test_rollout_is_found_through_the_shared_sessions_link(self):
        import tempfile as _tempfile
        from pathlib import Path as _Path
        with _tempfile.TemporaryDirectory() as tmp:
            root = _Path(tmp)
            shared = root / "codex-sessions" / "job-61" / "2026" / "09" / "23"
            shared.mkdir(parents=True)
            (shared / "rollout-x-thread-61.jsonl").write_text(
                json.dumps({"type": "turn_context",
                            "payload": {"model": "gpt-5.6-luna", "turn_id": "t1"}}) + "\n")
            kit = root / "kits" / "job-61.inv-1.dispatcher"
            kit.mkdir(parents=True)
            (kit / "sessions").symlink_to(root / "codex-sessions" / "job-61",
                                          target_is_directory=True)
            paths = harnesses.codex_rollout_paths_for_thread([kit], "thread-61")
            self.assertEqual([p.name for p in paths], ["rollout-x-thread-61.jsonl"])
            self.assertEqual(harnesses.parse_codex_rollout_model_text(paths[0].read_text()),
                             "gpt-5.6-luna")
