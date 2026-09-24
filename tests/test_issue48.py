"""Issue #48: dispatcher envelope from the OpenCode-hosted Luna turn.

Deterministic only. No live model CLIs. Covers: the harness takes the
last assistant message's text, extracts the last complete JSON object in
it (tolerating code fences and surrounding prose, with a bounded repair
for a model-emitted unbalanced tail like the live rehearsal shape),
validates it as an action envelope, and returns it, with the raw text
kept in the ledger; a missing envelope blocks quoting the first 200
characters of the assistant text; resume uses the same extraction; a
stub drill and a public-CLI drill run from an exhausted Codex reading
through Luna on OpenCode Go to a completed worker turn and completion.
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
from tests.fakes import FAKE_CLAUDE, FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable
LIVE_FILE = ROOT / "LIVE_RUNNER_RESULT_rehearsal3.txt"

LIVE_USAGE_MESSAGE = (
    "You've hit your usage limit. Visit "
    "https://chatgpt.com/codex/settings/usage to purchase more credits "
    "or try again at Sep 22nd, 2026 9:51 AM."
)
LIVE_RESET_ISO = "2026-09-22T09:51:00+00:00"


def live_assistant_text() -> str:
    """The exact assistant_text the live rehearsal supervisor recorded."""
    for line in LIVE_FILE.read_text().splitlines():
        if line.startswith("RUNNER_RESULT "):
            return json.loads(line[len("RUNNER_RESULT "):])["assistant_text"]
    raise AssertionError("no RUNNER_RESULT line in the live rehearsal file")


def runner_result(stdout_body: dict) -> str:
    return "RUNNER_RESULT " + json.dumps(stdout_body) + "\n"


def completion_body(session: str, output: str = "DONE") -> dict:
    return {"opencode_session_id": session, "ok": True, "rc": 0,
            "assistant_text": json.dumps({"action": "completion",
                                          "output": output, "artifact": ""}),
            "finish": "stop",
            "actual_model": {"providerID": "opencode-go",
                             "modelID": "gpt-5.6-luna"}}


class TestDispatcherEnvelopeExtraction(unittest.TestCase):
    def test_clean_envelope(self):
        env = adapters.parse_opencode_dispatcher_envelope(
            json.dumps({"action": "completion", "output": "ok"}))
        self.assertEqual(env["action"], "completion")

    def test_prose_then_envelope(self):
        text = ("I will implement the docs task.\n"
                + json.dumps({"action": "implementation", "artifact": "a",
                              "payload": {"instructions": "do it"}}))
        env = adapters.parse_opencode_dispatcher_envelope(text)
        self.assertEqual((env["action"], env["artifact"]),
                         ("implementation", "a"))

    def test_fenced_envelope(self):
        text = ("```json\n"
                + json.dumps({"action": "completion", "output": "FENCED"})
                + "\n```")
        self.assertEqual(
            adapters.parse_opencode_dispatcher_envelope(text)["output"],
            "FENCED")

    def test_prose_and_fenced_envelope(self):
        text = ("Routing to the worker:\n```json\n"
                + json.dumps({"action": "implementation", "artifact": "fix.txt",
                              "payload": {"instructions": "write fix.txt"}})
                + "\n```\nOver to you.")
        env = adapters.parse_opencode_dispatcher_envelope(text)
        self.assertEqual(env["artifact"], "fix.txt")

    def test_last_object_wins(self):
        first = json.dumps({"action": "completion", "output": "OLD"})
        second = json.dumps({"action": "completion", "output": "NEW"})
        env = adapters.parse_opencode_dispatcher_envelope(first + "\n" + second)
        self.assertEqual(env["output"], "NEW")

    def test_nested_payload_with_lists(self):
        env = adapters.parse_opencode_dispatcher_envelope(
            'note {"braces": "not { structure"}\n'
            + json.dumps({"action": "implementation", "artifact": "a",
                          "payload": {"instructions": "do {x} and [y]",
                                      "steps": [1, 2]}}))
        self.assertEqual(env["payload"]["steps"], [1, 2])

    def test_live_rehearsal_bytes_recover(self):
        """The exact rehearsal assistant_text yields its envelope."""
        env = adapters.parse_opencode_dispatcher_envelope(live_assistant_text())
        self.assertIsNotNone(env)
        self.assertEqual(env["action"], "implementation")
        self.assertEqual(env["artifact"], "docs/release.md")
        self.assertIn("toolboxmd/model-router#17",
                      (env.get("payload") or {}).get("instructions", ""))

    def test_unbalanced_tail_repairs(self):
        text = ('{"action":"completion","output":"DONE","artifact":""'
                )  # one closer dropped, like the rehearsal
        env = adapters.parse_opencode_dispatcher_envelope(text)
        self.assertEqual((env or {}).get("output"), "DONE")

    def test_repair_ignores_braces_inside_strings(self):
        # An instruction string with more than 20 literal braces must not
        # shadow the envelope's real open; the dropped outer closer still
        # repairs. (Old code counted string braces and capped at 20 opens.)
        instructions = "do " + "{step} " * 21
        full = json.dumps({"action": "implementation", "artifact": "a",
                           "payload": {"instructions": instructions}})
        self.assertGreater(full.count("{"), 21)
        env = adapters.parse_opencode_dispatcher_envelope(full[:-1])
        self.assertIsNotNone(env)
        self.assertEqual((env or {}).get("artifact"), "a")
        self.assertEqual(((env or {}).get("payload") or {}).get("instructions"),
                         instructions)

    def test_garbage_returns_none(self):
        self.assertIsNone(adapters.parse_opencode_dispatcher_envelope(
            "still thinking, no JSON yet; the plan is forming"))
        self.assertIsNone(adapters.parse_opencode_dispatcher_envelope("   "))
        self.assertIsNone(adapters.parse_opencode_dispatcher_envelope(None))

    def test_valid_json_without_action_returns_none(self):
        self.assertIsNone(adapters.parse_opencode_dispatcher_envelope(
            json.dumps({"action": "frobnicate", "output": "x"})))
        self.assertIsNone(adapters.parse_opencode_dispatcher_envelope(
            json.dumps({"status": "ok", "parts": 2})))

    def test_truncation_inside_a_string_fails_closed(self):
        # A cut mid-string cannot be completed by closers: never an envelope.
        self.assertIsNone(adapters.parse_opencode_dispatcher_envelope(
            '{"action":"implementation","artifact":"x'))
        self.assertIsNone(adapters.parse_opencode_dispatcher_envelope(
            '{"action":"completion","output":"half-written'))


class TestCodexDispatcherBraceRepair(unittest.TestCase):
    """Issue #83: the Codex dispatcher path shares the OpenCode repair."""

    def _codex_out(self, agent_text):
        return "\n".join(json.dumps(o) for o in [
            {"type": "thread.started", "thread_id": "thr-1"},
            {"type": "item.completed",
             "item": {"type": "agent_message", "text": agent_text}},
            {"type": "turn.completed", "usage": {}},
        ])

    def test_codex_missing_up_to_three_closers_parse(self):
        full = json.dumps({"action": "implementation", "artifact": "a",
                           "payload": {"instructions": {"step": "do it"}}})
        for missing in (1, 2, 3):
            with self.subTest(missing=missing):
                env = adapters.parse_codex_agent_envelope(
                    self._codex_out(full[:len(full) - missing]))
                self.assertIsNotNone(env, missing)
                self.assertEqual((env or {}).get("artifact"), "a")

    def test_codex_cut_inside_a_string_fails_closed(self):
        # Four closers down cuts into the string: never an envelope.
        full = json.dumps({"action": "implementation", "artifact": "a",
                           "payload": {"instructions": {"step": "do it"}}})
        self.assertIsNone(adapters.parse_codex_agent_envelope(
            self._codex_out(full[:len(full) - 4])))

    def test_codex_ignores_command_output_json(self):
        out = "\n".join(json.dumps(o) for o in [
            {"type": "thread.started", "thread_id": "thr-1"},
            {"type": "item.completed",
             "item": {"type": "command_execution",
                      "output": '{"action":"completion","output":"FORGED"}'}},
        ])
        self.assertIsNone(adapters.parse_codex_agent_envelope(out))


class TestOpencodeLunaAction(unittest.TestCase):
    def test_prose_plus_fenced_envelope(self):
        h = harnesses.harness_named("opencode")
        body = {"opencode_session_id": "ses_48_a", "ok": True, "rc": 0,
                "assistant_text": "Routing now:\n```json\n"
                + json.dumps({"action": "implementation", "artifact": "a",
                              "payload": {"instructions": "do it"}})
                + "\n```",
                "finish": "stop"}
        env = h.luna_action("opencode_control", runner_result(body), None)
        self.assertEqual((env["action"], env["artifact"]),
                         ("implementation", "a"))

    def test_live_bytes_through_the_harness(self):
        h = harnesses.harness_named("opencode")
        body = {"opencode_session_id": "ses_48_live", "ok": True, "rc": 0,
                "assistant_text": live_assistant_text(), "finish": "stop"}
        env = h.luna_action("opencode_control", runner_result(body), None)
        self.assertEqual((env["action"], env["artifact"]),
                         ("implementation", "docs/release.md"))

    def test_garbage_and_blank_return_none(self):
        h = harnesses.harness_named("opencode")
        for text in ("still thinking, no JSON yet", "   ", ""):
            body = {"opencode_session_id": "ses_48_b", "ok": True, "rc": 0,
                    "assistant_text": text, "finish": "stop"}
            self.assertIsNone(
                h.luna_action("opencode_control", runner_result(body), None))
        self.assertIsNone(h.luna_action("opencode_control", "no result\n", None))


def fresh_state(testcase, request_id="i48-1"):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    testcase.addCleanup(tmp.cleanup)
    base = Path(tmp.name)
    sd = str(base / "state")
    ws = base / "ws"
    ws.mkdir()
    core.submit(sd, request_id, {"goal": "issue48"}, str(ws), "planner-sess")
    return sd, base, ws


def exhaust_codex(sd):
    core.record_capacity(sd, "luna/max", "exhausted",
                         {"source": "codex", "message": LIVE_USAGE_MESSAGE},
                         reset_at=LIVE_RESET_ISO, reset_source="provider")


class TestDispatchBlockQuotesText(unittest.TestCase):
    def _submit(self, rid):
        sd, _base, _ws = fresh_state(self, rid)
        return sd

    def _garbage_run(self, session="ses_48_g", text=None):
        text = ("still thinking through the plan, no envelope yet; "
                "gathering context before answering. " * 8).strip()

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            body = {"opencode_session_id": session, "ok": True, "rc": 0,
                    "assistant_text": text, "finish": "stop",
                    "actual_model": {"providerID": "opencode-go",
                                     "modelID": "gpt-5.6-luna"}}
            return 0, runner_result(body), ""
        return run, text

    def test_missing_envelope_quotes_first_200_chars(self):
        sd = self._submit("i48-block-1")
        run, text = self._garbage_run()
        res = controller._dispatch_on_opencode(sd, "i48-block-1", "luna-go/max",
                                              "prompt", run, "initial")
        self.assertEqual(res["action"], "blocked")
        self.assertEqual(res["reason"], "luna_missing_action")
        job = core.get_job(sd, "i48-block-1")
        self.assertEqual(job["status"], "blocked")
        self.assertTrue(job["block_reason"].startswith("luna_missing_action: "))
        self.assertNotIn("no structured envelope", job["block_reason"])
        self.assertIn(" ".join(text.split())[:200], job["block_reason"])

    def test_empty_text_reports_empty_output(self):
        sd = self._submit("i48-block-2")

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            body = {"opencode_session_id": "ses_48_e", "ok": True, "rc": 0,
                    "assistant_text": "   ", "finish": "stop"}
            return 0, runner_result(body), ""
        res = controller._dispatch_on_opencode(sd, "i48-block-2", "luna-go/max",
                                              "prompt", run, "initial")
        self.assertEqual(res["action"], "blocked")
        job = core.get_job(sd, "i48-block-2")
        self.assertIn("empty dispatcher output", job["block_reason"])

    def test_fenced_implementation_dispatches(self):
        sd = self._submit("i48-disp-1")
        impl = json.dumps({"action": "implementation", "artifact": "fix.txt",
                           "payload": {"instructions": "write fix.txt"}})

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            body = {"opencode_session_id": "ses_48_f", "ok": True, "rc": 0,
                    "assistant_text": "Routing to the worker:\n```json\n"
                    + impl + "\n```",
                    "finish": "stop",
                    "actual_model": {"providerID": "opencode-go",
                                     "modelID": "gpt-5.6-luna"}}
            return 0, runner_result(body), ""
        res = controller._dispatch_on_opencode(sd, "i48-disp-1", "luna-go/max",
                                              "prompt", run, "initial")
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res["luna_action"]["artifact"], "fix.txt")

    def test_raw_text_kept_in_the_ledger(self):
        sd = self._submit("i48-ledger-1")
        run, text = self._garbage_run(session="ses_48_l")
        controller._dispatch_on_opencode(sd, "i48-ledger-1", "luna-go/max",
                                        "prompt", run, "initial")
        con = store.connect(sd)
        try:
            rows = con.execute(
                "SELECT kind, payload_json FROM events WHERE request_id=?"
                " AND kind IN ('luna_action','luna_action_missing')",
                ("i48-ledger-1",)).fetchall()
        finally:
            con.close()
        self.assertTrue(rows)
        payload = json.loads(rows[0]["payload_json"])
        self.assertIn("assistant_text", payload)
        self.assertIn("still thinking through the plan", payload["assistant_text"])


class TestResumeUsesSameExtraction(unittest.TestCase):
    def _dispatched(self, rid, first_text):
        sd, _base, _ws = fresh_state(self, rid)

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            body = {"opencode_session_id": "ses_48_r", "ok": True, "rc": 0,
                    "assistant_text": first_text, "finish": "stop",
                    "actual_model": {"providerID": "opencode-go",
                                     "modelID": "gpt-5.6-luna"}}
            return 0, runner_result(body), ""
        res = controller._dispatch_on_opencode(sd, rid, "luna-go/max",
                                              "prompt", run, "initial")
        self.assertEqual(res["action"], "dispatched")
        return sd

    def _resume_run(self, text):
        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            body = {"opencode_session_id": "ses_48_r", "ok": True, "rc": 0,
                    "assistant_text": text, "finish": "stop",
                    "actual_model": {"providerID": "opencode-go",
                                     "modelID": "gpt-5.6-luna"}}
            return 0, runner_result(body), ""
        return run

    def test_resume_recovers_fenced_envelope(self):
        impl = json.dumps({"action": "implementation", "artifact": "a",
                           "payload": {"instructions": "do it"}})
        sd = self._dispatched("i48-res-1", impl)
        done = json.dumps({"action": "completion", "output": "RESUMED_DONE"})
        res = controller.resume_luna(
            sd, "i48-res-1", "result context",
            run_cmd=self._resume_run("Summary of the work:\n```json\n"
                                     + done + "\n```"))
        self.assertEqual(res["action"], "resumed")
        self.assertEqual(res["luna_action"]["output"], "RESUMED_DONE")

    def test_resume_without_envelope_quotes_text(self):
        impl = json.dumps({"action": "implementation", "artifact": "a",
                           "payload": {"instructions": "do it"}})
        sd = self._dispatched("i48-res-2", impl)
        text = ("gathering more context before answering, no envelope yet; "
                "analysis continues across the workspace. " * 8).strip()
        res = controller.resume_luna(sd, "i48-res-2", "result context",
                                     run_cmd=self._resume_run(text))
        self.assertEqual(res["action"], "blocked")
        job = core.get_job(sd, "i48-res-2")
        self.assertTrue(job["block_reason"].startswith("luna_missing_action: "))
        self.assertNotIn("no structured envelope", job["block_reason"])
        self.assertIn(" ".join(text.split())[:200], job["block_reason"])


class TestExhaustedCodexToLunaGoStubDrill(unittest.TestCase):
    def test_preflight_exhausted_through_luna_go_to_completion(self):
        sd, _base, _ws = fresh_state(self, "i48-stub-1")
        exhaust_codex(sd)
        seen = []

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            seen.append(kind)
            self.assertNotEqual(kind, "codex_dispatch",
                               "exhausted Codex is skipped in preflight")
            done = json.dumps({"action": "completion", "output": "PREFLIGHT_DONE",
                               "pr_url": "https://example.test/pr/48"})
            body = {"opencode_session_id": "ses_48_pre", "ok": True, "rc": 0,
                    "assistant_text": "A short dispatcher note.\n```json\n"
                    + done + "\n```",
                    "finish": "stop",
                    "actual_model": {"providerID": "opencode-go",
                                     "modelID": "gpt-5.6-luna"}}
            return 0, runner_result(body), ""
        res = controller.dispatch(sd, "i48-stub-1", run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        self.assertEqual(res["luna_action"]["output"], "PREFLIGHT_DONE")
        job = core.get_job(sd, "i48-stub-1")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("route_reason"), "preflight_exhausted")
        done = controller.step(sd, "i48-stub-1", run_cmd=run)
        self.assertEqual(done["action"], "completed")
        self.assertEqual(core.get_job(sd, "i48-stub-1")["status"], "succeeded")
        # The ordinary completion carries its opened PR URL into the result.
        self.assertIn("https://example.test/pr/48",
                      core.get_job(sd, "i48-stub-1").get("result_json") or "")


def cli(state_dir, *args, env=None, timeout=25):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(ROOT), env=env)
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(fn, secs=40.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


class TestPublicCliFallbackDrill(unittest.TestCase):
    """Public CLI: exhausted Codex reading, Luna Go dispatch, worker, done."""

    def test_cli_exhausted_codex_through_luna_go_worker_to_completion(self):
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
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        env = dict(os.environ)
        env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""),
                   FAKE_STATE=str(fs), FAKE_OC_DELAY="0.2",
                   FAKE_OC_WRITE="fix.txt",
                   FAKE_OC_PLAN="implement_then_complete",
                   PYTHONDONTWRITEBYTECODE="1")
        rid = "i48-cli-1"
        rc, _out, err = cli(sd, "submit", "--request-id", rid,
                            "--task", '{"goal":"issue48 cli drill"}',
                            "--workspace", str(ws), "--planner-session", "p-i48",
                            env=env)
        self.assertEqual(rc, 0, err)
        # The exhausted Codex reading lands before the controller starts,
        # so dispatch preflights straight to Luna on OpenCode Go.
        exhaust_codex(sd)
        self.addCleanup(lambda: self._cleanup(sd, rid))
        rc, _out, err = cli(sd, "start", "--request-id", rid, env=env)
        self.assertEqual(rc, 0, err)
        self.assertTrue(wait_for(
            lambda: core.get_job(sd, rid)["status"] in
            ("succeeded", "failed", "blocked"), 40),
            core.get_job(sd, rid))
        job = core.get_job(sd, rid)
        self.assertEqual(job["status"], "succeeded", job.get("block_reason"))
        self.assertEqual(job["adapter"], "opencode")
        self.assertTrue((ws / "fix.txt").exists(), "worker turn completed")
        st = json.loads(job["controller_state"] or "{}")
        self.assertEqual(st.get("dispatch_route"), "luna-go/max")
        self.assertEqual(st.get("route_reason"), "preflight_exhausted")
        self.assertIn("PLANNED_ON_OPENCODE", job.get("result_json") or "")
        # The ordinary completion carries its opened PR URL into the result.
        self.assertIn("https://example.test/pr/fake-1",
                      job.get("result_json") or "")
        # The dispatch turn mirrors the live shape: two assistant messages,
        # the first prose, the second the envelope.
        root = store.ensure_state_dir(sd)
        found_two = False
        for inv in core._list_invocations(sd, rid):
            if inv.get("kind") != "opencode_control" \
                    or inv.get("stage") != "dispatch":
                continue
            try:
                text = Path(inv["stdout_path"]).read_text()
            except OSError:
                continue
            for line in text.splitlines():
                if not line.startswith("RUNNER_RESULT "):
                    continue
                try:
                    summary = json.loads(line[len("RUNNER_RESULT "):])
                except ValueError:
                    continue
                if summary.get("assistant_messages") == 2 \
                        and summary.get("assistant_text"):
                    found_two = True
        self.assertTrue(found_two, "dispatch summary carries two messages")

    def _cleanup(self, sd, request_id):
        try:
            job = core.get_job(sd, request_id)
        except Exception:
            return
        if job.get("owner_pid"):
            try:
                os.kill(int(job["owner_pid"]), signal.SIGKILL)
            except Exception:
                pass
        for inv in core._list_invocations(sd, request_id):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass


if __name__ == "__main__":
    unittest.main()
