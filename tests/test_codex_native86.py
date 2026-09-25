"""#86: native Codex dispatcher transport regression tests.

Proves the job-owned native app-server transport: proven protocol params,
loopback-only endpoints, native-to-CLI event conversion, original-turn
identity, observer attach/steer/disconnect/reconnect, cancellation via
turn/interrupt, recovery, and failure cases. Deterministic and stdlib
only: a fake ``codex`` binary serves the real WebSocket framing with
scripted dispatcher answers; no live model is called.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import codex_native, controller, core, harnesses, policy, store  # noqa: E402
from tests.fakes import FAKE_CODEX_NATIVE_APP, write_fake  # noqa: E402

PY = sys.executable
THREAD = "native-thread-001"


def sql(sd, stmt, args=()):
    con = store.connect(sd)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(stmt, args)
        con.execute("COMMIT")
    finally:
        con.close()


class ProtocolParams(unittest.TestCase):
    def test_thread_start_names_model_sandbox_and_never_ask(self):
        params = codex_native.thread_start_params("/tmp/ws", "gpt-5.6-luna")
        self.assertEqual(params["cwd"], "/tmp/ws")
        self.assertEqual(params["model"], "gpt-5.6-luna")
        self.assertEqual(params["sandbox"], "read-only")
        self.assertEqual(params["approvalPolicy"], "never")
        with self.assertRaises(ValueError):
            codex_native.thread_start_params("", "m")
        with self.assertRaises(ValueError):
            codex_native.thread_start_params("/tmp/ws", "")

    def test_turn_start_carries_input_shape_and_effort(self):
        params = codex_native.turn_start_params("thr-1", "do work", "max")
        self.assertEqual(params["threadId"], "thr-1")
        self.assertEqual(params["effort"], "max")
        self.assertEqual(params["input"],
                         [{"type": "text", "text": "do work", "text_elements": []}])
        self.assertEqual(codex_native.turn_start_params("t", "x", "")["effort"], "max")
        with self.assertRaises(ValueError):
            codex_native.turn_start_params("", "x", "max")
        with self.assertRaises(ValueError):
            codex_native.turn_start_params("t", "", "max")

    def test_resume_is_metadata_only_and_never_changes_settings(self):
        params = codex_native.resume_params("thr-1")
        self.assertEqual(params, {"threadId": "thr-1", "excludeTurns": True})
        for banned in ("model", "sandbox", "approvalPolicy", "permissions",
                       "cwd", "config"):
            self.assertNotIn(banned, params)
        with self.assertRaises(ValueError):
            codex_native.resume_params("")

    def test_steer_names_the_expected_control_turn(self):
        params = codex_native.steer_params("thr-1", "turn-9", "ack please")
        self.assertEqual(params["threadId"], "thr-1")
        self.assertEqual(params["expectedTurnId"], "turn-9")
        self.assertEqual(params["input"][0]["text"], "ack please")
        with self.assertRaises(ValueError):
            codex_native.steer_params("thr-1", "", "x")

    def test_read_and_interrupt_params(self):
        self.assertEqual(codex_native.read_params("thr-1"),
                         {"threadId": "thr-1", "includeTurns": True})
        self.assertEqual(codex_native.interrupt_params("thr-1", "turn-1"),
                         {"threadId": "thr-1", "turnId": "turn-1"})
        with self.assertRaises(ValueError):
            codex_native.read_params("")
        with self.assertRaises(ValueError):
            codex_native.interrupt_params("thr-1", "")


class EndpointPolicy(unittest.TestCase):
    def test_loopback_endpoints_validate(self):
        host, port = codex_native.validate_endpoint("ws://127.0.0.1:4242")
        self.assertEqual((host, port), ("127.0.0.1", 4242))
        codex_native.validate_endpoint("ws://localhost:1")
        codex_native.validate_endpoint("ws://[::1]:2")

    def test_remote_scheme_credentials_and_port_refused(self):
        for bad in ("http://127.0.0.1:1", "ws://10.0.0.5:9",
                    "ws://example.test:9", "ws://127.0.0.1",
                    "ws://user:pw@127.0.0.1:9", "", "ws://127.0.0.1:notaport"):
            with self.assertRaises(codex_native.NativeError, msg=bad):
                codex_native.validate_endpoint(bad)


class EventConversion(unittest.TestCase):
    def test_thread_started_maps_to_cli_shape(self):
        out = codex_native.convert_event(
            {"method": "thread/started", "params": {"thread": {"id": "thr-x"}}})
        self.assertEqual(out, [{"type": "thread.started", "thread_id": "thr-x"}])
        self.assertEqual(codex_native.convert_event(
            {"method": "thread/started", "params": {}}), [])

    def test_agent_message_maps_command_and_reasoning_do_not(self):
        agent = {"method": "item/completed",
                 "params": {"item": {"type": "agentMessage", "id": "msg-1",
                                     "text": '{"action":"completion"}'},
                            "threadId": "t", "turnId": "turn-1"}}
        out = codex_native.convert_event(agent, control_turn_id="turn-1")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["item"]["type"], "agent_message")
        self.assertEqual(out[0]["item"]["id"], "msg-1")
        for itype in ("commandExecution", "reasoning", "userMessage"):
            ev = {"method": "item/completed",
                  "params": {"item": {"type": itype, "id": "x", "text": "t"},
                             "turnId": "turn-1"}}
            activity = codex_native.convert_event(ev, control_turn_id="turn-1")
            self.assertTrue(activity, itype)
            self.assertTrue(all(line["type"] == "native.activity" for line in activity))
            self.assertNotIn('"text"', json.dumps(activity))

    def test_deltas_mark_activity_without_partial_envelopes(self):
        ev = {"method": "item/agentMessage/delta",
              "params": {"delta": {"text": "hello"}, "turnId": "turn-1"}}
        activity = codex_native.convert_event(ev, control_turn_id="turn-1")
        self.assertTrue(activity)
        self.assertTrue(all(line["type"] == "native.activity" for line in activity))
        self.assertNotIn("hello", json.dumps(activity))

    def test_only_the_control_turn_converts(self):
        ours = {"method": "item/completed",
                "params": {"item": {"type": "agentMessage", "id": "m1",
                                    "text": "CONTROL"},
                           "turnId": "turn-1"}}
        foreign = {"method": "item/completed",
                   "params": {"item": {"type": "agentMessage", "id": "m2",
                                       "text": "FOREIGN"},
                              "turnId": "turn-9"}}
        self.assertEqual(len(codex_native.convert_event(ours, control_turn_id="turn-1")), 1)
        self.assertEqual(codex_native.convert_event(foreign, control_turn_id="turn-1"), [])
        done_ours = {"method": "turn/completed",
                     "params": {"threadId": "t", "turn": {"id": "turn-1"}}}
        done_foreign = {"method": "turn/completed",
                        "params": {"threadId": "t", "turn": {"id": "turn-9"}}}
        self.assertEqual(len(codex_native.convert_event(done_ours, control_turn_id="turn-1")), 1)
        self.assertEqual(codex_native.convert_event(done_foreign, control_turn_id="turn-1"), [])
        failed = {"method": "turn/failed",
                  "params": {"turn": {"id": "turn-1"},
                             "error": {"code": "UsageLimitExceeded",
                                       "message": "limit hit"}}}
        out = codex_native.convert_event(failed, control_turn_id="turn-1")
        self.assertEqual(out[0]["code"], "UsageLimitExceeded")

    def test_usage_maps_from_token_events(self):
        events = [
            {"method": "item/agentMessage/delta", "params": {}},
            {"method": "thread/tokenUsage/updated",
             "params": {"tokenUsage": {"total": {"totalTokens": 900}, "last": {"totalTokens": 100,
                                                 "inputTokens": 90,
                                                 "outputTokens": 10,
                                                 "cachedInputTokens": 80,
                                                 "reasoningOutputTokens": 4}}}},
        ]
        usage = codex_native.usage_from_token_events(events)
        self.assertEqual(usage["input_tokens"], 90)
        self.assertEqual(usage["output_tokens"], 10)
        self.assertEqual(usage["total_tokens"], 100)
        self.assertIsNone(codex_native.usage_from_token_events([]))

    def test_id_helpers_read_first_wins(self):
        stdout = ('{"type": "turn.started", "turn_id": "turn-1"}\n'
                  '{"type": "thread.started", "thread_id": "thr-1"}\n'
                  '{"type": "thread.started", "thread_id": "thr-2"}\n')
        self.assertEqual(codex_native.turn_id_from_lines(stdout), "turn-1")
        self.assertEqual(codex_native.thread_id_from_lines(stdout), "thr-1")
        self.assertIsNone(codex_native.turn_id_from_lines("not json\n"))


class ActionIdentity(unittest.TestCase):
    def test_driver_argv_carries_no_prompt_port_or_endpoint(self):
        cmd = codex_native.python_driver_cmd()
        blob = " ".join(cmd)
        self.assertNotIn("ws://", blob)
        self.assertIn("codex_native_turn.py", blob)
        meta = controller.native_turn_meta("dispatch", "PROMPT", "m", "e",
                                           "luna/max", "initial")
        self.assertEqual(meta["native"], {"op": "dispatch"})
        self.assertEqual(meta["prompt"], "PROMPT")
        key1 = core._action_key("codex_dispatch", cmd, meta)
        key2 = core._action_key("codex_dispatch", cmd, dict(meta))
        self.assertEqual(key1, key2)
        other = controller.native_turn_meta("dispatch", "OTHER", "m", "e",
                                            "luna/max", "initial")
        self.assertNotEqual(key1, core._action_key("codex_dispatch", cmd, other))


def driver_out(thread_id, turn_id, envelope, usage=None):
    lines = [
        {"type": "thread.started", "thread_id": thread_id},
        {"type": "turn.started", "turn_id": turn_id, "thread_id": thread_id},
        {"type": "item.completed",
         "item": {"type": "agent_message", "text": json.dumps(envelope), "id": "msg-1"}},
    ]
    tail = {"type": "turn.completed", "turn_id": turn_id, "thread_id": thread_id}
    if usage is not None:
        tail["usage"] = usage
    lines.append(tail)
    return "\n".join(json.dumps(line) for line in lines) + "\n"


class FakeServerIntegration(unittest.TestCase):
    """Real driver + real WS framing against a fake native app-server."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()
        self.fs = self.base / "fakestate"
        self.fs.mkdir()
        bindir = self.base / "bin"
        bindir.mkdir()
        header = ("import json, os, sys\nfrom pathlib import Path\n"
                  "st = Path(os.environ['FAKE_STATE'])\n"
                  "st.mkdir(parents=True, exist_ok=True)\n"
                  "argv = sys.argv[1:]\n"
                  "with open(st / 'codex.log', 'a') as _f:\n"
                  "    _f.write(json.dumps(argv) + '\\n')\n")
        write_fake(bindir, "codex", header + FAKE_CODEX_NATIVE_APP, PY)
        self.env = dict(os.environ)
        self.env.update(
            PATH=str(bindir) + os.pathsep + self.env.get("PATH", ""),
            FAKE_STATE=str(self.fs),
            FAKE_NATIVE_THREAD=THREAD,
            FAKE_NATIVE_PLAN="completion",
            FAKE_NATIVE_OUTPUT="NATIVE_DONE",
            MODEL_ROUTER_CODEX_HOME=str(self.base / "cx-home"),
            PYTHONDONTWRITEBYTECODE="1")
        (self.base / "cx-home").mkdir(exist_ok=True)
        # Hermetic PATH and Codex home for in-process server calls too.
        saved = {k: os.environ.get(k) for k in ("PATH", "MODEL_ROUTER_CODEX_HOME")}
        os.environ["PATH"] = self.env["PATH"]
        os.environ["MODEL_ROUTER_CODEX_HOME"] = self.env["MODEL_ROUTER_CODEX_HOME"]
        self.addCleanup(lambda: [os.environ.pop(k, None) if v is None
                                 else os.environ.__setitem__(k, v)
                                 for k, v in saved.items()])
        self.servers = []
        self.jobs = []
        self.addCleanup(self._kill_servers)

    def _kill_servers(self):
        for rid in self.jobs:
            codex_native.stop_server(self.sd, rid, "test-cleanup")
        for pid in self.servers:
            try:
                os.killpg(int(pid), signal.SIGKILL)
            except Exception:
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except Exception:
                    pass

    def _submit(self, rid):
        core.submit(self.sd, rid, {"goal": "native drill"}, str(self.ws), "planner-1")
        self.jobs.append(rid)

    def _insert_inv(self, rid, iid, kind, op, prompt="LUNA PROMPT"):
        root = store.ensure_state_dir(self.sd)
        outp = root / "outputs" / f"{rid}.{iid}.stdout"
        errp = root / "outputs" / f"{rid}.{iid}.stderr"
        store.secure_write_text(outp, "")
        store.secure_write_text(errp, "")
        meta = controller.native_turn_meta(op, prompt, "gpt-5.6-luna", "max",
                                           "luna/max", "initial")
        sql(self.sd,
            "INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
            "owner_token,stdout_path,stderr_path,started_at,state,timeout_secs,meta_json,"
            "action_key,stage,requested_route,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (iid, rid, kind, json.dumps(codex_native.python_driver_cmd()),
             str(self.ws), "tok", str(outp), str(errp), core._utcnow(), "running",
             120, json.dumps(meta),
             core._action_key(kind, codex_native.python_driver_cmd(), meta),
             "dispatch", "luna/max", "initial"))
        return outp, errp

    def _run_driver(self, rid, iid, timeout=60):
        env = dict(self.env)
        env.update(MR_NATIVE_STATE_DIR=self.sd, MR_NATIVE_REQUEST_ID=rid,
                   MR_NATIVE_INVOCATION_ID=iid)
        return subprocess.run([PY, str(ROOT / "runner" / "codex_native_turn.py")],
                              capture_output=True, text=True, timeout=timeout,
                              cwd=str(self.base), env=env)

    def _runtime(self, rid):
        return codex_native.read_runtime(core.get_job(self.sd, rid))

    def _remember_server(self, rid):
        pid = self._runtime(rid).get("pid")
        if pid and pid not in self.servers:
            self.servers.append(pid)

    def test_dispatch_turn_end_to_end(self):
        rid = "n86-dispatch"
        self._submit(rid)
        self._insert_inv(rid, "d1", "codex_dispatch", "dispatch")
        proc = self._run_driver(rid, "d1")
        self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
        lines = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
        by_type = [line.get("type") for line in lines]
        self.assertIn("thread.started", by_type)
        self.assertIn("turn.started", by_type)
        self.assertIn("turn.completed", by_type)
        agent = [line for line in lines
                 if line.get("type") == "item.completed"]
        self.assertEqual(len(agent), 1)
        envelope = json.loads(agent[0]["item"]["text"])
        self.assertEqual(envelope["action"], "completion")
        completed = [line for line in lines if line.get("type") == "turn.completed"][0]
        self.assertEqual(completed["usage"]["source"], "codex")
        runtime = self._runtime(rid)
        self.assertEqual(runtime.get("thread_id"), THREAD)
        self.assertTrue(str(runtime.get("control_turn_id") or "").startswith(THREAD))
        self.assertTrue(str(runtime.get("endpoint") or "").startswith("ws://127.0.0.1:"))
        self._remember_server(rid)
        # The harness reads the envelope and session from converted output.
        h = harnesses.harness_named("codex")
        self.assertEqual(h.parse_session("codex_dispatch", proc.stdout, "", None)[0], THREAD)
        self.assertEqual(h.luna_action("codex_dispatch", proc.stdout, None)["action"],
                         "completion")
        # Transport assertions on the recorded RPCs.
        rpcs = [json.loads(line) for line in
                (self.fs / "native-requests.jsonl").read_text().splitlines()]
        init = [r for r in rpcs if r["method"] == "initialize"][0]
        self.assertTrue(init["params"]["capabilities"]["experimentalApi"])
        started = [r for r in rpcs if r["method"] == "thread/start"][0]
        self.assertEqual(started["params"]["model"], "gpt-5.6-luna")
        self.assertEqual(started["params"]["sandbox"], "read-only")
        self.assertEqual(started["params"]["approvalPolicy"], "never")
        turn = [r for r in rpcs if r["method"] == "turn/start"][0]
        self.assertEqual(turn["params"]["effort"], "max")
        self.assertEqual(turn["params"]["input"][0]["text"], "LUNA PROMPT")

    def test_resume_reuses_server_and_thread(self):
        rid = "n86-resume"
        self._submit(rid)
        self._insert_inv(rid, "d1", "codex_dispatch", "dispatch")
        first = self._run_driver(rid, "d1")
        self.assertEqual(first.returncode, 0, first.stderr[-2000:])
        first_turn = codex_native.turn_id_from_lines(first.stdout)
        sql(self.sd, "UPDATE jobs SET codex_task_id=? WHERE request_id=?", (THREAD, rid))
        self._insert_inv(rid, "r1", "codex_resume", "resume", prompt="FOLLOWUP")
        second = self._run_driver(rid, "r1")
        self.assertEqual(second.returncode, 0, second.stderr[-2000:])
        second_turn = codex_native.turn_id_from_lines(second.stdout)
        self.assertNotEqual(first_turn, second_turn)
        self.assertEqual(codex_native.thread_id_from_lines(second.stdout), THREAD)
        runtime = self._runtime(rid)
        self.assertEqual(runtime.get("control_turn_id"), second_turn)
        self._remember_server(rid)
        spawns = [json.loads(line) for line in (self.fs / "codex.log").read_text().splitlines()
                  if len(line) > 2 and json.loads(line)[:2] == ["app-server", "--listen"]]
        self.assertEqual(len(spawns), 1, "one job-owned server across dispatch and resume")
        rpcs = [json.loads(line) for line in
                (self.fs / "native-requests.jsonl").read_text().splitlines()]
        resumes = [r for r in rpcs if r["method"] == "thread/resume"]
        self.assertTrue(resumes, "resume attaches before the new turn")
        for r in resumes:
            self.assertEqual(set(r["params"]), {"threadId", "excludeTurns"})
        # Observed model reads from the shared sessions dir still work.
        self.assertEqual(first_turn is not None, True)

    def test_second_client_observe_steer_disconnect_reconnect(self):
        rid = "n86-observer"
        self._submit(rid)
        self.env["FAKE_NATIVE_MODE"] = "hang"
        self._insert_inv(rid, "d1", "codex_dispatch", "dispatch")
        env = dict(self.env)
        env.update(MR_NATIVE_STATE_DIR=self.sd, MR_NATIVE_REQUEST_ID=rid,
                   MR_NATIVE_INVOCATION_ID="d1")
        proc = subprocess.Popen([PY, str(ROOT / "runner" / "codex_native_turn.py")],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, cwd=str(self.base), env=env)
        self.addCleanup(lambda: proc.kill() if proc.poll() is None else None)
        try:
            started = {}
            end = time.monotonic() + 30
            buf = ""
            while time.monotonic() < end and "turn.started" not in buf:
                chunk = proc.stdout.readline()
                if not chunk:
                    break
                buf += chunk
                for line in buf.splitlines():
                    try:
                        obj = json.loads(line)
                    except ValueError:
                        continue
                    if obj.get("type") in ("thread.started", "turn.started"):
                        started[obj["type"]] = obj
            self.assertIn("turn.started", buf, "driver started the control turn")
            turn_id = json.loads(
                [line for line in buf.splitlines()
                 if "turn.started" in line][0])["turn_id"]
            endpoint = self._runtime(rid).get("endpoint")
            self.assertTrue(endpoint)
            # Second native client: discover, metadata-only attach, steer.
            observer = codex_native.NativeClient.connect(endpoint, role="t3_observer")
            try:
                loaded = observer.thread_loaded_list()
                data = loaded.get("data") if isinstance(loaded, dict) else None
                self.assertIn(THREAD, data if isinstance(data, list) else [])
                resumed = observer.thread_resume(THREAD)
                self.assertEqual(resumed.get("thread", {}).get("id"), THREAD)
                steered = observer.turn_steer(THREAD, turn_id, "fleet question")
                self.assertEqual(steered.get("turnId"), turn_id)
            finally:
                observer.close()
            # Disconnect and reconnect: the same thread, agent undisturbed.
            observer2 = codex_native.NativeClient.connect(endpoint, role="t3_reconnected")
            try:
                read = observer2.thread_read(THREAD)
                self.assertEqual(read.get("thread", {}).get("id"), THREAD)
            finally:
                observer2.close()
            self.assertTrue(codex_native.server_reachable(endpoint))
            rpcs = [json.loads(line) for line in
                    (self.fs / "native-requests.jsonl").read_text().splitlines()]
            methods = [r["method"] for r in rpcs]
            self.assertIn("thread/loaded/list", methods)
            self.assertIn("turn/steer", methods)
            self.assertNotIn("thread/settings/update", methods)
            self.assertNotIn("turn/settings/update", methods)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
            try:
                if proc.stdout:
                    proc.stdout.close()
            except Exception:
                pass
            try:
                if proc.stderr:
                    proc.stderr.close()
            except Exception:
                pass
        interrupts = []
        if (self.fs / "native-interrupts.jsonl").exists():
            interrupts = [json.loads(line) for line in
                          (self.fs / "native-interrupts.jsonl").read_text().splitlines()]
        self.assertTrue(any(i.get("turnId") == turn_id for i in interrupts),
                        "terminating the driver interrupts its control turn")
        self._remember_server(rid)

    def test_busy_foreign_turn_finishes_before_control_resumes(self):
        rid = "n86-busy"
        self._submit(rid)
        self.env["FAKE_NATIVE_MODE"] = "busy"
        sql(self.sd, "UPDATE jobs SET codex_task_id=? WHERE request_id=?", (THREAD, rid))
        self._insert_inv(rid, "r1", "codex_resume", "resume", prompt="FOLLOWUP")
        proc = self._run_driver(rid, "r1")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("turn.completed", proc.stdout)
        self.assertIn("NATIVE_DONE", proc.stdout)
        self.assertNotIn("FOREIGN_REPLY", proc.stdout)
        self.assertFalse((self.fs / "native-interrupts.jsonl").exists())
        self._remember_server(rid)

    def test_auth_refusal_text_reaches_stderr(self):
        rid = "n86-auth"
        self._submit(rid)
        self.env["FAKE_NATIVE_MODE"] = "auth"
        self._insert_inv(rid, "d1", "codex_dispatch", "dispatch")
        proc = self._run_driver(rid, "d1")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("401", proc.stderr)
        self.assertIn("Unauthorized", proc.stderr)
        self._remember_server(rid)

    def test_usage_limit_code_survives_as_evidence(self):
        rid = "n86-limit"
        self._submit(rid)
        self.env["FAKE_NATIVE_MODE"] = "limit"
        self._insert_inv(rid, "d1", "codex_dispatch", "dispatch")
        proc = self._run_driver(rid, "d1")
        self.assertEqual(proc.returncode, 1)
        errors = [json.loads(line) for line in proc.stdout.splitlines()
                  if line.strip() and json.loads(line).get("type") == "error"]
        self.assertTrue(errors)
        self.assertIn("UsageLimitExceeded",
                      json.dumps([e.get("code") for e in errors] + [proc.stdout]))
        self._remember_server(rid)

    def test_server_reuse_and_stop_ownership(self):
        rid = "n86-lifecycle"
        self._submit(rid)
        self._insert_inv(rid, "d1", "codex_dispatch", "dispatch")
        proc = self._run_driver(rid, "d1")
        self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
        first = self._runtime(rid)
        self.assertTrue(first.get("pid"))
        endpoint = first["endpoint"]
        again = codex_native.ensure_server(self.sd, rid, str(self.ws))
        self.assertEqual(again.get("endpoint"), endpoint, "live server is reused")
        self.assertTrue(codex_native.stop_server(self.sd, rid, "test"))
        cleared = self._runtime(rid)
        self.assertIsNone(cleared.get("pid"))
        self.assertIsNone(cleared.get("endpoint"))
        self.assertEqual(cleared.get("thread_id"), THREAD, "thread id survives for resume")
        # A foreign PID is never signalled: the record drops, the process lives.
        foreign_pid = os.getpid()
        codex_native.write_runtime(self.sd, rid, {"pid": foreign_pid, "pgid": foreign_pid,
                                                  "endpoint": endpoint,
                                                  "start": "not-our-start"})
        self.assertTrue(codex_native.stop_server(self.sd, rid, "test-foreign"))
        self.assertTrue(core._is_pid_alive(foreign_pid))
        cleared = self._runtime(rid)
        self.assertIsNone(cleared.get("pid"))

    def test_recover_clears_dead_server_and_keeps_thread(self):
        rid = "n86-recover"
        self._submit(rid)
        self._insert_inv(rid, "d1", "codex_dispatch", "dispatch")
        proc = self._run_driver(rid, "d1")
        self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
        pid = self._runtime(rid).get("pid")
        os.killpg(int(pid), signal.SIGKILL)
        end = time.monotonic() + 10
        while time.monotonic() < end and core._is_pid_alive(pid):
            time.sleep(0.1)
        out = codex_native.recover_server(self.sd, rid)
        self.assertEqual(out["action"], "native-cleared-dead")
        runtime = self._runtime(rid)
        self.assertIsNone(runtime.get("endpoint"))
        self.assertEqual(runtime.get("thread_id"), THREAD)
        # The next turn restarts the server and resumes the same thread.
        sql(self.sd, "UPDATE jobs SET codex_task_id=? WHERE request_id=?", (THREAD, rid))
        self._insert_inv(rid, "r1", "codex_resume", "resume", prompt="AGAIN")
        second = self._run_driver(rid, "r1")
        self.assertEqual(second.returncode, 0, second.stderr[-2000:])
        self.assertEqual(codex_native.thread_id_from_lines(second.stdout), THREAD)
        self._remember_server(rid)


class ControllerNativeTurns(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.sd = str(self.base / "state")
        self.ws = self.base / "ws"
        self.ws.mkdir()

    def _submit(self, rid):
        core.submit(self.sd, rid, {"goal": "native controller drill"}, str(self.ws),
                    "planner-1")

    def test_dispatch_uses_stable_driver_argv_and_native_meta(self):
        rid = "n86-ctl-d"
        self._submit(rid)
        seen = {}

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            seen["cmd"] = list(cmd)
            seen["meta"] = dict(meta or {})
            turn = f"{THREAD}-turn-1"
            return 0, driver_out(THREAD, turn, {"action": "completion",
                                               "output": "CTL_DONE"}), ""

        res = controller.dispatch(self.sd, rid, run_cmd=run,
                                  probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(seen["cmd"], codex_native.python_driver_cmd())
        blob = " ".join(seen["cmd"])
        self.assertNotIn("ws://", blob)
        meta = seen["meta"]
        self.assertEqual(meta["native"], {"op": "dispatch"})
        self.assertIn("native controller drill", meta["prompt"])
        self.assertEqual(meta["model"], policy.ROUTES["luna/max"]["model"])
        self.assertEqual(meta["effort"], policy.ROUTES["luna/max"]["variant"])
        job = core.get_job(self.sd, rid)
        self.assertEqual(job["codex_task_id"], THREAD)
        runtime = codex_native.read_runtime(job)
        self.assertEqual(runtime.get("control_turn_id"), f"{THREAD}-turn-1")
        self.assertEqual(runtime.get("thread_id"), THREAD)

    def test_resume_meta_names_no_thread_in_argv(self):
        rid = "n86-ctl-r"
        self._submit(rid)
        sql(self.sd, "UPDATE jobs SET codex_task_id=? WHERE request_id=?", (THREAD, rid))
        seen = {}

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            seen["cmd"] = list(cmd)
            seen["meta"] = dict(meta or {})
            return 0, driver_out(THREAD, f"{THREAD}-turn-2",
                                 {"action": "completion", "output": "R2"}), ""

        res = controller.resume_luna(self.sd, rid, "followup context", run_cmd=run)
        self.assertEqual(res["action"], "resumed")
        self.assertNotIn(THREAD, " ".join(seen["cmd"]))
        self.assertEqual(seen["meta"]["native"], {"op": "resume"})
        self.assertIn("followup context", seen["meta"]["prompt"])
        runtime = codex_native.read_runtime(core.get_job(self.sd, rid))
        self.assertEqual(runtime.get("control_turn_id"), f"{THREAD}-turn-2")

    def test_status_runtime_publishes_endpoint_without_credentials(self):
        rid = "n86-ctl-s"
        self._submit(rid)

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            return 0, driver_out(THREAD, f"{THREAD}-turn-1",
                                 {"action": "completion", "output": "S"}), ""

        controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        view = core.status_view(self.sd, rid)
        runtime = view["job"]["runtime"]
        self.assertEqual(runtime["native"]["server"], "absent")
        self.assertEqual(runtime["native"]["thread_id"], THREAD)
        blob = json.dumps(view)
        self.assertNotIn("password", blob.lower())
        self.assertNotIn("secret", blob.lower())
        self.assertNotIn("bearer", blob.lower())

    def test_auth_block_from_native_error_text(self):
        rid = "n86-ctl-a"
        self._submit(rid)

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            out = ('{"type": "thread.started", "thread_id": "thr-auth"}\n'
                   '{"type": "error", "code": 401, '
                   '"message": "failed to connect to websocket: HTTP error: '
                   '401 Unauthorized"}\n')
            err = ("native thread/start failed: failed to connect to websocket: "
                   "HTTP error: 401 Unauthorized\n")
            return 1, out, err

        res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        self.assertEqual(res["action"], "blocked")
        self.assertEqual(res["reason"], "codex_auth_failed")

    def test_limit_falls_back_and_marks_capacity(self):
        rid = "n86-ctl-l"
        self._submit(rid)
        calls = []

        def run(cmd, cwd=None, timeout=None, kind=None, meta=None):
            calls.append(kind)
            if kind == "codex_dispatch":
                out = ('{"type": "thread.started", "thread_id": "thr-lim"}\n'
                       '{"type": "error", "code": "UsageLimitExceeded", '
                       '"message": "You have hit your usage limit. Try again later.", '
                       '"resets_at": "2026-09-22T09:51:00+00:00"}\n')
                return 1, out, "native turn/start failed: usage limit"
            summary = {"opencode_session_id": "ses-fb-1", "ok": True, "rc": 0,
                       "assistant_text": json.dumps({"action": "completion",
                                                     "output": "FB_DONE"}),
                       "finish": "stop",
                       "actual_model": {"providerID": "opencode-go",
                                        "modelID": "gpt-5.6-luna"},
                       "usage": {}, "native_ids": {}}
            return 0, "RUNNER_RESULT " + json.dumps(summary) + "\n", ""

        res = controller.dispatch(self.sd, rid, run_cmd=run, probe=lambda *a: None)
        self.assertEqual(res["action"], "dispatched")
        self.assertEqual(res.get("route"), "luna-go/max")
        self.assertEqual(calls.count("codex_dispatch"), 1)
        self.assertIn("luna/max", core.exhausted_routes(self.sd))


if __name__ == "__main__":
    unittest.main()
