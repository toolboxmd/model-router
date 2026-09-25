"""Regression checks for native transport ownership and event continuity."""
import json
import fcntl
import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from runner import codex_native as native
from runner import codex_native_turn as driver


class NativeSafety(unittest.TestCase):
    def test_unrecorded_live_launch_prevents_workspace_release(self):
        with tempfile.TemporaryDirectory() as state_dir:
            fd = os.open(Path(state_dir) / "job.native.lock", os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch("runner.core.get_job", return_value={}):
                    self.assertFalse(native.stop_server(state_dir, "job"))
            finally:
                os.close(fd)
            with patch("runner.core.get_job", return_value={}):
                self.assertTrue(native.stop_server(state_dir, "job"))

    def test_unknown_process_identity_cannot_be_discarded(self):
        job = {"controller_state": json.dumps({"native": {"pid": 1234, "start": "owned"}})}
        with patch("runner.core.get_job", return_value=job), \
             patch.object(native, "_pid_alive", return_value=True), \
             patch.object(native, "_process_start", return_value=None), \
             patch.object(native, "write_runtime") as write:
            self.assertFalse(native.stop_server("unused", "job"))
            write.assert_not_called()

    def test_foreign_conversation_completes_without_interrupt_or_envelope(self):
        class Client:
            snapshots = iter([
                {"thread": {"status": {"type": "active"}, "turns": [
                    {"id": "user-turn", "status": "inProgress"}]}},
                {"thread": {"status": {"type": "idle"}, "turns": []}}])
            events = iter([
                {"method": "item/completed", "params": {"turnId": "user-turn",
                    "item": {"type": "agentMessage", "text": '{"action":"completion"}'}}},
                {"method": "turn/completed", "params": {"turn": {"id": "user-turn"}}}])
            def thread_read(self, thread_id, include_turns=False):
                return next(self.snapshots)
            def next_event(self):
                return next(self.events)
        with patch.object(driver, "_cancel_requested", return_value=False), \
             patch.object(driver, "_emit") as emit:
            self.assertIsNone(driver._await_idle(Client(), "thread", "state", "job", "control-turn"))
            self.assertTrue(emit.called)
            self.assertNotIn("completion", str(emit.call_args_list))

    def test_rpc_keeps_notifications_arriving_before_response(self):
        event = {"method": "turn/completed", "params": {"turn": {"id": "t"}}}
        class Wire:
            messages = iter([event, {"id": 1, "result": {"turn": {"id": "t"}}}])
            def send_text(self, text):
                pass
            def recv_message(self, timeout):
                return {"text": json.dumps(next(self.messages))}
        client = native.NativeClient(Wire())
        client.request("turn/start", {})
        self.assertEqual(client.next_event(0.1), event)

    def test_partial_frame_survives_a_poll_timeout(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        ws = native.NativeWs(left)
        right.sendall(b'\x81\x05he')
        self.assertIsNone(ws.recv_message(0.01))
        right.sendall(b'llo')
        self.assertEqual(ws.recv_message(0.1), {"text": "hello"})

    def test_failed_native_completion_is_not_success(self):
        event = {"method": "turn/completed", "params": {"turnId": "t",
            "turn": {"id": "t", "status": "failed", "error": {
                "message": "usage limit", "codexErrorInfo": "UsageLimitExceeded"}}}}
        result = native.convert_event(event, "t")
        self.assertEqual(result[0]["type"], "error")
        self.assertIn("UsageLimitExceeded", json.dumps(result))

    def test_tool_activity_keeps_stall_clock_live_without_becoming_envelope(self):
        event = {"method": "item/commandExecution/outputDelta", "params": {
            "threadId": "thread", "turnId": "t", "delta": '{"action":"completion"}'}}
        result = native.convert_event(event, "t")
        self.assertTrue(result)
        self.assertNotIn("agent_message", json.dumps(result))
        self.assertNotIn("completion", json.dumps(result))
        self.assertEqual(native.convert_event(event, "other"), [])

    def test_turn_usage_excludes_previous_turns_and_duplicate_updates(self):
        event = {"method": "thread/tokenUsage/updated", "params": {
            "turnId": "t", "tokenUsage": {
                "total": {"inputTokens": 1200, "outputTokens": 90},
                "last": {"inputTokens": 200, "outputTokens": 10}}}}
        self.assertEqual(native.usage_from_token_events([event, event]),
                         {"input_tokens": 200, "output_tokens": 10})

    def test_live_unreachable_server_cannot_be_replaced(self):
        job = {"controller_state": json.dumps({"native": {
            "pid": 1234, "start": "identity", "endpoint": "ws://127.0.0.1:4321"}})}
        with tempfile.TemporaryDirectory() as state_dir, \
             patch("runner.core.get_job", return_value=job), \
             patch.object(native, "_pid_alive", return_value=True), \
             patch.object(native, "_recorded_server_alive", return_value=True), \
             patch.object(native, "server_reachable", return_value=False), \
             patch.object(native.subprocess, "Popen") as spawn:
            with self.assertRaises(native.NativeError):
                native.ensure_server(state_dir, "job", "/tmp")
            self.assertEqual(spawn.call_count, 0)

    def test_persistence_failure_is_not_silently_accepted(self):
        with patch("runner.store.connect", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                native.write_runtime("unused", "job", {"pid": 1234})
