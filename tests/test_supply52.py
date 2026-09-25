"""Issue #52: kit-based supply, unique loader session id.

Deterministic only. No live model CLIs. Proves with a fake loader that
mimics the per-session hook cache (block on the first call per session
id, empty output on a repeat) that:

- every loader call from the runner uses a unique session_id so the cache
  never suppresses a block, still exactly once per invocation;
- supply follows the route's kit: a kit naming
  agentsmd-project-direction records hook with the hash and no block in
  the prompt; a kit without it records runner with the block;
- Codex, Claude, Grok stay hook;
- loader failure records none with the reason and the job continues;
- the worker turn, the dispatcher turn, and the dispatcher resume all
  record a hash, and the prompt carries the block exactly when supply is
  runner.
"""
from __future__ import annotations

import hashlib
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

from runner import adapters, core, direction, policy, store  # noqa: E402
from tests.fakes import FAKE_CLAUDE, FAKE_GROK, FAKE_OPENCODE, write_fake  # noqa: E402

PY = sys.executable

FAKE_CODEX_FAIL = r"""
import sys
print("codex failed to start")
sys.exit(1)
"""

# Fake AgentsMD loader mimicking the per-session hook cache: the block is
# returned on the first call per session_id, and empty output on a repeat
# with the same session_id. Every call is logged with its session_id so
# tests prove uniqueness and exactly-once-per-invocation.
FAKE_LOADER_CACHE = r"""
import json, os, sys
from pathlib import Path
state = Path(os.environ.get("FAKE_LOADER_STATE", "/tmp/fake-loader-state"))
state.mkdir(parents=True, exist_ok=True)
try:
    req = json.loads(sys.stdin.read() or "{}")
except ValueError:
    req = {}
sid = str(req.get("session_id") or "missing")
with open(state / "loader-calls.jsonl", "a") as f:
    f.write(json.dumps({"session_id": sid, "cwd": req.get("cwd"),
                        "event": req.get("hook_event_name")}) + "\n")
safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in sid)[:128] or "missing"
seen = state / ("seen-" + safe)
if seen.exists():
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                                             "additionalContext": ""}}))
    sys.exit(0)
seen.write_text("1")
payload = {
    "status": "ready",
    "repository_root": "/fake/ws",
    "boundary": "test boundary",
    "files": [
        {"name": "VISION.md", "path": "/fake/ws/VISION.md",
         "sha256": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
         "content": "vision content"},
        {"name": "MISSION.md", "path": "/fake/ws/MISSION.md",
         "sha256": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
         "content": "mission content"},
        {"name": "OBJECTIVE.md", "path": "/fake/ws/OBJECTIVE.md",
         "sha256": "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
         "content": "objective content"},
    ],
    "instructions": {"target": "/fake/AGENTS.md",
                     "resolved_target": "/fake/canonical/AGENTS.md",
                     "sha256": "d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
                     "status": "valid-stable-link"},
    "preferences": {"status": "absent"},
}
block = "<<<AGENTSMD_PROJECT_DIRECTION_V1>>>" + json.dumps(payload) + "<<<END_AGENTSMD_PROJECT_DIRECTION_V1>>>"
print(json.dumps({"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                                         "additionalContext": block}}))
"""

FAKE_LOADER_FAIL = r"""
import sys
sys.stderr.write("loader exploded\n")
sys.exit(1)
"""


def cli(state_dir, *args, env=None, timeout=25):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(ROOT), env=env)
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(fn, secs=30.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def kill_job(sd, rid):
    try:
        job = core.get_job(sd, rid)
    except Exception:
        return
    if job.get("owner_pid"):
        try:
            os.kill(int(job["owner_pid"]), signal.SIGKILL)
        except Exception:
            pass
    for inv in core._list_invocations(sd, rid):
        for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
            if pg:
                try:
                    os.killpg(int(pg), signal.SIGKILL)
                except Exception:
                    pass


def make_fake_homes(base: Path):
    oc_home = base / "fake-opencode-home"
    cx_home = base / "fake-codex-home"
    cl_home = base / "fake-claude-home"
    gr_home = base / "fake-grok-home"
    ag_home = base / "fake-agents-home"
    for d in (oc_home / "skills" / "operations",
              oc_home / "skills" / "project-direction",
              ag_home / "skills"):
        d.mkdir(parents=True, exist_ok=True)
    (oc_home / "skills" / "operations" / "SKILL.md").write_text("# operations\n")
    (oc_home / "skills" / "project-direction" / "SKILL.md").write_text("# project-direction\n")
    plugins = oc_home / "plugins"
    plugins.mkdir(parents=True, exist_ok=True)
    (plugins / "agentsmd-project-direction.js").write_text("// fake plugin\n")
    (oc_home / "opencode.json").write_text(json.dumps({
        "mcp": {"treg": {"type": "remote", "url": "https://example.invalid/mcp"}}}))
    (oc_home / "AGENTS.md").write_text("# AGENTS.md\n")
    for d in (cx_home, cl_home, gr_home, ag_home):
        d.mkdir(parents=True, exist_ok=True)
    return {
        "MODEL_ROUTER_OPENCODE_HOME": str(oc_home),
        "MODEL_ROUTER_CODEX_HOME": str(cx_home),
        "MODEL_ROUTER_CLAUDE_HOME": str(cl_home),
        "MODEL_ROUTER_GROK_HOME": str(gr_home),
        "MODEL_ROUTER_AGENTS_HOME": str(ag_home),
    }


def write_loader(bindir: Path, body: str):
    path = bindir / "project-direction"
    path.write_text("#!" + PY + "\n" + body, encoding="utf-8")
    path.chmod(0o700)
    return path


def prompt_texts(fs: Path):
    reqs_path = fs / "opencode-requests.jsonl"
    if not reqs_path.exists():
        return []
    out = []
    for line in reqs_path.read_text().splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("method") == "POST" and rec.get("path", "").endswith("/prompt_async"):
            parts = (rec.get("body") or {}).get("parts") or []
            out.append(" ".join(p.get("text", "") for p in parts if isinstance(p, dict)))
    return out


def loader_calls(loader_state: Path):
    p = loader_state / "loader-calls.jsonl"
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


class TestUniqueSessionId(unittest.TestCase):
    def test_repeated_session_id_suppresses_but_unique_does_not(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        bindir = base / "bin"
        bindir.mkdir()
        ws = base / "ws"
        ws.mkdir()
        loader_state = base / "loader-state"
        loader_state.mkdir()
        write_loader(bindir, FAKE_LOADER_CACHE)
        loader = str(bindir / "project-direction")
        saved = {k: os.environ.get(k) for k in ("FAKE_LOADER_STATE",)}
        os.environ["FAKE_LOADER_STATE"] = str(loader_state)
        self.addCleanup(lambda: _restore(saved))
        # Same session_id twice: the cache suppresses the second.
        first = direction.load_direction(str(ws), host="opencode",
                                         loader=loader, session_id="model-router-fixed-1")
        self.assertTrue(first["ok"], first)
        self.assertEqual(first["session_id"], "model-router-fixed-1")
        second = direction.load_direction(str(ws), host="opencode",
                                          loader=loader, session_id="model-router-fixed-1")
        self.assertFalse(second["ok"])
        self.assertIn("no direction block", second["reason"])
        # Unique session_ids: both succeed despite the same workspace state.
        third = direction.load_direction(str(ws), host="opencode", loader=loader)
        fourth = direction.load_direction(str(ws), host="opencode", loader=loader)
        self.assertTrue(third["ok"], third)
        self.assertTrue(fourth["ok"], fourth)
        self.assertNotEqual(third["session_id"], fourth["session_id"])
        self.assertTrue(third["session_id"].startswith("model-router-"))
        self.assertTrue(fourth["session_id"].startswith("model-router-"))
        calls = loader_calls(loader_state)
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[0]["session_id"], "model-router-fixed-1")
        self.assertEqual(calls[1]["session_id"], "model-router-fixed-1")


def _restore(saved):
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


class TestKitSupplyLabels(unittest.TestCase):
    def test_hook_hosts_stay_hook(self):
        for host in ("codex", "claude", "grok"):
            self.assertEqual(direction.supply_for_harness(host, True, "worker"), "hook")
            self.assertEqual(direction.supply_for_harness(host, True), "hook")
            self.assertEqual(direction.supply_for_harness(host, False, "worker"), "none")

    def test_owned_server_kit_with_plugin_is_hook_without_block(self):
        payload = {"status": "ready", "files": [],
                   "instructions": {"resolved_target": "/fake/AGENTS.md", "sha256": "abc"}}
        block = direction.BLOCK_START + json.dumps(payload) + direction.BLOCK_END
        din = {"ok": True, "block": block, "payload": payload,
               "status": "ready", "reason": None}
        # Every current worker-path kit names the plugin.
        for kit in ("worker", "dispatcher", "reviewer", "correction", "recovery"):
            self.assertTrue(direction.kit_has_direction_plugin(kit), kit)
            self.assertEqual(direction.supply_for_harness("opencode", True, kit), "hook", kit)
            prompt, rec = direction.session_input("do work", None, "opencode", din,
                                                  kit_name=kit)
            self.assertEqual(rec["supply"], "hook", kit)
            self.assertEqual(prompt, "do work", kit)
            self.assertNotIn(direction.BLOCK_START, prompt, kit)
            self.assertIsNotNone(rec["block_hash"], kit)

    def test_owned_server_kit_without_plugin_is_runner_with_block(self):
        payload = {"status": "ready", "files": [],
                   "instructions": {"resolved_target": "/fake/AGENTS.md", "sha256": "abc"}}
        block = direction.BLOCK_START + json.dumps(payload) + direction.BLOCK_END
        din = {"ok": True, "block": block, "payload": payload,
               "status": "ready", "reason": None}
        # The planner kit names no plugins, and an explicit empty plugin
        # list is a kit without the hook.
        self.assertFalse(direction.kit_has_direction_plugin("planner"))
        self.assertEqual(direction.supply_for_harness("opencode", True, "planner"), "runner")
        self.assertEqual(direction.supply_for_harness("opencode", True, kit_plugins=[]), "runner")
        prompt, rec = direction.session_input("do work", None, "opencode", din,
                                              kit_name="planner")
        self.assertEqual(rec["supply"], "runner")
        self.assertIn(direction.BLOCK_START, prompt)
        self.assertIsNotNone(rec["block_hash"])
        prompt2, rec2 = direction.session_input("do work", None, "opencode", din,
                                                kit_plugins=[])
        self.assertEqual(rec2["supply"], "runner")
        self.assertIn(direction.BLOCK_START, prompt2)
        # Loader failure still records none with the reason.
        none_prompt, rec3 = direction.session_input(
            "do work", None, "opencode",
            {"ok": False, "block": None, "status": "gap", "reason": "loader failed: boom"},
            kit_name="worker")
        self.assertEqual(rec3["supply"], "none")
        self.assertIsNone(rec3["block_hash"])
        for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
            self.assertIn(name, none_prompt)


class TestOwnedServerDriveKitSupply(unittest.TestCase):
    def _drive(self, kit_name, block=None, reason="loader missing", nested=False):
        from runner import supervisor as supervisor_mod
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        (ws / "AGENTS.md").write_text("# workspace agents: WORKSPACE_MARKER_52\n")
        if nested:
            subprocess.run(["git", "init", "-q", str(ws)], check=True)
            ws = ws / "subproject"
            ws.mkdir()
            (ws / "AGENTS.md").write_text("# closer instructions: NESTED_MARKER_52\n")
        core.submit(sd, "drive52", {"goal": "x"}, str(ws), "pl")
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='tok' WHERE request_id='drive52'")
            root = store.ensure_state_dir(sd)
            stdout = root / "outputs" / "drive52.inv1.stdout"
            stderr = root / "outputs" / "drive52.inv1.stderr"
            store.secure_write_text(stdout, "opencode server listening on http://127.0.0.1:18777\n")
            store.secure_write_text(stderr, "")
            meta = {"prompt": "do work", "route": "muse-spark-xhigh-free",
                    "stage": "implementation", "direction_status": "ready",
                    "kit": kit_name,
                    "model": "opencode/muse-spark-1.3-contributor-free",
                    "variant": "xhigh", "agent": "build"}
            if block is not None:
                meta["direction_block"] = block
            else:
                meta["direction_status"] = "gap"
                meta["direction_reason"] = reason
            con.execute("INSERT INTO invocations(invocation_id,request_id,kind,cmd_json,workspace,"
                        "owner_token,pid,pgid,stdout_path,stderr_path,started_at,state,meta_json,"
                        "stage,requested_route,direction_supply,direction_hash) VALUES"
                        "('inv1','drive52','opencode_control','[]',?,'tok',NULL,NULL,?,?,?,"
                        "'running',?,'implementation','muse-spark-xhigh-free','none',NULL)",
                        (str(ws), str(stdout), str(stderr), core._utcnow(),
                         json.dumps(meta)))
        finally:
            con.close()
        job = core.get_job(sd, "drive52")

        class _FakeProc:
            def poll(self):
                return None

        captured = {}

        class _FakeClient:
            def __init__(self, *a, **k):
                self.prompted = False
                self.sid = "ses_test123456"

            def health(self):
                return {"healthy": True}

            def create_session(self, title=None, permission=None):
                return {"id": self.sid}

            @staticmethod
            def session_id_from(payload):
                val = payload.get("id") if isinstance(payload, dict) else None
                return val if isinstance(val, str) and val.startswith("ses") else None

            def messages(self, sid):
                if not self.prompted:
                    return []
                return [
                    {"info": {"id": "msg_user_1", "role": "user", "sessionID": sid},
                     "parts": [{"type": "text", "text": "do work"}]},
                    {"info": {"id": "msg_asst_1", "role": "assistant", "sessionID": sid,
                              "time": {"created": 1, "completed": 2},
                              "providerID": "opencode",
                              "modelID": "muse-spark-1.3-contributor-free",
                              "variant": "xhigh", "finish": "stop",
                              "tokens": {"input": 10, "output": 5}, "cost": 0.0},
                     "parts": [{"type": "text", "text": "IMPLEMENTED"}]},
                ]

            def prompt_async(self, sid, text, model=None, variant=None, agent=None):
                self.prompted = True
                captured["prompt"] = text
                return None

            def session_status(self, sid):
                return {"type": "idle"}

            def abort(self, sid):
                return True

            def wait_idle(self, sid, timeout=30.0, interval=0.5):
                return {"idle": True, "status": {"type": "idle"}}

        fake_client = _FakeClient()
        orig_client = adapters.OpenCodeClient
        adapters.OpenCodeClient = lambda *a, **k: fake_client  # type: ignore
        try:
            result, saved, skind = supervisor_mod._drive_opencode_control(
                sd, "drive52", "inv1", _FakeProc(),
                str(stdout), str(stderr), "pw", dict(meta), job,
                time.monotonic() + 20.0, str(ws))
        finally:
            adapters.OpenCodeClient = orig_client
        meas = {m["kind"]: m for m in core.invocation_measurements(sd, "drive52")}
        return result, captured, meas["opencode_control"]

    def test_hook_kit_prompt_has_no_block_but_hash(self):
        block = direction.BLOCK_START + json.dumps({"status": "ready"}) + direction.BLOCK_END
        result, captured, row = self._drive("worker", block=block)
        self.assertTrue(result.get("ok"), result)
        prompt = captured.get("prompt", "")
        self.assertNotIn(direction.BLOCK_START, prompt)
        self.assertNotIn(direction.BLOCK_END, prompt)
        self.assertNotIn("END PROJECT DIRECTION", prompt)
        self.assertEqual(prompt.count("WORKSPACE_MARKER_52"), 1)
        self.assertTrue(prompt.rstrip().endswith("do work"))
        self.assertEqual(row["supply"], "hook")
        self.assertEqual(row["direction_hash"], direction.block_hash(block))
        self.assertEqual(row["direction_status"], "ready")

    def test_loader_gap_still_supplies_workspace_instructions(self):
        result, captured, row = self._drive("worker", reason="loader missing")
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(captured["prompt"].count("WORKSPACE_MARKER_52"), 1)
        self.assertEqual(row["supply"], "none")
        self.assertEqual(row["direction_status"], "gap")

    def test_nested_workspace_receives_parent_and_closer_instructions(self):
        block = direction.BLOCK_START + json.dumps({"status": "ready"}) + direction.BLOCK_END
        for kit in ("worker", "planner"):
            with self.subTest(kit=kit):
                result, captured, row = self._drive(kit, block=block, nested=True)
                self.assertTrue(result.get("ok"), result)
                prompt = captured["prompt"]
                self.assertEqual(prompt.count("WORKSPACE_MARKER_52"), 1)
                self.assertEqual(prompt.count("NESTED_MARKER_52"), 1)
                self.assertLess(prompt.index("WORKSPACE_MARKER_52"), prompt.index("NESTED_MARKER_52"))

    def test_runner_kit_prompt_carries_block(self):
        payload = {
            "status": "ready",
            "files": [
                {"name": "VISION.md", "path": "/fake/ws/VISION.md",
                 "sha256": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                 "content": "vision content"},
                {"name": "MISSION.md", "path": "/fake/ws/MISSION.md",
                 "sha256": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
                 "content": "mission content"},
                {"name": "OBJECTIVE.md", "path": "/fake/ws/OBJECTIVE.md",
                 "sha256": "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
                 "content": "objective content"},
            ],
            "instructions": {"target": "/fake/AGENTS.md",
                             "resolved_target": "/fake/canonical/AGENTS.md",
                             "sha256": "d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
                             "status": "valid-stable-link"},
        }
        block = direction.BLOCK_START + json.dumps(payload) + direction.BLOCK_END
        result, captured, row = self._drive("planner", block=block)
        self.assertTrue(result.get("ok"), result)
        prompt = captured.get("prompt", "")
        self.assertIn(direction.BLOCK_START, prompt)
        self.assertIn(direction.BLOCK_END, prompt)
        self.assertIn("PROJECT DIRECTION (status=ready", prompt)
        for sha in ("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                    "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
                    "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"):
            self.assertIn(sha, prompt)
        self.assertIn("/fake/canonical/AGENTS.md", prompt)
        self.assertIn("WORKSPACE_MARKER_52", prompt)
        self.assertIn("END PROJECT DIRECTION", prompt)
        self.assertTrue(prompt.rstrip().endswith("do work"))
        self.assertEqual(row["supply"], "runner")
        self.assertEqual(row["direction_hash"], direction.block_hash(block))
        self.assertEqual(row["direction_status"], "ready")


class TestWorkerDispatcherResumeWithCacheLoader(unittest.TestCase):
    def test_all_owned_turns_record_hash_and_hook_without_duplicate(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        (ws / "VISION.md").write_text("# vision\n")
        (ws / "MISSION.md").write_text("# mission\n")
        (ws / "OBJECTIVE.md").write_text("# objective\n")
        (ws / "AGENTS.md").write_text("# workspace agents\n")
        bindir = base / "bin"
        bindir.mkdir()
        fs = base / "fakestate"
        fs.mkdir()
        loader_state = base / "loader-state"
        loader_state.mkdir()
        homes = make_fake_homes(base)
        write_fake(bindir, "codex", FAKE_CODEX_FAIL, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        write_fake(bindir, "grok", FAKE_GROK, PY)
        write_loader(bindir, FAKE_LOADER_CACHE)
        env = dict(os.environ)
        env.update(homes)
        env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""),
                   FAKE_STATE=str(fs), FAKE_LOADER_STATE=str(loader_state),
                   FAKE_OC_DELAY="0.2", FAKE_OC_WRITE="fix.txt",
                   FAKE_OC_PLAN="implement_then_complete",
                   PYTHONDONTWRITEBYTECODE="1")
        env.pop("MODEL_ROUTER_PROJECT_DIRECTION_BIN", None)
        rid = "supply52-cli-1"
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"supply52 drill","proof":"true"}',
                           "--workspace", str(ws), "--planner-session", "p-52",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: kill_job(sd, rid))
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 40),
                        core.get_job(sd, rid))
        # Every owned-server turn (dispatch, worker, dispatcher resume)
        # records a hash with hook supply, because both kits carry the
        # plugin. The prompt body carries the block exactly when supply is
        # runner, so here no prompt carries it.
        meas = core.invocation_measurements(sd, rid)
        owned = [m for m in meas if m["kind"] == "opencode_control"]
        self.assertGreaterEqual(len(owned), 3, meas)
        dispatchers = [m for m in owned if m["kit"] == "dispatcher"]
        workers = [m for m in owned if m["kit"] == "worker"]
        # Initial dispatch fallback plus dispatcher resume: two dispatcher
        # rows prove the resume turn ran; one worker row proves the worker.
        self.assertGreaterEqual(len(dispatchers), 2, meas)
        self.assertGreaterEqual(len(workers), 1, meas)
        # Distinct invocations prove the resume is a second turn, not a
        # relabel of the dispatch.
        self.assertEqual(len({m["invocation"] for m in dispatchers}),
                         len(dispatchers), meas)
        # The worker turn runs between the dispatch and its resume.
        order = {m["invocation"]: i for i, m in enumerate(owned)}
        first_disp = min(order[m["invocation"]] for m in dispatchers)
        last_disp = max(order[m["invocation"]] for m in dispatchers)
        first_work = min(order[m["invocation"]] for m in workers)
        self.assertLess(first_disp, first_work, meas)
        self.assertLess(first_work, last_disp, meas)
        for m in dispatchers + workers:
            self.assertEqual(m["supply"], "hook", m)
            self.assertIsNotNone(m["direction_hash"], m)
            self.assertEqual(m["direction_status"], "ready", m)
        texts = prompt_texts(fs)
        self.assertTrue(texts, "fake server must have received prompts")
        for text in texts:
            self.assertNotIn(direction.BLOCK_START, text)
        # The loader ran exactly once per durable harness invocation with
        # unique session ids (proof rows are verification evidence, not
        # harness turns, so they carry no loader call).
        calls = loader_calls(loader_state)
        invs = core._list_invocations(sd, rid)
        loader_invs = [i for i in invs
                       if i.get("state") != "abandoned"
                       and i.get("kind") != "proof"]
        self.assertEqual(len(calls), len(loader_invs), (calls, len(loader_invs)))
        sids = [c["session_id"] for c in calls]
        self.assertEqual(len(set(sids)), len(sids))
        for sid in sids:
            self.assertTrue(sid.startswith("model-router-"), sid)
        # The durable rows also carry the unique session id in meta.
        meta_sids = []
        for inv in loader_invs:
            try:
                meta_sids.append(json.loads(inv.get("meta_json") or "{}").get("direction_session_id"))
            except ValueError:
                meta_sids.append(None)
        self.assertTrue(all(isinstance(s, str) and s.startswith("model-router-") for s in meta_sids),
                        meta_sids)
        self.assertEqual(len(set(meta_sids)), len(meta_sids))


if __name__ == "__main__":
    unittest.main()
