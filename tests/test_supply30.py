"""Direction supply and measurement through the harness seam (ISSUE_30).

Deterministic only. No live model CLIs. Proves through the public CLI
with the fake harnesses and a fake loader on PATH that every role
session's input carries the current Project Direction of its workspace,
and the ledger records kit, supply, block hash, skills, and tools.
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

FAKE_CODEX_OK = r"""
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
st.mkdir(parents=True, exist_ok=True)
argv = sys.argv[1:]
with open(st / "codex.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
tid = "supply-thread-001"
if argv[:2] != ["exec", "resume"]:
    env = {"action": "implementation", "artifact": "fix.txt",
           "payload": {"instructions": "write fix.txt"}}
else:
    env = {"action": "completion", "output": "SUPPLY_DONE"}
lp = argv[argv.index("--output-last-message") + 1]
Path(lp).parent.mkdir(parents=True, exist_ok=True)
Path(lp).write_text(json.dumps(env))
for obj in ({"type": "thread.started", "thread_id": tid},
            {"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(env)}},
            {"type": "turn.completed", "usage": {}}):
    print(json.dumps(obj), flush=True)
"""

FAKE_CODEX_FAIL = r"""
import sys
print("codex failed to start")
sys.exit(1)
"""

# Fake AgentsMD loader on PATH: success emits the hook JSON whose
# additionalContext is the verbatim direction block (status, three files
# with hashes, core instruction link). Failure exits non-zero.
FAKE_LOADER_OK = r"""
import json, sys
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
# Real loader shape: hook JSON carrying the block verbatim.
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


def _restore_env(saved):
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


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


class TestDirectionUnit(unittest.TestCase):
    def test_supply_labels(self):
        self.assertEqual(direction.supply_for_harness("opencode", True), "runner")
        self.assertEqual(direction.supply_for_harness("codex", True), "hook")
        self.assertEqual(direction.supply_for_harness("claude", True), "hook")
        self.assertEqual(direction.supply_for_harness("grok", True), "hook")
        self.assertEqual(direction.supply_for_harness("opencode", False), "none")
        self.assertEqual(direction.supply_for_harness("codex", False), "none")

    def test_fallback_names_three_files(self):
        text = direction.fallback_text("loader missing")
        for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
            self.assertIn(name, text)

    def test_session_input_runner_hook_none(self):
        block_payload = {"status": "ready",
                         "files": [{"name": "VISION.md"}, {"name": "MISSION.md"}, {"name": "OBJECTIVE.md"}],
                         "instructions": {"resolved_target": "/fake/AGENTS.md", "sha256": "abc"}}
        block = (direction.BLOCK_START + json.dumps(block_payload)
                 + direction.BLOCK_END)
        din = {"ok": True, "block": block, "payload": block_payload,
               "status": "ready", "reason": None}
        runner_prompt, rec = direction.session_input("do work", None, "opencode", din)
        self.assertEqual(rec["supply"], "runner")
        self.assertIn(direction.BLOCK_START, runner_prompt)
        self.assertIn("VISION.md", runner_prompt)
        self.assertIn("/fake/AGENTS.md", runner_prompt)
        self.assertTrue(runner_prompt.rstrip().endswith("do work"))
        hook_prompt, rec2 = direction.session_input("do work", None, "codex", din)
        self.assertEqual(rec2["supply"], "hook")
        self.assertEqual(hook_prompt, "do work")
        self.assertNotIn(direction.BLOCK_START, hook_prompt)
        none_prompt, rec3 = direction.session_input(
            "do work", None, "opencode",
            {"ok": False, "block": None, "status": "gap", "reason": "loader missing"})
        self.assertEqual(rec3["supply"], "none")
        for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
            self.assertIn(name, none_prompt)

    def test_kit_for_invocation(self):
        name, h, skills = direction.kit_for_invocation("muse-spark-xhigh-free", "implementation")
        self.assertEqual(name, "worker")
        self.assertEqual(h, policy.kit_hash(policy.kit_for_role("worker")))
        self.assertEqual(skills, policy.kit_for_role("worker")["skills"])
        name2, _, _ = direction.kit_for_invocation("luna/max", "dispatch")
        self.assertEqual(name2, "dispatcher")
        name3, _, _ = direction.kit_for_invocation("luna-go-review", "review")
        self.assertEqual(name3, "reviewer")
        name4, _, _ = direction.kit_for_invocation("kimi-k2.7-code-go", "correction")
        self.assertEqual(name4, "correction")

    def test_find_loader_prefers_path_fake(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        bindir = Path(tmp.name) / "bin"
        bindir.mkdir()
        write_loader(bindir, FAKE_LOADER_OK)
        env_path = str(bindir) + os.pathsep + "/usr/bin:/bin"
        saved_path = os.environ.get("PATH")
        saved_bin = os.environ.get("MODEL_ROUTER_PROJECT_DIRECTION_BIN")
        os.environ["PATH"] = env_path
        os.environ.pop("MODEL_ROUTER_PROJECT_DIRECTION_BIN", None)
        try:
            found = direction.find_loader()
            self.assertIsNotNone(found)
            self.assertTrue(str(found).startswith(str(bindir)))
        finally:
            if saved_path is None:
                os.environ.pop("PATH", None)
            else:
                os.environ["PATH"] = saved_path
            if saved_bin is not None:
                os.environ["MODEL_ROUTER_PROJECT_DIRECTION_BIN"] = saved_bin

    def test_load_direction_success_and_failure(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        bindir = base / "bin"
        bindir.mkdir()
        ws = base / "ws"
        ws.mkdir()
        write_loader(bindir, FAKE_LOADER_OK)
        ok = direction.load_direction(str(ws), host="opencode",
                                      loader=str(bindir / "project-direction"))
        self.assertTrue(ok["ok"])
        self.assertIn(direction.BLOCK_START, ok["block"] or "")
        self.assertEqual(ok["status"], "ready")
        self.assertEqual(len(ok["files"]), 3)
        self.assertIsNotNone(direction.block_hash(ok["block"]))
        write_loader(bindir, FAKE_LOADER_FAIL)
        bad = direction.load_direction(str(ws), host="opencode",
                                       loader=str(bindir / "project-direction"))
        self.assertFalse(bad["ok"])
        self.assertIsNone(bad["block"])
        self.assertIn("loader failed", bad["reason"])


class TestSupplyPublicCLI(unittest.TestCase):
    def _start(self, codex_body, loader_body_or_none, rid, with_agents=True,
               tools=False):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        (ws / "VISION.md").write_text("# vision\n")
        (ws / "MISSION.md").write_text("# mission\n")
        (ws / "OBJECTIVE.md").write_text("# objective\n")
        if with_agents:
            (ws / "AGENTS.md").write_text("# workspace agents: WORKSPACE_MARKER_30\n")
        bindir = base / "bin"
        bindir.mkdir()
        fs = base / "fakestate"
        fs.mkdir()
        homes = make_fake_homes(base)
        write_fake(bindir, "codex", codex_body, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        write_fake(bindir, "grok", FAKE_GROK, PY)
        if loader_body_or_none is not None:
            write_loader(bindir, loader_body_or_none)
            path_val = str(bindir) + os.pathsep + os.environ.get("PATH", "")
        else:
            # Missing loader: isolate PATH so no real binary leaks in.
            path_val = str(bindir) + os.pathsep + "/usr/bin:/bin"
        env = dict(os.environ)
        env.update(homes)
        env.update(PATH=path_val, FAKE_STATE=str(fs), FAKE_OC_DELAY="0.2",
                    FAKE_OC_WRITE="fix.txt", PYTHONDONTWRITEBYTECODE="1")
        if tools:
            # Fake server appends one tool part to text assistant messages.
            env["FAKE_OC_TOOLS"] = "1"
        env.pop("MODEL_ROUTER_PROJECT_DIRECTION_BIN", None)
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"supply proof"}',
                           "--workspace", str(ws), "--planner-session", "p-supply",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: kill_job(sd, rid))
        return tmp, base, sd, ws, fs, env, rid

    def test_worker_dispatch_review_inputs_carry_block(self):
        # Dispatch fallback (Codex fails) and worker (Codex ok) are both
        # owned-server sessions and must carry the block. The reviewer kit
        # on the owned server is proven by
        # test_review_input_carries_block_through_owned_server_seam: review
        # stages execute on hosts, so no controller flow spawns a review
        # turn. Two jobs keep workspaces disjoint (one active job per
        # workspace).
        tmp1, base1, sd1, ws1, fs1, env1, rid1 = self._start(
            FAKE_CODEX_FAIL, FAKE_LOADER_OK, "supply-disp-001")
        self.assertTrue(wait_for(lambda: core.get_job(sd1, rid1)["status"] == "succeeded", 30),
                        core.get_job(sd1, rid1))
        texts = prompt_texts(fs1)
        self.assertEqual(len(texts), 1, texts)
        for text in texts:
            self.assertIn(direction.BLOCK_START, text)
            self.assertIn(direction.BLOCK_END, text)
            self.assertIn("ready", text)
            for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
                self.assertIn(name, text)
            for h in ("a1b2c3d4e5f6a1b2", "b2c3d4e5f6a1b2c3", "c3d4e5f6a1b2c3d4"):
                self.assertIn(h, text)
            self.assertIn("/fake/canonical/AGENTS.md", text)
            self.assertIn("WORKSPACE_MARKER_30", text)
        # Ledger: dispatch fallback carries dispatcher kit with runner supply.
        meas = core.invocation_measurements(sd1, rid1)
        kinds = [m for m in meas if m["kind"] == "opencode_control"]
        self.assertEqual(len(kinds), 1, meas)
        disp = kinds[0]
        self.assertEqual(disp["kit"], "dispatcher")
        self.assertEqual(disp["supply"], "runner")
        self.assertEqual(disp["kit_hash"], policy.kit_hash(policy.kit_for_role("dispatcher")))
        self.assertEqual(disp["direction_status"], "ready")
        self.assertIsNotNone(disp["direction_hash"])
        self.assertEqual(disp["skills_loaded"], policy.kit_for_role("dispatcher")["skills"])
        # Text-only turn: no tool parts observed.
        self.assertEqual(disp["tools_called"], [])
        # Worker job (Codex ok): worker session input carries the block.
        # FAKE_OC_TOOLS makes the fake emit one tool part, proving
        # distinct tool-part recording is non-empty when tools run.
        tmp2, base2, sd2, ws2, fs2, env2, rid2 = self._start(
            FAKE_CODEX_OK, FAKE_LOADER_OK, "supply-work-001", tools=True)
        self.assertTrue(wait_for(lambda: core.get_job(sd2, rid2)["status"] == "succeeded", 30),
                        core.get_job(sd2, rid2))
        texts2 = prompt_texts(fs2)
        self.assertEqual(len(texts2), 1, texts2)
        self.assertIn(direction.BLOCK_START, texts2[0])
        self.assertIn("WORKSPACE_MARKER_30", texts2[0])
        for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
            self.assertIn(name, texts2[0])
        meas2 = core.invocation_measurements(sd2, rid2)
        workers = [m for m in meas2 if m["kit"] == "worker"]
        self.assertTrue(workers, meas2)
        worker = workers[0]
        self.assertEqual(worker["supply"], "runner")
        self.assertEqual(worker["kit_hash"], policy.kit_hash(policy.kit_for_role("worker")))
        invs = core._list_invocations(sd2, rid2)
        winv = next(i for i in invs if (json.loads(i.get("meta_json") or "{}").get("kit")) == "worker")
        wmeta = json.loads(winv.get("meta_json") or "{}")
        self.assertIn(direction.BLOCK_START, wmeta.get("direction_block") or "")
        self.assertEqual(winv.get("direction_hash"),
                         hashlib.sha256(wmeta["direction_block"].encode("utf-8")).hexdigest())
        self.assertEqual(worker["direction_hash"], winv.get("direction_hash"))
        self.assertEqual(worker["direction_status"], "ready")
        self.assertEqual(worker["skills_loaded"], policy.kit_for_role("worker")["skills"])
        self.assertEqual(worker["tools_called"], ["read"])
        # status and result expose the Agent Observer mapping.
        for sd, rid in ((sd1, rid1), (sd2, rid2)):
            status = core.status_view(sd, rid)
            self.assertIn("measurements", status["job"])
            for m in status["job"]["measurements"]:
                for key in ("kit", "kit_hash", "supply", "direction_hash",
                            "direction_status", "skills_loaded", "tools_called"):
                    self.assertIn(key, m, (key, m))
            result = core.result_view(sd, rid)
            for m in result["measurements"]:
                for key in ("kit", "kit_hash", "supply", "direction_hash",
                            "direction_status", "skills_loaded", "tools_called"):
                    self.assertIn(key, m, (key, m))

    def test_review_input_carries_block_through_owned_server_seam(self):
        # Review stages execute on hosts (review_ticket/review_final
        # executor host; the controller blocks on review actions instead of
        # spawning them), so no controller flow can emit a review turn. The
        # owned-server review shape (luna-go-review, plan agent) is driven
        # here through the same durable-run plus supervisor plus fake-server
        # stack the CLI jobs use, on a job submitted via the public CLI.
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        sd = str(base / "state")
        ws = base / "ws"
        ws.mkdir()
        (ws / "VISION.md").write_text("# vision\n")
        (ws / "MISSION.md").write_text("# mission\n")
        (ws / "OBJECTIVE.md").write_text("# objective\n")
        (ws / "AGENTS.md").write_text("# workspace agents: WORKSPACE_MARKER_30\n")
        bindir = base / "bin"
        bindir.mkdir()
        fs = base / "fakestate"
        fs.mkdir()
        homes = make_fake_homes(base)
        write_fake(bindir, "codex", FAKE_CODEX_OK, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        write_fake(bindir, "grok", FAKE_GROK, PY)
        write_loader(bindir, FAKE_LOADER_OK)
        path_val = str(bindir) + os.pathsep + os.environ.get("PATH", "")
        env = dict(os.environ)
        env.update(homes)
        env.update(PATH=path_val, FAKE_STATE=str(fs), FAKE_OC_DELAY="0.2",
                   PYTHONDONTWRITEBYTECODE="1")
        env.pop("MODEL_ROUTER_PROJECT_DIRECTION_BIN", None)
        rid = "supply-review-001"
        rc, out, err = cli(sd, "submit", "--request-id", rid,
                           "--task", '{"goal":"supply proof"}',
                           "--workspace", str(ws), "--planner-session", "p-supply",
                           env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: kill_job(sd, rid))
        # No controller owns this pending job; claim its lease the way the
        # controller drills do so the durable-run stack accepts the turn.
        con = store.connect(sd)
        try:
            con.execute("UPDATE jobs SET owner_token='supply-review-tok' WHERE request_id=?",
                        (rid,))
        finally:
            con.close()
        # The supervisor and the fake server inherit this process's
        # environment, so point it at the fakes for the turn only.
        saved = {k: os.environ.get(k) for k in (
            "PATH", "FAKE_STATE", "FAKE_OC_DELAY", "FAKE_OC_WRITE",
            "FAKE_OC_TOOLS", "MODEL_ROUTER_OPENCODE_HOME",
            "MODEL_ROUTER_CODEX_HOME", "MODEL_ROUTER_CLAUDE_HOME",
            "MODEL_ROUTER_GROK_HOME", "MODEL_ROUTER_AGENTS_HOME",
            "MODEL_ROUTER_PROJECT_DIRECTION_BIN", "PYTHONDONTWRITEBYTECODE")}
        self.addCleanup(lambda: _restore_env(saved))
        os.environ.update(homes)
        os.environ.update(PATH=path_val, FAKE_STATE=str(fs), FAKE_OC_DELAY="0.2",
                          PYTHONDONTWRITEBYTECODE="1")
        for k in ("FAKE_OC_WRITE", "FAKE_OC_TOOLS", "MODEL_ROUTER_PROJECT_DIRECTION_BIN"):
            os.environ.pop(k, None)
        # Route plus stage resolve the reviewer kit, as for every invocation.
        rname, _, rskills = direction.kit_for_invocation("luna-go-review", "review")
        self.assertEqual(rname, "reviewer")
        self.assertEqual(rskills, policy.kit_for_role("reviewer")["skills"])
        run = core.make_durable_run_cmd(sd, rid, "supply-review-tok")
        model, variant, agent = policy.opencode_route_params("luna-go-review")
        rc2, _, err2 = run(adapters.build_opencode_serve_cmd(), str(ws), None,
                           kind="opencode_control",
                           meta={"prompt": "review the change",
                                 "allowance": policy.route_allowance("luna-go-review"),
                                 "model": model, "variant": variant, "agent": agent,
                                 "seq": 0, "stage": "review", "route": "luna-go-review",
                                 "reason": "review fallback drill"})
        self.assertEqual(rc2, 0, (err2 or "")[-2000:])
        texts = [t for t in prompt_texts(fs) if "review the change" in t]
        self.assertEqual(len(texts), 1, texts)
        text = texts[0]
        self.assertIn(direction.BLOCK_START, text)
        self.assertIn(direction.BLOCK_END, text)
        self.assertIn("ready", text)
        for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
            self.assertIn(name, text)
        for h in ("a1b2c3d4e5f6a1b2", "b2c3d4e5f6a1b2c3", "c3d4e5f6a1b2c3d4"):
            self.assertIn(h, text)
        self.assertIn("/fake/canonical/AGENTS.md", text)
        self.assertIn("WORKSPACE_MARKER_30", text)
        # Ledger: reviewer kit with runner supply, block hash, and skills.
        meas = core.invocation_measurements(sd, rid)
        kinds = [m for m in meas if m["kind"] == "opencode_control"]
        self.assertEqual(len(kinds), 1, meas)
        rev = kinds[0]
        self.assertEqual(rev["kit"], "reviewer")
        self.assertEqual(rev["supply"], "runner")
        self.assertEqual(rev["kit_hash"], policy.kit_hash(policy.kit_for_role("reviewer")))
        self.assertEqual(rev["direction_status"], "ready")
        self.assertIsNotNone(rev["direction_hash"])
        self.assertEqual(rev["skills_loaded"], policy.kit_for_role("reviewer")["skills"])
        self.assertEqual(rev["tools_called"], [])
        invs = core._list_invocations(sd, rid)
        rinv = next(i for i in invs if i.get("kind") == "opencode_control")
        rmeta = json.loads(rinv.get("meta_json") or "{}")
        self.assertIn(direction.BLOCK_START, rmeta.get("direction_block") or "")
        self.assertEqual(rinv.get("direction_hash"),
                         hashlib.sha256(rmeta["direction_block"].encode("utf-8")).hexdigest())
        self.assertEqual(rev["direction_hash"], rinv.get("direction_hash"))
        # The public CLI exposes the same Agent Observer mapping.
        for cmd in ("status", "result"):
            rc3, view, err3 = cli(sd, cmd, "--request-id", rid)
            self.assertEqual(rc3, 0, err3)
            rows = ((view.get("job") or {}).get("measurements")
                    if cmd == "status" else view.get("measurements"))
            rows = [m for m in (rows or []) if m.get("kit") == "reviewer"]
            self.assertTrue(rows, (cmd, view))
            for key in ("kit", "kit_hash", "supply", "direction_hash",
                        "direction_status", "skills_loaded", "tools_called"):
                self.assertIn(key, rows[0], (cmd, key, rows[0]))

    def test_codex_native_records_hook_without_duplication(self):
        tmp, base, sd, ws, fs, env, rid = self._start(
            FAKE_CODEX_OK, FAKE_LOADER_OK, "supply-hook-001")
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 30),
                        core.get_job(sd, rid))
        # Codex prompt (argv) must not duplicate the block; the host hook
        # supplies it.
        codex_log = fs / "codex.log"
        self.assertTrue(codex_log.exists())
        argv_text = codex_log.read_text()
        self.assertNotIn(direction.BLOCK_START, argv_text)
        meas = {m["kit"]: m for m in core.invocation_measurements(sd, rid)}
        # Dispatch ran on Codex (hook), worker on the owned server (runner).
        dispatches = [m for m in core.invocation_measurements(sd, rid)
                      if m["kind"] in ("codex_dispatch",)]
        self.assertTrue(dispatches, meas)
        self.assertEqual(dispatches[0]["supply"], "hook")
        self.assertEqual(dispatches[0]["kit"], "dispatcher")
        self.assertIsNotNone(dispatches[0]["direction_hash"])
        workers = [m for m in core.invocation_measurements(sd, rid)
                   if m["kit"] == "worker"]
        self.assertTrue(workers)
        self.assertEqual(workers[0]["supply"], "runner")

    def test_loader_failure_records_none_and_continues(self):
        tmp, base, sd, ws, fs, env, rid = self._start(
            FAKE_CODEX_FAIL, FAKE_LOADER_FAIL, "supply-fail-001")
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 30),
                        core.get_job(sd, rid))
        texts = prompt_texts(fs)
        self.assertTrue(texts)
        for text in texts:
            self.assertNotIn(direction.BLOCK_START, text)
            for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
                self.assertIn(name, text)
        meas = core.invocation_measurements(sd, rid)
        for m in meas:
            if m["kind"] == "opencode_control":
                self.assertEqual(m["supply"], "none")
                self.assertIsNone(m["direction_hash"])
                self.assertIn("loader failed", m["supply_reason"] or "")
                self.assertEqual(m["direction_status"], "gap")
        status = core.status_view(sd, rid)
        gaps = [m for m in status["job"]["measurements"] if m["supply"] == "none"]
        self.assertTrue(gaps)

    def test_loader_missing_records_none_and_continues(self):
        tmp, base, sd, ws, fs, env, rid = self._start(
            FAKE_CODEX_FAIL, None, "supply-missing-001")
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 30),
                        core.get_job(sd, rid))
        texts = prompt_texts(fs)
        self.assertTrue(texts)
        for text in texts:
            self.assertNotIn(direction.BLOCK_START, text)
            for name in ("VISION.md", "MISSION.md", "OBJECTIVE.md"):
                self.assertIn(name, text)
        meas = core.invocation_measurements(sd, rid)
        nones = [m for m in meas if m["supply"] == "none"]
        self.assertTrue(nones)
        self.assertIn("loader missing", (nones[0]["supply_reason"] or ""))


if __name__ == "__main__":
    unittest.main()
