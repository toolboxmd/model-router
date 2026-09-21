"""Role kits as policy data and per-role session configuration (ISSUE_29).

Deterministic only. No live model CLIs. Proves through the public CLI
with the fake harnesses that the configuration and session settings sent
to the fake server equal the kit, for a kit that names a paid MCP server
(worker, treg) and for a kit that names none (dispatcher).
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

from runner import adapters, core, harnesses, policy, store  # noqa: E402
from runner import kits as kitmod  # noqa: E402
from tests.fakes import FAKE_CLAUDE, FAKE_OPENCODE, VERSION_GUARD, write_fake  # noqa: E402

PY = sys.executable

FAKE_CODEX_OK = r"""
import json, os, sys
from pathlib import Path
st = Path(os.environ["FAKE_STATE"])
st.mkdir(parents=True, exist_ok=True)
argv = sys.argv[1:]
with open(st / "codex.log", "a") as f:
    f.write(json.dumps(argv) + "\n")
tid = "kit-thread-001"
n_f = st / "codex_n"
n = int(n_f.read_text()) + 1 if n_f.exists() else 1
n_f.write_text(str(n))
if argv[:2] != ["exec", "resume"]:
    env = {"action": "implementation", "artifact": "fix.txt",
           "payload": {"instructions": "write fix.txt"}}
else:
    env = {"action": "completion", "output": "KIT_DONE"}
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


def cli(state_dir, *args, env=None, timeout=25):
    cmd = [PY, "-m", "runner", "--state-dir", str(state_dir), *args]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                       cwd=str(ROOT), env=env)
    try:
        out = json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        out = {"raw": p.stdout}
    return p.returncode, out, p.stderr


def wait_for(fn, secs=25.0):
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


def make_fake_homes(base: Path):
    """Isolated homes with a paid MCP (treg) plus one extra entry.

    Returns env overrides for MODEL_ROUTER_* homes.
    """
    oc_home = base / "fake-opencode-home"
    cx_home = base / "fake-codex-home"
    cl_home = base / "fake-claude-home"
    gr_home = base / "fake-grok-home"
    ag_home = base / "fake-agents-home"
    for d in (oc_home / "skills" / "operations",
              oc_home / "skills" / "project-direction",
              cx_home / "skills",
              cl_home / "skills",
              ag_home / "skills"):
        d.mkdir(parents=True, exist_ok=True)
    (oc_home / "skills" / "operations" / "SKILL.md").write_text("# operations\n")
    (oc_home / "skills" / "project-direction" / "SKILL.md").write_text("# project-direction\n")
    plugins = oc_home / "plugins"
    plugins.mkdir(parents=True, exist_ok=True)
    (plugins / "agentsmd-project-direction.js").write_text("// fake plugin\n")
    (oc_home / "opencode.json").write_text(json.dumps({
        "mcp": {
            "treg": {"type": "remote", "url": "https://example.invalid/mcp"},
            "other": {"type": "remote", "url": "https://example.invalid/other"},
        }
    }))
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


class TestKitPolicy(unittest.TestCase):
    def test_kit_rows_per_role(self):
        self.assertEqual(tuple(policy.KIT_ROLES),
                         ("planner", "dispatcher", "reviewer", "worker", "correction", "recovery"))
        for role in policy.KIT_ROLES:
            kit = policy.kit_for_role(role)
            self.assertIsInstance(kit.get("instructions"), str)
            self.assertTrue(kit["instructions"].strip(), role)
            self.assertIn(kit.get("permission_set"), ("full", "read-only"), role)
            self.assertIsInstance(kit.get("skills"), list, role)
            self.assertIsInstance(kit.get("plugins"), list, role)
            self.assertIsInstance(kit.get("mcp"), list, role)
        # Every policy role maps to a kit; unknown roles never substitute.
        self.assertEqual(policy.kit_name_for_route("muse-spark-xhigh-free"), "worker")
        self.assertEqual(policy.kit_name_for_route("luna/max"), "dispatcher")
        self.assertEqual(policy.kit_name_for_route("luna-go/max"), "dispatcher")
        self.assertEqual(policy.kit_name_for_route("luna-go-review"), "reviewer")
        self.assertEqual(policy.kit_name_for_route("kimi-k2.7-code-go"), "correction")
        self.assertEqual(policy.kit_name_for_route("grok-4.6-go"), "recovery")
        self.assertEqual(policy.kit_for_route("muse-spark-xhigh-free")["permission_set"], "full")
        self.assertEqual(policy.kit_for_route("luna/max")["permission_set"], "read-only")
        with self.assertRaises(ValueError):
            policy.kit_for_role("bogus")
        with self.assertRaises(ValueError):
            policy.kit_name_for_route("bogus-route")
        # Hash is stable and distinguishes kits.
        self.assertEqual(policy.kit_hash(policy.kit_for_role("worker")),
                         policy.kit_hash(policy.kit_for_role("worker")))
        self.assertNotEqual(policy.kit_hash(policy.kit_for_role("worker")),
                            policy.kit_hash(policy.kit_for_role("dispatcher")))

    def test_validate_rejects_unknown_skill_plugin_mcp(self):
        orig = {k: dict(v, skills=list(v.get("skills", [])),
                        plugins=list(v.get("plugins", [])),
                        mcp=list(v.get("mcp", []))) for k, v in policy.KITS.items()}
        try:
            policy.KITS["worker"]["skills"] = ["no-such-skill-xyz"]
            self.assertTrue(any("no-such-skill-xyz" in p for p in policy.kit_problems("worker")),
                            policy.kit_problems("worker"))
            self.assertTrue(any("no-such-skill-xyz" in p for p in policy.validate_policy()))
            policy.KITS["worker"]["skills"] = list(orig["worker"]["skills"])
            policy.KITS["worker"]["plugins"] = ["no-such-plugin-xyz"]
            self.assertTrue(any("no-such-plugin-xyz" in p for p in policy.kit_problems("worker")))
            policy.KITS["worker"]["plugins"] = list(orig["worker"]["plugins"])
            policy.KITS["worker"]["mcp"] = ["no-such-mcp-xyz"]
            self.assertTrue(any("no-such-mcp-xyz" in p for p in policy.kit_problems("worker")))
        finally:
            for k, v in orig.items():
                policy.KITS[k].clear()
                policy.KITS[k].update(v)
        self.assertEqual(policy.kit_problems(), [])

    def test_session_permissions_follow_kit(self):
        full = {r["permission"]: r["action"]
                for r in policy.session_permissions("muse-spark-xhigh-free")}
        self.assertEqual(full["external_directory"], "allow")
        self.assertEqual(full["webfetch"], "allow")
        ro = {r["permission"]: r["action"]
              for r in policy.session_permissions("luna/max")}
        self.assertEqual(ro["external_directory"], "deny")
        self.assertEqual(ro["edit"], "deny")
        rec = {r["permission"]: r["action"]
               for r in policy.session_permissions("grok-4.6-go")}
        self.assertEqual(rec["external_directory"], "allow")

    def test_skill_table_lists_kits(self):
        rendered = policy.render_skill_table()
        for role in policy.KIT_ROLES:
            self.assertIn(f"| {role} |", rendered, role)
        self.assertIn("`treg`", rendered)
        self.assertIn("kit_hash", rendered)
        on_disk = (ROOT / "skills" / "model-routing" / "references" / "codex.md").read_text()
        self.assertEqual(on_disk, rendered)

    def test_runner_docs_manual_kit_recipe(self):
        doc = (ROOT / "RUNNER.md").read_text()
        self.assertIn("runner-generated configuration", doc)
        self.assertIn("OPENCODE_CONFIG_DIR", doc)
        self.assertIn("XDG_CONFIG_HOME", doc)
        self.assertIn("CODEX_HOME", doc)
        self.assertIn("CLAUDE_CONFIG_DIR", doc)
        self.assertIn("GROK_HOME", doc)
        self.assertIn("keeps the user's own session", doc)
        self.assertIn("never", doc)
        self.assertNotIn("opencode serve --pure", doc)
        # #53: the kit no longer needs an XDG shadow; the manual recipe
        # drops it while the doc still explains why XDG is left alone.
        self.assertNotIn("xdg-shadow", doc)
        self.assertNotIn("XDG_CONFIG_HOME=/tmp", doc)


class TestKitMaterialization(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.homes = make_fake_homes(self.base)
        self._saved = {k: os.environ.get(k) for k in self.homes}
        os.environ.update(self.homes)
        self.addCleanup(self._restore)

    def _restore(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_opencode_worker_kit_with_paid_mcp(self):
        dest = self.base / "kit-worker"
        kitmod.materialize_opencode_kit("worker", dest, route="muse-spark-xhigh-free")
        kit = policy.kit_for_role("worker")
        self.assertEqual(kit["mcp"], ["treg"])
        stored = json.loads((dest / "kit.json").read_text())
        self.assertEqual(stored["kit"], "worker")
        self.assertEqual(stored["hash"], policy.kit_hash(kit))
        self.assertEqual(stored["route"], "muse-spark-xhigh-free")
        self.assertEqual(stored["permission_set"], "full")
        # MCP subset equals the kit: paid treg kept, unrelated entry dropped.
        cfg = json.loads((dest / "opencode.json").read_text())
        self.assertEqual(set(cfg["mcp"]), {"treg"})
        self.assertEqual(list(cfg["plugin"]), kit["plugins"])
        self.assertTrue((dest / "skills" / "operations").exists())
        self.assertTrue((dest / "skills" / "project-direction").exists())
        self.assertTrue((dest / "plugins" / "agentsmd-project-direction.js").exists())
        self.assertTrue((dest / "AGENTS.md").is_file())
        # #53: no XDG shadow dir; the kit env leaves XDG alone so the
        # owned server inherits the user's shell environment.
        self.assertFalse((dest / "xdg-shadow").exists())
        env = kitmod.opencode_kit_env(dest)
        self.assertEqual(env["OPENCODE_CONFIG_DIR"], str(dest))
        self.assertEqual(env["OPENCODE_CONFIG"], str(dest / "opencode.json"))
        self.assertNotIn("XDG_CONFIG_HOME", env)
        self.assertEqual(set(env), {"OPENCODE_CONFIG_DIR", "OPENCODE_CONFIG"})

    def test_opencode_dispatcher_kit_without_mcp(self):
        dest = self.base / "kit-dispatcher"
        kitmod.materialize_opencode_kit("dispatcher", dest, route="luna-go/max")
        kit = policy.kit_for_role("dispatcher")
        self.assertEqual(kit["mcp"], [])
        cfg = json.loads((dest / "opencode.json").read_text())
        self.assertEqual(cfg["mcp"], {})
        stored = json.loads((dest / "kit.json").read_text())
        self.assertEqual(stored["kit"], "dispatcher")
        self.assertEqual(stored["permission_set"], "read-only")

    def test_codex_claude_grok_specs(self):
        # Codex spawn spec carries its kit equivalent via CODEX_HOME.
        inv = {"request_id": "r1", "invocation_id": "i1",
               "stdout_path": str(self.base / "state" / "outputs" / "r1.i1.stdout"),
               "meta_json": json.dumps({"route": "luna/max", "stage": "dispatch"})}
        (self.base / "state" / "outputs").mkdir(parents=True, exist_ok=True)
        env, pwd = harnesses.harness_named("codex").spawn_spec(
            inv, {"CODEX_HOME": "/user-should-not-leak", "PATH": "x"})
        self.assertIn("CODEX_HOME", env)
        self.assertNotEqual(env["CODEX_HOME"], "/user-should-not-leak")
        self.assertTrue(Path(env["CODEX_HOME"]).is_dir())
        self.assertTrue((Path(env["CODEX_HOME"]) / "kit.json").is_file())
        self.assertIsNone(pwd)
        # Claude planner keeps the user's own session untouched.
        env2, pwd2 = harnesses.harness_named("claude").spawn_spec(
            inv, {"CLAUDE_CONFIG_DIR": "/user/claude", "PATH": "x"})
        self.assertEqual(env2["CLAUDE_CONFIG_DIR"], "/user/claude")
        self.assertIsNone(pwd2)
        manual = harnesses.harness_named("claude").kit_spec_for_manual_run(
            "reviewer", self.base / "claude-manual")
        self.assertIn("CLAUDE_CONFIG_DIR", manual)
        # Grok Build spawn spec carries its kit equivalent via GROK_HOME,
        # keeping the full headless worker options (workspace, model, effort).
        dest = self.base / "grok-kit"
        kitmod.materialize_grok_kit("recovery", dest)
        genv = kitmod.grok_kit_env(dest)
        self.assertEqual(genv["GROK_HOME"], str(dest))
        cmd, genv2 = adapters.build_grok_cmd("do work", "/tmp/ws", model="grok-4.6",
                                             config_dir=str(dest))
        self.assertEqual(cmd[0], "grok")
        self.assertIn("grok-4.6", cmd)
        self.assertEqual(cmd[cmd.index("--cwd") + 1], "/tmp/ws")
        self.assertIn("--verbatim", cmd)
        self.assertIn("--always-approve", cmd)
        self.assertEqual(genv2.get("GROK_HOME"), str(dest))
        cmd_plain, env_plain = adapters.build_grok_cmd("do work", "/tmp/ws", model="grok-4.6")
        self.assertEqual(cmd_plain[0], "grok")
        self.assertEqual(env_plain, {})
        # The live grok harness isolates GROK_HOME on the route's kit.
        ginv = {"request_id": "r9", "invocation_id": "i9",
                "stdout_path": str(self.base / "state" / "outputs" / "r9.i9.stdout"),
                "meta_json": json.dumps({"route": "grok-4.6-build",
                                         "stage": "implementation"})}
        (self.base / "state" / "outputs").mkdir(parents=True, exist_ok=True)
        genv3, _pwd3 = harnesses.harness_named("grok").spawn_spec(
            ginv, {"GROK_HOME": "/user-should-not-leak", "PATH": "x"})
        self.assertIn("GROK_HOME", genv3)
        self.assertNotEqual(genv3["GROK_HOME"], "/user-should-not-leak")
        self.assertTrue((Path(genv3["GROK_HOME"]) / "kit.json").is_file())
        gspec = harnesses.harness_named("opencode").grok_kit_spec_for_manual_run(
            "recovery", self.base / "grok-manual")
        self.assertIn("GROK_HOME", gspec)

    def _assert_skills_plugins_equal_kit(self, dest: Path, kit_name: str):
        kit = policy.kit_for_role(kit_name)
        stored = json.loads((dest / "kit.json").read_text())
        self.assertEqual(stored["kit"], kit_name)
        self.assertEqual(stored["hash"], policy.kit_hash(kit))
        self.assertEqual(stored["skills"], kit["skills"])
        self.assertEqual(stored["plugins"], kit["plugins"])
        self.assertEqual(stored["mcp"], kit["mcp"])
        expected_skills = set()
        for skill in kit["skills"]:
            src = kitmod._find_skill_source(skill)
            self.assertIsNotNone(src, skill)
            expected_skills.add(src.name)
            self.assertTrue((dest / "skills" / src.name).exists(), src.name)
        skills_dir = dest / "skills"
        if expected_skills:
            self.assertTrue(skills_dir.is_dir())
            self.assertEqual({p.name for p in skills_dir.iterdir()}, expected_skills)
        else:
            self.assertFalse(skills_dir.exists(),
                             f"{kit_name} names no skills but {skills_dir} exists")
        expected_plugins = set()
        for plugin in kit["plugins"]:
            src = kitmod._find_plugin_source(plugin)
            self.assertIsNotNone(src, plugin)
            expected_plugins.add(src.name)
            self.assertTrue((dest / "plugins" / src.name).exists(), src.name)
        plugins_dir = dest / "plugins"
        if expected_plugins:
            self.assertTrue(plugins_dir.is_dir())
            self.assertEqual({p.name for p in plugins_dir.iterdir()}, expected_plugins)
        else:
            self.assertFalse(plugins_dir.exists(),
                             f"{kit_name} names no plugins but {plugins_dir} exists")
        mcp_doc = json.loads((dest / "mcp.json").read_text())
        self.assertEqual(set(mcp_doc["mcp"]), set(kit["mcp"]))
        return kit

    def test_codex_kit_dir_equals_kit(self):
        # Dispatcher kit (no MCP): plugins must be present, not dropped.
        dest = self.base / "codex-dispatcher"
        kitmod.materialize_codex_kit("dispatcher", dest)
        kit = self._assert_skills_plugins_equal_kit(dest, "dispatcher")
        cfg = (dest / "config.toml").read_text()
        self.assertIn("dispatcher", cfg)
        self.assertIn("operations", cfg)
        self.assertIn("agentsmd-project-direction", cfg)
        self.assertIn("# kit mcp: none", cfg)
        self.assertNotIn("[mcp_servers.", cfg)
        # Worker kit (paid treg): skills, plugins, and MCP subset kept.
        dest_w = self.base / "codex-worker"
        kitmod.materialize_codex_kit("worker", dest_w)
        self._assert_skills_plugins_equal_kit(dest_w, "worker")
        mcp_doc = json.loads((dest_w / "mcp.json").read_text())
        self.assertEqual(set(mcp_doc["mcp"]), {"treg"})
        cfg_w = (dest_w / "config.toml").read_text()
        self.assertIn("# kit mcp: treg", cfg_w)

    def test_claude_kit_dir_equals_kit(self):
        # Reviewer kit (no MCP): skills and plugins must be present.
        dest = self.base / "claude-reviewer"
        kitmod.materialize_claude_kit("reviewer", dest)
        self._assert_skills_plugins_equal_kit(dest, "reviewer")
        settings = json.loads((dest / "settings.json").read_text())
        kit = policy.kit_for_role("reviewer")
        self.assertEqual(settings["skills"], kit["skills"])
        self.assertEqual(settings["plugins"], kit["plugins"])
        self.assertEqual(settings["mcp"], [])
        self.assertEqual(settings["permissions"], "read-only")
        self.assertTrue((dest / "CLAUDE.md").is_file())
        # Worker kit (paid treg): MCP subset kept as data.
        dest_w = self.base / "claude-worker"
        kitmod.materialize_claude_kit("worker", dest_w)
        self._assert_skills_plugins_equal_kit(dest_w, "worker")
        settings_w = json.loads((dest_w / "settings.json").read_text())
        self.assertEqual(settings_w["mcp"], ["treg"])
        mcp_doc = json.loads((dest_w / "mcp.json").read_text())
        self.assertEqual(set(mcp_doc["mcp"]), {"treg"})

    def test_grok_kit_dir_equals_kit(self):
        # Recovery kit (no MCP): skills and plugins must be present.
        dest = self.base / "grok-recovery"
        kitmod.materialize_grok_kit("recovery", dest)
        self._assert_skills_plugins_equal_kit(dest, "recovery")
        cfg = (dest / "config.toml").read_text()
        self.assertIn("operations", cfg)
        self.assertIn("agentsmd-project-direction", cfg)
        self.assertIn("# kit mcp: none", cfg)
        # Worker kit (paid treg): MCP subset kept as data.
        dest_w = self.base / "grok-worker"
        kitmod.materialize_grok_kit("worker", dest_w)
        self._assert_skills_plugins_equal_kit(dest_w, "worker")
        mcp_doc = json.loads((dest_w / "mcp.json").read_text())
        self.assertEqual(set(mcp_doc["mcp"]), {"treg"})
        cfg_w = (dest_w / "config.toml").read_text()
        self.assertIn("# kit mcp: treg", cfg_w)

    def test_non_opencode_kits_raise_on_missing_plugin(self):
        orig = list(policy.KITS["dispatcher"]["plugins"])
        policy.KITS["dispatcher"]["plugins"] = ["no-such-plugin-xyz"]
        try:
            with self.assertRaises(ValueError):
                kitmod.materialize_codex_kit("dispatcher", self.base / "codex-bad-plugin")
            with self.assertRaises(ValueError):
                kitmod.materialize_claude_kit("dispatcher", self.base / "claude-bad-plugin")
            with self.assertRaises(ValueError):
                kitmod.materialize_grok_kit("dispatcher", self.base / "grok-bad-plugin")
        finally:
            policy.KITS["dispatcher"]["plugins"] = orig

    def test_non_opencode_kits_raise_on_missing_skill(self):
        orig = list(policy.KITS["reviewer"]["skills"])
        policy.KITS["reviewer"]["skills"] = ["no-such-skill-xyz"]
        try:
            with self.assertRaises(ValueError):
                kitmod.materialize_claude_kit("reviewer", self.base / "claude-bad-skill")
            with self.assertRaises(ValueError):
                kitmod.materialize_grok_kit("reviewer", self.base / "grok-bad-skill")
        finally:
            policy.KITS["reviewer"]["skills"] = orig

    def test_agentsmd_link_with_relative_installed_target(self):
        import os as _os
        oc_home = Path(self.homes["MODEL_ROUTER_OPENCODE_HOME"])
        real = oc_home / "canonical" / "AGENTS.md"
        real.parent.mkdir(parents=True, exist_ok=True)
        real.write_text("# canonical\n")
        installed = oc_home / "AGENTS.md"
        try:
            if installed.is_symlink() or installed.exists():
                installed.unlink()
        except OSError:
            pass
        # Installed global link uses a relative target (resolves against
        # the opencode home, not against the kit dir).
        _os.symlink(str(Path("canonical") / "AGENTS.md"), str(installed))
        self.assertEqual(_os.readlink(str(installed)), str(Path("canonical") / "AGENTS.md"))
        dest = self.base / "kit-relative-link"
        kitmod.materialize_opencode_kit("dispatcher", dest, route="luna-go/max")
        kit_link = dest / "AGENTS.md"
        # The kit link must not dangle: it resolves to the canonical file.
        self.assertTrue(kit_link.exists(), f"{kit_link} dangles")
        self.assertEqual(kit_link.read_text(), "# canonical\n")


class TestKitReviewFindings(unittest.TestCase):
    """Regression tests for FINDINGS_1 (blocker, major, minors)."""

    def test_policy_validate_entrypoint_runs_validation(self):
        # `python -m runner.policy validate` must invoke validation:
        # exit 0 on installed kits, non-zero when kits are missing.
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        homes = make_fake_homes(base)
        env = dict(os.environ)
        env.update(homes)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        good = subprocess.run(
            [PY, "-m", "runner.policy", "validate"],
            capture_output=True, text=True, timeout=25, cwd=str(ROOT), env=env)
        self.assertEqual(good.returncode, 0, good.stdout + good.stderr)
        empty = base / "empty-homes"
        empty.mkdir()
        env_bad = dict(env)
        for key in ("MODEL_ROUTER_OPENCODE_HOME", "MODEL_ROUTER_CODEX_HOME",
                    "MODEL_ROUTER_CLAUDE_HOME", "MODEL_ROUTER_GROK_HOME",
                    "MODEL_ROUTER_AGENTS_HOME"):
            env_bad[key] = str(empty / key)
            (empty / key).mkdir(parents=True, exist_ok=True)
        bad = subprocess.run(
            [PY, "-m", "runner.policy", "validate"],
            capture_output=True, text=True, timeout=25, cwd=str(ROOT), env=env_bad)
        self.assertNotEqual(bad.returncode, 0,
                            "validate must fail when kit skills/plugins/MCP are missing")
        self.assertIn("kit worker", bad.stdout)

    def test_codex_kit_raises_on_missing_skill(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        homes = make_fake_homes(base)
        saved = {k: os.environ.get(k) for k in homes}
        os.environ.update(homes)
        try:
            orig = list(policy.KITS["dispatcher"]["skills"])
            policy.KITS["dispatcher"]["skills"] = ["no-such-skill-xyz"]
            try:
                with self.assertRaises(ValueError):
                    kitmod.materialize_codex_kit("dispatcher", base / "codex-bad")
                with self.assertRaises(ValueError):
                    kitmod.materialize_opencode_kit(
                        "dispatcher", base / "opencode-bad", route="luna-go/max")
            finally:
                policy.KITS["dispatcher"]["skills"] = orig
            # Parity restored: the installed kit materializes with equal skills.
            kitmod.materialize_codex_kit("dispatcher", base / "codex-good")
            stored = json.loads((base / "codex-good" / "kit.json").read_text())
            self.assertEqual(stored["skills"], orig)
            for skill in orig:
                src = kitmod._find_skill_source(skill)
                self.assertIsNotNone(src, skill)
                self.assertTrue((base / "codex-good" / "skills" / src.name).exists())
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_skill_dir_alone_is_not_an_mcp_server(self):
        # A skill directory named treg must not satisfy an MCP requirement.
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        oc_home = base / "oc"
        cx_home = base / "cx"
        cl_home = base / "cl"
        gr_home = base / "gr"
        ag_home = base / "ag"
        for d in (oc_home / "skills" / "operations",
                  cx_home / "skills" / "treg",
                  cl_home / "skills",
                  ag_home / "skills"):
            d.mkdir(parents=True, exist_ok=True)
        (oc_home / "skills" / "operations" / "SKILL.md").write_text("# operations\n")
        (oc_home / "opencode.json").write_text(json.dumps({"mcp": {}}))
        (cx_home / "config.toml").write_text("# no mcp_servers here\n")
        saved = {}
        overrides = {
            "MODEL_ROUTER_OPENCODE_HOME": str(oc_home),
            "MODEL_ROUTER_CODEX_HOME": str(cx_home),
            "MODEL_ROUTER_CLAUDE_HOME": str(cl_home),
            "MODEL_ROUTER_GROK_HOME": str(gr_home),
            "MODEL_ROUTER_AGENTS_HOME": str(ag_home),
        }
        for k, v in overrides.items():
            saved[k] = os.environ.get(k)
            os.environ[k] = v
        try:
            self.assertNotIn("treg", policy._codex_mcp_names())
            self.assertFalse(policy.is_mcp_installed("treg"))
            self.assertTrue(any("treg" in p for p in policy.kit_problems("worker")))
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


class TestKitPublicCLI(unittest.TestCase):
    def _start_job(self, codex_body, request_id):
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
        homes = make_fake_homes(base)
        write_fake(bindir, "codex", codex_body, PY)
        write_fake(bindir, "claude", FAKE_CLAUDE, PY)
        write_fake(bindir, "opencode", FAKE_OPENCODE, PY)
        env = dict(os.environ)
        env.update(homes)
        env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""),
                   FAKE_STATE=str(fs), FAKE_OC_DELAY="0.2",
                   FAKE_OC_WRITE="fix.txt", PYTHONDONTWRITEBYTECODE="1")
        rc, out, err = cli(sd, "submit", "--request-id", request_id,
                           "--task", '{"goal":"kit proof"}',
                           "--workspace", str(ws), "--planner-session", "p-kit",
                           "--start", env=env)
        self.assertEqual(rc, 0, err)
        self.addCleanup(lambda: self._cleanup(sd, request_id))
        return tmp, base, sd, ws, fs, env, request_id

    def _cleanup(self, sd, rid):
        try:
            job = core.get_job(sd, rid)
        except Exception:
            return
        if job.get("owner_pid"):
            kill_pid(job["owner_pid"])
        for inv in core._list_invocations(sd, rid):
            for pg in (inv.get("pgid"), inv.get("supervisor_pgid")):
                if pg:
                    try:
                        os.killpg(int(pg), signal.SIGKILL)
                    except Exception:
                        pass

    def _kit_dirs(self, sd):
        root = Path(sd) / "kits"
        if not root.is_dir():
            return {}
        out = {}
        for child in root.iterdir():
            kj = child / "kit.json"
            if kj.is_file():
                try:
                    out[child.name] = json.loads(kj.read_text())
                except ValueError:
                    pass
        return out

    def test_worker_kit_paid_mcp_via_public_cli(self):
        tmp, base, sd, ws, fs, env, rid = self._start_job(FAKE_CODEX_OK, "kit-paid-001")
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 25),
                        core.get_job(sd, rid))
        job = core.get_job(sd, rid)
        self.assertEqual(job["route"], "muse-spark-xhigh-free")
        # Serve runs without --pure on a runner-generated kit dir.
        oc_log = (fs / "opencode.log").read_text().splitlines() if (fs / "opencode.log").exists() else []
        self.assertTrue(oc_log, "fake opencode must have served")
        for line in oc_log:
            argv = json.loads(line)
            self.assertEqual(argv[0], "serve")
            self.assertNotIn("--pure", argv)
        # Kit env points at the generated dir under the state dir.
        env_lines = (fs / "opencode-env.jsonl").read_text().splitlines() if (fs / "opencode-env.jsonl").exists() else []
        self.assertTrue(env_lines)
        kit_env = json.loads(env_lines[-1])
        self.assertTrue(kit_env["OPENCODE_CONFIG_DIR"].startswith(str(Path(sd).resolve()))
                        or kit_env["OPENCODE_CONFIG_DIR"].startswith(sd),
                        kit_env)
        # #53: no XDG override on the owned server; it keeps the parent's
        # value (or absence) while the kit vars still point at the kit.
        self.assertEqual(kit_env.get("XDG_CONFIG_HOME", ""), env.get("XDG_CONFIG_HOME", ""),
                         kit_env)
        self.assertNotIn("xdg-shadow", kit_env.get("XDG_CONFIG_HOME", ""))
        self.assertEqual(kit_env["OPENCODE_CONFIG"],
                         kit_env["OPENCODE_CONFIG_DIR"] + "/opencode.json")
        # Generated configuration equals the worker kit (paid treg kept, other dropped).
        kits = self._kit_dirs(sd)
        worker = [v for v in kits.values() if v.get("kit") == "worker"]
        self.assertTrue(worker, kits)
        w = worker[-1]
        kit = policy.kit_for_role("worker")
        self.assertEqual(w["hash"], policy.kit_hash(kit))
        self.assertEqual(w["mcp"], ["treg"])
        kit_dir = next(p for p in (Path(sd) / "kits").iterdir()
                       if (p / "kit.json").is_file()
                       and json.loads((p / "kit.json").read_text()).get("kit") == "worker")
        cfg = json.loads((kit_dir / "opencode.json").read_text())
        self.assertEqual(set(cfg["mcp"]), {"treg"})
        self.assertTrue((kit_dir / "skills" / "operations").exists())
        self.assertFalse((kit_dir / "xdg-shadow").exists())
        # Session settings equal the kit: full permissions, policy model/variant/agent.
        reqs = [json.loads(l) for l in (fs / "opencode-requests.jsonl").read_text().splitlines()]
        creates = [r for r in reqs if r["method"] == "POST" and r["path"] == "/session"]
        self.assertTrue(creates)
        sent = {(x["permission"], x["action"]) for x in creates[-1]["body"]["permission"]}
        expected = {(r["permission"], r["action"])
                    for r in policy.session_permissions("muse-spark-xhigh-free")}
        self.assertEqual(sent, expected)
        prompts = [r for r in reqs if r["path"].endswith("/prompt_async")]
        self.assertTrue(prompts)
        model, variant, agent = policy.opencode_route_params("muse-spark-xhigh-free")
        prov, _, mid = model.partition("/")
        self.assertEqual(prompts[-1]["body"]["model"], {"providerID": prov, "modelID": mid})
        self.assertEqual(prompts[-1]["body"].get("variant"), variant)
        self.assertEqual(prompts[-1]["body"].get("agent"), agent)
        self.assertTrue((ws / "fix.txt").exists())

    def test_dispatcher_kit_no_mcp_via_public_cli(self):
        # Failing Codex forces the OpenCode dispatch fallback (dispatcher kit, no MCP).
        tmp, base, sd, ws, fs, env, rid = self._start_job(FAKE_CODEX_FAIL, "kit-free-001")
        self.assertTrue(wait_for(lambda: core.get_job(sd, rid)["status"] == "succeeded", 30),
                        core.get_job(sd, rid))
        kits = self._kit_dirs(sd)
        by_kit: dict[str, list] = {}
        for name, v in kits.items():
            by_kit.setdefault(v.get("kit"), []).append(v)
        self.assertIn("dispatcher", by_kit, kits)
        d = by_kit["dispatcher"][-1]
        self.assertEqual(d["mcp"], [])
        # Dispatcher appears both as a Codex kit (no opencode.json) and as an
        # OpenCode kit (with opencode.json): assert the OpenCode one.
        candidates = [p for p in (Path(sd) / "kits").iterdir()
                      if (p / "kit.json").is_file()
                      and json.loads((p / "kit.json").read_text()).get("kit") == "dispatcher"
                      and (p / "opencode.json").is_file()]
        self.assertTrue(candidates, "dispatcher OpenCode kit dir missing")
        kit_dir = candidates[-1]
        cfg = json.loads((kit_dir / "opencode.json").read_text())
        self.assertEqual(cfg["mcp"], {})
        # Dispatcher session is read-only in plan mode (fallback completes
        # without a worker turn, so exactly one OpenCode session exists).
        reqs = [json.loads(l) for l in (fs / "opencode-requests.jsonl").read_text().splitlines()]
        creates = [r for r in reqs if r["method"] == "POST" and r["path"] == "/session"]
        self.assertEqual(len(creates), 1, creates)
        disp_expected = {(r["permission"], r["action"])
                         for r in policy.session_permissions("luna-go/max")}
        sent = {(x["permission"], x["action"]) for x in creates[0]["body"]["permission"]}
        self.assertEqual(sent, disp_expected)
        prompts = [r for r in reqs if r["path"].endswith("/prompt_async")]
        self.assertEqual(len(prompts), 1)
        self.assertEqual(prompts[0]["body"].get("agent"), "plan")
        self.assertEqual(prompts[0]["body"]["model"],
                         {"providerID": "opencode-go", "modelID": "gpt-5.6-luna"})
        # #53: the dispatcher turn also carries no XDG override; it keeps
        # the parent's value while the kit vars still point at the kit.
        env_lines = (fs / "opencode-env.jsonl").read_text().splitlines() if (fs / "opencode-env.jsonl").exists() else []
        self.assertTrue(env_lines)
        disp_env = json.loads(env_lines[-1])
        self.assertEqual(disp_env.get("XDG_CONFIG_HOME", ""), env.get("XDG_CONFIG_HOME", ""),
                         disp_env)
        self.assertNotIn("xdg-shadow", disp_env.get("XDG_CONFIG_HOME", ""))
        self.assertTrue(disp_env["OPENCODE_CONFIG_DIR"].startswith(str(Path(sd).resolve()))
                        or disp_env["OPENCODE_CONFIG_DIR"].startswith(sd),
                        disp_env)
        self.assertEqual(disp_env["OPENCODE_CONFIG"],
                         disp_env["OPENCODE_CONFIG_DIR"] + "/opencode.json")
        self.assertFalse((kit_dir / "xdg-shadow").exists())


if __name__ == "__main__":
    unittest.main()
