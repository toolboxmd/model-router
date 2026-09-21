"""Issue #53: owned-server environment keeps the user's shell environment.

Deterministic only. No live model CLIs. Proves that the owned OpenCode
server inherits the runner's ``XDG_CONFIG_HOME`` (the user's shell value)
while ``OPENCODE_CONFIG_DIR``/``OPENCODE_CONFIG`` still point at the kit,
and that ``python -m runner.policy validate`` with ``OPENCODE_CONFIG_DIR``
set and no XDG override still resolves the user's installed skills.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import harnesses, policy  # noqa: E402
from runner import kits as kitmod  # noqa: E402

PY = sys.executable


def make_fake_homes(base: Path) -> dict:
    oc_home = base / "fake-opencode-home"
    cx_home = base / "fake-codex-home"
    cl_home = base / "fake-claude-home"
    gr_home = base / "fake-grok-home"
    ag_home = base / "fake-agents-home"
    for d in (oc_home / "skills" / "operations",
              oc_home / "skills" / "project-direction"):
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


class TestNoXdgShadow(unittest.TestCase):
    """Issue #53 shape, as amended by #57: the owned server keeps the user's
    environment through a per-kit XDG mirror, not by leaving XDG alone."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.homes = make_fake_homes(self.base)
        self._saved = {k: os.environ.get(k) for k in list(self.homes) + ["XDG_CONFIG_HOME"]}
        os.environ.update(self.homes)
        # Fake user config source for the mirror (every entry except
        # opencode must appear as a symlink).
        self.user_config = self.base / "fake-user-config-53"
        (self.user_config / "gh").mkdir(parents=True, exist_ok=True)
        (self.user_config / "gh" / "hosts.yml").write_text("user: u\n")
        (self.user_config / "sometool").mkdir(parents=True, exist_ok=True)
        (self.user_config / "opencode").mkdir(parents=True, exist_ok=True)
        os.environ["XDG_CONFIG_HOME"] = str(self.user_config)
        self.addCleanup(self._restore)

    def _restore(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_kit_env_carries_mirror_and_flag(self):
        dest = self.base / "kit-worker"
        kitmod.materialize_opencode_kit("worker", dest, route="muse-spark-xhigh-free")
        self.assertFalse((dest / "xdg-shadow").exists())
        env = kitmod.opencode_kit_env(dest)
        self.assertEqual(env, {
            "OPENCODE_CONFIG_DIR": str(dest),
            "OPENCODE_CONFIG": str(dest / "opencode.json"),
            "XDG_CONFIG_HOME": str(dest / "xdg-mirror"),
            "OPENCODE_DISABLE_EXTERNAL_SKILLS": "1",
        })
        mirror = dest / "xdg-mirror"
        self.assertTrue(mirror.is_dir())
        self.assertFalse((mirror / "opencode").exists())
        self.assertTrue((mirror / "gh").is_symlink())
        self.assertTrue((mirror / "sometool").is_symlink())

    def test_spawn_spec_sets_mirror_for_worker_and_dispatcher(self):
        for route, stage in (("muse-spark-xhigh-free", "implementation"),
                             ("luna-go/max", "dispatch")):
            with self.subTest(route=route):
                (self.base / "state" / "outputs").mkdir(parents=True, exist_ok=True)
                inv = {"request_id": "r53", "invocation_id": f"i-{stage}",
                       "stdout_path": str(self.base / "state" / "outputs" / f"r53.{stage}.stdout"),
                       "meta_json": json.dumps({"route": route, "stage": stage})}
                env, _pwd = harnesses.harness_named("opencode").spawn_spec(
                    inv, {"PATH": "x", "XDG_CONFIG_HOME": str(self.user_config)})
                kit_dir = Path(env["OPENCODE_CONFIG_DIR"])
                self.assertEqual(env.get("XDG_CONFIG_HOME"), str(kit_dir / "xdg-mirror"))
                self.assertEqual(env.get("OPENCODE_DISABLE_EXTERNAL_SKILLS"), "1")
                self.assertNotIn("xdg-shadow", env.get("XDG_CONFIG_HOME", ""))
                self.assertTrue((kit_dir / "kit.json").is_file())
                self.assertEqual(env["OPENCODE_CONFIG"], str(kit_dir / "opencode.json"))
                self.assertFalse((kit_dir / "xdg-shadow").exists())
                self.assertTrue((kit_dir / "xdg-mirror" / "gh").is_symlink())
                self.assertFalse((kit_dir / "xdg-mirror" / "opencode").exists())

    def test_spawn_spec_without_parent_xdg_sets_mirror_from_runner_env(self):
        (self.base / "state" / "outputs").mkdir(parents=True, exist_ok=True)
        inv = {"request_id": "r53", "invocation_id": "i-absent",
               "stdout_path": str(self.base / "state" / "outputs" / "r53.i-absent.stdout"),
               "meta_json": json.dumps({"route": "muse-spark-xhigh-free",
                                        "stage": "implementation"})}
        env, _pwd = harnesses.harness_named("opencode").spawn_spec(inv, {"PATH": "x"})
        kit_dir = Path(env["OPENCODE_CONFIG_DIR"])
        # The runner's own XDG (fake source) still feeds the mirror.
        self.assertEqual(env.get("XDG_CONFIG_HOME"), str(kit_dir / "xdg-mirror"))
        self.assertEqual(env.get("OPENCODE_DISABLE_EXTERNAL_SKILLS"), "1")


class TestValidateWithKitConfigDir(unittest.TestCase):
    def test_validate_with_opencode_config_dir_set_resolves_user_skills(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name)
        homes = make_fake_homes(base)
        kit_dir = base / "kit-like-dir"
        kit_dir.mkdir(parents=True, exist_ok=True)
        (kit_dir / "opencode.json").write_text("{}")
        env = dict(os.environ)
        env.update(homes)
        # The owned server sets OPENCODE_CONFIG_DIR at the kit; the lookup
        # must still resolve the user's installed skills with no XDG
        # override (rehearsal-5 reported every kit skill "not installed"
        # when the XDG shadow diverted the lookup).
        env["OPENCODE_CONFIG_DIR"] = str(kit_dir)
        env["OPENCODE_CONFIG"] = str(kit_dir / "opencode.json")
        env.pop("XDG_CONFIG_HOME", None)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        proc = subprocess.run(
            [PY, "-m", "runner.policy", "validate"],
            capture_output=True, text=True, timeout=25, cwd=str(ROOT), env=env)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        saved = {k: os.environ.get(k) for k in list(homes) + ["OPENCODE_CONFIG_DIR",
                                                              "OPENCODE_CONFIG",
                                                              "XDG_CONFIG_HOME"]}
        try:
            os.environ.update(homes)
            os.environ["OPENCODE_CONFIG_DIR"] = str(kit_dir)
            os.environ["OPENCODE_CONFIG"] = str(kit_dir / "opencode.json")
            os.environ.pop("XDG_CONFIG_HOME", None)
            self.assertTrue(policy.is_skill_installed("operations"))
            self.assertTrue(policy.is_skill_installed("project-direction"))
            self.assertEqual(policy.kit_problems(), [])
            self.assertEqual(policy.validate_policy(), [])
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


if __name__ == "__main__":
    unittest.main()
