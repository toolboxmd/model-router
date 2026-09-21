"""Issue #57: skill isolation on the owned OpenCode server.

Deterministic only. No live model CLIs. Proves that the owned-server kit
builds a per-kit XDG mirror (one symlink per user config entry except
``opencode``, rebuilt per invocation) with ``OPENCODE_DISABLE_EXTERNAL_SKILLS=1``,
that ``policy._opencode_home`` falls back to ``~/.config/opencode`` under the
mirror, and that ``skills_loaded`` is observed from the kit dir plus the
built-in, not copied from policy.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import harnesses, policy  # noqa: E402
from runner import kits as kitmod  # noqa: E402


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


def make_fake_user_config(base: Path) -> Path:
    src = base / "fake-user-config-57"
    (src / "gh").mkdir(parents=True, exist_ok=True)
    (src / "gh" / "hosts.yml").write_text("user: u\n")
    (src / "git").mkdir(parents=True, exist_ok=True)
    (src / "sometool.conf").write_text("x=1\n")
    (src / "opencode").mkdir(parents=True, exist_ok=True)
    (src / "opencode" / "opencode.json").write_text("{}\n")
    return src


class TestXdgMirror(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.homes = make_fake_homes(self.base)
        self.user_config = make_fake_user_config(self.base)
        self._saved = {k: os.environ.get(k) for k in list(self.homes) + ["XDG_CONFIG_HOME"]}
        os.environ.update(self.homes)
        os.environ["XDG_CONFIG_HOME"] = str(self.user_config)
        self.addCleanup(self._restore)

    def _restore(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_mirror_holds_every_entry_except_opencode(self):
        dest = self.base / "kit-worker-57"
        kitmod.materialize_opencode_kit("worker", dest, route="muse-spark-xhigh-free")
        mirror = dest / "xdg-mirror"
        self.assertTrue(mirror.is_dir())
        self.assertFalse((mirror / "opencode").exists())
        for name in ("gh", "git", "sometool.conf"):
            target = mirror / name
            self.assertTrue(target.is_symlink(), name)
            self.assertEqual(os.path.realpath(str(target)),
                             os.path.realpath(str(self.user_config / name)))
        # No extra entries beyond the source minus opencode.
        self.assertEqual({p.name for p in mirror.iterdir()}, {"gh", "git", "sometool.conf"})

    def test_kit_env_carries_mirror_and_disable_flag(self):
        dest = self.base / "kit-env-57"
        kitmod.materialize_opencode_kit("dispatcher", dest, route="luna-go/max")
        env = kitmod.opencode_kit_env(dest)
        self.assertEqual(env["OPENCODE_CONFIG_DIR"], str(dest))
        self.assertEqual(env["OPENCODE_CONFIG"], str(dest / "opencode.json"))
        self.assertEqual(env["XDG_CONFIG_HOME"], str(dest / "xdg-mirror"))
        self.assertEqual(env["OPENCODE_DISABLE_EXTERNAL_SKILLS"], "1")

    def test_spawn_spec_worker_and_dispatcher_carry_mirror(self):
        (self.base / "state" / "outputs").mkdir(parents=True, exist_ok=True)
        for route, stage in (("muse-spark-xhigh-free", "implementation"),
                             ("luna-go/max", "dispatch")):
            with self.subTest(route=route):
                inv = {"request_id": "r57", "invocation_id": f"i-{stage}-57",
                       "stdout_path": str(self.base / "state" / "outputs" / f"r57.{stage}.stdout"),
                       "meta_json": json.dumps({"route": route, "stage": stage})}
                env, _pwd = harnesses.harness_named("opencode").spawn_spec(
                    inv, {"PATH": "x", "XDG_CONFIG_HOME": str(self.user_config)})
                kit_dir = Path(env["OPENCODE_CONFIG_DIR"])
                self.assertEqual(env["XDG_CONFIG_HOME"], str(kit_dir / "xdg-mirror"))
                self.assertEqual(env["OPENCODE_DISABLE_EXTERNAL_SKILLS"], "1")
                self.assertTrue((kit_dir / "xdg-mirror" / "gh").is_symlink())
                self.assertFalse((kit_dir / "xdg-mirror" / "opencode").exists())

    def test_mirror_rebuilt_per_invocation(self):
        dest = self.base / "kit-rebuild-57"
        kitmod.materialize_opencode_kit("worker", dest, route="muse-spark-xhigh-free")
        mirror = dest / "xdg-mirror"
        self.assertFalse((mirror / "later-tool").exists())
        (self.user_config / "later-tool").mkdir(parents=True, exist_ok=True)
        kitmod.materialize_opencode_kit("worker", dest, route="muse-spark-xhigh-free")
        self.assertTrue((mirror / "later-tool").is_symlink())
        # Removed entries disappear on the next materialization.
        import shutil
        shutil.rmtree(self.user_config / "later-tool")
        (self.user_config / "git").rename(self.user_config / "git-renamed")
        try:
            kitmod.materialize_opencode_kit("worker", dest, route="muse-spark-xhigh-free")
            self.assertFalse((mirror / "later-tool").exists())
            self.assertFalse((mirror / "git").exists())
            self.assertTrue((mirror / "git-renamed").is_symlink())
        finally:
            (self.user_config / "git-renamed").rename(self.user_config / "git")


class TestOpencodeHomeFallback(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.homes = make_fake_homes(self.base)
        self.oc_home = Path(self.homes["MODEL_ROUTER_OPENCODE_HOME"])

    def test_override_keeps_precedence_under_mirror(self):
        saved = {k: os.environ.get(k) for k in
                 ("MODEL_ROUTER_OPENCODE_HOME", "XDG_CONFIG_HOME")}
        mirror = self.base / "kit-57" / "xdg-mirror"
        mirror.mkdir(parents=True, exist_ok=True)
        try:
            os.environ["MODEL_ROUTER_OPENCODE_HOME"] = str(self.oc_home)
            os.environ["XDG_CONFIG_HOME"] = str(mirror.parent)
            self.assertEqual(policy._opencode_home(), self.oc_home)
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_fallback_when_mirror_has_no_opencode(self):
        saved = {k: os.environ.get(k) for k in
                 ("MODEL_ROUTER_OPENCODE_HOME", "XDG_CONFIG_HOME")}
        mirror_parent = self.base / "kit-57b"
        (mirror_parent / "xdg-mirror" / "gh").mkdir(parents=True, exist_ok=True)
        try:
            os.environ.pop("MODEL_ROUTER_OPENCODE_HOME", None)
            os.environ["XDG_CONFIG_HOME"] = str(mirror_parent / "xdg-mirror")
            # The mirror carries no opencode entry: fall back to the real home.
            self.assertEqual(policy._opencode_home(),
                             Path.home() / ".config" / "opencode")
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_xdg_opencode_present_is_used(self):
        saved = {k: os.environ.get(k) for k in
                 ("MODEL_ROUTER_OPENCODE_HOME", "XDG_CONFIG_HOME")}
        xdg = self.base / "xdg-with-opencode"
        (xdg / "opencode").mkdir(parents=True, exist_ok=True)
        try:
            os.environ.pop("MODEL_ROUTER_OPENCODE_HOME", None)
            os.environ["XDG_CONFIG_HOME"] = str(xdg)
            self.assertEqual(policy._opencode_home(), xdg / "opencode")
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


class TestObservedSkillsLoaded(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.homes = make_fake_homes(self.base)
        self.user_config = make_fake_user_config(self.base)
        self._saved = {k: os.environ.get(k) for k in list(self.homes) + ["XDG_CONFIG_HOME"]}
        os.environ.update(self.homes)
        os.environ["XDG_CONFIG_HOME"] = str(self.user_config)
        self.addCleanup(self._restore)

    def _restore(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_observed_equals_kit_dirs_plus_builtin(self):
        for kit_name in ("worker", "dispatcher"):
            with self.subTest(kit=kit_name):
                dest = self.base / f"kit-{kit_name}-57"
                route = "muse-spark-xhigh-free" if kit_name == "worker" else "luna-go/max"
                kitmod.materialize_opencode_kit(kit_name, dest, route=route)
                observed = kitmod.observed_opencode_skills(dest)
                kit = policy.kit_for_role(kit_name)
                self.assertEqual(sorted(observed), sorted(list(kit["skills"]) + ["customize-opencode"]))
                stored = json.loads((dest / "kit.json").read_text())
                self.assertEqual(sorted(stored["skills_loaded"]), sorted(observed))
                # The ledger helper reads the recorded value.
                from runner import kits as _k
                state_dir = self.base / "state-obs"
                inv_id = f"inv-{kit_name}"
                target = _k.kit_dir_for(str(state_dir), "r57", inv_id, kit_name)
                target.parent.mkdir(parents=True, exist_ok=True)
                import shutil
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(str(dest), str(target), symlinks=True)
                got = _k.observed_skills_for_invocation(str(state_dir), "r57", inv_id)
                self.assertEqual(sorted(got or []), sorted(observed))

    def test_runner_docs_state_observed_skills(self):
        doc = (ROOT / "RUNNER.md").read_text()
        self.assertIn("skills_loaded", doc)
        self.assertIn("customize-opencode", doc)
        self.assertIn("observed", doc.lower())
        glossary = (ROOT / "GLOSSARY.md").read_text()
        self.assertIn("customize-opencode", glossary)
        self.assertIn("not the policy list", glossary)


if __name__ == "__main__":
    unittest.main()
