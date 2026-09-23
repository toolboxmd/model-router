"""AgentsMD procedure bundles and legacy split skills, without model calls."""
from __future__ import annotations

from contextlib import nullcontext
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

from runner import direction, kits, policy
from tests.test_kits import make_fake_homes


class TestAgentsmdBundle(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.homes = make_fake_homes(self.base)
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, self.homes).start()
        patch.dict(os.environ, {"XDG_CONFIG_HOME": str(self.base / "config")}).start()
        self.installed = Path(self.homes["MODEL_ROUTER_OPENCODE_HOME"])
        self.operations = self.installed / "skills" / "operations"
        self.legacy = self.installed / "skills" / "project-direction"
        self.materializers = {
            "opencode": kits.materialize_opencode_kit,
            "codex": kits.materialize_codex_kit,
            "claude": kits.materialize_claude_kit,
            "grok": kits.materialize_grok_kit,
        }

    def install_bundle(self, keep_legacy=False):
        if not keep_legacy:
            shutil.rmtree(self.legacy)
        files = {
            "SKILL.md": ("---\nname: operations\nmetadata:\n"
                         "  agentsmd-layout: procedures-v1\n---\n"
                         "[Direction](workflows/project-direction/index.md)\n"),
            "workflows/project-direction/index.md": (
                "[Context](references/context.md)\n"
                "[Implementation](../../references/implementation.md)\n"),
            "workflows/project-direction/references/context.md": "# Complete direction context\n",
            "references/implementation.md": "[Review](../workflows/review/index.md)\n",
            "workflows/review/index.md": "# Read-only review\n",
        }
        for relative, content in files.items():
            target = self.operations / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        return files

    def test_every_non_planner_role_on_every_host_with_both_layouts(self):
        for bundled in (False, True):
            files = self.install_bundle() if bundled else {}
            self.assertEqual(policy.kit_problems(), [])
            for copied in (False, True):
                for host, materialize in self.materializers.items():
                    for role in policy.KIT_ROLES[1:]:
                        with self.subTest(bundled=bundled, copied=copied, host=host, role=role):
                            dest = kits.kit_dir_for(self.base / "state", str(bundled),
                                                    f"{host}-{copied}-{role}", role)
                            fallback = patch("os.symlink", side_effect=OSError("disabled"))
                            with fallback if copied else nullcontext():
                                materialize(role, dest)
                            ops = dest / "skills" / "operations"
                            self.assertEqual(ops.is_symlink(), not copied)
                            self.assertTrue(ops.stat().st_mode & 0o100)
                            for relative, content in files.items():
                                target = ops / relative
                                self.assertEqual(target.read_text(), content)
                                for link in re.findall(r"\]\(([^)]+)\)", content):
                                    self.assertTrue((target.parent / link).is_file(), link)
                            expected = ["operations"]
                            if not bundled and role in ("worker", "correction"):
                                expected.append("project-direction")
                            if host == "opencode":
                                expected.append("customize-opencode")
                            observed = kits.observed_skills_for_kit_dir(dest)
                            self.assertEqual(observed, sorted(expected))
                            self.assertEqual(kits.observed_skills_for_invocation(
                                self.base / "state", str(bundled), f"{host}-{copied}-{role}"), observed)
                            stored = json.loads((dest / "kit.json").read_text())
                            self.assertEqual(stored["skills"], policy.KITS[role]["skills"])
                            self.assertEqual(stored["hash"], policy.kit_hash(policy.KITS[role]))
                            self.assertEqual(stored["permission_set"], policy.KITS[role]["permission_set"])
                            self.assertEqual(stored["plugins"], ["agentsmd-project-direction"])
                            self.assertTrue((dest / "plugins" / "agentsmd-project-direction.js").is_file())
                            self.assertEqual(direction.supply_for_harness(host, True, role), "hook")
                            if host == "opencode":
                                self.assertEqual(stored["skills_loaded"], observed)
                                config = json.loads((dest / "opencode.json").read_text())
                                self.assertEqual(sorted(config["skills"]),
                                                 sorted(set(expected) - {"customize-opencode"}))
                            if host == "claude":
                                config = json.loads((dest / "settings.json").read_text())
                                self.assertEqual(sorted(config["skills"]), sorted(expected))

    def test_partial_bundle_never_falls_back_to_legacy_direction(self):
        self.install_bundle(keep_legacy=True)
        for relative in ("workflows/project-direction/index.md",
                         "workflows/project-direction/references/context.md"):
            required = self.operations / relative
            content = required.read_text()
            required.unlink()
            for role in policy.KIT_ROLES[1:]:
                self.assertTrue(any(str(required) in problem for problem in policy.kit_problems(role)))
                for host, materialize in self.materializers.items():
                    with self.subTest(role=role, host=host, missing=relative):
                        with self.assertRaisesRegex(ValueError, re.escape(str(required))):
                            materialize(role, self.base / "missing" / host / role)
            required.write_text(content)

    def test_declared_bundle_with_no_workflows_never_falls_back(self):
        self.install_bundle(keep_legacy=True)
        shutil.rmtree(self.operations / "workflows")
        for role in policy.KIT_ROLES[1:]:
            self.assertTrue(any("required AgentsMD procedure" in problem
                                for problem in policy.kit_problems(role)))
            for host, materialize in self.materializers.items():
                with self.subTest(role=role, host=host):
                    with self.assertRaisesRegex(ValueError, "required AgentsMD procedure"):
                        materialize(role, self.base / "missing-tree" / host / role)

    def test_layout_declaration_is_checked_before_legacy_fallback(self):
        for declared in ("procedures-v2", "", "null"):
            (self.operations / "SKILL.md").write_text(
                f"---\nname: operations\nmetadata:\n  agentsmd-layout: {declared}\n---\n")
            self.assertTrue(any("unsupported AgentsMD layout" in problem
                                for problem in policy.kit_problems("worker")))
            with self.assertRaisesRegex(ValueError, "unsupported AgentsMD layout"):
                kits.materialize_codex_kit("worker", self.base / "unsupported")

    def test_layout_declaration_quotes_and_directory_detection(self):
        self.install_bundle()
        for header in ("", "---\nmetadata:\n  agentsmd-layout: 'procedures-v1'\n---\n",
                       '---\nmetadata:\n  agentsmd-layout: "procedures-v1" # bundle\n---\n'):
            (self.operations / "SKILL.md").write_text(header + "# Operations\n")
            self.assertEqual(policy.kit_problems(), [])
            self.assertEqual(list(policy.kit_skill_sources(policy.KITS["worker"])), ["operations"])

    def test_missing_legacy_direction_or_plugin_fails(self):
        for dependency in (self.operations / "SKILL.md", self.legacy,
                           self.installed / "plugins" / "agentsmd-project-direction.js"):
            renamed = dependency.with_name(dependency.name + ".saved")
            dependency.rename(renamed)
            for role in ("worker", "correction"):
                self.assertTrue(policy.kit_problems(role))
                for host, materialize in self.materializers.items():
                    with self.subTest(role=role, host=host, dependency=dependency.name):
                        with self.assertRaisesRegex(ValueError, "not installed"):
                            materialize(role, self.base / "missing" / host / role)
            renamed.rename(dependency)

    def test_broken_workflows_link_is_a_partial_bundle(self):
        (self.operations / "workflows").symlink_to(self.base / "missing-workflows")
        for role in policy.KIT_ROLES[1:]:
            self.assertTrue(any("required AgentsMD procedure" in problem
                                for problem in policy.kit_problems(role)))

    def test_bundle_replaces_stale_separate_skill_in_reused_kit(self):
        for copied in (False, True):
            for host, materialize in self.materializers.items():
                dest = self.base / f"reused-{host}-{copied}"
                fallback = patch("os.symlink", side_effect=OSError("disabled"))
                with fallback if copied else nullcontext():
                    materialize("worker", dest)
        self.install_bundle(keep_legacy=True)
        for copied in (False, True):
            for host, materialize in self.materializers.items():
                dest = self.base / f"reused-{host}-{copied}"
                materialize("worker", dest)
                self.assertNotIn("project-direction", kits.observed_skills_for_kit_dir(dest))
                self.assertTrue((self.legacy / "SKILL.md").is_file())


if __name__ == "__main__":
    unittest.main()
