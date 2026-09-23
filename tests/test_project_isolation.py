"""Project-local configuration must not widen an owned OpenCode kit."""
from pathlib import Path
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runner import kits


class ProjectIsolationTests(unittest.TestCase):
    def test_owned_server_disables_both_external_and_project_configuration(self):
        env = kits.opencode_kit_env(Path("/kit"))
        self.assertEqual(env["OPENCODE_DISABLE_EXTERNAL_SKILLS"], "1")
        self.assertEqual(env["OPENCODE_DISABLE_PROJECT_CONFIG"], "1")

    @unittest.skipUnless(os.environ.get("MODEL_ROUTER_TEST_OPENCODE_DISCOVERY") == "1",
                         "opt-in local OpenCode discovery, without a model request")
    def test_local_binary_discovers_only_kit_skills_and_mcp(self):
        binary = shutil.which("opencode")
        if binary is None:
            self.skipTest("OpenCode is not installed")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = root / "project"
            hidden_skill = workspace / ".opencode/skills/project-extra"
            hidden_skill.mkdir(parents=True)
            (hidden_skill / "SKILL.md").write_text("---\nname: project-extra\ndescription: Isolated regression fixture.\n---\nFixture.\n")
            (workspace / "opencode.json").write_text(json.dumps({"mcp": {
                "project-extra": {"type": "remote", "url": "https://example.invalid/mcp", "enabled": False}}}))
            subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)
            kit = root / "kit"
            with patch.dict(os.environ, {"XDG_CONFIG_HOME": str(root / "config")}), patch.object(kits.policy, "kit_for_role", return_value={"skills": [], "plugins": [], "mcp": []}):
                kits.materialize_opencode_kit("fixture", kit)
            env = {**os.environ, **kits.opencode_kit_env(kit), "OPENCODE_CONFIG_CONTENT": "{}"}
            def debug(command, environment):
                result = subprocess.run([binary, "debug", command], cwd=workspace, env=environment,
                                        capture_output=True, text=True, timeout=45)
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)
            control = {**env, "OPENCODE_DISABLE_PROJECT_CONFIG": "0"}
            self.assertIn("project-extra", {item["name"] for item in debug("skill", control)})
            self.assertIn("project-extra", debug("config", control).get("mcp", {}))
            actual = {item["name"] for item in debug("skill", env)}
            self.assertEqual(actual, set(kits.observed_opencode_skills(kit)))
            self.assertEqual(debug("config", env).get("mcp", {}), {})


if __name__ == "__main__":
    unittest.main()
