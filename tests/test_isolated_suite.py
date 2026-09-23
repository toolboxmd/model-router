"""The proof entry point must work without a developer's installed kits."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class IsolatedSuite(unittest.TestCase):
    def run_suite(self, *names):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            installed = root / "user-config"
            installed.mkdir()
            marker = installed / "keep.txt"
            marker.write_text("user-owned configuration\n")
            scratch = root / "scratch"
            scratch.mkdir()
            env = dict(os.environ, TMPDIR=str(scratch))
            for host in ("OPENCODE", "CODEX", "CLAUDE", "GROK", "AGENTS"):
                env[f"MODEL_ROUTER_{host}_HOME"] = str(installed)
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/test.py"), *names],
                cwd=root, env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(marker.read_text(), "user-owned configuration\n")
            self.assertEqual(list(installed.iterdir()), [marker])
            self.assertEqual(list(scratch.iterdir()), [])
            return result

    def test_empty_host_uses_disposable_kits_and_preserves_validation(self):
        result = self.run_suite(
            "tests.test_policy_v2.PolicyData.test_policy_consistent",
            "tests.test_kits.TestKitPolicy.test_validate_rejects_unknown_skill_plugin_mcp")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Ran 2 tests", result.stderr)

    def test_test_failure_is_returned_and_fixtures_are_removed(self):
        result = self.run_suite("tests.test_policy_v2.MissingTestCase")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("FAILED", result.stderr)
