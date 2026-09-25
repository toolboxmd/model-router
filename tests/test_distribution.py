"""Exercise the whole plugin from an unrelated project without Python setup."""
from pathlib import Path
import json
import os
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class DistributionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.package = self.root / "plugin cache/model-router"
        shutil.copytree(ROOT, self.package, ignore=shutil.ignore_patterns(".git", "__pycache__", "*.pyc"))
        self.workspace = self.root / "outside project"
        self.workspace.mkdir()
        self.state = self.root / "state"
        self.env = dict(os.environ)
        self.env.pop("PYTHONPATH", None)
        self.env["PYTHONDONTWRITEBYTECODE"] = "1"

    def command(self, *args):
        result = subprocess.run([str(self.package / "bin/model-router"), *args],
                                cwd=self.workspace, env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        return result.stdout

    def test_installed_launcher_preserves_relative_inputs_and_persists_offline_job(self):
        self.assertEqual(self.command("--version").strip(), (ROOT / "VERSION").read_text().strip())
        self.assertIn("submit", self.command("--help"))
        (self.workspace / "packet.json").write_text(json.dumps({"goal": "offline package acceptance"}))
        result = json.loads(self.command("--state-dir", str(self.state), "submit", "--request-id", "package-test",
            "--task-file", "packet.json", "--workspace", ".", "--planner-session", "offline-fixture", "--planner-t3-thread", "planner-t3",
            "--no-start"))
        self.assertTrue(result["acknowledged"])
        status = json.loads(self.command("--state-dir", str(self.state), "status", "--request-id", "package-test"))
        self.assertEqual(Path(status["job"]["workspace"]).resolve(), self.workspace.resolve())
        self.assertFalse(status["launches"])
        # Neither sys.path nor caller cwd may supply a replacement runtime.
        (self.workspace / "runner.py").write_text("raise RuntimeError('wrong runtime')\n")
        self.assertIn("submit", self.command("--help"))

    def test_project_record_and_all_native_manifests_describe_one_complete_package(self):
        record = json.loads((self.package / ".toolboxmd/project.json").read_text())
        version = (self.package / record["factSources"]["version"]).read_text().strip()
        self.assertEqual(record["id"], "model-router")
        self.assertEqual(set(record["factSources"]["delivery"]), {"codex", "claude-code", "grok-build"})
        for relative in record["factSources"]["delivery"].values():
            manifest = json.loads((self.package / relative).read_text())
            self.assertEqual((manifest["name"], manifest["version"]), (record["id"], version))
        # The marketplace rejects an empty path list; a module without Skills omits the field.
        self.assertNotIn("skills", record["factSources"])
        for field in ("documentation", "requirements", "proof"):
            for relative in record["factSources"][field]:
                self.assertTrue((self.package / relative).is_file(), relative)
        self.assertFalse((self.package / "skills").exists())
        for relative in ("bin/model-router", "runner/__main__.py", "runner/cli.py", "runner/core.py"):
            self.assertTrue((self.package / relative).is_file(), relative)


if __name__ == "__main__":
    unittest.main()
