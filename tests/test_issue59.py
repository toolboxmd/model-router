"""Issue #59: resolver ignores OpenCode's run-time scratch in the XDG mirror.

Deterministic only. No live model CLIs. Proves that ``policy._opencode_home``
prefers ``$XDG_CONFIG_HOME/opencode`` only when it is a real OpenCode home
(holding ``opencode.json``, ``opencode.jsonc``, ``skills/``, or ``plugins/``),
falls back to ``~/.config/opencode`` for run-time scratch
(``package.json``, ``node_modules/``), and that
``MODEL_ROUTER_OPENCODE_HOME`` keeps precedence.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runner import policy  # noqa: E402


class TestOpencodeHomeResolver59(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self._saved = {k: os.environ.get(k) for k in
                       ("MODEL_ROUTER_OPENCODE_HOME", "XDG_CONFIG_HOME")}
        self.addCleanup(self._restore)

    def _restore(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _resolve(self, xdg: Path, override: str | None = None) -> Path:
        if override is None:
            os.environ.pop("MODEL_ROUTER_OPENCODE_HOME", None)
        else:
            os.environ["MODEL_ROUTER_OPENCODE_HOME"] = override
        os.environ["XDG_CONFIG_HOME"] = str(xdg)
        return policy._opencode_home()

    def test_scratch_only_resolves_to_fallback(self):
        xdg = self.base / "xdg-scratch"
        scratch = xdg / "opencode"
        (scratch / "node_modules" / "dep").mkdir(parents=True, exist_ok=True)
        (scratch / "package.json").write_text("{}\n")
        (scratch / "package-lock.json").write_text("{}\n")
        (scratch / ".gitignore").write_text("node_modules\n")
        self.assertEqual(self._resolve(xdg), Path.home() / ".config" / "opencode")

    def test_empty_and_missing_resolve_to_fallback(self):
        xdg_empty = self.base / "xdg-empty"
        (xdg_empty / "opencode").mkdir(parents=True, exist_ok=True)
        self.assertEqual(self._resolve(xdg_empty), Path.home() / ".config" / "opencode")
        xdg_missing = self.base / "xdg-missing"
        xdg_missing.mkdir(parents=True, exist_ok=True)
        self.assertEqual(self._resolve(xdg_missing), Path.home() / ".config" / "opencode")

    def test_real_home_markers_resolve_to_itself(self):
        markers = [
            ("opencode.json", True),
            ("opencode.jsonc", True),
            ("skills", False),
            ("plugins", False),
        ]
        for name, is_file in markers:
            with self.subTest(marker=name):
                xdg = self.base / f"xdg-{name.replace('.', '_')}"
                cand = xdg / "opencode"
                cand.mkdir(parents=True, exist_ok=True)
                if is_file:
                    (cand / name).write_text("{}\n")
                else:
                    (cand / name).mkdir(parents=True, exist_ok=True)
                self.assertEqual(self._resolve(xdg), cand)

    def test_skills_marker_resolves_to_itself(self):
        xdg = self.base / "xdg-skills"
        cand = xdg / "opencode" / "skills"
        cand.mkdir(parents=True, exist_ok=True)
        self.assertEqual(self._resolve(xdg), xdg / "opencode")

    def test_override_wins_over_scratch_and_real_home(self):
        override = self.base / "override-home"
        override.mkdir(parents=True, exist_ok=True)
        xdg_scratch = self.base / "xdg-scratch-ovr"
        scratch = xdg_scratch / "opencode"
        (scratch / "node_modules").mkdir(parents=True, exist_ok=True)
        (scratch / "package.json").write_text("{}\n")
        self.assertEqual(self._resolve(xdg_scratch, override=str(override)), override)
        xdg_real = self.base / "xdg-real-ovr"
        (xdg_real / "opencode" / "skills").mkdir(parents=True, exist_ok=True)
        self.assertEqual(self._resolve(xdg_real, override=str(override)), override)

    def test_runner_docs_note_scratch_ignored(self):
        doc = (ROOT / "RUNNER.md").read_text()
        self.assertIn("plugin scratch", doc)
        self.assertIn("_opencode_home", doc)


if __name__ == "__main__":
    unittest.main()
