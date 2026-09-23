#!/usr/bin/env python3
"""Run deterministic tests with disposable dependency homes, without installation."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests.test_kits import make_fake_homes  # noqa: E402


def main():
    arguments = sys.argv[1:] or ["discover", "-s", "tests", "-p", "test_*.py"]
    with tempfile.TemporaryDirectory(prefix="model-router-test-kits-") as tmp:
        # Reuse the suite's existing fake inventory. Real kit validation still
        # runs, including regressions that remove or replace dependencies.
        env = dict(os.environ, **make_fake_homes(Path(tmp)))
        return subprocess.call([sys.executable, "-m", "unittest", *arguments],
                               cwd=ROOT, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
