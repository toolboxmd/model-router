#!/usr/bin/env python3
"""Run deterministic tests offline, isolated from the user's own T3 server."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def main():
    arguments = sys.argv[1:] or ["discover", "-s", "tests", "-p", "test_*.py"]
    with tempfile.TemporaryDirectory(prefix="model-router-test-") as tmp:
        # A test that forgets its fake server reaches a closed port and a
        # disposable T3 home, never the user's running T3 (port 3773).
        env = dict(os.environ, T3CODE_HOME=tmp, T3_HOME=tmp,
                   T3_SERVER_URL="http://127.0.0.1:9")
        env.pop("T3_SERVER_TOKEN", None)
        return subprocess.call([sys.executable, "-m", "unittest", *arguments],
                               cwd=ROOT, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
