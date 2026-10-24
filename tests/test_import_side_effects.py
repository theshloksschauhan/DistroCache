import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="node/app.py opens its SQLite database at import time (fixed in Stage 1)",
)
def test_importing_apps_creates_no_files(tmp_path: Path) -> None:
    env = {k: v for k, v in os.environ.items() if k != "STORAGE_PATH"}
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    # Run in a fresh interpreter so modules imported by other tests don't hide the side effect.
    result = subprocess.run(
        [sys.executable, "-c", "import backend.app, node.app"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"import failed:\n{result.stderr}")

    created = sorted(p.name for p in tmp_path.iterdir())
    assert created == [], f"importing the apps created files in the working directory: {created}"
