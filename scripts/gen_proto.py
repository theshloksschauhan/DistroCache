from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO_DIR = ROOT / "proto"
OUT_DIR = ROOT / "common" / "generated"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{PROTO_DIR}",
            f"--python_out={OUT_DIR}",
            f"--grpc_python_out={OUT_DIR}",
            str(PROTO_DIR / "cache.proto"),
        ]
    )
    init_file = OUT_DIR / "__init__.py"
    init_file.touch(exist_ok=True)
    _patch_grpc_imports(OUT_DIR / "cache_pb2_grpc.py")


def _patch_grpc_imports(target: Path) -> None:
    if not target.exists():
        return
    content = target.read_text(encoding="utf-8")
    updated = content.replace(
        "import cache_pb2 as cache__pb2",
        "from . import cache_pb2 as cache__pb2",
    )
    if updated != content:
        target.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
