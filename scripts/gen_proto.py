from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO_DIR = ROOT / "proto"

OUTPUTS = [
    ROOT / "backend" / "generated",
    ROOT / "node" / "generated",
]


def main() -> None:
    for out_dir in OUTPUTS:
        out_dir.mkdir(parents=True, exist_ok=True)
        subprocess.check_call(
            [
                "python",
                "-m",
                "grpc_tools.protoc",
                f"-I{PROTO_DIR}",
                f"--python_out={out_dir}",
                f"--grpc_python_out={out_dir}",
                str(PROTO_DIR / "cache.proto"),
            ]
        )
        init_file = out_dir / "__init__.py"
        init_file.touch(exist_ok=True)
        _patch_grpc_imports(out_dir / "cache_pb2_grpc.py")


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
