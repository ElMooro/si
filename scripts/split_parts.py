#!/usr/bin/env python3
"""Split a file into Contents-API-sized parts + manifest (deploy lane v2, sender side).

  python3 scripts/split_parts.py aws/lambdas/justhodl-stock-buying/source/lambda_function.py
  python3 scripts/split_parts.py path/to/new_version.py --target aws/lambdas/justhodl-x/source/lambda_function.py

Writes aws/ops/patchers/parts/<upload-id>/{manifest.json,part-001..}. Commit and push
that folder (each part is <= --chunk bytes, default 16000); the apply lane reassembles,
verifies the sha256, writes the target and dispatches the deploy.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def split(source: Path, target: str, chunk: int, note: str, root: Path = ROOT) -> Path:
    data = source.read_bytes()
    upload_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + Path(target).parts[-3 if len(Path(target).parts) >= 3 else 0]
    folder = root / "aws/ops/patchers/parts" / upload_id
    folder.mkdir(parents=True, exist_ok=False)
    parts = []
    for i in range(0, len(data), chunk):
        name = f"part-{len(parts) + 1:03d}"
        (folder / name).write_bytes(data[i:i + chunk])
        parts.append(name)
    manifest = {"target": target, "parts": parts, "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data), "note": note or f"{target} via split_parts"}
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return folder


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--target", help="repo-relative destination (default: the source path)")
    ap.add_argument("--chunk", type=int, default=16000)
    ap.add_argument("--note", default="")
    a = ap.parse_args()
    src = Path(a.source)
    target = a.target or str(src.resolve().relative_to(ROOT))
    folder = split(src, target, a.chunk, a.note)
    print(f"wrote {folder.relative_to(ROOT)} ({len(list(folder.glob('part-*')))} parts) -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
