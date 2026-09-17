#!/usr/bin/env python3
"""Split a file into connector-sized parts + manifest (deploy lane v3, sender side, for lanes with a shell).

  python3 scripts/split_parts.py aws/lambdas/justhodl-stock-buying/source/lambda_function.py
  python3 scripts/split_parts.py path/to/new_version.py --target aws/lambdas/justhodl-x/source/lambda_function.py

Writes aws/ops/patchers/parts/<upload-id>/{part-001.., manifest.json}. Commit and push that folder;
the apply lane reassembles it, checks it, writes the target and dispatches the deploy.

Text files are split on line boundaries with the v3 marker lines (@@PART n/N@@ ... @@END n@@) and a
manifest that also carries sha256/bytes for exactness. A file that is not UTF-8 text, has CRLF, no
trailing newline, or a single line longer than a part is split raw ("join": "bytes", sha256 checked).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKER_OVERHEAD = 40   # "@@PART nnn/nnn@@\n" + "@@END nnn@@\n"


def _text_lines(data: bytes) -> list[str] | None:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if "\r" in text or not text.endswith("\n"):
        return None
    return text.splitlines(keepends=True)


def split(source: Path, target: str, chunk: int, note: str, root: Path = ROOT) -> Path:
    data = source.read_bytes()
    upload_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + Path(target).parts[-3 if len(Path(target).parts) >= 3 else 0]
    folder = root / "aws/ops/patchers/parts" / upload_id
    folder.mkdir(parents=True, exist_ok=False)
    manifest = {"target": target, "complete": True, "note": note or f"{target} via split_parts",
                "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    lines = _text_lines(data)
    budget = chunk - MARKER_OVERHEAD
    if lines is not None and all(len(ln.encode("utf-8")) <= budget for ln in lines):
        groups, cur, size = [], [], 0
        for ln in lines:
            n = len(ln.encode("utf-8"))
            if cur and size + n > budget:
                groups.append(cur); cur, size = [], 0
            cur.append(ln); size += n
        if cur:
            groups.append(cur)
        total = len(groups)
        for i, grp in enumerate(groups, 1):
            (folder / f"part-{i:03d}").write_text(f"@@PART {i}/{total}@@\n" + "".join(grp) + f"@@END {i}@@\n", encoding="utf-8")
        nonblank = [ln.strip() for ln in lines if ln.strip()]
        manifest.update(parts=total, join="lines", first_line=nonblank[0] if nonblank else "", last_line=nonblank[-1] if nonblank else "")
    else:
        names = []
        for i in range(0, len(data), chunk):
            name = f"part-{len(names) + 1:03d}"
            (folder / name).write_bytes(data[i:i + chunk]); names.append(name)
        manifest.update(parts=len(names), join="bytes")
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return folder


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--target", help="repo-relative destination (default: the source path)")
    ap.add_argument("--chunk", type=int, default=12000, help="max bytes per part (default 12000; the connector truncates ~40 KB)")
    ap.add_argument("--note", default="")
    a = ap.parse_args()
    src = Path(a.source)
    target = a.target or str(src.resolve().relative_to(ROOT))
    folder = split(src, target, a.chunk, a.note)
    print(f"wrote {folder.relative_to(ROOT)} ({len(list(folder.glob('part-*')))} parts) -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
