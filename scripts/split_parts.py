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
import re
import runpy
import time
import uuid
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


def _write_parts(source: Path, target: str, chunk: int, note: str, folder: Path) -> dict:
    if not 256 <= chunk <= 12000:
        raise ValueError("chunk must be between 256 and 12000 bytes (connector-safe limit)")
    data = source.read_bytes()
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
            (folder / f"part-{i:03d}").write_text(f"@@PART {i}/{total}@@\n" + "".join(grp) + f"@@END {i}@@\n", encoding="utf-8", newline="\n")
        nonblank = [ln.strip() for ln in lines if ln.strip()]
        manifest.update(parts=total, join="lines", first_line=nonblank[0] if nonblank else "", last_line=nonblank[-1] if nonblank else "")
    else:
        names = []
        for i in range(0, len(data), chunk):
            name = f"part-{len(names) + 1:03d}"
            (folder / name).write_bytes(data[i:i + chunk]); names.append(name)
        manifest.update(parts=len(names), join="bytes")
    return manifest


def _upload_folder(root: Path, kind: str = "parts", lane: str = "") -> Path:
    if lane and not re.fullmatch(r"[A-Za-z0-9_-]{1,16}", lane):
        raise ValueError("lane must be 1-16 letters, digits, dashes or underscores")
    upload_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12]
    if lane:
        upload_id = lane + "-" + upload_id
    return root / "aws/ops/patchers" / kind / (upload_id.lower() if kind == "batch" else upload_id)


def _manifest(folder: Path, value: dict) -> None:
    (folder / "manifest.json").write_text(json.dumps(value, indent=1) + "\n", encoding="utf-8", newline="\n")


def split(source: Path, target: str, chunk: int, note: str, root: Path = ROOT) -> Path:
    folder = _upload_folder(root)
    _manifest(folder, _write_parts(source, target, chunk, note, folder))
    return folder


def split_batch(files: list[dict], chunk: int = 12000, note: str = "", root: Path = ROOT, lane: str = "") -> Path:
    """Generate the existing v4 batch format; source paths are relative to root."""
    if not files or not isinstance(files, list):
        raise ValueError("batch must be a non-empty list of source/target objects")
    if not 256 <= chunk <= 12000:
        raise ValueError("chunk must be between 256 and 12000 bytes")
    targets = [item["target"] for item in files]
    if len(set(targets)) != len(targets):
        raise ValueError("batch contains duplicate targets")
    batch_module = runpy.run_path(str(ROOT / "scripts/assemble_batch.py"))
    for target in targets:
        batch_module["_safe_target"](target)
    folder = _upload_folder(root, "batch", lane)
    members = []
    for item in files:
        source = root / item["source"]
        if source.stat().st_size <= 10000:
            data = batch_module["_whole_bytes"](source, item["target"])
            destination = folder / "files" / item["target"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
            member = {"target": item["target"], "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
        else:
            member = _write_parts(source, item["target"], chunk, note, folder / "parts" / item["target"])
            member.pop("complete")
        for option in ("base_sha", "base_blob_sha", "shrink_ok", "fragment"):
            if option in item:
                member[option] = item[option]
        members.append(member)
    _manifest(folder, {"complete": True, "lane": lane, "note": note, "files": members})
    return folder


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source", nargs="?")
    ap.add_argument("--batch", help="JSON file containing a list of source/target objects; upload as one release")
    ap.add_argument("--target", help="repo-relative destination (default: the source path)")
    ap.add_argument("--chunk", type=int, default=12000, help="max bytes per part (default 12000; the connector truncates ~40 KB)")
    ap.add_argument("--note", default="")
    ap.add_argument("--lane", default="", help="Optional lane prefix for a batch ID")
    a = ap.parse_args()
    if a.batch:
        if a.source or a.target:
            ap.error("--batch cannot be combined with source or --target")
        folder = split_batch(json.loads(Path(a.batch).read_text(encoding="utf-8")), a.chunk, a.note, lane=a.lane)
        print(f"wrote batch {folder.relative_to(ROOT)}; upload every member's parts, then manifest.json LAST")
        return 0
    if not a.source:
        ap.error("give a source or --batch spec.json")
    src = Path(a.source)
    target = a.target or src.resolve().relative_to(ROOT).as_posix()
    folder = split(src, target, a.chunk, a.note)
    print(f"wrote {folder.relative_to(ROOT)} ({len(list(folder.glob('part-*')))} parts) -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
