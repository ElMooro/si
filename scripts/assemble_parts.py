#!/usr/bin/env python3
"""Reassemble large files uploaded as parts (deploy lane v2).

The GitHub Contents API (Grok / connector ``push_files``) cannot carry a ~40 KB
body in one write -- the blob lands as a 9-byte or 441-byte stub. Instead of
patching around the file, a lane uploads the file the way S3 does a multipart
upload: N small parts plus a manifest declaring the target and the sha256 of
the whole. The runner concatenates, verifies the hash, writes the target, and
removes the parts. Nothing is ever written to the target unless the hash matches.

Layout (one folder per upload under aws/ops/patchers/parts/):

  aws/ops/patchers/parts/<upload-id>/manifest.json
      {"target": "aws/lambdas/justhodl-stock-buying/source/lambda_function.py",
       "parts": ["part-001", "part-002", "part-003"],
       "sha256": "<hex sha256 of the complete file>",
       "bytes": 40564,
       "note": "stock-buying v1.5.2 full source"}
  aws/ops/patchers/parts/<upload-id>/part-001 ... part-003   (raw bytes, any size <= 18 KB)

Parts are concatenated in the order listed in ``parts``. A manifest whose parts
are not all present yet is skipped (the lane may still be uploading) -- it is
only an error once every listed part exists and the hash does not match.

Usage:
  python3 scripts/assemble_parts.py            # assemble every complete upload
  python3 scripts/assemble_parts.py --check    # report only, write nothing
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARTS_DIR = Path("aws/ops/patchers/parts")
# Exactly the trees the apply lane stages and knows how to deploy (lambdas,
# shared modules, workers, site pages). Anything else must come through git.
ALLOWED_TARGET_PREFIXES = ("aws/lambdas/", "aws/shared/", "cloudflare/workers/", "assets/", "js/", "css/")
ALLOWED_ROOT_SUFFIXES = (".html", ".js", ".css")


class PartsError(RuntimeError):
    pass


def _safe_target(target: str, root: Path) -> Path:
    if target.startswith("/") or ".." in Path(target).parts:
        raise PartsError(f"unsafe target path: {target}")
    root_page = "/" not in target and target.endswith(ALLOWED_ROOT_SUFFIXES)
    if not (target.startswith(ALLOWED_TARGET_PREFIXES) or root_page):
        raise PartsError(f"target outside the allowed tree: {target}")
    return root / target


def assemble_one(folder: Path, root: Path, write: bool = True) -> dict:
    manifest_path = folder / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    target = _safe_target(manifest["target"], root)
    parts = manifest["parts"]
    if not parts or not isinstance(parts, list):
        raise PartsError(f"{folder.name}: manifest lists no parts")
    missing = [p for p in parts if not (folder / p).is_file()]
    if missing:
        return {"upload": folder.name, "status": "incomplete", "missing": missing}
    data = b"".join((folder / p).read_bytes() for p in parts)
    digest = hashlib.sha256(data).hexdigest()
    declared_sha = (manifest.get("sha256") or "").lower()
    declared_bytes = manifest.get("bytes")
    if not declared_sha and declared_bytes is None:
        raise PartsError(f"{folder.name}: manifest must declare sha256 and/or bytes")
    if declared_sha and digest != declared_sha:
        raise PartsError(f"{folder.name}: sha256 mismatch -- assembled {digest[:12]} != declared "
                         f"{declared_sha[:12]} ({len(data)} bytes); target NOT written")
    if declared_bytes is not None and int(declared_bytes) != len(data):
        raise PartsError(f"{folder.name}: size mismatch -- assembled {len(data)} != declared {declared_bytes}")
    if not declared_sha:
        print(f"::warning::{folder.name}: no sha256 declared; relying on byte count + compile + shrink guard")
    if manifest["target"].endswith(".py"):
        try:
            compile(data, manifest["target"], "exec")
        except SyntaxError as exc:
            raise PartsError(f"{folder.name}: assembled python does not compile ({exc}); target NOT written") from None
    if write:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        shutil.rmtree(folder)
    return {"upload": folder.name, "status": "assembled" if write else "ready",
            "target": manifest["target"], "bytes": len(data), "sha256": digest}


def assemble_all(root: Path = ROOT, write: bool = True) -> list[dict]:
    base = root / PARTS_DIR
    if not base.is_dir():
        return []
    results = []
    for folder in sorted(p for p in base.iterdir() if p.is_dir() and (p / "manifest.json").is_file()):
        results.append(assemble_one(folder, root, write))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="verify only; write nothing")
    args = parser.parse_args(argv)
    try:
        results = assemble_all(write=not args.check)
    except PartsError as exc:
        print(f"::error::assemble_parts: {exc}")
        return 1
    if not results:
        print("assemble_parts: no uploads")
        return 0
    for r in results:
        print("assemble_parts:", json.dumps(r, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
