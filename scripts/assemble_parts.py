#!/usr/bin/env python3
"""Reassemble large files uploaded as parts (deploy lane v3, 2026-09-17).

Why v3: a no-shell lane (Grok's connector, ChatGPT without a sandbox) cannot compute a sha256 or
count bytes, so the v2 manifest was unusable in practice -- zero uploads ever landed, 45 patchers
did, and every ~40 KB engine push turned into a stub. v3 asks only for what a model writes
reliably: numbered parts of whole lines, two marker lines that prove each part arrived intact,
and a manifest written LAST that says "complete". Exactness (sha256 / bytes) stays optional for
lanes with a shell (scripts/split_parts.py writes it).

Layout -- one folder per upload under aws/ops/patchers/parts/<upload-id>/:

  part-001, part-002, ...      whole lines, <= 12 KB each (larger bodies get truncated by the connector)
      @@PART 1/3@@             first line of every part: its number and the total
      ...the text...
      @@END 1@@                last line of every part: a missing @@END = the write was cut off
  manifest.json                written LAST, when every part is in the repo:
      {"target": "aws/lambdas/justhodl-x/source/lambda_function.py",
       "complete": true,
       "note": "what this is",
       "first_line": "import json",        # optional: first non-empty line of the whole file
       "last_line": "    return out",       # optional: last non-empty line of the whole file
       "shrink_ok": false}                   # true only for a deliberate rewrite that loses >50% of the bytes
  Optional exactness: "sha256", "bytes", "parts": <count>, "join": "bytes" (raw concatenation, no markers,
  no newline normalisation). A v2 manifest ("parts": [names], sha256/bytes) still works unchanged.

Related files use the v4 batch contract in scripts/assemble_batch.py.

Text targets are joined on line boundaries: each part ends with exactly one newline, CRLF becomes LF,
the file ends with a newline. Checks before anything is written: contiguous part numbers, marker
numbers/total, part size, first/last line, sha256/bytes when declared, the target compiles
(.py) / parses (.json) / passes `node --check` (.js) / is a whole document (.html), and a shrink
guard (new size < 50% of the existing file needs shrink_ok).

Outcomes, every run:
  incomplete  -> nothing written, nothing recorded (no manifest yet, complete != true, parts still arriving)
  assembled   -> target written, folder removed, receipt at aws/ops/patchers/parts/_receipts/<upload-id>.json
  rejected    -> nothing written, folder kept, <folder>/STATUS.json and the receipt say exactly why;
                 fix that one part (or the manifest) and push it again -- the lane re-runs by itself
The apply-lane workflow pushes STATUS/receipts back to main first, then goes red (PARTS_REJECTED).

Usage:
  python3 scripts/assemble_parts.py            # assemble every complete upload
  python3 scripts/assemble_parts.py --check    # report only, write nothing (no STATUS, no receipts)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARTS_DIR = Path("aws/ops/patchers/parts")
RECEIPTS = "_receipts"
# Exactly the trees the apply lane stages and knows how to deploy (lambdas, shared modules,
# workers, site pages/assets). Anything else must come through git.
ALLOWED_TARGET_PREFIXES = ("aws/lambdas/", "aws/shared/", "cloudflare/workers/", "assets/", "js/", "css/")
ALLOWED_ROOT_SUFFIXES = (".html", ".js", ".css")

PART_NAME = re.compile(r"^part-(\d{3,})$")
MARK_START = re.compile(r"^\s*@@PART\s+(\d+)\s*/\s*(\d+)\s*@@\s*$")
MARK_END = re.compile(r"^\s*@@END\s+(\d+)\s*@@\s*$")
MAX_PART_BYTES = 40_000        # the connector is known to truncate around here; refuse rather than trust
RECOMMENDED_PART_BYTES = 12_000
MIN_TARGET_BYTES = 64
SHRINK_FRACTION = 0.5
SHRINK_MIN_PREV = 1_000


class PartsError(RuntimeError):
    """A rejection reason. Never propagates out of assemble_all -- it becomes status 'rejected'."""


def _safe_target(target: str, root: Path) -> Path:
    if (not isinstance(target, str) or not target or "\\" in target or ":" in target
            or any(p in ("", ".", "..") for p in target.split("/"))):
        raise PartsError(f"unsafe target path: {target!r}")
    root_page = "/" not in target and target.endswith(ALLOWED_ROOT_SUFFIXES)
    if not (target.startswith(ALLOWED_TARGET_PREFIXES) or root_page):
        raise PartsError(f"target outside the allowed tree: {target}")
    if not (root / target).resolve().is_relative_to(root.resolve()):
        raise PartsError(f"target resolves outside the repository: {target}")
    return root / target


def _discover(folder: Path, manifest: dict):
    """Return (ordered part paths, total) or a dict status when the upload is still arriving."""
    if "complete" in manifest and manifest["complete"] is not True:
        return {"status": "incomplete", "reason": "manifest.complete is not true yet"}
    listed = manifest.get("parts")
    if isinstance(listed, list):                         # v2 contract: explicit order
        if not listed:
            raise PartsError("manifest lists no parts")
        if any(not isinstance(p, str) or not PART_NAME.fullmatch(p) for p in listed) or len(set(listed)) != len(listed):
            raise PartsError("parts must list unique part-NNN filenames inside this upload")
        if any(not (folder / p).resolve().is_relative_to(folder.resolve()) for p in listed):
            raise PartsError("a part resolves outside this upload")
        missing = [p for p in listed if not (folder / p).is_file()]
        if missing:
            return {"status": "incomplete", "missing": missing}
        return [folder / p for p in listed], len(listed)
    if manifest.get("complete") is not True:
        return {"status": "incomplete", "reason": "manifest.complete is not true yet"}
    found = {}
    for p in folder.iterdir():
        m = PART_NAME.match(p.name)
        if m and p.is_file():
            if int(m.group(1)) == 0 or int(m.group(1)) in found:
                raise PartsError(f"invalid or duplicate part number: {p.name}")
            if not p.resolve().is_relative_to(folder.resolve()):
                raise PartsError(f"{p.name} resolves outside this upload")
            found[int(m.group(1))] = p
    if not found:
        raise PartsError("no part-NNN files in the folder")
    total = max(found)
    gaps = [f"part-{n:03d}" for n in range(1, total + 1) if n not in found]
    if gaps:
        raise PartsError(f"part numbers are not contiguous -- missing {', '.join(gaps)} (present: {len(found)}, highest: {total}); "
                         f"upload the missing part(s), or renumber and re-push manifest.json")
    if isinstance(listed, int) and listed != total:
        raise PartsError(f"manifest says parts={listed} but {total} contiguous parts are present")
    return [found[n] for n in range(1, total + 1)], total


def _part_text(path: Path, index: int, total: int) -> str:
    raw = path.read_bytes()
    if len(raw) > MAX_PART_BYTES:
        raise PartsError(f"{path.name} is {len(raw):,} bytes -- over the {MAX_PART_BYTES // 1000} KB connector limit; "
                         f"split it into parts of <= {RECOMMENDED_PART_BYTES // 1000} KB")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PartsError(f"{path.name} is not UTF-8 text ({exc}); use \"join\": \"bytes\" with a sha256 for binary content") from None
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    if lines and lines[-1] == "":                    # trailing newline -> drop the empty tail element
        lines.pop()
    if not lines:
        raise PartsError(f"{path.name} is empty")
    start = MARK_START.match(lines[0])
    if not start:
        raise PartsError(f"{path.name} has no opening @@PART {index}/{total}@@ line -- "
                         f"re-upload the whole part with both @@PART and @@END markers")
    n, t = int(start.group(1)), int(start.group(2))
    if n != index:
        raise PartsError(f"{path.name} says @@PART {n}/{t}@@ but it is part {index}")
    if t != total:
        raise PartsError(f"{path.name} says @@PART {n}/{t}@@ but {total} parts are present -- fix the total or the folder")
    lines = lines[1:]
    # the END marker may be followed by blank lines only
    tail = len(lines)
    while tail > 0 and lines[tail - 1].strip() == "":
        tail -= 1
    end = MARK_END.match(lines[tail - 1]) if tail > 0 else None
    if end:
        if int(end.group(1)) != index:
            raise PartsError(f"{path.name} ends with @@END {end.group(1)}@@ but it is part {index}")
        lines = lines[:tail - 1]
    else:
        raise PartsError(f"{path.name} has @@PART {index}/{total}@@ but no @@END {index}@@ line -- the write was cut off "
                         f"({len(raw):,} bytes arrived); re-upload {path.name} whole")
    body = "\n".join(lines)
    if not body.strip():
        raise PartsError(f"{path.name} carries no content between its markers")
    return body + "\n"


def _language_check(target: str, data: bytes, manifest: dict) -> dict:
    info = {}
    if target.endswith(".py"):
        try:
            compile(data, target, "exec")
        except SyntaxError as exc:
            raise PartsError(f"assembled python does not compile ({exc})") from None
        info["check"] = "py_compile"
    elif target.endswith(".json"):
        try:
            json.loads(data.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise PartsError(f"assembled JSON does not parse ({exc})") from None
        info["check"] = "json"
    elif target.endswith((".js", ".mjs")):
        node = shutil.which("node")
        if node:
            with tempfile.NamedTemporaryFile("wb", suffix=Path(target).suffix, delete=False) as fh:
                fh.write(data)
            try:
                proc = subprocess.run([node, "--check", fh.name], capture_output=True, text=True, timeout=60)
            finally:
                os.unlink(fh.name)
            if proc.returncode != 0:
                raise PartsError(f"assembled JavaScript fails node --check: {(proc.stderr or proc.stdout).strip()[:300]}")
            info["check"] = "node --check"
        else:
            raise PartsError("node is required to validate JavaScript; install Node on the runner and retry")
    elif target.endswith(".html") and not manifest.get("fragment"):
        low = data.decode("utf-8", "replace").lower()
        if "<html" not in low or "</html>" not in low:
            raise PartsError("assembled HTML is not a whole document (no <html> ... </html>); set \"fragment\": true for a partial")
        if low.count("<script") != low.count("</script>"):
            raise PartsError(f"assembled HTML has {low.count('<script')} <script> but {low.count('</script>')} </script>")
        info["check"] = "html document"
    return info


def _edge_line(text: str, first: bool) -> str:
    lines = [ln for ln in text.split("\n") if ln.strip()]
    if not lines:
        return ""
    return (lines[0] if first else lines[-1]).strip()


def _check_base_sha(target_rel: str, data: bytes, manifest: dict, root: Path) -> None:
    target = root / target_rel
    old = target.read_bytes() if target.is_file() else None
    # Contents API GET returns this SHA already; a no-shell sender only copies it.
    # null explicitly means 'create only'. Absence retains the v3 contract.
    guard = "base_blob_sha" if "base_blob_sha" in manifest else "base_sha"
    if "base_blob_sha" in manifest and "base_sha" in manifest and manifest["base_blob_sha"] != manifest["base_sha"]:
        raise PartsError(f"{target_rel}: conflicting base_sha and base_blob_sha")
    if guard in manifest and old != data:
        actual = hashlib.sha1(b"blob " + str(len(old)).encode() + b"\0" + old).hexdigest() if old is not None else None
        expected = manifest[guard].lower() if isinstance(manifest[guard], str) else manifest[guard]
        if expected != actual:
            raise PartsError(f"{target_rel}: stale -- changed since it was read ({guard} mismatch); read main again and merge before retrying")


def _prepare_one(folder: Path, root: Path, manifest: dict):
    """Validate and return bytes without modifying any target."""
    target_rel = manifest.get("target")
    target = _safe_target(target_rel, root)
    found = _discover(folder, manifest)
    if isinstance(found, dict):
        return {"upload": folder.name, **found}
    paths, total = found
    mode = manifest.get("join") or "lines"
    if mode == "bytes":
        if not manifest.get("sha256") and manifest.get("bytes") is None:
            raise PartsError('raw "join": "bytes" requires sha256 or bytes for integrity; '
                             'use marked text parts with "join": "lines" when neither is available')
        for p in paths:
            if p.stat().st_size > MAX_PART_BYTES:
                raise PartsError(f"{p.name} is {p.stat().st_size:,} bytes -- over the {MAX_PART_BYTES // 1000} KB connector limit")
        data = b"".join(p.read_bytes() for p in paths)
        text = None
    elif mode == "lines":
        text = "".join(_part_text(p, i, total) for i, p in enumerate(paths, 1))
        data = text.encode("utf-8")
    else:
        raise PartsError(f"unknown join mode {mode!r} (use \"lines\" or \"bytes\")")
    if len(data) < MIN_TARGET_BYTES:
        raise PartsError(f"assembled file is only {len(data)} bytes -- that is a stub, not a source")
    digest = hashlib.sha256(data).hexdigest()
    declared_sha = (manifest.get("sha256") or "").lower()
    if declared_sha and digest != declared_sha:
        raise PartsError(f"sha256 mismatch -- assembled {digest[:12]} != declared {declared_sha[:12]} ({len(data)} bytes)")
    declared_bytes = manifest.get("bytes")
    if declared_bytes is not None and int(declared_bytes) != len(data):
        raise PartsError(f"size mismatch -- assembled {len(data)} != declared {declared_bytes}")
    if text is not None:
        for key, first in (("first_line", True), ("last_line", False)):
            want = manifest.get(key)
            if want:
                got = _edge_line(text, first)
                if got != str(want).strip():
                    raise PartsError(f"{key} mismatch -- assembled file {'starts' if first else 'ends'} with {got[:80]!r}, "
                                     f"manifest says {str(want).strip()[:80]!r}; a part is missing or out of order")
    info = _language_check(target_rel, data, manifest)
    old = target.read_bytes() if target.is_file() else None
    _check_base_sha(target_rel, data, manifest, root)
    if target.is_file():
        prev = target.stat().st_size
        if prev >= SHRINK_MIN_PREV and len(data) < prev * SHRINK_FRACTION and not manifest.get("shrink_ok"):
            raise PartsError(f"assembled file is {len(data):,} bytes, the existing target is {prev:,} -- lost more than half; "
                             f"a deliberate rewrite must say \"shrink_ok\": true in manifest.json")
    result = {"upload": folder.name, "status": "ready", "target": target_rel,
              "bytes": len(data), "sha256": digest, "parts": total, "join": mode, "changed": old != data, **info}
    return result, target, data


def _validate_twins(root: Path, prepared: list) -> None:
    """Check the proposed tree, including every existing bundle of a changed config."""
    proposed = {r["target"]: data for r, _, data in prepared}
    sources = {p.relative_to(root).as_posix() for p in (root / "aws/lambdas").glob("*/source/*.json")}
    sources.update(p for p in proposed if re.fullmatch(r"aws/lambdas/[^/]+/source/[^/]+\.json", p))
    for src in sorted(sources):
        twin = "config/" + Path(src).name
        if src not in proposed and twin not in proposed:
            continue
        if twin not in proposed and not (root / twin).is_file():
            continue
        left = proposed[src] if src in proposed else (root / src).read_bytes()
        right = proposed[twin] if twin in proposed else (root / twin).read_bytes()
        if left != right:
            raise PartsError(f"bundled config mismatch: {src} != {twin}; upload both byte-identically in one batch")


def _prepare_upload(folder: Path, root: Path):
    try:
        manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PartsError(f"manifest.json does not parse ({exc})") from None
    if not isinstance(manifest, dict):
        raise PartsError("manifest.json is not an object")
    if manifest.get("cancel") is True:
        return {"upload": folder.name, "status": "cancelled", "note": manifest.get("note")}, []
    prepared = _prepare_one(folder, root, manifest)
    if isinstance(prepared, dict):
        return prepared, []
    _validate_twins(root, [prepared])
    return prepared[0], [prepared]


def _write_targets(prepared: list) -> None:
    """Rollback a failed write; no commit can contain only part of a batch."""
    staged, backups = [], []
    try:
        for _, target, data in prepared:
            old = target.read_bytes() if target.is_file() else None
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.upload")
            staged.append((temporary, target, old))
            temporary.write_bytes(data)
        for temporary, target, old in staged:
            os.replace(temporary, target)
            backups.append((target, old))
    except Exception:
        for target, old in reversed(backups):
            if old is None:
                target.unlink(missing_ok=True)
            else:
                target.write_bytes(old)
        raise
    finally:
        for temporary, _, _ in staged:
            temporary.unlink(missing_ok=True)


def assemble_one(folder: Path, root: Path, write: bool = True) -> dict:
    result, prepared = _prepare_upload(folder, root)
    if write and result["status"] == "ready":
        _write_targets(prepared)
        result["status"] = "assembled"
        for member in result.get("files", []):
            member["status"] = "assembled"
    if write and result["status"] == "cancelled":
        shutil.rmtree(folder)
    return result


def _record(root: Path, folder: Path, result: dict) -> None:
    receipts = root / PARTS_DIR / RECEIPTS
    receipts.mkdir(parents=True, exist_ok=True)
    result = {**result, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    if os.environ.get("GITHUB_RUN_ID"):
        result.update(apply_run_id=os.environ["GITHUB_RUN_ID"], handoff_ref="ops-evidence",
                      handoff_path=f"aws/ops/reports/apply-lane/{os.environ['GITHUB_RUN_ID']}.json")
    (receipts / f"{folder.name}.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    if result["status"] == "rejected" and folder.is_dir():
        (folder / "STATUS.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    elif result["status"] in ("assembled", "cancelled") and folder.is_dir():
        shutil.rmtree(folder)


def assemble_all(root: Path = ROOT, write: bool = True) -> list[dict]:
    base = root / PARTS_DIR
    if not base.is_dir():
        return []
    plans, owners = [], {}
    for folder in sorted(p for p in base.iterdir() if p.is_dir() and p.name != RECEIPTS and (p / "manifest.json").is_file()):
        try:
            result, prepared = _prepare_upload(folder, root)
        except PartsError as exc:
            result = {"upload": folder.name, "status": "rejected", "reason": str(exc)}
        except Exception as exc:  # noqa: BLE001 -- an unexpected error is still a rejection with a reason, never a silent skip
            result = {"upload": folder.name, "status": "rejected", "reason": f"{type(exc).__name__}: {exc}"}
        if result["status"] != "ready":
            prepared = []
        plans.append((folder, result, prepared))
        for _, target, _ in prepared:
            owners.setdefault(target.resolve(), []).append(result)
    for target, uploads in owners.items():
        if len(uploads) > 1:
            for result in uploads:
                result.update(status="rejected", reason=f"overlapping uploads target {target.relative_to(root.resolve())}; combine related files into one batch or remove the superseded upload")
    results = []
    for folder, result, prepared in plans:
        if write and result["status"] == "ready":
            # I/O/rollback failures must stop the workflow before it can commit any tree.
            # Only validation failures are recoverable per-upload rejections.
            _write_targets(prepared)
            result["status"] = "assembled"
            for member in result.get("files", []):
                member["status"] = "assembled"
        if write and result["status"] in ("assembled", "rejected", "cancelled"):
            _record(root, folder, result)
        results.append(result)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="verify only; write nothing")
    args = parser.parse_args(argv)
    results = assemble_all(write=not args.check)
    if not results:
        print("assemble_parts: no uploads")
        return 0
    rejected = []
    for r in results:
        print("assemble_parts:", json.dumps(r, sort_keys=True))
        if r["status"] == "rejected":
            rejected.append(r["upload"])
            print(f"::warning::upload {r['upload']} rejected: {r.get('reason')}")
    env = os.environ.get("GITHUB_ENV")
    if rejected and env:
        with open(env, "a") as fh:
            fh.write("PARTS_REJECTED=" + " ".join(rejected) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
