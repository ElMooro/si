#!/usr/bin/env python3
"""Batch upload: several related files land together, or not at all (deploy lane v4, 2026-09-17).

The gap this closes: a no-shell lane writes one file per request, and every write to
aws/lambdas/** deploys immediately -- so an engine could go live before its helper module or
config.json arrived, or a helper before the engine that imports it. A batch is a staging folder
that deploys nothing until its manifest says "complete"; the runner then checks every file,
writes ALL targets into the working tree, and the apply lane commits them as ONE commit and
dispatches ONE pinned deploy for every function touched.

Layout -- one folder per batch under aws/ops/patchers/batch/<batch-id>/  (id: lowercase, 8+ chars,
[a-z0-9._-]; recommended  <lane>-<YYYYMMDDTHHMMSSZ>-<slug>  so two lanes can never pick the same one):

  files/<target path>                      a file written whole (<= 10 KB is safe), e.g.
      files/aws/lambdas/justhodl-x/config.json
      files/aws/lambdas/justhodl-x/source/fmp_client.py
  parts/<target path>/part-001 ...         a large file in v3 parts (whole lines, @@PART n/N@@ first,
      parts/aws/lambdas/justhodl-x/source/lambda_function.py/part-001    @@END n@@ last, <= 12 KB each)
  manifest.json                            written LAST -- the go signal:
      {"complete": true,
       "lane": "grok",
       "note": "stock-buying v1.6: engine + FMP client + config",
       "files": [                                        # recommended: exactly what the batch holds
         {"target": "aws/lambdas/justhodl-x/source/lambda_function.py", "first_line": "import json", "last_line": "    return out"},
         {"target": "aws/lambdas/justhodl-x/source/fmp_client.py"},
         {"target": "aws/lambdas/justhodl-x/config.json"}
       ],
       "shrink_ok": false}                              # batch default; a file entry may override it

Per-file options (in its "files" entry): first_line, last_line, shrink_ok, fragment (HTML partial),
sha256, join ("lines" default for parts | "bytes"). Targets allowed: aws/lambdas/, aws/shared/,
config/, schemas/, docs/, cloudflare/workers/, assets/, js/, css/, root *.html|*.js|*.css, and
aws/ops/pending/ (an ops script that gates the release runs after the deploy is dispatched).
Never workflows, never secrets.

Outcomes, every run (receipt at aws/ops/patchers/batch/_receipts/<batch-id>.json):
  incomplete  -> no manifest yet, or complete != true: nothing happens, nothing recorded
  assembled   -> every target written, folder removed; the receipt lists each file (bytes, sha256,
                 check), the functions that will be deployed, pages/workers/ops touched
  rejected    -> NOTHING written (all-or-nothing); <folder>/STATUS.json + the receipt list EVERY
                 problem across all files, so one more push fixes them all; the run then goes red
A batch id that already has a receipt is refused (upload-name collision protection).

Usage (apply-staged-large-files.yml):  python3 scripts/assemble_batch.py [--check]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import runpy
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BATCH_DIR = Path("aws/ops/patchers/batch")
RECEIPTS = "_receipts"
PARTS = runpy.run_path(str(ROOT / "scripts/assemble_parts.py"))      # v3 helpers: markers, checks, constants
PartsError = PARTS["PartsError"]
ALLOWED_PREFIXES = PARTS["ALLOWED_TARGET_PREFIXES"] + ("config/", "schemas/", "docs/", "aws/ops/pending/")
ALLOWED_ROOT_SUFFIXES = PARTS["ALLOWED_ROOT_SUFFIXES"]
TEXT_SUFFIXES = (".py", ".js", ".mjs", ".json", ".html", ".css", ".md", ".txt", ".yml", ".yaml", ".toml", ".csv")
ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{7,79}$")
PART_NAME = PARTS["PART_NAME"]
MIN_SOURCE_BYTES = PARTS["MIN_TARGET_BYTES"]


def _safe_target(target: str) -> str:
    if not isinstance(target, str) or not target or target.startswith("/") or ".." in Path(target).parts:
        raise PartsError(f"unsafe target path: {target!r}")
    root_page = "/" not in target and target.endswith(ALLOWED_ROOT_SUFFIXES)
    if not (target.startswith(ALLOWED_PREFIXES) or root_page):
        raise PartsError(f"target outside the allowed trees: {target}")
    if target.startswith(".github/") or "/.github/" in target:
        raise PartsError(f"workflow files never come through a batch: {target}")
    return target


def _present(folder: Path) -> dict[str, tuple[str, Path]]:
    """target -> ("whole", file) | ("parts", folder). A target given both ways is a conflict."""
    found: dict[str, tuple[str, Path]] = {}
    files_root, parts_root = folder / "files", folder / "parts"
    if files_root.is_dir():
        for p in sorted(files_root.rglob("*")):
            if p.is_file():
                found[p.relative_to(files_root).as_posix()] = ("whole", p)
    if parts_root.is_dir():
        for p in sorted(parts_root.rglob("part-*")):
            if p.is_file() and PART_NAME.match(p.name):
                target = p.parent.relative_to(parts_root).as_posix()
                if target in found and found[target][0] == "whole":
                    raise PartsError(f"{target} is present both whole (files/) and in parts (parts/) -- keep one")
                found[target] = ("parts", p.parent)
    return found


def _whole_bytes(path: Path, target: str) -> bytes:
    raw = path.read_bytes()
    if len(raw) > PARTS["MAX_PART_BYTES"]:
        raise PartsError(f"{target}: {len(raw):,} bytes written whole -- over the connector limit; upload it in parts/")
    if target.endswith(TEXT_SUFFIXES):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PartsError(f"{target}: not UTF-8 text ({exc})") from None
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        if text and not text.endswith("\n"):
            text += "\n"
        raw = text.encode("utf-8")
    return raw


def _parts_bytes(folder: Path, target: str, entry: dict) -> bytes:
    names = {}
    for p in folder.iterdir():
        m = PART_NAME.match(p.name)
        if m and p.is_file():
            names[int(m.group(1))] = p
    total = max(names)
    gaps = [f"part-{n:03d}" for n in range(1, total + 1) if n not in names]
    if gaps:
        raise PartsError(f"{target}: parts not contiguous -- missing {', '.join(gaps)}")
    ordered = [names[n] for n in range(1, total + 1)]
    if (entry.get("join") or "lines") == "bytes":
        return b"".join(p.read_bytes() for p in ordered)
    try:
        text = "".join(PARTS["_part_text"](p, i, total) for i, p in enumerate(ordered, 1))
    except PartsError as exc:
        raise PartsError(f"{target}: {exc}") from None
    return text.encode("utf-8")


def _check_target(target: str, data: bytes, entry: dict, batch: dict, root: Path) -> dict:
    """Every check for one assembled file; raises PartsError with the target in the message."""
    # the connector's known failure is a 9-441 byte lambda_function.py / page stub; a small helper module is legitimate
    if (target.endswith("/lambda_function.py") or target.endswith(".html")) and len(data) < MIN_SOURCE_BYTES:
        raise PartsError(f"{target}: only {len(data)} bytes -- a stub, not a source")
    if not data.strip():
        raise PartsError(f"{target}: empty file")
    digest = hashlib.sha256(data).hexdigest()
    want = (entry.get("sha256") or "").lower()
    if want and want != digest:
        raise PartsError(f"{target}: sha256 mismatch (assembled {digest[:12]} != declared {want[:12]})")
    if target.endswith(TEXT_SUFFIXES):
        text = data.decode("utf-8", "replace")
        for key, first in (("first_line", True), ("last_line", False)):
            expect = entry.get(key)
            if expect:
                got = PARTS["_edge_line"](text, first)
                if got != str(expect).strip():
                    raise PartsError(f"{target}: {key} mismatch -- file {'starts' if first else 'ends'} with {got[:80]!r}, "
                                     f"manifest says {str(expect).strip()[:80]!r}")
    try:
        info = PARTS["_language_check"](target, data, entry)
    except PartsError as exc:
        raise PartsError(f"{target}: {exc}") from None
    existing = root / target
    if existing.is_file():
        prev = existing.stat().st_size
        shrink_ok = entry.get("shrink_ok", batch.get("shrink_ok", False))
        if prev >= PARTS["SHRINK_MIN_PREV"] and len(data) < prev * PARTS["SHRINK_FRACTION"] and not shrink_ok:
            raise PartsError(f"{target}: {len(data):,} bytes vs {prev:,} existing -- lost more than half; set \"shrink_ok\": true for a deliberate rewrite")
    return {"target": target, "bytes": len(data), "sha256": digest, **info}


def _classify(targets: list[str]) -> dict:
    functions = sorted({t.split("/")[2] for t in targets if t.startswith("aws/lambdas/") and t.count("/") >= 3})
    return {"functions": functions,
            "shared": sorted(t for t in targets if t.startswith("aws/shared/")),
            "config": sorted(t for t in targets if t.startswith(("config/", "schemas/"))),
            "workers": sorted({t.split("/")[2] for t in targets if t.startswith("cloudflare/workers/") and t.count("/") >= 3}),
            "pages": sorted(t for t in targets if t.startswith(("assets/", "js/", "css/")) or ("/" not in t and t.endswith(ALLOWED_ROOT_SUFFIXES))),
            "ops_scripts": sorted(t for t in targets if t.startswith("aws/ops/pending/")),
            "docs": sorted(t for t in targets if t.startswith("docs/"))}


def _blob_sha(data: bytes) -> str:
    """git's blob id for these bytes -- what the Contents API returns as `sha` on GET."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def _twin_paths(target: str, root: Path, assembled: dict[str, bytes]) -> list[str]:
    """Every other path that must carry identical bytes -- the same rule as the deploy gate
    (tests/deployment/test_bundled_config_identity.py): config/<name> <-> every aws/lambdas/*/source/<name>."""
    name = target.rsplit("/", 1)[-1]
    is_config = target.startswith("config/") and target.count("/") == 1
    is_bundled = target.startswith("aws/lambdas/") and "/source/" in target
    if not (is_config or is_bundled):
        return []
    config_twin = f"config/{name}"
    has_config = config_twin in assembled or (root / "config" / name).is_file()
    if not has_config:
        return []
    twins = {config_twin}
    twins.update(str(p.relative_to(root)) for p in (root / "aws" / "lambdas").glob(f"*/source/{name}"))
    twins.update(t for t in assembled if t.startswith("aws/lambdas/") and t.endswith("/source/" + name))
    twins.discard(target)
    return sorted(twins)


def _twin_problems(assembled: dict[str, bytes], root: Path) -> list[str]:
    problems = []
    for target, data in assembled.items():
        if not target.endswith(".json"):
            continue
        for twin in _twin_paths(target, root, assembled):
            other = assembled.get(twin)
            if other is None and (root / twin).is_file():
                other = (root / twin).read_bytes()
            if other is not None and other != data:
                problems.append(f"{target} must be byte-identical to its twin {twin} -- put both copies in the batch with the same content"
                                + ("" if twin in assembled else " (the twin on main differs; the deploy gate would refuse this)"))
    return sorted(set(problems))


def assemble_batch(folder: Path, root: Path, write: bool, receipted: set[str], claimed: dict[str, str] | None = None) -> dict:
    manifest_path = folder / "manifest.json"
    if not manifest_path.is_file():
        return {"batch": folder.name, "status": "incomplete", "reason": "no manifest.json yet"}
    try:
        manifest = json.loads(manifest_path.read_text())
        if not isinstance(manifest, dict):
            raise PartsError("manifest.json is not an object")
    except json.JSONDecodeError as exc:
        raise PartsError(f"manifest.json does not parse ({exc})") from None
    if manifest.get("complete") is not True:
        return {"batch": folder.name, "status": "incomplete", "reason": "manifest.complete is not true yet"}
    if not ID_RE.match(folder.name):
        raise PartsError(f"batch id {folder.name!r} is invalid -- lowercase [a-z0-9._-], 8-80 chars, e.g. grok-20260917T150000Z-stock-buying")
    if folder.name in receipted:
        raise PartsError(f"batch id {folder.name!r} was already used (receipt exists) -- pick a new id; names are never reused")
    present = _present(folder)
    entries: dict[str, dict] = {}
    listed = manifest.get("files")
    if listed is not None:
        if not isinstance(listed, list) or not all(isinstance(e, dict) and e.get("target") for e in listed):
            raise PartsError("manifest.files must be a list of {\"target\": ...} objects")
        entries = {e["target"]: e for e in listed}
        missing = sorted(set(entries) - set(present))
        extra = sorted(set(present) - set(entries))
        problems = []
        if missing:
            problems.append("listed in manifest.files but not uploaded: " + ", ".join(missing))
        if extra:
            problems.append("uploaded but not in manifest.files (typo? forgotten entry?): " + ", ".join(extra))
        if problems:
            raise PartsError("; ".join(problems))
    else:
        entries = {t: {"target": t} for t in present}
    if not entries:
        raise PartsError("the batch holds no files (nothing under files/ or parts/)")
    assembled: dict[str, bytes] = {}
    results, problems = [], []
    for target in sorted(entries):
        entry = entries[target]
        try:
            _safe_target(target)
            kind, path = present[target]
            data = _whole_bytes(path, target) if kind == "whole" else _parts_bytes(path, target, entry)
            info = _check_target(target, data, entry, manifest, root)
            base = (entry.get("base_sha") or "").lower()          # the `sha` the lane got when it GET the file it edited
            if base:
                existing = root / target
                live = _blob_sha(existing.read_bytes()) if existing.is_file() else ""
                if live != base:
                    raise PartsError(f"{target}: stale -- edited from blob {base[:10]} but main now has {live[:10] or 'no file'}; "
                                     f"GET it again, re-apply your change, re-upload")
                info["base_sha"] = base
            results.append({**info, "how": kind})
            assembled[target] = data
        except PartsError as exc:
            problems.append(str(exc))
    problems += _twin_problems(assembled, root)
    if claimed is not None:
        for target in assembled:
            if target in claimed:
                problems.append(f"{target} is also written by batch {claimed[target]} in this same run -- two batches must not "
                                f"overlap; merge them or push the second after the first lands")
    if problems:
        raise PartsError(" | ".join(problems))
    if claimed is not None:
        claimed.update({t: folder.name for t in assembled})
    summary = _classify(list(assembled))
    result = {"batch": folder.name, "status": "assembled" if write else "ready", "lane": manifest.get("lane"),
              "note": manifest.get("note"), "files": results, **summary,
              "apply_run_id": os.environ.get("GITHUB_RUN_ID"),
              "means": "files written and committed by the apply lane; NOT proof of a deploy",
              "verify": [f"https://justhodl.ai/data/ops/releases/{fn}.json  (commit must equal the apply-lane commit)" for fn in summary["functions"]]
                        + (["pages.yml run for the apply-lane commit"] if summary["pages"] else [])
                        + ([f"deploy-workers.yml run for {w}" for w in summary["workers"]])
                        + ([f"aws/ops/reports/latest/<N>_<slug>.md for {o}" for o in summary["ops_scripts"]])}
    if write:
        for target, data in assembled.items():           # all-or-nothing: every check passed before the first write
            dest = root / target
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
        shutil.rmtree(folder)
    return result


def _record(root: Path, folder: Path, result: dict) -> None:
    receipts = root / BATCH_DIR / RECEIPTS
    receipts.mkdir(parents=True, exist_ok=True)
    result = {**result, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (receipts / f"{folder.name}.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    if result["status"] == "rejected" and folder.is_dir():
        (folder / "STATUS.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")


def assemble_all(root: Path = ROOT, write: bool = True) -> list[dict]:
    base = root / BATCH_DIR
    if not base.is_dir():
        return []
    receipted = set()
    if (base / RECEIPTS).is_dir():
        for p in (base / RECEIPTS).glob("*.json"):
            try:
                if json.loads(p.read_text()).get("status") == "assembled":   # a rejected batch is repaired in place, same id
                    receipted.add(p.stem)
            except Exception:  # noqa: BLE001
                receipted.add(p.stem)
    claimed: dict[str, str] = {}                                              # target -> batch id, within this run
    results = []
    for folder in sorted(p for p in base.iterdir() if p.is_dir() and p.name != RECEIPTS):
        try:
            result = assemble_batch(folder, root, write, receipted, claimed)
        except PartsError as exc:
            result = {"batch": folder.name, "status": "rejected", "reason": str(exc)}
        except Exception as exc:  # noqa: BLE001 -- never a silent skip
            result = {"batch": folder.name, "status": "rejected", "reason": f"{type(exc).__name__}: {exc}"}
        if write and result["status"] in ("assembled", "rejected"):
            _record(root, folder, result)
        results.append(result)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="verify only; write nothing")
    args = parser.parse_args(argv)
    results = assemble_all(write=not args.check)
    if not results:
        print("assemble_batch: no batches")
        return 0
    rejected, ops = [], []
    for r in results:
        print("assemble_batch:", json.dumps({k: v for k, v in r.items() if k != "files"}, sort_keys=True))
        for f in r.get("files", []):
            print("   ", f["how"], f["target"], f["bytes"], "bytes", f.get("check", ""))
        if r["status"] == "rejected":
            rejected.append(r["batch"])
            print(f"::warning::batch {r['batch']} rejected: {r.get('reason')}")
        ops.extend(r.get("ops_scripts", []))
    env = os.environ.get("GITHUB_ENV")
    if env:
        # the parts step may already have set PARTS_REJECTED in this job: append, never overwrite
        rejected = os.environ.get("PARTS_REJECTED", "").split() + rejected
        with open(env, "a") as fh:
            if rejected:
                fh.write("PARTS_REJECTED=" + " ".join(rejected) + "\n")
            if ops:
                fh.write("BATCH_OPS=" + " ".join(ops) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
