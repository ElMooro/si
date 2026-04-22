#!/usr/bin/env python3
"""
Phase 3b discovery — map every consumer of data.json.

Scans:
  - All .py files under aws/lambdas/
  - All .html and .js files in the repo root (justhodl.ai pages)

For each file that appears to read data.json, extracts:
  - How it gets the data (boto3.client('s3').get_object / urllib / fetch)
  - What keys/dotted paths it reads (khalid_index.score, market_regime.label, etc.)
  - What placeholder/default values it falls back to when keys are missing
    (this is what produces [REGIME] / [SCORE] in the chat output)

Also: downloads the current LIVE data.json from S3 (justhodl-dashboard-live)
and records its top-level shape for comparison.

Produces a schema report showing:
  1. What the producer (justhodl-daily-report-v3) writes
  2. What each consumer expects to read
  3. Every mismatch (consumer looks for key X that producer doesn't write)
  4. Every placeholder string that could be visible to users

NO CHANGES. Read-only analysis. Output goes to
aws/ops/reports/latest/data_json_schema.md plus a structured .json.
"""

import json
import os
import re
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

from ops_report import report
import boto3

REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY    = "data.json"
S3_URL    = f"https://{S3_BUCKET}.s3.amazonaws.com/{S3_KEY}"

# Patterns that identify "this file reads data.json"
READS_DATA_JSON = [
    re.compile(r"['\"]data\.json['\"]"),
    re.compile(r"data\.json"),
]

# Patterns for key access. These are heuristics — they'll have false positives
# in library code, but the producer/consumer map is still useful.
ACCESS_PATTERNS = [
    # report['khalid_index']['score']   or   report.get('khalid_index', {}).get('score')
    re.compile(r"(?:report|data|d|intel|resp|cfg)\[\s*['\"]([a-z_][a-z0-9_]*)['\"]\s*\]"),
    re.compile(r"\.get\(\s*['\"]([a-z_][a-z0-9_]*)['\"]"),
    # Dotted: obj.key
    re.compile(r"\b(?:report|data|intel)\.([a-z_][a-z0-9_]{2,})\b"),
]

# Placeholders the user sees when values are missing (these are the bug)
PLACEHOLDER_PATTERNS = [
    re.compile(r"\[(REGIME|SCORE|KHALID_INDEX|VIX|REGIME_LABEL|STATUS|PRICE|DATA)\]"),
    re.compile(r"['\"](\[[A-Z_]+\])['\"]"),
]

# Heuristic: the "main" key a consumer reads from data.json will be in the
# code near a fetch of data.json. Extract a window of ~40 lines around it.
WINDOW_LINES = 50


def scan_python_file(path: Path):
    """Return dict of findings or None if file doesn't reference data.json."""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

    if not any(p.search(text) for p in READS_DATA_JSON):
        return None

    findings = {
        "file": str(path.relative_to(REPO_ROOT)),
        "keys_read": set(),
        "placeholders": set(),
        "fetch_mechanism": None,
    }

    if "boto3" in text and "get_object" in text:
        findings["fetch_mechanism"] = "s3.get_object (boto3)"
    elif "urllib.request.urlopen" in text:
        findings["fetch_mechanism"] = "urllib"
    elif "requests.get" in text:
        findings["fetch_mechanism"] = "requests"
    else:
        findings["fetch_mechanism"] = "unknown"

    # Extract window around data.json mention for scoped key scanning
    lines = text.splitlines()
    hit_lines = [i for i, line in enumerate(lines) if "data.json" in line]

    # If file is small, scan all of it. Otherwise scan windows.
    if len(lines) < 200 or not hit_lines:
        scan_text = text
    else:
        windows = []
        for hit in hit_lines:
            lo = max(0, hit - WINDOW_LINES)
            hi = min(len(lines), hit + WINDOW_LINES)
            windows.append("\n".join(lines[lo:hi]))
        scan_text = "\n".join(windows)

    for pattern in ACCESS_PATTERNS:
        for m in pattern.finditer(scan_text):
            k = m.group(1)
            if k in ("get", "json", "body", "Body", "Key", "Bucket", "read", "decode", "encode", "loads"):
                continue
            findings["keys_read"].add(k)

    for pattern in PLACEHOLDER_PATTERNS:
        for m in pattern.finditer(text):
            findings["placeholders"].add(m.group(0))

    return findings


def scan_html_file(path: Path):
    """HTML/JS files — pattern is usually fetch('.../data.json').then(r => r.json()).then(d => d.foo)"""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

    if "data.json" not in text and "report.json" not in text:
        return None

    findings = {
        "file": str(path.relative_to(REPO_ROOT)),
        "keys_read": set(),
        "placeholders": set(),
        "fetch_mechanism": "fetch()",
    }

    # JS access patterns: d.foo, data.foo, report.foo, d['foo']
    js_patterns = [
        re.compile(r"\b(?:d|data|report|intel|json)\.([a-zA-Z_][a-zA-Z0-9_]{2,})\b"),
        re.compile(r"\b(?:d|data|report|intel|json)\[\s*['\"]([a-zA-Z_][a-zA-Z0-9_]*)['\"]\s*\]"),
    ]

    lines = text.splitlines()
    hit_lines = [i for i, line in enumerate(lines) if "data.json" in line]
    if not hit_lines:
        return None

    # Window around each hit
    for hit in hit_lines:
        lo = max(0, hit - 30)
        hi = min(len(lines), hit + 80)
        window = "\n".join(lines[lo:hi])
        for pattern in js_patterns:
            for m in pattern.finditer(window):
                k = m.group(1)
                if k in ("json", "then", "catch", "error", "status", "ok", "data", "length", "toFixed", "log", "error"):
                    continue
                findings["keys_read"].add(k)

    # Placeholders anywhere in the file
    for pattern in PLACEHOLDER_PATTERNS:
        for m in pattern.finditer(text):
            findings["placeholders"].add(m.group(0))

    return findings


def fetch_live_data_json(r):
    """Download the current data.json from S3 and return its top-level shape."""
    try:
        with urllib.request.urlopen(S3_URL, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
        data = json.loads(raw)
    except Exception as e:
        r.fail(f"Couldn't fetch live data.json: {e}")
        return None

    top_keys = sorted(data.keys()) if isinstance(data, dict) else []
    r.ok(f"Live data.json has {len(top_keys)} top-level keys")
    shape = {}
    for k in top_keys:
        v = data[k]
        if isinstance(v, dict):
            shape[k] = {"type": "dict", "subkeys": sorted(v.keys())[:10]}
        elif isinstance(v, list):
            shape[k] = {"type": "list", "length": len(v)}
        elif isinstance(v, (int, float)):
            shape[k] = {"type": "number", "example": v}
        elif isinstance(v, str):
            preview = v[:40] + ("…" if len(v) > 40 else "")
            shape[k] = {"type": "string", "example": preview}
        elif v is None:
            shape[k] = {"type": "null"}
        else:
            shape[k] = {"type": type(v).__name__}
    return shape


with report("data_json_schema") as r:
    r.heading("data.json schema audit")
    r.log(f"Repo root: {REPO_ROOT}")

    # 1. Snapshot what the producer actually writes
    r.section("Live data.json shape (from S3)")
    live_shape = fetch_live_data_json(r)
    if live_shape:
        for k in sorted(live_shape.keys()):
            info = live_shape[k]
            if info["type"] == "dict":
                r.log(f"  `{k}` → dict with subkeys: {info['subkeys']}")
            elif info["type"] == "list":
                r.log(f"  `{k}` → list (length {info['length']})")
            else:
                r.log(f"  `{k}` → {info['type']} (e.g., {info.get('example', '—')})")

    # 2. Scan every consumer
    r.section("Scanning consumers")
    all_findings = []

    py_files = list((REPO_ROOT / "aws" / "lambdas").rglob("*.py"))
    r.log(f"Python files to scan: {len(py_files)}")
    for p in py_files:
        f = scan_python_file(p)
        if f:
            all_findings.append(f)

    html_files = list(REPO_ROOT.glob("*.html")) + list(REPO_ROOT.glob("*.js"))
    r.log(f"HTML/JS files to scan: {len(html_files)}")
    for p in html_files:
        f = scan_html_file(p)
        if f:
            all_findings.append(f)

    r.ok(f"Found {len(all_findings)} files that read data.json")

    # 3. Build per-file table
    r.section("Consumer summary")
    for f in sorted(all_findings, key=lambda x: x["file"]):
        keys = sorted(f["keys_read"])
        placeholders = sorted(f["placeholders"])
        r.kv(
            file=f["file"],
            fetch=f["fetch_mechanism"],
            keys_read=", ".join(keys[:8]) + ("…" if len(keys) > 8 else ""),
            num_keys=len(keys),
            placeholders=", ".join(placeholders) if placeholders else "—",
        )

    # 4. Build union of all keys read vs live keys
    r.section("Key usage vs. live shape")
    all_keys_read = set()
    for f in all_findings:
        all_keys_read |= f["keys_read"]

    live_keys = set(live_shape.keys()) if live_shape else set()

    only_in_consumers = all_keys_read - live_keys
    only_in_producer  = live_keys - all_keys_read

    r.log(f"Keys consumers look for: {len(all_keys_read)}")
    r.log(f"Keys producer writes:    {len(live_keys)}")

    # Filter out obvious false positives (common Python/JS idioms)
    false_pos = {
        "append", "items", "keys", "values", "splitlines", "strip", "split",
        "lower", "upper", "replace", "format", "encode", "decode", "join",
        "read", "write", "close", "get", "post", "put", "delete", "head",
        "json", "body", "text", "status", "ok", "response",
    }
    suspect_consumer_keys = sorted(only_in_consumers - false_pos)
    orphan_producer_keys  = sorted(only_in_producer)

    r.section("⚠ Consumer expects keys that don't exist in live data.json")
    if suspect_consumer_keys:
        for k in suspect_consumer_keys[:40]:
            # Which files reference this key?
            files_using = [f["file"] for f in all_findings if k in f["keys_read"]]
            r.log(f"  `{k}` — read by: {', '.join(files_using[:3])}"
                  f"{' (+' + str(len(files_using) - 3) + ')' if len(files_using) > 3 else ''}")
        if len(suspect_consumer_keys) > 40:
            r.log(f"  …and {len(suspect_consumer_keys) - 40} more")
    else:
        r.log("  (none — all consumer keys exist in producer output)")

    r.section("ℹ Producer writes keys nobody reads")
    if orphan_producer_keys:
        for k in orphan_producer_keys[:20]:
            r.log(f"  `{k}`")
    else:
        r.log("  (none — all producer keys are consumed)")

    # 5. Placeholder sightings
    r.section("Placeholders found in consumer code")
    files_with_placeholders = [f for f in all_findings if f["placeholders"]]
    if files_with_placeholders:
        r.log("These files contain literal placeholder strings that appear in user-facing output when a key is missing:")
        for f in files_with_placeholders:
            r.log(f"  - `{f['file']}`: {', '.join(sorted(f['placeholders']))}")
    else:
        r.log("  (no literal placeholders found)")

    # 6. Dump full machine-readable data alongside the markdown
    json_out = REPO_ROOT / "aws" / "ops" / "reports" / "latest" / "data_json_schema.json"
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps({
        "live_shape": live_shape,
        "consumers": [
            {
                "file": f["file"],
                "fetch_mechanism": f["fetch_mechanism"],
                "keys_read": sorted(f["keys_read"]),
                "placeholders": sorted(f["placeholders"]),
            }
            for f in all_findings
        ],
        "suspect_consumer_keys": suspect_consumer_keys,
        "orphan_producer_keys": orphan_producer_keys,
    }, indent=2))
    r.log(f"Full data: {json_out.relative_to(REPO_ROOT)}")

    r.log("Done")
