#!/usr/bin/env python3
"""
Step 126 — Loop 1 prep: inspect calibration shape + identify consumers.

Before writing the shared helper, we need to know exactly:
  1. What does /justhodl/calibration/weights look like in SSM?
     Shape: {signal_type: weight} or {signal_type: {horizon: weight}}?
     What's the data type — float, dict, JSON-encoded string?
  2. What does /justhodl/calibration/accuracy look like?
  3. What format does the calibrator Lambda WRITE? (find calibrator
     source, read its put_parameter call)
  4. Which consumer Lambdas would benefit? Find every Lambda that
     CONSTRUCTS a prediction by combining sub-signals — those are
     the ones that should weight inputs by historical accuracy.
     Likely list: justhodl-intelligence, justhodl-morning-intelligence,
     justhodl-edge-engine, justhodl-daily-report-v3.
  5. For each candidate, find the sub-signal aggregation point in
     the source. That's where weights need to multiply.

Output: a clear spec for what the shared helper needs to support
and where exactly to integrate it.
"""
import json
import os
import re
from collections import defaultdict
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

ssm = boto3.client("ssm", region_name=REGION)


with report("inspect_calibration_for_loop1") as r:
    r.heading("Loop 1 prep — calibration shape + consumer identification")

    # ─── 1. Read SSM calibration parameters ─────────────────────────────
    r.section("1. SSM calibration parameters")
    for key in ["/justhodl/calibration/weights",
                "/justhodl/calibration/accuracy",
                "/justhodl/calibration/report",
                "/justhodl/calibration/last_run"]:
        try:
            resp = ssm.get_parameter(Name=key, WithDecryption=False)
            val = resp["Parameter"]["Value"]
            r.log(f"\n  {key}")
            r.log(f"    Type: {resp['Parameter']['Type']}, length: {len(val)}B")
            try:
                parsed = json.loads(val)
                r.log(f"    Parsed type: {type(parsed).__name__}")
                if isinstance(parsed, dict):
                    r.log(f"    Top-level keys ({len(parsed)}): {sorted(parsed.keys())[:10]}")
                    # Show a sample entry
                    if parsed:
                        sample_key = sorted(parsed.keys())[0]
                        sample_val = parsed[sample_key]
                        r.log(f"    Sample entry: {sample_key!r} → {json.dumps(sample_val, default=str)[:200]}")
                elif isinstance(parsed, list):
                    r.log(f"    Length: {len(parsed)}")
                    if parsed:
                        r.log(f"    First entry: {json.dumps(parsed[0], default=str)[:200]}")
            except json.JSONDecodeError:
                r.log(f"    Raw value: {val[:200]}")
        except ssm.exceptions.ParameterNotFound:
            r.warn(f"  {key}: NOT FOUND")
        except Exception as e:
            r.fail(f"  {key}: {e}")

    # ─── 2. Read calibrator source — what does it actually write? ──────
    r.section("2. Calibrator source — what shape does it produce?")
    calib_dir = REPO_ROOT / "aws/lambdas/justhodl-calibrator/source"
    if calib_dir.exists():
        for src_file in calib_dir.rglob("*.py"):
            content = src_file.read_text(encoding="utf-8", errors="ignore")
            r.log(f"  {src_file.relative_to(REPO_ROOT)} ({content.count(chr(10))} LOC)")

            # Find every put_parameter call
            for m in re.finditer(
                r"ssm\.put_parameter\s*\(\s*Name\s*=\s*['\"](\S+)['\"]",
                content,
            ):
                r.log(f"    Writes to SSM: {m.group(1)}")

            # Find weight calculation logic
            for m in re.finditer(r"^(\s*)(weights|weight)\s*\[", content, re.MULTILINE):
                line_num = content[:m.start()].count("\n") + 1
                # Get the actual line
                lines = content.split("\n")
                if line_num <= len(lines):
                    r.log(f"    L{line_num}: {lines[line_num - 1].strip()[:120]}")
    else:
        r.warn(f"  Calibrator source not in repo at {calib_dir}")

    # ─── 3. Find consumer Lambdas (predictors) ─────────────────────────
    r.section("3. Consumer Lambda identification")
    candidates = [
        "justhodl-intelligence",
        "justhodl-morning-intelligence",
        "justhodl-edge-engine",
        "justhodl-daily-report-v3",
        "justhodl-investor-agents",
        "justhodl-signal-logger",      # READS predictions to log them
        "justhodl-outcome-checker",     # SCORES predictions
        "justhodl-calibrator",          # PRODUCES weights (don't patch — would create a cycle)
    ]

    lambdas_dir = REPO_ROOT / "aws/lambdas"
    consumer_findings = {}
    for name in candidates:
        src_dir = lambdas_dir / name / "source"
        if not src_dir.exists():
            continue
        details = {
            "lines_total": 0,
            "reads_calibration": False,
            "uses_signal_types": set(),
            "aggregation_hints": [],
        }
        for src_file in src_dir.rglob("*.py"):
            content = src_file.read_text(encoding="utf-8", errors="ignore")
            details["lines_total"] += content.count("\n")

            # Already reads calibration?
            if "/justhodl/calibration" in content:
                details["reads_calibration"] = True

            # What signal_types does it reference?
            for m in re.finditer(
                r"['\"]([a-z_]+)['\"][\s,)]",
                content,
            ):
                token = m.group(1)
                if token in {
                    "khalid_index", "edge_regime", "edge_composite", "carry_risk",
                    "ml_risk", "market_phase", "plumbing_stress", "crypto_fear_greed",
                    "crypto_risk_score", "screener_top_pick",
                    "momentum_uso", "momentum_gld", "momentum_spy", "momentum_tlt",
                    "momentum_uup",
                }:
                    details["uses_signal_types"].add(token)

            # Hints of aggregation/scoring (where calibration would matter)
            for m in re.finditer(
                r"^\s*(.*(?:score|composite|aggregate|combined|weighted|blend).*=.*)$",
                content,
                re.MULTILINE | re.IGNORECASE,
            ):
                line = m.group(1).strip()
                if 20 < len(line) < 140 and not line.startswith("#"):
                    details["aggregation_hints"].append(line[:120])

        if details["lines_total"] > 0:
            consumer_findings[name] = details

    for name, d in consumer_findings.items():
        r.log(f"\n  {name} ({d['lines_total']} LOC)")
        r.log(f"    Already reads calibration: {d['reads_calibration']}")
        r.log(f"    Signal types referenced: {sorted(d['uses_signal_types'])}")
        if d["aggregation_hints"]:
            r.log(f"    Aggregation hints (top 3):")
            for hint in d["aggregation_hints"][:3]:
                r.log(f"      • {hint}")

    # ─── 4. Decision matrix ──────────────────────────────────────────────
    r.section("4. Patch priority")
    r.log("  Priority order based on findings:")
    r.log("    1. justhodl-intelligence — produces ML risk score (5/5 critical)")
    r.log("    2. justhodl-morning-intelligence — composes daily brief")
    r.log("    3. justhodl-edge-engine — produces edge composite")
    r.log("    4. justhodl-daily-report-v3 — produces khalid_index itself")
    r.log("       (special case — its OUTPUT is calibrated, not its inputs)")
    r.log("\n  Skip:")
    r.log("    - signal-logger (records, doesn't predict)")
    r.log("    - outcome-checker (scores, doesn't predict)")
    r.log("    - calibrator (produces weights, would create cycle)")

    r.log("Done")
