#!/usr/bin/env python3
"""Offline audit regression gate and checker for explicitly supplied captured artifacts.
No AWS clients, deployments, invocations, providers, notifications or live network.
Examples:
  python aws/ops/checks/audit_20260909_risk.py
  python aws/ops/checks/audit_20260909_risk.py --skip-tests --artifact katlin=/tmp/katlin.json
"""
import argparse
import importlib.util
import json
import re
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/shared"))
from capital_contract import authority_view, publication_summary  # noqa:E402


def run_tests():
    commands = [[sys.executable, str(ROOT / f"aws/lambdas/justhodl-{engine}/tests/run_tests.py")]
                for engine in ("katlin", "risk-sizer", "risk-gate", "engine-fusion", "khalid-risk", "tradingview")]
    commands.append(["node", "--test", "tests/capital-view.test.js", "tests/frontend-freshness.test.js", "tests/khalidrisk-renderer.test.js", "tests/risk-gate-renderer.test.js"])
    results = []
    for command in commands:
        proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=120)
        record = {"test": str(Path(command[1]).relative_to(ROOT)) if command[0] == sys.executable else "capital and risk UI tests", "passed": proc.returncode == 0}
        lines = (proc.stdout + proc.stderr).strip().splitlines()
        record["summary"] = lines[-1] if lines else "no output"
        count = re.search(r"(?:tests passed:|tests)\s+(\d+)", proc.stdout)
        if count: record["test_count"] = int(count.group(1))
        if proc.returncode:
            record["failure"] = "\n".join(lines[-20:])
        results.append(record)
    return results


def check_artifact(kind, payload):
    kind = kind.removeprefix("justhodl-")
    json.dumps(payload, allow_nan=False)
    if kind in {"katlin", "risk-sizer"}:
        result = publication_summary(payload, kind)
        js = "const fs=require('fs'),a=require('./capital-view.js'); const p=JSON.parse(fs.readFileSync(0,'utf8'));console.log(JSON.stringify(a.permissionErrors(p,process.argv[1])));"
        proc = subprocess.run(["node", "-e", js, kind], input=json.dumps(payload), cwd=ROOT, capture_output=True, text=True, check=True)
        errors = json.loads(proc.stdout)
        # Historical blocked evidence is legitimate. A stale artifact may not still claim permission.
        board = payload.get("war_room", {}) if kind == "katlin" else payload
        if board.get("entries_allowed") is True and errors:
            raise ValueError("; ".join(errors))
        return {"status": result["status"], "entries_allowed": board.get("entries_allowed"), "evidence_warnings": errors}
    if kind == "khalid-risk":
        spec = importlib.util.spec_from_file_location("audit_risk_engine", ROOT / "aws/lambdas/justhodl-khalid-risk/source/risk_engine.py")
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        module.validate_output(payload)
        view = authority_view(payload)
        if payload["policy"]["allows_new_entries"] and view["status"] != "FRESH":
            raise ValueError("; ".join(view["errors"]))
        return {"status": payload["status"], "permission_status": view["status"]}
    if kind == "risk-gate":
        assert payload.get("engine") == "justhodl-risk-gate", "wrong producer"
        legs = payload["legs"]
        weighted = sum(legs[k]["score_fused"] * legs[k]["weight"] for k in ("funding", "credit", "dollar", "carry", "growth", "structure"))
        overlays = sum(x["contribution"] for x in payload["overlays"])
        assert abs(round(weighted + overlays, 3) - payload["composite"]) < 1e-8, "live composite does not reconcile"
        assert payload["indicators"]["indicators"]["acm_term_premium"].get("source") != "FRED T10Y2Y", "yield slope mislabeled ACM"
        inputs = payload["fleet_context"]["inputs"]
        for key, row in inputs.items():
            if "jplg" in key and row.get("status") == "OK":
                assert row.get("unit") == "% YoY" and row.get("evidence", {}).get("contract_version") == "boj-loan-growth-yoy.v1", "JPLG lacks versioned percentage growth contract"
        return {"status": payload["posture"], "composite_reconciled": True}
    if kind == "engine-fusion":
        assert payload.get("engine") == "justhodl-engine-fusion", "wrong producer"
        unavailable = [p for p in payload["inactive_packets"] if not p.get("scoring_excluded")]
        expected = "NO_ACTIVE_EVIDENCE" if not payload["packets"] else "DEGRADED" if unavailable else "OK"
        assert payload["status"] == expected, "policy exclusion incorrectly degrades health"
        return {"status": expected, "policy_excluded_sources": payload["coverage"].get("policy_excluded_sources")}
    raise ValueError("unsupported artifact kind: " + kind)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-tests", action="store_true", help="Inspect supplied artifacts only")
    parser.add_argument("--artifact", action="append", default=[], metavar="ENGINE=PATH")
    args = parser.parse_args()
    result = {"scope": "offline regression and explicitly supplied captured artifacts; no deployment assertion", "tests": [] if args.skip_tests else run_tests(), "artifacts": []}
    for item in args.artifact:
        kind, sep, name = item.partition("=")
        try:
            if not sep: raise ValueError("use ENGINE=PATH")
            payload = json.loads(Path(name).read_text())
            result["artifacts"].append({"engine": kind, "passed": True, **check_artifact(kind, payload)})
        except Exception as exc:
            result["artifacts"].append({"engine": kind, "passed": False, "error": str(exc)})
    result["ok"] = all(row["passed"] for row in result["tests"] + result["artifacts"])
    print(json.dumps(result, indent=2, allow_nan=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
