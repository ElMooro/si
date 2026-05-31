#!/usr/bin/env python3
"""1079 — find the real writer of cot/extremes/current.json + diagnose
the edge-engine and options-flow no-op stubs."""
import io, json, os, pathlib, urllib.request, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1079_cot_writer_and_stubs.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)


def download_code(name):
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    files = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            if fname.endswith(".py"):
                files[fname] = zf.read(fname).decode("utf-8", errors="replace")
    return files


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: search all Lambdas for "cot/extremes" S3 writes
    print("[1079] phase 1: find real cot/extremes writer…")
    paginator = lam.get_paginator("list_functions")
    matches = []
    n = 0
    for page in paginator.paginate():
        for f in page["Functions"]:
            n += 1
            name = f["FunctionName"]
            try:
                files = download_code(name)
                for fname, content in files.items():
                    if "cot/extremes" in content or "extremes/current" in content:
                        matches.append({
                            "lambda": name,
                            "file":   fname,
                            "evidence": next((L.strip()[:200] for L in content.split("\n")
                                                if "cot/extremes" in L or "extremes/current" in L),
                                                None)
                        })
                        break
            except Exception:
                continue
    out["cot_writers"] = matches
    out["n_scanned"] = n
    
    # Phase 2: inspect edge-engine + options-flow code for early-return / stub
    print("[1079] phase 2: diagnose dead Lambdas…")
    for stub_name in ["justhodl-edge-engine", "justhodl-options-flow"]:
        try:
            files = download_code(stub_name)
            entry = {"files": list(files.keys()), "code_signatures": {}}
            for fname, content in files.items():
                if fname.endswith(".py") and ("lambda_function" in fname
                                                or "handler" in fname.lower()):
                    lines = content.split("\n")
                    entry["code_signatures"][fname] = {
                        "total_lines": len(lines),
                        "first_30":    lines[:30],
                        "handler_excerpt": [],
                    }
                    # Find handler function and grab first 20 lines of it
                    for i, L in enumerate(lines):
                        if ("def handler" in L or "def lambda_handler" in L) and L.strip().endswith(":"):
                            entry["code_signatures"][fname]["handler_excerpt"] = lines[i:i+20]
                            break
            out[stub_name] = entry
        except Exception as e:
            out[stub_name] = {"err": str(e)[:200]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1079] DONE — scanned {n} lambdas")


if __name__ == "__main__":
    main()
