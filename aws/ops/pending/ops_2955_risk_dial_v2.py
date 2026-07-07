#!/usr/bin/env python3
"""ops 2955 — RISK-ASSET TRANSMISSION dial v2: from 3 to 9 direct drivers
(DXY 28% + US10Y 18% anchor by doctrine, + real yield, Fed net liquidity,
VIX, HY credit, bond vol, NFCI, oil shock; breakeven quality test classifies
good vs bad yield rises). Deploys dollar-radar + equity-confluence
env/timeout-preserving, hard-verifies live outputs, prints the driver table.
Propagation: 17 engines read dollar-radar.json directly, 12 more via the
risk-regime hub — feed refresh IS the wiring."""
import json, sys
from pathlib import Path
import boto3
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = Path(__file__).resolve().parents[2]

def get_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())

def main():
    with report("2955_risk_dial_v2") as rep:
        fails = []
        for fn in ("justhodl-dollar-radar", "justhodl-equity-confluence"):
            c = LAM.get_function_configuration(FunctionName=fn)
            env = c.get("Environment", {}).get("Variables", {}) or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=ROOT / "lambdas" / fn / "source",
                          env_vars=env, timeout=int(c.get("Timeout", 180)),
                          memory=int(c.get("MemorySize", 512)),
                          description="risk dial v2 (ops 2955)", smoke=True)

        dr = get_json("data/dollar-radar.json")
        rt = dr.get("risk_transmission") or {}
        comps = rt.get("components") or []
        rep.kv(dial_components=len(comps), risk_score=rt.get("score"),
               risk_verdict=rt.get("verdict"),
               dollar_pressure=dr.get("dollar_pressure"),
               canaries=len(dr.get("canaries") or []))
        for c in comps:
            rep.log("driver: %s | %s | lean %+d (w %.2f)" % (
                c.get("label"), c.get("reading"), c.get("lean"),
                c.get("weight", 0)))
        if len(comps) < 7:
            fails.append("only %d dial components (expected >=7)" % len(comps))
        if rt.get("verdict") in (None, "UNKNOWN"):
            fails.append("dial verdict missing")

        ec = get_json("data/equity-confluence.json")
        e2 = ec.get("dollar_context") or {}
        rep.kv(equity_confluence_ctx=bool(e2),
               ec_verdict=e2.get("risk_transmission_verdict"))
        if not e2:
            fails.append("equity-confluence missing dollar_context")

        line = ("dial v2: %d drivers -> %s(%s) | dollar %s(%s) | eq-ctx=%s"
                % (len(comps), rt.get("verdict"), rt.get("score"),
                   dr.get("regime"), dr.get("dollar_pressure"), bool(e2)))
        print(line); rep.kv(summary=line)
        if fails:
            for f in fails: rep.fail(f)
            print("FAILURES: " + " | ".join(fails)); sys.exit(1)
        rep.ok("risk dial v2 live; fleet inherits via 17 direct + 12 hub readers")

if __name__ == "__main__":
    main()
