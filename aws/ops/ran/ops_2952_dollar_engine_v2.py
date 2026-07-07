#!/usr/bin/env python3
"""ops 2952 — DOLLAR ENGINE v2 ship.

justhodl-dollar-radar enhanced from 10 to 16 pump/dump canaries (adds: US10Y
trend, 2y Fed-path repricing, Fed swap lines = eurodollar relief valve,
Fed-vs-ECB/BoJ stance gap, China credit impulse, CFTC positioning extremes)
plus the new Risk-Asset Transmission dial (DXY x US10Y read for risk assets)
and a daily history ledger. risk-regime and crypto-confluence now carry an
additive dollar_context block (context only — scores untouched pending
scorecard, per house trust doctrine).

Deploys preserve each function's CURRENT env/timeout/memory (helper REPLACES
env — read-then-passthrough is mandatory). Order matters: dollar-radar first
(smoke writes the v2 feed), consumers after (their smoke reads it).
"""
import json
import sys
from pathlib import Path

import boto3
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = Path(__file__).resolve().parents[2]  # aws/

FNS = ["justhodl-dollar-radar", "justhodl-risk-regime",
       "justhodl-crypto-confluence"]


def current_cfg(fn):
    c = LAM.get_function_configuration(FunctionName=fn)
    return (c.get("Environment", {}).get("Variables", {}) or {},
            int(c.get("Timeout", 180)), int(c.get("MemorySize", 512)))


def get_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    with report("2952_dollar_engine_v2") as rep:
        fails = []
        for fn in FNS:
            env, to, mem = current_cfg(fn)
            rep.section(fn)
            rep.kv(**{fn.replace("-", "_") + "_env_keys": len(env),
                      fn.replace("-", "_") + "_timeout": to})
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=ROOT / "lambdas" / fn / "source",
                          env_vars=env, timeout=to, memory=mem,
                          description="dollar-engine v2 ship (ops 2952)",
                          smoke=True)

        rep.section("verify outputs on S3")
        dr = get_json("data/dollar-radar.json")
        rt = dr.get("risk_transmission") or {}
        ncan = len(dr.get("canaries") or [])
        rep.kv(schema=dr.get("schema_version"), canaries=ncan,
               dollar_pressure=dr.get("dollar_pressure"),
               regime=dr.get("regime"),
               risk_verdict=rt.get("verdict"), risk_score=rt.get("score"))
        if dr.get("schema_version") != "2.0":
            fails.append("schema not 2.0")
        if ncan < 12:
            fails.append(f"only {ncan} canaries (expected >=12)")
        if rt.get("verdict") in (None, "UNKNOWN"):
            fails.append("risk_transmission missing")
        for c in dr.get("canaries") or []:
            rep.log("canary: %s | %s | %s (w %.2f)" % (
                c.get("label"), c.get("reading"), c.get("signal"),
                c.get("weight", 0)))

        hist = get_json("data/dollar-radar-history.json")
        nrows = len(hist.get("rows") or [])
        rep.kv(history_rows=nrows)
        if nrows < 1:
            fails.append("history ledger empty")

        rr = get_json("data/risk-regime.json")
        rc = rr.get("dollar_context") or {}
        rep.kv(risk_regime_dollar_context=bool(rc),
               rr_verdict=rc.get("risk_transmission_verdict"))
        if not rc:
            fails.append("risk-regime missing dollar_context")

        cc = get_json("data/crypto-confluence.json")
        c2 = cc.get("dollar_context") or {}
        rep.kv(crypto_confluence_dollar_context=bool(c2),
               cc_verdict=c2.get("risk_transmission_verdict"))
        if not c2:
            fails.append("crypto-confluence missing dollar_context")

        line = ("dollar v2: canaries=%d pressure=%s regime=%s | risk_tx=%s(%s)"
                " | rr_ctx=%s cc_ctx=%s hist=%d"
                % (ncan, dr.get("dollar_pressure"), dr.get("regime"),
                   rt.get("verdict"), rt.get("score"), bool(rc), bool(c2),
                   nrows))
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            print("FAILURES: " + " | ".join(fails))
            sys.exit(1)
        rep.ok("dollar engine v2 live end-to-end")


if __name__ == "__main__":
    main()
