#!/usr/bin/env python3
"""ops 2953 — dollar v2 second pass: redeploy with the TGA units fix ($mn ->
$bn, a pre-existing v1 bug corrupting the net-liquidity + TGA canaries) and
widened sibling probes; then verify live end-to-end including the page, and
DUMP the four sibling feeds' shapes for the record."""
import json, sys, time, urllib.request
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

def shape(key):
    try:
        d = get_json(key)
        if isinstance(d, dict):
            return "dict keys=" + ",".join(list(d.keys())[:12])
        return "list len=%d first-keys=%s" % (len(d),
            ",".join(list(d[0].keys())[:8]) if d and isinstance(d[0], dict) else "-")
    except Exception as e:
        return "MISSING (%s)" % type(e).__name__

def main():
    with report("2953_dollar_v2_verify") as rep:
        fails = []
        for k in ("data/eurodollar-stress.json", "data/cb-stance.json",
                  "data/china-liquidity.json", "data/cftc-all-cache.json"):
            line = "sibling %s: %s" % (k, shape(k))
            print(line); rep.log(line)

        c = LAM.get_function_configuration(FunctionName="justhodl-dollar-radar")
        env = c.get("Environment", {}).get("Variables", {}) or {}
        deploy_lambda(report=rep, function_name="justhodl-dollar-radar",
                      source_dir=ROOT / "lambdas" / "justhodl-dollar-radar" / "source",
                      env_vars=env, timeout=int(c.get("Timeout", 180)),
                      memory=int(c.get("MemorySize", 512)),
                      description="dollar v2.1: TGA units fix + probe widening (ops 2953)",
                      smoke=True)

        dr = get_json("data/dollar-radar.json")
        rt = dr.get("risk_transmission") or {}
        cans = dr.get("canaries") or []
        by = {c["label"]: c for c in cans}
        tga = by.get("Treasury General Account", {})
        nl = by.get("Fed net liquidity (13w change)", {})
        rep.kv(canaries=len(cans), pressure=dr.get("dollar_pressure"),
               regime=dr.get("regime"), risk=f"{rt.get('verdict')}({rt.get('score')})",
               tga_reading=tga.get("reading"), netliq_reading=nl.get("reading"))
        for cn in cans:
            rep.log("canary: %s | %s | %s" % (cn.get("label"), cn.get("reading"), cn.get("signal")))
        def bn_ok(txt):
            try:
                return abs(float(str(txt).split()[0].replace("+", ""))) < 1500
            except Exception:
                return False
        if not bn_ok(tga.get("reading", "9e9")):
            fails.append("TGA reading still absurd: %s" % tga.get("reading"))
        if not bn_ok(nl.get("reading", "9e9")):
            fails.append("net-liq reading still absurd: %s" % nl.get("reading"))
        if len(cans) < 12:
            fails.append("canaries %d < 12" % len(cans))
        if rt.get("verdict") in (None, "UNKNOWN"):
            fails.append("risk_transmission missing")

        # live page carries the new strip
        html = ""
        for _ in range(5):
            try:
                html = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/dollar.html?_=%d" % int(time.time()),
                    headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=15
                ).read().decode("utf-8", "replace")
            except Exception:
                html = ""
            if "Risk-Asset Transmission" in html:
                break
            time.sleep(25)
        rep.kv(page_has_risk_strip=("Risk-Asset Transmission" in html))
        if "Risk-Asset Transmission" not in html:
            fails.append("dollar.html missing risk strip")

        line = ("v2.1: canaries=%d pressure=%s %s | risk=%s(%s) | tga=%s netliq=%s"
                % (len(cans), dr.get("dollar_pressure"), dr.get("regime"),
                   rt.get("verdict"), rt.get("score"),
                   tga.get("reading"), nl.get("reading")))
        print(line); rep.kv(summary=line)
        if fails:
            for f in fails: rep.fail(f)
            print("FAILURES: " + " | ".join(fails)); sys.exit(1)
        rep.ok("dollar v2.1 verified live: engine, units, page, consumers")

if __name__ == "__main__":
    main()
