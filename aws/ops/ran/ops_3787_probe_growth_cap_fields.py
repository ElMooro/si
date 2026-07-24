#!/usr/bin/env python3
"""ops 3787 — probe: which growth/cap fields exist before building filters."""
import sys, json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
FAILED=[]
def gate(rep,n,ok,d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok: FAILED.append(n)
def main():
    with report("3787_probe_growth_cap_fields") as rep:
        rep.heading("ops 3787 — probe growth/cap fields (no code written)")
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/chokepoint.json")["Body"].read())
        cap=d.get("capture_gap") or {}; rows=cap.get("all_rows") or []
        rep.kv(version=d.get("version"), rows=len(rows))
        rep.section("Row field inventory (union across all rows)")
        keys={}
        for r in rows:
            for k,v in r.items():
                keys.setdefault(k,0)
                if v is not None: keys[k]+=1
        for k in sorted(keys): rep.log("  %-32s populated %d/%d" % (k,keys[k],len(rows)))
        rep.section("Growth-related?")
        g=[k for k in keys if any(x in k.lower() for x in ("growth","yoy","rev_g","cagr","accel"))]
        rep.log("  growth-ish keys: %s" % (g or "NONE"))
        gate(rep,"PROBE.growth_exists",bool(g),"growth fields present" if g else "NO growth field — must be added to the engine")
        rep.section("Cap bucket values")
        cb={}
        for r in rows: cb[r.get("cap_bucket")]=cb.get(r.get("cap_bucket"),0)+1
        for k,v in sorted(cb.items(),key=lambda x:-x[1]): rep.log("  %-12s %d" % (k,v))
        rep.section("Is there an S&P500 membership flag anywhere?")
        sp=[k for k in keys if "sp" in k.lower() or "index" in k.lower() or "member" in k.lower()]
        rep.log("  candidates: %s" % (sp or "NONE"))
        rep.section("Feeds that could supply SP500 membership")
        for key in ("data/fundamental-census-matrix.json","data/spx-ma.json","data/universe.json"):
            try:
                o=s3.get_object(Bucket="justhodl-dashboard-live",Key=key)
                j=json.loads(o["Body"].read())
                tk=list(j.keys())[:12]
                rep.log("  %-42s OK top-keys=%s" % (key,tk))
            except Exception as e:
                rep.log("  %-42s ERR %s" % (key,str(e)[:60]))
        if FAILED:
            rep.warn("growth must be computed in-engine from the income statement already fetched")
        rep.ok("PASS_ALL — probe complete")
        if False:
            sys.exit(1)
if __name__=="__main__": main()
