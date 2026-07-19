"""ops 3507 — Tier-3 recon (read-only): master-ranker schema for the
factor-DNA radar join, sector-medians trigger cadence, forensic-screen
freshness. No writes.
"""
import json, sys
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]
s3c = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

with report("3507_recon_t3") as rep:
    out = {"ops": 3507, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:700]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:660]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3507 — Tier-3 recon")
    try:
        mr = json.loads(s3c.get_object(
            Bucket=BUCKET, Key="data/master-ranker.json")["Body"].read())
        top = sorted(mr.keys()) if isinstance(mr, dict) else type(mr).__name__
        rows = mr.get("rows") or mr.get("ranked") or mr.get("tickers") or []
        if isinstance(rows, dict):
            k0 = sorted(rows.keys())[:3]
            sample = {k: rows[k] for k in k0[:1]}
            rkeys = sorted((rows[k0[0]] or {}).keys()) if k0 else []
            n = len(rows)
        else:
            sample = rows[:1]
            rkeys = sorted((rows[0] or {}).keys()) if rows else []
            n = len(rows)
        nv = None
        if isinstance(rows, list):
            nv = next((r for r in rows if (r.get("ticker") or r.get("symbol")
                                           or r.get("t")) == "NVDA"), None)
        elif isinstance(rows, dict):
            nv = rows.get("NVDA")
        gate("R1_master_ranker_schema", n > 50,
             {"top_keys": top, "n_rows": n, "row_keys": rkeys[:30],
              "nvda_row": json.dumps(nv)[:600] if nv else None,
              "sample": json.dumps(sample)[:400]})
    except Exception as e:
        gate("R1_master_ranker_schema", False, str(e)[:300])

    try:
        sm = s3c.head_object(Bucket=BUCKET,
                             Key="data/fundgraph/sector-medians.json")
        fs = s3c.head_object(Bucket=BUCKET, Key="data/forensic-screen.json")
        gate("R2_freshness", True,
             {"sector_medians_lastmod": str(sm["LastModified"]),
              "forensic_lastmod": str(fs["LastModified"])})
    except Exception as e:
        gate("R2_freshness", False, str(e)[:200])

    try:
        scheds = []
        pag = sch.get_paginator("list_schedules")
        for pg in pag.paginate():
            for s0 in pg["Schedules"]:
                if "fundamental" in s0["Name"] or "fundgraph" in s0["Name"]:
                    d = sch.get_schedule(Name=s0["Name"],
                                         GroupName=s0.get("GroupName",
                                                          "default"))
                    scheds.append({"name": s0["Name"],
                                   "expr": d.get("ScheduleExpression"),
                                   "input": (d.get("Target", {})
                                             .get("Input") or "")[:120]})
        gate("R3_schedules", True, scheds or "none-matching")
    except Exception as e:
        gate("R3_schedules", False, str(e)[:200])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3507.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
