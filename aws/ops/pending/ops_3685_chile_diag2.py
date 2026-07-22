"""ops 3685 — Chile diagnosis v2 (3684 used the WRONG daily service and got
0 rows even for Shanghai — self-invalidating probe). Correct layer =
Daily_Ports_Data. Probe Chilean portids there + Shanghai control, and dump
distinct portids present in the last 60d so we see exactly which nations the
daily layer covers."""
import json, sys, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3  # noqa
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

BASE = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services")
REF = BASE + "/PortWatch_ports_database/FeatureServer/0/query"
DAILY = BASE + "/Daily_Ports_Data/FeatureServer/0/query"


def q(url, params):
    p = {"f": "json", "returnGeometry": "false"}
    p.update(params)
    return json.loads(urllib.request.urlopen(
        url + "?" + urllib.parse.urlencode(p), timeout=50).read())


with report("3685_chile_diag2") as rep:
    rep.heading("ops 3685 — Chile coverage, correct daily layer")
    out = {"gates": {}}
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3685.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=45)).strftime("%Y-%m-%d")
        j = q(REF, {"where": "country = 'Chile'",
                    "outFields": "portid,portname", "resultRecordCount": 40})
        cl = [f["attributes"] for f in (j.get("features") or [])]
        ids = [str(a["portid"]) for a in cl if a.get("portid")]
        out["chile_ref_n"] = len(cl)

        idlist = ",".join("'" + i + "'" for i in ids[:20])
        d = q(DAILY, {"where": f"portid IN ({idlist}) AND date >= "
                               f"timestamp '{since}'",
                      "outFields": "portid,date,portcalls",
                      "orderByFields": "date DESC",
                      "resultRecordCount": 12})
        rows = [f["attributes"] for f in (d.get("features") or [])]
        out["chile_daily_rows"] = len(rows)
        out["chile_sample"] = rows[:4]

        # Shanghai control through the SAME path
        sh = q(REF, {"where": "portname = 'Shanghai'",
                     "outFields": "portid,portname", "resultRecordCount": 2})
        spid = str((sh.get("features") or [{}])[0].get("attributes", {}).get("portid"))
        d2 = q(DAILY, {"where": f"portid = '{spid}' AND date >= "
                                f"timestamp '{since}'",
                       "outFields": "portid,date,portcalls",
                       "orderByFields": "date DESC", "resultRecordCount": 3})
        srows = [f["attributes"] for f in (d2.get("features") or [])]
        out["shanghai_control"] = {"portid": spid, "rows": len(srows),
                                    "sample": srows[:2]}

        # which portids DOES the daily layer carry?
        d3 = q(DAILY, {"where": f"date >= timestamp '{since}'",
                       "outFields": "portid", "returnDistinctValues": "true",
                       "resultRecordCount": 1000})
        dist = sorted({f["attributes"]["portid"] for f in (d3.get("features") or [])})
        out["daily_distinct_n"] = len(dist)
        out["chile_in_daily"] = [i for i in ids if i in dist]

        ok = len(srows) > 0  # control must work for the probe to be valid
        out["gates"]["G1_diag2"] = {"ok": ok, "detail":
            (f"chile_ref={len(cl)} chile_daily_rows={len(rows)} "
             f"chile_in_daily={out['chile_in_daily']} "
             f"daily_distinct_ports={len(dist)} "
             f"shanghai_rows={len(srows)} {json.dumps(srows[:1])[:160]} "
             f"chile_sample={json.dumps(rows[:2])[:200]}")}
        print(("PASS  " if ok else "FAIL  ") + "G1_diag2 — "
              + out["gates"]["G1_diag2"]["detail"][:800])
        out["verdict"] = "PASS_ALL" if ok else "GAPS: G1_diag2"
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3685.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
