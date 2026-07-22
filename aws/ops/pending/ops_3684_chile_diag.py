"""ops 3684 — Chile diagnosis: are Chilean portids present in the DAILY ports
layer at all? Query the daily layer directly for the 6 Chile portids from the
ref (port56 Antofagasta, port89/90, port92, port208, port263...) and report
row counts + date range. Determines: coverage gap (accept honestly) vs our
filter bug (fix)."""
import json, sys, urllib.parse, urllib.request
from pathlib import Path
import boto3  # noqa
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

REF = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
       "PortWatch_ports_database/FeatureServer/0/query")
DAILY = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
         "Daily_Port_Activity_Data_and_Trade_Estimates/FeatureServer/0/query")


def q(url, params):
    p = {"f": "json", "returnGeometry": "false"}
    p.update(params)
    return json.loads(urllib.request.urlopen(
        url + "?" + urllib.parse.urlencode(p), timeout=45).read())


with report("3684_chile_diag") as rep:
    rep.heading("ops 3684 — Chile daily-layer coverage diagnosis")
    out = {"gates": {}}
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3684.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        # Chile ports in ref
        j = q(REF, {"where": "country = 'Chile'",
                    "outFields": "portid,portname,country",
                    "resultRecordCount": 40})
        cl = [f["attributes"] for f in (j.get("features") or [])]
        out["chile_ref"] = [(a.get("portid"), a.get("portname")) for a in cl]
        ids = [str(a.get("portid")) for a in cl if a.get("portid")][:20]

        # do they exist in the daily layer?
        found = {}
        for pid in ids[:8]:
            try:
                d = q(DAILY, {"where": f"portid = '{pid}'",
                              "outFields": "portid,date,portcalls",
                              "orderByFields": "date DESC",
                              "resultRecordCount": 3})
                fs = [f["attributes"] for f in (d.get("features") or [])]
                found[pid] = {"rows": len(fs),
                              "sample": fs[:2]}
            except Exception as e:
                found[pid] = {"err": str(e)[:80]}
        out["daily_probe"] = found

        # counter-check: a known-good port (Shanghai) through same path
        try:
            sh = q(REF, {"where": "portname = 'Shanghai'",
                         "outFields": "portid,portname", "resultRecordCount": 2})
            spid = str((sh.get("features") or [{}])[0]
                       .get("attributes", {}).get("portid"))
            d2 = q(DAILY, {"where": f"portid = '{spid}'",
                           "outFields": "portid,date,portcalls",
                           "orderByFields": "date DESC",
                           "resultRecordCount": 2})
            out["shanghai_check"] = {"portid": spid,
                                     "rows": len(d2.get("features") or []),
                                     "sample": [f["attributes"] for f in
                                                (d2.get("features") or [])][:1]}
        except Exception as e:
            out["shanghai_check"] = {"err": str(e)[:100]}

        covered = sum(1 for v in found.values() if v.get("rows"))
        out["gates"]["G1_diag"] = {
            "ok": True,
            "detail": (f"chile_ref_n={len(cl)} probed={len(found)} "
                       f"with_daily_rows={covered} "
                       f"probe={json.dumps(found)[:420]} "
                       f"shanghai={json.dumps(out['shanghai_check'])[:200]}")}
        print("PASS  G1_diag —", out["gates"]["G1_diag"]["detail"][:820])
        out["verdict"] = "PASS_ALL"
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3684.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
