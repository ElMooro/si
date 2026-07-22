"""ops 3695 — AU/QA port hunt (canary-justified: iron ore = China steel =
global construction; Qatar LNG = 20% of world supply = European/Asian energy
cost + sits behind Hormuz). Sweep IMF ports_ref for Australia + Qatar rows,
report REAL names, then check the daily layer carries them (control: Shanghai).
CH-pharma is NOT hunted — Switzerland is landlocked and pharma is acyclical,
so it fails the canary test; it gets removed in the follow-up op."""
import json, sys, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3  # noqa
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

BASE = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
REF = BASE + "/PortWatch_ports_database/FeatureServer/0/query"
DAILY = BASE + "/Daily_Ports_Data/FeatureServer/0/query"


def q(url, params):
    p = {"f": "json", "returnGeometry": "false"}
    p.update(params)
    return json.loads(urllib.request.urlopen(
        url + "?" + urllib.parse.urlencode(p), timeout=50).read())


with report("3695_au_qa_hunt") as rep:
    rep.heading("ops 3695 — Australia + Qatar port discovery")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3695.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860])
            if not ok:
                fails.append(n)

        since = (datetime.now(timezone.utc) - timedelta(days=45)).strftime("%Y-%m-%d")
        found = {}
        for country in ("Australia", "Qatar"):
            j = q(REF, {"where": f"country = '{country}'",
                        "outFields": "portid,portname,country",
                        "resultRecordCount": 60})
            rows = [f["attributes"] for f in (j.get("features") or [])]
            found[country] = {"n": len(rows),
                              "ports": [(r.get("portid"), r.get("portname"))
                                        for r in rows[:16]]}
            ids = [str(r["portid"]) for r in rows if r.get("portid")][:20]
            if ids:
                idl = ",".join("'" + i + "'" for i in ids)
                d = q(DAILY, {"where": f"portid IN ({idl}) AND date >= "
                                       f"timestamp '{since}'",
                              "outFields": "portid,date,portcalls",
                              "orderByFields": "date DESC",
                              "resultRecordCount": 8})
                dr = [f["attributes"] for f in (d.get("features") or [])]
                found[country]["daily_rows"] = len(dr)
                found[country]["daily_sample"] = dr[:3]
        # control
        sh = q(REF, {"where": "portname = 'Shanghai'",
                     "outFields": "portid", "resultRecordCount": 1})
        spid = str((sh.get("features") or [{}])[0].get("attributes", {}).get("portid"))
        cd = q(DAILY, {"where": f"portid = '{spid}' AND date >= timestamp '{since}'",
                       "outFields": "portid,date,portcalls",
                       "resultRecordCount": 2})
        ctrl = len(cd.get("features") or [])
        out["found"] = found
        out["control_shanghai_rows"] = ctrl
        ok = ctrl > 0 and all(found[c]["n"] > 0 for c in found)
        gate("G1_hunt", ok,
             f"control={ctrl} " + json.dumps(found)[:700])
        out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3695.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
