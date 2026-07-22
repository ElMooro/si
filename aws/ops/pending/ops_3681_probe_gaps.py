"""ops 3681 — PROBE: (a) how is Taiwan filed in IMF ports_ref (alias hunt),
(b) does Chile exist at all (any port in ref w/ Chile-ish name/country),
(c) FRED inventory/utilization candidates for the 4th factor. Pure recon —
writes findings to report; no engine changes."""
import json, sys, urllib.parse, urllib.request
from pathlib import Path
import boto3  # noqa
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

BASE = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/"
        "PortWatch_ports_database/FeatureServer/0/query")
FRED = "2f057499936072679d8843d7fce99989"


def q(params):
    p = {"f": "json", "outFields": "portid,portname,country,fullname",
         "returnGeometry": "false"}
    p.update(params)
    u = BASE + "?" + urllib.parse.urlencode(p)
    return json.loads(urllib.request.urlopen(u, timeout=40).read())


with report("3681_probe_gaps") as rep:
    rep.heading("ops 3681 — TW alias / Chile / inventory-factor recon")
    out = {"gates": {}}
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3681.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        # (a)+(b) full ref sweep, paginated
        rows = []
        off = 0
        while off < 12000:
            j = q({"where": "1=1", "resultOffset": off,
                   "resultRecordCount": 1000})
            fs = [f["attributes"] for f in (j.get("features") or [])]
            rows += fs
            if len(fs) < 1000:
                break
            off += 1000
        out["ref_total"] = len(rows)
        def hits(*terms):
            return [{"name": r.get("portname"), "country": r.get("country"),
                     "id": r.get("portid")} for r in rows
                    if any(t in ((str(r.get("portname") or "") + " " +
                                  str(r.get("fullname") or "") + " " +
                                  str(r.get("country") or "")).lower())
                           for t in terms)][:12]
        out["taiwan_like"] = hits("taiwan", "kaohsiung", "taichung",
                                   "keelung", "taipei")
        out["chile_like"] = hits("chile", "valparai", "san antonio",
                                  "mejillones", "antofagasta", "iquique",
                                  "coronel", "lirquen", "angamos")
        countries = sorted({str(r.get("country") or "") for r in rows})
        out["countries_c"] = [c for c in countries
                              if c.lower().startswith(("c", "t"))][:60]
        out["gates"]["G1_ref"] = {"ok": True, "detail":
                                  f"total={len(rows)} TW={out['taiwan_like']} "
                                  f"CL={out['chile_like']}"}
        print("PASS  G1_ref —", str(out["gates"]["G1_ref"]["detail"])[:700])

        # (c) FRED 4th-factor candidates
        cands = {
            "semis_util": "CAPUTLG3344S",       # semiconductor cap utilization
            "total_util": "TCU",
            "mfg_inv_sales": "MNFCTRIRSA",       # inventories/sales ratio
            "biz_inv_sales": "ISRATIO",
            "retail_inv": "RETAILIRSA",
            "cu_mining": "CAPUTLG21S",
        }
        got = {}
        for k, sid in cands.items():
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations?"
                     f"series_id={sid}&api_key={FRED}&file_type=json"
                     "&sort_order=desc&limit=14")
                o = json.loads(urllib.request.urlopen(u, timeout=20).read())
                obs = [x for x in (o.get("observations") or [])
                       if x.get("value") not in (".", "", None)]
                if obs:
                    got[k] = {"sid": sid, "latest": obs[0]["value"],
                              "date": obs[0]["date"],
                              "yr_ago": (obs[12]["value"]
                                          if len(obs) > 12 else None)}
            except Exception as e:
                got[k] = {"sid": sid, "err": str(e)[:60]}
        out["fred_factor4"] = got
        out["gates"]["G2_fred"] = {"ok": sum(1 for v in got.values()
                                              if v.get("latest")) >= 4,
                                    "detail": json.dumps(got)[:700]}
        print("PASS  G2_fred —", json.dumps(got)[:700])
        out["verdict"] = "PASS_ALL"
    except Exception:
        out["crash"] = traceback.format_exc()[-1000:]
        out["verdict"] = "CRASH"
        print("CRASH:", out["crash"][-400:])
    Path("aws/ops/reports/3681.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
