"""ops/792 — probe Eurostat dissemination API + DBnomics for the data FRED
cannot supply: European industrial production and a fresh euro-area
unemployment aggregate (the OECD series on FRED are stale).

Confirms reachability, the JSON-stat parse, and exact series codes so the
flagship sovereign-stress engine is built only on verified live sources.
"""
import json, os, urllib.parse, urllib.request
from datetime import datetime, timezone

report = {"ops": 792, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe Eurostat + DBnomics for IP + EA unemployment"}

EUROSTAT = ("https://ec.europa.eu/eurostat/api/dissemination/statistics"
            "/1.0/data")


def _get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0",
                                 "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def jsonstat_series(d):
    """Extract [(period, value)] from a JSON-stat response pinned to one
    series (all dims size 1 except time)."""
    try:
        tindex = d["dimension"]["time"]["category"]["index"]
        val = d["value"]
        out = []
        for period, idx in tindex.items():
            v = val.get(str(idx), val.get(idx))
            if v is not None:
                out.append((period, float(v)))
        out.sort(key=lambda x: x[0])
        return out
    except Exception as e:
        return f"PARSE_ERR {type(e).__name__}: {str(e)[:90]}"


def eurostat_probe(dataset, params):
    try:
        url = f"{EUROSTAT}/{dataset}?{urllib.parse.urlencode(params)}"
        d = json.loads(_get(url))
        s = jsonstat_series(d)
        if isinstance(s, str):
            return {"ok": False, "note": s, "url": url[:160]}
        if not s:
            return {"ok": False, "note": "empty series", "url": url[:160]}
        return {"ok": True, "n": len(s), "latest": s[-1][0],
                "value": round(s[-1][1], 2), "url": url[:160]}
    except Exception as e:
        return {"ok": False, "note": f"{type(e).__name__}: {str(e)[:110]}"}


def dbnomics_probe(series_id):
    try:
        url = (f"https://api.db.nomics.world/v22/series/"
               f"{urllib.parse.quote(series_id)}?observations=1")
        d = json.loads(_get(url))
        docs = ((d.get("series") or {}).get("docs")) or []
        if not docs:
            return {"ok": False, "note": "no docs"}
        doc = docs[0]
        per = doc.get("period") or []
        val = doc.get("value") or []
        pts = [(p, v) for p, v in zip(per, val)
               if isinstance(v, (int, float))]
        if not pts:
            return {"ok": False, "note": "no numeric values"}
        return {"ok": True, "n": len(pts), "latest": pts[-1][0],
                "value": pts[-1][1]}
    except Exception as e:
        return {"ok": False, "note": f"{type(e).__name__}: {str(e)[:110]}"}


# ── 1. Eurostat unemployment une_rt_m — fresh EA aggregate + a country ──
report["eurostat_unemployment"] = {}
for geo in ["EA20", "EU27_2020", "DE", "EL"]:
    report["eurostat_unemployment"][geo] = eurostat_probe(
        "une_rt_m", {"format": "JSON", "freq": "M", "s_adj": "SA",
                     "age": "TOTAL", "unit": "PC_ACT", "sex": "T",
                     "geo": geo, "sinceTimePeriod": "2025-01"})

# ── 2. Eurostat industrial production sts_inpr_m — total industry ──
report["eurostat_ip_industry"] = {}
for geo in ["EA20", "DE", "FR", "IT", "ES", "NL"]:
    report["eurostat_ip_industry"][geo] = eurostat_probe(
        "sts_inpr_m", {"format": "JSON", "freq": "M", "indic_bt": "PROD",
                       "nace_r2": "B-D", "s_adj": "SCA", "unit": "I21",
                       "geo": geo, "sinceTimePeriod": "2025-01"})

# ── 3. Eurostat IP — manufacturing only (nace C), alt unit fallback ──
report["eurostat_ip_manufacturing"] = {
    "DE_C_I21": eurostat_probe("sts_inpr_m",
        {"format": "JSON", "freq": "M", "indic_bt": "PROD", "nace_r2": "C",
         "s_adj": "SCA", "unit": "I21", "geo": "DE",
         "sinceTimePeriod": "2025-01"}),
    "DE_BD_I15": eurostat_probe("sts_inpr_m",
        {"format": "JSON", "freq": "M", "indic_bt": "PROD", "nace_r2": "B-D",
         "s_adj": "SCA", "unit": "I15", "geo": "DE",
         "sinceTimePeriod": "2025-01"}),
}

# ── 4. DBnomics fallback for IP ──
report["dbnomics_ip"] = {
    "DE_B-D_SCA_I21": dbnomics_probe(
        "Eurostat/sts_inpr_m/M.PROD.B-D.SCA.I21.DE"),
    "DE_B-D_SCA_I15": dbnomics_probe(
        "Eurostat/sts_inpr_m/M.PROD.B-D.SCA.I15.DE"),
}

report["summary"] = {
    "eurostat_unemp_live": sorted(g for g, v in
        report["eurostat_unemployment"].items() if v.get("ok")),
    "eurostat_ip_live": sorted(g for g, v in
        report["eurostat_ip_industry"].items() if v.get("ok")),
    "dbnomics_ip_live": sorted(k for k, v in
        report["dbnomics_ip"].items() if v.get("ok")),
}

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/792_eurostat_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/792_eurostat_probe.json")
