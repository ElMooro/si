# re-trigger
"""ops/793 — DISCOVERY probe: find the exact working series codes for
European industrial production (Eurostat sts_inpr_m).

Two guess-free discovery methods:
  A. DBnomics dataset/series filter — returns the real series codes that
     exist for a given geo, with their latest observations.
  B. Eurostat direct API — fetch a geo slice without pinning s_adj/unit,
     then read back the dimension catalogues to see the valid codes.
"""
import json, os, urllib.parse, urllib.request
from datetime import datetime, timezone

report = {"ops": 793, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Discover exact Eurostat industrial-production codes"}


def _get(url, timeout=35):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0",
                                 "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


# ── A. DBnomics — list real series for Germany IP ──
report["dbnomics_discovery"] = {}
for tag, dims in [("DE_all", {"geo": ["DE"]}),
                  ("DE_prod_bd", {"geo": ["DE"], "indic_bt": ["PROD"],
                                  "nace_r2": ["B-D"]})]:
    try:
        url = ("https://api.db.nomics.world/v22/series/Eurostat/sts_inpr_m"
               f"?dimensions={urllib.parse.quote(json.dumps(dims))}"
               "&observations=1")
        d = json.loads(_get(url))
        docs = ((d.get("series") or {}).get("docs")) or []
        found = []
        for doc in docs[:10]:
            per = doc.get("period") or []
            val = doc.get("value") or []
            pts = [(p, v) for p, v in zip(per, val)
                   if isinstance(v, (int, float))]
            found.append({
                "series_code": doc.get("series_code"),
                "dims": doc.get("dimensions"),
                "latest": (pts[-1] if pts else None),
                "n": len(pts)})
        report["dbnomics_discovery"][tag] = {
            "num_series": ((d.get("series") or {}).get("num_found")),
            "sample": found}
    except Exception as e:
        report["dbnomics_discovery"][tag] = {
            "err": f"{type(e).__name__}: {str(e)[:120]}"}

# ── B. Eurostat direct — geo slice, read dimension catalogues ──
EUROSTAT = ("https://ec.europa.eu/eurostat/api/dissemination/statistics"
            "/1.0/data")
report["eurostat_discovery"] = {}
for tag, params in [
    ("DE_prod_bd", {"format": "JSON", "geo": "DE", "indic_bt": "PROD",
                    "nace_r2": "B-D", "sinceTimePeriod": "2026-01"}),
    ("DE_only", {"format": "JSON", "geo": "DE",
                 "sinceTimePeriod": "2026-02"})]:
    try:
        url = f"{EUROSTAT}/sts_inpr_m?{urllib.parse.urlencode(params)}"
        d = json.loads(_get(url))
        dims = {}
        for dname, dobj in (d.get("dimension") or {}).items():
            idx = ((dobj.get("category") or {}).get("index")) or {}
            dims[dname] = list(idx.keys())
        vc = len(d.get("value") or {})
        report["eurostat_discovery"][tag] = {
            "size": d.get("size"), "id_order": d.get("id"),
            "dimension_values": dims, "value_count": vc}
    except Exception as e:
        report["eurostat_discovery"][tag] = {
            "err": f"{type(e).__name__}: {str(e)[:140]}"}

# ── C. targeted Eurostat fetch using likely-correct codes ──
def es_series(params):
    try:
        url = f"{EUROSTAT}/sts_inpr_m?{urllib.parse.urlencode(params)}"
        d = json.loads(_get(url))
        ti = ((d["dimension"]["time"]["category"]).get("index")) or {}
        val = d.get("value") or {}
        pts = sorted((p, val.get(str(i)))
                     for p, i in ti.items() if val.get(str(i)) is not None)
        return ({"ok": True, "latest": pts[-1], "n": len(pts)}
                if pts else {"ok": False, "note": "empty",
                             "size": d.get("size")})
    except Exception as e:
        return {"ok": False, "note": f"{type(e).__name__}: {str(e)[:100]}"}


report["eurostat_targeted"] = {
    "DE_PROD_B-D_NSA_I21": es_series(
        {"format": "JSON", "freq": "M", "indic_bt": "PROD", "nace_r2": "B-D",
         "s_adj": "NSA", "unit": "I21", "geo": "DE",
         "sinceTimePeriod": "2025-06"}),
    "DE_PRD_B-D_SCA_I21": es_series(
        {"format": "JSON", "freq": "M", "indic_bt": "PRD", "nace_r2": "B-D",
         "s_adj": "SCA", "unit": "I21", "geo": "DE",
         "sinceTimePeriod": "2025-06"}),
    "DE_PROD_B-D_SCA_PCH": es_series(
        {"format": "JSON", "freq": "M", "indic_bt": "PROD", "nace_r2": "B-D",
         "s_adj": "SCA", "unit": "PCH_SM", "geo": "DE",
         "sinceTimePeriod": "2025-06"}),
}

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/793_ip_discovery.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/793_ip_discovery.json")
