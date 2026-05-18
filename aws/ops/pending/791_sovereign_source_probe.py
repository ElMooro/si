"""ops/791 — probe data sources for the sovereign & systemic-stress engine.

Verifies which series return fresh data BEFORE the flagship engine is built:
  - FRED: harmonised unemployment + industrial production for the major
    European economies (OECD MEI discontinuation makes some series stale —
    this probe finds the live ones).
  - ECB Data Portal: confirms the CISS headline + sub-index + SovCISS keys
    return data via the csvdata API.
"""
import csv, io, json, os, urllib.request
from datetime import datetime, timezone

FRED_KEY = "2f057499936072679d8843d7fce99989"
report = {"ops": 791, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe FRED EU real-economy + ECB CISS/SovCISS sources"}


def _get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fred_probe(sid):
    try:
        url = ("https://api.stlouisfed.org/fred/series/observations"
               f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
               "&sort_order=desc&limit=3")
        d = json.loads(_get(url))
        obs = [o for o in d.get("observations", [])
               if o.get("value") not in (None, ".", "")]
        if not obs:
            return {"id": sid, "ok": False, "note": "empty"}
        return {"id": sid, "ok": True, "latest_date": obs[0]["date"],
                "latest_value": obs[0]["value"]}
    except Exception as e:
        return {"id": sid, "ok": False, "note": f"{type(e).__name__}:"
                f"{str(e)[:80]}"}


def ecb_probe(key):
    try:
        url = (f"https://data-api.ecb.europa.eu/service/data/CISS/{key}"
               "?format=csvdata&lastNObservations=2")
        raw = _get(url).decode("utf-8", "ignore")
        rows = list(csv.reader(io.StringIO(raw)))
        if len(rows) < 2:
            return {"key": key, "ok": False, "note": "no rows"}
        hdr = rows[0]
        ti = hdr.index("TIME_PERIOD") if "TIME_PERIOD" in hdr else -1
        vi = hdr.index("OBS_VALUE") if "OBS_VALUE" in hdr else -1
        last = rows[-1]
        return {"key": key, "ok": ti >= 0 and vi >= 0,
                "latest": (last[ti] if ti >= 0 else None),
                "value": (last[vi] if vi >= 0 else None)}
    except Exception as e:
        return {"key": key, "ok": False, "note": f"{type(e).__name__}:"
                f"{str(e)[:80]}"}


# ── 1. FRED — harmonised unemployment rate (OECD LRHUTTTT family) ──
unemp = {
    "euro_area": "LRHUTTTTEZM156S", "germany": "LRHUTTTTDEM156S",
    "france": "LRHUTTTTFRM156S", "italy": "LRHUTTTTITM156S",
    "spain": "LRHUTTTTESM156S", "netherlands": "LRHUTTTTNLM156S",
    "uk": "LRHUTTTTGBM156S", "usa": "UNRATE",
}
report["fred_unemployment"] = {k: fred_probe(v) for k, v in unemp.items()}

# ── 2. FRED — industrial production (try several conventions) ──
ip_candidates = {
    "germany_PRINTO": "PRINTO01DEM661S", "germany_PRMNTO": "DEUPROINDMISMEI",
    "france_PRINTO": "PRINTO01FRM661S", "italy_PRINTO": "PRINTO01ITM661S",
    "spain_PRINTO": "PRINTO01ESM661S", "ea_PRINTO": "PRINTO01EZM661S",
    "uk_PRINTO": "PRINTO01GBM661S", "germany_PRMNTO02": "DEUPRMNTO01IXOBSAM",
    "ea_prod_b_d": "PRMNTO01EZM661S",
}
report["fred_industrial_production"] = {k: fred_probe(v)
                                        for k, v in ip_candidates.items()}

# ── 3. ECB CISS — headline, sub-indices, SovCISS ──
ecb_keys = {
    "ciss_ea_headline": "D.U2.Z0Z.4F.EC.SS_CI.IDX",
    "ciss_ea_headline_new": "D.U2.Z0Z.4F.EC.SS_CIN.IDX",
    "ciss_us_headline": "D.US.Z0Z.4F.EC.SS_CI.IDX",
    "ciss_cn_headline_new": "D.CN.Z0Z.4F.EC.SS_CIN.IDX",
    "ciss_uk_headline_new": "D.GB.Z0Z.4F.EC.SS_CIN.IDX",
    "ciss_ea_bond": "D.U2.Z0Z.4F.EC.BON_CI.IDX",
    "ciss_ea_equity": "D.U2.Z0Z.4F.EC.EQU_CI.IDX",
    "ciss_ea_money": "D.U2.Z0Z.4F.EC.MMS_CI.IDX",
    "ciss_us_bond": "D.US.Z0Z.4F.EC.BON_CI.IDX",
    "ciss_us_equity": "D.US.Z0Z.4F.EC.EQU_CI.IDX",
    "sovciss_ea": "M.U2.Z0Z.4F.EC.SOV_GDPW.IDX",
    "sovciss_de": "M.DE.Z0Z.4F.EC.SOV_CI.IDX",
    "sovciss_fr": "M.FR.Z0Z.4F.EC.SOV_CI.IDX",
    "sovciss_it": "M.IT.Z0Z.4F.EC.SOV_CI.IDX",
    "sovciss_es": "M.ES.Z0Z.4F.EC.SOV_CI.IDX",
    "sovciss_pt": "M.PT.Z0Z.4F.EC.SOV_CI.IDX",
    "sovciss_gr": "M.GR.Z0Z.4F.EC.SOV_CI.IDX",
}
report["ecb_ciss"] = {k: ecb_probe(v) for k, v in ecb_keys.items()}

# ── summary ──
report["summary"] = {
    "fred_unemp_live": sorted(k for k, v in
                              report["fred_unemployment"].items()
                              if v.get("ok")),
    "fred_ip_live": sorted(k for k, v in
                           report["fred_industrial_production"].items()
                           if v.get("ok")),
    "ecb_ciss_live": sorted(k for k, v in report["ecb_ciss"].items()
                            if v.get("ok")),
}

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/791_sovereign_source_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/791_sovereign_source_probe.json")
