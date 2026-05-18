"""ops/791 — probe the ECB CISS dataflow + FRED for systemic/sovereign
stress series, to source a justhodl-systemic-stress engine on solid keys.

The ECB publishes the Composite Indicator of Systemic Stress (CISS) and the
Composite Indicator of Systemic Sovereign Stress (SovCISS) on its Data
Portal. This lists every series in the CISS dataflow — countries, the
sovereign vs financial-system variants — and checks what FRED mirrors.
"""
import csv, io, json, os, time, urllib.request
from datetime import datetime, timezone

FRED_KEY = "2f057499936072679d8843d7fce99989"
report = {"ops": 791, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe ECB CISS/SovCISS + FRED stress series"}


def get(url, ua="justhodl-probe/1.0", timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


# ── 1. ECB Data Portal — entire CISS dataflow, one obs per series ──
ecb_series = []
ecb_err = None
try:
    url = ("https://data-api.ecb.europa.eu/service/data/CISS"
           "?format=csvdata&lastNObservations=1")
    raw = get(url).decode("utf-8", "ignore")
    rdr = csv.DictReader(io.StringIO(raw))
    seen = {}
    for row in rdr:
        key = row.get("KEY") or row.get("SERIES_KEY") or ""
        if not key or key in seen:
            continue
        seen[key] = True
        ecb_series.append({
            "key": key,
            "ref_area": row.get("REF_AREA"),
            "title": (row.get("TITLE") or row.get("TITLE_COMPL")
                      or "")[:90],
            "last_date": row.get("TIME_PERIOD"),
            "last_value": row.get("OBS_VALUE"),
        })
except Exception as e:
    ecb_err = f"{type(e).__name__}: {str(e)[:200]}"

report["ecb_ciss_dataflow"] = {
    "error": ecb_err,
    "n_series": len(ecb_series),
    "ref_areas": sorted({s["ref_area"] for s in ecb_series
                         if s["ref_area"]}),
    "sample_series": ecb_series[:40],
}

# ── 2. probe a few specific CISS keys (euro area / US / UK / CN) ──
candidate_keys = [
    "D.U2.Z0Z.4F.EC.SS_CIN.IDX",      # euro-area composite CISS
    "D.US.Z0Z.4F.EC.SS_CIN.IDX",      # US CISS
    "D.GB.Z0Z.4F.EC.SS_CIN.IDX",      # UK CISS
    "D.CN.Z0Z.4F.EC.SS_CIN.IDX",      # China CISS
    "D.DE.Z0Z.4F.EC.SS_CIN.IDX",      # Germany CISS
]
key_probe = {}
for k in candidate_keys:
    try:
        u = (f"https://data-api.ecb.europa.eu/service/data/CISS/{k}"
             "?format=csvdata&lastNObservations=2")
        raw = get(u).decode("utf-8", "ignore")
        rows = list(csv.DictReader(io.StringIO(raw)))
        key_probe[k] = {"ok": len(rows) > 0,
                        "last": (rows[-1].get("TIME_PERIOD"),
                                 rows[-1].get("OBS_VALUE")) if rows else None}
    except Exception as e:
        key_probe[k] = {"ok": False, "err": str(e)[:120]}
    time.sleep(0.4)
report["ecb_key_probe"] = key_probe

# ── 3. FRED — search for CISS / systemic / sovereign stress mirrors ──
fred_hits = []
for term in ["composite indicator of systemic stress",
             "systemic stress", "sovereign stress", "CISS"]:
    try:
        u = ("https://api.stlouisfed.org/fred/series/search"
             f"?search_text={urllib.parse.quote(term)}"
             f"&api_key={FRED_KEY}&file_type=json&limit=12"
             "&order_by=popularity&sort_order=desc")
        d = json.loads(get(u))
        for s in d.get("seriess", []):
            fred_hits.append({"id": s["id"], "title": s["title"][:80],
                              "freq": s.get("frequency_short"),
                              "end": s.get("observation_end"),
                              "term": term})
    except Exception as e:
        fred_hits.append({"term": term, "err": str(e)[:120]})
    time.sleep(0.3)
# dedupe by id
seen_ids, uniq = set(), []
for h in fred_hits:
    i = h.get("id")
    if i and i in seen_ids:
        continue
    if i:
        seen_ids.add(i)
    uniq.append(h)
report["fred_stress_search"] = uniq

report["verdict"] = (
    f"ECB CISS dataflow: {len(ecb_series)} series across ref-areas "
    f"{report['ecb_ciss_dataflow']['ref_areas']}. "
    f"Key probe hits: {[k for k,v in key_probe.items() if v.get('ok')]}. "
    f"FRED mirrors found: {len(uniq)}.")

print(json.dumps(report, indent=2, default=str)[:6000])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/791_ciss_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("\n[ok] wrote aws/ops/reports/791_ciss_probe.json")
