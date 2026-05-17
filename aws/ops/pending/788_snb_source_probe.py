"""ops/788 — probe SNB / Switzerland data sources before building the
justhodl-snb-detail engine. Read-only. Confirms which FRED series and which
data.snb.ch cubes actually return real, recent data, so the engine is built
on verified identifiers rather than guesses.
"""
import urllib.parse
import json, os, time, urllib.request, csv, io
from datetime import datetime, timezone

FRED_KEY = "2f057499936072679d8843d7fce99989"
report = {"ops": 788, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe SNB data sources (FRED + SNB data portal API)"}


def get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-probe/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


# ── A. FRED series search for SNB / Switzerland liquidity series ──
def fred_search(text, limit=18):
    url = ("https://api.stlouisfed.org/fred/series/search"
           f"?search_text={urllib.parse.quote(text)}&api_key={FRED_KEY}"
           f"&file_type=json&limit={limit}&order_by=popularity&sort_order=desc")
    try:
        d = json.loads(get(url))
        return [{"id": s["id"], "title": s["title"][:70],
                 "freq": s.get("frequency_short"),
                 "end": s.get("observation_end")}
                for s in d.get("seriess", [])]
    except Exception as e:
        return [{"error": str(e)[:150]}]


report["fred_search_snb"] = fred_search("Swiss National Bank")
report["fred_search_sight"] = fred_search("Switzerland sight deposits")
report["fred_search_assets"] = fred_search("Switzerland central bank assets")


# ── B. direct FRED checks on known/candidate series ──
def fred_probe(sid):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit=3")
    try:
        d = json.loads(get(url))
        obs = [o for o in d.get("observations", [])
               if o.get("value") not in (None, ".", "")]
        if not obs:
            return {"id": sid, "ok": False, "note": "empty"}
        return {"id": sid, "ok": True, "latest_date": obs[0]["date"],
                "latest_value": obs[0]["value"]}
    except Exception as e:
        return {"id": sid, "ok": False, "note": str(e)[:120]}


candidates = ["IR3TIB01CHM156N", "IRLTLT01CHM156N", "CHECPIALLMINMEI",
              "DEXSZUS", "DEXUSEU", "SNBREMBALPOS", "MABMM301CHM189S",
              "INTDSRCHM193N"]
report["fred_direct"] = [fred_probe(s) for s in candidates]


# ── C. SNB data portal API probes ──
def snb_probe(cube, dimsel=None, label=""):
    base = f"https://data.snb.ch/api/cube/{cube}/data/csv/en"
    yr = datetime.now().year
    qs = f"?fromDate={yr-1}-01&toDate={yr}-12"
    if dimsel:
        qs = f"?dimSel={dimsel}&fromDate={yr-1}-01&toDate={yr}-12"
    try:
        raw = get(base + qs, timeout=35).decode("utf-8", "ignore")
        lines = [l for l in raw.splitlines() if l.strip()]
        # SNB CSV: ~3 metadata lines, then header, then data
        data_lines = lines[4:] if len(lines) > 5 else lines
        return {"cube": cube, "label": label, "ok": len(data_lines) > 0,
                "total_lines": len(lines),
                "header_block": lines[:5],
                "last_rows": lines[-3:]}
    except Exception as e:
        return {"cube": cube, "label": label, "ok": False,
                "note": str(e)[:140]}


report["snb_api"] = [
    snb_probe("rendoblid", "D0(10J0)", "Confederation bond yield 10Y (daily)"),
    snb_probe("snbbipo", None, "SNB balance sheet items (candidate)"),
    snb_probe("snbmoba", None, "SNB monetary base (candidate)"),
    snb_probe("snboffzid", None, "SNB sight deposits (candidate)"),
    snb_probe("snbgwd", None, "SNB sight deposits weekly (candidate)"),
]

report["verdict"] = ("Probe complete — inspect fred_direct[].ok, "
                     "fred_search_* and snb_api[].ok / header_block to pick "
                     "the confirmed series & cube IDs for justhodl-snb-detail.")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/788_snb_source_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/788_snb_source_probe.json")
