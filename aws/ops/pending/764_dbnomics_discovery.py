"""ops/764 — DBnomics series discovery for Canary Grid Phase 3.

Finds and freshness-validates the correct DBnomics series for the signals
FRED serves badly: Korea exports, China exports, Swiss unemployment (all
flagged by the freshness guard), plus the new canaries (Taiwan export
orders, Swiss KOF barometer, Chile/Peru copper output).

For each target it: (1) searches DBnomics for matching datasets, (2) for
the top datasets pulls candidate series WITH their latest observation, so
we can pick the code that is both correct and FRESH (<3 months old).
Outputs the ranked candidates to aws/ops/reports/764_*.json.
"""
import json, os, urllib.parse, urllib.request
from datetime import datetime, timezone, date

API = "https://api.db.nomics.world/v22"
report = {"ops": 764, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "DBnomics series discovery — Canary Grid Phase 3"}


def get(path):
    url = f"{API}/{path}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "justhodl-ops/764", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.loads(r.read())


def age_days(period):
    """Approx age in days of a DBnomics period string (YYYY, YYYY-MM, YYYY-MM-DD)."""
    try:
        p = str(period)
        if len(p) == 4:
            d = date(int(p), 7, 1)
        elif len(p) == 7:
            d = date(int(p[:4]), int(p[5:7]), 15)
        else:
            d = date.fromisoformat(p[:10])
        return (datetime.now(timezone.utc).date() - d).days
    except Exception:
        return None


def search_datasets(query, limit=6):
    try:
        d = get(f"search?q={urllib.parse.quote(query)}&limit={limit}")
        docs = (d.get("results") or {}).get("docs") or []
        return [{"provider": x.get("provider_code"), "dataset": x.get("code"),
                 "name": x.get("name"), "nb_series": x.get("nb_series")}
                for x in docs]
    except Exception as e:
        return [{"error": str(e)[:160]}]


def probe_series(provider, dataset, refine, limit=6):
    """Pull candidate series in a dataset (filtered by refine term) + latest obs."""
    try:
        q = urllib.parse.quote(refine)
        d = get(f"series/{provider}/{dataset}?q={q}&observations=1&limit={limit}")
        docs = (d.get("series") or {}).get("docs") or []
        out = []
        for x in docs:
            periods = x.get("period") or []
            values = x.get("value") or []
            last_p, last_v = None, None
            for p, v in zip(periods, values):
                if v is not None and v != "NA":
                    last_p, last_v = p, v
            out.append({
                "series_id": f"{provider}/{dataset}/{x.get('series_code')}",
                "name": x.get("series_name"),
                "freq": x.get("@frequency") or x.get("frequency"),
                "last_period": last_p, "last_value": last_v,
                "age_days": age_days(last_p), "n_obs": len(periods)})
        return out
    except Exception as e:
        return [{"error": str(e)[:160]}]


TARGETS = [
    ("korea_exports",   "South Korea exports goods",      "export"),
    ("china_exports",   "China exports goods value",      "export"),
    ("swiss_unemploy",  "Switzerland unemployment rate",  "unemploy"),
    ("taiwan_exports",  "Taiwan export orders",           "export order"),
    ("kof_barometer",   "Switzerland KOF economic barometer", "barometer"),
    ("chile_copper",    "Chile copper production",        "copper"),
    ("peru_copper",     "Peru copper production",         "copper"),
]

findings = {}
for key, query, refine in TARGETS:
    entry = {"query": query, "datasets": [], "candidates": []}
    dsets = search_datasets(query, limit=6)
    entry["datasets"] = dsets
    for ds in dsets[:3]:
        if not ds.get("provider") or not ds.get("dataset"):
            continue
        cands = probe_series(ds["provider"], ds["dataset"], refine, limit=5)
        for c in cands:
            if "series_id" in c:
                entry["candidates"].append(c)
    # rank: prefer candidates with a real recent observation (fresh first)
    entry["candidates"].sort(
        key=lambda c: (c.get("age_days") is None, c.get("age_days") or 99999))
    entry["candidates"] = entry["candidates"][:8]
    fresh = [c for c in entry["candidates"]
             if c.get("age_days") is not None and c["age_days"] <= 95]
    entry["best_fresh"] = fresh[0] if fresh else None
    findings[key] = entry
    print(f"[764] {key}: {len(entry['candidates'])} candidates, "
          f"best_fresh={'yes' if fresh else 'NONE'}")

report["findings"] = findings
report["summary"] = {k: (v["best_fresh"]["series_id"] + f"  ({v['best_fresh']['age_days']}d old)"
                         if v.get("best_fresh") else "no fresh candidate found")
                     for k, v in findings.items()}

print(json.dumps(report["summary"], indent=2))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/764_dbnomics_discovery.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/764_dbnomics_discovery.json")
