"""ops/808 — probe FMP /stable/ response shapes to fix boom-radar.

boom-radar shipped with bad field mappings: beat-streak is always 0 (wrong
earnings endpoint), revenue growth reads 100%+ for steady businesses, and
price targets are 2-4x reality (forward-EPS field mis-mapped). This dumps
the raw FMP /stable/ responses for two names so the fix uses the real
field names — no guessing.

No deploy — pure diagnostic.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

FMP = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
BASE = "https://financialmodelingprep.com/stable"


def get(path, params):
    url = f"{BASE}/{path}?apikey={FMP}{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "jh/1.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:140]}"}


report = {"ops": 808, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe FMP /stable/ field shapes for boom-radar fix"}

probes = {}
for sym in ("ARW", "EVR"):
    s = {}
    # quote
    q = get("quote", f"&symbol={sym}")
    q0 = q[0] if isinstance(q, list) and q else q
    s["quote_keys"] = sorted(q0.keys()) if isinstance(q0, dict) else q
    s["quote_price_mcap"] = ({k: q0.get(k) for k in
                              ("price", "marketCap", "eps", "pe")}
                             if isinstance(q0, dict) else None)
    # quarterly income statement
    inc = get("income-statement", f"&symbol={sym}&period=quarter&limit=6")
    if isinstance(inc, list) and inc:
        s["income_keys"] = sorted(inc[0].keys())
        s["income_6q"] = [{"date": r.get("date"),
                           "revenue": r.get("revenue"),
                           "eps": r.get("eps"),
                           "epsDiluted": r.get("epsDiluted"),
                           "epsdiluted": r.get("epsdiluted"),
                           "netIncome": r.get("netIncome")} for r in inc]
    else:
        s["income_raw"] = inc
    # analyst estimates
    for variant in [("analyst-estimates",
                     f"&symbol={sym}&period=annual&limit=3"),
                    ("analyst-estimates", f"&symbol={sym}&limit=3")]:
        est = get(variant[0], variant[1])
        if isinstance(est, list) and est:
            s["estimates_keys"] = sorted(est[0].keys())
            s["estimates_sample"] = est[:3]
            break
        else:
            s["estimates_raw_%s" % variant[1]] = est
    # earnings / surprises — find the beat-history endpoint
    for ep, pr in [("earnings", f"&symbol={sym}&limit=6"),
                    ("earnings-surprises", f"&symbol={sym}&limit=6"),
                    ("historical-earnings", f"&symbol={sym}&limit=6")]:
        r = get(ep, pr)
        if isinstance(r, list) and r:
            s["beat_endpoint_OK"] = ep
            s["beat_keys"] = sorted(r[0].keys())
            s["beat_sample"] = r[:4]
            break
        else:
            s["beat_%s_raw" % ep] = r
    probes[sym] = s

report["probes"] = probes
print(json.dumps(report, indent=2, default=str)[:6000])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/808_fmp_field_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/808_fmp_field_probe.json")
