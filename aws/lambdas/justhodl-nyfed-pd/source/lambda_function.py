"""justhodl-nyfed-pd — PRIMARY DEALER NET POSITIONS (the missing ledger column).

Ops-2727 proved the fleet never had real PD positions data (dealer-survey =
FOMC link tracker; the Aug-2025 shim never wrote a feed). This engine is the
real thing: NY Fed markets API Primary Dealer statistics — weekly NET
OUTRIGHT POSITIONS by security class, straight from the dealers' own
regulatory reporting. It is BOTH the institutional treasuries/credit read
and an independent cross-check on CFTC spec positioning.

  API        markets.newyorkfed.org/api/pd
               /list/timeseries.json -> {pd:{timeseries:[{keyid,description}]}}
               /get/{keyid}.json     -> {pd:{timeseries:[{asofdate,value}]}}
             (values may be "" or "*" — skip; unit = $ MILLIONS)
  SPEC       data/config/nyfed-pd-spec.json — catalog-discovered keyids per
             class (self-heals: if absent/empty, rediscovers from catalog).
  OUTPUT     data/nyfed-primary-dealer.json:
               net_positions_usd_b per class, net_treasury_total_b (flat
               alias the footprint desk reads), wow_b, z_52w, as_of, series.
             History: data/history/nyfed-pd.json (per-class, last 400 weeks).
  CONSUMERS  institutional-footprint asset ledger (TREASURIES pd column +
             primary_dealer_net), bond-desk cross-checks (future).
"""
import json, re, time, urllib.request, statistics
from datetime import datetime, timezone
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/nyfed-primary-dealer.json"
SPEC_KEY, HIST_KEY = "data/config/nyfed-pd-spec.json", "data/history/nyfed-pd.json"
BASE = "https://markets.newyorkfed.org/api/pd"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com", "Accept": "application/json"}
s3 = boto3.client("s3", region_name="us-east-1")

CLASS_PATTERNS = {  # applied to lower(description); all require net+position-ish
    "TREASURY_BILLS":   [r"\bbill"],
    "TREASURY_COUPONS": [r"coupon"],
    "TIPS":             [r"inflation[- ]protected|\btips\b"],
    "TREASURY_FRN":     [r"floating"],
    "AGENCY_DEBT":      [r"agency(?!.*mbs).*(debenture|debt)|debenture"],
    "AGENCY_MBS":       [r"mbs|mortgage-backed"],
    "CORPORATE":        [r"corporate"],
    "MUNICIPAL":        [r"municipal|state and local"],
    "ABS":              [r"asset-backed"],
}

def _j(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _get(url, timeout=25):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return json.loads(r.read())

def _discover():
    cat = _get(BASE + "/list/timeseries.json", 35)
    rows = (cat or {}).get("pd", {}).get("timeseries", [])
    spec = {c: [] for c in CLASS_PATTERNS}
    for r in rows:
        kid, desc = r.get("keyid"), str(r.get("description") or "").lower()
        if not kid or "net" not in desc: continue
        if not ("position" in desc or "outright" in desc): continue
        if "fail" in desc or "financing" in desc or "transaction" in desc: continue
        for cls, pats in CLASS_PATTERNS.items():
            if any(re.search(p, desc) for p in pats):
                spec[cls].append({"keyid": kid, "desc": desc[:110]}); break
    spec = {c: v for c, v in spec.items() if v}
    doc = {"classes": spec, "discovered": datetime.now(timezone.utc).isoformat(),
           "unit": "USD millions", "n_series": sum(len(v) for v in spec.values())}
    s3.put_object(Bucket=BUCKET, Key=SPEC_KEY, Body=json.dumps(doc).encode(),
                  ContentType="application/json")
    print("[pd] discovered %d series across %d classes" % (doc["n_series"], len(spec)))
    return doc

def lambda_handler(event=None, context=None):
    spec = _j(SPEC_KEY) or {}
    if not spec.get("classes") or (event or {}).get("rediscover"):
        spec = _discover()
    classes = spec["classes"]
    per_class = {}
    series_used = {}
    for cls, items in classes.items():
        agg = {}   # asofdate -> summed value ($M)
        used = []
        for it in items[:14]:
            kid = it["keyid"]
            try:
                obs = _get("%s/get/%s.json" % (BASE, kid)).get("pd", {}).get("timeseries", [])
            except Exception as e:
                print("[pd] %s fetch err %s" % (kid, str(e)[:60])); continue
            n = 0
            for o in obs:
                v, d = o.get("value"), o.get("asofdate")
                if v in (None, "", "*") or not d: continue
                try: agg[d] = agg.get(d, 0.0) + float(v); n += 1
                except Exception: continue
            if n: used.append(kid)
            time.sleep(0.2)
        if agg:
            per_class[cls] = dict(sorted(agg.items()))
            series_used[cls] = used
    assert per_class, "no PD series parsed — catalog drift?"

    hist = _j(HIST_KEY, {}) or {}
    net_b, wow_b, z52 = {}, {}, {}
    as_of = None
    for cls, ser in per_class.items():
        dates = sorted(ser)
        vals_b = [ser[d] / 1e3 for d in dates]           # $M -> $B
        net_b[cls] = round(vals_b[-1], 1)
        wow_b[cls] = round(vals_b[-1] - vals_b[-2], 1) if len(vals_b) >= 2 else None
        w = vals_b[-52:]
        z52[cls] = round((vals_b[-1] - statistics.mean(w)) / statistics.stdev(w), 2) \
                   if len(w) >= 20 and statistics.stdev(w) > 0 else None
        as_of = max(as_of or dates[-1], dates[-1])
        hist[cls] = {d: round(ser[d] / 1e3, 1) for d in dates[-400:]}
    tsy = round(sum(net_b.get(c, 0) for c in
                    ("TREASURY_BILLS", "TREASURY_COUPONS", "TIPS", "TREASURY_FRN")), 1)
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist).encode(),
                  ContentType="application/json")
    doc = {"engine": "justhodl-nyfed-pd", "version": "1.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "as_of": as_of,
           "net_treasury_total_b": tsy,                  # flat alias for footprint _find
           "net_positions_usd_b": net_b, "wow_usd_b": wow_b, "z_52w": z52,
           "series_used": series_used,
           "source": "NY Fed Primary Dealer Statistics (markets.newyorkfed.org/api/pd), "
                     "weekly net outright positions, $B (reported $M)",
           "read": ("Dealers net %s $%.0fB UST%s" % (
                    "LONG" if tsy > 0 else "SHORT", abs(tsy),
                    "" if wow_b.get("TREASURY_COUPONS") is None else
                    ", coupons WoW %+.1fB" % wow_b["TREASURY_COUPONS"]))}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"ok": True, "as_of": as_of, "classes": len(net_b),
            "net_treasury_total_b": tsy, "net_b": net_b}
