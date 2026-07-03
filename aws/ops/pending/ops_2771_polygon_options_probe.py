"""ops 2771 — VERIFY Polygon options entitlement + prove GEX is buildable on it.
Read-only. Pulls SPY option-chain snapshot (greeks+OI+spot) with the existing
POLYGON_KEY and, if entitled, computes a sample net Gamma Exposure across strikes
as an end-to-end proof-of-capability. Determines whether an Options Flow + GEX
desk can be built on data Khalid already pays for, or needs an add-on.
Report: 2771_polygon_options_probe.json.
"""
import os, json, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
lam = boto3.client("lambda", region_name="us-east-1")
R = {"ops": 2771, "ts": datetime.now(timezone.utc).isoformat()}

# source Polygon key from a function that carries it
key = None
for donor in ("justhodl-theme-rotation", "justhodl-portfolio-risk", "justhodl-khalid-metrics", "justhodl-backtest-engine"):
    try:
        env = (lam.get_function_configuration(FunctionName=donor).get("Environment", {}) or {}).get("Variables", {}) or {}
        key = env.get("POLYGON_KEY") or env.get("POLY_KEY") or env.get("POLYGON_API_KEY")
        if key:
            print("key sourced from", donor, "(len %d)" % len(key)); break
    except ClientError:
        continue
assert key, "no Polygon key found"

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.status, json.loads(r.read())

# 1) option chain snapshot for SPY (near-the-money band)
url = ("https://api.polygon.io/v3/snapshot/options/SPY?limit=250&order=asc&sort=strike_price&apikey=" + key)
try:
    st, doc = get(url)
    R["http"] = st; R["status_field"] = doc.get("status")
    results = doc.get("results") or []
    R["n_contracts"] = len(results)
    have_g = [x for x in results if (x.get("greeks") or {}).get("gamma") is not None]
    have_oi = [x for x in results if x.get("open_interest") is not None]
    spot = None
    for x in results:
        ua = x.get("underlying_asset") or {}
        if ua.get("price"):
            spot = ua.get("price"); break
    R["has_greeks"] = len(have_g); R["has_open_interest"] = len(have_oi); R["spot"] = spot
    print("http=%s status=%s contracts=%d w/greeks=%d w/OI=%d spot=%s" % (
        st, doc.get("status"), len(results), len(have_g), len(have_oi), spot))
    if results:
        s0 = results[0]
        R["sample_contract_keys"] = list(s0)
        R["sample_greeks_keys"] = list((s0.get("greeks") or {}))
        print("  sample keys:", list(s0))
        print("  sample greeks:", s0.get("greeks"))
    # 2) compute sample net GEX if entitled
    if have_g and have_oi and spot:
        gex = 0.0; ncalls = nputs = 0
        by_strike = {}
        for x in have_g:
            g = (x.get("greeks") or {}).get("gamma")
            oi = x.get("open_interest") or 0
            det = x.get("details") or {}
            ctype = det.get("contract_type"); strike = det.get("strike_price")
            if g is None or not oi:
                continue
            sign = 1.0 if ctype == "call" else -1.0
            dollar_gamma = g * oi * 100 * spot * spot * 0.01 * sign
            gex += dollar_gamma
            by_strike[strike] = by_strike.get(strike, 0.0) + dollar_gamma
            if ctype == "call": ncalls += 1
            else: nputs += 1
        R["sample_net_gex_usd_per_1pct"] = round(gex)
        R["sample_gex_billions"] = round(gex / 1e9, 3)
        # crude gamma-flip proxy: strike where cumulative GEX crosses zero
        R["strikes_covered"] = len(by_strike)
        R["calls_used"] = ncalls; R["puts_used"] = nputs
        print("  SAMPLE NET GEX: $%.2fB per 1%% (calls=%d puts=%d strikes=%d)" % (gex / 1e9, ncalls, nputs, len(by_strike)))
        R["verdict"] = "OPTIONS_ENTITLED — GEX buildable on existing Polygon key (no new subscription)"
    else:
        R["verdict"] = "greeks/OI/spot missing — options DATA entitlement likely NOT on current plan"
except urllib.error.HTTPError as e:
    body = e.read().decode("utf-8", "ignore")[:200]
    R["http_error"] = "%s: %s" % (e.code, body)
    R["verdict"] = "NOT_AUTHORIZED — options entitlement not on plan" if e.code in (401, 403) else ("error %s" % e.code)
    print("HTTP ERROR", e.code, body)
except Exception as e:
    R["err"] = str(e)[:160]; R["verdict"] = "probe error"
    print("ERR", str(e)[:120])

# 3) also confirm the trades feed (for options flow / sweeps) reachability
try:
    st2, doc2 = get("https://api.polygon.io/v3/trades/O:SPY251219C00600000?limit=5&apikey=" + key)
    R["options_trades_http"] = st2; R["options_trades_status"] = doc2.get("status")
    R["options_trades_available"] = bool(doc2.get("results"))
    print("options trades feed: http=%s results=%s" % (st2, bool(doc2.get("results"))))
except urllib.error.HTTPError as e:
    R["options_trades_http_error"] = "%s" % e.code
    print("options trades HTTP", e.code)
except Exception as e:
    R["options_trades_err"] = str(e)[:80]

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2771_polygon_options_probe.json", "w"), indent=1, default=str)
print("\nVERDICT:", R.get("verdict"))
print("OPS 2771 COMPLETE")
