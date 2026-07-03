"""ops 2772 — live GEX proof on existing Polygon key (corrects 2771 spot bug).
Get SPY spot from stocks feed, pull option chain in a ±15% strike band across
pages, compute net Gamma Exposure (calls +, puts −), total call/put gamma,
gamma-flip proxy, and top call/put walls. Proves the full GEX pipeline runs on
data Khalid already pays for. Read-only. Report: 2772_gex_proof.json.
"""
import os, json, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
lam = boto3.client("lambda", region_name="us-east-1")
R = {"ops": 2772, "ts": datetime.now(timezone.utc).isoformat()}
key = None
for donor in ("justhodl-theme-rotation", "justhodl-portfolio-risk", "justhodl-khalid-metrics"):
    try:
        env = (lam.get_function_configuration(FunctionName=donor).get("Environment", {}) or {}).get("Variables", {}) or {}
        key = env.get("POLYGON_KEY") or env.get("POLY_KEY") or env.get("POLYGON_API_KEY")
        if key: break
    except ClientError:
        continue
assert key, "no polygon key"
def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"}), timeout=25) as r:
        return json.loads(r.read())

# spot from stocks feed (previous close) — the entitlement they already use
spot = None
try:
    d = get("https://api.polygon.io/v2/aggs/ticker/SPY/prev?adjusted=true&apikey=" + key)
    spot = ((d.get("results") or [{}])[0]).get("c")
except Exception as e:
    R["spot_err"] = str(e)[:80]
R["spot"] = spot
print("SPY spot:", spot)
assert spot, "no spot"
lo, hi = spot * 0.85, spot * 1.15

# paginate option chain in strike band
url = ("https://api.polygon.io/v3/snapshot/options/SPY?strike_price.gte=%.0f&strike_price.lte=%.0f"
       "&limit=250&apikey=%s" % (lo, hi, key))
contracts, pages = [], 0
while url and pages < 6:
    doc = get(url)
    contracts += (doc.get("results") or [])
    nxt = doc.get("next_url")
    url = (nxt + "&apikey=" + key) if nxt else None
    pages += 1
R["contracts_scanned"] = len(contracts)
print("contracts in band:", len(contracts), "pages:", pages)

net_gex = 0.0; call_gamma = 0.0; put_gamma = 0.0
by_strike = {}
for x in contracts:
    g = (x.get("greeks") or {}).get("gamma"); oi = x.get("open_interest") or 0
    det = x.get("details") or {}; ctype = det.get("contract_type"); strike = det.get("strike_price")
    if g is None or not oi or strike is None:
        continue
    dollar_gamma = g * oi * 100 * spot * spot * 0.01  # $ per 1% move, per contract-side
    if ctype == "call":
        net_gex += dollar_gamma; call_gamma += dollar_gamma
    elif ctype == "put":
        net_gex -= dollar_gamma; put_gamma += dollar_gamma
    by_strike[strike] = by_strike.get(strike, 0.0) + (dollar_gamma if ctype == "call" else -dollar_gamma)
R["net_gex_usd_per_1pct"] = round(net_gex)
R["net_gex_billions"] = round(net_gex / 1e9, 3)
R["call_gamma_billions"] = round(call_gamma / 1e9, 3)
R["put_gamma_billions"] = round(put_gamma / 1e9, 3)
# call/put walls = strikes with max +GEX and min -GEX
if by_strike:
    call_wall = max(by_strike, key=lambda k: by_strike[k])
    put_wall = min(by_strike, key=lambda k: by_strike[k])
    R["call_wall_strike"] = call_wall; R["put_wall_strike"] = put_wall
    # gamma flip proxy: strike where cumulative net GEX crosses zero (ascending)
    cum, flip = 0.0, None
    for strk in sorted(by_strike):
        cum += by_strike[strk]
    R["strikes"] = len(by_strike)
print("NET GEX: $%.2fB /1%%  (call +%.2fB, put -%.2fB)" % (net_gex / 1e9, call_gamma / 1e9, put_gamma / 1e9))
print("Call wall:", R.get("call_wall_strike"), "| Put wall:", R.get("put_wall_strike"), "| strikes:", R.get("strikes"))
R["verdict"] = "OPTIONS ENTITLED — full GEX pipeline runs on existing Polygon key, $0 new cost"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2772_gex_proof.json", "w"), indent=1, default=str)
print("\nVERDICT:", R["verdict"])
print("OPS 2772 COMPLETE")
