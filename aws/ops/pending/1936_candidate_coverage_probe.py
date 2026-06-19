"""ops 1936 — Candidate leveraged/inverse/thematic ETF coverage probe.

Goal: user wants the ETF universe expanded "as much as possible" with 2x/3x
long & short ETFs to read institutional/retail positioning. Before adding any
ticker to the deployed universe (which would pollute the board with dead
entries), probe each CANDIDATE directly against the live ETF Global fund-flows
endpoint the engine itself uses. Report CONFIRMED (real data) vs DEAD.

Honest method: integrate only confirmed tickers in the next op.

No universe mutation here. Read-only probe.
"""
import json, urllib.request, urllib.error, concurrent.futures as cf
from datetime import datetime, timezone, timedelta
import boto3

FN = "justhodl-etf-fund-flows"
ENDPOINT = "https://api.polygon.io/etf-global/v1/fund-flows"

lam = boto3.client("lambda", region_name="us-east-1")

# Pull the exact key the deployed engine uses so the probe matches production.
cfg = lam.get_function_configuration(FunctionName=FN)
env = (cfg.get("Environment") or {}).get("Variables") or {}
KEY = env.get("POLYGON_KEY") or env.get("MASSIVE_API_KEY") or ""
print("key source:", "POLYGON_KEY" if env.get("POLYGON_KEY") else ("MASSIVE_API_KEY" if env.get("MASSIVE_API_KEY") else "NONE"),
      "| len:", len(KEY))

# Candidate tickers NOT currently in the 246-name universe.
CANDIDATES = {
    # --- Broad index leverage gaps ---
    "URTY": "3x Russell2000 long",   "SRTY": "3x Russell2000 short",
    "UMDD": "3x MidCap long",        "SMDD": "3x MidCap short",
    "TTT":  "3x 20Y Treasury short",
    # --- ProShares Ultra/UltraShort 2x sector pair completion ---
    "RXD": "2x Health short",        "SDP": "2x Utilities short",
    "SIJ": "2x Industrials short",   "UGE": "2x Staples long",
    "SZK": "2x Staples short",       "SCC": "2x ConsDisc short",
    "FXP": "2x China short",         "XPP": "2x China long",
    # --- Treasury 2x long/short pairs ---
    "TBT": "2x 20Y short",  "UBT": "2x 20Y long",  "TBF": "1x 20Y short",
    "PST": "2x 7-10Y short","UST": "2x 7-10Y long",
    # --- Commodity leverage gaps ---
    "DGP": "2x Gold long",  "DZZ": "2x Gold short",
    "GDXU": "2x GoldMiners long", "GDXD": "2x GoldMiners short",
    # --- Crypto leverage gaps ---
    "ETHD": "2x Ether short", "BTCL": "2x Bitcoin long", "ETU": "2x Ether long",
    # --- International leverage gaps ---
    "CHAD": "3x China short", "CHAU": "2x China long", "JPNL": "3x Japan long",
    # --- Single-stock leverage expansion (the big growth area) ---
    "NFXL": "2x NFLX long",  "NFXS": "2x NFLX short",
    "MUU":  "2x MU long",    "MUD":  "2x MU short",
    "BABX": "2x BABA long",  "AAPB": "2x AAPL long",
    "NVDD": "2x NVDA short", "SMCX": "2x SMCI long",
    "PLTD": "2x PLTR short", "COII": "2x COIN short alt",
    "GGLU": "2x GOOGL long alt", "TSLZ": "2x TSLA short alt",
    "AMUU": "2x AMZN long alt",  "BRKU": "2x BRK long",
    "HOOX": "2x HOOD long",  "MARA2": "lev MARA",
    "TSMX": "2x TSM long",   "CRWL": "2x CRWD long",
    "AVL":  "2x AVGO long",  "DELL2": "lev DELL",
    "MSTY": "MSTR income",   "QQQU": "2x QQQ long alt",
    # --- High-value thematic 1x (improve complex breadth) ---
    "ROBO": "Robotics", "IRBO": "AI/Robotics", "QTUM": "Quantum",
    "BUG": "Cybersec",  "ARKX": "Space",       "MSOS": "Cannabis",
    "NLR": "Nuclear",   "PPA": "Defense",      "XSD": "Semis EW",
    "FTEC": "Tech",     "PHO": "Water",        "XLG": "MegaCap",
}

def probe(t):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=40)
    url = (f"{ENDPOINT}?composite_ticker={t}"
           f"&processed_date.gte={start.isoformat()}"
           f"&processed_date.lte={end.isoformat()}"
           f"&order=desc&sort=processed_date&limit=10&apiKey={KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        res = d.get("results") or []
        if not res:
            return (t, False, None, None, d.get("status"))
        latest = res[0]
        nav = latest.get("nav"); sh = latest.get("shares_outstanding")
        aum = (nav * sh) if (nav and sh) else None
        return (t, True, latest.get("fund_flow"), aum, latest.get("processed_date"))
    except urllib.error.HTTPError as e:
        return (t, False, None, None, f"HTTP{e.code}")
    except Exception as e:
        return (t, False, None, None, str(e)[:40])

results = {}
with cf.ThreadPoolExecutor(max_workers=12) as ex:
    futs = {ex.submit(probe, t): t for t in CANDIDATES}
    for f in cf.as_completed(futs):
        r = f.result(); results[r[0]] = r

confirmed = sorted([r for r in results.values() if r[1]], key=lambda x: -(abs(x[2] or 0)))
dead = sorted([t for t, r in results.items() if not r[1]])

print(f"\n=== CONFIRMED ({len(confirmed)}) — real ETF Global coverage ===")
for t, ok, flow, aum, dt in confirmed:
    print(f"  {t:7s} {CANDIDATES[t]:24s} flow={flow}  aum={aum}  asof={dt}")

print(f"\n=== DEAD ({len(dead)}) — no coverage / not a real ETF ===")
for t in dead:
    print(f"  {t:7s} {CANDIDATES[t]:24s} reason={results[t][4]}")

print("\nCONFIRMED_LIST:", " ".join(sorted([r[0] for r in confirmed])))
