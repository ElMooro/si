"""
justhodl-estimate-revisions v1.0 — analyst estimates + proprietary revisions
============================================================================
FMP /stable/analyst-estimates gives current-snapshot estimates per fiscal
year. Revisions are OURS: we snapshot per name, and when a fresh fetch
differs from a >=7-day-old prior, the delta becomes eps/revenue revision %.
Day-one value even before revisions age in: forward growth (fy2 eps / fy1).
Universe: valuations S&P 500 + HP 400 (~900 names), daily budget+cursor.
Output: data/estimate-revisions.json {tickers, movers_up/down, breadth}.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FMP = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
STATE_KEY = "data/_estrev/state.json"
OUT_KEY = "data/estimate-revisions.json"
BUDGET_S = 240
MIN_AGE_D = 7
VERSION = "1.0.0"
DIAG = []


def jget(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def fetch_est(sym):
    j = jget(f"https://financialmodelingprep.com/stable/analyst-estimates"
              f"?symbol={sym}&period=annual&limit=4&apikey={FMP}")
    rows = j if isinstance(j, list) else []
    rows = sorted((r for r in rows if r.get("date")), key=lambda r: r["date"])
    today = datetime.now(timezone.utc).date().isoformat()
    fut = [r for r in rows if r["date"] >= today[:4]]
    if not fut:
        fut = rows[-2:]
    out = {}
    for i, r in enumerate(fut[:2]):
        out[f"fy{i+1}"] = {"eps": f(r.get("epsAvg") or r.get("estimatedEpsAvg")),
                            "rev": f(r.get("revenueAvg") or r.get("estimatedRevenueAvg")),
                            "nA": r.get("numAnalystsEps") or r.get("numberAnalystsEstimatedEps"),
                            "y": r.get("date", "")[:4]}
    return out or None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    sv = json.loads(S3.get_object(Bucket=BUCKET, Key="data/stock-valuations.json")["Body"].read())
    uni = sorted({r["t"] for r in (sv.get("sp_table") or [])}
                  | {x["t"] for x in (sv.get("hp") or [])})
    try:
        st = json.loads(S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    except Exception:
        st = {"est": {}, "cursor": 0}
    est = st.get("est") or {}
    cur_i = st.get("cursor", 0) % max(1, len(uni))
    today = datetime.now(timezone.utc).date().isoformat()
    n_fetched = n_rolled = 0
    i = cur_i
    while time.time() - t0 < BUDGET_S and n_fetched < len(uni):
        sym = uni[i % len(uni)]
        i += 1
        n_fetched += 1
        try:
            new = fetch_est(sym)
        except Exception:
            continue
        if not new or not (new.get("fy1") or {}).get("eps"):
            continue
        slot = est.get(sym) or {}
        cur = slot.get("cur")
        if cur and slot.get("cur_date") and (
                datetime.fromisoformat(today) - datetime.fromisoformat(slot["cur_date"])
        ).days >= MIN_AGE_D and json.dumps(cur) != json.dumps(new):
            slot["prev"], slot["prev_date"] = cur, slot["cur_date"]
            n_rolled += 1
        slot["cur"], slot["cur_date"] = new, today
        est[sym] = slot
    st = {"est": est, "cursor": i % max(1, len(uni))}
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(st).encode(),
                  ContentType="application/json")
    tickers = {}
    ups, downs, flat = [], [], 0
    for sym, slot in est.items():
        cur = slot.get("cur") or {}
        fy1, fy2 = cur.get("fy1") or {}, cur.get("fy2") or {}
        row = {"eps_fy1": fy1.get("eps"), "rev_fy1": fy1.get("rev"),
                "n_analysts": fy1.get("nA"), "fy1": fy1.get("y"),
                "asof": slot.get("cur_date")}
        if fy1.get("eps") and fy2.get("eps") and fy1["eps"] > 0:
            row["est_g_pct"] = round((fy2["eps"] / fy1["eps"] - 1) * 100, 1)
        prev = slot.get("prev") or {}
        p1 = (prev.get("fy1") or {})
        if p1.get("eps") and fy1.get("eps") and abs(p1["eps"]) > 0.01:
            rv = round((fy1["eps"] / p1["eps"] - 1) * 100, 1)
            row["eps_rev_pct"] = rv
            row["rev_window_d"] = (datetime.fromisoformat(slot["cur_date"])
                                     - datetime.fromisoformat(slot["prev_date"])).days
            if rv >= 1:
                ups.append({"t": sym, "rv": rv, "eps_fy1": fy1["eps"]})
            elif rv <= -1:
                downs.append({"t": sym, "rv": rv, "eps_fy1": fy1["eps"]})
            else:
                flat += 1
        tickers[sym] = row
    ups.sort(key=lambda x: -x["rv"])
    downs.sort(key=lambda x: x["rv"])
    out = {"engine": "estimate-revisions", "version": VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "duration_s": round(time.time() - t0, 1),
            "coverage": len(tickers), "universe": len(uni),
            "with_revisions": len(ups) + len(downs) + flat,
            "breadth": {"up": len(ups), "down": len(downs), "flat": flat},
            "movers_up": ups[:20], "movers_down": downs[:20],
            "tickers": tickers, "diagnostics": list(DIAG) + [
                f"fetched {n_fetched} this run, rolled {n_rolled} prior snapshots"],
            "methodology": ("Analyst consensus snapshots (FMP analyst-estimates, annual "
                             "fy1/fy2). Revisions are proprietary: computed only when our "
                             "own prior snapshot is >=7 days old and differs — they age in "
                             "over the first weeks. est_g_pct = fy2/fy1 EPS growth, "
                             "available immediately. Feeds the HP rerating pillar, S&P "
                             "table and heatmap. Research, not advice.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[estrev] cov {len(tickers)}/{len(uni)} · rev {out['with_revisions']} · "
           f"{out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"coverage": len(tickers)})}
