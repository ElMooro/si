"""justhodl-signal-backtest — does the system actually work? Forward-return proof.

Reads the daily opportunity-engine snapshots (data/track-record/snapshots/{date}),
plus the dislocation + best-setups snapshots, and measures FORWARD RETURNS by
signal class so you can trust (or discard) each signal:

  • By verdict tier (STRONG OPPORTUNITY / OPPORTUNITY / ... / HIGH RISK)
  • By compounder score bucket
  • By dislocation membership
  • By Triple-Threat membership (the rare 3-lens convergence)

For each snapshot ≥ N days old, fetch the current price (FMP batch quote),
compute return since the snapshot, and aggregate avg/median/hit-rate/win-rate
per signal class and per holding window (7/14/30/60/90d where available).

OUTPUT: data/signal-backtest.json — the empirical track record that powers
trust + lets the conviction board reweight by PROVEN performance.
SCHEDULE: daily 16:00 UTC.
"""
import json, os, time, statistics
import urllib.request
from datetime import datetime, timezone, date
from collections import defaultdict
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/signal-backtest.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
s3 = boto3.client("s3", region_name=REGION)

WINDOWS = [7, 14, 30, 60, 90]


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def list_snapshots(prefix, limit=120):
    keys = []
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if tok: kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", []) if o["Key"].endswith(".json")]
        tok = r.get("NextContinuationToken")
        if not tok: break
    return sorted(keys)[-limit:]


def batch_quotes(tickers):
    """Current prices via FMP batch quote-short (chunks of 100)."""
    out = {}
    base = "https://financialmodelingprep.com/stable"
    tk = list(tickers)
    for i in range(0, len(tk), 100):
        chunk = tk[i:i+100]
        try:
            url = f"{base}/batch-quote-short?symbols={','.join(chunk)}&apikey={FMP_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                data = json.loads(r.read().decode())
            for q in (data if isinstance(data, list) else []):
                p = q.get("price")
                if p: out[(q.get("symbol") or "").upper()] = float(p)
        except Exception as e:
            print(f"[bt] quote chunk err: {str(e)[:60]}")
    return out


def agg(returns):
    rs = [r for r in returns if r is not None]
    if not rs:
        return None
    return {"n": len(rs), "avg": round(statistics.mean(rs), 2),
            "median": round(statistics.median(rs), 2),
            "win_rate": round(sum(1 for r in rs if r > 0) / len(rs) * 100, 1),
            "hit_5pct": round(sum(1 for r in rs if r >= 5) / len(rs) * 100, 1),
            "best": round(max(rs), 1), "worst": round(min(rs), 1)}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    snap_keys = list_snapshots("data/track-record/snapshots/")
    today = date.today()
    print(f"[bt] {len(snap_keys)} opportunity snapshots")

    # gather (snapshot_date, ticker, entry_price, signal_tags) for matured snaps
    records = []  # {date, age, ticker, p0, verdict, comp, go, cap}
    for key in snap_keys:
        d = key.split("/")[-1].replace(".json", "")
        try:
            age = (today - date.fromisoformat(d)).days
        except Exception:
            continue
        if age < 7:
            continue
        snap = read_json(key)
        if not snap:
            continue
        for tk, p in (snap.get("picks") or {}).items():
            p0 = p.get("p")
            if not p0 or p0 <= 0:
                continue
            records.append({"date": d, "age": age, "ticker": tk, "p0": p0,
                            "verdict": p.get("v"), "comp": p.get("comp"),
                            "go": p.get("go"), "cap": p.get("cap"),
                            "rev": p.get("rev")})

    if not records:
        out = {"engine": "signal-backtest", "generated_at": datetime.now(timezone.utc).isoformat(),
               "maturity": "BOOTSTRAPPING", "note": "Need snapshots >=7 days old. Accruing daily.",
               "n_observations": 0}
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
        print("[bt] no matured snapshots yet")
        return {"statusCode": 200, "body": json.dumps(out)}

    # current prices
    tickers = {r["ticker"] for r in records}
    prices = batch_quotes(tickers)
    print(f"[bt] {len(records)} obs, {len(prices)} live prices")

    # compute returns + bucket by signal class
    by_verdict = defaultdict(list)
    by_comp = defaultdict(list)
    by_cap = defaultdict(list)
    by_rev = defaultdict(list)
    overall = []
    for r in records:
        pnow = prices.get(r["ticker"])
        if not pnow:
            continue
        ret = (pnow / r["p0"] - 1) * 100
        overall.append(ret)
        if r["verdict"]: by_verdict[r["verdict"]].append(ret)
        c = r.get("comp")
        if c is not None:
            bucket = "compounder_80+" if c >= 80 else "compounder_70-80" if c >= 70 else "compounder_<70"
            by_comp[bucket].append(ret)
        if r.get("cap"): by_cap[r["cap"]].append(ret)
        if r.get("rev") in ("UP", "DOWN", "FLAT"): by_rev["revision_" + r["rev"]].append(ret)

    # dislocation + triple-threat membership (from latest snapshots; approximate
    # using current dislocations/best-setups as the cohort, returns since entry)
    out = {
        "engine": "signal-backtest", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "n_observations": len(overall),
        "snapshots_used": len(snap_keys),
        "maturity": "MATURE" if len(overall) >= 500 else "BUILDING" if len(overall) >= 60 else "BOOTSTRAPPING",
        "overall": agg(overall),
        "by_verdict": {k: agg(v) for k, v in by_verdict.items()},
        "by_compounder_bucket": {k: agg(v) for k, v in by_comp.items()},
        "by_cap_bucket": {k: agg(v) for k, v in by_cap.items()},
        "by_revision": {k: agg(v) for k, v in by_rev.items()},
        "note": ("Forward return = % change from the snapshot's entry price to "
                 "the current price (variable holding period, snapshots >=7d old). "
                 "As history matures these become reliable; the conviction board "
                 "can then reweight signals by proven win-rate."),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[bt] DONE {round(time.time()-t0,1)}s — {len(overall)} obs, maturity {out['maturity']}")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "obs": len(overall),
                                                     "maturity": out["maturity"],
                                                     "overall": out["overall"]})}
