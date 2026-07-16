"""justhodl-finviz-universe — pull the whole US equity universe from Finviz Elite export
across 5 views (overview/ownership/technical/performance/valuation), merge by ticker, and
publish:
  data/finviz-universe.json  — full by_ticker record (~11.3k tickers, ~40 fields)
  data/finviz-short.json     — slim short-float index for cheap squeeze/flow consumption

This is the canonical whole-market snapshot: one authenticated call per view replaces
thousands of per-ticker FMP fetches and fills gaps FMP can't (short float, float, rel-vol).
"""
import json
import time
from datetime import datetime, timezone
import boto3
import finviz as FV

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def lambda_handler(event=None, context=None):
    started = time.time()
    uni = FV.build_universe()
    n = len(uni)
    n_short = sum(1 for r in uni.values() if r.get("short_float_pct") is not None)
    n_relvol = sum(1 for r in uni.values() if r.get("rel_volume") is not None)
    n_float = sum(1 for r in uni.values() if r.get("float_shares") is not None)
    now = datetime.now(timezone.utc).isoformat()

    s3.put_object(
        Bucket=BUCKET, Key="data/finviz-universe.json",
        Body=json.dumps({"generated_at": now, "source": "finviz-elite-export",
                         "n_tickers": n, "n_with_short_float": n_short, "by_ticker": uni},
                        separators=(",", ":"), default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=900")

    slim = {tk: {k: r.get(k) for k in ("short_float_pct", "short_ratio", "float_shares", "rel_volume", "avg_volume")
                 if r.get(k) is not None}
            for tk, r in uni.items() if r.get("short_float_pct") is not None}
    s3.put_object(
        Bucket=BUCKET, Key="data/finviz-short.json",
        Body=json.dumps({"generated_at": now, "n": len(slim), "by_ticker": slim},
                        separators=(",", ":"), default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=900")

    # ── sector heatmap aggregation (whole-market perf by sector) ──
    from collections import defaultdict
    secs = defaultdict(list)
    for r in uni.values():
        if r.get("sector") and r.get("perf_m") is not None:
            secs[r["sector"]].append(r)
    def _capw(rows, key):  # market-cap-weighted perf = the sector's actual move (not micro-cap-skewed mean)
        num = den = 0.0
        for x in rows:
            v = x.get(key); mc = x.get("market_cap")
            if v is not None and mc:
                num += v * mc; den += mc
        return round(num / den, 2) if den else None
    def _med(a):
        a = sorted(a); return round(a[len(a)//2], 2) if a else None
    heat = []
    for sec, rows in secs.items():
        rs = sorted(rows, key=lambda x: x.get("perf_m") or 0)
        heat.append({
            "sector": sec, "n": len(rows),
            "avg_perf_w": _capw(rows, "perf_w"),
            "avg_perf_m": _capw(rows, "perf_m"),
            "avg_perf_ytd": _capw(rows, "perf_ytd"),
            "median_perf_m": _med([x["perf_m"] for x in rows if x.get("perf_m") is not None]),
            "total_mktcap_b": round(sum((x.get("market_cap") or 0) for x in rows) / 1000, 1),
            "top": [{"ticker": x["ticker"], "perf_m": x.get("perf_m"), "mktcap_m": x.get("market_cap")} for x in rs[-6:][::-1]],
            "bottom": [{"ticker": x["ticker"], "perf_m": x.get("perf_m")} for x in rs[:6]],
        })
    heat.sort(key=lambda x: x["avg_perf_m"] if x["avg_perf_m"] is not None else -999, reverse=True)
    s3.put_object(Bucket=BUCKET, Key="data/finviz-heatmap.json",
                  Body=json.dumps({"generated_at": now, "n_sectors": len(heat), "sectors": heat},
                                  separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")

    # ── earnings calendar (derived from earnings_date — zero extra API calls) ──
    from datetime import datetime as _dt, timedelta as _td
    _today = datetime.now(timezone.utc).date()
    _horizon = _today + _td(days=30)
    _bd = {}
    for tk, r in uni.items():
        ed = r.get("earnings_date")
        if not ed:
            continue
        dt = None
        for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y"):
            try:
                dt = _dt.strptime(str(ed).strip(), fmt); break
            except Exception:
                pass
        if not dt or not (_today <= dt.date() <= _horizon):
            continue
        when = "AMC" if dt.hour >= 16 else ("BMO" if 0 < dt.hour < 10 else "—")
        _bd.setdefault(dt.date().isoformat(), []).append({
            "ticker": tk, "company": r.get("company"), "sector": r.get("sector"),
            "time": when, "mktcap_m": r.get("market_cap"), "perf_m": r.get("perf_m"),
            "analyst_recom": r.get("analyst_recom")})
    cal = []
    for d in sorted(_bd):
        reps = sorted(_bd[d], key=lambda x: -(x.get("mktcap_m") or 0))
        cal.append({"date": d, "n": len(reps), "reporters": reps[:60]})
    s3.put_object(Bucket=BUCKET, Key="data/finviz-earnings-calendar.json",
                  Body=json.dumps({"generated_at": now, "horizon_days": 30, "n_days": len(cal),
                                   "n_reporters": sum(c["n"] for c in cal), "calendar": cal},
                                  separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print("[finviz-universe] earnings calendar: %d days, %d reporters next 30d"
          % (len(cal), sum(c["n"] for c in cal)))

    # ── ETF fund-flows layer (REAL net flows from Finviz vs the legacy volume proxy) ──
    SECTOR_ETFS = {"XLK": "Technology", "XLF": "Financial", "XLE": "Energy", "XLV": "Healthcare",
                   "XLY": "Consumer Cyclical", "XLP": "Consumer Defensive", "XLI": "Industrials",
                   "XLB": "Basic Materials", "XLU": "Utilities", "XLRE": "Real Estate",
                   "XLC": "Communication Services"}
    def _ef(tk, r):
        return {"ticker": tk, "name": r.get("company"), "aum": r.get("aum"),
                "flows_1m": r.get("flows_1m"), "flows_1m_pct": r.get("flows_1m_pct"),
                "flows_3m": r.get("flows_3m"), "flows_ytd": r.get("flows_ytd"),
                "flows_1y": r.get("flows_1y"),
                "expense": r.get("expense_ratio"), "n_holdings": r.get("n_holdings"),
                "ret_1y": r.get("ret_1y"), "etf_type": r.get("etf_type")}
    _etfs = [(tk, r) for tk, r in uni.items() if r.get("aum") is not None and r.get("flows_1m") is not None]
    sector_flows = [dict(_ef(tk, uni[tk]), sector=SECTOR_ETFS[tk]) for tk in SECTOR_ETFS if tk in uni]
    sector_flows.sort(key=lambda x: (x.get("flows_1m") or 0), reverse=True)
    _ranked = sorted(_etfs, key=lambda kv: (kv[1].get("flows_1m") or 0), reverse=True)
    s3.put_object(Bucket=BUCKET, Key="data/finviz-etf-flows.json",
                  Body=json.dumps({"generated_at": now, "n_etfs": len(_etfs),
                                   "sector_etfs": sector_flows,
                                   "top_inflows": [_ef(tk, r) for tk, r in _ranked[:25]],
                                   "top_outflows": [_ef(tk, r) for tk, r in _ranked[-25:][::-1]]},
                                  separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print("[finviz-universe] etf fund flows: %d ETFs, %d sector ETFs" % (len(_etfs), len(sector_flows)))

    # ── earnings-surprise layer (most-recent-quarter beats/misses, >= $1B to cut noise) ──
    def _sp(tk, r):
        eps = r.get("eps_surprise")
        return {"ticker": tk, "company": r.get("company"), "sector": r.get("sector"),
                "eps_surprise": eps if (eps is not None and abs(eps) <= 200) else None,
                "rev_surprise": r.get("rev_surprise"),
                "perf_m": r.get("perf_m"), "change": r.get("change"),
                "earnings_date": r.get("earnings_date"), "mktcap_m": r.get("market_cap")}
    # Rank by REVENUE surprise — revenue base is stable, so the % is meaningful (EPS% distorts at low base)
    _surp = [(tk, r) for tk, r in uni.items()
             if r.get("rev_surprise") is not None and (r.get("market_cap") or 0) >= 1000]
    _by = sorted(_surp, key=lambda kv: (kv[1].get("rev_surprise") or 0), reverse=True)
    s3.put_object(Bucket=BUCKET, Key="data/finviz-earnings-surprise.json",
                  Body=json.dumps({"generated_at": now, "n": len(_surp),
                                   "top_beats": [_sp(tk, r) for tk, r in _by[:30]],
                                   "top_misses": [_sp(tk, r) for tk, r in _by[-30:][::-1]]},
                                  separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print("[finviz-universe] earnings surprise: %d names >= $1B" % len(_surp))

    # ── index membership + change detection (S&P500/NDX/DJIA/Russell2000 ground truth) ──
    IDX = {"S&P 500": "sp500", "NDX": "ndx", "DJIA": "djia", "RUT": "russell2000"}
    members = {v: set() for v in IDX.values()}
    for tk, r in uni.items():
        im = r.get("index_membership")
        if not im:
            continue
        for label, key in IDX.items():
            if label in im:
                members[key].add(tk)
    try:
        _prev = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-index-membership.json")["Body"].read()).get("members", {})
    except Exception:
        _prev = {}
    changes = {}
    for key, cur in members.items():
        p = set(_prev.get(key, []))
        changes[key] = {"n": len(cur),
                        "additions": sorted(cur - p) if p else [],
                        "deletions": sorted(p - cur) if p else []}
    s3.put_object(Bucket=BUCKET, Key="data/finviz-index-membership.json",
                  Body=json.dumps({"generated_at": now,
                                   "members": {k: sorted(v) for k, v in members.items()},
                                   "changes": changes}, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    print("[finviz-universe] index membership: " + ", ".join("%s=%d" % (k, len(v)) for k, v in members.items())
          + " | changes: " + ", ".join("%s +%d/-%d" % (k, len(c["additions"]), len(c["deletions"])) for k, c in changes.items()))

    el = round(time.time() - started, 1)
    print("[finviz-universe] %d tickers | short_float=%d float=%d rel_volume=%d | %ss"
          % (n, n_short, n_float, n_relvol, el))
    return {"statusCode": 200,
            "body": json.dumps({"n_tickers": n, "short_float": n_short, "float": n_float,
                                "rel_volume": n_relvol, "elapsed_s": el})}
