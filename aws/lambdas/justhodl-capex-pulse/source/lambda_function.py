"""justhodl-capex-pulse — REAL CapEx dollars across the investable universe.

Khalid's mandate: the global-flow-desk tracked where PORTFOLIO money goes;
this tracks where CORPORATE money goes — capital expenditure, the other half
of the flow picture. Structural-presignals only counts capex MENTIONS in
filings; this engine measures the dollars.

  UNIVERSE   top ~160 by market cap from stock-xray cards (+ guaranteed
             hyperscalers: AMZN MSFT GOOGL META AAPL NVDA ORCL AVGO).
  DATA       FMP /stable/cash-flow-statement (period=quarter, limit=8):
             capex_ttm = sum |capitalExpenditure| of last 4 quarters;
             yoy vs the prior 4. (/v3+/v4 are dead — /stable only.)
  OUTPUT     data/capex-pulse.json: per-name rows, SECTOR aggregates
             (ttm $B, yoy%%, capex/mcap intensity), HYPERSCALER tile
             (the AI-buildout spend pulse), boards top_accelerators /
             top_cutters, market totals + history for trend.
  CONSUMERS  global-flow-desk capex block, signal-board "CapEx Pulse",
             global-flow-desk.html tile.
"""
import json, time, urllib.request
from datetime import datetime, timezone
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/capex-pulse.json"
HIST = "data/history/capex-pulse.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
HYPERSCALERS = ["AMZN", "MSFT", "GOOGL", "META", "AAPL", "NVDA", "ORCL", "AVGO"]
N_TOP = 160
s3 = boto3.client("s3", region_name="us-east-1")

def _j(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _fmp_cf(sym):
    url = ("https://financialmodelingprep.com/stable/cash-flow-statement"
           "?symbol=%s&period=quarter&limit=8&apikey=%s" % (sym, FMP))
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=20) as r:
        return json.loads(r.read())

def lambda_handler(event=None, context=None):
    x = _j("data/stock-xray.json", {}) or {}
    cards = x.get("cards") or {}
    ranked = sorted(((t, c) for t, c in cards.items() if c.get("mc_b")),
                    key=lambda tc: tc[1]["mc_b"], reverse=True)
    uni = [t for t, _ in ranked[:N_TOP]]
    for h in HYPERSCALERS:
        if h not in uni: uni.append(h)
    rows = []; fails = 0
    for t in uni:
        try:
            qs = _fmp_cf(t)
            if not isinstance(qs, list) or len(qs) < 5:
                fails += 1; continue
            cx = [abs(q.get("capitalExpenditure") or 0) for q in qs[:8]]
            ttm, prior = sum(cx[:4]), sum(cx[4:8])
            if ttm <= 0: fails += 1; continue
            yoy = round(100 * (ttm / prior - 1), 1) if prior > 0 else None
            c = cards.get(t) or {}
            rows.append({"ticker": t, "sector": c.get("sec") or "?",
                         "capex_ttm_b": round(ttm / 1e9, 2), "yoy_pct": yoy,
                         "mc_b": c.get("mc_b"),
                         "intensity_pct": round(100 * ttm / (c["mc_b"] * 1e9), 2) if c.get("mc_b") else None,
                         "asof": qs[0].get("date")})
        except Exception:
            fails += 1
        time.sleep(0.1)
    print("[capex] rows=%d fails=%d" % (len(rows), fails))

    sectors = {}
    for r in rows:
        e = sectors.setdefault(r["sector"], {"capex_ttm_b": 0.0, "prior_proxy_b": 0.0, "n": 0, "names": []})
        e["capex_ttm_b"] += r["capex_ttm_b"]; e["n"] += 1
        if r["yoy_pct"] is not None:
            e["prior_proxy_b"] += r["capex_ttm_b"] / (1 + r["yoy_pct"] / 100)
        e["names"].append((r["ticker"], r["capex_ttm_b"]))
    for s_, e in sectors.items():
        e["yoy_pct"] = round(100 * (e["capex_ttm_b"] / e["prior_proxy_b"] - 1), 1) if e["prior_proxy_b"] > 0 else None
        e["capex_ttm_b"] = round(e["capex_ttm_b"], 1)
        e["top"] = [t for t, _ in sorted(e.pop("names"), key=lambda kv: kv[1], reverse=True)[:4]]
        e.pop("prior_proxy_b", None)

    hyp = [r for r in rows if r["ticker"] in HYPERSCALERS]
    hyp_ttm = round(sum(r["capex_ttm_b"] for r in hyp), 1)
    hyp_prior = sum(r["capex_ttm_b"] / (1 + r["yoy_pct"] / 100) for r in hyp if r["yoy_pct"] is not None)
    hyperscalers = {"total_ttm_b": hyp_ttm,
                    "yoy_pct": round(100 * (sum(r["capex_ttm_b"] for r in hyp if r["yoy_pct"] is not None) / hyp_prior - 1), 1) if hyp_prior else None,
                    "rows": sorted(hyp, key=lambda r: r["capex_ttm_b"], reverse=True),
                    "read": "the AI-buildout spend pulse"}

    big = [r for r in rows if r["capex_ttm_b"] >= 0.5 and r["yoy_pct"] is not None]
    boards = {"top_accelerators": sorted(big, key=lambda r: r["yoy_pct"], reverse=True)[:12],
              "top_cutters": sorted(big, key=lambda r: r["yoy_pct"])[:12]}
    mkt_ttm = round(sum(r["capex_ttm_b"] for r in rows), 1)
    prior_m = sum(r["capex_ttm_b"] / (1 + r["yoy_pct"] / 100) for r in rows if r["yoy_pct"] is not None)
    mkt_yoy = round(100 * (sum(r["capex_ttm_b"] for r in rows if r["yoy_pct"] is not None) / prior_m - 1), 1) if prior_m else None

    hist = _j(HIST, {}) or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today] = {"mkt_ttm_b": mkt_ttm, "mkt_yoy": mkt_yoy, "hyp_ttm_b": hyp_ttm, "hyp_yoy": hyperscalers["yoy_pct"]}
    hist = dict(sorted(hist.items())[-400:])
    s3.put_object(Bucket=BUCKET, Key=HIST, Body=json.dumps(hist).encode(), ContentType="application/json")

    doc = {"engine": "justhodl-capex-pulse", "version": "1.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "n": len(rows), "fails": fails,
           "market": {"capex_ttm_b": mkt_ttm, "yoy_pct": mkt_yoy, "universe": "top-%d mcap (stock-xray) + hyperscalers" % N_TOP},
           "hyperscalers": hyperscalers, "sectors": sectors, "boards": boards, "rows": rows,
           "method": ("FMP /stable/cash-flow-statement quarterly x8 per name; TTM = last 4q "
                      "|capitalExpenditure|, yoy vs prior 4q; sector aggregates dollar-weighted; "
                      "intensity = capex/mcap.")}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"ok": True, "n": len(rows), "fails": fails, "mkt_ttm_b": mkt_ttm, "mkt_yoy": mkt_yoy,
            "hyp": {"ttm_b": hyp_ttm, "yoy": hyperscalers["yoy_pct"]}}
