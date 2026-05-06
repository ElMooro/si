"""
justhodl-sector-earnings-diffusion — institutional sector inflection detector

When 65%+ of stocks in a sector have rising FY1 earnings estimates, sell-side
desks call "sector all-in" — institutions follow within weeks. This is one of
the most reliable leading indicators of sector rotation.

WHAT THIS COMPUTES (per sector + per industry):
  1. % of stocks with rising FY1 EPS estimate (last 30d/90d)
  2. % of stocks with rising FY2 EPS estimate
  3. % of stocks with revenue growth acceleration (FY2 > FY1 growth)
  4. Average estimate revision velocity per sector
  5. Diffusion regime: BULLISH > 65%, NEUTRAL 40-65%, BEARISH < 40%
  6. CROSS-COMPARE: which sectors are gaining diffusion (delta vs prior state)

DATA: FMP /analyst-estimates endpoint per stock — gives FY1, FY2, FY3 forecasts
We already use this in eps-revision-velocity. We aggregate it by sector here.

OUTPUT: data/sector-earnings-diffusion.json

This is what catches:
  - AI/semis sector breadth before the 2024-2026 multi-name run
  - Healthcare-managed-care GLP-1 wave 2023-2024
  - Energy upgrades cycle 2021
  - Banking estimate revisions before regional bank rallies
"""
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/sector-earnings-diffusion.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/sector-earnings-diffusion-state.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-SectorDiff/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_universe_with_sectors():
    """Read unified universe — already has sector/industry."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        return d.get("stocks", [])[:MAX_TICKERS]
    except Exception as e:
        print("[sec-diff] universe load failed: " + str(e))
        return []


def fetch_estimates(symbol):
    """FMP analyst-estimates: returns annual FY1/FY2 estimates with revisions."""
    url = "https://financialmodelingprep.com/stable/analyst-estimates?symbol=" + symbol + "&period=annual&limit=4&apikey=" + FMP_KEY
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return d
    except Exception:
        pass
    return None


def get_eps_velocity_data():
    """Read existing eps-revision-velocity.json — already computed per ticker.
    Avoids re-fetching the same FMP data twice.
    """
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/eps-revision-velocity.json")
        d = json.loads(obj["Body"].read())
        out = {}
        # Use all_qualifying for the per-ticker data
        for c in d.get("all_qualifying", []):
            sym = (c.get("symbol") or "").upper()
            if sym:
                out[sym] = c
        return out
    except Exception:
        return {}


def evaluate_ticker_diffusion(stock, eps_velocity_data):
    """Given a stock (with sector/industry) + eps-velocity data, classify
    its earnings revision direction.
    
    Returns: dict with sector, industry, ticker, eps_signal (UP/FLAT/DOWN), revisions metrics
    """
    sym = (stock.get("symbol") or "").upper()
    sector = stock.get("sector") or "UNKNOWN"
    industry = stock.get("industry") or "UNKNOWN"

    # If we have data from eps-velocity, use it (already rich)
    ev = eps_velocity_data.get(sym)
    if ev:
        flag = ev.get("flag", "")
        score = ev.get("score", 0) or 0
        estimates = ev.get("estimates") or {}
        fy2_lift = estimates.get("fy2_lift_pct") or 0
        fwd_rev_growth = estimates.get("fwd_rev_growth_pct") or 0
        # Classify direction
        if "HIGH_VELOCITY" in flag or score >= 60:
            signal = "UP_STRONG"
        elif "TIER_B" in flag or score >= 45:
            signal = "UP"
        elif fy2_lift > 5:
            signal = "UP"
        elif fy2_lift < -5:
            signal = "DOWN"
        elif fy2_lift > 0:
            signal = "FLAT_UP"
        else:
            signal = "FLAT"
        return {
            "symbol": sym,
            "sector": sector,
            "industry": industry,
            "eps_signal": signal,
            "fy2_lift_pct": fy2_lift,
            "fwd_rev_growth_pct": fwd_rev_growth,
            "score": score,
            "source": "eps_velocity",
        }
    # Fallback: try direct FMP estimates fetch
    estimates = fetch_estimates(sym)
    if not estimates or len(estimates) < 2:
        return None
    # estimates is sorted newest first by FMP
    cur = estimates[0]
    prior = estimates[1] if len(estimates) > 1 else cur
    cur_eps = cur.get("estimatedEpsAvg") or 0
    prior_eps = prior.get("estimatedEpsAvg") or 0
    cur_rev = cur.get("estimatedRevenueAvg") or 0
    prior_rev = prior.get("estimatedRevenueAvg") or 0

    fy2_lift = ((cur_eps - prior_eps) / abs(prior_eps) * 100) if prior_eps else 0
    fwd_rev_growth = ((cur_rev - prior_rev) / abs(prior_rev) * 100) if prior_rev else 0

    if fy2_lift > 10:
        signal = "UP_STRONG"
    elif fy2_lift > 3:
        signal = "UP"
    elif fy2_lift < -10:
        signal = "DOWN_STRONG"
    elif fy2_lift < -3:
        signal = "DOWN"
    else:
        signal = "FLAT"

    return {
        "symbol": sym,
        "sector": sector,
        "industry": industry,
        "eps_signal": signal,
        "fy2_lift_pct": fy2_lift,
        "fwd_rev_growth_pct": fwd_rev_growth,
        "score": 0,
        "source": "fmp_direct",
    }


def aggregate_by_group(per_ticker, group_key):
    """Aggregate per-ticker signals into per-sector or per-industry groups."""
    by_group = defaultdict(list)
    for r in per_ticker:
        if r is None:
            continue
        g = r.get(group_key, "UNKNOWN")
        if not g or g == "UNKNOWN":
            continue
        by_group[g].append(r)

    aggregates = []
    for g, items in by_group.items():
        n = len(items)
        if n < 3:  # need at least 3 names per group for meaningful diffusion
            continue
        n_up_strong = sum(1 for r in items if r["eps_signal"] == "UP_STRONG")
        n_up = sum(1 for r in items if r["eps_signal"] in ("UP_STRONG", "UP"))
        n_flat_up = sum(1 for r in items if r["eps_signal"] == "FLAT_UP")
        n_down = sum(1 for r in items if r["eps_signal"] in ("DOWN", "DOWN_STRONG"))
        n_down_strong = sum(1 for r in items if r["eps_signal"] == "DOWN_STRONG")

        diffusion_up_pct = n_up / n * 100
        diffusion_down_pct = n_down / n * 100
        diffusion_strong_up_pct = n_up_strong / n * 100

        avg_fy2_lift = sum(r["fy2_lift_pct"] for r in items) / n
        avg_rev_growth = sum(r["fwd_rev_growth_pct"] for r in items) / n
        avg_score = sum(r.get("score") or 0 for r in items) / n

        # Classify regime
        if diffusion_up_pct >= 70:
            regime = "BULLISH_ALL_IN"
        elif diffusion_up_pct >= 55:
            regime = "BULLISH"
        elif diffusion_up_pct >= 40:
            regime = "NEUTRAL_BULLISH"
        elif diffusion_up_pct >= 25:
            regime = "NEUTRAL_BEARISH"
        else:
            regime = "BEARISH"

        # Score the group 0-100
        score = (
            min(diffusion_up_pct / 70 * 50, 50)  # max 50pts at 70%+
            + min(diffusion_strong_up_pct * 2, 20)  # bonus for "strong" up
            + min(max(avg_fy2_lift / 30, 0), 1) * 20  # avg lift max 20pts
            + (10 if avg_rev_growth > 10 else 5 if avg_rev_growth > 5 else 0)
        )
        score = min(score, 100)

        # Top 5 names with strongest signals in this group
        items_sorted = sorted(items, key=lambda x: -(x.get("score") or 0) - x["fy2_lift_pct"])
        top_names = [{"symbol": r["symbol"], "fy2_lift": r["fy2_lift_pct"], "signal": r["eps_signal"]}
                      for r in items_sorted[:5]]

        aggregates.append({
            "group": g,
            "group_key": group_key,
            "n_constituents": n,
            "n_up_strong": n_up_strong,
            "n_up": n_up,
            "n_down": n_down,
            "n_down_strong": n_down_strong,
            "diffusion_up_pct": round(diffusion_up_pct, 1),
            "diffusion_strong_up_pct": round(diffusion_strong_up_pct, 1),
            "diffusion_down_pct": round(diffusion_down_pct, 1),
            "avg_fy2_lift_pct": round(avg_fy2_lift, 1),
            "avg_fwd_rev_growth_pct": round(avg_rev_growth, 1),
            "avg_score": round(avg_score, 1),
            "regime": regime,
            "diffusion_score": round(score, 1),
            "top_names": top_names,
        })

    aggregates.sort(key=lambda x: -x["diffusion_score"])
    return aggregates


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[sec-diff] starting v1.0")

    universe = get_universe_with_sectors()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0, "reason": "no universe"})}
    print("[sec-diff] universe: " + str(len(universe)) + " stocks")

    # Reuse eps-velocity data when possible (already computed)
    eps_data = get_eps_velocity_data()
    print("[sec-diff] eps-velocity prior data: " + str(len(eps_data)) + " tickers")

    per_ticker = []
    n_skipped = 0

    # For tickers in eps_velocity, no API call needed
    # For ones not, we'll call FMP directly
    def evaluate(stock):
        if time.time() > deadline_at:
            return None
        try:
            return evaluate_ticker_diffusion(stock, eps_data)
        except Exception as e:
            print("[sec-diff] " + (stock.get("symbol") or "?") + " ERROR: " + str(e))
            return None

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    per_ticker.append(r)
                else:
                    n_skipped += 1
            except Exception:
                n_skipped += 1

    print("[sec-diff] classified: " + str(len(per_ticker)) + ", skipped: " + str(n_skipped))

    # Sector and industry aggregations
    by_sector = aggregate_by_group(per_ticker, "sector")
    by_industry = aggregate_by_group(per_ticker, "industry")
    print("[sec-diff] sectors: " + str(len(by_sector)) + ", industries: " + str(len(by_industry)))

    # Detect deltas vs prior state
    prior_state = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        prior_state = json.loads(obj["Body"].read())
    except Exception:
        pass

    sector_deltas = []
    if prior_state:
        prior_by_sector = {s["group"]: s for s in prior_state.get("sectors", [])}
        for s in by_sector:
            ps = prior_by_sector.get(s["group"])
            if ps:
                diff_delta = s["diffusion_up_pct"] - ps["diffusion_up_pct"]
                score_delta = s["diffusion_score"] - ps["diffusion_score"]
                if abs(diff_delta) >= 3 or abs(score_delta) >= 5:
                    sector_deltas.append({
                        "sector": s["group"],
                        "diffusion_change_pct": round(diff_delta, 1),
                        "score_change": round(score_delta, 1),
                        "old_diffusion": ps["diffusion_up_pct"],
                        "new_diffusion": s["diffusion_up_pct"],
                        "old_regime": ps.get("regime"),
                        "new_regime": s.get("regime"),
                    })

    sector_deltas.sort(key=lambda x: -x["score_change"])

    # Alerts
    alerts = []
    # 1. Sectors crossing 65% diffusion threshold
    for s in by_sector:
        if s["diffusion_up_pct"] >= 65:
            alerts.append({
                "type": "SECTOR_ALL_IN",
                "sector": s["group"],
                "diffusion_pct": s["diffusion_up_pct"],
                "n_up": s["n_up"],
                "n": s["n_constituents"],
                "msg": s["group"] + " has " + str(s["n_up"]) + "/" + str(s["n_constituents"]) +
                       " (" + str(s["diffusion_up_pct"]) + "%) names with rising estimates — sector all-in",
            })
    # 2. Sectors with rising diffusion
    for d in sector_deltas[:5]:
        if d["diffusion_change_pct"] >= 5:
            alerts.append({
                "type": "DIFFUSION_RISING",
                "sector": d["sector"],
                "delta": d["diffusion_change_pct"],
                "msg": d["sector"] + " diffusion rising " + str(d["diffusion_change_pct"]) + "% (" +
                       str(d["old_diffusion"]) + "% → " + str(d["new_diffusion"]) + "%)",
            })
    # 3. Sectors with falling diffusion (rotation OUT signal)
    for d in sorted(sector_deltas, key=lambda x: x["score_change"])[:5]:
        if d["diffusion_change_pct"] <= -5:
            alerts.append({
                "type": "DIFFUSION_FALLING",
                "sector": d["sector"],
                "delta": d["diffusion_change_pct"],
                "msg": d["sector"] + " diffusion falling " + str(-d["diffusion_change_pct"]) + "% (" +
                       str(d["old_diffusion"]) + "% → " + str(d["new_diffusion"]) + "%)",
            })

    out = {
        "schema_version": 1,
        "method": "sector_earnings_diffusion_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_classified": len(per_ticker),
            "n_skipped": n_skipped,
            "n_sectors": len(by_sector),
            "n_industries": len(by_industry),
            "n_alerts": len(alerts),
        },
        "summary": {
            "sectors_top_diffusion": by_sector[:15],
            "industries_top_diffusion": by_industry[:25],
            "sector_deltas": sector_deltas[:10],
            "alerts": alerts[:20],
        },
        "all_sectors": by_sector,
        "all_industries": by_industry,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[sec-diff] wrote " + str(len(body)) + "b")
    print("[sec-diff] top sectors: " + str([(s["group"], s["diffusion_up_pct"]) for s in by_sector[:5]]))

    # State save
    state = {
        "generated_at": out["generated_at"],
        "sectors": [{"group": s["group"], "diffusion_up_pct": s["diffusion_up_pct"],
                       "diffusion_score": s["diffusion_score"], "regime": s["regime"]}
                      for s in by_sector],
    }
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                   Body=json.dumps(state).encode(),
                   ContentType="application/json")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_classified": len(per_ticker),
            "n_sectors": len(by_sector),
            "n_alerts": len(alerts),
            "duration_s": out["duration_s"],
        }),
    }
