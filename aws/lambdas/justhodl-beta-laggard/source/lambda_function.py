"""
justhodl-beta-laggard  the catch-up rotation mapper.

When a sector is genuinely working, money rotates DOWN the quality/ibility
ladder: the obvious leader runs first, then the higher-beta names that have
not moved yet tend to close the gap. This engine finds those laggards.

It is deliberately strict, because the naive version is a trap:
   Same-sector names are correlated by construction  correlation alone is
    no signal. The signal is DISPERSION: a real performance gap behind a
    leader that actually ran.
   A laggard only catches up if the SECTOR is working  chasing a laggard
    in a dying sector is catching a falling knife. We gate on sector
    breadth and median trend.
   A laggard with collapsing estimates or distress risk lags because it
    DESERVES to. We gate out broken fundamentals  a catch-up candidate
    must be a healthy company the tape simply hasn't found yet.
   Beta cuts both ways: a high-beta laggard catches up with force, but
    falls hardest if the sector rolls. Stated honestly on every card.

INPUT  screener/data.json (universe, sector, beta, fundamentals)
       + FMP stable stock-price-change (real 1M/3M/6M returns)
OUTPUT data/beta-laggards.json     SCHEDULE daily 13:30 UTC
Real data only. Research, not advice.
"""
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from statistics import median

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/beta-laggards.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"
WORKERS = 8


def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def fmp_price_change(sym):
    """Real trailing returns for one symbol (1D/1M/3M/6M/YTD/1Y, %)."""
    url = f"{BASE}/stock-price-change?symbol={sym}&apikey={FMP}"
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            d = json.loads(r.read())
        row = d[0] if isinstance(d, list) and d else (
            d if isinstance(d, dict) else {})
        return sym, {
            "r1m": num(row.get("1M")), "r3m": num(row.get("3M")),
            "r6m": num(row.get("6M")), "r1y": num(row.get("1Y")),
        }
    except Exception:
        return sym, None


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    #  universe + fundamentals from the master screener 
    try:
        sc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": f"screener read failed: {e}"}

    rows = sc.get("stocks")
    if not isinstance(rows, list):
        bs = sc.get("by_symbol") or {}
        rows = list(bs.values()) if isinstance(bs, dict) else []

    uni = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sy = (r.get("symbol") or "").upper()
        sec = r.get("sector")
        if not sy or not sec:
            continue
        uni[sy] = {
            "symbol": sy, "name": r.get("name") or sy, "sector": sec,
            "industry": r.get("industry"),
            "beta": num(r.get("beta")), "market_cap": num(r.get("marketCap")),
            "eps_growth": num(r.get("epsGrowth")),
            "altman_z": num(r.get("altmanZ")),
            "grades": r.get("gradesConsensus"),
            "fwd_pe": num(r.get("forwardPE")),
            "target": num(r.get("priceTargetMedian"))
            or num(r.get("priceTargetMean")),
            "target_upside": num(r.get("priceTargetUpsidePct")),
            "price": num(r.get("price")),
        }

    #  real trailing returns 
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(fmp_price_change, sy) for sy in uni]
        for f in as_completed(futs):
            sy, pc = f.result()
            if pc:
                uni[sy].update(pc)

    have = [u for u in uni.values() if u.get("r3m") is not None]

    #  group by sector, find working sectors + leaders + laggards 
    sectors = {}
    for u in have:
        sectors.setdefault(u["sector"], []).append(u)

    groups = []
    for sec, members in sectors.items():
        if len(members) < 6:
            continue
        r3 = [m["r3m"] for m in members]
        med3 = median(r3)
        breadth = sum(1 for x in r3 if x > 0) / len(r3)
        top = max(members, key=lambda m: m["r3m"])
        # SECTOR-WORKING GATE  only play catch-up in a group that is in
        # favour with broad participation and a leader that genuinely ran
        if med3 <= 2.0 or breadth < 0.5 or top["r3m"] < 12.0:
            continue

        laggards = []
        for m in members:
            if m["symbol"] == top["symbol"]:
                continue
            gap = top["r3m"] - m["r3m"]
            beta = m["beta"]
            # DISPERSION + BETA gate
            if (m["r3m"] >= med3 or gap < 15.0 or beta is None
                    or beta < 0.9 or m["r3m"] < -30.0):
                continue
            # FUNDAMENTAL-HEALTH gate  exclude broken laggards (value traps)
            az = m["altman_z"]
            epsg = m["eps_growth"]
            if az is not None and az < 1.8:
                continue
            if epsg is not None and epsg < -0.25:
                continue
            up = m["target_upside"]
            grades = (m["grades"] or "").lower()
            healthy_signal = ((up is not None and up > 3)
                              or grades in ("buy", "strong buy", "outperform"))
            if not healthy_signal:
                continue

            # catch-up score  sector strength  gap  beta  health
            sec_str = clamp((med3 - 2) / 18.0, 0, 1) * 0.5 + breadth * 0.5
            gap_c = clamp(gap / 60.0, 0, 1)
            beta_c = clamp((beta - 0.9) / 1.1, 0, 1)
            health_c = clamp(((up or 0) / 40.0), 0, 1) * 0.6 + (
                0.4 if grades in ("buy", "strong buy", "outperform") else 0)
            score = round(clamp(
                100 * (0.34 * sec_str + 0.30 * gap_c + 0.20 * beta_c
                       + 0.16 * health_c), 0, 100), 1)

            note = []
            if epsg is not None and epsg > 0:
                note.append(f"EPS still growing {epsg * 100:.0f}%")
            if grades:
                note.append(f"analysts rate it {grades}")
            if az is not None:
                note.append(f"Altman-Z {az:.1f} (financially sound)")

            why = (
                f"{sec} is working  the sector is up a median "
                f"{med3:.0f}% over 3 months with {breadth * 100:.0f}% of "
                f"names participating. The leader {top['symbol']} has "
                f"already run {top['r3m']:+.0f}%, but {m['symbol']} is up "
                f"only {m['r3m']:+.0f}%  a {gap:.0f}-point gap. With a beta "
                f"of {beta:.2f} and healthy fundamentals "
                f"({'; '.join(note) or 'no red flags'}), it is the kind of "
                "high-beta laggard that tends to close the gap once "
                "rotation broadens. "
                + (f"Analyst target ${m['target']:.2f} "
                   f"({up:+.0f}%)." if m["target"] and up is not None
                   else ""))

            laggards.append({
                "symbol": m["symbol"], "name": m["name"],
                "beta": beta, "r1m": m["r1m"], "r3m": m["r3m"],
                "gap_vs_leader_pp": round(gap, 1),
                "catch_up_score": score,
                "price": m["price"], "price_target": m["target"],
                "upside_pct": up, "fundamental_note": "; ".join(note),
                "why": why,
                "risk": ("High beta cuts both ways  if " + sec + " rolls "
                         "over, this name falls faster than the leader. "
                         "Catch-up is a rotation tendency, not a guarantee; "
                         "confirm the sector is still trending up."),
            })

        if laggards:
            laggards.sort(key=lambda x: x["catch_up_score"], reverse=True)
            groups.append({
                "sector": sec,
                "sector_3m_median_pct": round(med3, 1),
                "sector_breadth_pct": round(breadth * 100, 0),
                "leader": {"symbol": top["symbol"], "name": top["name"],
                           "r3m": round(top["r3m"], 1),
                           "r1m": round(top["r1m"] or 0, 1)},
                "n_laggards": len(laggards),
                "laggards": laggards[:8],
            })

    groups.sort(key=lambda g: (g["laggards"][0]["catch_up_score"]
                               if g["laggards"] else 0), reverse=True)
    flat = [{**lg, "sector": g["sector"],
             "leader_symbol": g["leader"]["symbol"]}
            for g in groups for lg in g["laggards"]]
    flat.sort(key=lambda x: x["catch_up_score"], reverse=True)

    out = {
        "schema_version": "1.0",
        "method": "sector_dispersion_catch_up",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": len(groups) >= 1,
        "headline": (
            f"{len(flat)} high-beta catch-up candidates across "
            f"{len(groups)} working sectors  leaders have run, these "
            "healthy laggards tend to follow."),
        "how_to_read": (
            "Each group is a sector that is genuinely working (rising "
            "median, broad participation) where one leader has already "
            "run. The laggards listed are healthy, higher-beta names in "
            "the same sector that have NOT moved yet  historically the "
            "ones that close the gap as rotation broadens. The catch-up "
            "score blends sector strength, the size of the gap, beta and "
            "the laggard's fundamental health. High beta means high "
            "downside too if the sector rolls  size accordingly."),
        "n_working_sectors": len(groups),
        "n_candidates": len(flat),
        "groups": groups,
        "top_candidates": flat[:30],
        "universe_with_returns": len(have),
        "disclaimer": ("Rotation is a tendency, not a certainty. Research "
                       "and education only  not investment advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[beta-laggard] {len(groups)} working sectors, {len(flat)} "
          f"catch-up candidates, {len(have)} universe, {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": out["ok"], "working_sectors": len(groups),
        "candidates": len(flat)})}
