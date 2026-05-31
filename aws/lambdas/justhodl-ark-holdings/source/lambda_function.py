"""justhodl-ark-holdings — daily ARK Invest ETF holdings tracker (v2).

THESIS
══════
Cathie Wood publishes ARK Invest's COMPLETE ETF holdings every day. ARK's
own CSV downloads went behind a session-token portal in 2025, but a
community-maintained API (arkfunds.io by frefrik) provides clean JSON
access to the same underlying data — same-day position visibility (vs
45-day-lagged 13Fs).

Day-over-day diffs reveal:
  NEW POSITION    — ARK initiating a thesis (first appearance)
  POSITION ADD    — conviction increasing (share count rises)
  POSITION TRIM   — de-risking
  CLOSED POSITION — thesis broken (ticker disappears)

DATA SOURCE
═══════════
  https://arkfunds.io/api/v2/etf/holdings?symbol=ARKK,ARKQ,ARKW,ARKF,ARKG,ARKX
  Community API mirroring ARK's daily disclosure. Not affiliated with ARK.
  Free, no auth, supports multi-fund queries.

OUTPUT
══════
  data/ark-holdings.json — current snapshot + day-over-day diff
  data/ark-holdings-prev.json — prior snapshot for diffing
  Emits ark.position_change event for material adds/trims/new/closed.
"""
import json
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/ark-holdings.json"
PREV_KEY   = "data/ark-holdings-prev.json"

HTTP_TIMEOUT = 30
USER_AGENT = "JustHodl-ArkHoldings/1.0 (raafouis@gmail.com)"

# 6 ARK ETFs tracked
ARK_FUND_CODES = ["ARKK", "ARKQ", "ARKW", "ARKF", "ARKG", "ARKX"]
ARK_FUND_DESC = {
    "ARKK": "ARK Innovation ETF (flagship — disruptive innovation)",
    "ARKQ": "ARK Autonomous Tech & Robotics ETF",
    "ARKW": "ARK Next Generation Internet ETF",
    "ARKF": "ARK Fintech Innovation ETF",
    "ARKG": "ARK Genomic Revolution ETF",
    "ARKX": "ARK Space Exploration & Innovation ETF",
}

ARKFUNDS_API = "https://arkfunds.io/api/v2"

s3 = boto3.client("s3", region_name=REGION)


def _http_get(url, timeout=HTTP_TIMEOUT, retries=2):
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(3 * (attempt + 1))
                continue
            print(f"[ark] HTTP {e.code} from {url[:120]}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            print(f"[ark] err: {type(e).__name__} {str(e)[:100]}")
            return None
    return None


def fetch_holdings_all_funds() -> dict:
    """One call gets all 6 ETFs via arkfunds.io. Returns {fund: [positions]}."""
    symbols = ",".join(ARK_FUND_CODES)
    url = f"{ARKFUNDS_API}/etf/holdings?symbol={symbols}"
    print(f"[ark] fetching {symbols} from arkfunds.io…")
    body = _http_get(url, timeout=45)
    if not body:
        print("[ark] holdings fetch failed")
        return {}
    
    try:
        data = json.loads(body)
    except Exception as e:
        print(f"[ark] JSON parse err: {e}")
        return {}
    
    # arkfunds.io response format:
    # {"symbol": "ARKK,ARKQ,...", "date_from": "...", "date_to": "...",
    #  "holdings": [{"fund": "ARKK", "date": "...", "ticker": "TSLA", ...}, ...]}
    holdings_list = data.get("holdings") or []
    if not holdings_list:
        print(f"[ark] no holdings returned — response: {str(data)[:200]}")
        return {}
    
    by_fund = defaultdict(list)
    for h in holdings_list:
        fund = h.get("fund")
        if not fund or fund not in ARK_FUND_CODES:
            continue
        try:
            ticker = (h.get("ticker") or "").strip().upper()
            if not ticker or ticker in ("--", "N/A", ""):
                continue
            by_fund[fund].append({
                "fund":         fund,
                "ticker":       ticker,
                "company":      (h.get("company") or "").strip(),
                "cusip":        (h.get("cusip") or "").strip(),
                "shares":       int(h.get("shares", 0) or 0),
                "market_value": float(h.get("market_value", 0) or 0),
                "weight":       float(h.get("weight", 0) or 0),
                "date":         (h.get("date") or "")[:10],
            })
        except Exception:
            continue
    
    for fund_code in ARK_FUND_CODES:
        n = len(by_fund.get(fund_code, []))
        if n > 0:
            print(f"[ark]   ✅ {fund_code}: {n} positions")
        else:
            print(f"[ark]   ⚠ {fund_code}: 0 positions")
    
    return dict(by_fund)


def load_prev_snapshot():
    """Load yesterday's holdings snapshot from S3 for diffing."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=PREV_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        prev_map = {}
        for fund_code, positions in (data.get("holdings_by_fund") or {}).items():
            for p in positions:
                prev_map[(fund_code, p["ticker"])] = {
                    "shares":       p.get("shares", 0),
                    "market_value": p.get("market_value", 0),
                    "weight":       p.get("weight", 0),
                }
        print(f"[ark] loaded {len(prev_map)} prior positions (snapshot {data.get('generated_at','?')})")
        return prev_map, data.get("generated_at")
    except Exception as e:
        print(f"[ark] no prior snapshot: {e}")
        return {}, None


def compute_diffs(current: dict, prev_map: dict) -> dict:
    """Diff current vs prior. Returns categorized changes."""
    new_positions    = []
    closed_positions = []
    position_adds    = []
    position_trims   = []
    
    current_keys = set()
    
    for fund_code, positions in current.items():
        for p in positions:
            key = (fund_code, p["ticker"])
            current_keys.add(key)
            prev = prev_map.get(key)
            if not prev:
                new_positions.append({
                    "fund":         fund_code,
                    "ticker":       p["ticker"],
                    "company":      p["company"],
                    "shares":       p["shares"],
                    "market_value": p["market_value"],
                    "weight":       p["weight"],
                })
            else:
                share_delta = p["shares"] - prev["shares"]
                if share_delta > 0:
                    pct_change = (share_delta / max(prev["shares"], 1)) * 100
                    if pct_change >= 1.0:
                        position_adds.append({
                            "fund":           fund_code,
                            "ticker":         p["ticker"],
                            "company":        p["company"],
                            "shares_added":   share_delta,
                            "pct_change":     round(pct_change, 1),
                            "current_shares": p["shares"],
                            "current_value":  p["market_value"],
                            "weight":         p["weight"],
                        })
                elif share_delta < 0:
                    pct_change = (share_delta / max(prev["shares"], 1)) * 100
                    if pct_change <= -1.0:
                        position_trims.append({
                            "fund":           fund_code,
                            "ticker":         p["ticker"],
                            "company":        p["company"],
                            "shares_sold":    abs(share_delta),
                            "pct_change":     round(pct_change, 1),
                            "current_shares": p["shares"],
                            "current_value":  p["market_value"],
                            "weight":         p["weight"],
                        })
    
    for key in prev_map:
        if key not in current_keys:
            fund_code, ticker = key
            closed_positions.append({
                "fund":        fund_code,
                "ticker":      ticker,
                "prev_shares": prev_map[key]["shares"],
                "prev_value":  prev_map[key]["market_value"],
            })
    
    new_positions.sort(key=lambda x: -x["market_value"])
    position_adds.sort(key=lambda x: -x["pct_change"])
    position_trims.sort(key=lambda x: x["pct_change"])
    closed_positions.sort(key=lambda x: -x["prev_value"])
    
    return {
        "new_positions":    new_positions,
        "position_adds":    position_adds,
        "position_trims":   position_trims,
        "closed_positions": closed_positions,
    }


def cross_fund_aggregation(holdings_by_fund: dict) -> list:
    """Aggregate per-ticker across all ARK ETFs."""
    by_ticker = defaultdict(lambda: {
        "ticker": None, "company": None,
        "total_shares": 0, "total_value": 0.0,
        "funds": [], "max_weight": 0.0,
    })
    
    for fund_code, positions in holdings_by_fund.items():
        for p in positions:
            t = p["ticker"]
            rec = by_ticker[t]
            rec["ticker"] = t
            rec["company"] = p["company"]
            rec["total_shares"] += p["shares"]
            rec["total_value"] += p["market_value"]
            rec["funds"].append({
                "fund":   fund_code,
                "shares": p["shares"],
                "value":  p["market_value"],
                "weight": p["weight"],
            })
            if p["weight"] > rec["max_weight"]:
                rec["max_weight"] = p["weight"]
    
    result = list(by_ticker.values())
    for r in result:
        r["n_funds"] = len(r["funds"])
    result.sort(key=lambda x: -x["total_value"])
    return result


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    holdings_by_fund = fetch_holdings_all_funds()
    if not holdings_by_fund:
        print("[ark] no funds fetched — aborting")
        return {"statusCode": 200,
                  "body": json.dumps({"ok": False, "reason": "no_data"})}
    
    total_positions = sum(len(p) for p in holdings_by_fund.values())
    print(f"[ark] {len(holdings_by_fund)} funds, {total_positions} total positions")
    
    prev_map, prev_date = load_prev_snapshot()
    diffs = compute_diffs(holdings_by_fund, prev_map) if prev_map else {
        "new_positions": [], "position_adds": [],
        "position_trims": [], "closed_positions": [],
    }
    
    cross_fund = cross_fund_aggregation(holdings_by_fund)
    
    prev_snapshot = {
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "holdings_by_fund": holdings_by_fund,
    }
    try:
        s3.put_object(Bucket=BUCKET, Key=PREV_KEY,
                       Body=json.dumps(prev_snapshot, default=str,
                                         separators=(",", ":")).encode("utf-8"),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
    except Exception as e:
        print(f"[ark] prev snapshot write failed: {e}")
    
    out = {
        "schema_version":   "2.0",
        "method":           "ark_holdings_v2_arkfunds_io",
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":       round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "data_source":      "https://arkfunds.io/api/v2/etf/holdings (community-maintained)",
        "prev_snapshot_at": prev_date,
        
        "funds_tracked":     ARK_FUND_CODES,
        "fund_descriptions": ARK_FUND_DESC,
        "n_funds_fetched":   len(holdings_by_fund),
        "n_positions_total": total_positions,
        "n_unique_tickers":  len(cross_fund),
        
        "diff_vs_prev": {
            "n_new_positions":    len(diffs["new_positions"]),
            "n_position_adds":    len(diffs["position_adds"]),
            "n_position_trims":   len(diffs["position_trims"]),
            "n_closed_positions": len(diffs["closed_positions"]),
            "highlights": {
                "new_positions":    diffs["new_positions"][:15],
                "biggest_adds":     diffs["position_adds"][:15],
                "biggest_trims":    diffs["position_trims"][:15],
                "closed_positions": diffs["closed_positions"][:10],
            },
        },
        
        "cross_fund_top": cross_fund[:30],
        
        "holdings_by_fund": {
            fund: sorted(positions, key=lambda p: -p["market_value"])[:40]
            for fund, positions in holdings_by_fund.items()
        },
        
        "notes": (
            "ARK Invest's daily ETF holdings via arkfunds.io community API. "
            "Day-over-day diff against prior snapshot reveals NEW POSITIONS, "
            "ADDS, TRIMS, and CLOSED POSITIONS. 1%+ share-count change "
            "threshold filters routine rebalancing. Cross-fund aggregation "
            "highlights multi-fund conviction (strongest ARK signal). "
            "Migrated from direct CSV downloads (broken 2025) to arkfunds.io "
            "in ops 1062."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[ark] wrote {len(body):,}B  positions={total_positions}  "
          f"unique={len(cross_fund)}  duration={out['duration_s']}s")
    
    try:
        from system_events import publish_many
        events_pub = []
        for p in diffs["new_positions"][:3]:
            if p["market_value"] >= 5_000_000:
                events_pub.append(("ark.position_change", {
                    "change_type": "new_position",
                    "ticker":      p["ticker"],
                    "fund":        p["fund"],
                    "value":       p["market_value"],
                    "shares":      p["shares"],
                }))
        for p in diffs["position_adds"][:3]:
            if p["pct_change"] >= 10 and p["current_value"] >= 5_000_000:
                events_pub.append(("ark.position_change", {
                    "change_type":   "add",
                    "ticker":        p["ticker"],
                    "fund":          p["fund"],
                    "pct_change":    p["pct_change"],
                    "shares_added":  p["shares_added"],
                    "current_value": p["current_value"],
                }))
        if events_pub:
            publish_many(events_pub)
    except Exception as e:
        print(f"[ark] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":               True,
        "n_funds":          len(holdings_by_fund),
        "n_positions":      total_positions,
        "n_unique_tickers": len(cross_fund),
        "n_new_positions":  len(diffs["new_positions"]),
        "n_adds":           len(diffs["position_adds"]),
        "n_trims":          len(diffs["position_trims"]),
        "n_closed":         len(diffs["closed_positions"]),
        "duration_s":       out["duration_s"],
    })}


lambda_handler = handler
