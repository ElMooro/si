"""justhodl-ark-holdings — daily ARK Invest ETF holdings tracker.

THESIS
══════
Cathie Wood publishes ARK Invest's COMPLETE ETF holdings every day at
midnight EST. This is institutional-grade transparency at retail pricing
(free CSV downloads). Day-over-day diffs reveal:

  NEW POSITION    — ARK initiating a thesis (first appearance)
  POSITION ADD    — conviction increasing (share count rises)
  POSITION TRIM   — de-risking
  CLOSED POSITION — thesis broken (ticker disappears)

Why this beats 13F tracking for tactical edge:
  - 13Fs are 45-day-lagged quarterly snapshots
  - ARK gives you SAME-DAY position-change visibility
  - ARK manages ~$14B AUM across 6 ETFs
  - Cathie Wood positions get massive retail follow-through

ARK is publicly traceable smart money in disruptive innovation, AI, fintech,
genomics, robotics, space tech. Their day-1 buys often precede momentum
trends by weeks.

DATA SOURCE
═══════════
  https://ark-funds.com/wp-content/uploads/funds-etf-csv/
    ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv  (flagship)
    ARK_AUTONOMOUS_TECH_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv
    ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv
    ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv
    ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv
    ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv

CSV format: date,fund,company,ticker,cusip,shares,market_value,weight(%)

OUTPUT
══════
  data/ark-holdings.json — current snapshot + day-over-day diff
  data/ark-holdings-prev.json — prior snapshot for diffing
  Emits ark.position_change event for material adds/trims/new/closed.
"""
import json
import os
import re
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
PREV_KEY = "data/ark-holdings-prev.json"

HTTP_TIMEOUT = 30
USER_AGENT = "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)"

# 6 ARK ETFs we track
ARK_FUNDS = {
    "ARKK": ("ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
              "ARK Innovation ETF (flagship — disruptive innovation)"),
    "ARKQ": ("ARK_AUTONOMOUS_TECH_%26_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
              "ARK Autonomous Tech & Robotics ETF"),
    "ARKW": ("ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
              "ARK Next Generation Internet ETF"),
    "ARKF": ("ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv",
              "ARK Fintech Innovation ETF"),
    "ARKG": ("ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv",
              "ARK Genomic Revolution ETF"),
    "ARKX": ("ARK_SPACE_EXPLORATION_%26_INNOVATION_ETF_ARKX_HOLDINGS.csv",
              "ARK Space Exploration & Innovation ETF"),
}

BASE_URL = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/"

s3 = boto3.client("s3", region_name=REGION)


def _http_get(url, timeout=HTTP_TIMEOUT, retries=2):
    h = {"User-Agent": USER_AGENT, "Accept": "text/csv,*/*"}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(3)
                continue
            print(f"[ark] HTTP {e.code} from {url[:100]}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            print(f"[ark] err: {type(e).__name__} {str(e)[:80]}")
            return None
    return None


def parse_ark_csv(csv_text: str, fund_code: str) -> list:
    """Parse ARK's holdings CSV. Format varies slightly across funds but
    columns are: date, fund, company, ticker, cusip, shares, market_value,
    weight (sometimes with % sign).
    
    ARK appends a non-data footer ("The fund's holdings...") to the CSV
    that we must filter out.
    """
    import csv, io
    holdings = []
    
    # Lines starting with empty/whitespace or "The fund's" are footers
    lines = [l for l in csv_text.split("\n")
              if l.strip() and not l.strip().lower().startswith("the fund")]
    if not lines:
        return holdings
    
    # Use csv module on clean lines
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    for row in reader:
        try:
            # Field name varies: "ticker" vs "ticker_symbol"
            ticker = (row.get("ticker") or row.get("ticker_symbol")
                        or row.get("Ticker") or "").strip().upper()
            if not ticker or ticker in ("--", "N/A", ""):
                continue
            
            company = (row.get("company") or row.get("Company") or "").strip()
            cusip = (row.get("cusip") or row.get("CUSIP") or "").strip()
            shares_str = (row.get("shares") or row.get("Shares") or "0").replace(",", "")
            mv_str = (row.get("market value ($)") or row.get("market_value")
                       or row.get("Market Value($)") or "0").replace(",", "").replace("$", "")
            weight_str = (row.get("weight (%)") or row.get("weight")
                            or row.get("Weight(%)") or "0").replace("%", "")
            date_str = (row.get("date") or row.get("Date") or "").strip()
            
            try: shares = int(float(shares_str))
            except: shares = 0
            try: market_value = float(mv_str)
            except: market_value = 0.0
            try: weight = float(weight_str)
            except: weight = 0.0
            
            if shares <= 0 and market_value <= 0:
                continue
            
            holdings.append({
                "fund":         fund_code,
                "ticker":       ticker,
                "company":      company,
                "cusip":        cusip,
                "shares":       shares,
                "market_value": market_value,
                "weight":       weight,
                "date":         date_str[:10] if date_str else None,
            })
        except Exception:
            continue
    return holdings


def fetch_all_funds() -> dict:
    """Fetch holdings for all 6 ARK ETFs. Returns {fund_code: [holdings]}."""
    all_holdings = {}
    for fund_code, (csv_name, description) in ARK_FUNDS.items():
        url = BASE_URL + csv_name
        print(f"[ark] fetching {fund_code}…")
        body = _http_get(url, timeout=30)
        if not body:
            print(f"[ark]   ❌ {fund_code} fetch failed")
            continue
        
        holdings = parse_ark_csv(body, fund_code)
        if holdings:
            all_holdings[fund_code] = holdings
            print(f"[ark]   ✅ {fund_code}: {len(holdings)} positions")
        else:
            print(f"[ark]   ⚠ {fund_code}: parsed 0 holdings (CSV format change?)")
        
        time.sleep(0.5)  # be polite
    
    return all_holdings


def load_prev_snapshot() -> dict:
    """Load yesterday's holdings snapshot from S3 for diffing."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=PREV_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        # Build a map: (fund, ticker) -> {shares, market_value}
        prev_map = {}
        for fund_code, positions in (data.get("holdings_by_fund") or {}).items():
            for p in positions:
                prev_map[(fund_code, p["ticker"])] = {
                    "shares":       p.get("shares", 0),
                    "market_value": p.get("market_value", 0),
                    "weight":       p.get("weight", 0),
                }
        print(f"[ark] loaded {len(prev_map)} prior positions for diffing "
              f"(snapshot {data.get('generated_at','?')})")
        return prev_map, data.get("generated_at")
    except Exception as e:
        print(f"[ark] no prior snapshot for diffing: {e}")
        return {}, None


def compute_diffs(current: dict, prev_map: dict, prev_date: str = None) -> dict:
    """Diff current vs prior snapshot. Returns categorized changes."""
    new_positions   = []  # in current, not in prev
    closed_positions = []  # in prev, not in current
    position_adds    = []  # shares grew
    position_trims   = []  # shares shrunk
    
    current_keys = set()
    
    for fund_code, positions in current.items():
        for p in positions:
            key = (fund_code, p["ticker"])
            current_keys.add(key)
            prev = prev_map.get(key)
            if not prev:
                # NEW POSITION
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
                    if pct_change >= 1.0:  # threshold to avoid noise
                        position_adds.append({
                            "fund":          fund_code,
                            "ticker":        p["ticker"],
                            "company":       p["company"],
                            "shares_added":  share_delta,
                            "pct_change":    round(pct_change, 1),
                            "current_shares": p["shares"],
                            "current_value":  p["market_value"],
                            "weight":        p["weight"],
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
    
    # Closed positions: keys in prev but not in current
    for key in prev_map:
        if key not in current_keys:
            fund_code, ticker = key
            closed_positions.append({
                "fund":          fund_code,
                "ticker":        ticker,
                "prev_shares":   prev_map[key]["shares"],
                "prev_value":    prev_map[key]["market_value"],
            })
    
    # Sort by impact
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
    """Aggregate per-ticker across all ARK ETFs.
    Returns sorted list by total market value across all funds."""
    by_ticker = defaultdict(lambda: {
        "ticker":       None,
        "company":      None,
        "total_shares": 0,
        "total_value":  0.0,
        "funds":        [],
        "max_weight":   0.0,
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
    
    # Fetch all 6 fund holdings
    holdings_by_fund = fetch_all_funds()
    if not holdings_by_fund:
        print("[ark] no funds fetched — aborting")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "no_data"})}
    
    total_positions = sum(len(p) for p in holdings_by_fund.values())
    print(f"[ark] {len(holdings_by_fund)} funds, {total_positions} total positions")
    
    # Load prior snapshot + compute diffs
    prev_map, prev_date = load_prev_snapshot()
    diffs = compute_diffs(holdings_by_fund, prev_map, prev_date) if prev_map else {
        "new_positions": [], "position_adds": [], "position_trims": [], "closed_positions": [],
    }
    
    # Cross-fund aggregation
    cross_fund = cross_fund_aggregation(holdings_by_fund)
    
    # Save current as prev for tomorrow's diff
    prev_snapshot = {
        "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "holdings_by_fund": holdings_by_fund,
    }
    try:
        s3.put_object(
            Bucket=BUCKET, Key=PREV_KEY,
            Body=json.dumps(prev_snapshot, default=str, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=3600",
        )
    except Exception as e:
        print(f"[ark] prev snapshot write failed: {e}")
    
    # Main output
    out = {
        "schema_version":   "1.0",
        "method":           "ark_holdings_v1",
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":       round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "data_source":      "https://ark-funds.com/wp-content/uploads/funds-etf-csv/",
        "prev_snapshot_at": prev_date,
        
        "funds_tracked":    list(ARK_FUNDS.keys()),
        "fund_descriptions": {k: v[1] for k, v in ARK_FUNDS.items()},
        "n_funds_fetched":  len(holdings_by_fund),
        "n_positions_total": total_positions,
        "n_unique_tickers": len(cross_fund),
        
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
        
        "cross_fund_top": cross_fund[:30],  # top 30 by total ARK exposure
        
        "holdings_by_fund": {
            fund: sorted(positions, key=lambda p: -p["market_value"])[:40]
            for fund, positions in holdings_by_fund.items()
        },
        
        "notes": (
            "ARK Invest's daily CSV downloads provide same-day position visibility. "
            "Day-over-day diff against prior snapshot reveals NEW POSITIONS (thesis "
            "initiation), POSITION ADDS (conviction rising), POSITION TRIMS, and "
            "CLOSED POSITIONS (thesis broken). 1%+ share-count change threshold "
            "filters out routine rebalancing. Cross-fund aggregation highlights "
            "high-conviction names held across multiple ARK ETFs (multi-fund "
            "conviction = strongest ARK signal)."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[ark] wrote {len(body):,}B  positions={total_positions}  "
          f"unique={len(cross_fund)}  duration={out['duration_s']}s")
    
    # Emit events for material position changes
    try:
        from system_events import publish_many
        events_pub = []
        for p in diffs["new_positions"][:3]:
            if p["market_value"] >= 5_000_000:  # ≥$5M new position
                events_pub.append(("ark.position_change", {
                    "change_type":  "new_position",
                    "ticker":       p["ticker"],
                    "fund":         p["fund"],
                    "value":        p["market_value"],
                    "shares":       p["shares"],
                }))
        for p in diffs["position_adds"][:3]:
            if p["pct_change"] >= 10 and p["current_value"] >= 5_000_000:
                events_pub.append(("ark.position_change", {
                    "change_type":  "add",
                    "ticker":       p["ticker"],
                    "fund":         p["fund"],
                    "pct_change":   p["pct_change"],
                    "shares_added": p["shares_added"],
                    "current_value": p["current_value"],
                }))
        if events_pub:
            publish_many(events_pub)
    except Exception as e:
        print(f"[ark] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":                   True,
        "n_funds":              len(holdings_by_fund),
        "n_positions":          total_positions,
        "n_unique_tickers":     len(cross_fund),
        "n_new_positions":      len(diffs["new_positions"]),
        "n_adds":               len(diffs["position_adds"]),
        "n_trims":              len(diffs["position_trims"]),
        "n_closed":             len(diffs["closed_positions"]),
        "duration_s":           out["duration_s"],
    })}


lambda_handler = handler
