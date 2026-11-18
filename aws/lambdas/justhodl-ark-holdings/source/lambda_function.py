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
  data/ark-holdings-prev.json — the newest snapshot the engine has stored (the previous run's holdings)
  data/ark-holdings-base.json — the snapshot the published diff is measured against (v2.1.0)
  Emits ark.position_change event for material adds/trims/new/closed.

v2.1.0 (2026-11-18, Khalid: "make the ARK engine publish every move"):
  - diff_vs_prev.all carries EVERY new / added-to / cut / closed line (the 15-row `highlights` stay as they were);
    closed lines carry the company name; holdings_by_fund carries every position (it stopped at 40 per fund).
  - the baseline is the last snapshot with an EARLIER holdings date (ark_moves.choose_baseline): a second run on the
    same holdings, a weekend or a holiday no longer empties the diff — the last trading day's moves stay, dated by
    holdings_date / prev_holdings_date, and diff_is_carried says the source has published nothing newer.
  - ark.position_change events are published once per holdings date (on the run that first measures it), as before.
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

import ark_moves

VERSION = "2.1.0"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/ark-holdings.json"
PREV_KEY   = "data/ark-holdings-prev.json"
BASE_KEY   = "data/ark-holdings-base.json"

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


def load_snapshot(key):
    """A stored snapshot ({generated_at, holdings_date, holdings_by_fund}) or None when the key is not there."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        if not isinstance(data, dict) or not data.get("holdings_by_fund"):
            print(f"[ark] {key}: no holdings inside")
            return None
        n = sum(len(v or []) for v in data["holdings_by_fund"].values())
        print(f"[ark] loaded {key}: {n} positions, holdings dated {ark_moves.snapshot_date(data) or '?'} (snapshot {data.get('generated_at','?')})")
        return data
    except Exception as e:
        print(f"[ark] no snapshot at {key}: {e}")
        return None


def store_snapshot(key, doc):
    """True when the snapshot was written."""
    try:
        s3.put_object(Bucket=BUCKET, Key=key,
                       Body=json.dumps(doc, default=str,
                                         separators=(",", ":")).encode("utf-8"),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        return True
    except Exception as e:
        print(f"[ark] snapshot write to {key} failed: {e}")
        return False


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
                "company":     prev_map[key].get("company") or "",
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
    
    # v2.1.0: measure against the last snapshot with an EARLIER holdings date (ark_moves.choose_baseline) - the
    # previous run's snapshot only on the first run that sees a new day; the retained baseline on every run after it.
    holdings_date = ark_moves.holdings_date(holdings_by_fund)
    latest_doc = load_snapshot(PREV_KEY)
    base_doc = load_snapshot(BASE_KEY)
    baseline_doc, new_day = ark_moves.choose_baseline(holdings_date, latest_doc, base_doc)
    prev_map = ark_moves.snapshot_map(baseline_doc)
    prev_date = (baseline_doc or {}).get("generated_at")
    prev_holdings_date = ark_moves.snapshot_date(baseline_doc)
    print(f"[ark] holdings dated {holdings_date or '?'}; measured against {prev_holdings_date or 'nothing'} "
          f"({'new day' if new_day else 'same holdings as the last run - retained baseline'})")
    diffs = compute_diffs(holdings_by_fund, prev_map) if prev_map else {
        "new_positions": [], "position_adds": [],
        "position_trims": [], "closed_positions": [],
    }

    cross_fund = cross_fund_aggregation(holdings_by_fund)

    # Retain the baseline BEFORE the newest snapshot is replaced: if that write fails, the newest snapshot is left
    # alone so the next run sees the same new day and tries again (never a diff measured against the wrong day).
    baseline_kept = True
    if new_day and latest_doc is not None:
        baseline_kept = store_snapshot(BASE_KEY, latest_doc)
    if baseline_kept:
        store_snapshot(PREV_KEY, {
            "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "holdings_date":    holdings_date,
            "holdings_by_fund": holdings_by_fund,
        })
    moves = ark_moves.moves_block(diffs)

    out = {
        "schema_version":   "2.0",
        "version":          VERSION,
        "method":           "ark_holdings_v2_arkfunds_io",
        "generated_at":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":       round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "data_source":      "https://arkfunds.io/api/v2/etf/holdings (community-maintained)",
        "prev_snapshot_at": prev_date,
        "holdings_date":      holdings_date or None,
        "prev_holdings_date": prev_holdings_date or None,
        "diff_is_carried":    bool(prev_map) and not new_day,

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
            "threshold_pct": moves["threshold_pct"],
            "all": {k: moves[k] for k in ark_moves.KINDS},
        },

        "cross_fund_top": cross_fund[:30],

        "holdings_by_fund": {
            fund: sorted(positions, key=lambda p: -p["market_value"])
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
        # once per holdings date: a later run on the same holdings carries the diff but must not announce it again
        for p in (diffs["new_positions"][:3] if new_day else []):
            if p["market_value"] >= 5_000_000:
                events_pub.append(("ark.position_change", {
                    "change_type": "new_position",
                    "ticker":      p["ticker"],
                    "fund":        p["fund"],
                    "value":       p["market_value"],
                    "shares":      p["shares"],
                }))
        for p in (diffs["position_adds"][:3] if new_day else []):
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
        "version":          VERSION,
        "holdings_date":      holdings_date or None,
        "prev_holdings_date": prev_holdings_date or None,
        "new_day":          new_day,
        "baseline_kept":    baseline_kept,
    })}


lambda_handler = handler
