"""justhodl-lobbying-intel — forward signal from Congressional lobbying.

ROLE
════
Companies don't lobby hard on issues that don't matter to their P&L.
When lobbying spend SPIKES vs that client's own baseline, or when 3+
peers in the same sector suddenly lobby on the same issue, that's
typically a leading indicator of regulatory/policy change that hasn't
yet shown in stock prices.

Four distinct signal layers (composite scoring):
  1. SPIKE — client's recent lobbying spend vs own 90d trailing baseline
  2. CLUSTER — multiple companies same issue same window (sector catalyst)
  3. NEW ENTRANT — first-time lobbyist (regulatory exposure ramping)
  4. BILL TRACKER — H.R.XXXX / S.XXXX extracted from Specific_Issue text

This is NOT just "rank companies by lobbying dollar amount" — that's noise
(Boeing/Lockheed/J&J always spend big). The signal is in CHANGES and
CO-MOVEMENT across peers.

DATA SOURCE
═══════════
  https://api.quiverquant.com/beta/live/lobbying
  ~20K records no-auth, fields: {Date, Amount, Client, Issue,
  Specific_Issue, Registrant, Ticker}. Lobbying disclosures are
  filed quarterly under the Lobbying Disclosure Act (LD-2 forms).

OUTPUT
══════
  data/lobbying-intel.json
  
Emits lobbying.crowd_signal event for cluster signals
(3+ companies same issue, ≥$500k aggregate, in last 90d window).
"""
import json
import os
import re
import time
import urllib.error
import urllib.request
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone

import boto3
from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/lobbying-intel.json"
S3_CACHE_KEY = "data/quiver-lobbying-cache.json"

HTTP_TIMEOUT = 30
USER_AGENT = "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)"

# Recency windows
RECENT_DAYS = int(os.environ.get("RECENT_DAYS", "90"))     # primary
BASELINE_DAYS = int(os.environ.get("BASELINE_DAYS", "365")) # comparison

# Cluster thresholds — when do we call it a sector signal?
CLUSTER_MIN_CLIENTS = 3        # at least 3 distinct clients
CLUSTER_MIN_AMOUNT = 500_000   # at least $500k total in window

# Bill number patterns (House and Senate)
BILL_PATTERNS = [
    re.compile(r"\bH\.\s*R\.\s*(\d{1,5})\b", re.IGNORECASE),
    re.compile(r"\bH\.\s*Res\.?\s*(\d{1,5})\b", re.IGNORECASE),
    re.compile(r"\bS\.\s*(\d{1,5})\b"),
    re.compile(r"\bS\.\s*Res\.?\s*(\d{1,5})\b", re.IGNORECASE),
]

s3 = boto3.client("s3", region_name=REGION)


def _http_get(url, timeout=HTTP_TIMEOUT, retries=2):
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(5 * (attempt + 1))
                continue
            print(f"[lobbying] HTTP {e.code} from {url[:100]}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(3)
                continue
            print(f"[lobbying] err: {type(e).__name__} {str(e)[:100]}")
            return None
    return None


def fetch_lobbying_with_cache():
    """Try live Quiver first, fall back to S3 cache. Same resilience
    pattern as political-stocks (Quiver rate-limits repeat calls)."""
    print("[lobbying] fetching live Quiver /live/lobbying…")
    body = _http_get("https://api.quiverquant.com/beta/live/lobbying", timeout=45)
    if body:
        try:
            data = json.loads(body)
            if isinstance(data, list) and len(data) > 100:
                print(f"[lobbying] live OK: {len(data)} records")
                # Update S3 cache opportunistically
                try:
                    cache_obj = {
                        "schema_version": "1.0",
                        "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "n_records":      len(data),
                        "records":        data,
                    }
                    s3.put_object(
                        Bucket=BUCKET, Key=S3_CACHE_KEY,
                        Body=json.dumps(cache_obj, default=str,
                                         separators=(",", ":")).encode("utf-8"),
                        ContentType="application/json",
                        CacheControl="public, max-age=86400",
                    )
                except Exception as e:
                    print(f"[lobbying] cache write skipped: {e}")
                return data, "live"
        except Exception as e:
            print(f"[lobbying] live parse err: {e}")
    
    # Fall back to S3
    print("[lobbying] live failed/empty — trying S3 cache")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=S3_CACHE_KEY)
        cache = json.loads(obj["Body"].read().decode("utf-8"))
        records = cache.get("records") or []
        cache_dt = datetime.fromisoformat(
            cache["generated_at"].replace("Z", "+00:00").replace("+00:00+00:00", "+00:00")
        )
        age_h = (datetime.now(timezone.utc) - cache_dt).total_seconds() / 3600
        print(f"[lobbying] S3 cache: {len(records)} records (age {age_h:.1f}h)")
        return records, f"s3_cache_{age_h:.1f}h"
    except Exception as e:
        print(f"[lobbying] S3 cache missing: {e}")
        return [], "none"


def extract_bills(text: str) -> list:
    """Extract H.R.XXX / S.XXX bill references from Specific_Issue text."""
    if not text:
        return []
    bills = set()
    for pat in BILL_PATTERNS:
        for m in pat.finditer(text):
            num = m.group(1)
            label = pat.pattern.split(r"\\")[0].replace("\\b", "").replace(r"\.", ".").strip()
            # Normalize label
            if "H" in label.upper() and "Res" in label:
                bills.add(f"H.Res.{num}")
            elif "H" in label.upper():
                bills.add(f"H.R.{num}")
            elif "S" in label.upper() and "Res" in label:
                bills.add(f"S.Res.{num}")
            else:
                bills.add(f"S.{num}")
    return sorted(bills)


def parse_amount(val) -> float:
    """Quiver Amount field — sometimes string, sometimes number."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        try:
            s = str(val).replace("$", "").replace(",", "").strip()
            return float(s) if s else 0.0
        except Exception:
            return 0.0


# ─── Per-ticker aggregation ────────────────────────────────────────────

def aggregate_by_ticker(records: list) -> list:
    """Aggregate lobbying activity by ticker. Compute spike + new-issue +
    bill-mention signals."""
    today = datetime.now(timezone.utc).date()
    recent_cutoff = (today - timedelta(days=RECENT_DAYS)).isoformat()
    baseline_cutoff = (today - timedelta(days=BASELINE_DAYS)).isoformat()
    
    by_ticker = defaultdict(lambda: {
        "ticker": None,
        "client": None,
        "recent_amount":     0,    # last 90d
        "baseline_amount":   0,    # 90d-365d (prior 9 months)
        "all_records":       [],
        "recent_issues":     set(),
        "baseline_issues":   set(),
        "all_registrants":   set(),
        "bills_mentioned":   set(),
        "filing_dates":      [],
    })
    
    n_skipped = 0
    for r in records:
        try:
            ticker = (r.get("Ticker") or "").strip().upper()
            if not ticker or ticker in ("--", "N/A", ""):
                n_skipped += 1
                continue
            # Skip non-equity tickers
            ticker = ticker.split(" ")[0].split(".")[0]
            if not ticker.replace("-", "").isalpha() or len(ticker) > 6:
                n_skipped += 1
                continue
            
            date = (r.get("Date") or "")[:10]
            amount = parse_amount(r.get("Amount"))
            issue = (r.get("Issue") or "").strip()
            specific = r.get("Specific_Issue") or ""
            registrant = (r.get("Registrant") or "").strip()
            
            rec = by_ticker[ticker]
            rec["ticker"] = ticker
            rec["client"] = r.get("Client", "")
            rec["all_records"].append({
                "date":       date,
                "amount":     amount,
                "issue":      issue[:60],
                "registrant": registrant[:50],
                "specific":   specific[:200],
            })
            if registrant: rec["all_registrants"].add(registrant[:50])
            if date: rec["filing_dates"].append(date)
            
            if date >= recent_cutoff:
                rec["recent_amount"] += amount
                if issue: rec["recent_issues"].add(issue[:60])
            elif date >= baseline_cutoff:
                rec["baseline_amount"] += amount
                if issue: rec["baseline_issues"].add(issue[:60])
            
            # Extract bill references
            for b in extract_bills(specific):
                rec["bills_mentioned"].add(b)
        except Exception:
            n_skipped += 1
            continue
    
    print(f"[lobbying] processed {len(records)} records, {n_skipped} skipped, "
          f"{len(by_ticker)} unique tickers")
    
    # Build per-ticker scoring
    results = []
    for t, rec in by_ticker.items():
        # Acceleration: recent (90d) vs baseline (90d-365d, annualized to 90d equiv)
        baseline_per_90d = rec["baseline_amount"] * (90 / (BASELINE_DAYS - RECENT_DAYS))
        acceleration = (rec["recent_amount"] / baseline_per_90d) if baseline_per_90d > 0 else None
        
        # New issues: in recent but not baseline
        new_issues = rec["recent_issues"] - rec["baseline_issues"]
        # Disappeared issues: in baseline but not recent
        disappeared_issues = rec["baseline_issues"] - rec["recent_issues"]
        
        # First-time lobbyist? (no baseline activity, only recent)
        is_new_entrant = (rec["baseline_amount"] == 0 and rec["recent_amount"] > 0)
        
        # Score: 0-100
        score = 0
        # Spike contribution (up to +60)
        if acceleration is not None:
            if acceleration >= 5:   score += 60
            elif acceleration >= 3: score += 45
            elif acceleration >= 2: score += 30
            elif acceleration >= 1.5: score += 15
            elif acceleration < 0.5: score -= 10  # spending fading
        # New issues (+5 per, up to 20)
        score += min(20, len(new_issues) * 5)
        # New entrant (+25)
        if is_new_entrant: score += 25
        # Bill mentions (+5 per, up to 15)
        score += min(15, len(rec["bills_mentioned"]) * 5)
        # Recent absolute volume bonus
        if rec["recent_amount"] >= 1_000_000: score += 10
        elif rec["recent_amount"] >= 250_000: score += 5
        
        score = max(0, min(100, score))
        
        # Recent records sample (most recent 5)
        rec["all_records"].sort(key=lambda x: x["date"], reverse=True)
        sample = rec["all_records"][:5]
        
        # Thesis text
        thesis_bits = []
        if acceleration is not None and acceleration >= 2:
            thesis_bits.append(f"lobbying spend {acceleration:.1f}x baseline")
        if is_new_entrant:
            thesis_bits.append("first-time lobbyist (new regulatory exposure)")
        if len(new_issues) >= 2:
            thesis_bits.append(f"{len(new_issues)} new issues introduced")
        if rec["bills_mentioned"]:
            thesis_bits.append(f"specific bills tracked: {', '.join(sorted(rec['bills_mentioned'])[:3])}")
        thesis = " · ".join(thesis_bits) if thesis_bits else "Stable lobbying baseline"
        
        results.append({
            "ticker":               t,
            "client":               rec["client"],
            "score":                score,
            "recent_amount_usd":    rec["recent_amount"],
            "baseline_amount_usd":  rec["baseline_amount"],
            "acceleration_ratio":   round(acceleration, 2) if acceleration is not None else None,
            "n_recent_issues":      len(rec["recent_issues"]),
            "n_baseline_issues":    len(rec["baseline_issues"]),
            "new_issues":           sorted(new_issues),
            "disappeared_issues":   sorted(disappeared_issues),
            "is_new_entrant":       is_new_entrant,
            "registrants":          sorted(rec["all_registrants"]),
            "bills_mentioned":      sorted(rec["bills_mentioned"]),
            "n_records":            len(rec["all_records"]),
            "first_filing":         min(rec["filing_dates"]) if rec["filing_dates"] else None,
            "last_filing":          max(rec["filing_dates"]) if rec["filing_dates"] else None,
            "sample_records":       sample,
            "thesis":               thesis,
        })
    
    results.sort(key=lambda r: -r["score"])
    return results


# ─── Sector-wide issue clustering ──────────────────────────────────────

def detect_issue_clusters(records: list, ticker_to_sector: dict = None) -> list:
    """Find issues with 3+ distinct clients lobbying same direction in
    the recent window. These are sector-level catalysts."""
    today = datetime.now(timezone.utc).date()
    recent_cutoff = (today - timedelta(days=RECENT_DAYS)).isoformat()
    
    by_issue = defaultdict(lambda: {
        "issue":          None,
        "clients":        set(),
        "tickers":        set(),
        "total_amount":   0,
        "bill_mentions":  Counter(),
        "sample_records": [],
    })
    
    for r in records:
        date = (r.get("Date") or "")[:10]
        if date < recent_cutoff:
            continue
        issue = (r.get("Issue") or "").strip()
        if not issue:
            continue
        # Keep top of issue category (Quiver issues are often multi-line)
        issue_key = issue.split("\n")[0].strip()[:80]
        
        client = (r.get("Client") or "").strip()
        ticker = (r.get("Ticker") or "").strip().upper()
        amount = parse_amount(r.get("Amount"))
        
        rec = by_issue[issue_key]
        rec["issue"] = issue_key
        rec["clients"].add(client[:60])
        if ticker and ticker not in ("", "--", "N/A"):
            rec["tickers"].add(ticker)
        rec["total_amount"] += amount
        for b in extract_bills(r.get("Specific_Issue") or ""):
            rec["bill_mentions"][b] += 1
        if len(rec["sample_records"]) < 5:
            rec["sample_records"].append({
                "date":   date,
                "client": client[:60],
                "ticker": ticker,
                "amount": amount,
            })
    
    clusters = []
    for issue, rec in by_issue.items():
        if (len(rec["clients"]) >= CLUSTER_MIN_CLIENTS and
            rec["total_amount"] >= CLUSTER_MIN_AMOUNT):
            clusters.append({
                "issue":          rec["issue"],
                "n_clients":      len(rec["clients"]),
                "n_tickers":      len(rec["tickers"]),
                "total_amount":   rec["total_amount"],
                "top_tickers":    sorted(rec["tickers"])[:8],
                "top_clients":    sorted(rec["clients"])[:8],
                "top_bills":      [{"bill": b, "count": c}
                                    for b, c in rec["bill_mentions"].most_common(5)],
                "sample_records": rec["sample_records"],
            })
    
    clusters.sort(key=lambda c: -c["total_amount"])
    return clusters


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    # Fetch
    records, source = fetch_lobbying_with_cache()
    if not records:
        print("[lobbying] no data — skipping run")
        return {"statusCode": 200, "body": json.dumps({
            "ok": False, "reason": "no_data", "source": source,
        })}
    
    # Per-ticker aggregation
    by_ticker = aggregate_by_ticker(records)
    
    # Sector-wide issue clustering
    clusters = detect_issue_clusters(records)
    print(f"[lobbying] {len(clusters)} issue clusters detected")
    
    # Highlights
    spike_alerts = [r for r in by_ticker
                      if r["acceleration_ratio"] is not None
                      and r["acceleration_ratio"] >= 2.0
                      and r["recent_amount_usd"] >= 50_000][:25]
    new_lobbyists = [r for r in by_ticker if r["is_new_entrant"]
                       and r["recent_amount_usd"] >= 50_000][:20]
    bills_tracked = defaultdict(lambda: {"clients": set(), "tickers": set(), "amount": 0})
    for r in by_ticker:
        for b in r["bills_mentioned"]:
            bills_tracked[b]["clients"].add(r.get("client") or r["ticker"])
            bills_tracked[b]["tickers"].add(r["ticker"])
            bills_tracked[b]["amount"] += r["recent_amount_usd"]
    bills_summary = sorted([
        {"bill": b, "n_clients": len(d["clients"]),
         "n_tickers": len(d["tickers"]),
         "tickers": sorted(d["tickers"])[:10],
         "amount_usd": d["amount"]}
        for b, d in bills_tracked.items()
    ], key=lambda x: -x["amount_usd"])[:20]
    
    out = {
        "schema_version":  "1.0",
        "method":          "lobbying_intel_v1",
        "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":      round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "data_source":     "https://api.quiverquant.com/beta/live/lobbying",
        "lobbying_source": source,
        "windows": {
            "recent_days":   RECENT_DAYS,
            "baseline_days": BASELINE_DAYS,
        },
        
        "n_records_total":      len(records),
        "n_tickers":            len(by_ticker),
        "n_clusters":           len(clusters),
        "n_spike_alerts":       len(spike_alerts),
        "n_new_lobbyists":      len(new_lobbyists),
        "n_bills_tracked":      len(bills_summary),
        
        "highlights": {
            "spike_alerts":   spike_alerts,
            "new_lobbyists":  new_lobbyists,
            "issue_clusters": clusters[:30],
            "bills_tracked":  bills_summary,
        },
        
        "all_tickers": by_ticker[:200],
        
        "notes": (
            "Lobbying data from Quiver Quant. Signal is in CHANGES + CO-MOVEMENT, "
            "not absolute amounts. SPIKE = client's recent 90d spend ≥2x its own "
            "trailing baseline. CLUSTER = 3+ distinct clients on same Issue in 90d, "
            "≥$500k aggregate (likely sector catalyst). NEW ENTRANT = first-time "
            "lobbyist (regulatory exposure ramping). BILLS = H.R./S. references "
            "extracted from Specific_Issue field — tracks which legislation has "
            "the most concentrated lobbying interest."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=43200")
    print(f"[lobbying] wrote {len(body):,}B  "
          f"tickers={len(by_ticker)}  clusters={len(clusters)}  "
          f"spikes={len(spike_alerts)}  duration={out['duration_s']}s")
    
    # Emit cluster events (sector catalysts get Telegram alerts)
    try:
        from system_events import publish_many
        events_pub = []
        for c in clusters[:3]:
            events_pub.append(("lobbying.crowd_signal", {
                "issue":         c["issue"],
                "n_clients":     c["n_clients"],
                "n_tickers":     c["n_tickers"],
                "total_amount":  c["total_amount"],
                "top_tickers":   c["top_tickers"][:5],
                "top_bill":      c["top_bills"][0]["bill"] if c["top_bills"] else None,
            }))
        if events_pub:
            publish_many(events_pub)
    except Exception as e:
        print(f"[lobbying] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":              True,
        "n_records":       len(records),
        "n_tickers":       len(by_ticker),
        "n_clusters":      len(clusters),
        "n_spike_alerts":  len(spike_alerts),
        "n_new_lobbyists": len(new_lobbyists),
        "duration_s":      out["duration_s"],
        "source":          source,
    })}


lambda_handler = handler
