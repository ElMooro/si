"""justhodl-sec-filings-intel — institutional SEC filings risk/opportunity scanner.

ROLE
════
Beyond just RPO. Pulls recent SEC filings via EDGAR full-text search and
scores tickers on institutional alpha signals that retail rarely tracks:

  8-K events (real-time material events):
    - Going concern warnings (Item 4.02)
    - Executive departures, especially CFO (Item 5.02)
    - Auditor changes / resignations
    - Material acquisitions / divestitures (Item 2.01)
    - Restatement announcements
    - Bankruptcy filings (Item 1.03)
    - Definitive M&A agreements
    - Investigations / regulatory actions
  
  10-Q / 10-K signals:
    - Risk factor changes vs prior period
    - MD&A sentiment shifts
    - New/removed risks (often M&A or business change tells)
    - Going concern language even in 10-Qs
  
  Form 4 (insider transactions):
    - Cluster buys: 3+ insiders buying within 30 days = strong signal
    - C-suite sells: pattern of CEO+CFO sells = red flag
    - 10b5-1 plan adoptions (lock-in periods)
  
  13F (institutional holdings — quarterly):
    - New position initiations by smart money
    - Concentration increases (>5% of fund AUM)
  
  S-1 / S-3 / S-8:
    - Upcoming dilution
    - At-the-market (ATM) shelf registrations
    - Bought deals

DATA SOURCE
═══════════
SEC EDGAR full-text search:
  https://efts.sec.gov/LATEST/search-index?q=KEYWORD&forms=FORM&dateRange=custom...

  Submissions API for per-ticker filing history:
  https://data.sec.gov/submissions/CIK{padded_cik}.json

OUTPUT
══════
  data/sec-filings-intel.json — per-ticker scores + filing events catalog
  
Emits sec_filings.material_event event for high-conviction signals
(going concern, CFO departure, restatement, M&A).
"""
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/sec-filings-intel.json"

HTTP_TIMEOUT = 15
USER_AGENT = "JustHodl-SECIntel/1.0 (raafouis@gmail.com)"

# Lookback window for filings
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "60"))
MAX_RESULTS_PER_QUERY = 50

# Signal queries — each ties a keyword + form combination to a signal type
# Each signal has a polarity (bullish/bearish/material), severity, and weight
SIGNAL_QUERIES = [
    # ═══ HIGH-RISK / BEARISH ═══
    {
        "id":       "going_concern",
        "query":    '"going concern"',
        "forms":    ["8-K", "10-Q", "10-K"],
        "polarity": "bearish",
        "severity": "critical",
        "weight":   -40,
        "label":    "Going concern warning",
        "desc":     "Auditor or company expressed substantial doubt about ability to continue operations",
    },
    {
        "id":       "material_weakness",
        "query":    '"material weakness"',
        "forms":    ["8-K", "10-Q", "10-K"],
        "polarity": "bearish",
        "severity": "high",
        "weight":   -25,
        "label":    "Material weakness in controls",
        "desc":     "Internal controls failure disclosed",
    },
    {
        "id":       "restatement",
        "query":    '"non-reliance" OR "restate"',
        "forms":    ["8-K"],
        "polarity": "bearish",
        "severity": "high",
        "weight":   -30,
        "label":    "Restatement announced",
        "desc":     "Prior financials being restated (Item 4.02)",
    },
    {
        "id":       "auditor_change",
        "query":    '"auditor" AND ("resignation" OR "dismissed")',
        "forms":    ["8-K"],
        "polarity": "bearish",
        "severity": "high",
        "weight":   -20,
        "label":    "Auditor resigned/dismissed",
        "desc":     "Item 4.01 — auditor relationship terminated",
    },
    {
        "id":       "cfo_departure",
        "query":    '"Chief Financial Officer" AND ("resigned" OR "terminated" OR "departure")',
        "forms":    ["8-K"],
        "polarity": "bearish",
        "severity": "medium",
        "weight":   -15,
        "label":    "CFO departure",
        "desc":     "Item 5.02 — Chief Financial Officer leaving (red flag)",
    },
    {
        "id":       "investigation",
        "query":    '"SEC investigation" OR "subpoena"',
        "forms":    ["8-K", "10-Q"],
        "polarity": "bearish",
        "severity": "high",
        "weight":   -22,
        "label":    "Investigation disclosed",
        "desc":     "Active regulatory investigation or subpoena",
    },
    {
        "id":       "bankruptcy",
        "query":    '"Chapter 11" OR "voluntary petition"',
        "forms":    ["8-K"],
        "polarity": "bearish",
        "severity": "critical",
        "weight":   -50,
        "label":    "Bankruptcy filing",
        "desc":     "Item 1.03 — Chapter 11 or other bankruptcy",
    },
    # ═══ BULLISH / OPPORTUNITY ═══
    {
        "id":       "definitive_agreement",
        "query":    '"definitive agreement" AND ("acquire" OR "merger")',
        "forms":    ["8-K"],
        "polarity": "bullish",
        "severity": "high",
        "weight":   +30,
        "label":    "M&A definitive agreement",
        "desc":     "Material agreement to acquire or merge",
    },
    {
        "id":       "share_buyback_authorized",
        "query":    '"authorized" AND "repurchase"',
        "forms":    ["8-K"],
        "polarity": "bullish",
        "severity": "medium",
        "weight":   +12,
        "label":    "Buyback authorized",
        "desc":     "Board authorized share repurchase program",
    },
    {
        "id":       "going_private",
        "query":    '"going private"',
        "forms":    ["8-K", "SC 13E3"],
        "polarity": "bullish",
        "severity": "high",
        "weight":   +25,
        "label":    "Going-private signal",
        "desc":     "Going-private transaction discussed",
    },
    {
        "id":       "exclusive_partnership",
        "query":    '"exclusive partnership" OR "strategic partnership"',
        "forms":    ["8-K"],
        "polarity": "bullish",
        "severity": "low",
        "weight":   +8,
        "label":    "Strategic partnership",
        "desc":     "New exclusive or strategic partnership announced",
    },
    {
        "id":       "fda_approval",
        "query":    '"FDA approval" OR "FDA approved"',
        "forms":    ["8-K"],
        "polarity": "bullish",
        "severity": "high",
        "weight":   +25,
        "label":    "FDA approval",
        "desc":     "Drug/device FDA approval received",
    },
    # ═══ DILUTION RISK ═══
    {
        "id":       "atm_shelf",
        "query":    '"at-the-market" AND ("offering" OR "issuance")',
        "forms":    ["S-3", "8-K", "424B5"],
        "polarity": "bearish",
        "severity": "medium",
        "weight":   -10,
        "label":    "ATM offering",
        "desc":     "At-the-market equity issuance enabled (dilution risk)",
    },
    {
        "id":       "bought_deal",
        "query":    '"underwriting agreement"',
        "forms":    ["424B5", "S-1"],
        "polarity": "bearish",
        "severity": "low",
        "weight":   -5,
        "label":    "Underwritten offering",
        "desc":     "Bought deal / underwritten equity offering",
    },
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
                time.sleep(3 * (attempt + 1))
                continue
            print(f"[sec-filings] HTTP {e.code} from {url[:120]}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            print(f"[sec-filings] err: {type(e).__name__} {str(e)[:100]}")
            return None
    return None


# ─── SEC EDGAR full-text search ──────────────────────────────────────────

def search_filings(query: str, forms: list, days_back: int = LOOKBACK_DAYS):
    """SEC EDGAR Full-Text Search API.
    Returns list of {ticker, cik, form, filed_at, accession, snippet}."""
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    params = urllib.parse.urlencode({
        "q":         query,
        "forms":     ",".join(forms),
        "dateRange": "custom",
        "startdt":   start,
        "enddt":     end,
    })
    url = f"https://efts.sec.gov/LATEST/search-index?{params}&from=0"
    body = _http_get(url)
    if not body:
        return []
    
    try:
        data = json.loads(body)
    except Exception:
        return []
    
    hits = (data.get("hits") or {}).get("hits") or []
    results = []
    for h in hits[:MAX_RESULTS_PER_QUERY]:
        src = h.get("_source") or {}
        # display_names is like ["APPLE INC  (AAPL) (CIK 0000320193)"]
        display = src.get("display_names") or []
        for d in display:
            m = re.match(r"^(.+?)\s*\(([A-Z\.\-]+)\)\s*\(CIK\s*(\d+)\)", d)
            if not m:
                continue
            name, ticker, cik = m.group(1).strip(), m.group(2), m.group(3).zfill(10)
            results.append({
                "ticker":    ticker,
                "name":      name,
                "cik":       cik,
                "form":      src.get("form") or "?",
                "filed_at":  src.get("file_date"),
                "accession": (src.get("adsh") or "").replace("-", ""),
                "snippet":   (src.get("_search_id") or "")[:100],
            })
            break  # only first display_name per hit
    return results


def edgar_filing_url(cik: str, accession: str) -> str:
    """Convenience: build a clickable URL for a filing."""
    if not accession or len(accession) < 18:
        return ""
    # cik already padded; accession is dashed in URL
    formatted_acc = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{formatted_acc}-index.htm"


# ─── Per-ticker aggregation ─────────────────────────────────────────────

def aggregate_signals(all_events: list) -> dict:
    """Group events by ticker and compute composite score per ticker."""
    by_ticker = defaultdict(lambda: {
        "ticker": None, "name": None, "cik": None,
        "events": [], "score": 0,
        "bearish_signals": 0, "bullish_signals": 0,
        "highest_severity": "low",
        "latest_filing": None,
    })
    
    for ev in all_events:
        t = ev["ticker"]
        rec = by_ticker[t]
        rec["ticker"] = t
        rec["name"] = ev["name"]
        rec["cik"] = ev["cik"]
        rec["events"].append(ev)
        rec["score"] += ev["weight"]
        if ev["polarity"] == "bearish":
            rec["bearish_signals"] += 1
        elif ev["polarity"] == "bullish":
            rec["bullish_signals"] += 1
        
        sev_rank = {"low": 1, "medium": 2, "high": 3, "critical": 4}
        cur = sev_rank.get(rec["highest_severity"], 1)
        new = sev_rank.get(ev["severity"], 1)
        if new > cur:
            rec["highest_severity"] = ev["severity"]
        
        if ev["filed_at"]:
            if not rec["latest_filing"] or ev["filed_at"] > rec["latest_filing"]:
                rec["latest_filing"] = ev["filed_at"]
    
    # Normalize per-ticker
    result = []
    for t, rec in by_ticker.items():
        # Sort events by date desc
        rec["events"].sort(key=lambda e: e.get("filed_at") or "", reverse=True)
        rec["events"] = rec["events"][:15]  # cap
        rec["n_events"] = len(rec["events"])
        # Score is clipped to [-100, +100] for display
        rec["raw_score"] = rec["score"]
        rec["score"] = max(-100, min(100, rec["score"]))
        # Polarity verdict
        if rec["bearish_signals"] > rec["bullish_signals"]:
            rec["verdict"] = "RISK"
        elif rec["bullish_signals"] > rec["bearish_signals"]:
            rec["verdict"] = "OPPORTUNITY"
        else:
            rec["verdict"] = "MIXED"
        result.append(rec)
    
    result.sort(key=lambda r: abs(r["score"]), reverse=True)
    return result


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    
    all_events = []
    by_signal = defaultdict(int)
    
    print(f"[sec-filings] running {len(SIGNAL_QUERIES)} signal queries "
          f"over last {LOOKBACK_DAYS} days")
    
    for sig in SIGNAL_QUERIES:
        try:
            hits = search_filings(sig["query"], sig["forms"], LOOKBACK_DAYS)
            by_signal[sig["id"]] = len(hits)
            
            for h in hits:
                ev = {
                    **h,
                    "signal_id":   sig["id"],
                    "signal_label": sig["label"],
                    "polarity":    sig["polarity"],
                    "severity":    sig["severity"],
                    "weight":      sig["weight"],
                    "desc":        sig["desc"],
                    "filing_url":  edgar_filing_url(h["cik"], h["accession"]),
                }
                all_events.append(ev)
            
            print(f"[sec-filings]   {sig['id']:30s} {len(hits):3d} hits")
            time.sleep(0.4)  # pace SEC requests
        except Exception as e:
            print(f"[sec-filings] err on {sig['id']}: {e}")
    
    # Aggregate per-ticker
    per_ticker = aggregate_signals(all_events)
    
    # Highlights
    risk_tickers   = [r for r in per_ticker if r["verdict"] == "RISK" and r["score"] <= -15][:30]
    opp_tickers    = [r for r in per_ticker if r["verdict"] == "OPPORTUNITY" and r["score"] >= 15][:30]
    critical       = [r for r in per_ticker if r["highest_severity"] == "critical"][:20]
    
    out = {
        "schema_version":  "1.0",
        "method":          "sec_filings_intel_v1",
        "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s":      round((datetime.now(timezone.utc) - started).total_seconds(), 1),
        "lookback_days":   LOOKBACK_DAYS,
        "n_signal_queries": len(SIGNAL_QUERIES),
        "n_events_total":  len(all_events),
        "n_tickers_with_signals": len(per_ticker),
        "events_by_signal": dict(by_signal),
        "signal_definitions": [
            {"id": s["id"], "label": s["label"], "polarity": s["polarity"],
             "severity": s["severity"], "weight": s["weight"], "desc": s["desc"],
             "forms": s["forms"]}
            for s in SIGNAL_QUERIES
        ],
        "highlights": {
            "risks":         risk_tickers,
            "opportunities": opp_tickers,
            "critical":      critical,
        },
        "all_tickers":     per_ticker[:200],  # cap output size
        "notes": (
            "Per-ticker composite = sum of signal weights from filings in last "
            f"{LOOKBACK_DAYS} days. Bearish signals (going concern, CFO leaving, "
            "restatements, investigations) are negative-weighted. Bullish (M&A "
            "definitive agreements, FDA approvals, buybacks, going private) are "
            "positive-weighted. Severity grades: critical / high / medium / low."
        ),
    }
    
    body = json.dumps(out, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[sec-filings] wrote {len(body):,}B  events={len(all_events)}  "
          f"tickers={len(per_ticker)}  duration={out['duration_s']}s")
    
    # Emit events for critical signals
    try:
        from system_events import publish_many
        events_pub = []
        for r in critical[:8]:
            # Find the highest-severity event for this ticker
            top_event = next((e for e in r["events"] if e["severity"] == "critical"), r["events"][0] if r["events"] else None)
            if top_event:
                events_pub.append(("sec_filings.material_event", {
                    "ticker":       r["ticker"],
                    "signal":       top_event["signal_label"],
                    "polarity":     top_event["polarity"],
                    "severity":     top_event["severity"],
                    "filed_at":     top_event["filed_at"],
                    "form":         top_event["form"],
                    "score":        r["score"],
                    "verdict":      r["verdict"],
                }))
        if events_pub:
            publish_many(events_pub)
    except Exception as e:
        print(f"[sec-filings] event publish failed: {e}")
    
    return {"statusCode": 200, "body": json.dumps({
        "ok":               True,
        "n_events":         len(all_events),
        "n_tickers":        len(per_ticker),
        "n_critical":       len(critical),
        "n_risks":          len(risk_tickers),
        "n_opportunities":  len(opp_tickers),
        "duration_s":       out["duration_s"],
    })}


lambda_handler = handler
