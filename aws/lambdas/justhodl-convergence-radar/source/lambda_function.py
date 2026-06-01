"""
justhodl-convergence-radar
══════════════════════════
Reads all 20+ engine outputs every 30 minutes. For each ticker, scores
how many INDEPENDENT engines are flagging it right now. Detects
ACCELERATION (was 0 engines yesterday, now 5 today = pre-pump setup).
Telegram alert fires the moment a ticker crosses the 4-engine threshold.

WHY THIS EXISTS
═══════════════
Today's 200%-velocity alert fired on SAP/CXAI/NBIS/ARM/RDDT — by then
they had already pumped. Post-audit showed:
  ARM:  10 engines flagging it BEFORE pump
  RDDT:  4 engines flagging it
  NBIS:  2 engines flagging it
The leading signal was already in our data — just not unified.

OUTPUT
══════
s3://justhodl-dashboard-live/data/convergence-radar.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "tickers": [                  # sorted by convergence_score desc
    {
      "ticker":          "ARM",
      "n_engines":       8,
      "convergence_score": 87,
      "acceleration":    4,    # engines today vs yesterday
      "domain_coverage": ["sentiment", "flows", "earnings", "insider"],
      "engines": {
        "buzz-velocity":     {"score": 65, "details": {...}},
        "momentum-breakout": {"tier": "A", "score": 78},
        ...
      },
      "is_new_high":     true,  # crossed 4-engine threshold this run
      "is_accelerating": true,
    }
  ],
  "summary": {
    "n_tickers_radar":      45,
    "n_new_high_convergence": 3,
    "n_accelerating":         12,
    "top_5":                 ["ARM", "RDDT", ...],
  }
}

ALERTS
══════
- NEW_HIGH: ticker crossed 4-engine threshold this run (was below 4 last run)
- ACCELERATING: n_engines jumped 3+ in single 30-min interval
- ULTRA: 8+ engines simultaneously flagging same ticker
"""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET    = "justhodl-dashboard-live"
OUTPUT_KEY   = "data/convergence-radar.json"
STATE_KEY    = "data/_alerts/convergence-radar-state.json"
ALERT_KEY    = "data/_alerts/convergence-radar-alerted.json"

TELEGRAM_TOKEN   = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# ENGINE EXTRACTORS
# Each entry: (s3_key, list_or_dict_path, ticker_field, score_field, domain)
# Returns ordered list of (ticker, signal_data) tuples
# ═════════════════════════════════════════════════════════════════════

ENGINE_EXTRACTORS = {
    # ─── Retail Sentiment Domain ─────────────────────────────
    "retail-sentiment": {
        "key":    "data/retail-sentiment.json",
        "path":   "top_30_by_mentions",
        "ticker": "ticker",
        "score":  "velocity_pct",
        "domain": "sentiment",
        "extra":  ["mentions", "upvotes", "rank"],
    },
    "stocktwits-trending": {
        "key":    "data/retail-sentiment.json",
        "path":   "stocktwits_trending",
        "ticker": "symbol",
        "score":  "trending_score",
        "domain": "sentiment",
        "extra":  ["watchlist_count", "sector"],
    },
    "buzz-velocity": {
        "key":    "data/buzz-velocity.json",
        "path":   "top_30",
        "ticker": "ticker",
        "score":  "composite_velocity",
        "domain": "sentiment",
        "extra":  ["reddit_velocity", "news_velocity", "sentiment", "price_perf_7d_pct"],
    },
    "ticker-trends": {
        "key":    "data/ticker-trends.json",
        "path":   "top_20",
        "ticker": "ticker",
        "score":  "velocity",
        "domain": "sentiment",
        "extra":  ["current_level", "interp", "price_7d_pct"],
    },
    "news-velocity": {
        "key":     "data/news-velocity.json",
        "path":    "by_ticker",
        "is_dict": True,
        "score":   "spike_score",
        "domain":  "sentiment",
        "extra":   ["current_volume", "z_score"],
    },

    # ─── Momentum / Flow Domain ──────────────────────────────
    "momentum-breakout": {
        "key":    "data/momentum-breakout.json",
        "path":   "all_qualifying",
        "ticker": "symbol",
        "score":  "score",
        "domain": "momentum",
        "extra":  ["tier", "flags", "is_parabolic"],
    },
    "options-flow": {
        "key":    "data/options-flow.json",
        "path":   "all_qualifying",
        "ticker": "symbol",
        "score":  "score",
        "domain": "options",
        "extra":  ["tier", "flags"],
    },
    "sympathetic-momentum": {
        "key":    "data/sympathetic-momentum.json",
        "path":   "top_proxies",
        "ticker": "ticker",
        "score":  "score",
        "domain": "momentum",
        "extra":  ["correlation", "leader"],
    },

    # ─── Earnings / Fundamentals Domain ──────────────────────
    "eps-revision-velocity": {
        "key":    "data/eps-revision-velocity.json",
        "path":   "all_qualifying",
        "ticker": "symbol",
        "score":  "score",
        "domain": "earnings",
        "extra":  ["flag", "status"],
    },
    "earnings-tracker-upcoming": {
        "key":    "data/earnings-tracker.json",
        "path":   "upcoming_14d",
        "ticker": "ticker",
        "score":  None,
        "domain": "earnings",
        "extra":  ["earnings_date", "time", "eps_consensus"],
        "default_score": 50,  # presence-only signal
    },
    "earnings-pead": {
        "key":    "data/earnings-pead.json",
        "path":   "all_qualifying",
        "ticker": "symbol",
        "score":  "score",
        "domain": "earnings",
        "extra":  ["tier", "beat_streak"],
    },
    "earnings-cascade": {
        "key":    "data/earnings-cascade.json",
        "path":   "strong_cascades",
        "ticker": "ticker",
        "score":  "cascade_score",
        "domain": "earnings",
        "extra":  ["band", "sector"],
    },
    "earnings-whisper": {
        "key":    "data/earnings-whisper.json",
        "path":   "top_setups",
        "ticker": "ticker",
        "score":  None,
        "domain": "earnings",
        "extra":  ["earnings_date", "days_to_earnings"],
        "default_score": 55,
    },
    "fundamentals-quality": {
        "key":    "data/fundamentals.json",
        "path":   "companies",
        "ticker": "ticker",
        "score":  "piotroski",  # 0-9 piotroski (high = quality)
        "domain": "valuation",
        "extra":  ["valuation_label", "dcf_gap_pct", "altman_z"],
        "score_scale": (0, 9),  # normalize to 0-100
    },

    # ─── Insider / Institutional Domain ──────────────────────
    "sec-filings-intel": {
        "key":    "data/sec-filings-intel.json",
        "path":   "all_tickers",
        "ticker": "ticker",
        "score":  "score",
        "domain": "insider",
        "extra":  ["highest_severity", "events"],
    },
    "political-trades": {
        "key":    "data/political-trades.json",
        "path":   "high_watch_recent_15",
        "ticker": "ticker",
        "score":  None,
        "domain": "insider",
        "extra":  ["member", "transaction_type", "amount_range"],
        "default_score": 60,
    },
    "ark-holdings": {
        "key":    "data/ark-holdings.json",
        "path":   "cross_fund_top",
        "ticker": "ticker",
        "score":  "n_funds",
        "domain": "institutional",
        "extra":  ["total_value", "max_weight"],
        "score_scale": (0, 5),
    },
    "lobbying-intel": {
        "key":    "data/lobbying-intel.json",
        "path":   "all_tickers",
        "ticker": "ticker",
        "score":  "score",
        "domain": "insider",
        "extra":  ["recent_amount_usd", "acceleration_ratio"],
    },
    "hiring-velocity": {
        "key":    "data/hiring-velocity.json",
        "path":   "double_confirmed",
        "ticker": "symbol",
        "score":  "expansion_score",
        "domain": "fundamentals",
        "extra":  ["sector", "headcount_latest"],
    },
    "capital-return": {
        "key":    "data/capital-return.json",
        "path":   "cannibals",
        "ticker": "symbol",
        "score":  "cannibal_score",
        "domain": "fundamentals",
        "extra":  ["buyback_yield_pct", "dividend_yield_pct"],
    },
}

# Domains for cross-coverage bonus
ALL_DOMAINS = {"sentiment", "momentum", "options", "earnings", "valuation",
                "insider", "institutional", "fundamentals"}


# ═════════════════════════════════════════════════════════════════════
# Engine fetching + extraction
# ═════════════════════════════════════════════════════════════════════

def fetch_engine_raw(spec_name: str, spec: dict) -> tuple:
    """Fetch + parse a single engine's data file. Returns (spec_name, items_list, age_h)."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=spec["key"])
        d = json.loads(obj["Body"].read())
        age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
        if spec.get("is_dict"):
            # by_ticker dict — convert to list with synthetic ticker field
            container = d.get(spec["path"], {})
            items = []
            for tk, v in container.items():
                if isinstance(v, dict):
                    items.append({**v, "_ticker_from_dict_key": tk})
            return spec_name, items, age_h
        container = d.get(spec["path"], [])
        return spec_name, container if isinstance(container, list) else [], age_h
    except Exception as e:
        print(f"[{spec_name}] fetch err: {e}")
        return spec_name, [], None


def normalize_ticker(t: str) -> str:
    """Clean up ticker symbol: uppercase, strip $/whitespace."""
    if not t:
        return ""
    return str(t).strip().upper().lstrip("$").split(".")[0]


def normalize_score(raw, score_scale=None) -> Optional[float]:
    """Map raw score to 0-100 scale. score_scale=(min,max) optional."""
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if score_scale:
        lo, hi = score_scale
        if hi == lo:
            return 50.0
        return max(0, min(100, (v - lo) / (hi - lo) * 100))
    # If already 0-100ish, leave it; if it's percentage (-100..+100), normalize
    if abs(v) > 200:
        # Velocity percentage could be 1000% — clip to 100
        return min(100, max(0, abs(v) / 10))
    return min(100, max(0, abs(v)))


def extract_ticker_signals_from_engine(spec_name: str, spec: dict, items: list) -> dict:
    """Returns {ticker: signal_dict} for this engine's items."""
    out = {}
    if spec.get("is_dict"):
        ticker_field = "_ticker_from_dict_key"
    else:
        ticker_field = spec["ticker"]
    score_field   = spec.get("score")
    extra_fields  = spec.get("extra", [])
    default_score = spec.get("default_score")
    score_scale   = spec.get("score_scale")

    for item in items:
        if not isinstance(item, dict):
            continue
        t = normalize_ticker(item.get(ticker_field, ""))
        if not t or len(t) > 6 or not t.isalnum():
            continue  # filter junk

        raw_score = item.get(score_field) if score_field else default_score
        norm = normalize_score(raw_score, score_scale=score_scale) if raw_score is not None else default_score

        signal = {
            "domain":      spec["domain"],
            "score":       round(norm, 1) if norm is not None else None,
            "raw_score":   raw_score,
        }
        for f in extra_fields:
            v = item.get(f)
            if v is not None:
                signal[f] = v
        out[t] = signal
    return out


def fetch_all_engines() -> dict:
    """Parallel fetch + extract all engines.
    Returns {ticker: {engine_name: signal}}"""
    ticker_signals: Dict[str, Dict[str, dict]] = {}
    engine_ages: Dict[str, float] = {}

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(fetch_engine_raw, name, spec): (name, spec)
                    for name, spec in ENGINE_EXTRACTORS.items()}
        for fut in as_completed(futures, timeout=90):
            name, spec = futures[fut]
            try:
                _, items, age_h = fut.result()
                engine_ages[name] = age_h
                if not items:
                    continue
                # Skip stale engines (> 24h old)
                if age_h is not None and age_h > 24:
                    continue
                tick_map = extract_ticker_signals_from_engine(name, spec, items)
                for t, sig in tick_map.items():
                    ticker_signals.setdefault(t, {})[name] = sig
            except Exception as e:
                print(f"[{name}] extract err: {e}")
    return ticker_signals, engine_ages


# ═════════════════════════════════════════════════════════════════════
# Convergence scoring
# ═════════════════════════════════════════════════════════════════════

def compute_convergence(ticker: str, engines: Dict[str, dict]) -> dict:
    """Build the convergence record for a single ticker."""
    n_engines = len(engines)
    domains = set(sig.get("domain") for sig in engines.values() if sig.get("domain"))
    domain_coverage = sorted(domains)

    # Sum of normalized scores (None → 50 as presence indicator)
    score_sum = sum((sig.get("score") or 50.0) for sig in engines.values())
    score_avg = score_sum / n_engines if n_engines else 0

    # Convergence score: weighted by n_engines, domain breadth, avg signal strength
    # Max possible per ticker: 100 (we cap at this)
    n_engine_pts   = min(n_engines * 8, 50)     # 8 pts per engine up to 6.25 engines
    domain_pts     = len(domains) * 5            # 5 pts per unique domain
    strength_pts   = min(score_avg * 0.30, 30)  # avg signal strength contributes up to 30
    convergence_score = round(n_engine_pts + domain_pts + strength_pts, 1)
    convergence_score = min(100, convergence_score)

    return {
        "ticker":           ticker,
        "n_engines":        n_engines,
        "convergence_score": convergence_score,
        "domain_coverage":  domain_coverage,
        "n_domains":        len(domains),
        "avg_signal":       round(score_avg, 1),
        "engines":          engines,
    }


def classify_tier(score: float, n_engines: int) -> str:
    """Tier the convergence:
      ULTRA = 8+ engines OR score >= 85
      HIGH  = 5-7 engines OR score >= 65
      MED   = 3-4 engines OR score >= 40
      LOW   = 2 engines
      NOISE = 1 engine
    """
    if n_engines >= 8 or score >= 85:
        return "ULTRA"
    if n_engines >= 5 or score >= 65:
        return "HIGH"
    if n_engines >= 3 or score >= 40:
        return "MED"
    if n_engines >= 2:
        return "LOW"
    return "NOISE"


# ═════════════════════════════════════════════════════════════════════
# State management — for acceleration + transition detection
# ═════════════════════════════════════════════════════════════════════

def load_prior_state() -> dict:
    """Load prior run's ticker scores for acceleration detection."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        print(f"[state] load err: {e}")
        return {}


def save_state(records: List[dict]) -> None:
    state = {
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
        "tickers":     {r["ticker"]: {
            "n_engines":         r["n_engines"],
            "convergence_score": r["convergence_score"],
            "tier":              r["tier"],
        } for r in records},
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=STATE_KEY,
                        Body=json.dumps(state, default=str),
                        ContentType="application/json")
    except Exception as e:
        print(f"[state] save err: {e}")


def load_recent_alerts() -> dict:
    """Track which tickers we've already alerted on recently (24h cool-down)."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=ALERT_KEY)
        d = json.loads(obj["Body"].read())
        # Prune entries older than 24h
        now = datetime.now(timezone.utc)
        pruned = {}
        for t, iso in d.items():
            try:
                age_h = (now - datetime.fromisoformat(iso.replace("Z", "+00:00"))).total_seconds() / 3600
                if age_h < 24:
                    pruned[t] = iso
            except Exception:
                pass
        return pruned
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        print(f"[alert state] load err: {e}")
        return {}


def save_recent_alerts(alerts: dict) -> None:
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=ALERT_KEY,
                        Body=json.dumps(alerts, default=str),
                        ContentType="application/json")
    except Exception as e:
        print(f"[alert state] save err: {e}")


# ═════════════════════════════════════════════════════════════════════
# Telegram alerting on NEW high-convergence tickers
# ═════════════════════════════════════════════════════════════════════

def send_telegram(text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id":  TELEGRAM_CHAT_ID, "text": text[:4096],
        "parse_mode": "Markdown", "disable_web_page_preview": True,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=payload,
                                        headers={"Content-Type": "application/json"},
                                        method="POST")
        with urllib.request.urlopen(req, timeout=10) as r:
            return bool(json.loads(r.read())["ok"])
    except Exception as e:
        print(f"[telegram] err: {e}")
        return False


def build_alert_message(new_high: list, accelerating: list, ultra: list) -> str:
    lines = []
    lines.append("🎯 *Convergence Radar Alert*")
    lines.append(f"_{datetime.now(timezone.utc).strftime('%b %d %H:%M')} UTC_\n")

    if ultra:
        lines.append("🚨 *ULTRA convergence* (8+ engines):")
        for r in ultra:
            engines = ", ".join(sorted(r["engines"].keys())[:6])
            extra = "..." if r["n_engines"] > 6 else ""
            lines.append(f"  • *{r['ticker']}*  ({r['n_engines']} engines, score {r['convergence_score']:.0f}/100)")
            lines.append(f"    domains: {' · '.join(r['domain_coverage'])}")
            lines.append(f"    engines: {engines}{extra}")

    if new_high:
        lines.append("\n⚡ *NEW high convergence* (just crossed 4-engine threshold):")
        for r in new_high:
            lines.append(f"  • *{r['ticker']}*  ({r['n_engines']} engines, was {r.get('prior_n_engines', 0)})")
            lines.append(f"    domains: {' · '.join(r['domain_coverage'])}")

    if accelerating:
        lines.append("\n📈 *Accelerating* (n_engines jumped 3+ in 30 min):")
        for r in accelerating:
            lines.append(f"  • *{r['ticker']}*  {r.get('prior_n_engines', 0)} → {r['n_engines']} engines")

    lines.append("\n🔗 https://justhodl.ai/pre-pump-radar.html")
    return "\n".join(lines)


def maybe_alert(records: List[dict], prior_state: dict, recent_alerts: dict) -> dict:
    """Detect transitions and emit Telegram alerts.

    FIRST RUN (empty prior state): send SYSTEM_INITIALIZED with current top 5 ULTRA
      tickers as a snapshot — don't flood with all 130 multi-engine candidates.

    SUBSEQUENT RUNS: only fire on genuine TRANSITIONS:
      - NEW_HIGH:     crossed 4-engine threshold this cycle (was below 4)
      - ACCELERATING: n_engines jumped 3+ in single cycle, AND ended at >= 5
      - ULTRA:        crossed into 8+ engines this cycle (was below 8)

    Cool-down: 24h per ticker (no repeat alerts).
    """
    prior_tickers = (prior_state or {}).get("tickers", {})
    is_first_run = (len(prior_tickers) == 0)

    new_high      = []
    accelerating  = []
    ultra_new     = []

    for r in records:
        t = r["ticker"]
        cur_n = r["n_engines"]
        cur_score = r["convergence_score"]
        prior = prior_tickers.get(t, {})
        prior_n = prior.get("n_engines", 0)

        r["prior_n_engines"] = prior_n
        # For first run we suppress transition detection so we don't flood
        if is_first_run:
            r["is_new_high"] = False
            r["is_accelerating"] = False
            r["is_ultra_new"] = False
        else:
            r["is_new_high"]     = (cur_n >= 4 and prior_n < 4)
            r["is_accelerating"] = (cur_n - prior_n >= 3 and cur_n >= 5)
            r["is_ultra_new"]    = (cur_n >= 8 and prior_n < 8)

        # Skip if already alerted in last 24h
        if t in recent_alerts:
            continue

        if r["is_ultra_new"]:
            ultra_new.append(r)
        elif r["is_new_high"] and cur_score >= 60:
            # Only NEW_HIGH if score is meaningful (avoid noise from low-quality 4-engine convergences)
            new_high.append(r)
        elif r["is_accelerating"] and cur_score >= 55:
            accelerating.append(r)

    sent = False
    msg = None

    if is_first_run:
        # Boot message with current top ULTRA tickers
        cur_ultra = [r for r in records if r["n_engines"] >= 8][:8]
        cur_high  = [r for r in records if r["tier"] == "HIGH"][:6]
        lines = [
            "🆕 *Convergence Radar ONLINE*",
            f"_{datetime.now(timezone.utc).strftime('%b %d %H:%M')} UTC_",
            "",
            f"📊 Tracking *{len(records)}* multi-engine tickers",
            f"🚨 Currently ULTRA: *{len(cur_ultra)}*  |  HIGH: *{len(cur_high)}*",
            "",
            "*Current ULTRA convergence (8+ engines):*",
        ]
        for r in cur_ultra:
            lines.append(f"  • *{r['ticker']}*  ({r['n_engines']} engines, score {r['convergence_score']:.0f}/100)")
        lines.append("")
        lines.append("Next runs will alert only on NEW transitions (no flooding).")
        lines.append("")
        lines.append("🔗 https://justhodl.ai/pre-pump-radar.html")
        msg = "\n".join(lines)
        sent = send_telegram(msg)
    elif ultra_new or new_high or accelerating:
        msg = build_alert_message(new_high[:6], accelerating[:6], ultra_new[:4])
        sent = send_telegram(msg)

    if sent:
        now_iso = datetime.now(timezone.utc).isoformat()
        for r in ultra_new + new_high + accelerating:
            recent_alerts[r["ticker"]] = now_iso
        save_recent_alerts(recent_alerts)

    return {
        "sent":              sent,
        "is_first_run":      is_first_run,
        "n_ultra_new":       len(ultra_new),
        "n_new_high":        len(new_high),
        "n_accelerating":    len(accelerating),
        "ultra_new_tickers": [r["ticker"] for r in ultra_new],
        "new_high_tickers":  [r["ticker"] for r in new_high],
        "accelerating_tickers": [r["ticker"] for r in accelerating],
    }


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[convergence-radar] start {datetime.now(timezone.utc).isoformat()}")

    # 1. Fetch all engines + extract per-ticker signals
    print("[ws] phase 1: fetch + extract all engines…")
    ticker_signals, engine_ages = fetch_all_engines()
    n_tickers_raw = len(ticker_signals)
    print(f"[ws] {n_tickers_raw} unique tickers across {len(engine_ages)} engines")

    # 2. Filter to multi-engine tickers (≥2 engines = the minimum signal floor)
    multi_engine = {t: e for t, e in ticker_signals.items() if len(e) >= 2}

    # 3. Compute convergence per ticker
    records = []
    for t, engines in multi_engine.items():
        rec = compute_convergence(t, engines)
        rec["tier"] = classify_tier(rec["convergence_score"], rec["n_engines"])
        records.append(rec)

    # 4. Sort by convergence_score desc, then n_engines desc
    records.sort(key=lambda r: (-r["convergence_score"], -r["n_engines"]))

    # 5. Load state + detect transitions + alert
    prior_state   = load_prior_state()
    recent_alerts = load_recent_alerts()
    alert_info    = maybe_alert(records, prior_state, recent_alerts)

    # 6. Save current state for next run's acceleration detection
    save_state(records[:100])  # top 100 to keep state small

    # 7. Build summary + write output
    summary = {
        "n_tickers_total":        n_tickers_raw,
        "n_tickers_multi_engine": len(records),
        "n_ultra":                sum(1 for r in records if r["tier"] == "ULTRA"),
        "n_high":                 sum(1 for r in records if r["tier"] == "HIGH"),
        "n_med":                  sum(1 for r in records if r["tier"] == "MED"),
        "n_new_high_convergence": alert_info["n_new_high"],
        "n_accelerating":         alert_info["n_accelerating"],
        "top_10":                 [r["ticker"] for r in records[:10]],
        "engines_loaded":         len(engine_ages),
        "engines_stale":          sum(1 for a in engine_ages.values() if a is not None and a > 24),
    }

    output = {
        "schema_version":  "1.0",
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":     round(time.time() - t0, 2),
        "summary":         summary,
        "tickers":         records[:200],  # cap to top 200
        "engine_ages_h":   {k: round(v, 1) if v is not None else None for k, v in engine_ages.items()},
        "alert_info":      alert_info,
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=900",
    )
    archive_key = (f"data/archive/convergence-radar/"
                    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception:
        pass

    resp_summary = {
        "status":         "ok",
        "elapsed_sec":    output["elapsed_sec"],
        "n_tickers":      summary["n_tickers_multi_engine"],
        "n_ultra":        summary["n_ultra"],
        "n_high":         summary["n_high"],
        "alert_sent":     alert_info["sent"],
        "top_3":          summary["top_10"][:3],
    }
    print(f"[convergence-radar] done: {resp_summary}")
    return {"statusCode": 200, "body": json.dumps(resp_summary)}
