"""
justhodl-earnings-whisper — Pre-earnings surprise probability scorer.

WHY THIS EXISTS
───────────────
Platform has earnings-tracker (calendar + post-earnings PEAD) and pead-detector
(post-earnings drift), but NO Lambda combines pre-announcement signals to predict
which names are MOST LIKELY to beat earnings in the next 14-30 days.

Academic basis: Bartov et al. (1992) and many follow-ups document 4-8% alpha
over 21 days for names with the right combination of: positive analyst revisions,
unusual call activity, no insider selling, positive material disclosures.

ALGORITHM (per upcoming-earnings ticker)
─────────────────────────────────────────
For each name on earnings-tracker.upcoming_14d:

  Component 1: EPS REVISION VELOCITY (max 25 pts)
    From eps-revision-velocity.json — score / TIER mapping:
      TIER_S_HIGHEST     → 25
      TIER_A_HIGH        → 18
      TIER_B_MODERATE    → 10
      TIER_C / not found → 0

  Component 2: OPTIONS FLOW BULLISHNESS (max 20 pts)
    From options-flow.json — score / tier:
      TIER_A_BULLISH_FLOW (score>=80) → 20
      TIER_B_BULLISH (score>=60)      → 12
      TIER_C / no data                → 0
    Bonus: +5 if 'CPR_SURGING' or 'CALL_VOL_3X' in flags

  Component 3: INSIDER POSITIONING (max 20 pts)
    From insider-trades.json:
      Recent CEO/CFO buy (P code) within 90d → 20
      Any insider buy within 90d            → 12
      No insider selling within 30d         → 5
      Insider selling within 30d            → -5

  Component 4: 8-K MATERIAL POSITIVE (max 15 pts)
    From 8k-filings.json — recent positive items per ticker:
      Item 2.02 (results) within 7d → 8
      Item 7.01 (Reg FD) within 7d  → 6
      Material acquisition / contract → 10
    Cap at 15.

  Component 5: REVENUE ACCELERATION (max 10 pts)
    From revenue-acceleration.json:
      TIER_S_INFLECTION  → 10
      TIER_A_ACCELERATING → 7
      TIER_B_BUILDING    → 4

  Component 6: SMART MONEY (max 10 pts)
    From smart-money-clusters.json:
      score >= 80     → 10
      score 60-80     → 7
      legend buyers   → +3 bonus (capped)

  TOTAL: 0-100 Surprise Probability Score

OUTPUT
──────
  s3://justhodl-dashboard-live/data/earnings-whisper.json
  {
    as_of, n_upcoming,
    by_tier: {S: n, A: n, B: n, C: n},
    top_setups: [
      { ticker, name, earnings_date, time, days_to_earnings,
        whisper_score, tier (S/A/B/C),
        components: { eps_velocity, options_flow, insider, eight_k,
                      revenue_accel, smart_money },
        flags: [],
        rationale: "..."
      }, ...
    ]
  }

TIERS
─────
  TIER_S: score >= 70 — high conviction, likely beat + drift
  TIER_A: 50-70       — bullish bias, increased probability
  TIER_B: 30-50       — mild positive lean
  TIER_C: < 30        — neutral / flagged components

SCHEDULE
────────
  cron(15 8 * * ? *) — daily at 08:15 UTC (3:15 AM ET)
  Runs after earnings-tracker (which updates at various points)
  and after most overnight news/SEC processing.

ZERO DETERIORATION
  ✓ Pure consumer of existing 7 S3 feeds + earnings-tracker calendar
  ✓ No Lambda touched
  ✓ No FMP / external API calls — entirely S3-based
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/earnings-whisper.json")

S3 = boto3.client("s3", region_name=REGION)


def fetch_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[whisper] fetch_json({key}) failed: {e}")
        return default


# ─────────────────────────────────────────────────────────────────────────────
# COMPONENT SCORERS (each returns 0-max points)
# ─────────────────────────────────────────────────────────────────────────────
def score_eps_velocity(ticker, eps_idx):
    """Component 1: EPS Revision Velocity (max 25 pts)."""
    rec = eps_idx.get(ticker)
    if not rec:
        return 0, "no eps revision data"
    flag = (rec.get("flag") or "").upper()
    score = rec.get("score") or 0
    if "TIER_S" in flag or score >= 90:
        return 25, f"TIER_S EPS revision velocity (raw={score:.0f})"
    if "TIER_A" in flag or score >= 70:
        return 18, f"TIER_A EPS revision velocity (raw={score:.0f})"
    if "TIER_B" in flag or score >= 50:
        return 10, f"TIER_B EPS revision velocity (raw={score:.0f})"
    return max(0, int(score / 10)), f"weak eps revision (raw={score:.0f})"


def score_options_flow(ticker, options_idx):
    """Component 2: Options Flow Bullishness (max 20 pts)."""
    rec = options_idx.get(ticker)
    if not rec:
        return 0, "no options flow data"
    score = rec.get("score") or 0
    tier = rec.get("tier") or ""
    flags = rec.get("flags") or []

    base = 0
    if "BULLISH" in tier and "TIER_A" in tier:
        base = 20
    elif "BULLISH" in tier:
        base = 12
    elif score >= 80:
        base = 18
    elif score >= 60:
        base = 10

    # Bonus for surge flags
    bonus = 0
    if "CPR_SURGING" in flags:
        bonus += 3
    if "CALL_VOL_3X" in flags:
        bonus += 2
    if "ABS_CPR_3X" in flags:
        bonus += 2
    total = min(20, base + bonus)
    if total > 0:
        return total, f"options flow {tier} (raw={score:.0f}) {flags[:3]}"
    return 0, "weak options flow"


def score_insider(ticker, transactions_by_ticker, days_to_earnings):
    """Component 3: Insider positioning (max 20 pts).

    Looks at insider transactions within the last 90 days.
    Recent buys are bullish; recent sells are bearish.
    """
    txns = transactions_by_ticker.get(ticker, [])
    if not txns:
        return 5, "no recent insider activity (neutral)"

    # Look at last 90 days (90d back from today)
    today = datetime.now(timezone.utc).date()
    points = 0
    notes = []
    has_buy = False
    has_sell = False
    has_ceo_cfo_buy = False

    for t in txns:
        try:
            tx_date = datetime.fromisoformat((t.get("txn_date") or "").replace("Z", "+00:00")).date()
        except Exception:
            continue
        days_ago = (today - tx_date).days
        if days_ago > 90 or days_ago < 0:
            continue

        side = (t.get("side") or "").upper()
        code = (t.get("code") or "").upper()
        role = (t.get("role") or "").upper()

        if code == "P" or side == "BUY":
            has_buy = True
            if "CEO" in role or "CFO" in role or "CHIEF" in role:
                has_ceo_cfo_buy = True
        elif code == "S" or side == "SELL":
            has_sell = True

    if has_ceo_cfo_buy:
        points = 20
        notes.append("CEO/CFO buying recently")
    elif has_buy and not has_sell:
        points = 14
        notes.append("insider buying, no selling")
    elif has_buy:
        points = 8
        notes.append("mixed insider activity (buying + selling)")
    elif has_sell:
        points = -5
        notes.append("only insider selling — bearish")
    else:
        points = 5
        notes.append("no recent insider activity")

    return max(0, points), "; ".join(notes)


def score_eight_k(ticker, filings_by_ticker, days_to_earnings):
    """Component 4: 8-K material positive (max 15 pts)."""
    fl = filings_by_ticker.get(ticker, [])
    if not fl:
        return 0, "no recent 8-K"
    today = datetime.now(timezone.utc).date()
    points = 0
    notes = []
    for f in fl:
        try:
            f_date = datetime.fromisoformat((f.get("filed_at") or "").replace("Z", "+00:00")).date()
        except Exception:
            continue
        days_ago = (today - f_date).days
        if days_ago > 14 or days_ago < 0:
            continue
        items = f.get("items") or []
        for item in items:
            item_str = str(item).strip()
            if "2.02" in item_str:
                points += 8
                notes.append("results filed (2.02)")
            elif "7.01" in item_str:
                points += 6
                notes.append("Reg FD disclosure (7.01)")
            elif item_str in ("1.01", "2.01", "8.01"):
                points += 5
                notes.append(f"material event ({item_str})")
    return min(15, points), "; ".join(notes) if notes else "no relevant 8-K items"


def score_revenue_accel(ticker, rev_idx):
    """Component 5: Revenue acceleration (max 10 pts)."""
    rec = rev_idx.get(ticker)
    if not rec:
        return 0, ""
    tier = (rec.get("tier") or "").upper()
    if "TIER_S" in tier or "INFLECTION" in tier:
        return 10, f"revenue inflection ({tier})"
    if "TIER_A" in tier or "ACCELERATING" in tier:
        return 7, f"revenue accelerating ({tier})"
    if "TIER_B" in tier or "BUILDING" in tier:
        return 4, f"revenue building ({tier})"
    return 0, ""


def score_smart_money(ticker, smart_idx):
    """Component 6: Smart money 13F (max 10 pts)."""
    rec = smart_idx.get(ticker)
    if not rec:
        return 0, ""
    score = rec.get("score") or 0
    legends = rec.get("legend_buyers") or []
    base = 10 if score >= 80 else (7 if score >= 60 else 0)
    bonus = 3 if legends else 0
    total = min(10, base + bonus)
    notes = []
    if score:
        notes.append(f"smart money score={score:.0f}")
    if legends:
        notes.append(f"legends: {', '.join(legends[:2])}")
    return total, "; ".join(notes)


# ─────────────────────────────────────────────────────────────────────────────
# INDEX BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
def build_eps_idx(eps_data):
    if not eps_data:
        return {}
    out = {}
    for r in (eps_data.get("all_qualifying") or []):
        sym = r.get("symbol")
        if sym:
            out[sym] = r
    return out


def build_options_idx(opt_data):
    if not opt_data:
        return {}
    out = {}
    for r in (opt_data.get("all_qualifying") or []):
        sym = r.get("symbol")
        if sym:
            out[sym] = r
    return out


def build_transactions_idx(insider_data):
    if not insider_data:
        return {}
    out = {}
    for t in (insider_data.get("transactions") or []):
        sym = t.get("ticker")
        if sym and sym != "N/A":
            out.setdefault(sym, []).append(t)
    return out


def build_filings_idx(filings_data):
    if not filings_data:
        return {}
    out = {}
    for f in (filings_data.get("filings") or []):
        sym = f.get("ticker")
        if sym and sym != "N/A":
            out.setdefault(sym, []).append(f)
    return out


def build_revenue_idx(rev_data):
    if not rev_data:
        return {}
    out = {}
    for r in (rev_data.get("all_qualifying") or rev_data.get("top_setups") or []):
        sym = r.get("symbol") or r.get("ticker")
        if sym:
            out[sym] = r
    return out


def build_smart_idx(smart_data):
    if not smart_data:
        return {}
    out = {}
    for r in (smart_data.get("clusters") or []):
        sym = r.get("ticker")
        if sym:
            out[sym] = r
    return out


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def tier_for_score(score):
    if score >= 70: return "S"
    if score >= 50: return "A"
    if score >= 30: return "B"
    return "C"


def lambda_handler(event, context):
    started = time.time()

    # Load all 7 inputs
    print("[whisper] Loading data…")
    earnings_tracker = fetch_json("data/earnings-tracker.json") or {}
    eps_data         = fetch_json("data/eps-revision-velocity.json")
    options_data     = fetch_json("data/options-flow.json")
    insider_data     = fetch_json("data/insider-trades.json")
    filings_data     = fetch_json("data/8k-filings.json")
    revenue_data     = fetch_json("data/revenue-acceleration.json")
    smart_data       = fetch_json("data/smart-money-clusters.json")

    # Build indices
    eps_idx     = build_eps_idx(eps_data)
    options_idx = build_options_idx(options_data)
    txn_idx     = build_transactions_idx(insider_data)
    filings_idx = build_filings_idx(filings_data)
    rev_idx     = build_revenue_idx(revenue_data)
    smart_idx   = build_smart_idx(smart_data)

    feed_health = {
        "eps_velocity": bool(eps_idx),
        "options_flow": bool(options_idx),
        "insider_transactions": bool(txn_idx),
        "eight_k_filings": bool(filings_idx),
        "revenue_acceleration": bool(rev_idx),
        "smart_money": bool(smart_idx),
    }
    print(f"[whisper] Feed health: {feed_health}")

    # Score each upcoming earnings name
    upcoming = earnings_tracker.get("upcoming_14d") or []
    today = datetime.now(timezone.utc).date()
    setups = []
    for u in upcoming:
        ticker = u.get("ticker")
        if not ticker:
            continue
        try:
            ed = datetime.fromisoformat((u.get("earnings_date") or "").replace("Z","+00:00")).date()
            days_to = (ed - today).days
        except Exception:
            days_to = None

        # Score each component
        c1, n1 = score_eps_velocity(ticker, eps_idx)
        c2, n2 = score_options_flow(ticker, options_idx)
        c3, n3 = score_insider(ticker, txn_idx, days_to)
        c4, n4 = score_eight_k(ticker, filings_idx, days_to)
        c5, n5 = score_revenue_accel(ticker, rev_idx)
        c6, n6 = score_smart_money(ticker, smart_idx)

        total = c1 + c2 + c3 + c4 + c5 + c6
        tier = tier_for_score(total)

        # Aggregate flags for quick scanning
        flags = []
        if c1 >= 18: flags.append("EPS_REVISION_STRONG")
        if c2 >= 15: flags.append("UNUSUAL_CALL_FLOW")
        if c3 >= 15: flags.append("INSIDER_BUYING")
        if c4 >= 10: flags.append("MATERIAL_8K")
        if c5 >= 7:  flags.append("REVENUE_ACCEL")
        if c6 >= 7:  flags.append("SMART_MONEY_IN")
        if c3 < 0:   flags.append("INSIDER_SELLING")

        rationale_parts = [p for p in [n1, n2, n3, n4, n5, n6] if p]

        setups.append({
            "ticker": ticker,
            "name": u.get("name"),
            "earnings_date": u.get("earnings_date"),
            "time": u.get("time"),
            "days_to_earnings": days_to,
            "eps_consensus": u.get("eps_consensus"),
            "n_estimates": u.get("n_estimates"),
            "market_cap": u.get("market_cap"),
            "whisper_score": total,
            "tier": tier,
            "components": {
                "eps_velocity":  c1,
                "options_flow":  c2,
                "insider":       c3,
                "eight_k":       c4,
                "revenue_accel": c5,
                "smart_money":   c6,
            },
            "flags": flags,
            "rationale": "; ".join(rationale_parts)[:300],
        })

    # Sort by whisper_score descending
    setups.sort(key=lambda s: s["whisper_score"], reverse=True)

    # Tier counts
    tier_counts = {"S": 0, "A": 0, "B": 0, "C": 0}
    for s in setups:
        tier_counts[s["tier"]] += 1

    payload = {
        "schema_version": "1.0",
        "method": "earnings_whisper_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_upcoming": len(setups),
        "tier_counts": tier_counts,
        "feed_health": feed_health,
        "top_setups": setups[:30],   # top 30
        "all_setups": setups,         # full list for completeness
        "duration_s": round(time.time() - started, 2),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=300",
    )

    print(f"[whisper] DONE in {payload['duration_s']}s · "
          f"{len(setups)} names · S={tier_counts['S']}, "
          f"A={tier_counts['A']}, B={tier_counts['B']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_upcoming": len(setups),
            "tier_counts": tier_counts,
            "duration_s": payload["duration_s"],
        }),
    }
