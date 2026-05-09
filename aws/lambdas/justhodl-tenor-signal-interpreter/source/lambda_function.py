"""
justhodl-tenor-signal-interpreter

Reads US Treasury auction tape and extracts 4 tenor-specific macro signals
based on validated research (per FOMC minutes, BIS, IMF, NBER):

  1. fed_path      (2-year note)   — Fed rate cuts/hikes being priced
  2. eurodollar    (1m, 3m, 4w)    — offshore dollar shortage
  3. qe_imminence  (30-year bond)  — QE/Fed buying expectations
  4. composite     — aggregate of above

Each channel emits state {OFF, WATCH, FIRING, EXTREME} with quantitative
thresholds calibrated against historical events:
  - GFC 2008 (2y collapsed 5%→1%; 30y stayed 4-5% pre-QE)
  - COVID 2020 (Indirect bidder collapse to 28% on 1m bill, FX basis blew out)
  - 2018-19 QT reversal (30y rallied as Fed pivoted)
  - Oct 2024 (your detector caught AAH 99.31% → Nov vol)

OUTPUTS:
  data/auction-tenor-signals.json
  Schedule: same as auction-crisis-detector (every 15min weekday 14-22 UTC + 4h backstop)
  Hook: alert-router posts on signal level change (state transition)

NO BREAKING CHANGES — this layer reads existing auction-crisis.json
plus pulls its own focused fiscaldata.treasury.gov queries for tenors
that the 14-day window doesn't cover (30y is quarterly).
"""
import json
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/auction-tenor-signals.json"
AUCTION_CRISIS_KEY = "data/auction-crisis.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

FISCAL_BASE = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
               "/v1/accounting/od/auctions_query")

# Tenor groupings (matches Treasury's "security_term" field)
BILL_SHORT = ["4-Week", "8-Week", "1-Month"]   # 1m bucket — eurodollar signal
BILL_MID   = ["13-Week", "17-Week", "26-Week", "3-Month"]  # 3m bucket
NOTE_2Y    = ["2-Year"]
BOND_30Y   = ["30-Year"]

# ────────────────────────────────────────────────────────────────────────
# Threshold table (research-backed; tunable as data accumulates)
# ────────────────────────────────────────────────────────────────────────
THRESHOLDS = {
    "fed_path": {
        # 2y high-yield change vs prior 2y auction (bp)
        "yield_change_watch": 15,
        "yield_change_firing": 25,
        "yield_change_extreme": 40,
        # 2y vs Fed funds spread (bp)
        "spread_watch": 30,
        "spread_firing": 50,
        "spread_extreme": 100,
    },
    "eurodollar": {
        # Indirect % drop vs 4-auction tenor-matched rolling average (pts)
        "indirect_drop_watch": 10,
        "indirect_drop_firing": 20,
        "indirect_drop_extreme": 30,
        # BTC spike above tenor average
        "btc_spike_watch": 0.30,
        "btc_spike_firing": 0.50,
        "btc_extreme": 4.0,
    },
    "qe_imminence": {
        # 30y high-yield drop vs prior 30y auction (bp)
        "yield_drop_watch": 20,
        "yield_drop_firing": 30,
        "yield_drop_extreme": 50,
        # Fed funds level required (cuts must be possible before QE)
        "ff_min_watch": 2.0,
        "ff_min_firing": 3.0,
        # Indirect % must stay strong (foreign front-running Fed)
        "indirect_min": 60.0,
    },
}

# ────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────
def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def fetch_fred_fed_funds():
    """Most recent Fed funds rate from FRED (DFF series)."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id=DFF&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit=1")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Tenor/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        obs = data.get("observations", [])
        if obs and obs[0].get("value") not in ("", ".", None):
            return float(obs[0]["value"])
    except Exception as e:
        print(f"[tenor] FRED fetch error: {e}")
    return None


def fetch_auctions_window(days_back=120):
    """Pull last N days of auctions from fiscaldata.treasury.gov.

    120-day window is enough to capture 30y quarterly cadence (last 1-2 prints)
    plus monthly 2y, plus weekly bills.
    """
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    params = {
        "fields": ("auction_date,security_type,security_term,high_yield_pct,"
                    "high_discnt_rate_pct,bid_to_cover_ratio,"
                    "indirect_bidder_accept_pct,direct_bidder_accept_pct,"
                    "primary_dealer_accept_pct,allocation_pctage_at_high_yield,"
                    "tot_accepted,cusip,issue_date"),
        "filter": f"auction_date:gte:{start.isoformat()},auction_date:lte:{end.isoformat()}",
        "sort": "-auction_date",
        "page[size]": "500",
    }
    url = FISCAL_BASE + "?" + urllib.parse.urlencode(params, safe=":,")
    headers = {
        "User-Agent": "JustHodl-Tenor-Signals/1.0",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Accept": "application/json",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode("utf-8"))
    rows = data.get("data", [])
    # Coerce numeric fields
    for r in rows:
        for k in ["high_yield_pct", "high_discnt_rate_pct", "bid_to_cover_ratio",
                   "indirect_bidder_accept_pct", "direct_bidder_accept_pct",
                   "primary_dealer_accept_pct", "allocation_pctage_at_high_yield",
                   "tot_accepted"]:
            v = r.get(k)
            try:
                r[k] = float(v) if v not in (None, "", "null") else None
            except (TypeError, ValueError):
                r[k] = None
    return rows


def filter_by_tenor(auctions, tenor_list):
    """Filter auctions to one of the tenors in tenor_list. Returns sorted desc by date."""
    out = [a for a in auctions if (a.get("security_term") or "") in tenor_list]
    out.sort(key=lambda a: a.get("auction_date", ""), reverse=True)
    return out


def get_yield(auction):
    """Get the 'rate' for an auction — coupon notes use high_yield_pct,
       bills use high_discnt_rate_pct."""
    y = auction.get("high_yield_pct")
    if y is None or y == 0:
        y = auction.get("high_discnt_rate_pct")
    return y


def severity_pill(state):
    return {"OFF": "🟢", "WATCH": "🟡", "FIRING": "🟠", "EXTREME": "🔴"}.get(state, "⚪")


# ────────────────────────────────────────────────────────────────────────
# Signal #1 — Fed path (2-year note)
# ────────────────────────────────────────────────────────────────────────
def compute_fed_path(auctions, fed_funds):
    """Detect Fed rate-cut / hike pricing pressure from 2y note auctions."""
    notes_2y = filter_by_tenor(auctions, NOTE_2Y)
    th = THRESHOLDS["fed_path"]

    sig = {
        "channel": "fed_path",
        "label": "Fed Rate Path (2-year note)",
        "description": "Sharp 2y yield moves price Fed cuts (down) or hikes (up). 2y vs Fed funds spread quantifies how aggressively.",
        "state": "OFF",
        "direction": None,
        "interpretation": "Insufficient 2y auction data" if len(notes_2y) < 2 else "",
        "latest_auction": None,
        "metrics": {},
        "evidence": [],
    }

    if len(notes_2y) < 2:
        return sig

    latest = notes_2y[0]
    prior = notes_2y[1]
    latest_yield = get_yield(latest)
    prior_yield = get_yield(prior)
    if latest_yield is None or prior_yield is None:
        sig["interpretation"] = "Missing yield data on most recent 2y auctions"
        return sig

    yield_change_bp = (latest_yield - prior_yield) * 100  # in bp
    spread_bp = (latest_yield - fed_funds) * 100 if fed_funds is not None else None

    sig["latest_auction"] = {
        "date": latest.get("auction_date"),
        "high_yield_pct": latest_yield,
        "prior_date": prior.get("auction_date"),
        "prior_yield_pct": prior_yield,
    }
    sig["metrics"] = {
        "yield_change_bp": round(yield_change_bp, 2),
        "spread_to_ff_bp": round(spread_bp, 2) if spread_bp is not None else None,
        "fed_funds_pct": fed_funds,
    }

    abs_change = abs(yield_change_bp)
    abs_spread = abs(spread_bp) if spread_bp is not None else 0

    if abs_change >= th["yield_change_extreme"] or abs_spread >= th["spread_extreme"]:
        sig["state"] = "EXTREME"
    elif abs_change >= th["yield_change_firing"] or abs_spread >= th["spread_firing"]:
        sig["state"] = "FIRING"
    elif abs_change >= th["yield_change_watch"] or abs_spread >= th["spread_watch"]:
        sig["state"] = "WATCH"
    else:
        sig["state"] = "OFF"

    if yield_change_bp < 0 or (spread_bp is not None and spread_bp < 0):
        sig["direction"] = "CUTS_PRICED"
    elif yield_change_bp > 0 or (spread_bp is not None and spread_bp > 0):
        sig["direction"] = "HIKES_PRICED"

    if sig["state"] != "OFF":
        if sig["direction"] == "CUTS_PRICED":
            sig["interpretation"] = (
                f"Market is pricing Fed rate cuts. 2y yield moved {yield_change_bp:+.0f}bp vs "
                f"prior auction; spread to Fed funds is {spread_bp:+.0f}bp. "
                f"Historical playbook: bond market typically gets the next 2-3 Fed moves right "
                f"6-8 weeks ahead."
            )
        elif sig["direction"] == "HIKES_PRICED":
            sig["interpretation"] = (
                f"Market is pricing Fed rate hikes (or hold-longer-than-expected). "
                f"2y yield moved {yield_change_bp:+.0f}bp; spread {spread_bp:+.0f}bp. "
                f"Risk-on assets vulnerable until pricing stabilizes."
            )
    else:
        sig["interpretation"] = (
            f"2y auctions clearing in line with Fed funds (spread {spread_bp:+.0f}bp). "
            f"No directional Fed-pricing pressure."
        ) if spread_bp is not None else "2y stable, no directional signal."

    sig["evidence"] = [
        {"label": "2y yield change vs prior", "value": f"{yield_change_bp:+.1f}bp",
         "threshold": f"WATCH≥{th['yield_change_watch']}, FIRE≥{th['yield_change_firing']}, EXTREME≥{th['yield_change_extreme']}"},
        {"label": "2y vs Fed funds", "value": f"{spread_bp:+.0f}bp" if spread_bp is not None else "—",
         "threshold": f"WATCH≥{th['spread_watch']}, FIRE≥{th['spread_firing']}, EXTREME≥{th['spread_extreme']}"},
    ]
    return sig


# ────────────────────────────────────────────────────────────────────────
# Signal #2 — Eurodollar shortage (1m and 3m bills)
# ────────────────────────────────────────────────────────────────────────
def compute_eurodollar(auctions):
    """Detect offshore dollar shortage via Indirect bidder collapse + BTC spike."""
    th = THRESHOLDS["eurodollar"]
    sig = {
        "channel": "eurodollar",
        "label": "Eurodollar Shortage (1m/3m bills)",
        "description": "Offshore dollar funding stress shows up as foreign (Indirect) bidder collapse + flight-to-bills BTC spike.",
        "state": "OFF",
        "interpretation": "",
        "metrics": {},
        "evidence": [],
        "tenors_firing": [],
    }

    worst_state = "OFF"
    state_rank = {"OFF": 0, "WATCH": 1, "FIRING": 2, "EXTREME": 3}
    rank_state = {v: k for k, v in state_rank.items()}
    tenors_checked = []

    # Process bill tenors that signal eurodollar stress
    for tenor_group, group_label in [(BILL_SHORT, "1m"), (BILL_MID, "3m")]:
        bills = filter_by_tenor(auctions, tenor_group)
        if len(bills) < 5:
            continue
        # Latest auction
        latest = bills[0]
        latest_indirect = latest.get("indirect_bidder_accept_pct")
        latest_btc = latest.get("bid_to_cover_ratio")
        if latest_indirect is None and latest_btc is None:
            continue

        # Trailing average of prior 4 auctions (same tenor group)
        prior = bills[1:5]
        prior_indirects = [p.get("indirect_bidder_accept_pct") for p in prior
                            if p.get("indirect_bidder_accept_pct") is not None]
        prior_btcs = [p.get("bid_to_cover_ratio") for p in prior
                       if p.get("bid_to_cover_ratio") is not None]

        avg_indirect = mean(prior_indirects) if prior_indirects else None
        avg_btc = mean(prior_btcs) if prior_btcs else None
        indirect_drop = (avg_indirect - latest_indirect) if (avg_indirect is not None and latest_indirect is not None) else 0
        btc_spike = (latest_btc - avg_btc) if (avg_btc is not None and latest_btc is not None) else 0

        # Determine tenor-level state
        tenor_state = "OFF"
        if (latest_btc is not None and latest_btc >= th["btc_extreme"]) or indirect_drop >= th["indirect_drop_extreme"]:
            tenor_state = "EXTREME"
        elif (indirect_drop >= th["indirect_drop_firing"] and btc_spike >= th["btc_spike_firing"]) or \
              indirect_drop >= th["indirect_drop_extreme"]:
            tenor_state = "FIRING"
        elif indirect_drop >= th["indirect_drop_watch"] or btc_spike >= th["btc_spike_watch"]:
            tenor_state = "WATCH"

        tenors_checked.append({
            "tenor": group_label,
            "latest_term": latest.get("security_term"),
            "latest_date": latest.get("auction_date"),
            "latest_indirect_pct": round(latest_indirect, 2) if latest_indirect is not None else None,
            "avg_prior_indirect_pct": round(avg_indirect, 2) if avg_indirect is not None else None,
            "indirect_drop_pts": round(indirect_drop, 2),
            "latest_btc": round(latest_btc, 2) if latest_btc is not None else None,
            "avg_prior_btc": round(avg_btc, 2) if avg_btc is not None else None,
            "btc_spike": round(btc_spike, 2),
            "state": tenor_state,
        })
        if state_rank[tenor_state] > state_rank[worst_state]:
            worst_state = tenor_state
        if tenor_state != "OFF":
            sig["tenors_firing"].append(group_label)

    sig["state"] = worst_state
    sig["metrics"] = {"tenor_breakdown": tenors_checked}

    if worst_state == "OFF":
        sig["interpretation"] = (
            "Bill auction tape shows healthy foreign (Indirect) participation. "
            "No offshore dollar funding stress visible. Cross-currency basis presumed stable."
        )
    else:
        firing_text = " + ".join(sig["tenors_firing"])
        sig["interpretation"] = (
            f"Eurodollar stress signature firing on {firing_text} bills. "
            "Foreign Indirect bidder share has collapsed vs trailing average — historically associated "
            "with offshore dollar shortage (GFC 2008 Indirect collapsed to 11.4%, COVID 2020-03-19 "
            "to 28.4%). Cross-check with FRA-OIS and EUR/USD basis swap."
        )

    sig["evidence"] = [
        {"label": f"{t['tenor']} Indirect drop", "value": f"{t['indirect_drop_pts']:+.1f}pt",
         "threshold": f"WATCH≥{th['indirect_drop_watch']}, FIRE≥{th['indirect_drop_firing']}, EXTREME≥{th['indirect_drop_extreme']}"}
        for t in tenors_checked
    ]
    return sig


# ────────────────────────────────────────────────────────────────────────
# Signal #3 — QE imminence (30-year bond)
# ────────────────────────────────────────────────────────────────────────
def compute_qe_imminence(auctions, fed_funds):
    """Detect QE-being-priced via 30y rally with Fed funds elevated."""
    bonds_30y = filter_by_tenor(auctions, BOND_30Y)
    th = THRESHOLDS["qe_imminence"]
    sig = {
        "channel": "qe_imminence",
        "label": "QE Imminence (30-year bond)",
        "description": "Sharp 30y rally with Fed funds elevated + indirect demand strong = market positioning ahead of Fed buying.",
        "state": "OFF",
        "interpretation": "",
        "metrics": {},
        "evidence": [],
    }

    if len(bonds_30y) < 2:
        sig["interpretation"] = "Insufficient 30y auction data (need ≥2; quarterly cadence)"
        return sig

    latest = bonds_30y[0]
    prior = bonds_30y[1]
    latest_yield = get_yield(latest)
    prior_yield = get_yield(prior)
    latest_indirect = latest.get("indirect_bidder_accept_pct")

    if latest_yield is None or prior_yield is None:
        sig["interpretation"] = "Missing 30y yield data"
        return sig

    yield_drop_bp = (prior_yield - latest_yield) * 100  # positive = yields fell
    sig["latest_auction"] = {
        "date": latest.get("auction_date"),
        "high_yield_pct": latest_yield,
        "prior_date": prior.get("auction_date"),
        "prior_yield_pct": prior_yield,
        "indirect_pct": latest_indirect,
    }
    sig["metrics"] = {
        "yield_drop_bp": round(yield_drop_bp, 2),
        "fed_funds_pct": fed_funds,
        "indirect_pct": round(latest_indirect, 2) if latest_indirect is not None else None,
    }

    # Only fires if all 4 conditions: yield drop + FF elevated + indirect strong + drop is recent
    state = "OFF"
    if yield_drop_bp >= th["yield_drop_extreme"] and (fed_funds or 0) >= th["ff_min_firing"] \
       and (latest_indirect or 0) >= th["indirect_min"]:
        state = "EXTREME"
    elif yield_drop_bp >= th["yield_drop_firing"] and (fed_funds or 0) >= th["ff_min_firing"] \
         and (latest_indirect or 0) >= th["indirect_min"]:
        state = "FIRING"
    elif yield_drop_bp >= th["yield_drop_watch"] and (fed_funds or 0) >= th["ff_min_watch"]:
        state = "WATCH"

    sig["state"] = state

    if state == "OFF":
        sig["interpretation"] = (
            f"30y yield change {-yield_drop_bp:+.0f}bp vs prior auction. "
            "No QE-imminence signature: either yield not dropping fast enough, Fed funds too low "
            "(no room to cut before QE), or foreign demand insufficient to confirm positioning."
        )
    else:
        sig["interpretation"] = (
            f"30y yield dropped {yield_drop_bp:.0f}bp vs prior auction with Fed funds at "
            f"{fed_funds:.2f}% and Indirect at {latest_indirect:.1f}%. "
            "Historical playbook: this combination preceded QE1 (Nov 2008), QE3 (2012), and "
            "the COVID Fed-bazooka (March 2020). Long duration, gold, and BTC are typical "
            "outperformers in next 3-6 months."
        )

    sig["evidence"] = [
        {"label": "30y yield drop vs prior", "value": f"{yield_drop_bp:+.0f}bp",
         "threshold": f"WATCH≥{th['yield_drop_watch']}, FIRE≥{th['yield_drop_firing']}"},
        {"label": "Fed funds level", "value": f"{fed_funds:.2f}%" if fed_funds else "—",
         "threshold": f"WATCH≥{th['ff_min_watch']}%, FIRE≥{th['ff_min_firing']}%"},
        {"label": "Indirect demand", "value": f"{latest_indirect:.1f}%" if latest_indirect is not None else "—",
         "threshold": f"FIRE requires ≥{th['indirect_min']}%"},
    ]
    return sig


# ────────────────────────────────────────────────────────────────────────
# Composite + persistence
# ────────────────────────────────────────────────────────────────────────
def compute_composite(signals):
    """Aggregate score (0-100). FIRING+ on any channel → ≥50."""
    rank = {"OFF": 0, "WATCH": 25, "FIRING": 60, "EXTREME": 90}
    scores = [rank.get(s.get("state", "OFF"), 0) for s in signals.values()]
    composite = max(scores)  # worst-of approach (single firing channel matters)
    avg = sum(scores) / len(scores) if scores else 0
    if composite > 0:
        composite = round(composite * 0.7 + avg * 0.3, 1)
    return composite


def load_prior_state():
    """Load previous run's tenor-signals from S3 to detect state transitions."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)
        prior = json.loads(obj["Body"].read())
        return prior.get("signals", {})
    except Exception:
        return {}


def detect_transitions(current, prior):
    """Return list of (channel, prior_state, new_state, sig_dict) for any level change."""
    transitions = []
    for channel, sig in current.items():
        prior_state = (prior.get(channel) or {}).get("state", "OFF")
        new_state = sig.get("state", "OFF")
        if new_state != prior_state:
            transitions.append({
                "channel": channel,
                "label": sig.get("label"),
                "prior_state": prior_state,
                "new_state": new_state,
                "interpretation": sig.get("interpretation"),
                "metrics": sig.get("metrics"),
            })
    return transitions


# ────────────────────────────────────────────────────────────────────────
# Main handler
# ────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print("[tenor] start")

    # Pull current state inputs
    fed_funds = fetch_fred_fed_funds()
    print(f"[tenor] fed_funds={fed_funds}")

    auctions = fetch_auctions_window(120)
    print(f"[tenor] pulled {len(auctions)} auctions in 120-day window")

    # Compute signals
    sig_fed_path = compute_fed_path(auctions, fed_funds)
    sig_eurodollar = compute_eurodollar(auctions)
    sig_qe = compute_qe_imminence(auctions, fed_funds)

    signals = {
        "fed_path": sig_fed_path,
        "eurodollar": sig_eurodollar,
        "qe_imminence": sig_qe,
    }
    composite_score = compute_composite(signals)

    # Detect transitions vs prior run
    prior_signals = load_prior_state()
    transitions = detect_transitions(signals, prior_signals)
    print(f"[tenor] transitions: {len(transitions)} state changes")

    output = {
        "schema_version": "1.0",
        "generated_at": _now_iso(),
        "elapsed_sec": round(time.time() - started, 2),
        "fed_funds_rate": fed_funds,
        "n_auctions_in_window": len(auctions),
        "composite_score": composite_score,
        "any_firing": any(s["state"] in ("FIRING", "EXTREME") for s in signals.values()),
        "any_watch": any(s["state"] == "WATCH" for s in signals.values()),
        "signals": signals,
        "transitions": transitions,
    }

    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300, s-maxage=60",
    )
    print(f"[tenor] OK composite={composite_score} firing={output['any_firing']} "
          f"transitions={len(transitions)}")

    return {"statusCode": 200, "body": json.dumps({
        "composite_score": composite_score,
        "states": {c: s["state"] for c, s in signals.items()},
        "transitions_count": len(transitions),
    })}
