"""justhodl-hedge-planner -- the firm Hedge Execution Planner.

The Tail Hedge Overlay (justhodl-tail-hedge) decides HOW MUCH convex
protection the book needs. It does not say what to trade. This engine
is the execution desk that sits under it: it turns the sleeve
recommendation into a concrete, worked order ticket and -- because it
keeps the standing hedge sleeve as state -- each run emits the
REBALANCE DELTA, not a gross position.

What a hedge-overlay execution desk actually does, and what this does:

  * Breaks the sleeve into executable LEGS -- a primary listed leg and
    a convex tail leg -- with side, structure (strikes as %-OTM and as
    live point levels), tenor, premium budget and a working style.
  * Tracks the STANDING sleeve in data/hedge-book.json. The ticket is
    target minus standing: open when flat, add / trim when the target
    moves, roll when the sleeve decays past its tenor, switch the
    instrument class when the binding scenario changes.
  * Respects the regime STANCE from the overlay -- ACCUMULATE builds
    toward target, HOLD carries, MONETIZE harvests the convex leg.
  * Runs PRE-TRADE checks -- spend cap, gap cover, carry budget, and a
    sanity check that the hedge is protecting a genuinely net-long book.

It places no orders. It produces the ticket a trader would work, and
paper-fills it into the standing sleeve so the next run sees the new
book. All sizing is in %-of-book; dollar and contract figures use a
clearly-labelled assumed paper-book notional purely to make the ticket
concrete. Stylised option-premium rules of thumb, not live chains.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone, date

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/hedge-planner.json"
STATE_KEY = "data/hedge-book.json"
HIST_KEY = "data/hedge-planner-history.json"
SCHEMA = "1.0"
USER_AGENT = "justhodl-hedge-planner/1.0"

# Assumed paper-book notional. The firm risk stack is normalised to
# %-of-book and carries no NAV; this constant exists ONLY so the ticket
# can show dollar premiums and illustrative contract counts. The
# %-of-book figures are the real output.
BOOK_NOTIONAL_USD = 1_000_000.0

ROLL_DAYS = 35           # a sleeve held longer than this is rolled
MAX_HEDGE_SPEND_PCT = 1.5   # hard cap on standing sleeve premium
MIN_TICKET_PCT = 0.03    # do not emit a trade smaller than this
STALE_HOURS = 30.0

ENGINES = {
    "tail-hedge": "data/tail-hedge.json",
    "firm-book": "data/firm-book.json",
    "firm-risk-board": "data/firm-risk-board.json",
}

# Per-scenario-class leg breakdown. The Tail Hedge Overlay owns "how
# much sleeve"; the planner owns "how the sleeve breaks into worked
# legs". primary_pct + convex_pct = 1.0. debit_pct is a stylised net
# option premium as a fraction of underlying spot -- a desk rule of
# thumb for an OTM listed structure of that tenor, NOT a live quote.
SLEEVE_LEGS = {
    "EQUITY_CRASH": {
        "primary": {"name": "SPY put spread", "underlying": "SPY",
                    "premium_share": 0.92, "tenor": "3M",
                    "structure": "buy ~7% OTM put / sell ~15% OTM put",
                    "buy_otm": 0.07, "sell_otm": 0.15, "debit_pct": 0.011,
                    "kind": "put_spread"},
        "convex": {"name": "VIX calls", "underlying": "VIX",
                   "premium_share": 0.08, "tenor": "2M",
                   "structure": "OTM VIX calls (~+40-60% on spot VIX)",
                   "kind": "vix_calls"},
    },
    "RATES_SHOCK": {
        "primary": {"name": "TLT puts", "underlying": "TLT",
                    "premium_share": 0.80, "tenor": "3M",
                    "structure": "buy ~6% OTM puts", "buy_otm": 0.06,
                    "debit_pct": 0.018, "kind": "put"},
        "convex": {"name": "2s10s curve steepener", "underlying": "RATES",
                   "premium_share": 0.20, "tenor": "OTC",
                   "structure": "payer-swaption-style steepener",
                   "kind": " otc"},
    },
    "MOMENTUM_UNWIND": {
        "primary": {"name": "MTUM puts", "underlying": "MTUM",
                    "premium_share": 0.75, "tenor": "2M",
                    "structure": "buy ~7% OTM puts", "buy_otm": 0.07,
                    "debit_pct": 0.016, "kind": "put"},
        "convex": {"name": "VIX calls + crowded-long trim",
                   "underlying": "VIX", "premium_share": 0.25,
                   "tenor": "1M",
                   "structure": "OTM VIX 1M calls; trim crowded longs",
                   "kind": "vix_calls"},
    },
    "CREDIT_EVENT": {
        "primary": {"name": "HYG puts", "underlying": "HYG",
                    "premium_share": 0.70, "tenor": "3M",
                    "structure": "buy ~5% OTM puts", "buy_otm": 0.05,
                    "debit_pct": 0.014, "kind": "put"},
        "convex": {"name": "IWM put spread", "underlying": "IWM",
                   "premium_share": 0.30, "tenor": "2M",
                   "structure": "buy ~6% OTM put / sell ~14% OTM put",
                   "buy_otm": 0.06, "sell_otm": 0.14, "debit_pct": 0.012,
                   "kind": "put_spread"},
    },
    "VOL_SPIKE": {
        "primary": {"name": "VIX calls", "underlying": "VIX",
                    "premium_share": 0.65, "tenor": "1-2M",
                    "structure": "OTM VIX 1-2M calls", "kind": "vix_calls"},
        "convex": {"name": "SPY put spread", "underlying": "SPY",
                   "premium_share": 0.35, "tenor": "2M",
                   "structure": "buy ~6% OTM put / sell ~14% OTM put",
                   "buy_otm": 0.06, "sell_otm": 0.14, "debit_pct": 0.010,
                   "kind": "put_spread"},
    },
}
QUOTABLE = ("SPY", "QQQ", "TLT", "HYG", "IWM", "MTUM")

ACTION_COLOR = {
    "OPEN": "green", "ADD": "green", "ROLL": "cyan", "SWITCH": "orange",
    "TRIM": "orange", "HARVEST": "cyan", "HOLD": "dim", "UNWIND": "orange",
    "NONE": "dim",
}

s3 = boto3.client("s3")


# --------------------------------------------------------------------------
def read_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read()), obj["LastModified"]
    except Exception:
        return None, None


def num(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def hours_since(lm, now):
    if lm is None:
        return None
    try:
        if lm.tzinfo is None:
            lm = lm.replace(tzinfo=timezone.utc)
        return round((now - lm).total_seconds() / 3600.0, 1)
    except Exception:
        return None


def fmp_quote(symbol, fmp_key):
    """Live spot for a listed ETF via FMP /stable/quote. ETFs only --
    VIX and OTC legs are not quotable and return None by design."""
    if not fmp_key or symbol not in QUOTABLE:
        return None
    try:
        url = ("https://financialmodelingprep.com/stable/quote?symbol="
               "%s&apikey=%s" % (symbol, fmp_key))
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            return num(data[0].get("price")) or None
    except Exception:
        pass
    return None


def days_between(iso_date, today):
    if not iso_date:
        return None
    try:
        d = date.fromisoformat(str(iso_date)[:10])
        return (today - d).days
    except Exception:
        return None


def build_leg(leg_type, spec, premium_pct, side, spot):
    """Turn a sleeve leg spec + a premium budget into a worked leg."""
    premium_pct = round(premium_pct, 4)
    premium_usd = round(premium_pct / 100.0 * BOOK_NOTIONAL_USD, 0)
    strikes = None
    est_contracts = None
    kind = spec.get("kind", "").strip()

    if spot is not None and spot > 0:
        if kind == "put_spread":
            buy_k = round(spot * (1.0 - spec["buy_otm"]), 1)
            sell_k = round(spot * (1.0 - spec["sell_otm"]), 1)
            strikes = ("buy %.1f put / sell %.1f put (spot %.2f)"
                       % (buy_k, sell_k, spot))
        elif kind == "put":
            buy_k = round(spot * (1.0 - spec["buy_otm"]), 1)
            strikes = "buy %.1f put (spot %.2f)" % (buy_k, spot)
        # contract estimate: premium / (net debit per contract).
        debit = spec.get("debit_pct")
        if debit and kind in ("put_spread", "put"):
            per_contract = spot * 100.0 * debit
            if per_contract > 0:
                est_contracts = int(round(premium_usd / per_contract))

    if kind == "vix_calls":
        working = ("Work the VIX call chain; size to premium budget, not "
                   "a strike count -- VIX is not quotable here.")
    elif kind == "otc":
        working = ("OTC rates structure -- size with the rates desk to the "
                   "premium budget.")
    elif leg_type == "convex":
        working = ("Convex leg -- thin and gappy; work patiently with "
                   "limits, do not pay up.")
    else:
        working = ("Primary listed leg -- liquid; can be worked more "
                   "aggressively into the close.")

    return {
        "leg": leg_type,
        "instrument": spec["name"],
        "underlying": spec["underlying"],
        "side": side,
        "structure": spec["structure"],
        "tenor": spec["tenor"],
        "strike_levels": strikes,
        "est_contracts": est_contracts,
        "premium_pct_of_book": premium_pct,
        "premium_usd": premium_usd,
        "working_style": working,
    }


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    today = now.date()
    fmp_key = (event or {}).get("FMP_KEY") or __import__("os").environ.get(
        "FMP_KEY", "")

    feeds, ages = {}, {}
    for name, key in ENGINES.items():
        body, lm = read_json(key)
        feeds[name] = body if isinstance(body, dict) else {}
        ages[name] = hours_since(lm, now)

    th = feeds["tail-hedge"]
    fb = feeds["firm-book"]
    board = feeds["firm-risk-board"]

    th_fresh = bool(th) and ages["tail-hedge"] is not None \
        and ages["tail-hedge"] <= STALE_HOURS

    # ---- target, straight from the Tail Hedge Overlay --------------------
    req_block = th.get("hedge_requirement", {}) if isinstance(th, dict) else {}
    sleeve = th.get("hedge_sleeve", {}) if isinstance(th, dict) else {}
    regime = th.get("regime", {}) if isinstance(th, dict) else {}
    tail_exp = th.get("tail_exposure", {}) if isinstance(th, dict) else {}

    hedge_required = bool(req_block.get("required"))
    target_class = sleeve.get("scenario_class")
    target_budget = num(sleeve.get("hedge_budget_pct_of_book")) or 0.0
    target_payoff = num(sleeve.get("expected_payoff_in_worst_scenario_pct"))
    payoff_mult = num(sleeve.get("payoff_multiple"))
    target_carry = num(sleeve.get("annualised_carry_pct")) or 0.0
    stance = (regime.get("stance") or "HOLD").upper()
    required_protection = num(req_block.get("required_protection_pct_of_book"))
    gap_pp = num(req_block.get("gap_pp"))
    worst_scenario = tail_exp.get("worst_scenario")
    worst_loss = num(tail_exp.get("worst_loss_pct"))

    net_exp = num(tail_exp.get("net_exposure_pct"))
    gross_exp = num(tail_exp.get("gross_exposure_pct"))
    if net_exp is None and isinstance(fb.get("firm"), dict):
        net_exp = num(fb["firm"].get("net_exposure_pct"))
        gross_exp = num(fb["firm"].get("gross_exposure_pct"))

    # ---- standing sleeve state -------------------------------------------
    state, _ = read_json(STATE_KEY)
    if not isinstance(state, dict):
        state = {}
    standing_class = state.get("scenario_class")
    standing_budget = num(state.get("target_budget_pct")) or 0.0
    standing_legs = state.get("legs") or []
    placed_date = state.get("placed_date")
    has_standing = standing_budget > 0 and bool(standing_legs)
    age_days = days_between(placed_date, today)

    # ---- decide the action ----------------------------------------------
    if not th_fresh:
        action = "NONE"
        reason = ("The Tail Hedge Overlay feed is missing or stale -- no "
                  "ticket can be sized until it refreshes.")
    elif not hedge_required:
        if has_standing:
            action = "UNWIND"
            reason = ("The overlay no longer requires a hedge -- the worst "
                      "modelled scenario is back inside the soft loss "
                      "limit. Close the standing sleeve.")
        else:
            action = "NONE"
            reason = ("No hedge required and no standing sleeve -- the book "
                      "is flat of protection and needs none.")
    elif not has_standing:
        action = "OPEN"
        reason = ("No protection is on and the overlay requires a hedge -- "
                  "open the full target sleeve.")
    elif target_class and standing_class and target_class != standing_class:
        action = "SWITCH"
        reason = ("The binding scenario has changed from %s to %s -- close "
                  "the old sleeve and open the new instrument class."
                  % (standing_class, target_class))
    elif stance == "MONETIZE":
        action = "HARVEST"
        reason = ("Regime stance is MONETIZE -- stress is realising. "
                  "Harvest the convex leg into the move and keep the core.")
    elif target_budget - standing_budget > MIN_TICKET_PCT:
        action = "ADD"
        reason = ("The target sleeve has grown past the standing one -- add "
                  "the difference.")
    elif standing_budget - target_budget > MIN_TICKET_PCT:
        action = "TRIM"
        reason = ("The target sleeve has shrunk below the standing one -- "
                  "trim the difference back.")
    elif age_days is not None and age_days > ROLL_DAYS:
        action = "ROLL"
        reason = ("The standing sleeve has been on %d days and is decaying "
                  "toward its tenor -- roll it forward." % age_days)
    else:
        action = "HOLD"
        reason = ("The standing sleeve already matches the target and is "
                  "within tenor -- carry it, no trade.")

    # ---- size the ticket -------------------------------------------------
    # quote only the underlyings the chosen sleeve actually needs.
    spots, n_quote_ok, n_quote_try = {}, 0, 0
    use_class = target_class if action != "UNWIND" else standing_class
    if use_class in SLEEVE_LEGS:
        for lt in ("primary", "convex"):
            u = SLEEVE_LEGS[use_class][lt]["underlying"]
            if u in QUOTABLE and u not in spots:
                n_quote_try += 1
                q = fmp_quote(u, fmp_key)
                spots[u] = q
                if q is not None:
                    n_quote_ok += 1

    legs = []
    ticket_premium = 0.0
    side_summary = "NO TRADE"
    closing_note = None

    def legs_for(cls, budget, side):
        out = []
        spec = SLEEVE_LEGS.get(cls)
        if not spec or budget <= 0:
            return out
        for lt in ("primary", "convex"):
            ls = spec[lt]
            prem = budget * ls["premium_share"]
            out.append(build_leg(lt, ls, prem, side,
                                 spots.get(ls["underlying"])))
        return out

    if action in ("OPEN", "ROLL"):
        legs = legs_for(target_class, target_budget, "BUY")
        ticket_premium = target_budget
        side_summary = "BUY" if action == "OPEN" else "ROLL (close + reopen)"
    elif action == "SWITCH":
        legs = legs_for(target_class, target_budget, "BUY")
        ticket_premium = target_budget
        side_summary = "SWITCH"
        closing_note = ("Close the entire standing %s sleeve (%.2f%% of "
                        "book) before establishing the new sleeve below."
                        % (standing_class, standing_budget))
    elif action == "ADD":
        delta = round(target_budget - standing_budget, 4)
        legs = legs_for(target_class, delta, "BUY")
        ticket_premium = delta
        side_summary = "BUY (add to standing)"
    elif action == "TRIM":
        delta = round(standing_budget - target_budget, 4)
        legs = legs_for(standing_class or target_class, delta, "SELL")
        ticket_premium = -delta
        side_summary = "SELL (trim standing)"
    elif action == "HARVEST":
        # sell the convex leg, keep the core.
        spec = SLEEVE_LEGS.get(standing_class or target_class)
        if spec:
            cv = spec["convex"]
            cv_prem = standing_budget * cv["premium_share"]
            legs = [build_leg("convex", cv, cv_prem, "SELL",
                              spots.get(cv["underlying"]))]
            ticket_premium = -cv_prem
        side_summary = "SELL (harvest convex leg)"
    elif action == "UNWIND":
        legs = legs_for(standing_class, standing_budget, "SELL")
        ticket_premium = -standing_budget
        side_summary = "SELL (close sleeve)"
    # HOLD / NONE leave legs empty.

    ticket_premium = round(ticket_premium, 4)
    ticket_premium_usd = round(abs(ticket_premium) / 100.0
                               * BOOK_NOTIONAL_USD, 0)

    # ---- standing sleeve AFTER the ticket (paper fill) -------------------
    new_state = dict(state)
    if action in ("OPEN", "ROLL", "SWITCH"):
        new_state = {"scenario_class": target_class,
                     "target_budget_pct": round(target_budget, 4),
                     "legs": [{"leg": l["leg"], "instrument": l["instrument"],
                               "premium_pct_of_book": l["premium_pct_of_book"]}
                              for l in legs],
                     "placed_date": today.isoformat(),
                     "last_action": action}
    elif action in ("ADD", "TRIM"):
        new_state = {"scenario_class": target_class,
                     "target_budget_pct": round(target_budget, 4),
                     "legs": [{"leg": lt,
                               "instrument": SLEEVE_LEGS[target_class][lt]
                               ["name"],
                               "premium_pct_of_book": round(
                                   target_budget
                                   * SLEEVE_LEGS[target_class][lt]
                                   ["premium_share"], 4)}
                              for lt in ("primary", "convex")]
                     if target_class in SLEEVE_LEGS else standing_legs,
                     "placed_date": placed_date or today.isoformat(),
                     "last_action": action}
    elif action == "HARVEST":
        cls = standing_class or target_class
        core_budget = (standing_budget
                       * SLEEVE_LEGS[cls]["primary"]["premium_share"]
                       if cls in SLEEVE_LEGS else 0.0)
        new_state = {"scenario_class": cls,
                     "target_budget_pct": round(core_budget, 4),
                     "legs": [{"leg": "primary",
                               "instrument": SLEEVE_LEGS[cls]["primary"]
                               ["name"] if cls in SLEEVE_LEGS else "core",
                               "premium_pct_of_book": round(core_budget, 4)}],
                     "placed_date": placed_date or today.isoformat(),
                     "last_action": action}
    elif action == "UNWIND":
        new_state = {"scenario_class": None, "target_budget_pct": 0.0,
                     "legs": [], "placed_date": None,
                     "last_action": action}
    # HOLD / NONE: state unchanged.
    new_state["updated_at"] = now.isoformat()
    standing_after = num(new_state.get("target_budget_pct")) or 0.0

    # ---- pre-trade checks ------------------------------------------------
    checks = []

    bc = board.get("binding_constraint", {}) if isinstance(board, dict) else {}
    bc_dim = bc.get("dimension")
    checks.append({
        "check": "tail_is_binding_constraint",
        "ok": (not hedge_required) or bc_dim == "TAIL_STRESS",
        "detail": "firm-risk-board binding constraint = %s" % (bc_dim or "n/a"),
    })

    checks.append({
        "check": "standing_within_spend_cap",
        "ok": standing_after <= MAX_HEDGE_SPEND_PCT + 1e-6,
        "detail": ("standing sleeve after = %.2f%% of book vs %.2f%% cap"
                   % (standing_after, MAX_HEDGE_SPEND_PCT)),
    })

    if hedge_required and required_protection is not None \
            and target_payoff is not None:
        checks.append({
            "check": "sleeve_closes_the_gap",
            "ok": target_payoff + 1e-6 >= required_protection,
            "detail": ("target sleeve pays ~%.2f%% in the worst case vs "
                       "%.2f%% of protection required"
                       % (target_payoff, required_protection)),
        })

    if hedge_required:
        checks.append({
            "check": "carry_within_budget",
            "ok": target_carry <= 3.0,
            "detail": "target sleeve carry %.2f%%/yr" % target_carry,
        })

    if hedge_required and net_exp is not None:
        checks.append({
            "check": "hedge_protects_net_long_book",
            "ok": net_exp > 0,
            "detail": ("book net exposure %s%.0f%% -- crash protection is %s"
                       % ("+" if net_exp >= 0 else "", net_exp,
                          "warranted" if net_exp > 0 else
                          "questionable on a net-short book")),
        })

    if action not in ("HOLD", "NONE"):
        checks.append({
            "check": "ticket_above_min_size",
            "ok": abs(ticket_premium) + 1e-6 >= MIN_TICKET_PCT
            or action in ("UNWIND", "HARVEST"),
            "detail": ("ticket premium %.3f%% of book vs %.2f%% min"
                       % (abs(ticket_premium), MIN_TICKET_PCT)),
        })

    n_fail = sum(1 for c in checks if not c["ok"])

    # ---- confidence ------------------------------------------------------
    n_stale = sum(1 for a in ages.values()
                  if a is None or a > STALE_HOURS)
    quote_gap = n_quote_try > 0 and n_quote_ok < n_quote_try
    if not th_fresh:
        confidence = "LOW"
    elif n_stale >= 2 or (quote_gap and n_quote_ok == 0):
        confidence = "LOW"
    elif n_stale >= 1 or quote_gap:
        confidence = "MEDIUM"
    else:
        confidence = "HIGH"

    # ---- headline --------------------------------------------------------
    if action in ("HOLD", "NONE"):
        headline = "%s -- %s" % (action, reason)
    else:
        verb = {"OPEN": "Open", "ADD": "Add to", "TRIM": "Trim",
                "ROLL": "Roll", "SWITCH": "Switch", "HARVEST": "Harvest",
                "UNWIND": "Unwind"}.get(action, action)
        headline = ("%s the hedge sleeve -- %s ticket for %.2f%% of book "
                    "(~$%s) across %d leg(s) in the %s class."
                    % (verb, side_summary.split(" (")[0],
                       abs(ticket_premium), "{:,.0f}".format(
                           ticket_premium_usd), len(legs),
                       (use_class or "n/a")))

    # ---- CRO note --------------------------------------------------------
    bits = [reason]
    if action not in ("HOLD", "NONE", "UNWIND"):
        bits.append("Stance is %s; the worst modelled scenario is %s at "
                    "%.1f%% of book, %.1fpp past the soft loss limit."
                    % (stance, worst_scenario or "n/a",
                       worst_loss if worst_loss is not None else 0.0,
                       abs(gap_pp) if gap_pp is not None else 0.0))
    if closing_note:
        bits.append(closing_note)
    if legs:
        bits.append("The ticket below is target minus the standing sleeve "
                    "-- work the primary listed leg first, then the convex "
                    "leg on patient limits.")
    if n_fail:
        bits.append("%d pre-trade check(s) flagged -- review before "
                    "working the ticket." % n_fail)
    if confidence != "HIGH":
        bits.append("Confidence is %s; treat contract counts as indicative."
                    % confidence)
    cro_note = " ".join(bits)

    # ---- history + deltas ------------------------------------------------
    hist_body, _ = read_json(HIST_KEY)
    snaps = []
    if isinstance(hist_body, dict):
        snaps = hist_body.get("snapshots") or []
    prior = None
    iso_today = today.isoformat()
    for sn in reversed(snaps):
        if sn.get("date") != iso_today:
            prior = sn
            break
    snap = {"date": iso_today, "generated_at": now.isoformat(),
            "action": action, "ticket_premium_pct": ticket_premium,
            "standing_after_pct": standing_after,
            "scenario_class": use_class, "stance": stance}
    snaps = [s for s in snaps if s.get("date") != iso_today]
    snaps.append(snap)
    snaps = snaps[-180:]
    deltas = None
    if prior:
        deltas = {
            "prior_date": prior.get("date"),
            "prior_action": prior.get("action"),
            "prior_standing_pct": prior.get("standing_after_pct"),
            "standing_change_pp": (
                round(standing_after - (prior.get("standing_after_pct") or 0),
                      3)),
        }

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-hedge-planner",
        "method": "hedge_execution_planner",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),

        "action": action,
        "action_color": ACTION_COLOR.get(action, "dim"),
        "headline": headline,
        "cro_note": cro_note,
        "confidence": confidence,
        "stance": stance,
        "hedge_required": hedge_required,
        "scenario_class": use_class,

        "ticket": {
            "side_summary": side_summary,
            "closing_instruction": closing_note,
            "legs": legs,
            "total_premium_pct_of_book": abs(ticket_premium),
            "signed_premium_pct_of_book": ticket_premium,
            "total_premium_usd": ticket_premium_usd,
            "total_premium_bps_of_book": round(abs(ticket_premium) * 100, 1),
        },

        "standing_sleeve_before": {
            "scenario_class": standing_class,
            "budget_pct_of_book": round(standing_budget, 4),
            "legs": standing_legs,
            "placed_date": placed_date,
            "age_days": age_days,
        },
        "standing_sleeve_after": {
            "scenario_class": new_state.get("scenario_class"),
            "budget_pct_of_book": standing_after,
            "legs": new_state.get("legs"),
            "placed_date": new_state.get("placed_date"),
        },
        "rebalance_delta": {
            "budget_pct_delta": round(standing_after - standing_budget, 4),
            "from_pct": round(standing_budget, 4),
            "to_pct": standing_after,
        },

        "target": {
            "worst_scenario": worst_scenario,
            "worst_loss_pct": worst_loss,
            "gap_pp": gap_pp,
            "required_protection_pct_of_book": required_protection,
            "hedge_budget_pct_of_book": round(target_budget, 4),
            "expected_payoff_pct_of_book": target_payoff,
            "payoff_multiple": payoff_mult,
            "annualised_carry_pct": target_carry,
            "scenario_class": target_class,
        },

        "pre_trade_checks": checks,
        "n_checks_flagged": n_fail,

        "book": {
            "notional_usd_assumed": BOOK_NOTIONAL_USD,
            "net_exposure_pct": net_exp,
            "gross_exposure_pct": gross_exp,
        },
        "spots": spots,
        "deltas": deltas,
        "feed_ages_hours": ages,

        "parameters": {
            "roll_days": ROLL_DAYS,
            "max_hedge_spend_pct": MAX_HEDGE_SPEND_PCT,
            "min_ticket_pct": MIN_TICKET_PCT,
            "book_notional_usd_assumed": BOOK_NOTIONAL_USD,
        },

        "how_to_read": (
            "The firm Hedge Execution Planner. The Tail Hedge Overlay says "
            "how much convex protection the book needs; this desk turns "
            "that into a worked order ticket. It keeps the standing sleeve "
            "as state, so each run shows the rebalance delta -- open when "
            "flat, add or trim as the target moves, roll when the sleeve "
            "decays, switch the instrument class when the binding scenario "
            "changes, harvest the convex leg when the regime says monetise. "
            "It places no orders -- it produces the ticket a trader works."),
        "disclaimer": (
            "Built on a hypothetical research book. Sizing is in "
            "percent-of-book; dollar and contract figures use an assumed "
            "$" + "{:,.0f}".format(BOOK_NOTIONAL_USD) + " paper-book "
            "notional purely to make the ticket concrete. Option premiums "
            "are stylised desk rules of thumb, not live option chains. "
            "Research and education only, not investment advice."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json")
    s3.put_object(Bucket=S3_BUCKET, Key=STATE_KEY,
                  Body=json.dumps(new_state, default=str).encode("utf-8"),
                  ContentType="application/json")
    s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                  Body=json.dumps({"schema_version": SCHEMA,
                                   "engine": "justhodl-hedge-planner",
                                   "updated_at": now.isoformat(),
                                   "snapshots": snaps},
                                  default=str).encode("utf-8"),
                  ContentType="application/json")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "action": action, "side": side_summary,
        "ticket_premium_pct": ticket_premium,
        "standing_after_pct": standing_after,
        "n_checks_flagged": n_fail, "confidence": confidence})}
