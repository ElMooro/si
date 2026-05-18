"""
justhodl-tail-hedge -- the firm Tail Hedge Overlay.
===================================================
WHY THIS EXISTS
---------------
The firm risk stack ends with a CRO Risk Board, and that board's
standing finding is unambiguous: the binding constraint is TAIL_STRESS.
The 15-scenario stress desk models the book losing more than the soft
loss limit in a COVID-class crash. Statistical risk (99% 1-day VaR) is
small -- this is not a normal-vol problem, it is a convex tail problem.

A multi-strategy fund whose CRO board flags tail stress as the number
one vulnerability does exactly one thing next: it runs a tail-hedge
overlay -- a small, deliberately convex sleeve of protection that is a
drag in calm regimes and pays explosively in a crash, sized so the
worst modelled scenario is brought back inside the loss limit.

The Factor Risk Model already sizes hedges, but those are LINEAR
factor-neutralising trades (short IWM to flatten a SIZE bet). They
shrink day-to-day VaR. They do nothing for crash convexity. A tail
hedge is a different instrument for a different risk, so this is a new
engine, not a rebuild.

WHAT THIS ENGINE IS NOT
-----------------------
It places no trades. Like every engine in the risk stack it is an
advisory overlay: it reads the deployed risk outputs and SIZES and
RECOMMENDS a hedge sleeve. Execution stays with the operator.

HOW IT IS BUILT  (institution-grade, four design rules)
  1 COST-BUDGETED  -- protection bleeds carry; an always-on hedge
    destroys calm-regime returns. The sleeve is sized to a premium
    budget, not to a fear level.
  2 SCENARIO-TARGETED -- an equity crash, a rates shock and a momentum
    unwind need different instruments. The worst named scenario from
    firm-stress is classified and mapped to the right hedge sleeve.
  3 BOOK-AWARE  -- the book is not the index. The tail loss is the one
    firm-stress already modelled on the actual firm book; net market
    beta and net exposure describe the residual directional risk the
    hedge has to cover.
  4 REGIME-TIMED -- protection is cheapest in calm regimes. The overlay
    ACCUMULATES cheap and MONETIZES into stress, with an explicit
    harvest signal -- the opposite of panic-buying after the move.

SIZING TARGET
-------------
  gap = worst_scenario_loss - soft_loss_limit       (a negative pp gap)
  required_protection = |gap|                       (% of book to recover)
  hedge_budget = required_protection / payoff_multiple
  annualised_carry = hedge_budget * roll_factor * carry_multiplier
The objective is concrete and auditable: spend `hedge_budget` of premium
so the convex sleeve returns `required_protection` of book in the worst
named scenario, dragging the worst-case loss back to the soft limit.

OUTPUT   data/tail-hedge.json  (+ append data/tail-hedge-history.json)
SCHEDULE daily 04:30 UTC -- after the firm-risk-board (04:00), so the
overlay always reads a complete, fresh risk stack and the board's own
binding-constraint readout.
"""
import json
import time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/tail-hedge.json"
HIST_KEY = "data/tail-hedge-history.json"
SCHEMA = "1.0"

STALE_HOURS = 30.0
HIST_CAP = 180

# Annualisation of a rolling ~3-month protection sleeve. Rolling spreads
# does not re-pay full premium each roll, so this is below a naive 4x.
ROLL_FACTOR = 3.6

# Engine inputs -- all already produced by the deployed risk stack.
ENGINES = {
    "firm-stress": "data/firm-stress.json",
    "factor-risk": "data/factor-risk.json",
    "firm-book": "data/firm-book.json",
    "firm-risk-board": "data/firm-risk-board.json",
    "eurodollar-stress": "data/eurodollar-stress.json",
    "canary-grid": "data/canary-grid.json",
}
# the three feeds the overlay cannot run without
CRITICAL_FEEDS = ("firm-stress", "factor-risk", "firm-book")

# Hedge sleeves. payoff_multiple = protection returned per unit of
# premium in that sleeve's named worst scenario -- conservative desk
# rules of thumb (a deep crash put-spread compounds ~6x; a VIX-heavy
# vol sleeve more; a rates or factor sleeve less convex).
HEDGE_SLEEVES = {
    "EQUITY_CRASH": {
        "label": "Equity crash protection",
        "instruments": "SPY/QQQ put spreads + VIX calls",
        "primary_leg": "SPY 3M ~7% OTM put spread (~8% wide)",
        "convex_leg": "VIX 2M calls, ~5% of sleeve premium",
        "payoff_multiple": 6.0,
    },
    "RATES_SHOCK": {
        "label": "Rates / duration shock protection",
        "instruments": "TLT puts + 2s10s steepener",
        "primary_leg": "TLT 3M OTM puts",
        "convex_leg": "payer-swaption-style curve steepener",
        "payoff_multiple": 4.0,
    },
    "MOMENTUM_UNWIND": {
        "label": "Momentum-crowding / factor-unwind protection",
        "instruments": "MTUM puts + de-gross crowded longs",
        "primary_leg": "MTUM 2M OTM puts",
        "convex_leg": "VIX 1M calls + crowded-long trim",
        "payoff_multiple": 5.0,
    },
    "CREDIT_EVENT": {
        "label": "Credit / small-cap stress protection",
        "instruments": "HYG puts + IWM put spreads",
        "primary_leg": "HYG 3M OTM puts",
        "convex_leg": "IWM 2M put spread",
        "payoff_multiple": 6.0,
    },
    "VOL_SPIKE": {
        "label": "Vol-spike / convexity protection",
        "instruments": "VIX calls + SPY put spreads",
        "primary_leg": "VIX 1-2M calls",
        "convex_leg": "SPY 2M put spread",
        "payoff_multiple": 9.0,
    },
}

SEV_WORD = {0: "OK", 1: "WATCH", 2: "ALERT"}
SEV_COLOR = {0: "green", 1: "orange", 2: "red"}


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


def hours_since(last_mod, now):
    if last_mod is None:
        return None
    try:
        if last_mod.tzinfo is None:
            last_mod = last_mod.replace(tzinfo=timezone.utc)
        return round((now - last_mod).total_seconds() / 3600.0, 1)
    except Exception:
        return None


def classify_scenario(name):
    """Map a firm-stress scenario name to a hedge sleeve class.

    Classification is by the stable scenario name -- the stress battery
    is a fixed, named set. Order matters: the most specific token wins.
    """
    n = (name or "").lower()
    if any(t in n for t in ("volmageddon", "vol ")):
        return "VOL_SPIKE"
    if any(t in n for t in ("momentum", "quant", "crowding")):
        return "MOMENTUM_UNWIND"
    if any(t in n for t in ("rate shock", "rates +", "rates+", "taper",
                            "stagflation")):
        return "RATES_SHOCK"
    if any(t in n for t in ("regional bank", "credit")):
        return "CREDIT_EVENT"
    # everything else -- COVID, GFC, broad selloffs, China deval,
    # flight-to-quality -- is an equity-crash tail.
    return "EQUITY_CRASH"


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    feeds, ages = {}, {}
    for name, key in ENGINES.items():
        body, lm = read_json(key)
        feeds[name] = body or {}
        ages[name] = hours_since(lm, now)

    fs = feeds["firm-stress"]
    fr = feeds["factor-risk"]
    fb = feeds["firm-book"]
    board = feeds["firm-risk-board"]
    euro = feeds["eurodollar-stress"]
    canary = feeds["canary-grid"]

    # -- feed health -------------------------------------------------------
    feed_health = {}
    for name in ENGINES:
        age = ages[name]
        present = bool(feeds[name])
        fresh = present and age is not None and age <= STALE_HOURS
        feed_health[name] = {"present": present, "fresh": fresh,
                             "age_hours": age}
    n_critical_bad = sum(
        1 for f in CRITICAL_FEEDS if not feed_health[f]["fresh"])

    # ---------------------------------------------------------------------
    # 1) TAIL EXPOSURE -- read straight from the stress desk.
    # ---------------------------------------------------------------------
    fs_summary = fs.get("summary", {}) if isinstance(fs, dict) else {}
    loss_limits = fs.get("loss_limits", {}) if isinstance(fs, dict) else {}
    soft_limit = num(loss_limits.get("soft_pct"))
    hard_limit = num(loss_limits.get("hard_pct"))
    if soft_limit is None:
        soft_limit = -12.0
    if hard_limit is None:
        hard_limit = -20.0

    worst_scenario = fs_summary.get("worst_scenario")
    worst_loss = num(fs_summary.get("worst_loss_pct"))
    median_pnl = num(fs_summary.get("median_scenario_pnl_pct"))
    n_losing = fs_summary.get("n_losing_scenarios")
    n_scen = fs_summary.get("n_scenarios")

    fr_firm = fr.get("firm", {}) if isinstance(fr, dict) else {}
    net_beta = num(fr_firm.get("net_market_beta"))
    var99 = num(fr_firm.get("var_99_1d_pct"))
    annual_vol = num(fr_firm.get("annual_vol_pct"))

    fb_firm = fb.get("firm", {}) if isinstance(fb, dict) else {}
    gross_exp = num(fb_firm.get("gross_exposure_pct"))
    net_exp = num(fb_firm.get("net_exposure_pct"))
    long_short = num(fb_firm.get("long_short_ratio"))
    n_names = fb_firm.get("n_equity_names")

    stress_available = worst_loss is not None and worst_scenario

    # ---------------------------------------------------------------------
    # 2) HEDGE REQUIREMENT -- gap of the worst case past the soft limit.
    # ---------------------------------------------------------------------
    # gap is worst minus soft; both negative. A negative gap means the
    # worst scenario loses MORE than the soft limit allows.
    gap_pp = None
    breach_vs_hard_pp = None
    required_protection = 0.0
    hedge_required = False
    if stress_available:
        gap_pp = round(worst_loss - soft_limit, 2)
        breach_vs_hard_pp = round(worst_loss - hard_limit, 2)
        if gap_pp < 0:
            hedge_required = True
            required_protection = round(abs(gap_pp), 2)

    # ---------------------------------------------------------------------
    # 3) SCENARIO-TARGETED SLEEVE.
    # ---------------------------------------------------------------------
    scen_class = classify_scenario(worst_scenario)
    sleeve = HEDGE_SLEEVES[scen_class]
    payoff_multiple = sleeve["payoff_multiple"]

    # ---------------------------------------------------------------------
    # 4) REGIME -- accumulate cheap vs monetize rich.
    # ---------------------------------------------------------------------
    euro_score = num(euro.get("composite_score")) if isinstance(
        euro, dict) else None
    canary_level = num(canary.get("early_warning_level")) if isinstance(
        canary, dict) else None
    board_posture = (board.get("firm_posture")
                     if isinstance(board, dict) else None)

    regime_inputs = [v for v in (euro_score, canary_level) if v is not None]
    regime_score = (round(sum(regime_inputs) / len(regime_inputs), 1)
                    if regime_inputs else None)

    # protection costs more when vol is already bid.
    rs_for_cost = regime_score if regime_score is not None else 40.0
    carry_multiplier = round(1.0 + rs_for_cost / 100.0, 2)

    board_red = (board_posture or "").upper() == "RED"
    if regime_score is None:
        stance = "HOLD"
        stance_reason = ("No regime feed -- hold the current hedge and "
                         "re-time on the next eurodollar-stress / canary "
                         "update.")
    elif board_red or regime_score >= 65.0:
        stance = "MONETIZE"
        stance_reason = ("Stress is realising (regime score %.0f / firm "
                         "posture %s) -- harvest the convex leg into the "
                         "move and re-risk as protection gets rich."
                         % (regime_score, board_posture or "n/a"))
    elif regime_score <= 35.0:
        stance = "ACCUMULATE"
        stance_reason = ("Calm regime (score %.0f) -- protection is cheap; "
                         "this is the window to build or roll the sleeve "
                         "forward at low premium." % regime_score)
    else:
        stance = "HOLD"
        stance_reason = ("Mid regime (score %.0f) -- carry the existing "
                         "sleeve; neither accumulate nor harvest."
                         % regime_score)

    # ---------------------------------------------------------------------
    # 5) HEDGE SIZING.
    # ---------------------------------------------------------------------
    hedge_budget = 0.0
    annualised_carry = 0.0
    expected_payoff = 0.0
    if hedge_required and payoff_multiple > 0:
        hedge_budget = round(required_protection / payoff_multiple, 3)
        annualised_carry = round(
            hedge_budget * ROLL_FACTOR * carry_multiplier, 2)
        expected_payoff = required_protection

    # insurance ratio = tail payoff per year of carry. Below ~1 the hedge
    # only pays if the tail hits inside a year; that is the honest cost.
    insurance_ratio = (round(expected_payoff / annualised_carry, 2)
                       if annualised_carry > 0 else None)

    # ---------------------------------------------------------------------
    # 6) POSTURE + SEVERITY.
    # ---------------------------------------------------------------------
    if not stress_available:
        hedge_posture = "NO STRESS FEED"
        severity = 1
    elif not hedge_required:
        hedge_posture = "UNHEDGED -- NOT REQUIRED"
        severity = 0
    else:
        hedge_posture = "HEDGE RECOMMENDED"
        # a small breach of the soft limit is a WATCH; a large breach,
        # or any breach of the hard limit, is an ALERT.
        if (breach_vs_hard_pp is not None and breach_vs_hard_pp <= 0) \
                or required_protection >= 4.0:
            severity = 2
        else:
            severity = 1

    status_color = SEV_COLOR[severity]
    status_word = SEV_WORD[severity]

    # 0-100 tail-risk score: how deep into the soft->hard band the worst
    # case sits. 0 at/inside soft, 100 at/through hard.
    tail_score = 0.0
    if stress_available and soft_limit is not None and hard_limit is not None:
        band = soft_limit - hard_limit  # positive width, e.g. 8
        if band > 0:
            tail_score = max(0.0, min(
                100.0, (soft_limit - worst_loss) / band * 100.0))
    tail_score = round(tail_score, 1)

    # ---------------------------------------------------------------------
    # 7) HEADLINE.
    # ---------------------------------------------------------------------
    if not stress_available:
        headline = ("Tail Hedge Overlay cannot size -- the firm-stress "
                    "feed is missing or unreadable.")
    elif not hedge_required:
        headline = ("Tail hedge NOT REQUIRED -- worst modelled scenario "
                    "%s%.1f%% sits inside the %.0f%% soft loss limit."
                    % ("+" if worst_loss >= 0 else "", worst_loss,
                       soft_limit))
    else:
        headline = ("HEDGE RECOMMENDED -- %s loses %.1f%%, %.1fpp past "
                    "the %.0f%% soft limit. Size ~%.2f%% of book in %s; "
                    "covers ~%.1f%% in the tail for ~%.2f%%/yr carry."
                    % ((worst_scenario or "worst scenario").split(" (")[0],
                       worst_loss, required_protection, soft_limit,
                       hedge_budget, sleeve["label"].lower(),
                       expected_payoff, annualised_carry))

    # ---------------------------------------------------------------------
    # 8) CONSISTENCY CHECKS.
    # ---------------------------------------------------------------------
    checks = []

    # the tail loss must out-run the statistical 99% 1-day VaR, or the
    # stress battery is not actually reaching the tail.
    if worst_loss is not None and var99 is not None:
        ok = abs(worst_loss) >= abs(var99)
        checks.append({
            "check": "stress_exceeds_var99", "ok": ok,
            "detail": ("worst stress %.1f%% vs 99%% VaR %.2f%%"
                       % (worst_loss, var99))})

    # if a hedge is recommended, the firm-risk-board's own binding
    # constraint should be the tail axis -- the two engines must agree.
    if hedge_required and isinstance(board, dict):
        bc = board.get("binding_constraint", {})
        bc_dim = bc.get("dimension") if isinstance(bc, dict) else None
        agree = bc_dim == "TAIL_STRESS"
        checks.append({
            "check": "board_agrees_tail_is_binding", "ok": agree,
            "detail": ("board binding constraint = %s"
                       % (bc_dim or "n/a"))})

    # the carry should be a small drag, not a structural one. A tail
    # sleeve costing more than ~3%/yr is mis-sized or the regime is
    # extreme -- flag it for review rather than silently recommend it.
    if hedge_required:
        cheap = annualised_carry <= 3.0
        checks.append({
            "check": "carry_within_budget", "ok": cheap,
            "detail": ("annualised carry %.2f%%/yr" % annualised_carry)})

    checks.append({
        "check": "critical_feeds_fresh", "ok": n_critical_bad == 0,
        "detail": ("%d of %d critical feeds stale/missing"
                   % (n_critical_bad, len(CRITICAL_FEEDS)))})

    # ---------------------------------------------------------------------
    # 9) CONFIDENCE.
    # ---------------------------------------------------------------------
    n_stale = sum(1 for f in feed_health.values() if not f["fresh"])
    if n_critical_bad > 0 or n_stale >= 3:
        confidence = "LOW"
    elif n_stale >= 1:
        confidence = "MEDIUM"
    else:
        confidence = "HIGH"

    # ---------------------------------------------------------------------
    # 10) HISTORY + DELTAS.
    # ---------------------------------------------------------------------
    today = now.date().isoformat()
    hist_body, _ = read_json(HIST_KEY)
    snapshots = []
    if isinstance(hist_body, dict):
        snapshots = hist_body.get("snapshots") or []
    elif isinstance(hist_body, list):
        snapshots = hist_body
    prior = None
    for snap in reversed(snapshots):
        if snap.get("date") != today:
            prior = snap
            break

    snapshot = {
        "date": today,
        "generated_at": now.isoformat(),
        "hedge_posture": hedge_posture,
        "severity": severity,
        "worst_scenario": worst_scenario,
        "worst_loss_pct": worst_loss,
        "gap_pp": gap_pp,
        "required_protection_pct": required_protection,
        "hedge_budget_pct": hedge_budget,
        "annualised_carry_pct": annualised_carry,
        "regime_score": regime_score,
        "regime_stance": stance,
    }
    snapshots = [s for s in snapshots if s.get("date") != today]
    snapshots.append(snapshot)
    snapshots = snapshots[-HIST_CAP:]

    def delta(cur, prev):
        if cur is None or prev is None:
            return None
        try:
            return round(cur - prev, 2)
        except Exception:
            return None

    deltas = None
    if prior:
        deltas = {
            "worst_loss_pp": delta(worst_loss, prior.get("worst_loss_pct")),
            "required_protection_pp": delta(
                required_protection, prior.get("required_protection_pct")),
            "annualised_carry_pp": delta(
                annualised_carry, prior.get("annualised_carry_pct")),
            "regime_score": delta(regime_score, prior.get("regime_score")),
            "prior_date": prior.get("date"),
            "prior_posture": prior.get("hedge_posture"),
            "prior_stance": prior.get("regime_stance"),
        }

    # ---------------------------------------------------------------------
    # 11) DETERMINISTIC CRO HEDGE BRIEF  (templated -- auditable, free).
    # ---------------------------------------------------------------------
    bits = []
    if not stress_available:
        bits.append("The Tail Hedge Overlay has no stress feed to size "
                     "against -- firm-stress is missing or stale. No "
                     "recommendation can be made until it refreshes.")
    elif not hedge_required:
        bits.append("No tail hedge is required. The worst of %s modelled "
                     "scenarios loses %.1f%%, still inside the %.0f%% soft "
                     "loss limit, so the book carries no structural breach."
                     % (n_scen or "the", worst_loss, soft_limit))
        if stance == "ACCUMULATE":
            bits.append("The regime is calm (score %.0f), so protection is "
                        "cheap -- an opportunistic starter sleeve can be "
                        "pre-positioned even though it is not mandated."
                        % regime_score)
    else:
        bits.append("A tail hedge is recommended. The binding scenario is "
                     "%s: it loses %.1f%% of book, %.1fpp beyond the %.0f%% "
                     "soft loss limit." % (
                         (worst_scenario or "the worst scenario"),
                         worst_loss, required_protection, soft_limit))
        if breach_vs_hard_pp is not None and breach_vs_hard_pp <= 0:
            bits.append("This also breaches the %.0f%% HARD loss limit -- "
                        "the overlay is an ALERT, not a watch."
                        % hard_limit)
        bits.append("Sleeve: %s (%s). Size roughly %.2f%% of book in "
                    "premium; at the sleeve's ~%.1fx tail payoff that "
                    "returns about %.1f%% of book in the named scenario, "
                    "pulling the worst case back to the soft limit."
                    % (sleeve["label"], sleeve["instruments"],
                       hedge_budget, payoff_multiple, expected_payoff))
        bits.append("Carry: about %.2f%% per year at the current regime "
                    "(carry multiplier %.2fx). Tail payoff per year of "
                    "carry is %s."
                    % (annualised_carry, carry_multiplier,
                       ("%.1fx" % insurance_ratio)
                       if insurance_ratio is not None else "n/a"))
    bits.append("Regime stance: %s. %s" % (stance, stance_reason))
    if net_beta is not None:
        bits.append("Book context: net market beta %.2f, net exposure "
                    "%s%.0f%% -- this is the residual directional risk "
                    "the sleeve is protecting."
                    % (net_beta,
                       "+" if (net_exp or 0) >= 0 else "",
                       net_exp if net_exp is not None else 0.0))
    if confidence != "HIGH":
        bits.append("Confidence is %s -- %d feed(s) are stale or missing; "
                    "treat the sizing as indicative until they refresh."
                    % (confidence, n_stale))
    cro_brief = " ".join(bits)

    # ---------------------------------------------------------------------
    # 12) ASSEMBLE + WRITE.
    # ---------------------------------------------------------------------
    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-tail-hedge",
        "method": "tail_hedge_overlay",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),

        "hedge_posture": hedge_posture,
        "severity": severity,
        "status": status_word,
        "status_color": status_color,
        "tail_score": tail_score,
        "headline": headline,
        "cro_brief": cro_brief,
        "confidence": confidence,

        "tail_exposure": {
            "worst_scenario": worst_scenario,
            "worst_loss_pct": worst_loss,
            "soft_loss_limit_pct": soft_limit,
            "hard_loss_limit_pct": hard_limit,
            "median_scenario_pnl_pct": median_pnl,
            "n_losing_scenarios": n_losing,
            "n_scenarios": n_scen,
            "net_market_beta": net_beta,
            "annual_vol_pct": annual_vol,
            "var_99_1d_pct": var99,
            "gross_exposure_pct": gross_exp,
            "net_exposure_pct": net_exp,
            "long_short_ratio": long_short,
            "n_equity_names": n_names,
            "breach_vs_soft_pp": gap_pp,
            "breach_vs_hard_pp": breach_vs_hard_pp,
        },

        "hedge_requirement": {
            "required": hedge_required,
            "gap_pp": gap_pp,
            "required_protection_pct_of_book": required_protection,
            "target": ("bring the worst modelled stress P&L back inside "
                       "the %.0f%% soft loss limit" % soft_limit),
        },

        "hedge_sleeve": {
            "scenario_class": scen_class,
            "label": sleeve["label"],
            "instruments": sleeve["instruments"],
            "primary_leg": sleeve["primary_leg"],
            "convex_leg": sleeve["convex_leg"],
            "payoff_multiple": payoff_multiple,
            "hedge_budget_pct_of_book": hedge_budget,
            "annualised_carry_pct": annualised_carry,
            "expected_payoff_in_worst_scenario_pct": expected_payoff,
        },

        "regime": {
            "regime_score": regime_score,
            "eurodollar_stress_score": euro_score,
            "canary_grid_level": canary_level,
            "firm_posture": board_posture,
            "carry_multiplier": carry_multiplier,
            "stance": stance,
            "rationale": stance_reason,
        },

        "cost_benefit": {
            "annualised_carry_pct": annualised_carry,
            "tail_loss_averted_pct": expected_payoff,
            "insurance_ratio": insurance_ratio,
            "return_drag_pct": annualised_carry,
        },

        "consistency_checks": checks,
        "deltas": deltas,
        "feed_health": feed_health,
        "feed_ages_hours": ages,

        "limits": {
            "soft_loss_limit_pct": soft_limit,
            "hard_loss_limit_pct": hard_limit,
            "roll_factor": ROLL_FACTOR,
            "stale_hours": STALE_HOURS,
        },

        "how_to_read": (
            "The firm Tail Hedge Overlay. It reads the 15-scenario stress "
            "desk, the factor risk model and the firm book, and sizes a "
            "deliberately convex protection sleeve so the worst modelled "
            "scenario is pulled back inside the soft loss limit. The sleeve "
            "is scenario-targeted (the worst named scenario picks the "
            "instrument), cost-budgeted (sized to a premium budget, not a "
            "fear level) and regime-timed (accumulate cheap, monetise into "
            "stress). It places no trades -- it sizes and recommends."),
        "disclaimer": (
            "Built on a hypothetical research book with no costs, slippage "
            "or financing. Hedge sizing uses stylised option payoff and "
            "carry rules of thumb, not live option chains. Research and "
            "education only, not investment advice."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json")
    s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                  Body=json.dumps(
                      {"schema_version": SCHEMA,
                       "engine": "justhodl-tail-hedge",
                       "updated_at": now.isoformat(),
                       "snapshots": snapshots},
                      default=str).encode("utf-8"),
                  ContentType="application/json")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "hedge_posture": hedge_posture,
        "severity": severity, "scenario_class": scen_class,
        "hedge_budget_pct": hedge_budget,
        "annualised_carry_pct": annualised_carry,
        "stance": stance, "confidence": confidence})}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
