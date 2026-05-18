"""
justhodl-firm-risk-board -- the firm CRO Risk Board.
=====================================================
WHY THIS EXISTS
---------------
The platform now runs a full firm risk stack: a capital allocator, a
consolidated firm book, a mandate risk-monitor, a liquidity & capacity
monitor, a multi-factor risk model (VaR / ES / hedges), a P&L
attribution ledger, a 15-scenario stress desk and a merger-arb
deal-break monitor. Eight engines, eight S3 outputs, eight pages.

But a Chief Risk Officer does NOT open eight screens. Every real
multi-strategy firm -- Millennium, Citadel, Point72, Balyasny -- runs
ONE risk board: a single pane of glass that rolls every risk axis into
one firm posture, one binding-constraint readout, one daily brief, and
an escalation ladder that says "we are RED because of X". Without it
the stack is nine instruments with no dashboard.

This engine is that board. It is a pure synthesis layer -- it reads the
eight engine outputs and reduces them; it never re-computes risk and
never modifies an upstream engine. It mirrors the signal-board pattern
already used for the macro engines, but for the firm risk side.

THE SIX RISK DIMENSIONS  (each: status OK / WATCH / ALERT, 0-100 score,
the one headline number, the source engine, and a freshness flag)
  1 MANDATE       hard mandate limits           <- risk-monitor
  2 MARKET_VAR    statistical loss (99% 1d VaR) <- factor-risk
  3 TAIL_STRESS   named worst-case scenario     <- firm-stress
  4 LIQUIDITY     days-to-liquidate / trapped   <- liquidity-capacity
  5 CONCENTRATION top-10 names + dominant desk  <- firm-book + allocator
  6 EVENT_ARB     merger-arb cluster-break      <- merger-arb-risk
Plus a PERFORMANCE context panel (Sharpe / drawdown <- pnl-attribution);
its drawdown can escalate posture, its ratios are context only.

FIRM POSTURE  -- worst-of escalation, exactly how a CRO board works:
  firm severity = max severity across all six dimensions.
  A hard mandate breach (risk-monitor RED) always forces RED.
  2 -> RED, 1 -> AMBER, 0 -> GREEN.

ALSO  limit-utilisation table (every axis as a % of its limit), the
ranked top firm risks, day-over-day deltas vs the prior board snapshot,
cross-engine consistency checks (stress must out-run statistical VaR),
and a deterministic CRO brief (templated, not LLM -- auditable, free,
always available).

OUTPUT   data/firm-risk-board.json  (+ append data/firm-risk-board-history.json)
SCHEDULE daily 04:00 UTC -- after merger-arb-risk (03:30), the last
upstream engine, so the board always reads a complete fresh stack.
"""
import json
import time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/firm-risk-board.json"
HIST_KEY = "data/firm-risk-board-history.json"
SCHEMA = "1.0"

# every upstream engine runs once a day; a healthy feed is < ~30h old.
STALE_HOURS = 30.0
HIST_CAP = 180

# board-set risk limits (a CRO sets these as constants, not derived).
VAR99_SOFT = 3.0       # 99% 1-day VaR, % of book
VAR99_HARD = 5.0
DD_SOFT = -8.0         # firm drawdown soft / hard, % of book
DD_HARD = -15.0
TOP10_WATCH = 35.0     # top-10 single-name concentration, % gross
TOP10_ALERT = 50.0
DESK_WATCH = 32.0      # dominant desk weight, % of firm capital
DESK_ALERT = 42.0

ENGINES = {
    "risk-monitor": "data/risk-monitor.json",
    "factor-risk": "data/factor-risk.json",
    "firm-stress": "data/firm-stress.json",
    "liquidity-capacity": "data/liquidity-capacity.json",
    "merger-arb-risk": "data/merger-arb-risk.json",
    "pnl-attribution": "data/pnl-attribution.json",
    "desk-allocator": "data/desk-allocator.json",
    "firm-book": "data/firm-book.json",
}

SEV_WORD = {0: "OK", 1: "WATCH", 2: "ALERT"}
POSTURE_WORD = {0: "GREEN", 1: "AMBER", 2: "RED"}


# --------------------------------------------------------------------------
def read_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = json.loads(obj["Body"].read())
        return body, obj["LastModified"]
    except Exception:
        return None, None


def num(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def clamp01(v):
    return max(0.0, min(100.0, v))


def hours_since(last_mod, now):
    if last_mod is None:
        return None
    try:
        if last_mod.tzinfo is None:
            last_mod = last_mod.replace(tzinfo=timezone.utc)
        return round((now - last_mod).total_seconds() / 3600.0, 1)
    except Exception:
        return None


def sev_from_posture(word):
    """Map an engine posture word to a 0/1/2 severity."""
    w = (word or "").upper()
    if w in ("RED", "ALERT", "CRITICAL", "BREACH"):
        return 2
    if w in ("AMBER", "WATCH", "WARN", "CAUTION", "ELEVATED"):
        return 1
    return 0  # GREEN / OK / WARMING / unknown


def dim(name, label, source, severity, score, headline, detail,
        stale_h, value=None, limit=None):
    flagged = stale_h is not None and stale_h > STALE_HOURS
    return {
        "dimension": name,
        "label": label,
        "source": source,
        "status": SEV_WORD[severity],
        "severity": severity,
        "score": round(clamp01(score), 1),
        "headline": headline,
        "detail": detail,
        "value": value,
        "limit": limit,
        "data_age_hours": stale_h,
        "stale": flagged,
    }


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    feeds = {}
    ages = {}
    for name, key in ENGINES.items():
        body, lm = read_json(key)
        feeds[name] = body or {}
        ages[name] = hours_since(lm, now)

    rm = feeds["risk-monitor"]
    fr = feeds["factor-risk"]
    fs = feeds["firm-stress"]
    lc = feeds["liquidity-capacity"]
    mar = feeds["merger-arb-risk"]
    pnl = feeds["pnl-attribution"]
    alloc = feeds["desk-allocator"]
    fb = feeds["firm-book"]

    fr_firm = fr.get("firm") or {}
    fs_sum = fs.get("summary") or {}
    lc_firm = lc.get("firm") or {}
    mar_sum = mar.get("summary") or {}
    pnl_firm = pnl.get("firm") or {}
    alloc_firm = alloc.get("firm") or {}
    fb_firm = fb.get("firm") or {}

    dimensions = []

    # -- 1 MANDATE -----------------------------------------------------------
    rm_posture = rm.get("risk_posture") or rm.get("posture")
    rm_sev = sev_from_posture(rm_posture)
    rm_util = num(rm.get("risk_budget_utilization_pct"))
    n_br = rm.get("n_breaches")
    n_wt = rm.get("n_watches")
    dimensions.append(dim(
        "MANDATE", "Mandate Limits", "risk-monitor", rm_sev,
        rm_util if rm_util is not None else (0 if rm_sev == 0 else 60),
        ("%s breach(es), %s watch(es)" % (n_br, n_wt)
         if (n_br is not None) else (rm_posture or "no data")),
        rm.get("headline") or "Mandate monitor unavailable.",
        ages["risk-monitor"], value=rm_util, limit=100.0))

    # -- 2 MARKET / VaR ------------------------------------------------------
    var99 = num(fr_firm.get("var_99_1d_pct"))
    var95 = num(fr_firm.get("var_95_1d_pct"))
    es95 = num(fr_firm.get("es_95_1d_pct"))
    if var99 is None:
        var_sev, var_score = 0, 0.0
        var_head = "no VaR data"
    else:
        if var99 >= VAR99_HARD:
            var_sev = 2
        elif var99 >= VAR99_SOFT:
            var_sev = 1
        else:
            var_sev = 0
        var_score = var99 / VAR99_HARD * 100.0
        var_head = "99%% 1-day VaR %.2f%% of book (limit %.1f%%)" % (
            var99, VAR99_HARD)
    dimensions.append(dim(
        "MARKET_VAR", "Market Risk (VaR)", "factor-risk", var_sev,
        var_score, var_head,
        ("95%% VaR %.2f%%, ES %.2f%%, 99%% VaR %.2f%% -- soft %.1f%% / "
         "hard %.1f%%." % (var95 or 0, es95 or 0, var99 or 0,
                           VAR99_SOFT, VAR99_HARD)
         if var99 is not None else "Factor risk model unavailable."),
        ages["factor-risk"], value=var99, limit=VAR99_HARD))

    # -- 3 TAIL / STRESS -----------------------------------------------------
    fs_posture = fs.get("posture")
    fs_sev = sev_from_posture(fs_posture)
    worst_loss = num(fs_sum.get("worst_loss_pct"))
    worst_scen = fs_sum.get("worst_scenario")
    ll = fs.get("loss_limits") or {}
    hard_ll = num(ll.get("hard_pct")) or DD_HARD
    if worst_loss is not None and hard_ll:
        stress_score = abs(worst_loss) / abs(hard_ll) * 100.0
    else:
        stress_score = 0 if fs_sev == 0 else 65
    dimensions.append(dim(
        "TAIL_STRESS", "Tail / Stress P&L", "firm-stress", fs_sev,
        stress_score,
        ("worst scenario %.1f%% (%s)" % (worst_loss, worst_scen)
         if worst_loss is not None else (fs_posture or "no data")),
        fs.get("headline") or "Stress desk unavailable.",
        ages["firm-stress"], value=worst_loss, limit=hard_ll))

    # -- 4 LIQUIDITY ---------------------------------------------------------
    lc_posture = lc.get("liquidity_posture")
    lc_sev = sev_from_posture(lc_posture)
    lc_score = num(lc.get("liquidity_score"))
    wavg_days = num(lc_firm.get("wavg_days_to_liquidate"))
    trapped = num(lc_firm.get("trapped_book_pct"))
    # liquidity_score is a 0-100 health score; invert to a 0-100 risk score.
    liq_risk = (100.0 - lc_score) if lc_score is not None else (
        0 if lc_sev == 0 else 60)
    dimensions.append(dim(
        "LIQUIDITY", "Liquidity & Capacity", "liquidity-capacity", lc_sev,
        liq_risk,
        ("%.1fd to liquidate, %.1f%% trapped" % (wavg_days or 0, trapped or 0)
         if wavg_days is not None else (lc_posture or "no data")),
        lc.get("headline") or "Liquidity monitor unavailable.",
        ages["liquidity-capacity"], value=wavg_days, limit=None))

    # -- 5 CONCENTRATION -----------------------------------------------------
    top10 = num(fb_firm.get("top10_concentration_pct"))
    dom_desk = alloc_firm.get("dominant_desk")
    dom_w = None
    for d in (alloc.get("desks") or []):
        nm = d.get("name") or d.get("desk")
        w = num(d.get("weight_pct") or d.get("capital_pct")
                or d.get("alloc_pct") or d.get("weight"))
        if nm and nm == dom_desk and w is not None:
            dom_w = w
        if w is not None and (dom_w is None or w > dom_w):
            dom_w = w
            if not dom_desk:
                dom_desk = nm
    c_sev = 0
    if (top10 is not None and top10 >= TOP10_ALERT) or \
       (dom_w is not None and dom_w >= DESK_ALERT):
        c_sev = 2
    elif (top10 is not None and top10 >= TOP10_WATCH) or \
         (dom_w is not None and dom_w >= DESK_WATCH):
        c_sev = 1
    c_score = max(
        (top10 / TOP10_ALERT * 100.0) if top10 is not None else 0.0,
        (dom_w / DESK_ALERT * 100.0) if dom_w is not None else 0.0)
    dimensions.append(dim(
        "CONCENTRATION", "Concentration", "firm-book + desk-allocator",
        c_sev, c_score,
        ("top-10 %.1f%% of gross, %s %.1f%%"
         % (top10 or 0, dom_desk or "lead desk", dom_w or 0)),
        ("Single-name top-10 watch %.0f%% / alert %.0f%%; dominant-desk "
         "watch %.0f%% / alert %.0f%%."
         % (TOP10_WATCH, TOP10_ALERT, DESK_WATCH, DESK_ALERT)),
        max([a for a in (ages["firm-book"], ages["desk-allocator"])
             if a is not None] or [0]),
        value=top10, limit=TOP10_ALERT))

    # -- 6 EVENT / ARB -------------------------------------------------------
    mar_posture = mar.get("posture")
    mar_sev = sev_from_posture(mar_posture)
    cluster = num(mar_sum.get("cluster_break_pct"))
    sleeve = num(mar_sum.get("sleeve_pct_of_book"))
    mll = mar.get("loss_limits") or {}
    mar_hard = num(mll.get("hard_pct")) or -15.0
    if cluster is not None and mar_hard:
        arb_score = abs(cluster) / abs(mar_hard) * 100.0
    else:
        arb_score = 0 if mar_sev == 0 else 50
    dimensions.append(dim(
        "EVENT_ARB", "Event / Merger-Arb Risk", "merger-arb-risk", mar_sev,
        arb_score,
        ("cluster break %.2f%% of book, sleeve %.1f%%"
         % (cluster or 0, sleeve or 0)
         if cluster is not None else (mar_posture or "no data")),
        mar.get("headline") or "Merger-arb risk monitor unavailable.",
        ages["merger-arb-risk"], value=cluster, limit=mar_hard))

    # -- PERFORMANCE context (can escalate only via drawdown) ---------------
    pnl_warming = (pnl.get("posture") == "WARMING")
    sharpe = num(pnl_firm.get("sharpe"))
    cur_dd = num(pnl_firm.get("current_drawdown_pct"))
    max_dd = num(pnl_firm.get("max_drawdown_pct"))
    perf_sev = 0
    if cur_dd is not None:
        if cur_dd <= DD_HARD:
            perf_sev = 2
        elif cur_dd <= DD_SOFT:
            perf_sev = 1
    if pnl_warming:
        perf_head = "P&L ledger warming (%s obs)" % (
            pnl.get("ledger_observations"))
    elif sharpe is not None:
        perf_head = "Sharpe %.2f, drawdown %.1f%% (max %.1f%%)" % (
            sharpe, cur_dd or 0, max_dd or 0)
    else:
        perf_head = "performance ledger unavailable"
    performance = dim(
        "PERFORMANCE", "Risk-Adjusted Performance", "pnl-attribution",
        perf_sev, (abs(cur_dd) / abs(DD_HARD) * 100.0)
        if cur_dd is not None else 0.0,
        perf_head,
        ("Drawdown soft %.0f%% / hard %.0f%%. Ratios are context; only a "
         "drawdown-limit breach escalates firm posture."
         % (DD_SOFT, DD_HARD)),
        ages["pnl-attribution"], value=cur_dd, limit=DD_HARD)
    dimensions.append(performance)

    # -- FIRM POSTURE -- worst-of escalation --------------------------------
    core = [d for d in dimensions if d["dimension"] != "PERFORMANCE"]
    firm_sev = max([d["severity"] for d in core] + [perf_sev])
    # a hard mandate breach always forces RED
    if rm_sev == 2:
        firm_sev = 2
    firm_posture = POSTURE_WORD[firm_sev]

    n_alert = sum(1 for d in dimensions if d["severity"] == 2)
    n_watch = sum(1 for d in dimensions if d["severity"] == 1)

    # binding constraint = the worst dimension, highest score breaks ties
    ranked = sorted(dimensions, key=lambda d: (-d["severity"], -d["score"]))
    binding = ranked[0]

    # -- LIMIT UTILISATION TABLE --------------------------------------------
    def util_row(label, value, limit, unit):
        u = None
        if value is not None and limit not in (None, 0):
            u = round(abs(value) / abs(limit) * 100.0, 1)
        return {"limit": label, "value": value, "ceiling": limit,
                "unit": unit, "utilization_pct": u}

    utilization = [
        util_row("Mandate risk budget", rm_util, 100.0, "%"),
        util_row("99% 1-day VaR", var99, VAR99_HARD, "% of book"),
        util_row("Worst-case stress P&L", worst_loss, hard_ll, "% of book"),
        util_row("Merger-arb cluster break", cluster, mar_hard,
                 "% of book"),
        util_row("Top-10 single-name", top10, TOP10_ALERT, "% of gross"),
        util_row("Dominant desk weight", dom_w, DESK_ALERT, "% of capital"),
        util_row("Current drawdown", cur_dd, DD_HARD, "% of book"),
    ]
    util_live = [u for u in utilization if u["utilization_pct"] is not None]
    tightest = max(util_live, key=lambda u: u["utilization_pct"]) \
        if util_live else None

    # -- TOP FIRM RISKS -- ranked across engines ----------------------------
    top_risks = []
    if worst_loss is not None:
        top_risks.append({
            "risk": "Tail scenario",
            "detail": "%s costs %.1f%% of book" % (worst_scen, worst_loss),
            "metric_pct": abs(worst_loss),
            "source": "firm-stress"})
    if cluster is not None:
        top_risks.append({
            "risk": "Merger-arb cluster break",
            "detail": "full sleeve break costs %.2f%% of book" % cluster,
            "metric_pct": abs(cluster), "source": "merger-arb-risk"})
    # biggest factor bet
    big_factor = None
    for f in (fr.get("factor_exposures") or []):
        beta = num(f.get("exposure") or f.get("beta")
                   or f.get("net_exposure"))
        fname = f.get("factor") or f.get("name")
        if beta is None or not fname:
            continue
        if fname.upper() == "MKT":
            continue
        if big_factor is None or abs(beta) > abs(big_factor[1]):
            big_factor = (fname, beta)
    if big_factor:
        top_risks.append({
            "risk": "Style factor exposure",
            "detail": "net %s beta %.2f" % (big_factor[0], big_factor[1]),
            "metric_pct": abs(big_factor[1]) * 100.0,
            "source": "factor-risk"})
    if top10 is not None:
        top_risks.append({
            "risk": "Single-name concentration",
            "detail": "top-10 names = %.1f%% of gross" % top10,
            "metric_pct": top10, "source": "firm-book"})
    if dom_w is not None:
        top_risks.append({
            "risk": "Desk concentration",
            "detail": "%s = %.1f%% of firm capital"
                      % (dom_desk or "lead desk", dom_w),
            "metric_pct": dom_w, "source": "desk-allocator"})
    top_risks.sort(key=lambda r: -r["metric_pct"])
    top_risks = top_risks[:6]

    # -- CONSISTENCY CHECKS -------------------------------------------------
    checks = []
    if var99 is not None and worst_loss is not None:
        ok = abs(worst_loss) >= abs(var99)
        checks.append({
            "check": "stress_exceeds_var",
            "ok": ok,
            "note": ("Worst named scenario (%.1f%%) %s the 99%% statistical "
                     "VaR (%.2f%%) -- %s."
                     % (worst_loss, "out-runs" if ok else "is milder than",
                        var99,
                        "tail coverage consistent" if ok else
                        "stress set may be under-capturing the tail"))})
    stale_feeds = [n for n, a in ages.items()
                   if a is not None and a > STALE_HOURS]
    missing_feeds = [n for n, a in ages.items() if a is None]
    checks.append({
        "check": "feeds_fresh",
        "ok": not stale_feeds and not missing_feeds,
        "note": ("All %d engine feeds fresh." % len(ENGINES)) if (
            not stale_feeds and not missing_feeds) else
        ("Stale: %s. Missing: %s."
         % (", ".join(stale_feeds) or "none",
            ", ".join(missing_feeds) or "none"))})
    n_stale = len(stale_feeds) + len(missing_feeds)
    confidence = ("HIGH" if n_stale == 0 else
                  "MEDIUM" if n_stale <= 2 else "LOW")

    # -- HISTORY + DELTAS ---------------------------------------------------
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
        "firm_posture": firm_posture,
        "firm_severity": firm_sev,
        "n_alert": n_alert,
        "n_watch": n_watch,
        "var_99_1d_pct": var99,
        "worst_stress_pct": worst_loss,
        "cluster_break_pct": cluster,
        "mandate_util_pct": rm_util,
        "sharpe": sharpe,
    }
    snapshots = [s for s in snapshots if s.get("date") != today]
    snapshots.append(snapshot)
    snapshots = snapshots[-HIST_CAP:]

    def delta(cur, prev):
        if cur is None or prev is None:
            return None
        return round(cur - prev, 2)

    deltas = {}
    if prior:
        deltas = {
            "vs_date": prior.get("date"),
            "posture_changed": prior.get("firm_posture") != firm_posture,
            "prior_posture": prior.get("firm_posture"),
            "var_99_1d_pct": delta(var99, num(prior.get("var_99_1d_pct"))),
            "worst_stress_pct": delta(worst_loss,
                                      num(prior.get("worst_stress_pct"))),
            "cluster_break_pct": delta(cluster,
                                       num(prior.get("cluster_break_pct"))),
            "mandate_util_pct": delta(rm_util,
                                      num(prior.get("mandate_util_pct"))),
            "n_alert": (n_alert - (prior.get("n_alert") or 0)),
            "n_watch": (n_watch - (prior.get("n_watch") or 0)),
        }

    # -- CRO BRIEF -- deterministic, auditable, zero-cost -------------------
    if firm_sev == 2:
        lead = ("Firm posture RED. The book has breached a hard risk limit "
                "and requires immediate de-risking.")
    elif firm_sev == 1:
        lead = ("Firm posture AMBER. The book is inside hard mandate but at "
                "least one risk axis is elevated and warrants attention.")
    else:
        lead = ("Firm posture GREEN. Every risk axis is inside its limit; "
                "the book is operating normally.")
    bind = ("The binding constraint is %s (%s) -- %s."
            % (binding["label"], binding["status"], binding["headline"]))
    var_line = ("Statistical risk: 99%% 1-day VaR is %.2f%% of book against "
                "a %.1f%% limit; the worst named stress scenario is %.1f%%."
                % (var99, VAR99_HARD, worst_loss)
                if (var99 is not None and worst_loss is not None)
                else "Statistical risk feeds are incomplete this run.")
    if pnl_warming:
        perf_line = ("Performance ledger is still warming -- risk-adjusted "
                     "metrics unlock shortly.")
    elif sharpe is not None:
        perf_line = ("Performance: Sharpe %.2f, current drawdown %.1f%% "
                     "(max %.1f%%)." % (sharpe, cur_dd or 0, max_dd or 0))
    else:
        perf_line = "Performance ledger unavailable this run."
    if prior and deltas.get("posture_changed"):
        chg_line = ("Posture moved %s -> %s since %s."
                    % (deltas["prior_posture"], firm_posture,
                       deltas["vs_date"]))
    elif prior:
        dv = deltas.get("var_99_1d_pct")
        chg_line = ("Posture unchanged since %s; 99%% VaR %s."
                    % (deltas["vs_date"],
                       ("%+.2f pt" % dv) if dv is not None else "flat"))
    else:
        chg_line = "First board snapshot -- no prior comparison yet."
    if confidence != "HIGH":
        conf_line = (" Read with %s confidence: %d engine feed(s) stale or "
                     "missing." % (confidence, n_stale))
    else:
        conf_line = ""
    cro_brief = " ".join([lead, bind, var_line, perf_line,
                          chg_line]) + conf_line

    headline = ("Firm risk posture %s -- %d alert / %d watch across 6 risk "
                "axes. Binding constraint: %s."
                % (firm_posture, n_alert, n_watch, binding["label"]))

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-firm-risk-board",
        "method": "firm_risk_synthesis",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "firm_posture": firm_posture,
        "firm_severity": firm_sev,
        "headline": headline,
        "cro_brief": cro_brief,
        "n_alert": n_alert,
        "n_watch": n_watch,
        "binding_constraint": {
            "dimension": binding["dimension"],
            "label": binding["label"],
            "status": binding["status"],
            "headline": binding["headline"],
        },
        "dimensions": dimensions,
        "limit_utilization": utilization,
        "tightest_limit": tightest,
        "top_firm_risks": top_risks,
        "consistency_checks": checks,
        "confidence": confidence,
        "feed_ages_hours": ages,
        "deltas": deltas,
        "limits": {
            "var99_soft_pct": VAR99_SOFT, "var99_hard_pct": VAR99_HARD,
            "drawdown_soft_pct": DD_SOFT, "drawdown_hard_pct": DD_HARD,
            "top10_watch_pct": TOP10_WATCH, "top10_alert_pct": TOP10_ALERT,
            "desk_watch_pct": DESK_WATCH, "desk_alert_pct": DESK_ALERT,
        },
        "how_to_read": (
            "The firm CRO Risk Board. Six risk axes -- mandate, market "
            "(VaR), tail (stress), liquidity, concentration and event/arb "
            "-- each scored OK / WATCH / ALERT. Firm posture is worst-of: "
            "the firm is as risky as its worst breached limit, and a hard "
            "mandate breach always forces RED. Pure synthesis -- this board "
            "reads the eight risk engines and never re-computes risk."),
        "disclaimer": (
            "Built on a hypothetical research book with no costs, slippage "
            "or financing. Research and education only, not investment "
            "advice."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json")
    s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                  Body=json.dumps(
                      {"schema_version": SCHEMA,
                       "engine": "justhodl-firm-risk-board",
                       "updated_at": now.isoformat(),
                       "snapshots": snapshots},
                      default=str).encode("utf-8"),
                  ContentType="application/json")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "firm_posture": firm_posture,
        "n_alert": n_alert, "n_watch": n_watch,
        "binding": binding["dimension"], "confidence": confidence})}
