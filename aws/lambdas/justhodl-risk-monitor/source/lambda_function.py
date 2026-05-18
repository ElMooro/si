"""
justhodl-risk-monitor -- the firm Risk Mandate Monitor.
=======================================================
WHY THIS EXISTS
---------------
A multi-strategy fund does not let the desks and the allocator run
unsupervised. It runs an independent RISK function -- a risk committee
that sets hard MANDATE limits (max gross, net band, single-name cap,
sector cap, concentration cap, minimum diversification, per-desk
drawdown stop) and checks the firm book against them every single day.
At Millennium / Citadel / Point72 risk does not pick trades; it sets the
boundaries the trades must live inside, and it can cut a desk that
breaches them. The platform built the seven desks, the allocator, the
return feed and the consolidated firm book -- but nothing yet POLICES
that book against a mandate.

This engine is that risk department. It reads the consolidated firm
book, the allocator's desk weights and the desk-return feed, scores the
book against a documented institutional mandate, and publishes one
firm RISK POSTURE: GREEN (inside mandate), AMBER (a limit is within 80%
of its ceiling -- a WATCH), or RED (a hard limit is BREACHED).

THE MANDATE  (institutional defaults -- tunable; a real risk committee
owns these numbers)
  gross ceiling          total exposure, net of cross-desk offsets
  net exposure band      directional tilt kept inside a hard band
  single-name cap        no one ticker above N% of capital
  sector cap             no one sector above N% net
  top-10 concentration   the ten largest names capped as a share of gross
  desk capital cap       no one desk above N% of firm capital
  diversification floor  the allocator's diversification ratio kept above
  desk drawdown stop     per-desk peak-to-trough -- the de-risk trigger;
                         dormant (WARMING) until the return feed has the
                         history to measure a drawdown
  conflict watch         count of names one desk is long and another short

Reads only existing S3 sidecars. Pure synthesis, no external API. The
risk overlay above the firm book; distinct from portfolio-risk, which
scores the user's actual portfolio.

OUTPUT   data/risk-monitor.json          SCHEDULE  daily 01:30 UTC
"""
import json
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/risk-monitor.json"
FIRM_KEY = "data/firm-book.json"
ALLOC_KEY = "data/desk-allocator.json"
RETURNS_KEY = "data/desk-returns.json"
SCHEMA = "1.0"

s3 = boto3.client("s3", region_name="us-east-1")

EPS = 1e-9
WATCH_FRAC = 0.80          # >= 80% of a limit -> WATCH (AMBER)
DD_MIN_OBS = 5             # daily return obs before a drawdown is measurable

# ---- THE MANDATE -----------------------------------------------------------
# Institutional defaults. The book deploys ~100% of notional capital across
# seven desks, so the binding risks here are concentration and net tilt, not
# leverage. A risk committee would own and periodically re-set these.
MANDATE = {
    "gross_ceiling_pct": 140.0,
    "net_band_pct": [-40.0, 75.0],
    "single_name_cap_pct": 8.0,
    "sector_cap_pct": 35.0,
    "top10_concentration_cap_pct": 50.0,
    "desk_capital_cap_pct": 45.0,
    "diversification_ratio_floor": 1.40,
    "desk_drawdown_stop_pct": -10.0,
    "desk_conflict_watch_count": 10,
}


def get_json(key):
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def status_for(util):
    """Map a 0..n utilization fraction to a status string."""
    if util > 1.0 + 1e-9:
        return "BREACH"
    if util >= WATCH_FRAC:
        return "WATCH"
    return "OK"


def ceiling_limit(name, current, ceiling, unit, note):
    """A one-sided ceiling limit: current must stay at or below ceiling."""
    util = (abs(current) / ceiling) if ceiling > EPS else 0.0
    st = status_for(util)
    return {
        "limit": name, "current": round(current, 3), "ceiling": ceiling,
        "unit": unit, "utilization_pct": round(util * 100, 1),
        "headroom": round(ceiling - abs(current), 3),
        "status": st, "detail": note,
    }


def floor_limit(name, current, floor, unit, note):
    """A floor limit: current must stay at or above floor.

    Utilization is how close current is to the floor from above; at the
    floor utilization is 1.0, well above it utilization is small.
    """
    if current is None:
        return {"limit": name, "current": None, "floor": floor,
                "unit": unit, "utilization_pct": None, "status": "WARMING",
                "detail": note + " (input unavailable)"}
    if current <= floor:
        util = 1.01
    elif current >= 2 * floor:
        util = 0.5
    else:
        # between floor and 2x floor -> util 1.0 down to 0.5
        util = 1.0 - 0.5 * ((current - floor) / floor)
    st = status_for(util)
    return {
        "limit": name, "current": round(current, 3), "floor": floor,
        "unit": unit, "utilization_pct": round(util * 100, 1),
        "headroom": round(current - floor, 3),
        "status": st, "detail": note,
    }


def band_limit(name, current, lo, hi, unit, note):
    """A two-sided band: current must stay inside [lo, hi]."""
    if current is None:
        return {"limit": name, "current": None, "band": [lo, hi],
                "unit": unit, "utilization_pct": None, "status": "WARMING",
                "detail": note}
    if current > hi or current < lo:
        util = 1.01
    elif current >= 0:
        util = current / hi if hi > EPS else 0.0
    else:
        util = current / lo if lo < -EPS else 0.0
    st = status_for(util)
    near = hi if current >= 0 else lo
    return {
        "limit": name, "current": round(current, 3), "band": [lo, hi],
        "unit": unit, "utilization_pct": round(util * 100, 1),
        "headroom": round(abs(near - current), 3),
        "status": st, "detail": note,
    }


def max_drawdown(returns):
    """Peak-to-trough drawdown of a daily-return series, as a percent.

    returns: list of {ret: float}. Builds the equity curve by compounding
    and tracks the worst decline from a running peak. Returns (dd_pct, n).
    """
    rets = [r.get("ret") for r in returns
            if isinstance(r, dict) and isinstance(r.get("ret"), (int, float))]
    if len(rets) < DD_MIN_OBS:
        return None, len(rets)
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for r in rets:
        equity *= (1.0 + r)
        peak = max(peak, equity)
        dd = equity / peak - 1.0
        worst = min(worst, dd)
    return worst * 100.0, len(rets)


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    firm = get_json(FIRM_KEY)
    alloc = get_json(ALLOC_KEY)
    returns = get_json(RETURNS_KEY)

    inputs_ok = {
        "firm_book": firm is not None,
        "desk_allocator": alloc is not None,
        "desk_returns": returns is not None,
    }
    if firm is None:
        out = {"schema_version": SCHEMA, "engine": "justhodl-risk-monitor",
               "generated_at": now.isoformat(), "ok": False,
               "error": "firm-book.json unavailable - cannot score the "
                         "mandate without the consolidated book",
               "inputs_available": inputs_ok}
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out).encode("utf-8"),
                      ContentType="application/json")
        return {"statusCode": 200,
                "body": json.dumps({"ok": False, "reason": "no firm-book"})}

    fm = firm.get("firm") or {}
    equity_book = firm.get("equity_book") or []
    sector_exposure = firm.get("sector_exposure") or []
    desk_conflicts = firm.get("desk_conflicts") or []

    af = (alloc or {}).get("firm") or {}
    alloc_desks = (alloc or {}).get("desks") or []
    ret_desks = (returns or {}).get("desks") or {}

    limits = []

    # ---- 1) gross exposure ceiling ----
    gross = fm.get("gross_exposure_pct")
    if isinstance(gross, (int, float)):
        limits.append(ceiling_limit(
            "Gross exposure", gross, MANDATE["gross_ceiling_pct"], "%",
            "Total firm exposure, long plus short, net of cross-desk "
            "offsets. The book deploys roughly one turn of capital; the "
            "ceiling leaves room for the macro sleeve and the short book."))

    # ---- 2) net exposure band ----
    net = fm.get("net_exposure_pct")
    if isinstance(net, (int, float)):
        lo, hi = MANDATE["net_band_pct"]
        limits.append(band_limit(
            "Net exposure", net, lo, hi, "%",
            "Directional tilt of the firm. The mandate keeps the book "
            "long-biased but bounded - never a pure beta vehicle, never "
            "net short."))

    # ---- 3) single-name concentration ----
    sn_cap = MANDATE["single_name_cap_pct"]
    sized_names = [b for b in equity_book
                   if isinstance(b.get("net_pct"), (int, float))]
    sn_breaches = sorted(
        [{"symbol": b.get("symbol"), "name": b.get("name"),
          "sector": b.get("sector"), "side": b.get("side"),
          "net_pct": round(b.get("net_pct"), 3),
          "desks": list((b.get("desks") or {}).keys())}
         for b in sized_names if abs(b.get("net_pct")) > sn_cap],
        key=lambda x: -abs(x["net_pct"]))
    max_name = max((abs(b["net_pct"]) for b in sized_names), default=0.0)
    biggest = max(sized_names, key=lambda b: abs(b.get("net_pct")),
                  default=None)
    sn_detail = "No single name above the cap."
    if sn_breaches:
        sn_detail = ("%d name(s) above the %.0f%% single-name cap - "
                     "largest %s at %.1f%%."
                     % (len(sn_breaches), sn_cap,
                        sn_breaches[0]["symbol"], sn_breaches[0]["net_pct"]))
    sn_row = ceiling_limit("Single-name concentration", max_name, sn_cap,
                           "% of capital", sn_detail)
    sn_row["largest_name"] = (biggest.get("symbol") if biggest else None)
    sn_row["n_breaches"] = len(sn_breaches)
    limits.append(sn_row)

    # ---- 4) sector concentration ----
    sec_cap = MANDATE["sector_cap_pct"]
    sec_rows = []
    for s in sector_exposure:
        nm = s.get("sector") or s.get("name") or s.get("label")
        npc = s.get("net_pct")
        if nm and isinstance(npc, (int, float)):
            sec_rows.append((nm, npc))
    sec_breaches = sorted(
        [{"sector": nm, "net_pct": round(npc, 3)}
         for nm, npc in sec_rows if abs(npc) > sec_cap],
        key=lambda x: -abs(x["net_pct"]))
    max_sector = max((abs(npc) for _, npc in sec_rows), default=0.0)
    top_sector = max(sec_rows, key=lambda x: abs(x[1]), default=(None, 0.0))
    sec_detail = "No sector above the cap."
    if sec_breaches:
        sec_detail = ("%d sector(s) above the %.0f%% cap - largest %s at "
                      "%.1f%%." % (len(sec_breaches), sec_cap,
                                   sec_breaches[0]["sector"],
                                   sec_breaches[0]["net_pct"]))
    sec_row = ceiling_limit("Sector concentration", max_sector, sec_cap,
                            "% net", sec_detail)
    sec_row["largest_sector"] = top_sector[0]
    sec_row["n_breaches"] = len(sec_breaches)
    limits.append(sec_row)

    # ---- 5) top-10 concentration ----
    top10 = fm.get("top10_concentration_pct")
    if isinstance(top10, (int, float)):
        limits.append(ceiling_limit(
            "Top-10 concentration", top10,
            MANDATE["top10_concentration_cap_pct"], "% of gross",
            "Share of gross exposure carried by the ten largest names - "
            "guards against the book hiding in a handful of positions."))

    # ---- 6) desk capital concentration ----
    desk_w = [(d.get("name"), d.get("capital_weight_pct"))
              for d in alloc_desks
              if isinstance(d.get("capital_weight_pct"), (int, float))]
    if desk_w:
        top_desk = max(desk_w, key=lambda x: x[1])
        dc_row = ceiling_limit(
            "Desk capital concentration", top_desk[1],
            MANDATE["desk_capital_cap_pct"], "% of capital",
            "Largest single desk's share of firm capital (%s). The "
            "allocator enforces this cap; the monitor confirms it held."
            % top_desk[0])
        dc_row["largest_desk"] = top_desk[0]
        limits.append(dc_row)

    # ---- 7) diversification ratio floor ----
    div = af.get("diversification_ratio")
    limits.append(floor_limit(
        "Diversification ratio", div if isinstance(div, (int, float))
        else None, MANDATE["diversification_ratio_floor"], "ratio",
        "How much the seven-desk structure cuts firm volatility below "
        "the weighted average of the desks standing alone. A floor keeps "
        "the multi-desk book genuinely decorrelated."))

    # ---- 8) per-desk drawdown stop ----
    dd_cap = MANDATE["desk_drawdown_stop_pct"]
    desk_dd = []
    worst_dd = 0.0
    measured = 0
    for key, dv in ret_desks.items():
        series = (dv or {}).get("returns") or []
        dd, n = max_drawdown(series)
        row = {"desk": key, "observations": n}
        if dd is None:
            row["drawdown_pct"] = None
            row["status"] = "WARMING"
        else:
            row["drawdown_pct"] = round(dd, 2)
            row["status"] = "BREACH" if dd <= dd_cap else (
                "WATCH" if dd <= dd_cap * WATCH_FRAC else "OK")
            worst_dd = min(worst_dd, dd)
            measured += 1
        desk_dd.append(row)
    desk_dd.sort(key=lambda r: (r["drawdown_pct"] is None,
                                r["drawdown_pct"] or 0.0))
    if measured == 0:
        dd_row = {"limit": "Desk drawdown stop", "current": None,
                  "ceiling": dd_cap, "unit": "% peak-to-trough",
                  "utilization_pct": None, "status": "WARMING",
                  "detail": "The desk-return feed is still seeding - a "
                            "drawdown needs return history to measure. "
                            "Activates automatically as the feed warms."}
    else:
        util = abs(worst_dd) / abs(dd_cap) if abs(dd_cap) > EPS else 0.0
        dd_row = {"limit": "Desk drawdown stop", "current": round(worst_dd, 2),
                  "ceiling": dd_cap, "unit": "% peak-to-trough",
                  "utilization_pct": round(util * 100, 1),
                  "status": status_for(util),
                  "detail": "Worst peak-to-trough decline across the desks "
                            "with measurable return history (%d of %d)."
                            % (measured, len(ret_desks))}
    limits.append(dd_row)

    # ---- 9) desk-conflict watch ----
    n_conf = len(desk_conflicts)
    conf_cap = MANDATE["desk_conflict_watch_count"]
    conf_util = n_conf / conf_cap if conf_cap > 0 else 0.0
    conf_row = {
        "limit": "Cross-desk conflicts", "current": n_conf,
        "ceiling": conf_cap, "unit": "names",
        "utilization_pct": round(conf_util * 100, 1),
        "status": status_for(conf_util),
        "detail": "Names one desk is long and another is short. A few are "
                  "healthy - different desks, different views - but a "
                  "rising count means the desks are fighting each other.",
    }
    limits.append(conf_row)

    # ---- firm risk posture -----------------------------------------------
    hard = [lm for lm in limits if lm["status"] != "WARMING"]
    breaches = [lm for lm in hard if lm["status"] == "BREACH"]
    watches = [lm for lm in hard if lm["status"] == "WATCH"]
    warming = [lm for lm in limits if lm["status"] == "WARMING"]

    if breaches:
        posture, color = "RED", "#ff5577"
    elif watches:
        posture, color = "AMBER", "#ffd266"
    else:
        posture, color = "GREEN", "#26ffaf"

    utils = [lm["utilization_pct"] for lm in hard
             if isinstance(lm.get("utilization_pct"), (int, float))]
    risk_budget = round(sum(utils) / len(utils), 1) if utils else None

    if posture == "RED":
        headline = ("RISK POSTURE RED - %d hard limit(s) breached: %s. The "
                    "book is outside mandate and needs to be brought back "
                    "inside." % (len(breaches),
                                 ", ".join(b["limit"] for b in breaches)))
    elif posture == "AMBER":
        headline = ("RISK POSTURE AMBER - inside mandate but %d limit(s) on "
                    "WATCH (within %d%% of the ceiling): %s."
                    % (len(watches), int(WATCH_FRAC * 100),
                       ", ".join(w["limit"] for w in watches)))
    else:
        headline = ("RISK POSTURE GREEN - the firm book is inside every "
                    "hard mandate limit. Risk-budget utilization %s%%."
                    % (risk_budget if risk_budget is not None else "n/a"))

    payload = {
        "schema_version": SCHEMA,
        "engine": "justhodl-risk-monitor",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "headline": headline,
        "risk_posture": posture,
        "posture_color": color,
        "risk_budget_utilization_pct": risk_budget,
        "n_breaches": len(breaches),
        "n_watches": len(watches),
        "n_warming": len(warming),
        "limits": limits,
        "breaches": [b["limit"] for b in breaches],
        "watches": [w["limit"] for w in watches],
        "single_name_breaches": sn_breaches,
        "sector_breaches": sec_breaches,
        "desk_drawdowns": desk_dd,
        "mandate": MANDATE,
        "inputs_available": inputs_ok,
        "firm_snapshot": {
            "gross_exposure_pct": fm.get("gross_exposure_pct"),
            "net_exposure_pct": fm.get("net_exposure_pct"),
            "long_short_ratio": fm.get("long_short_ratio"),
            "n_equity_names": fm.get("n_equity_names"),
            "top10_concentration_pct": fm.get("top10_concentration_pct"),
            "diversification_ratio": af.get("diversification_ratio"),
        },
        "how_to_read": (
            "This is the firm's risk department. Every limit is a hard "
            "mandate boundary the book must live inside. GREEN means the "
            "book is fully inside mandate; AMBER means a limit is within "
            "80% of its ceiling and is on WATCH; RED means a hard limit is "
            "BREACHED and the book must be brought back inside. The "
            "single-name and sector breach lists name exactly what is "
            "over the line. The desk drawdown stop is the de-risk trigger "
            "- it shows WARMING until the desk-return feed has the history "
            "to measure a peak-to-trough decline."),
        "methodology": (
            "Reads the consolidated firm book (justhodl-firm-book), the "
            "Desk Allocator's desk weights and diversification ratio, and "
            "the desk-return feed. Each mandate limit is scored as "
            "current / ceiling utilization - at or above 80% is a WATCH, "
            "over 100% is a BREACH; the net-exposure limit is a two-sided "
            "band and the diversification ratio is a floor. Firm posture "
            "is RED on any breach, AMBER on any watch, else GREEN. The "
            "mandate numbers are institutional defaults and are meant to "
            "be owned and tuned by the user as a risk committee would."),
        "disclaimer": (
            "Research and education only - not investment advice. The "
            "mandate limits are a risk-budgeting framework, not a "
            "directive, and apply to the model desk book, not the user's "
            "actual portfolio."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")

    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "risk_posture": posture,
                                "n_breaches": len(breaches),
                                "n_watches": len(watches),
                                "risk_budget_pct": risk_budget})}
