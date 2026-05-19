"""justhodl-cro-escalation -- the intraday firm-risk tripwire.

The CRO Morning Brief (justhodl-cro-digest) sets the day's risk
baseline at 12:15 UTC. The risk stack itself is a batch that runs
once overnight -- so re-reading it at midday tells you nothing. What
moves intraday is the tape. This engine is the tripwire that watches
it.

Through the US session it runs four times and asks one question:
has the firm's risk picture deteriorated since the morning brief?

It checks two things:

  * THE TAPE -- a small set of cross-asset gauges (SPY, QQQ, HYG, TLT,
    IWM and VIX) read live from FMP. A lone equity dip is noise; an
    equity sell-off WITH a vol bid, a credit air-pocket, or a pure vol
    explosion is signal. Moves are scored against a documented
    tripwire ladder -- WATCH / ALERT / SEVERE.
  * STACK DRIFT -- if a risk engine has re-run intraday and the Firm
    Risk Board now shows a worse posture, or the Hedge Planner action
    has flipped off HOLD, that is an escalation on its own.

It escalates -- one sharp Telegram ping -- ONLY when the severity is
strictly worse than anything already escalated today. First break
pings; a genuine second leg down pings again; a quiet re-run is
silent. State resets each morning with the new baseline. Most days it
sends nothing -- that silence is the product.

event.dry_run assembles without sending. event.simulate injects a
synthetic tape for verification. It places no orders and re-computes
no risk engine -- it is a monitor over the stack the brief reported.
"""
import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cro-escalation.json"
SCHEMA = "1.0"
RISK_DESK_URL = "https://justhodl.ai/risk-desk.html"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
FMP_KEY = os.environ.get("FMP_KEY", "")

# Tape gauges. SPY/QQQ/HYG/TLT/IWM quote cleanly on FMP /stable; VIX is
# tried as ^VIX, then VIXY (the short-term VIX-futures ETF) as a proxy.
GAUGES = ["SPY", "QQQ", "HYG", "TLT", "IWM"]

# ---- intraday tripwire ladder --------------------------------------------
# Cross-confirmed by design. L1/L2 equity tripwires require a vol bid
# alongside the sell-off -- a lone equity dip does not trip. Credit
# (HYG) and a pure vol explosion can trip alone: they are rarer and
# more dispositive. Levels are day-move percentages unless noted.
L1_SPY, L1_VIXUP = -1.0, 8.0          # WATCH: SPY down AND vol bid
L1_HYG = -1.0                         #   or credit air-pocket alone
L2_SPY, L2_VIXUP = -2.0, 15.0         # ALERT
L2_HYG = -1.8
L2_VIX_LVL, L2_VIX_SOLO = 28.0, 30.0  #   or VIX>=28 AND up>=30% (pure vol)
L3_SPY = -3.5                         # SEVERE
L3_HYG = -3.0
L3_VIX_LVL, L3_VIX_SOLO = 35.0, 50.0

SEV_LABEL = {0: "CLEAR", 1: "WATCH", 2: "ALERT", 3: "SEVERE"}
POSTURE_RANK = {"GREEN": 0, "AMBER": 1, "RED": 2}

# Mirror of the Hedge Planner's sleeve classes -- so the escalation can
# name the exact leg the intraday move implies, in the planner's own
# vocabulary. "vix_leg" is which leg is the VIX-call leg (harvestable
# into a vol spike), or None if the class carries no VIX leg.
SLEEVE_CLASS_LEGS = {
    "EQUITY_CRASH":    {"primary": "SPY put spread", "convex": "VIX calls",
                        "vix_leg": "convex"},
    "VOL_SPIKE":       {"primary": "VIX calls", "convex": "SPY put spread",
                        "vix_leg": "primary"},
    "MOMENTUM_UNWIND": {"primary": "MTUM puts",
                        "convex": "VIX calls + crowded-long trim",
                        "vix_leg": "convex"},
    "CREDIT_EVENT":    {"primary": "HYG puts", "convex": "IWM put spread",
                        "vix_leg": None},
    "RATES_SHOCK":     {"primary": "TLT puts",
                        "convex": "2s10s curve steepener", "vix_leg": None},
}
# a VIX up-move past this makes the VIX-call leg richly in-the-money --
# the move stops being "add protection" and becomes "harvest the spike".
VIX_HARVEST_PCT = 25.0

# emojis as unicode escapes -- source stays pure ASCII
SIREN = "\U0001F6A8"
WARN = "\u26A0"
DOWN = "\u25BC"
ARROW = "\u2192"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


# --------------------------------------------------------------------------
def get_chat_id():
    try:
        return ssm.get_parameter(
            Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception:
        return os.environ.get("CHAT_ID", "")


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def num(v):
    try:
        return None if v is None else float(v)
    except Exception:
        return None


def h(s):
    return (str("" if s is None else s)
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def fmp_quote(symbol):
    """Live quote via FMP /stable. Returns (price, day_pct) or (None, None)."""
    if not FMP_KEY:
        return None, None
    url = ("https://financialmodelingprep.com/stable/quote?symbol=%s"
           "&apikey=%s" % (urllib.parse.quote(symbol), FMP_KEY))
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-cro-escalation/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            row = data[0]
            price = num(row.get("price"))
            pct = num(row.get("changePercentage"))
            if pct is None:
                pct = num(row.get("changesPercentage"))
            return price, pct
    except Exception as e:
        print("quote %s fail: %s" % (symbol, e))
    return None, None


def read_tape(simulate):
    """Live cross-asset tape, or a synthetic one when simulate is set."""
    tape = {}
    if isinstance(simulate, dict):
        for g in GAUGES:
            tape[g] = {"price": None,
                       "day_pct": num(simulate.get(g.lower() + "_pct"))}
        tape["VIX"] = {"price": num(simulate.get("vix")),
                       "day_pct": num(simulate.get("vix_pct")),
                       "source": "simulated"}
        tape["_simulated"] = True
        return tape
    for g in GAUGES:
        p, pct = fmp_quote(g)
        tape[g] = {"price": p, "day_pct": pct}
    # VIX: prefer the index, fall back to the VIXY proxy for the % move
    vp, vpct = fmp_quote("^VIX")
    if vp is not None:
        tape["VIX"] = {"price": vp, "day_pct": vpct, "source": "^VIX"}
    else:
        yp, ypct = fmp_quote("VIXY")
        tape["VIX"] = {"price": None, "day_pct": ypct,
                       "source": "VIXY-proxy" if ypct is not None
                       else "unavailable"}
    tape["_simulated"] = False
    return tape


# --------------------------------------------------------------------------
def score_tape(tape):
    """Map the live tape to an intraday severity 0-3 + tripped reasons."""
    spy = tape.get("SPY", {}).get("day_pct")
    hyg = tape.get("HYG", {}).get("day_pct")
    vix = tape.get("VIX", {})
    vix_pct = vix.get("day_pct")
    vix_lvl = vix.get("price")
    sev, trips = 0, []

    def vix_up(thr):
        return vix_pct is not None and vix_pct >= thr

    # ---- SEVERE -----------------------------------------------------------
    if spy is not None and spy <= L3_SPY:
        sev = 3
        trips.append("SPY %.1f%% intraday -- a crash-scale move" % spy)
    if hyg is not None and hyg <= L3_HYG:
        sev = 3
        trips.append("HYG %.1f%% -- a severe credit air-pocket" % hyg)
    if vix_lvl is not None and vix_lvl >= L3_VIX_LVL and vix_up(L3_VIX_SOLO):
        sev = 3
        trips.append("VIX +%.0f%% to %.1f -- a vol explosion"
                     % (vix_pct, vix_lvl))
    # ---- ALERT ------------------------------------------------------------
    if sev < 2:
        if spy is not None and spy <= L2_SPY and vix_up(L2_VIXUP):
            sev = 2
            trips.append("SPY %.1f%% with VIX +%.0f%% -- equity sell-off "
                         "confirmed by a vol bid" % (spy, vix_pct))
        if hyg is not None and hyg <= L2_HYG:
            sev = max(sev, 2)
            trips.append("HYG %.1f%% -- credit is leading risk lower" % hyg)
        if vix_lvl is not None and vix_lvl >= L2_VIX_LVL \
                and vix_up(L2_VIX_SOLO):
            sev = max(sev, 2)
            trips.append("VIX +%.0f%% to %.1f -- a standalone vol event"
                         % (vix_pct, vix_lvl))
    # ---- WATCH ------------------------------------------------------------
    if sev < 1:
        if spy is not None and spy <= L1_SPY and vix_up(L1_VIXUP):
            sev = 1
            trips.append("SPY %.1f%% with VIX +%.0f%% -- the tape is "
                         "softening" % (spy, vix_pct))
        if hyg is not None and hyg <= L1_HYG:
            sev = max(sev, 1)
            trips.append("HYG %.1f%% -- a credit wobble" % hyg)
    return sev, trips


def score_drift(board, planner, base_posture, base_action):
    """Escalation from a risk engine having re-run to a worse state."""
    sev, trips = 0, []
    if isinstance(board, dict):
        cur = (board.get("firm_posture") or "").upper()
        if cur and base_posture and cur in POSTURE_RANK \
                and base_posture in POSTURE_RANK \
                and POSTURE_RANK[cur] > POSTURE_RANK[base_posture]:
            sev = max(sev, 2 if cur == "AMBER" else 3)
            trips.append("Firm Risk Board re-ran intraday: posture %s %s %s"
                         % (base_posture, ARROW, cur))
    if isinstance(planner, dict):
        cur_a = (planner.get("action") or "").upper()
        if cur_a and base_action and cur_a != base_action \
                and cur_a not in ("HOLD", "NONE"):
            sev = max(sev, 2)
            trips.append("Hedge Planner action flipped %s %s %s -- a hedge "
                         "trade is now on the ticket"
                         % (base_action, ARROW, cur_a))
    return sev, trips


# --------------------------------------------------------------------------
def classify_tape_scenario(tape):
    """Which Hedge Planner sleeve class the live intraday move resembles.

    A priority ladder, deliberately simple and robust: the gauge that is
    most clearly leading the stress decides the class.
    """
    def g(sym):
        return (tape.get(sym) or {}).get("day_pct")
    spy, hyg, tlt = g("SPY"), g("HYG"), g("TLT")
    qqq, iwm, vix = g("QQQ"), g("IWM"), g("VIX")
    if hyg is not None and hyg <= -1.5:
        return "CREDIT_EVENT"
    if tlt is not None and tlt <= -2.0:
        return "RATES_SHOCK"
    if vix is not None and vix >= 35.0 and (spy is None or spy > -2.5):
        return "VOL_SPIKE"
    if spy is not None:
        growth = [x for x in (qqq, iwm) if x is not None]
        if growth and min(growth) <= spy - 1.5:
            return "MOMENTUM_UNWIND"
    return "EQUITY_CRASH"


def hedge_implication(severity, tape, standing_class, standing_budget,
                      hedge_required):
    """The specific sleeve adjustment an ALERT/SEVERE move implies.

    Not a worked ticket -- a directional read in the Hedge Planner's own
    vocabulary (OPEN / ADD / HARVEST / SWITCH-REVIEW). Returns None below
    ALERT: a watch-grade wobble does not warrant re-hedging.
    """
    if severity < 2:
        return None
    tape_class = classify_tape_scenario(tape)
    vix_pct = (tape.get("VIX") or {}).get("day_pct")
    legs = SLEEVE_CLASS_LEGS.get(standing_class or "", {})
    boundary = ("The Hedge Planner produces the worked ticket -- strikes "
                "and contracts -- at its next 05:00 UTC run; this is the "
                "intraday directional read, not a ticket.")

    # no protection on the book
    if not hedge_required or not standing_class or (standing_budget or 0) <= 0:
        return {
            "recommended_action": "OPEN",
            "tape_scenario_class": tape_class,
            "standing_sleeve_class": standing_class,
            "sleeve_class_match": False,
            "target_leg": SLEEVE_CLASS_LEGS.get(tape_class, {}).get(
                "primary"),
            "rationale": ("No protective sleeve is on the book and a %s-grade "
                          "move is underway intraday. The read is OPEN -- "
                          "stand up a %s sleeve; this warrants an off-cycle "
                          "review rather than waiting for the 05:00 UTC run."
                          % (tape_class, tape_class)),
            "boundary": boundary,
        }

    class_match = tape_class == standing_class
    # the sleeve is built for a different stress than the tape is showing
    if not class_match:
        return {
            "recommended_action": "SWITCH_REVIEW",
            "tape_scenario_class": tape_class,
            "standing_sleeve_class": standing_class,
            "sleeve_class_match": False,
            "target_leg": None,
            "rationale": ("Live stress is %s-led but the standing sleeve is "
                          "built for %s (%s). The binding scenario class may "
                          "be rotating -- the read is REVIEW FOR SWITCH; the "
                          "Hedge Planner re-derives the class at 05:00 UTC."
                          % (tape_class, standing_class,
                             legs.get("primary", "n/a"))),
            "boundary": boundary,
        }

    # right class -- harvest the convex VIX leg if vol has spiked, else add
    vix_leg_key = legs.get("vix_leg")
    if vix_leg_key and vix_pct is not None and vix_pct >= VIX_HARVEST_PCT:
        leg_name = legs.get(vix_leg_key)
        return {
            "recommended_action": "HARVEST",
            "tape_scenario_class": tape_class,
            "standing_sleeve_class": standing_class,
            "sleeve_class_match": True,
            "target_leg": leg_name,
            "rationale": ("VIX +%.0f%% -- the %s leg of the standing %s "
                          "sleeve is now richly in-the-money. The read is "
                          "HARVEST: monetise that leg into the spike and "
                          "recycle the premium into fresh OTM protection."
                          % (vix_pct, leg_name, standing_class)),
            "boundary": boundary,
        }
    return {
        "recommended_action": "ADD",
        "tape_scenario_class": tape_class,
        "standing_sleeve_class": standing_class,
        "sleeve_class_match": True,
        "target_leg": legs.get("primary"),
        "rationale": ("The standing %s sleeve (%s) is the right class for "
                      "this move, but the tape has eaten into the protection "
                      "budget. The read is ADD -- scale the primary leg "
                      "toward the upper end of the sleeve budget."
                      % (standing_class, legs.get("primary", "n/a"))),
        "boundary": boundary,
    }


def build_message(now, severity, trips, base, tape, simulate,
                   implication=None):
    icon = SIREN if severity >= 3 else WARN
    lines = []
    tag = "[VERIFICATION TEST] " if simulate else ""
    lines.append("%s <b>%sCRO RISK ESCALATION</b>" % (icon, tag))
    lines.append("<i>%s UTC &middot; intraday tripwire</i>"
                 % now.strftime("%H:%M"))
    lines.append("")
    lines.append("<b>Severity: %s</b>" % SEV_LABEL.get(severity, "?"))
    lines.append("The firm risk picture has deteriorated since this "
                 "morning's brief.")
    lines.append("")
    lines.append("<b>What tripped:</b>")
    for t in trips[:6]:
        lines.append("%s %s" % (WARN, h(t)))
    lines.append("")
    bp = base.get("firm_posture") or "n/a"
    ba = base.get("hedge_action") or "n/a"
    lines.append("Morning brief: posture <b>%s</b> &middot; hedge <b>%s</b>"
                 % (h(bp), h(ba)))
    ws = base.get("worst_scenario")
    if ws:
        lines.append("The tape is moving toward the <b>%s</b> scenario the "
                     "tail sleeve is sized for." % h(ws))
    lines.append("")
    if implication:
        tgt = implication.get("target_leg")
        lines.append("<b>Hedge implication: %s%s</b>"
                     % (h(implication.get("recommended_action")),
                        (" -- " + h(tgt)) if tgt else ""))
        lines.append(h(implication.get("rationale")))
        if implication.get("boundary"):
            lines.append("<i>%s</i>" % h(implication["boundary"]))
        lines.append("")
    else:
        lines.append("%s Review the standing hedge sleeve and the Risk Desk."
                     % ARROW)
    lines.append("Risk Desk cockpit: %s" % RISK_DESK_URL)
    lines.append("<i>Hypothetical research book. Research and education "
                 "only, not investment advice.</i>")
    return "\n".join(lines)


def send_telegram(chat_id, text):
    if not TELEGRAM_TOKEN:
        return False, "missing_token", None
    if not chat_id:
        return False, "missing_chat_id", None
    api = "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN
    payload = {"chat_id": chat_id, "text": text[:4096],
               "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        req = urllib.request.Request(
            api, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode())
        return True, "sent", (body.get("result") or {}).get("message_id")
    except urllib.error.HTTPError as e:
        print("HTML send failed: %s" % e)
        try:
            import re
            plain = re.sub(r"<[^>]+>", "", text)
            p2 = {"chat_id": chat_id, "text": plain[:4096],
                  "disable_web_page_preview": True}
            req2 = urllib.request.Request(
                api, data=json.dumps(p2).encode(),
                headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req2, timeout=15) as resp:
                body = json.loads(resp.read().decode())
            return True, "sent_plain_fallback", (
                body.get("result") or {}).get("message_id")
        except Exception as e2:
            return False, "plain_retry_fail: %s" % e2, None
    except Exception as e:
        return False, str(e), None


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    event = event or {}
    dry_run = bool(event.get("dry_run"))
    simulate = event.get("simulate")

    # ---- morning baseline ------------------------------------------------
    digest = read_json("data/cro-digest.json") or {}
    th = read_json("data/tail-hedge.json") or {}
    base = {
        "date": (digest.get("generated_at") or "")[:10],
        "firm_posture": (digest.get("firm_posture") or "UNKNOWN").upper(),
        "hedge_action": (digest.get("hedge_action") or "NONE").upper(),
        "worst_scenario": (th.get("tail_exposure") or {}).get(
            "worst_scenario"),
    }
    baseline_today = base["date"] == today

    # ---- the tape + stack drift -----------------------------------------
    tape = read_tape(simulate)
    tape_sev, tape_trips = score_tape(tape)
    board = read_json("data/firm-risk-board.json")
    planner = read_json("data/hedge-planner.json")
    drift_sev, drift_trips = score_drift(
        board, planner, base["firm_posture"], base["hedge_action"])

    severity = max(tape_sev, drift_sev)
    trips = drift_trips + tape_trips

    # ---- the specific hedge-sleeve adjustment the move implies -----------
    # populated at ALERT/SEVERE; reads the standing sleeve off the planner.
    pl = planner if isinstance(planner, dict) else {}
    standing = pl.get("standing_sleeve_after") or {}
    implication = hedge_implication(
        severity, tape,
        standing.get("scenario_class") or pl.get("scenario_class"),
        num(standing.get("budget_pct_of_book")),
        bool(pl.get("hedge_required")))

    # ---- per-day escalation state ---------------------------------------
    prior = read_json(OUT_KEY) or {}
    day_state = prior.get("day_state") or {}
    if day_state.get("date") != today:
        day_state = {"date": today, "max_severity_escalated": 0,
                     "n_pings": 0, "pings": []}
    max_esc = day_state.get("max_severity_escalated", 0)

    # escalate only on a STRICTLY worse break than already sent today
    should_escalate = severity >= 1 and severity > max_esc
    escalated, tg_info, message_id, message = False, "not_escalated", None, None

    if should_escalate:
        message = build_message(now, severity, trips, base, tape,
                                bool(simulate), implication)
        if dry_run:
            tg_info = "dry_run"
        else:
            chat_id = event.get("chat_id") or get_chat_id()
            ok, tg_info, message_id = send_telegram(chat_id, message)
            escalated = ok
            if ok:
                day_state["max_severity_escalated"] = severity
                day_state["n_pings"] = day_state.get("n_pings", 0) + 1
                day_state["pings"] = (day_state.get("pings") or []) + [{
                    "at": now.isoformat(), "severity": severity,
                    "label": SEV_LABEL.get(severity),
                    "trips": trips[:6], "message_id": message_id}]

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-cro-escalation",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "checkpoint_utc": now.strftime("%H:%M"),
        "dry_run": dry_run,
        "simulated": bool(simulate),

        "severity": severity,
        "severity_label": SEV_LABEL.get(severity, "?"),
        "escalated": escalated,
        "should_escalate": should_escalate,
        "tripped": trips,
        "tape_severity": tape_sev,
        "drift_severity": drift_sev,

        "tape": {k: v for k, v in tape.items() if not k.startswith("_")},
        "drift": drift_trips,
        "hedge_implication": implication,
        "morning_baseline": base,
        "baseline_is_today": baseline_today,

        "day_state": day_state,
        "telegram_info": tg_info,
        "message_id": message_id,
        "message": message,

        "thresholds": {
            "watch": "SPY<=%.1f%% & VIX+>=%.0f%%, or HYG<=%.1f%%"
                     % (L1_SPY, L1_VIXUP, L1_HYG),
            "alert": "SPY<=%.1f%% & VIX+>=%.0f%%, or HYG<=%.1f%%, or "
                     "VIX>=%.0f & +>=%.0f%%"
                     % (L2_SPY, L2_VIXUP, L2_HYG, L2_VIX_LVL, L2_VIX_SOLO),
            "severe": "SPY<=%.1f%%, or HYG<=%.1f%%, or VIX>=%.0f & +>=%.0f%%"
                      % (L3_SPY, L3_HYG, L3_VIX_LVL, L3_VIX_SOLO),
        },
        "how_to_read": (
            "The intraday firm-risk tripwire. The CRO Morning Brief sets "
            "the day's baseline; this watcher runs through the US session "
            "and escalates one Telegram ping only when the live tape -- or "
            "a risk engine that has re-run -- has deteriorated strictly "
            "past anything already flagged today. Silence means the "
            "morning brief still stands. It re-computes no risk engine."),
        "disclaimer": (
            "Built on a hypothetical research book. Live gauges via FMP. "
            "Research and education only, not investment advice."),
    }

    if not dry_run:
        try:
            s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                          Body=json.dumps(out, default=str).encode("utf-8"),
                          ContentType="application/json")
        except Exception as e:
            print("state write fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "dry_run": dry_run, "simulated": bool(simulate),
        "severity": severity, "severity_label": SEV_LABEL.get(severity),
        "escalated": escalated, "should_escalate": should_escalate,
        "n_tripped": len(trips), "telegram_info": tg_info,
        "message_id": message_id,
        "hedge_implication": implication,
        "max_severity_escalated_today": day_state.get(
            "max_severity_escalated", 0),
        "message": message if dry_run else None})}
