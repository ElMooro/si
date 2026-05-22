"""justhodl-cro-digest -- the firm CRO morning brief, pushed to Telegram.

The Risk Desk cockpit (risk-desk.html) is the live screen; this engine
is the morning note. Once a day, after the risk stack has run through
the night, it reads:

  * firm-risk-board.json -- the synthesised firm verdict, binding
    constraint, top risks, limit utilisation and consistency checks.
  * hedge-planner.json   -- today's hedge order ticket and action.
  * tail-hedge.json      -- the convexity context behind the ticket.
  * a freshness sweep of all twelve risk-stack feeds.

and assembles a structured CRO morning brief -- firm posture with the
overnight change, the binding constraint, today's hedge action, an
alerts block, the top firm risks and a stack-health line -- then
pushes it to Khalid's Telegram.

It keeps data/cro-digest.json as state, so each run knows what it last
reported and derives the overnight posture / action change from it.
event.dry_run=true assembles and returns the brief without sending.

It places no orders and computes no risk -- it is a notifier over the
existing stack. The Firm Risk Board stays the single source of truth.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cro-digest.json"
HIST_KEY = "data/cro-digest-history.json"
SCHEMA = "1.0"
STALE_HOURS = 30.0
RISK_DESK_URL = "https://justhodl.ai/risk-desk.html"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")

# The twelve risk-stack feeds -- swept for freshness for the health line.
STACK_FEEDS = [
    "firm-risk-board.json", "tail-hedge.json", "hedge-planner.json",
    "risk-monitor.json", "liquidity-capacity.json", "firm-stress.json",
    "merger-arb-risk.json", "firm-book.json", "factor-risk.json",
    "pnl-attribution.json", "desk-allocator.json", "desk-returns.json",
]

# Emojis as unicode escapes so the source stays pure ASCII.
DOT = {"GREEN": "\U0001F7E2", "AMBER": "\U0001F7E1", "RED": "\U0001F534"}
SHIELD = "\U0001F6E1"
UP = "\u25B2"
DOWN = "\u25BC"
WARN = "\u26A0"
CHECK = "\u2713"
ACTION_ICON = {
    "OPEN": "\u2795", "ADD": "\u2795", "TRIM": "\u2796", "ROLL": "\U0001F504",
    "SWITCH": "\U0001F500", "HARVEST": "\U0001F4B0", "UNWIND": "\u2716",
    "HOLD": "\u25AA", "NONE": "\u25AA",
}
POSTURE_RANK = {"GREEN": 0, "AMBER": 1, "RED": 2}

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
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read()), obj["LastModified"]
    except Exception:
        return None, None


def hours_since(lm, now):
    if lm is None:
        return None
    try:
        if lm.tzinfo is None:
            lm = lm.replace(tzinfo=timezone.utc)
        return round((now - lm).total_seconds() / 3600.0, 1)
    except Exception:
        return None


def num(v):
    try:
        return None if v is None else float(v)
    except Exception:
        return None


def h(s):
    """Escape for Telegram HTML parse mode."""
    return (str("" if s is None else s)
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def send_telegram(chat_id, text):
    """HTML send with a plain-text fallback -- mirrors the house pattern."""
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
        mid = (body.get("result") or {}).get("message_id")
        return True, "sent", mid
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:300] if hasattr(e, "read") else ""
        print("HTML send failed (%s): %s" % (e.code, detail))
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
            mid = (body.get("result") or {}).get("message_id")
            return True, "sent_plain_fallback", mid
        except Exception as e2:
            return False, "plain_retry_fail: %s" % e2, None
    except Exception as e:
        return False, str(e), None


# --------------------------------------------------------------------------
def build_digest(now):
    """Assemble the CRO brief. Returns (html_text, structured_fields)."""
    board, board_lm = read_json("data/firm-risk-board.json")
    planner, _ = read_json("data/hedge-planner.json")
    tail, _ = read_json("data/tail-hedge.json")
    board = board if isinstance(board, dict) else {}
    planner = planner if isinstance(planner, dict) else {}
    tail = tail if isinstance(tail, dict) else {}

    # ---- freshness sweep of the twelve feeds -----------------------------
    n_fresh, stale = 0, []
    for jf in STACK_FEEDS:
        _, lm = read_json("data/" + jf)
        age = hours_since(lm, now)
        if age is not None and age <= STALE_HOURS:
            n_fresh += 1
        else:
            stale.append(jf.replace(".json", ""))

    # ---- prior digest -- for the overnight change ------------------------
    prior, _ = read_json(OUT_KEY)
    prior = prior if isinstance(prior, dict) else {}
    prior_posture = prior.get("firm_posture")
    prior_action = prior.get("hedge_action")

    # ---- firm posture ----------------------------------------------------
    posture = (board.get("firm_posture") or "UNKNOWN").upper()
    dot = DOT.get(posture, "\u26AA")
    if prior_posture and prior_posture != posture:
        pr, cr = POSTURE_RANK.get(prior_posture, 1), POSTURE_RANK.get(posture, 1)
        if cr > pr:
            change = "%s deteriorated from %s overnight" % (UP, prior_posture)
        else:
            change = "%s improved from %s overnight" % (DOWN, prior_posture)
        posture_changed = True
    else:
        change = "unchanged overnight" if prior_posture else "first brief"
        posture_changed = False

    bc = board.get("binding_constraint") or {}
    date_str = now.strftime("%a %d %b %Y")

    lines = []
    lines.append("%s <b>FIRM RISK -- CRO MORNING BRIEF</b>" % SHIELD)
    lines.append("<i>%s &middot; %s UTC</i>" % (date_str, now.strftime("%H:%M")))
    lines.append("")

    if not board:
        lines.append("%s <b>Firm Risk Board feed unavailable.</b> The "
                      "synthesised firm verdict could not be read -- treat "
                      "the stack posture as stale until it refreshes."
                      % WARN)
    else:
        lines.append("<b>POSTURE: %s %s</b>" % (dot, posture))
        lines.append("<i>%s</i>" % h(change))
        if board.get("headline"):
            lines.append(h(board["headline"]))
        lines.append("")
        if bc.get("label"):
            lines.append("<b>Binding constraint:</b> %s" % h(bc["label"]))
            if bc.get("headline"):
                lines.append(h(bc["headline"]))

    # ---- today's hedge ---------------------------------------------------
    lines.append("")
    lines.append("<b>-- TODAY'S HEDGE --</b>")
    action = (planner.get("action") or "NONE").upper()
    icon = ACTION_ICON.get(action, "\u25AA")
    ticket = planner.get("ticket") or {}
    sleeve_cls = planner.get("scenario_class") or "n/a"
    if not planner:
        lines.append("%s Hedge Execution Planner feed unavailable." % WARN)
        action_required = False
    elif action in ("HOLD", "NONE"):
        sb = planner.get("standing_sleeve_before") or {}
        budget = num(sb.get("budget_pct_of_book")) or 0.0
        lines.append("%s <b>%s</b> -- no hedge trade today." % (icon, action))
        lines.append("Standing sleeve carried: %s, %.2f%% of book."
                     % (h(sleeve_cls), budget))
        action_required = False
    else:
        prem = num(ticket.get("total_premium_pct_of_book")) or 0.0
        lines.append("%s <b>ACTION: %s the %s sleeve</b>"
                     % (WARN, action, h(sleeve_cls)))
        lines.append("%s -- %.2f%% of book across %d leg(s)."
                     % (h(ticket.get("side_summary") or action), prem,
                        len(ticket.get("legs") or [])))
        for lg in (ticket.get("legs") or [])[:4]:
            spec = lg.get("strike_levels") or lg.get("structure") or ""
            lines.append("  &middot; <code>%s %s</code> %s -- %s"
                         % (h(lg.get("side")), h(lg.get("instrument")),
                            h(lg.get("tenor")), h(spec)))
        if ticket.get("closing_instruction"):
            lines.append("  %s" % h(ticket["closing_instruction"]))
        action_required = True
    if prior_action and prior_action != action:
        lines.append("<i>(action changed from %s)</i>" % h(prior_action))

    # ---- alerts ----------------------------------------------------------
    alerts = []
    n_alert = board.get("n_alert") or 0
    n_watch = board.get("n_watch") or 0
    if n_alert:
        alerts.append("%d risk dimension(s) in ALERT" % n_alert)
    if n_watch:
        alerts.append("%d on WATCH" % n_watch)
    tl = board.get("tightest_limit") or {}
    tl_util = num(tl.get("utilization_pct"))
    if tl_util is not None and tl_util >= 85.0:
        alerts.append("%s at %.0f%% of limit"
                      % (tl.get("limit", "tightest limit"), tl_util))
    for c in (board.get("consistency_checks") or []):
        if not c.get("ok"):
            alerts.append("check failed: %s" % (c.get("note")
                                                or c.get("check")))
    npf = planner.get("n_checks_flagged") or 0
    if npf:
        alerts.append("%d hedge pre-trade check(s) flagged" % npf)
    if stale:
        alerts.append("%d stale feed(s): %s"
                      % (len(stale), ", ".join(stale[:4])))

    lines.append("")
    lines.append("<b>-- ALERTS --</b>")
    if alerts:
        for a in alerts[:8]:
            lines.append("%s %s" % (WARN, h(a)))
    else:
        lines.append("%s No limit breaches. Stack clean, %d/%d feeds fresh."
                     % (CHECK, n_fresh, len(STACK_FEEDS)))

    # ---- top firm risks --------------------------------------------------
    top = board.get("top_firm_risks") or []
    if top:
        lines.append("")
        lines.append("<b>-- TOP FIRM RISKS --</b>")
        for i, r in enumerate(top[:3], 1):
            if isinstance(r, dict):
                lines.append("%d. <b>%s</b> -- %s"
                             % (i, h(r.get("risk")), h(r.get("detail"))))
            else:
                lines.append("%d. %s" % (i, h(r)))

    # ---- footer ----------------------------------------------------------
    lines.append("")
    lines.append("Stack health: <b>%d/%d</b> feeds fresh &middot; "
                 "board confidence %s"
                 % (n_fresh, len(STACK_FEEDS),
                    h(board.get("confidence") or "n/a")))
    lines.append("Risk Desk cockpit: %s" % RISK_DESK_URL)
    lines.append("<i>Hypothetical research book, no costs or slippage. "
                 "Research and education only, not investment advice.</i>")

    text = "\n".join(lines)
    fields = {
        "firm_posture": posture,
        "posture_changed": posture_changed,
        "prior_posture": prior_posture,
        "hedge_action": action,
        "prior_action": prior_action,
        "action_required": action_required,
        "n_alert": n_alert, "n_watch": n_watch,
        "alerts": alerts,
        "feeds_fresh": n_fresh, "feeds_total": len(STACK_FEEDS),
        "stale_feeds": stale,
        "binding_constraint": bc.get("label"),
        "board_confidence": board.get("confidence"),
    }
    return text, fields


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    event = event or {}
    dry_run = bool(event.get("dry_run"))

    text, fields = build_digest(now)

    sent, info, message_id = False, "dry_run", None
    chat_id = event.get("chat_id") or get_chat_id()
    if not dry_run:
        sent, info, message_id = send_telegram(chat_id, text)

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-cro-digest",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "dry_run": dry_run,
        "sent": sent,
        "telegram_info": info,
        "message_id": message_id,
        "chat_id_used": (chat_id[:4] + "...") if chat_id else None,
        "text": text,
        "char_count": len(text),
    }
    out.update(fields)

    if not dry_run:
        # persist as state so the next run can read the overnight change
        try:
            s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                          Body=json.dumps(out, default=str).encode("utf-8"),
                          ContentType="application/json")
        except Exception as e:
            # audit P2.5: emit EMF metric for silent put_object failure
            print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
            print("state write fail: %s" % e)
        # history
        try:
            hist, _ = read_json(HIST_KEY)
            snaps = hist.get("snapshots") if isinstance(hist, dict) else []
            snaps = [s for s in (snaps or [])
                     if s.get("date") != now.date().isoformat()]
            snaps.append({
                "date": now.date().isoformat(),
                "generated_at": now.isoformat(),
                "firm_posture": fields["firm_posture"],
                "hedge_action": fields["hedge_action"],
                "action_required": fields["action_required"],
                "n_alert": fields["n_alert"], "n_watch": fields["n_watch"],
                "sent": sent,
            })
            snaps = snaps[-180:]
            s3.put_object(
                Bucket=BUCKET, Key=HIST_KEY,
                Body=json.dumps({"schema_version": SCHEMA,
                                 "engine": "justhodl-cro-digest",
                                 "updated_at": now.isoformat(),
                                 "snapshots": snaps},
                                default=str).encode("utf-8"),
                ContentType="application/json")
        except Exception as e:
            # audit P2.5: emit EMF metric for silent put_object failure
            print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
            print("history write fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "dry_run": dry_run, "sent": sent, "info": info,
        "message_id": message_id, "firm_posture": fields["firm_posture"],
        "hedge_action": fields["hedge_action"],
        "action_required": fields["action_required"],
        "char_count": len(text),
        "text": text if dry_run else None})}
