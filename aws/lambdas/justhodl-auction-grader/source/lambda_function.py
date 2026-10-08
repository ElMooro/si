"""
justhodl-auction-grader — publishes the desk's A-F participation grades.

Since 2.0.0 this engine no longer runs its own rubric. The desk
(justhodl-auction-desk, data/auction-desk.json) grades every auction against
its comparable cohort (same instrument kind, term bucket and reopening status)
and publishes deterministic alert flags. Two engines grading the same auction
differently was a bug (a 4-week bill could be F on one page and C+ on
another); the grader now projects the desk's grades into the legacy
data/auction-grades.json shape that treasury-auctions.html and
intelligence/index.html already read, and forwards new desk alert flags to
Telegram when credentials exist.

Outputs:
  data/auction-grades.json — graded_auctions (desk grades), summary, by_tenor,
                             alerts (desk flags), source lineage.

Telegram (optional; TELEGRAM_TOKEN / TELEGRAM_CHAT_ID):
  - new desk watch flags (dealer share +2σ, indirect -2σ, bid-to-cover -2σ,
    three consecutive D/F in a tenor, wide stop-vs-median) since the prior run
  - new A / F coupon grades

Schedule: cron(0 16 ? * MON-FRI *) — the desk refreshes earlier in the day.
Descriptive only: no calls, no sizing.
"""
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import boto3
import urllib.request

VERSION = "2.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = "data/auction-grades.json"
S3_KEY_DESK = "data/auction-desk.json"
S3_KEY_CRISIS = "data/auction-crisis.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

# Legacy 4-point scale kept for the intelligence page bars; derived from the desk letter only.
GRADE_SCORES = {"A": 4.0, "B": 3.0, "C": 2.0, "D": 1.0, "F": 0.0}
LETTERS = ("A", "B", "C", "D", "F")
WINDOW_DAYS = 45
MAX_ROWS = 40


def score_to_grade(score):
    if score is None:
        return None
    return "A" if score >= 3.5 else "B" if score >= 2.5 else "C" if score >= 1.5 else "D" if score >= 0.5 else "F"


def desk_grade(a):
    """Only a complete current-contract desk grade counts; anything else is withheld."""
    trace = a.get("grading_inputs") or {}
    if trace.get("contract") != "auction-participation-inputs.v1" or trace.get("status") != "complete":
        return None
    letter = a.get("grade")
    return letter if letter in LETTERS and trace.get("grade") == letter else None


def tenor_bucket(a):
    q = a.get("quality") or {}
    term = q.get("cohort_term") or a.get("original_term") or a.get("term") or "unknown"
    kind = a.get("instrument_kind") or "UNKNOWN"
    return "%s_%s" % (kind.lower(), str(term).lower().replace("-", "_").replace(" ", "_"))


def grade_card(a):
    letter = desk_grade(a)
    z = a.get("z") or {}
    t = a.get("trailing12") or {}
    read = a.get("read") or {}
    beh = a.get("behaviour") or {}
    hits = beh.get("hit_ratio_pct") or {}
    accepted = a.get("total_accepted")
    narrative = read.get("what_it_means") or a.get("verdict") or "Participation inputs unavailable."
    if letter is None:
        narrative = "Grade withheld: %s" % ((a.get("grading_inputs") or {}).get("problems") or (a.get("grading_inputs") or {}).get("missing_features") or "incomplete comparable cohort")
    return {
        "cusip": a.get("cusip"), "auction_date": a.get("auction_date"), "issue_date": a.get("issue_date"),
        "security_type": a.get("type"), "security_term": a.get("term"), "reopening": bool(a.get("reopening")),
        "tenor_bucket": tenor_bucket(a), "instrument_kind": a.get("instrument_kind"),
        "accepted_billions": round(accepted / 1e9, 2) if isinstance(accepted, (int, float)) else None,
        "total_accepted_usd": accepted, "high_rate": a.get("high_yield"),
        # legacy + intelligence-page field names, all from the desk row
        "overall_grade": letter or "n/a", "grade_letter": letter or "n/a",
        "grade_numeric": GRADE_SCORES.get(letter), "composite_score": GRADE_SCORES.get(letter),
        "demand_score": a.get("demand_score"),
        "bid_to_cover": a.get("btc"), "indirect_bidder_pct": a.get("indirect_pct"), "primary_dealer_pct": a.get("pd_pct"), "direct_bidder_pct": a.get("direct_pct"),
        "tail_bps": None, "tail_note": "when-issued tail not captured; prior-close par gap is context only",
        "prior_close_gap_bp": a.get("tail_bp"), "dealer_hit_pct": hits.get("pd"), "high_minus_median_bp": a.get("high_minus_median_bp"),
        "dimensions": {
            "bid_to_cover": {"value": a.get("btc"), "z": z.get("btc"), "cohort_mean": t.get("btc"), "weight": 1.0},
            "indirect_pct": {"value": a.get("indirect_pct"), "z": z.get("indirect"), "cohort_mean": t.get("indirect_pct"), "weight": 0.8},
            "primary_dealer_pct": {"value": a.get("pd_pct"), "z": z.get("pd"), "cohort_mean": t.get("pd_pct"), "weight": -0.8},
            "tail_bp": {"value": None, "note": "no when-issued feed; excluded"},
        },
        "cohort_n": t.get("n"), "narrative": narrative, "headline": read.get("headline"),
        "source": "justhodl-auction-desk", "call": None, "sizing_eligible": False,
    }


def detector_card(r):
    """Fallback row from the crisis detector sidecar when the desk packet is unavailable: identity
    and raw participation only, grade withheld (the desk owns the cohort grade)."""
    accepted = r.get("accepted_billions")
    return {
        "cusip": r.get("cusip"), "auction_date": r.get("auction_date"), "issue_date": r.get("issue_date"),
        "security_type": r.get("security_type"), "security_term": r.get("security_term"), "reopening": None,
        "tenor_bucket": str(r.get("tenor_bucket") or "unknown"), "instrument_kind": r.get("instrument_kind"),
        "accepted_billions": accepted, "total_accepted_usd": round(accepted * 1e9) if isinstance(accepted, (int, float)) else None,
        "high_rate": r.get("high_rate"),
        "overall_grade": "n/a", "grade_letter": "n/a", "grade_numeric": None, "composite_score": None, "demand_score": None,
        "bid_to_cover": r.get("btc"), "indirect_bidder_pct": r.get("indirect_pct"), "primary_dealer_pct": r.get("primary_dealer_pct"), "direct_bidder_pct": r.get("direct_pct"),
        "tail_bps": None, "tail_note": "when-issued tail not captured; prior-close par gap is context only",
        "prior_close_gap_bp": r.get("tail_bp"), "dealer_hit_pct": None, "high_minus_median_bp": r.get("high_minus_median_bp"),
        "dimensions": {}, "cohort_n": None,
        "narrative": "Grade withheld: auction desk packet unavailable; raw participation from the crisis detector only.",
        "headline": None, "source": "justhodl-auction-crisis-detector", "call": None, "sizing_eligible": False,
    }


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[s3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return False
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        print(f"[tg] sent: {msg[:80]}")
        return True
    except Exception as e:
        print(f"[tg] err: {e}")
        return False


def _esc(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def alert_key(item):
    return "%s|%s|%s" % (item.get("id"), item.get("cusip"), item.get("date"))


def build_output(desk, crisis, now):
    today = now.date().isoformat()
    since = (now - timedelta(days=WINDOW_DAYS)).date().isoformat()
    rows = [a for a in (desk.get("auctions") or []) if isinstance(a, dict) and since <= str(a.get("auction_date") or "") <= today]
    rows.sort(key=lambda a: (a.get("auction_date") or "", a.get("term") or ""), reverse=True)
    graded = [grade_card(a) for a in rows[:MAX_ROWS]]
    basis = "desk"
    if not graded:
        fallback = [r for r in (crisis.get("recent_auctions") or []) if isinstance(r, dict) and r.get("cusip")]
        fallback.sort(key=lambda r: (r.get("auction_date") or "", r.get("security_term") or ""), reverse=True)
        graded = [detector_card(r) for r in fallback[:MAX_ROWS]]
        basis = "detector_fallback_grades_withheld" if graded else "no_rows"
    letters = [g for g in graded if g["grade_numeric"] is not None]
    grade_dist = defaultdict(int)
    by_tenor = defaultdict(list)
    for g in graded:
        grade_dist[g["overall_grade"]] += 1
        by_tenor[g["tenor_bucket"]].append(g)
    avg_score = round(sum(g["grade_numeric"] for g in letters) / len(letters), 2) if letters else None
    alerts = desk.get("alerts") or {}
    return {
        "schema_version": "2.0", "method": "desk_projection_v2", "version": VERSION, "row_basis": basis,
        "generated_at": now.isoformat(),
        "input_desk_generated_at": desk.get("generated_at"), "input_desk_version": desk.get("version"),
        "input_auction_data_modified": desk.get("generated_at"),
        "n_graded": len(graded), "n_with_letter": len(letters), "window_days": WINDOW_DAYS,
        "summary": {
            "average_score": avg_score, "overall_gpa_letter": score_to_grade(avg_score) or "n/a",
            "grade_distribution": dict(grade_dist),
            "n_failing": sum(1 for g in letters if g["overall_grade"] in ("D", "F")),
            "n_strong": sum(1 for g in letters if g["overall_grade"] == "A"),
            "n_withheld": len(graded) - len(letters),
            "scale": "desk letter A-F mapped to 4/3/2/1/0; withheld grades excluded from the average",
        },
        "graded_auctions": graded,
        "by_tenor": {k: v for k, v in by_tenor.items()},
        "alerts": {"contract": alerts.get("contract"), "as_of": alerts.get("as_of"), "n_watch": alerts.get("n_watch"),
                   "items": alerts.get("items") or [], "rules": alerts.get("rules") or [], "note": alerts.get("note")},
        "curve_map": {"rows": [{k: r.get(k) for k in ("bucket", "auction_date", "grade", "score", "streak_string", "consecutive_weak", "pd_hit_pct", "high_minus_median_bp")}
                               for r in ((desk.get("curve_map") or {}).get("rows") or [])]},
        "crisis_nearest": (desk.get("crisis_fingerprints") or {}).get("nearest"),
        "regime_from_crisis_detector": crisis.get("regime"),
        "composite_score_crisis": crisis.get("composite_score"),
        "grading_basis": "justhodl-auction-desk participation grade: z(bid-to-cover) + 0.8 z(indirect) - 0.8 z(dealer) over 3, versus the trailing 4-12 auctions of the same instrument kind, term bucket and reopening status; descriptive only",
        "source": {"desk": S3_KEY_DESK, "crisis": S3_KEY_CRISIS},
        "call": None, "sizing_eligible": False,
    }


def new_alert_messages(output, prior_run):
    seen = {alert_key(x) for x in ((prior_run.get("alerts") or {}).get("items") or []) if isinstance(x, dict)}
    seen |= {str(x) for x in (prior_run.get("notified_alert_keys") or [])}
    fresh = [x for x in output["alerts"]["items"] if alert_key(x) not in seen]
    watch = [x for x in fresh if x.get("severity") == "watch"]
    extreme = [x for x in fresh if x.get("id") == "grade_extreme" and x.get("type") in ("Note", "Bond")]
    messages = []
    if watch:
        lines = ["• %s" % _esc(x.get("text")) for x in watch[:6]]
        messages.append("⚠️ <b>Treasury auction watch flags</b>\n<i>Descriptive threshold flags from the auction desk; not trade signals</i>\n" + "\n".join(lines))
    if extreme:
        lines = ["• %s" % _esc(x.get("text")) for x in extreme[:4]]
        messages.append("🏛 <b>Treasury coupon auction graded A or F</b>\n<i>Participation versus the comparable cohort; not a forecast</i>\n" + "\n".join(lines))
    return messages, [alert_key(x) for x in fresh]


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print("[auction-grader] starting v%s" % VERSION)
    desk = get_s3_json(S3_KEY_DESK, {}) or {}
    crisis = get_s3_json(S3_KEY_CRISIS, {}) or {}
    if not desk.get("auctions"):
        print("[auction-grader] desk packet unavailable; grades withheld, detector rows carried for identity only")
    output = build_output(desk, crisis, now)
    if not output["graded_auctions"]:
        print("[auction-grader] no rows from desk or detector; leaving prior grades in place")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "n_graded": 0, "reason": "no_rows"})}
    prior_run = get_s3_json(S3_KEY_OUT, {}) or {}
    messages, fresh_keys = new_alert_messages(output, prior_run)
    notified = []
    for msg in messages:
        if maybe_telegram(msg):
            notified.append(msg[:60])
    retained = [str(k) for k in (prior_run.get("notified_alert_keys") or [])][-400:]
    output["notified_alert_keys"] = retained + fresh_keys
    output["telegram_sent"] = len(notified)
    output["duration_s"] = round(time.time() - t0, 2)
    put_s3_json(S3_KEY_OUT, output)
    s = output["summary"]
    print(f"[auction-grader] graded={output['n_graded']} letters={output['n_with_letter']} gpa={s['overall_gpa_letter']} avg={s['average_score']} alerts_new={len(fresh_keys)} tg={len(notified)}")
    for g in output["graded_auctions"][:6]:
        print(f"  {g['security_term']:<18} {g['auction_date']} {g['overall_grade']:>3} btc={g['bid_to_cover']} ind={g['indirect_bidder_pct']} pd={g['primary_dealer_pct']}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "n_graded": output["n_graded"], "overall_gpa": s["overall_gpa_letter"],
                            "average_score": s["average_score"], "n_failing": s["n_failing"], "n_strong": s["n_strong"],
                            "new_alerts": len(fresh_keys), "telegram_sent": len(notified)}),
    }
