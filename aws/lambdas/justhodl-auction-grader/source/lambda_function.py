"""
justhodl-auction-grader — A-F letter grades for US Treasury auctions.

Reads the rich per-auction data already pulled by justhodl-auction-crisis-detector
(481 auctions, daily refresh) and assigns each recent auction a letter grade
based on weighted scoring across 4 dimensions:

  BID-TO-COVER (40% weight):
    Compare current BTC to 1Y historical avg for the same tenor_bucket.
    A+: >= avg + 0.4    A: >= avg + 0.2    B: >= avg
    C:  >= avg - 0.2    D: >= avg - 0.4    F: < avg - 0.4

  INDIRECT BIDDER % (25% weight):
    Foreign + offshore demand. Higher = stronger auction.
    A: >= 65%    B: 55-65%    C: 45-55%    D: 35-45%    F: < 35%

  TAIL bps (20% weight):
    Difference between high yield and when-issued yield. Tighter = stronger.
    A: <= 0     B: 0-1bp    C: 1-2bp    D: 2-5bp    F: > 5bp

  PRIMARY DEALER % (15% weight):
    Dealers absorb supply when end-buyers don't show. Lower = stronger.
    A: <= 15%   B: 15-25%   C: 25-35%   D: 35-45%   F: > 45%

Composite numeric → letter via thresholds.

Outputs:
  data/auction-grades.json — per-auction grade card with all 4 dimensions
                              + composite grade + narrative.

Telegram alerts:
  - Any auction graded D or F (weak demand = funding stress flag)
  - Persistent C or below over 3 consecutive auctions same tenor
  - A+ auction (rare, indicates flight-to-safety / very strong demand)

Schedule: cron(0 16 ? * MON-FRI *) — daily 16:00 UTC (after most auctions settle)
"""
import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import urllib.request

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/auction-grades.json"
S3_KEY_AUCTIONS = "data/auction-crisis.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


# ─── Grading rubric ──────────────────────────────────────────────────────
# Grade scores: A+=4.3, A=4.0, A-=3.7, B+=3.3, B=3.0, B-=2.7, C+=2.3, C=2.0,
# C-=1.7, D+=1.3, D=1.0, F=0.0

GRADE_SCORES = {
    "A+": 4.3, "A": 4.0, "A-": 3.7,
    "B+": 3.3, "B": 3.0, "B-": 2.7,
    "C+": 2.3, "C": 2.0, "C-": 1.7,
    "D+": 1.3, "D": 1.0, "F": 0.0,
}


def score_to_grade(score):
    """Convert 0-4.3 composite to letter."""
    if score >= 4.15: return "A+"
    if score >= 3.85: return "A"
    if score >= 3.5:  return "A-"
    if score >= 3.15: return "B+"
    if score >= 2.85: return "B"
    if score >= 2.5:  return "B-"
    if score >= 2.15: return "C+"
    if score >= 1.85: return "C"
    if score >= 1.5:  return "C-"
    if score >= 1.15: return "D+"
    if score >= 0.5:  return "D"
    return "F"


def grade_btc(btc, peer_avg=None):
    """Bid-to-cover. peer_avg from same tenor bucket if available."""
    if btc is None: return ("C", None, "BTC missing")
    if peer_avg is None:
        # Static fallback grading
        if btc >= 3.0: return ("A", btc, "BTC strong (≥ 3.0)")
        if btc >= 2.7: return ("A-", btc, "BTC robust (≥ 2.7)")
        if btc >= 2.5: return ("B+", btc, "BTC healthy (≥ 2.5)")
        if btc >= 2.4: return ("B", btc, "BTC adequate")
        if btc >= 2.3: return ("B-", btc, "BTC light")
        if btc >= 2.2: return ("C+", btc, "BTC soft")
        if btc >= 2.0: return ("C", btc, "BTC weak")
        if btc >= 1.8: return ("D", btc, "BTC very weak")
        return ("F", btc, "BTC failed (< 1.8)")
    # Peer-relative
    delta = btc - peer_avg
    if delta >= 0.4: return ("A", btc, f"BTC {btc:.2f} vs peers {peer_avg:.2f} (+{delta:.2f})")
    if delta >= 0.2: return ("A-", btc, f"BTC {btc:.2f} vs peers {peer_avg:.2f} (+{delta:.2f})")
    if delta >= 0:   return ("B", btc, f"BTC {btc:.2f} vs peers {peer_avg:.2f} ({delta:+.2f})")
    if delta >= -0.2: return ("C", btc, f"BTC {btc:.2f} vs peers {peer_avg:.2f} ({delta:.2f})")
    if delta >= -0.4: return ("D", btc, f"BTC {btc:.2f} vs peers {peer_avg:.2f} ({delta:.2f})")
    return ("F", btc, f"BTC {btc:.2f} severely below peers {peer_avg:.2f}")


def grade_indirect(pct):
    if pct is None: return ("C", None, "Indirect % missing")
    if pct >= 70: return ("A+", pct, f"Indirect {pct:.1f}% — exceptional foreign demand")
    if pct >= 65: return ("A", pct, f"Indirect {pct:.1f}% — strong foreign demand")
    if pct >= 60: return ("A-", pct, f"Indirect {pct:.1f}% — robust foreign demand")
    if pct >= 55: return ("B+", pct, f"Indirect {pct:.1f}% — healthy foreign demand")
    if pct >= 50: return ("B", pct, f"Indirect {pct:.1f}% — adequate foreign demand")
    if pct >= 45: return ("C", pct, f"Indirect {pct:.1f}% — soft foreign demand")
    if pct >= 40: return ("D+", pct, f"Indirect {pct:.1f}% — weak foreign demand")
    if pct >= 35: return ("D", pct, f"Indirect {pct:.1f}% — very weak foreign demand")
    return ("F", pct, f"Indirect {pct:.1f}% — foreign demand collapsed")


def grade_tail(tail_bp):
    if tail_bp is None: return ("B", None, "Tail bps missing (bills don't tail)")
    if tail_bp <= 0:     return ("A+", tail_bp, f"Tail {tail_bp:.1f}bp — stopped through")
    if tail_bp <= 0.5:   return ("A", tail_bp, f"Tail {tail_bp:.1f}bp — tight pricing")
    if tail_bp <= 1.0:   return ("A-", tail_bp, f"Tail {tail_bp:.1f}bp — tight")
    if tail_bp <= 1.5:   return ("B+", tail_bp, f"Tail {tail_bp:.1f}bp — modest")
    if tail_bp <= 2.0:   return ("B", tail_bp, f"Tail {tail_bp:.1f}bp — average")
    if tail_bp <= 3.0:   return ("C", tail_bp, f"Tail {tail_bp:.1f}bp — wide")
    if tail_bp <= 5.0:   return ("D", tail_bp, f"Tail {tail_bp:.1f}bp — very wide (stress)")
    return ("F", tail_bp, f"Tail {tail_bp:.1f}bp — failed auction")


def grade_pd(pct):
    if pct is None: return ("B", None, "PD % missing")
    if pct <= 12:  return ("A+", pct, f"PD {pct:.1f}% — minimal dealer absorption")
    if pct <= 18:  return ("A", pct, f"PD {pct:.1f}% — strong end-demand")
    if pct <= 22:  return ("A-", pct, f"PD {pct:.1f}% — healthy")
    if pct <= 28:  return ("B+", pct, f"PD {pct:.1f}% — normal")
    if pct <= 32:  return ("B", pct, f"PD {pct:.1f}% — typical")
    if pct <= 38:  return ("C", pct, f"PD {pct:.1f}% — soft (dealers absorbing)")
    if pct <= 45:  return ("D+", pct, f"PD {pct:.1f}% — weak (dealers stuck)")
    if pct <= 55:  return ("D", pct, f"PD {pct:.1f}% — very weak")
    return ("F", pct, f"PD {pct:.1f}% — failed (dealers eat majority)")


def compute_peer_avg_btc(auctions, tenor_bucket, exclude_cusip=None):
    """1Y avg BTC for the same tenor bucket."""
    peers = [a.get("btc") for a in auctions
              if a.get("tenor_bucket") == tenor_bucket
              and a.get("cusip") != exclude_cusip
              and a.get("btc") is not None]
    if not peers: return None
    return sum(peers) / len(peers)


def grade_auction(auction, all_auctions):
    """Returns a complete grade card for one auction."""
    tenor = auction.get("tenor_bucket", "unknown")
    cusip = auction.get("cusip")

    peer_btc_avg = compute_peer_avg_btc(all_auctions, tenor, exclude_cusip=cusip)

    btc_g, btc_v, btc_note = grade_btc(auction.get("btc"), peer_btc_avg)
    ind_g, ind_v, ind_note = grade_indirect(auction.get("indirect_pct"))
    tail_g, tail_v, tail_note = grade_tail(auction.get("tail_bp"))
    pd_g, pd_v, pd_note = grade_pd(auction.get("primary_dealer_pct"))

    # Composite weighted score
    composite = (
        GRADE_SCORES[btc_g]  * 0.40 +
        GRADE_SCORES[ind_g]  * 0.25 +
        GRADE_SCORES[tail_g] * 0.20 +
        GRADE_SCORES[pd_g]   * 0.15
    )
    overall = score_to_grade(composite)

    # Narrative
    issues = []
    if GRADE_SCORES[btc_g] < 2.0:  issues.append("weak demand")
    if GRADE_SCORES[ind_g] < 2.0:  issues.append("foreign exodus")
    if GRADE_SCORES[tail_g] < 2.0: issues.append("wide tail")
    if GRADE_SCORES[pd_g] < 2.0:   issues.append("dealer absorption")
    strengths = []
    if GRADE_SCORES[btc_g] >= 3.5: strengths.append("strong demand")
    if GRADE_SCORES[ind_g] >= 3.5: strengths.append("foreign bid")
    if GRADE_SCORES[tail_g] >= 3.5: strengths.append("tight pricing")
    if GRADE_SCORES[pd_g] >= 3.5: strengths.append("low dealer take")

    if overall in ("A+", "A", "A-"):
        narrative = (f"Strong auction. " +
                      (f"{', '.join(strengths).capitalize()}." if strengths else ""))
    elif overall in ("B+", "B", "B-"):
        narrative = "Normal-range auction. No standout features either direction."
    elif overall in ("C+", "C", "C-"):
        narrative = f"Soft auction. " + (
            f"Issues: {', '.join(issues)}." if issues else "Mixed signals.")
    else:
        narrative = (f"WEAK auction — funding stress flag. "
                      f"Issues: {', '.join(issues) if issues else 'multiple dimensions soft'}.")

    return {
        "cusip": cusip,
        "auction_date": auction.get("auction_date"),
        "issue_date": auction.get("issue_date"),
        "security_type": auction.get("security_type"),
        "security_term": auction.get("security_term"),
        "tenor_bucket": tenor,
        "accepted_billions": auction.get("accepted_billions"),
        "high_rate": auction.get("high_rate"),
        "overall_grade": overall,
        "composite_score": round(composite, 2),
        "dimensions": {
            "bid_to_cover":     {"grade": btc_g,  "value": btc_v,  "note": btc_note,
                                  "weight": 0.40, "peer_avg": peer_btc_avg},
            "indirect_pct":     {"grade": ind_g,  "value": ind_v,  "note": ind_note,
                                  "weight": 0.25},
            "tail_bp":          {"grade": tail_g, "value": tail_v, "note": tail_note,
                                  "weight": 0.20},
            "primary_dealer_pct": {"grade": pd_g, "value": pd_v,   "note": pd_note,
                                  "weight": 0.15},
        },
        "narrative": narrative,
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
        return
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
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[auction-grader] starting")

    auction_data = get_s3_json(S3_KEY_AUCTIONS, {}) or {}
    recent = auction_data.get("recent_auctions") or []

    if not recent:
        print("[auction-grader] no recent auctions in auction-crisis sidecar")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "n_graded": 0, "reason": "no_data"})}

    # Grade all recent auctions (sidecar has last 10 by default)
    graded = [grade_auction(a, recent) for a in recent]

    # Aggregate by tenor + by grade
    grade_dist = defaultdict(int)
    by_tenor = defaultdict(list)
    for g in graded:
        grade_dist[g["overall_grade"]] += 1
        by_tenor[g["tenor_bucket"]].append(g)

    # GPA across all recent auctions
    avg_score = sum(g["composite_score"] for g in graded) / max(1, len(graded))
    overall_gpa_letter = score_to_grade(avg_score)

    output = {
        "schema_version": "1.0",
        "method": "auction_grader_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_auction_data_modified": auction_data.get("generated_at"),
        "n_graded": len(graded),
        "summary": {
            "average_score": round(avg_score, 2),
            "overall_gpa_letter": overall_gpa_letter,
            "grade_distribution": dict(grade_dist),
            "n_failing": sum(1 for g in graded if g["overall_grade"] in ("D", "D+", "F")),
            "n_strong": sum(1 for g in graded if g["overall_grade"] in ("A+", "A", "A-")),
        },
        "graded_auctions": graded,
        "by_tenor": {k: v for k, v in by_tenor.items()},
        "regime_from_crisis_detector": auction_data.get("regime"),
        "composite_score_crisis": auction_data.get("composite_score"),
        "duration_s": round(time.time() - t0, 2),
    }

    prior_run = get_s3_json(S3_KEY_OUT, {}) or {}
    put_s3_json(S3_KEY_OUT, output)

    print(f"[auction-grader] graded={len(graded)} gpa={overall_gpa_letter} avg={avg_score:.2f}")
    for g in graded[:5]:
        print(f"  {g['security_term']:<12} {g['cusip']:<12} {g['overall_grade']:>3} "
              f"BTC={g['dimensions']['bid_to_cover']['value']} "
              f"Ind={g['dimensions']['indirect_pct']['value']}%")

    # ─── ALERTS ────────────────────────────────────────────────────────
    try:
        prior_cusips = {g.get("cusip") for g in (prior_run.get("graded_auctions") or [])
                          if isinstance(g, dict)}
        new_grades = [g for g in graded if g["cusip"] not in prior_cusips]
        weak = [g for g in new_grades if g["overall_grade"] in ("D", "D+", "F")]
        excellent = [g for g in new_grades if g["overall_grade"] == "A+"]

        if weak:
            lines = []
            for g in weak[:5]:
                lines.append(
                    f"• <b>{g['security_term']}</b> {g['auction_date']} · "
                    f"<b>{g['overall_grade']}</b> · {g['narrative'][:120]}"
                )
            maybe_telegram(
                f"⚠️ <b>WEAK TREASURY AUCTION (Grade D/F)</b>\n"
                f"<i>Funding stress signal — foreign or dealer absorption issues</i>\n" +
                "\n".join(lines)
            )

        if excellent:
            lines = []
            for g in excellent[:3]:
                lines.append(
                    f"• <b>{g['security_term']}</b> {g['auction_date']} · "
                    f"<b>{g['overall_grade']}</b> · {g['narrative'][:120]}"
                )
            maybe_telegram(
                f"🏆 <b>EXCEPTIONAL TREASURY AUCTION (A+)</b>\n"
                f"<i>Flight-to-safety or very strong demand</i>\n" +
                "\n".join(lines)
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "n_graded": len(graded),
            "overall_gpa": overall_gpa_letter,
            "average_score": round(avg_score, 2),
            "n_failing": output["summary"]["n_failing"],
            "n_strong": output["summary"]["n_strong"],
        }),
    }
