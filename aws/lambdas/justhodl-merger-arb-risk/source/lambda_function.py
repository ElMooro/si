"""
justhodl-merger-arb-risk -- the Merger-Arb Book Risk Monitor.

The firm Stress Desk re-prices every position through six equity style
factors. That is the wrong risk model for a risk-arbitrage book: an arb
position's P&L is not driven by market beta, it is driven by ONE binary
event -- the deal closes (collect the spread) or the deal breaks (gap down
to the unaffected price). And deal breaks CLUSTER: a financing freeze or a
regulatory regime shift breaks many deals at once, which is exactly the
tail a COVID-style shock represents.

This engine models the merger-arb sleeve on its correct axis. It joins:

  data/firm-book.json   -- the consolidated book; the merger-arb sleeve is
                           every name carried by the Merger-Arb desk, with
                           its net_pct weight
  data/merger-arb.json  -- the Merger-Arb Spread Desk's per-deal record:
                           gross spread, annualised return, downside-to-
                           unaffected, deal-risk score, tier

For each live arb position it computes the loss to the FIRM book if that
one deal breaks, the carry if it closes, and the market-implied break
probability. It then runs the portfolio scenarios a risk-arb PM actually
watches:

  * cluster break   -- every live deal breaks at once
  * worst-quartile  -- the riskiest 25% of deals break, the rest close
  * model expected  -- probability-weighted using the desk's deal-risk score
  * full close      -- every deal closes on schedule

Posture is the cluster-break loss against the firm's arb loss limits -- a
risk-arb book should be sized so a full cluster break is survivable.

Output: data/merger-arb-risk.json. Schedule: cron(30 3 * * ? *), 03:30 UTC,
after the firm Stress Desk -- it completes the firm risk stack.
"""
import json
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/merger-arb-risk.json"
FIRM_KEY = "data/firm-book.json"
ARB_KEY = "data/merger-arb.json"
STRESS_KEY = "data/firm-stress.json"
SCHEMA = "1.0"

# the firm-book desk display name for the merger-arb desk (firm-book maps
# the internal key "merger-arb" to this label); matched case-insensitively
ARB_DESK_LABEL = "Merger-Arb Desk"

# cluster-break loss limits for the arb sleeve, as % of the FIRM book
LOSS_LIMIT_SOFT = -8.0      # AMBER at or below
LOSS_LIMIT_HARD = -15.0     # RED at or below

# model break probability is the desk's deal-risk score, clamped to a sane band
P_BREAK_FLOOR = 0.02
P_BREAK_CEIL = 0.60

s3 = boto3.client("s3", region_name="us-east-1")


def get_json(key):
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(raw)
    except Exception:
        return None


def put_json(key, obj):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="no-cache")


def is_arb_desk(desk_key):
    return "merger" in str(desk_key).lower() and "arb" in str(desk_key).lower()


def empty_payload(reason):
    return {
        "schema": SCHEMA,
        "engine": "justhodl-merger-arb-risk",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "posture": "UNKNOWN",
        "headline": "Merger-arb book risk could not be assessed: " + reason,
        "reason": reason,
        "summary": {"n_arb_positions": 0, "n_live_deals": 0,
                    "n_no_deal_data": 0, "sleeve_pct_of_book": 0.0},
        "positions": [],
        "scenarios": [],
    }


def lambda_handler(event, context):
    t0 = time.time()

    firm = get_json(FIRM_KEY)
    if not firm or not firm.get("equity_book"):
        out = empty_payload("firm-book.json unavailable")
        put_json(OUT_KEY, out)
        return {"statusCode": 200, "body": json.dumps({"ok": False,
                "error": "no firm-book"})}

    arb = get_json(ARB_KEY) or {}
    deals = arb.get("all_priced") or []
    # index priced deals by target ticker
    deal_by_target = {}
    for d in deals:
        tgt = (d.get("target") or "").upper()
        if tgt:
            deal_by_target[tgt] = d

    # ---- 1) isolate the merger-arb sleeve from the firm book --------------
    # the arb position is the Merger-Arb desk's OWN signed contribution to
    # the name (pct of firm capital) -- NOT the firm-net position, which can
    # conflate other desks holding the same ticker. A risk-arb desk is
    # structurally long its targets, so this contribution is normally > 0.
    sleeve = []
    for r in firm["equity_book"]:
        dks = r.get("desks") or {}
        arb_pos = sum(float(v) for k, v in dks.items() if is_arb_desk(k))
        if abs(arb_pos) < 1e-6:
            continue
        r = dict(r)
        r["_arb_pos"] = arb_pos
        sleeve.append(r)

    if not sleeve:
        out = empty_payload("no merger-arb positions in the firm book")
        put_json(OUT_KEY, out)
        return {"statusCode": 200, "body": json.dumps({"ok": True,
                "posture": "UNKNOWN", "n_arb_positions": 0})}

    # ---- 2) per-position deal-break economics -----------------------------
    positions = []
    n_live = n_nodata = 0
    for r in sleeve:
        sym = r["symbol"].upper()
        arb_pos = float(r["_arb_pos"])           # Merger-Arb desk's position
        firm_net = float(r.get("net_pct") or 0.0)
        d = deal_by_target.get(sym)
        rec = {
            "symbol": sym,
            "name": r.get("name") or sym,
            "sector": r.get("sector") or "Special Situations",
            "side": "LONG" if arb_pos > 0 else "SHORT",
            "position_pct": round(arb_pos, 3),
            "firm_net_pct": round(firm_net, 3),
        }
        if not d:
            n_nodata += 1
            rec.update({
                "has_live_deal": False,
                "note": ("carried by the Merger-Arb desk but no priced deal "
                         "in the current feed -- deal may have closed or is "
                         "not yet parsed"),
            })
            positions.append(rec)
            continue
        n_live += 1
        spread = float(d.get("gross_spread_pct") or 0.0)        # %, +ve
        downside = d.get("downside_to_unaffected_pct")          # %, -ve
        downside = float(downside) if downside is not None else None
        deal_risk = d.get("deal_risk")
        # firm-book P&L impact, as % of the whole firm book. carry can be
        # negative for a BUMP-WATCH name trading above deal value (negative
        # gross spread) -- that is correct economics for a long arb leg.
        carry_if_closes = round(arb_pos * spread / 100.0, 3)
        break_loss = (round(arb_pos * downside / 100.0, 3)
                      if downside is not None else None)
        # market-implied break probability  p = s / (s + |d|) -- only defined
        # for a positive gross spread; a non-positive spread is a bump /
        # over-completion story, not a break-probability one
        implied_break = None
        if (downside is not None and spread > 0
                and (spread + abs(downside)) > 1e-6):
            implied_break = round(spread / (spread + abs(downside)), 3)
        # model break probability from the desk's deal-risk score
        p_break_model = None
        if isinstance(deal_risk, (int, float)):
            p_break_model = max(P_BREAK_FLOOR,
                                min(P_BREAK_CEIL, deal_risk / 100.0))
        rec.update({
            "has_live_deal": True,
            "acquirer": d.get("acquirer"),
            "deal_type": d.get("deal_type"),
            "tier": d.get("tier"),
            "est_close_days": d.get("est_close_days"),
            "gross_spread_pct": round(spread, 2),
            "annualized_return_pct": d.get("annualized_return_pct"),
            "downside_to_unaffected_pct": downside,
            "deal_risk": deal_risk,
            "reward_risk": d.get("reward_risk"),
            "carry_if_closes_pct": carry_if_closes,
            "break_loss_pct": break_loss,
            "implied_break_prob": implied_break,
            "model_break_prob": (round(p_break_model, 3)
                                 if p_break_model is not None else None),
        })
        positions.append(rec)

    live = [p for p in positions if p.get("has_live_deal")]
    sleeve_pct = round(sum(abs(p["position_pct"]) for p in positions), 2)

    # ---- 3) portfolio deal-break scenarios --------------------------------
    def s(v):
        return v if isinstance(v, (int, float)) else 0.0

    cluster_break = round(sum(s(p.get("break_loss_pct")) for p in live), 2)
    full_close = round(sum(s(p.get("carry_if_closes_pct")) for p in live), 2)

    # worst-quartile: riskiest 25% by deal_risk break; the rest close
    ranked = sorted(
        [p for p in live if p.get("break_loss_pct") is not None],
        key=lambda p: -(p.get("deal_risk") or 0))
    q = max(1, len(ranked) // 4) if ranked else 0
    worst_q_break = ranked[:q]
    worst_q_set = {p["symbol"] for p in worst_q_break}
    worst_quartile = round(
        sum(s(p.get("break_loss_pct")) for p in worst_q_break)
        + sum(s(p.get("carry_if_closes_pct")) for p in live
              if p["symbol"] not in worst_q_set), 2)

    # model-expected: probability-weighted by the desk's deal-risk score
    model_expected = 0.0
    for p in live:
        pb = p.get("model_break_prob")
        bl = p.get("break_loss_pct")
        cc = p.get("carry_if_closes_pct")
        if pb is None or bl is None or cc is None:
            continue
        model_expected += pb * bl + (1.0 - pb) * cc
    model_expected = round(model_expected, 3)

    scenarios = [
        {"scenario": "Cluster break (every live deal breaks at once)",
         "type": "tail", "book_pnl_pct": cluster_break,
         "note": ("the risk-arb catastrophe -- a financing freeze or "
                  "regulatory shift breaks deals together")},
        {"scenario": "Worst-quartile break (riskiest 25% break, rest close)",
         "type": "stress", "book_pnl_pct": worst_quartile,
         "note": "a selective break concentrated in the highest-risk deals"},
        {"scenario": "Model-expected P&L (deal-risk weighted)",
         "type": "base", "book_pnl_pct": model_expected,
         "note": ("probability-weighted by the desk's deal-risk score on "
                  "each name")},
        {"scenario": "Full close (every deal closes on schedule)",
         "type": "upside", "book_pnl_pct": full_close,
         "note": "the carry the sleeve earns if nothing breaks"},
    ]

    # ---- 4) concentration -------------------------------------------------
    by_break = sorted(
        [p for p in live if p.get("break_loss_pct") is not None],
        key=lambda p: p.get("break_loss_pct"))
    largest = by_break[0] if by_break else None
    top5 = by_break[:5]

    # ---- 5) posture -------------------------------------------------------
    if cluster_break <= LOSS_LIMIT_HARD:
        posture = "RED"
    elif cluster_break <= LOSS_LIMIT_SOFT:
        posture = "AMBER"
    else:
        posture = "GREEN"

    # ---- 6) cross-reference the factor-model Stress Desk ------------------
    factor_xref = None
    stress = get_json(STRESS_KEY)
    if isinstance(stress, dict):
        scen = stress.get("scenarios") or []
        if scen:
            worst = scen[0]    # firm-stress sorts worst-first
            for dr in (worst.get("desk_pnl") or []):
                if is_arb_desk(dr.get("desk")):
                    factor_xref = {
                        "stress_scenario": worst.get("scenario"),
                        "factor_model_desk_pnl_pct": dr.get("pnl_pct"),
                        "note": ("the firm Stress Desk's six-factor model "
                                 "estimate for the Merger-Arb desk in its "
                                 "worst scenario -- shown for comparison; "
                                 "the deal-break model above is the correct "
                                 "risk axis for an arb book"),
                    }
                    break

    median_risk = None
    risks = sorted(p["deal_risk"] for p in live
                   if isinstance(p.get("deal_risk"), (int, float)))
    if risks:
        median_risk = risks[len(risks) // 2]

    headline = (
        "Merger-arb sleeve is %.1f%% of the firm book across %d live deal(s). "
        "A full cluster break costs %.1f%% of book; the sleeve earns %.1f%% "
        "if every deal closes. Posture %s (cluster-break limits: soft %.0f%%, "
        "hard %.0f%%)."
        % (sleeve_pct, n_live, cluster_break, full_close, posture,
           LOSS_LIMIT_SOFT, LOSS_LIMIT_HARD))

    out = {
        "schema": SCHEMA,
        "engine": "justhodl-merger-arb-risk",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "firm_book_asof": firm.get("generated_at", ""),
        "merger_arb_feed_asof": arb.get("generated_at", ""),
        "posture": posture,
        "headline": headline,
        "loss_limits": {"soft_pct": LOSS_LIMIT_SOFT,
                        "hard_pct": LOSS_LIMIT_HARD},
        "summary": {
            "n_arb_positions": len(positions),
            "n_live_deals": n_live,
            "n_no_deal_data": n_nodata,
            "sleeve_pct_of_book": sleeve_pct,
            "cluster_break_pct": cluster_break,
            "worst_quartile_pct": worst_quartile,
            "model_expected_pct": model_expected,
            "full_close_carry_pct": full_close,
            "median_deal_risk": median_risk,
            "largest_single_break": ({
                "symbol": largest["symbol"],
                "break_loss_pct": largest.get("break_loss_pct"),
                "deal_risk": largest.get("deal_risk"),
            } if largest else None),
        },
        "scenarios": scenarios,
        "top_break_risks": [{
            "symbol": p["symbol"], "name": p["name"],
            "position_pct": p["position_pct"],
            "downside_to_unaffected_pct": p.get("downside_to_unaffected_pct"),
            "break_loss_pct": p.get("break_loss_pct"),
            "carry_if_closes_pct": p.get("carry_if_closes_pct"),
            "deal_risk": p.get("deal_risk"),
            "tier": p.get("tier"),
        } for p in top5],
        "positions": positions,
        "factor_model_cross_reference": factor_xref,
        "method": (
            "The merger-arb sleeve is every firm-book name carried by the "
            "Merger-Arb desk, joined by ticker to the Merger-Arb Spread "
            "Desk's per-deal record. Break loss = position weight times the "
            "deal's downside-to-unaffected; carry = position weight times "
            "the gross spread. The cluster-break scenario breaks every live "
            "deal at once -- the correct tail for a risk-arb book, where "
            "breaks correlate. Implied break probability is spread / (spread "
            "+ |downside|)."),
        "disclaimer": (
            "Risk-analytics model for the platform's model firm book. Deal "
            "terms and downside are sourced from SEC filings via the "
            "Merger-Arb Spread Desk. Not investment advice."),
    }
    put_json(OUT_KEY, out)
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "posture": posture,
        "cluster_break_pct": cluster_break, "n_live_deals": n_live,
        "n_arb_positions": len(positions)})}


if __name__ == "__main__":
    print(lambda_handler({}, None))
