"""justhodl-hedge-pnl -- the hedge overlay scorecard.

The Tail Hedge Overlay sizes convex protection; the Hedge Planner
works it into tickets and accretes a daily record in
data/hedge-planner-history.json. This engine answers the question that
record exists to answer: is the overlay earning its carry?

A tail hedge is insurance. It is *supposed* to bleed a small carry
most days -- that bleed is the premium -- and pay convexly in the rare
stress episode. So "earning its carry" is not "is it green"; a hedge
that is always green is mis-sized. It is a cost-versus-cover question:

  * CARRY PAID -- each day the standing sleeve bleeds a slice of its
    annualised carry, scaled to the sleeve size on the books that day.
    Summed, this is the running cost of the overlay.
  * STRESS PAYOFF -- on a day the tape sells off past a documented
    stress threshold, the convex sleeve pays. The payoff is modelled
    from the sleeve's own payoff multiple and how deep the move ran
    relative to the worst modelled scenario -- zero on a calm day,
    convex on a deep one.
  * The score -- cumulative payoff against cumulative carry, plus a
    forward cover-per-carry ratio. Bucketed so carry bled in a calm
    regime is read as the expected cost of insurance, not a failure.

The track is honest about its own youth: until it has enough marked
days it reports WARMING. Stylised marks on a hypothetical research
book -- the value is the shape, not a P&L claim.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/hedge-pnl.json"
HIST_KEY = "data/hedge-pnl-history.json"
PLANNER_HIST_KEY = "data/hedge-planner-history.json"
SCHEMA = "1.0"
FMP_KEY = os.environ.get("FMP_KEY", "")

# ---- documented model parameters -----------------------------------------
# A daily SPY move past this is a "stress day" where the convex sleeve
# pays; above it the sleeve is approximately inert -- convexity does
# almost nothing on small moves and gains sharply on large ones.
STRESS_THRESHOLD_PCT = -1.5
TRADING_DAYS = 252
WARMING_DAYS = 10            # below this many marked days the track WARMS
SCENARIO_DEPTH_FALLBACK = 12.0   # worst-scenario depth if tail feed is thin

s3 = boto3.client("s3", region_name=REGION)


# --------------------------------------------------------------------------
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


def fmp_history(symbol):
    """Daily EOD closes via FMP /stable. Returns {date: close}."""
    if not FMP_KEY:
        return {}
    url = ("https://financialmodelingprep.com/stable/"
           "historical-price-eod/light?symbol=%s&apikey=%s"
           % (urllib.parse.quote(symbol), FMP_KEY))
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-hedge-pnl/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else (data or {}).get(
            "historical", [])
        out = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            d = str(row.get("date") or "")[:10]
            c = num(row.get("price"))
            if c is None:
                c = num(row.get("close"))
            if d and c is not None:
                out[d] = c
        return out
    except Exception as e:
        print("fmp_history %s fail: %s" % (symbol, e))
        return {}


def daily_returns(closes):
    """{date: close} -> {date: pct return vs the prior trading day}."""
    dates = sorted(closes.keys())
    rets = {}
    for i in range(1, len(dates)):
        prev, cur = closes[dates[i - 1]], closes[dates[i]]
        if prev:
            rets[dates[i]] = (cur - prev) / prev * 100.0
    return rets


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    planner_hist = read_json(PLANNER_HIST_KEY) or {}
    snaps = planner_hist.get("snapshots") or []
    snaps = sorted([s for s in snaps if isinstance(s, dict) and s.get("date")],
                   key=lambda s: s["date"])

    tail = read_json("data/tail-hedge.json") or {}
    sleeve = tail.get("hedge_sleeve") or {}
    tail_exp = tail.get("tail_exposure") or {}
    payoff_multiple = num(sleeve.get("payoff_multiple")) or 0.0
    annual_carry = num(sleeve.get("annualised_carry_pct")) or 0.0
    expected_payoff = num(sleeve.get("expected_payoff_in_worst_scenario_pct"))
    scenario_class = sleeve.get("scenario_class")
    worst_loss = num(tail_exp.get("worst_loss_pct"))
    depth = abs(worst_loss) if worst_loss else SCENARIO_DEPTH_FALLBACK
    if depth <= 0:
        depth = SCENARIO_DEPTH_FALLBACK

    # the current standing sleeve size -- carry scales to sleeve size, and
    # this is the reference the per-day scaling normalises against.
    standings = [num(s.get("standing_after_pct")) or 0.0 for s in snaps]
    ref_standing = next((x for x in reversed(standings) if x > 0), 0.0)
    if ref_standing <= 0:
        ref_standing = max(standings) if standings else 0.0

    spy_rets = daily_returns(fmp_history("SPY"))

    daily = []
    cum_carry = 0.0
    cum_payoff = 0.0
    n_stress = 0
    worst_day = None

    for s in snaps:
        d = s["date"]
        standing = num(s.get("standing_after_pct")) or 0.0
        if standing <= 0:
            # no sleeve on the books that day -- nothing to score
            daily.append({"date": d, "standing_pct": 0.0,
                          "spy_ret_pct": spy_rets.get(d),
                          "carry_pct": 0.0, "payoff_pct": 0.0,
                          "day_pnl_pct": 0.0, "regime": "FLAT"})
            continue
        # carry drag: a slice of annualised carry, scaled to sleeve size
        size_ratio = (standing / ref_standing) if ref_standing > 0 else 1.0
        carry = (annual_carry / TRADING_DAYS) * size_ratio
        # stress payoff: convex gain only when the tape sells off hard
        spy_ret = spy_rets.get(d)
        payoff = 0.0
        regime = "CALM"
        if spy_ret is not None and spy_ret <= STRESS_THRESHOLD_PCT:
            excess = abs(spy_ret) - abs(STRESS_THRESHOLD_PCT)
            frac = min(1.0, max(0.0, excess) / depth) if depth > 0 else 0.0
            payoff = standing * payoff_multiple * frac
            regime = "STRESS"
            n_stress += 1
            if worst_day is None or spy_ret < worst_day["spy_ret_pct"]:
                worst_day = {"date": d, "spy_ret_pct": round(spy_ret, 2),
                             "payoff_pct": round(payoff, 4)}
        cum_carry += carry
        cum_payoff += payoff
        daily.append({
            "date": d, "standing_pct": round(standing, 4),
            "spy_ret_pct": round(spy_ret, 2) if spy_ret is not None else None,
            "carry_pct": round(-carry, 5),
            "payoff_pct": round(payoff, 4),
            "day_pnl_pct": round(payoff - carry, 5),
            "regime": regime,
        })

    marked = [r for r in daily if r["regime"] != "FLAT"]
    n_days = len(marked)
    n_calm = sum(1 for r in marked if r["regime"] == "CALM")
    net_pnl = cum_payoff - cum_carry
    carry_eff = (cum_payoff / cum_carry) if cum_carry > 1e-9 else None
    cover_per_carry = ((expected_payoff / annual_carry)
                       if (expected_payoff is not None and annual_carry > 1e-9)
                       else None)

    # ---- verdict ---------------------------------------------------------
    maturity = "WARMING" if n_days < WARMING_DAYS else "ESTABLISHED"
    if maturity == "WARMING":
        verdict = "WARMING"
        vcolor = "dim"
        if n_stress:
            headline = (
                "Hedge overlay scorecard WARMING -- a %d-day track is not "
                "yet a meaningful read. So far the sleeve has cost %.3f%% of "
                "book in carry and marked %.3f%% of convex payoff across %d "
                "stress day(s); net %+.3f%%."
                % (n_days, cum_carry, cum_payoff, n_stress, net_pnl))
        else:
            headline = (
                "Hedge overlay scorecard WARMING -- a %d-day track is not "
                "yet a meaningful read. The sleeve has cost %.3f%% of book "
                "in carry with no stress episode yet to mark against -- the "
                "expected cost of insurance in a calm regime."
                % (n_days, cum_carry))
    elif carry_eff is not None and carry_eff >= 1.0:
        verdict = "EARNING ITS CARRY"
        vcolor = "green"
        headline = (
            "The overlay is earning its carry -- %.2f%% of convex payoff "
            "captured against %.2f%% of carry bled (%.1fx). The protection "
            "has paid for itself over the track." % (cum_payoff, cum_carry,
                                                     carry_eff))
    elif carry_eff is not None and carry_eff >= 0.4:
        verdict = "FAIRLY PRICED"
        vcolor = "cyan"
        headline = (
            "The overlay is fairly priced -- stress payoff has offset most "
            "of the carry (%.2f%% vs %.2f%%, %.1fx). It is bleeding modestly "
            "between events, exactly as a tail hedge should."
            % (cum_payoff, cum_carry, carry_eff))
    elif n_stress == 0:
        verdict = "PURE CARRY -- NO STRESS YET"
        vcolor = "yellow"
        headline = (
            "The overlay has only cost carry -- %.2f%% of book over %d days "
            "-- because the regime has been calm. That is the expected cost "
            "of holding insurance, not a failure; it pays in the tail."
            % (cum_carry, n_days))
    else:
        verdict = "CARRY-HEAVY"
        vcolor = "orange"
        headline = (
            "The overlay is carry-heavy -- %.2f%% bled against only %.2f%% of "
            "payoff captured (%.1fx) even through %d stress day(s). Worth "
            "reviewing whether the sleeve is over-sized."
            % (cum_carry, cum_payoff, carry_eff or 0.0, n_stress))

    cro_bits = [headline]
    if cover_per_carry is not None:
        cro_bits.append(
            "Forward, the standing sleeve buys ~%.1f%% of book of tail cover "
            "for every 1%%/yr of carry (%.2f%% cover at %.2f%%/yr)."
            % (cover_per_carry, expected_payoff or 0.0, annual_carry))
    if maturity == "WARMING":
        cro_bits.append("The score sharpens as the daily record accretes -- "
                        "a full read needs a stress episode in the window.")
    cro_note = " ".join(cro_bits)

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-hedge-pnl",
        "method": "stylised_hedge_overlay_scorecard",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),

        "maturity": maturity,
        "verdict": verdict,
        "verdict_color": vcolor,
        "headline": headline,
        "cro_note": cro_note,

        "track": {
            "n_days": n_days,
            "first_date": marked[0]["date"] if marked else None,
            "last_date": marked[-1]["date"] if marked else None,
            "n_stress_days": n_stress,
            "n_calm_days": n_calm,
        },
        "carry": {
            "cumulative_pct": round(cum_carry, 4),
            "daily_avg_pct": round(cum_carry / n_days, 5) if n_days else 0.0,
            "annualised_carry_pct": annual_carry,
        },
        "payoff": {
            "cumulative_pct": round(cum_payoff, 4),
            "n_stress_days": n_stress,
            "worst_stress_day": worst_day,
        },
        "net_overlay_pnl_pct": round(net_pnl, 4),
        "carry_efficiency": round(carry_eff, 2) if carry_eff is not None
        else None,

        "forward": {
            "expected_payoff_in_worst_scenario_pct": expected_payoff,
            "annualised_carry_pct": annual_carry,
            "cover_per_carry": round(cover_per_carry, 1)
            if cover_per_carry is not None else None,
            "payoff_multiple": payoff_multiple,
        },

        "sleeve": {
            "scenario_class": scenario_class,
            "label": sleeve.get("label"),
            "standing_pct_now": ref_standing,
        },
        "daily": daily[-90:],
        "spy_history_days": len(spy_rets),

        "parameters": {
            "stress_threshold_pct": STRESS_THRESHOLD_PCT,
            "trading_days": TRADING_DAYS,
            "warming_days": WARMING_DAYS,
            "scenario_depth_pct": round(depth, 2),
        },
        "how_to_read": (
            "Scores whether the tail-hedge overlay is earning its carry. A "
            "tail hedge is insurance -- it is meant to bleed a small carry "
            "between events and pay convexly in a stress episode, so the "
            "test is cumulative convex payoff against cumulative carry, not "
            "whether it is green. Carry is a slice of the sleeve's "
            "annualised carry each day; payoff is modelled from the sleeve's "
            "payoff multiple and how deep the tape sold off, and is zero on "
            "a calm day. WARMING until the track is long enough to read."),
        "disclaimer": (
            "Stylised marks on a hypothetical research book -- a transparent "
            "model of a stylised sleeve, not real option P&L. Research and "
            "education only, not investment advice."),
    }

    try:
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print("output write fail: %s" % e)

    try:
        hist = read_json(HIST_KEY)
        hsnaps = hist.get("snapshots") if isinstance(hist, dict) else []
        today = now.date().isoformat()
        hsnaps = [x for x in (hsnaps or []) if x.get("date") != today]
        hsnaps.append({
            "date": today, "generated_at": now.isoformat(),
            "verdict": verdict, "maturity": maturity,
            "cumulative_carry_pct": round(cum_carry, 4),
            "cumulative_payoff_pct": round(cum_payoff, 4),
            "net_overlay_pnl_pct": round(net_pnl, 4),
            "carry_efficiency": round(carry_eff, 2)
            if carry_eff is not None else None,
            "n_days": n_days,
        })
        hsnaps = hsnaps[-180:]
        s3.put_object(
            Bucket=BUCKET, Key=HIST_KEY,
            Body=json.dumps({"schema_version": SCHEMA,
                             "engine": "justhodl-hedge-pnl",
                             "updated_at": now.isoformat(),
                             "snapshots": hsnaps},
                            default=str).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print("history write fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "maturity": maturity, "verdict": verdict,
        "n_days": n_days, "n_stress_days": n_stress,
        "cumulative_carry_pct": round(cum_carry, 4),
        "cumulative_payoff_pct": round(cum_payoff, 4),
        "net_overlay_pnl_pct": round(net_pnl, 4),
        "carry_efficiency": round(carry_eff, 2)
        if carry_eff is not None else None})}
