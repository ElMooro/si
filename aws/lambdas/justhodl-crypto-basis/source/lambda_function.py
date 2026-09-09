"""justhodl-crypto-basis · v1.0 — futures basis / cash-and-carry term structure.

The perp funding engine tells you near-term leverage pressure. This tells you the CARRY REGIME:
how richly the dated-futures curve is priced over spot, i.e. how much leverage-long demand is
embedded and the gross mark-implied basis before execution costs and financing.

  - ANNUALIZED BASIS per dated future = (mark − index)/index × 365/days_to_expiry.
        rich contango (high +basis) = heavy leverage-long demand / juicy carry;
        backwardation (negative basis) = deleveraging / stress.
  - BASIS TERM STRUCTURE across the quarterly curve (the shape of the carry).
  - LEGACY cash_and_carry_yield_3m_pct = the ~3-month gross mark-implied annualized basis.
  - PERP PREMIUM + funding as the near-term cross-check.

SOURCE: Deribit (free) — index + funding from the perp ticker (one call), dated marks from the
bulk book-summary. BTC + ETH. No free historical basis term structure exists, so (like the
options surface) it self-accumulates a daily snapshot and registers a contrarian leverage signal
in the central FDR ledger for honest live grading.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from donor_contract import inspect_donor, numeric
from equity_donor_inputs import safe_evidence, load_inputs, crypto_funding_context

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-basis.json"
HIST_KEY = "data/crypto-basis-history.json"
DERIBIT = "https://www.deribit.com/api/v2/public/"


def _get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def surface(ccy):
    now = int(time.time() * 1000)
    perp = _get(DERIBIT + "ticker?instrument_name=%s-PERPETUAL" % ccy)["result"]
    index = numeric(perp.get("index_price"))
    if index is None or index <= 0:
        return {"_err": "no index"}
    funding_8h = numeric(perp.get("funding_8h"))  # explicit venue 8-hour statistic only; current_funding has different semantics
    perp_mark = numeric(perp.get("mark_price"))
    perp_premium = round((perp_mark / index - 1) * 100, 4) if perp_mark is not None and perp_mark > 0 else None
    funding_annual = round(funding_8h * 3 * 365 * 100, 2) if funding_8h is not None else None  # illustrative repeat of observed 8h rate; never locked

    inst = _get(DERIBIT + "get_instruments?currency=%s&kind=future&expired=false" % ccy)["result"]
    imap = {i["instrument_name"]: i for i in inst}
    book = _get(DERIBIT + "get_book_summary_by_currency?currency=%s&kind=future" % ccy)["result"]

    rows = []
    for b in book:
        nm = b.get("instrument_name")
        mark = numeric(b.get("mark_price"))
        meta = imap.get(nm)
        if not meta or mark is None or mark <= 0:
            continue
        if meta.get("settlement_period") == "perpetual":
            continue
        days = (meta.get("expiration_timestamp", 0) - now) / 86400000.0
        if days <= 0.5:
            continue
        basis_pct = (mark / index - 1) * 100
        ann = basis_pct * 365.0 / days
        rows.append({
            "instrument": nm, "days": round(days, 1),
            "mark_price": mark, "index_price": index, "expiration_timestamp": meta.get("expiration_timestamp"),
            "mark_timestamp": b.get("timestamp"), "price_type": "venue model mark, not executable bid/ask",
            "basis_pct": round(basis_pct, 3),
            "annualized_basis_pct": round(ann, 2),
            "open_interest": b.get("open_interest"),
            "volume_usd": b.get("volume_usd"),
        })
    rows.sort(key=lambda r: r["days"])

    def nearest(td):
        return min(rows, key=lambda r: abs(r["days"] - td)) if rows else None

    q3m = nearest(90)
    m1 = nearest(30)
    cc_yield = q3m["annualized_basis_pct"] if q3m else None

    regime = None
    if cc_yield is not None:
        regime = ("HOT CONTANGO (heavy leverage-long / rich carry)" if cc_yield > 15
                  else "HEALTHY CONTANGO" if cc_yield > 8
                  else "MILD CONTANGO" if cc_yield > 2
                  else "FLAT" if cc_yield > -2
                  else "BACKWARDATION (deleveraging / stress)")

    return {"index": round(index, 1), "basis_venue": "Deribit",
            "spot_observation_timestamp": perp.get("timestamp"),
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "funding_8h_observed_rate": funding_8h, "funding_interval_hours": 8 if funding_8h is not None else None,
            "funding_annualization_basis": "simple repeat of Deribit funding_8h; retrospective indicator, not fixed future cash flow",
            "reference_3m_instrument": q3m.get("instrument") if q3m else None,
            "reference_3m_days": q3m.get("days") if q3m else None,
            "perp_premium_pct": perp_premium, "funding_annualized_pct": funding_annual,
            "basis_30d_ann_pct": m1["annualized_basis_pct"] if m1 else None,
            "cash_and_carry_yield_3m_pct": cc_yield,
            "regime": regime, "curve": rows}


def compare_perpetual_funding(result, coin, doc, now=None):
    """D30: compare different venues' gross leverage context, never promise carry."""
    now = now or datetime.now(timezone.utc)
    crypto_funding_context(result, coin, doc)
    context = result["perpetual_funding_context"]
    row = context.get("record") or {}
    observed = row.get("venue_observed_at")
    # OKX timestamps are milliseconds encoded as strings; parse the venue clock,
    # not the future funding settlement time or this receiver's run time.
    observed_ms = numeric(observed)
    observed = observed_ms if observed_ms is not None else observed
    health = inspect_donor({**row, "generated_at": doc.get("generated_at"), "observation_time": observed}, "data/crypto-funding.json", 2,
        observed_paths=("observation_time",), max_observation_age_hours=2,
        required_paths=("instId", "current_funding_rate", "funding_interval_hours", "interval_source", "funding_rate_units"),
        units={"current_funding_rate": "fraction per venue interval", "funding_interval_hours": "hours", "oi_usd": "USD"}, now=now)
    interval, rate, oi, z = (numeric(row.get(key)) for key in ("funding_interval_hours", "current_funding_rate", "oi_usd", "funding_z_score"))
    interval_source = str(row.get("interval_source") or "")
    if (interval is None or not 0 < interval <= 24 or rate is None or abs(rate) > 1
            or interval_source not in ("venue_schedule", "observed_settlement_interval")
            or row.get("funding_rate_units") != "fraction_per_observed_interval"
            or doc.get("version") != "1.0.0" or doc.get("source") != "OKX /api/v5/public + /api/v5/market"
            or not str(row.get("instId", "")).startswith(coin.upper() + "-")
            or (row.get("oi_usd") is not None and (oi is None or oi < 0))
            or (row.get("funding_z_score") is not None and z is None)):
        health.update(status="INVALID", usable=False); health["errors"].append("Funding interval, instrument, rate or OI contract invalid")
    ann = rate * 24 / interval * 365 * 100 if health["usable"] else None
    basis = numeric(result.get("cash_and_carry_yield_3m_pct"))
    gap = basis - ann if basis is not None and ann is not None else None
    conflict = gap is not None and ((basis > 0 and ann < 0) or (basis < 0 and ann > 0))
    history_n = numeric(row.get("n_history_periods"))
    crowding_usable = (health["usable"] and z is not None and history_n is not None and history_n >= 5
                       and row.get("funding_moments_units") == "fraction_per_8h_equivalent")
    crowding = crowding_usable and abs(z) >= 2
    context.update(health=health, usable=health["usable"], annualized_from_observed_interval_pct=ann,
                   interval_status="OBSERVED" if health["usable"] else "UNKNOWN_OR_INVALID",
                   units={"rate": "fraction per stated venue interval", "annualized": "simple percent per year"},
                   market_composite=doc.get("market_composite"),
                   cross_venue_comparison=True, quote_alignment="separately collected venue snapshots; executable quote alignment unverified",
                   history_periods=row.get("n_history_periods"), crowding_review=bool(crowding),
                   crowding_status="AVAILABLE" if crowding_usable else "UNAVAILABLE",
                   current_funding_time=row.get("current_funding_time"), next_funding_time=row.get("next_funding_time"),
                   venue_observed_at=row.get("venue_observed_at"))
    result["funding_basis_comparison"] = {
        "status": "CROSS_VENUE_RESEARCH_CONTEXT" if gap is not None else "UNAVAILABLE",
        "dated_basis_venue": "Deribit", "perpetual_venue": doc.get("source"),
        "reference_instrument": result.get("reference_3m_instrument"), "reference_days": result.get("reference_3m_days"),
        "basis_minus_current_funding_annualized_pp": round(gap, 4) if gap is not None else None,
        "opposite_direction_review": bool(conflict),
        "meaning": "gross annualized mark basis minus repeated current perp funding; not same-venue arbitrage, net yield or locked funding",
        "locked_carry": False,
    }
    result["carry_review_required"] = gap is None or not crowding_usable or bool(conflict) or bool(crowding)
    result["carry_review_reasons"] = (["Funding source/interval unverified"] if not health["usable"] else []) + (["Dated basis unavailable"] if basis is None else []) + (["Comparable funding history is unavailable for crowding review"] if not crowding_usable else []) + (["Dated basis and perp funding have opposite signs across venues"] if conflict else []) + (["Extreme historical funding z-score; leverage crowding review"] if crowding else [])
    return result


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    docs, receipts = load_inputs(s3, BUCKET, [("data/crypto-funding.json", 2, ("by_coin",))])
    out["donor_health"] = receipts
    for ccy in ("BTC", "ETH"):
        try:
            out[ccy.lower()] = compare_perpetual_funding(surface(ccy), ccy, docs.get("data/crypto-funding.json", {}))
        except Exception as e:
            out[ccy.lower()] = {"_err": str(e)[:120], "execution_eligible": False, "carry_review_required": True,
                                "carry_review_reasons": ["Venue surface or donor comparison failed"]}
            diag.append("%s:%s" % (ccy, str(e)[:60]))

    btc = out.get("btc") or {}
    out["cash_and_carry_yield_3m_pct"] = btc.get("cash_and_carry_yield_3m_pct")
    out["regime"] = btc.get("regime")
    cc = btc.get("cash_and_carry_yield_3m_pct")
    out["interpretation"] = (("Gross Deribit mark-implied 3m annualized basis %.1f%% — %s; not executable net yield" % (cc, btc.get("regime")))
                             if cc is not None else None)

    try:
        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"series": []}
        ser = hist.get("series", [])
        today = datetime.now(timezone.utc).date().isoformat()
        snap = {"date": today, "btc_cc_3m": cc, "btc_perp_premium": btc.get("perp_premium_pct"),
                "btc_funding_ann": btc.get("funding_annualized_pct"),
                "eth_cc_3m": (out.get("eth") or {}).get("cash_and_carry_yield_3m_pct")}
        ser = [x for x in ser if x.get("date") != today] + [snap]
        ser = ser[-365:]
        hist["series"] = ser
        hist["updated_at"] = out["generated_at"]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(safe_evidence(hist), default=str, allow_nan=False).encode(),
                      ContentType="application/json")
        out["history_n"] = len(ser)
    except Exception as e:
        diag.append("hist:%s" % str(e)[:60])

    out["event_study_note"] = ("No free historical basis term structure; the carry-extreme signal "
                               "(cc_basis_extreme) is graded live via the central FDR ledger + self-history.")
    out["duration_s"] = round(time.time() - t0, 1)
    if diag:
        out["_diag"] = diag
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(safe_evidence(out), default=str, allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"cc_yield_3m": out.get("cash_and_carry_yield_3m_pct"),
                                                    "regime": out.get("regime"),
                                                    "history_n": out.get("history_n")})}
