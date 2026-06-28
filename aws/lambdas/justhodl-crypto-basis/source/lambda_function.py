"""justhodl-crypto-basis · v1.0 — futures basis / cash-and-carry term structure.

The perp funding engine tells you near-term leverage pressure. This tells you the CARRY REGIME:
how richly the dated-futures curve is priced over spot, i.e. how much leverage-long demand is
embedded and what a delta-neutral trader earns.

  - ANNUALIZED BASIS per dated future = (mark − index)/index × 365/days_to_expiry.
        rich contango (high +basis) = heavy leverage-long demand / juicy carry;
        backwardation (negative basis) = deleveraging / stress.
  - BASIS TERM STRUCTURE across the quarterly curve (the shape of the carry).
  - CASH-AND-CARRY YIELD = the ~3-month annualized basis (what spot-long + future-short earns).
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
    index = perp.get("index_price")
    if not index:
        return {"_err": "no index"}
    funding_8h = perp.get("funding_8h")
    if funding_8h is None:
        funding_8h = perp.get("current_funding")
    perp_premium = round((perp.get("mark_price", index) / index - 1) * 100, 4)
    funding_annual = round((funding_8h or 0.0) * 3 * 365 * 100, 2)  # 8h rate -> %/yr

    inst = _get(DERIBIT + "get_instruments?currency=%s&kind=future&expired=false" % ccy)["result"]
    imap = {i["instrument_name"]: i for i in inst}
    book = _get(DERIBIT + "get_book_summary_by_currency?currency=%s&kind=future" % ccy)["result"]

    rows = []
    for b in book:
        nm = b.get("instrument_name")
        mark = b.get("mark_price")
        meta = imap.get(nm)
        if not meta or not mark:
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

    return {"index": round(index, 1),
            "perp_premium_pct": perp_premium, "funding_annualized_pct": funding_annual,
            "basis_30d_ann_pct": m1["annualized_basis_pct"] if m1 else None,
            "cash_and_carry_yield_3m_pct": cc_yield,
            "regime": regime, "curve": rows}


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    for ccy in ("BTC", "ETH"):
        try:
            out[ccy.lower()] = surface(ccy)
        except Exception as e:
            out[ccy.lower()] = {"_err": str(e)[:120]}
            diag.append("%s:%s" % (ccy, str(e)[:60]))

    btc = out.get("btc") or {}
    out["cash_and_carry_yield_3m_pct"] = btc.get("cash_and_carry_yield_3m_pct")
    out["regime"] = btc.get("regime")
    cc = btc.get("cash_and_carry_yield_3m_pct")
    out["interpretation"] = (("3m cash-and-carry yield %.1f%% — %s" % (cc, btc.get("regime")))
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
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        out["history_n"] = len(ser)
    except Exception as e:
        diag.append("hist:%s" % str(e)[:60])

    out["event_study_note"] = ("No free historical basis term structure; the carry-extreme signal "
                               "(cc_basis_extreme) is graded live via the central FDR ledger + self-history.")
    out["duration_s"] = round(time.time() - t0, 1)
    if diag:
        out["_diag"] = diag
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"cc_yield_3m": out.get("cash_and_carry_yield_3m_pct"),
                                                    "regime": out.get("regime"),
                                                    "history_n": out.get("history_n")})}
