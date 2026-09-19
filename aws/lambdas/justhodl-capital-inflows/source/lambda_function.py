"""
justhodl-capital-inflows — US NET CAPITAL INFLOWS (the foreign-funding tap).
================================================================================
The single most under-watched macro driver: how hard the rest of the world is
pushing money INTO US assets. Foreign inflows finance the twin deficits and bid
up US stocks and bonds — they are bull-run fuel. A *sudden stop* or reversal of
those inflows is a classic funding-crisis trigger (dollar squeeze, yield spike,
risk-off). The level tells you the regime; the RATE OF CHANGE is the tell.

Built on the REAL Treasury International Capital (TIC) net-transaction series from
FRED (release 3), latest monthly, by asset class:
  • FORLTTOTALNET99996  — foreign net purchases of ALL US long-term securities  (THE headline)
       ├ FORLTTREASNET99996  Treasuries
       ├ FORLTAGCYNET99996   agency bonds
       ├ FORLTCORPNET99996   corporate bonds
       └ FORLTEQTYNET99996   equities
  • FORSTTREASNET99996  — short-term Treasury bills (hot-money / risk-off parking)
  • USLTTOTALNET99996   — US net purchases of FOREIGN securities (the outflow leg)
  →  NET long-term cross-border flow = foreign-into-US  −  US-into-foreign

REGIME = level (12-month rolling sum) + a SUDDEN-STOP / acceleration detector
(3-month annualised run-rate vs the 12-month trend, plus a fresh-outflow flip).

OUTPUT: data/capital-inflows.json     SCHEDULE: weekly (TIC releases monthly)
This is macro context — research, not advice.
"""
import os, json, time, urllib.request
from datetime import datetime, timezone

import boto3
from concurrent.futures import ThreadPoolExecutor
from managed_secret import managed_secret
from tic_contract import align, roll, monthly_quality

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/capital-inflows.json"
VERSION = "1.1.0"
FRED_KEY = managed_secret(("FRED_API_KEY", "FRED_KEY"), ("/justhodl/fred/api-key",))

# foreign net purchases of US long-term securities, by asset class ($M, monthly)
INTO_US = {
    "treasuries":      "FORLTTREASNET99996",
    "agency_bonds":    "FORLTAGCYNET99996",
    "corporate_bonds": "FORLTCORPNET99996",
    "equities":        "FORLTEQTYNET99996",
}
TOTAL_INTO_US = "FORLTTOTALNET99996"   # all US LT securities (grand total)
SHORT_TREAS   = "FORSTTREASNET99996"   # short-term T-bills
US_ABROAD     = "USLTTOTALNET99996"    # US net purchases of foreign LT securities (outflow)


def fred(series_id, limit=40, vintage=None):
    """Monthly observations newest-first as [(date, value_$M)]; [] on failure."""
    if not FRED_KEY:
        return []
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
               f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}"
               f"&realtime_start={vintage or datetime.now(timezone.utc).date()}&realtime_end={vintage or datetime.now(timezone.utc).date()}")
        j = json.loads(urllib.request.urlopen(url, timeout=25).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", []) if o["value"] != "."]
    except Exception as e:
        print(f"[fred] {series_id}: {type(e).__name__}")
        return []


def _legacy_unvalidated_handler(event=None, context=None):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    published = now.isoformat()
    vintage = now.date().isoformat()
    ids = {"total": TOTAL_INTO_US, **INTO_US, "short_treasury": SHORT_TREAS, "us_abroad": US_ABROAD,
           "official": "FORLTTOTALNET99990", "private": "FORLTTOTALNET99991"}
    with ThreadPoolExecutor(max_workers=4) as pool:
        raw = dict(zip(ids, pool.map(lambda sid: fred(sid, vintage=vintage), ids.values())))
    asof = max((d for d,_ in raw["total"]), default=None)
    quality = monthly_quality(raw, asof, published, now)
    rows = {k:align(obs, asof) for k,obs in raw.items()}
    total = rows["total"]
    legs = {k:rows[k] for k in INTO_US}
    st, abroad = rows["short_treasury"], rows["us_abroad"]

    # headline foreign net purchases of ALL US long-term securities
    into_12 = roll(total, 12)
    into_3ann = round(sum(v for _,v in total[:3]) / 1000 * 4, 1) if roll(total, 3) is not None else None
    into_12_prior = roll(total[12:], 12) if roll(total, 24) is not None else None      # the 12mo ending a year ago
    last_month = round(total[0][1] / 1000.0, 1) if total else None
    prev_month = round(total[1][1] / 1000.0, 1) if len(total) > 1 else None

    # net cross-border long-term flow (foreign into US  −  US abroad)
    net_12 = None
    if into_12 is not None and roll(abroad, 12) is not None:
        net_12 = round(into_12 - roll(abroad, 12), 1)

    by_asset = {}
    for k, obs in legs.items():
        by_asset[k] = {"latest_month_b": round(obs[0][1] / 1000.0, 1) if obs else None,
                       "rolling_12mo_b": roll(obs, 12), "data_asof": asof if obs else None, "unit": "usd_bn"}
    st_12 = roll(st, 12)

    # ── regime: level + sudden-stop / acceleration detector ──
    flags = []
    inflow_positive = (into_12 or 0) > 0
    accel_ratio = (into_3ann / into_12) if (into_12 and into_12 > 0 and into_3ann is not None) else None
    fresh_outflow = last_month is not None and last_month < 0 and (prev_month is not None and prev_month < 0)
    yoy = (round(into_12 - into_12_prior, 1) if (into_12 is not None and into_12_prior is not None) else None)

    if quality["status"] != "fresh" or into_12 is None or into_3ann is None:
        regime = "UNAVAILABLE"
        flags.append("Current monthly observations are incomplete, invalid or outside the publication SLA; regime expired.")
    elif not inflow_positive:
        regime = "PERSISTENT_OUTFLOW"
        flags.append("12-month flows are net NEGATIVE — the world is pulling capital OUT of US long-term assets.")
    elif fresh_outflow or (accel_ratio is not None and accel_ratio < 0):
        regime = "SUDDEN_STOP"
        flags.append("Inflows have flipped to outflows on a 1-3 month basis while the 12mo is still positive — the classic sudden-stop tell.")
    elif accel_ratio is not None and accel_ratio < 0.6:
        regime = "DECELERATING"
        flags.append("3-month run-rate is well below the 12-month trend — foreign funding is fading.")
    elif accel_ratio is not None and accel_ratio > 1.3:
        regime = "ACCELERATING_INFLOW"
        flags.append("3-month run-rate is running hot vs the 12-month trend — foreign money is piling in relative to the trailing twelve months.")
    else:
        regime = "STEADY_INFLOW"
        flags.append("Net foreign purchases are positive and the recent run-rate is near the trailing total.")

    interp = {
        "UNAVAILABLE": "Aligned current observations are required before classifying the transaction trend.",
        "ACCELERATING_INFLOW": "The annualized three-month transaction total exceeds 1.3 times the trailing twelve-month total.",
        "STEADY_INFLOW":       "Net foreign purchases remain positive, with a three-month run-rate near the twelve-month total.",
        "DECELERATING":        "The annualized three-month total is below 0.6 times the positive twelve-month total.",
        "SUDDEN_STOP":         "Recent net transactions turned negative while the twelve-month total remains positive; this does not establish a funding crisis.",
        "PERSISTENT_OUTFLOW":  "The twelve-month net foreign transaction total is nonpositive.",
    }[regime]

    out = {
        "engine": "capital-inflows", "version": VERSION, "ok": quality["status"] in ("fresh", "incomplete"),
        "generated_at": published,
        "quality": quality, "call": None,
        "duration_s": round(time.time() - t0, 1),
        "data_asof": asof,
        "thesis": ("How hard the rest of the world is funding US assets. Level = regime; rate-of-change = the tell. "
                   "These are securities transactions, not all capital flows, holdings changes or a directional asset-price forecast."),
        "headline": {
            "foreign_net_into_us_lt_12mo_b": into_12,
            "net_cross_border_lt_12mo_b": net_12,
            "latest_month_b": last_month,
            "run_rate_3mo_annualized_b": into_3ann,
            "yoy_change_12mo_b": yoy,
            "short_term_treasury_12mo_b": st_12,
        },
        "by_asset_class": by_asset,
        "regime": regime, "regime_interpretation": interp, "flags": flags,
        "history_12mo_rolling_b": [
            {"asof": total[i][0], "rolling_12mo_b": roll(total[i:], 12)}
            for i in range(0, min(18, max(0, len(total) - 11)))
        ],
        "sources": {"release": "FRED release 3 — Treasury International Capital (TIC), net transactions, grand total",
                    "total_series": TOTAL_INTO_US, "asset_series": INTO_US,
                    "short_treasury": SHORT_TREAS, "us_abroad": US_ABROAD},
        "disclaimer": "Macro context from official TIC data — research, not advice.",
        # ops 5623 quality
        "units": "usd_bn",
        "vintage_note": "TIC FRED release 3 is monthly and lags several weeks; data_asof is the observation month, not print day.",

    }
    # Holder identity must reconcile in the same month; no guessed suffix split.
    official, private = rows["official"], rows["private"]
    split_ok = bool(total and official and private)
    gap = (total[0][1]-official[0][1]-private[0][1])/1000 if split_ok else None
    split_ok = split_ok and abs(gap) <= 0.2
    out["holder_splits"] = {"lt_total": {"status": "OK" if split_ok else "UNAVAILABLE",
        "month": asof, "recon_gap_bn": round(gap,3) if gap is not None else None,
        "official": {"latest": round(official[0][1]/1000,1) if split_ok else None,
                     "sum_12m": roll(official,12) if split_ok else None},
        "private": {"latest": round(private[0][1]/1000,1) if split_ok else None,
                    "sum_12m": roll(private,12) if split_ok else None}}}
    if not split_ok:
        quality["missing"] = sorted(set(quality["missing"] + ["reconciled_official_private_split"]))
        if quality["status"] == "fresh": quality["status"] = "incomplete"
        out["regime"] = "UNAVAILABLE"
        out["flags"] = ["Official/private holder split unavailable at this vintage; regime expired."]
        out["regime_interpretation"] = "Official/private holder totals could not be reconciled at this vintage."
    out["sources"].update(canonical_feed=OUT_KEY, vintage_date=vintage, retrieval_date=published,
                           official_series=ids["official"], private_series=ids["private"],
                           definition="TIC net securities transactions at market value; excludes valuation and other position changes.")
    out["field_units"] = {"headline.*_b":"usd_bn", "by_asset_class.*.*_b":"usd_bn", "holder_splits.*.*.latest":"usd_bn"}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str, allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[capital-inflows] asof={asof} into_us_12mo=${into_12}B net=${net_12}B "
          f"3mo_ann=${into_3ann}B regime={regime} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["headline"])}


def lambda_handler(event=None, context=None):
    """Original TIC research; HTTP serves the retained packet without collection."""
    from tic_research import CONTRACT, CURRENT, encoded
    from tic_store import raw_reader, run
    try:
        event=event or {}
        if (event.get('requestContext') or {}).get('http') or event.get('httpMethod'):
            packet=json.loads(raw_reader(S3,BUCKET)(CURRENT))
            if packet.get('contract')!=CONTRACT:raise ValueError('reviewed TIC publication unavailable')
            return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':encoded(packet).decode()}
        result=run(S3,BUCKET,FRED_KEY,context)
        return {'statusCode':200,'body':encoded(result).decode()}
    except Exception as exc:
        print('[tic-research] '+type(exc).__name__)
        return {'statusCode':503,'body':json.dumps({'ok':False,'reason':'Original TIC research unavailable; last verified publication retained.'})}
