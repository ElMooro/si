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

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/capital-inflows.json"
VERSION = "1.0.0"
FRED_KEY = os.environ.get("FRED_API_KEY", "")

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


def fred(series_id, limit=40):
    """Monthly observations newest-first as [(date, value_$M)]; [] on failure."""
    if not FRED_KEY:
        return []
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
               f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
        j = json.loads(urllib.request.urlopen(url, timeout=25).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", []) if o["value"] != "."]
    except Exception as e:
        print(f"[fred] {series_id}: {str(e)[:60]}")
        return []


def roll(obs, n):
    """Sum of the most recent n monthly values ($M -> $B)."""
    return round(sum(v for _, v in obs[:n]) / 1000.0, 1) if len(obs) >= n else None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    total = fred(TOTAL_INTO_US)
    if not total:
        out = {"engine": "capital-inflows", "version": VERSION, "ok": False,
               "error": "FRED TIC total series unavailable",
               "generated_at": datetime.now(timezone.utc).isoformat()}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                      ContentType="application/json")
        print("[capital-inflows] FRED unavailable")
        return {"statusCode": 502, "body": "no data"}

    asof = total[0][0]
    legs = {k: fred(sid) for k, sid in INTO_US.items()}
    st = fred(SHORT_TREAS)
    abroad = fred(US_ABROAD)

    # headline foreign net purchases of ALL US long-term securities
    into_12 = roll(total, 12)
    into_3ann = round(sum(v for _, v in total[:3]) / 1000.0 * 4, 1) if len(total) >= 3 else None
    into_12_prior = roll(total[12:], 12) if len(total) >= 24 else None      # the 12mo ending a year ago
    last_month = round(total[0][1] / 1000.0, 1)
    prev_month = round(total[1][1] / 1000.0, 1) if len(total) > 1 else None

    # net cross-border long-term flow (foreign into US  −  US abroad)
    net_12 = None
    if abroad and into_12 is not None:
        net_12 = round(into_12 - (roll(abroad, 12) or 0), 1)

    by_asset = {}
    for k, obs in legs.items():
        by_asset[k] = {"latest_month_b": round(obs[0][1] / 1000.0, 1) if obs else None,
                       "rolling_12mo_b": roll(obs, 12)}
    st_12 = roll(st, 12)

    # ── regime: level + sudden-stop / acceleration detector ──
    flags = []
    inflow_positive = (into_12 or 0) > 0
    accel_ratio = (into_3ann / into_12) if (into_12 and into_12 > 0 and into_3ann is not None) else None
    fresh_outflow = last_month < 0 and (prev_month is not None and prev_month < 0)
    yoy = (round(into_12 - into_12_prior, 1) if (into_12 is not None and into_12_prior is not None) else None)

    if not inflow_positive:
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
        flags.append("3-month run-rate is running hot vs the 12-month trend — foreign money is piling in (bull-run funding).")
    else:
        regime = "STEADY_INFLOW"
        flags.append("Foreign funding is steady and supportive.")

    interp = {
        "ACCELERATING_INFLOW": "Strong, accelerating foreign funding — supportive of US assets and the dollar; the durable-rally condition.",
        "STEADY_INFLOW":       "Healthy foreign funding of US assets — a tailwind, not a flag.",
        "DECELERATING":        "Foreign funding is fading — not yet a stop, but the bull-run tailwind is weakening; watch the next prints.",
        "SUDDEN_STOP":         "Foreign capital is reversing out of US assets — the funding-crisis setup. Dollar-squeeze and yield-spike risk rises.",
        "PERSISTENT_OUTFLOW":  "Sustained net outflows — US assets are being de-funded by the rest of the world.",
    }[regime]

    out = {
        "engine": "capital-inflows", "version": VERSION, "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "data_asof": asof,
        "thesis": ("How hard the rest of the world is funding US assets. Level = regime; rate-of-change = the tell. "
                   "Accelerating inflows are bull-run fuel; a sudden stop is a funding-crisis trigger."),
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
            {"asof": total[i][0], "rolling_12mo_b": round(sum(v for _, v in total[i:i + 12]) / 1000.0, 1)}
            for i in range(0, min(18, max(0, len(total) - 12)))
        ],
        "sources": {"release": "FRED release 3 — Treasury International Capital (TIC), net transactions, grand total",
                    "total_series": TOTAL_INTO_US, "asset_series": INTO_US,
                    "short_treasury": SHORT_TREAS, "us_abroad": US_ABROAD},
        "disclaimer": "Macro context from official TIC data — research, not advice.",
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[capital-inflows] asof={asof} into_us_12mo=${into_12}B net=${net_12}B "
          f"3mo_ann=${into_3ann}B regime={regime} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["headline"])}
