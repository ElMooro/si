"""
justhodl-vix-curve v2.0.0 — Bloomberg-grade VIX Term Structure Engine

DATA SOURCE
===========
CBOE Indices CDN — free, no auth, low-latency, no AWS IP throttling:
  VIX9D, VIX, VIX3M, VIX6M, VVIX (vol of vol), VXN (NDX vol), RVX (RUT vol)

WHAT'S COMPUTED
===============
  Term structure spreads (9d_30d, 30d_3m, 3m_6m, 9d_6m)
  Per-spread + composite regimes (BACKWARDATED_STRESS / STEEP_CONTANGO_GRIND / etc.)
  60d z-scores for each index + each spread
  1y and all-time percentile ranks
  Sustained-signal counts (n days backwardated in last 5/20)
  Cross-asset dispersion (VXN-VIX growth premium, RVX-VIX small-cap stress)
  VVIX/VIX ratio (vol of vol — uncertainty signal)
"""
import io, json, os, time, urllib.request, csv as _csv
from datetime import datetime, timezone
import boto3

VERSION = "2.0.0"

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = os.environ.get("S3_KEY", "data/vix-curve.json")
HISTORY_KEY = "data/vix-curve-history.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
HTTP_TIMEOUT = 25

CBOE_INDICES = {
    "vix9d": "VIX9D_History.csv",
    "vix": "VIX_History.csv",
    "vix3m": "VIX3M_History.csv",
    "vix6m": "VIX6M_History.csv",
    "vvix": "VVIX_History.csv",
    "vxn": "VXN_History.csv",
    "rvx": "RVX_History.csv",
}
CBOE_BASE = "https://cdn.cboe.com/api/global/us_indices/daily_prices"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0

def _stdev(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5

def _percentile_rank(xs, value):
    if not xs: return None
    below = sum(1 for x in xs if x < value)
    return below / len(xs)

def _zscore(xs, value):
    if len(xs) < 2: return 0.0
    m = _mean(xs); sd = _stdev(xs)
    return (value - m) / sd if sd > 0 else 0.0


def fetch_cboe_history(filename):
    """Returns list of (date_iso, close) tuples sorted ascending."""
    url = f"{CBOE_BASE}/{filename}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36",
        "Accept": "text/csv",
    })
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        text = r.read().decode("utf-8")
    reader = _csv.DictReader(io.StringIO(text))
    out = []
    for row in reader:
        try:
            d_raw = (row.get("DATE") or row.get("Date") or "").strip()
            if "/" in d_raw:
                m, d, y = d_raw.split("/")
                d_iso = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
            else:
                d_iso = d_raw
            close_raw = (row.get("CLOSE") or row.get("Close") or row.get("VVIX") or "").strip()
            if not close_raw: continue
            out.append((d_iso, float(close_raw)))
        except Exception:
            continue
    out.sort(key=lambda r: r[0])
    return out


def fetch_all_series():
    series = {}
    for key, fname in CBOE_INDICES.items():
        try:
            t0 = time.time()
            rows = fetch_cboe_history(fname)
            series[key] = rows
            print(f"  fetched {key}: {len(rows)} rows in {time.time()-t0:.2f}s")
        except Exception as e:
            print(f"  {key} fetch err: {str(e)[:120]}")
            series[key] = []
    return series


def classify_spread(spread):
    if spread < -3: return "STEEP_CONTANGO"
    if spread < -1: return "CONTANGO"
    if spread <= 0.5: return "FLAT"
    if spread <= 2: return "MILD_BACKWARDATION"
    return "BACKWARDATED"


def composite_regime(vix9d, vix, vix3m, vix6m, vvix):
    spreads = {
        "9d_30d": vix9d - vix,
        "30d_3m": vix - vix3m,
        "3m_6m": vix3m - vix6m,
        "9d_6m": vix9d - vix6m,
    }
    n_inv = sum(1 for s in spreads.values() if s > 0.5)
    vvix_ratio = vvix / vix if vix > 0 else 0
    if vvix_ratio > 7:
        return "VOL_OF_VOL_SPIKE", spreads, vvix_ratio, (
            "VVIX/VIX > 7 — market uncertain about future vol magnitude · expect regime change"
        )
    if n_inv >= 3:
        return "BACKWARDATED_STRESS", spreads, vvix_ratio, (
            "Full-curve inversion — historical bottom signal · trend down until 9d-30d reverts"
        )
    if n_inv >= 1:
        return "BACKWARDATED", spreads, vvix_ratio, (
            "Partial inversion — front-end stress · monitor for full inversion or normalization"
        )
    avg_slope = (spreads["30d_3m"] + spreads["3m_6m"]) / 2
    if avg_slope < -3 and vix < 16:
        return "STEEP_CONTANGO_GRIND", spreads, vvix_ratio, (
            "Steep contango with low VIX — vol-sell carry regime · low realized vol expected"
        )
    if avg_slope < -1:
        return "NORMAL_CONTANGO", spreads, vvix_ratio, (
            "Normal contango · vol pricing balanced across curve · no immediate stress"
        )
    return "FLAT_TRANSITION", spreads, vvix_ratio, (
        "Curve flattening · transition zone · regime shift possible"
    )


def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text):
    if not TELEGRAM_TOKEN: return False
    chat = get_chat_id()
    if not chat: return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": chat, "text": text[:4096],
                            "parse_mode": "Markdown",
                            "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"  telegram err: {str(e)[:80]}")
        return False


def load_prior():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)
        return json.loads(obj["Body"].read()).get("composite_regime")
    except Exception:
        return None


def lambda_handler(event, context):
    started = time.time()
    print(f"=== VIX-CURVE v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    prior = load_prior()
    series = fetch_all_series()

    core = ["vix9d", "vix", "vix3m", "vix6m"]
    if any(len(series.get(k) or []) < 30 for k in core):
        return {"statusCode": 500,
                 "body": json.dumps({"err": "insufficient core history",
                                       "counts": {k: len(series.get(k) or []) for k in core}})}

    indexed = {k: dict(rows) for k, rows in series.items()}
    common_dates = set(indexed["vix"].keys())
    for k in core[1:]:
        common_dates &= set(indexed[k].keys())
    sorted_dates = sorted(common_dates)
    print(f"  {len(sorted_dates)} common date intersections")

    joined = []
    for d in sorted_dates:
        row = {"date": d}
        for k in CBOE_INDICES.keys():
            row[k] = indexed.get(k, {}).get(d)
        joined.append(row)

    latest = joined[-1]
    vix9d = latest["vix9d"]; vix = latest["vix"]
    vix3m = latest["vix3m"]; vix6m = latest["vix6m"]
    vvix = latest.get("vvix"); vxn = latest.get("vxn"); rvx = latest.get("rvx")

    s_9d_30d = vix9d - vix
    s_30d_3m = vix - vix3m
    s_3m_6m = vix3m - vix6m
    s_9d_6m = vix9d - vix6m
    avg_slope = (s_30d_3m + s_3m_6m) / 2

    last_60 = joined[-60:] if len(joined) >= 60 else joined
    vix9d_60 = [r["vix9d"] for r in last_60 if r.get("vix9d") is not None]
    vix_60 = [r["vix"] for r in last_60 if r.get("vix") is not None]
    vix3m_60 = [r["vix3m"] for r in last_60 if r.get("vix3m") is not None]
    spread_30d_3m_60 = [(r["vix"] - r["vix3m"]) for r in last_60
                          if r.get("vix") is not None and r.get("vix3m") is not None]
    spread_9d_30d_60 = [(r["vix9d"] - r["vix"]) for r in last_60
                          if r.get("vix9d") is not None and r.get("vix") is not None]

    last_252 = joined[-252:] if len(joined) >= 252 else joined
    vix_252 = [r["vix"] for r in last_252 if r.get("vix") is not None]
    spread_30d_3m_252 = [(r["vix"] - r["vix3m"]) for r in last_252
                           if r.get("vix") is not None and r.get("vix3m") is not None]

    vix_all = [r["vix"] for r in joined if r.get("vix") is not None]

    last_5 = joined[-5:]; last_20 = joined[-20:]
    n_5_back = sum(1 for r in last_5 if (r.get("vix") or 0) > (r.get("vix3m") or 0))
    n_20_back = sum(1 for r in last_20 if (r.get("vix") or 0) > (r.get("vix3m") or 0))
    n_5_front_inv = sum(1 for r in last_5 if (r.get("vix9d") or 0) > (r.get("vix") or 0))
    n_20_front_inv = sum(1 for r in last_20 if (r.get("vix9d") or 0) > (r.get("vix") or 0))

    regime, spreads_dict, vvix_ratio, signal = composite_regime(
        vix9d, vix, vix3m, vix6m, vvix or 0)
    spread_regimes = {k: classify_spread(v) for k, v in spreads_dict.items()}

    dod = {}
    if len(joined) >= 2:
        prev = joined[-2]
        dod = {
            "vix_change": round(vix - (prev.get("vix") or vix), 2),
            "vix9d_change": round(vix9d - (prev.get("vix9d") or vix9d), 2),
            "vix3m_change": round(vix3m - (prev.get("vix3m") or vix3m), 2),
            "spread_30d_3m_change": round(
                s_30d_3m - ((prev.get("vix") or 0) - (prev.get("vix3m") or 0)), 2),
        }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "cdn.cboe.com/api/global/us_indices/daily_prices",
        "data_date": latest["date"],
        "elapsed_seconds": round(time.time() - started, 2),
        "current": {
            "vix9d": round(vix9d, 2),
            "vix": round(vix, 2),
            "vix3m": round(vix3m, 2),
            "vix6m": round(vix6m, 2),
            "vvix": round(vvix, 2) if vvix else None,
            "vxn": round(vxn, 2) if vxn else None,
            "rvx": round(rvx, 2) if rvx else None,
            "vvix_vix_ratio": round(vvix_ratio, 2),
        },
        "spreads": {
            "9d_vs_30d": round(s_9d_30d, 2),
            "30d_vs_3m": round(s_30d_3m, 2),
            "3m_vs_6m": round(s_3m_6m, 2),
            "9d_vs_6m": round(s_9d_6m, 2),
            "avg_slope_30d_to_6m": round(avg_slope, 2),
        },
        "spread_regimes": spread_regimes,
        "composite_regime": regime,
        "composite_signal": signal,
        "regime_changed_from_prior": (prior != regime) if prior else False,
        "z_scores_60d": {
            "vix9d_z": round(_zscore(vix9d_60, vix9d), 2),
            "vix_z": round(_zscore(vix_60, vix), 2),
            "vix3m_z": round(_zscore(vix3m_60, vix3m), 2),
            "spread_30d_3m_z": round(_zscore(spread_30d_3m_60, s_30d_3m), 2),
            "spread_9d_30d_z": round(_zscore(spread_9d_30d_60, s_9d_30d), 2),
        },
        "percentile_ranks": {
            "vix_pct_1y": round((_percentile_rank(vix_252, vix) or 0) * 100, 1),
            "vix_pct_all_time": round((_percentile_rank(vix_all, vix) or 0) * 100, 1),
            "spread_30d_3m_pct_1y": round((_percentile_rank(spread_30d_3m_252, s_30d_3m) or 0) * 100, 1),
        },
        "sustained_signals": {
            "n_5d_backwardated_30d_3m": n_5_back,
            "n_20d_backwardated_30d_3m": n_20_back,
            "n_5d_front_inverted_9d_30d": n_5_front_inv,
            "n_20d_front_inverted_9d_30d": n_20_front_inv,
        },
        "cross_asset_dispersion": {
            "vxn_minus_vix": round(vxn - vix, 2) if vxn else None,
            "rvx_minus_vix": round(rvx - vix, 2) if rvx else None,
            "nasdaq_stress_premium": ("ELEVATED" if (vxn and vxn - vix > 4)
                                       else "NORMAL" if vxn else None),
            "small_cap_stress_premium": ("ELEVATED" if (rvx and rvx - vix > 6)
                                          else "NORMAL" if rvx else None),
        },
        "day_over_day": dod,
        "thresholds": {
            "steep_contango": -3, "contango_min": -1,
            "flat_max": 0.5, "backwardation_min": 2,
            "vvix_vix_spike": 7,
        },
        "n_history_days": len(joined),
        "history_first_date": joined[0]["date"],
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ vix-curve.json written ({round(len(json.dumps(payload))/1024,1)} KB)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Full chart history (last ~7 years)
    try:
        chart_window = joined[-1764:] if len(joined) > 1764 else joined
        chart_data = {
            "generated_at": payload["generated_at"],
            "n_days": len(chart_window),
            "first_date": chart_window[0]["date"], "last_date": chart_window[-1]["date"],
            "dates": [r["date"] for r in chart_window],
            "series": {k: [r.get(k) for r in chart_window] for k in CBOE_INDICES.keys()},
            "spreads": {
                "30d_vs_3m": [
                    ((r.get("vix") or 0) - (r.get("vix3m") or 0))
                    if (r.get("vix") is not None and r.get("vix3m") is not None) else None
                    for r in chart_window
                ],
                "9d_vs_30d": [
                    ((r.get("vix9d") or 0) - (r.get("vix") or 0))
                    if (r.get("vix9d") is not None and r.get("vix") is not None) else None
                    for r in chart_window
                ],
            },
        }
        s3.put_object(Bucket=S3_BUCKET, Key=HISTORY_KEY,
            Body=json.dumps(chart_data, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=3600")
        print(f"  ✓ vix-curve-history.json written")
    except Exception as e:
        print(f"  history put err: {str(e)[:120]}")

    alert_sent = False
    if (prior and prior != regime) or regime in (
        "BACKWARDATED_STRESS", "BACKWARDATED", "VOL_OF_VOL_SPIKE"
    ):
        lines = [
            f"📈 *VIX Term Structure · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
            f"⚡ {regime}",
            f"_{signal}_\n",
            f"📊 VIX: *{vix:.1f}* (z={payload['z_scores_60d']['vix_z']:.2f}, {payload['percentile_ranks']['vix_pct_1y']:.0f}% pct 1y)",
            f"📊 VIX9D: {vix9d:.1f} · VIX3M: {vix3m:.1f} · VIX6M: {vix6m:.1f}",
            f"📐 Spread (VIX-VIX3M): *{s_30d_3m:+.2f}* ({spread_regimes['30d_3m']})",
            f"📐 Slope: {avg_slope:+.2f}",
        ]
        if vvix: lines.append(f"\n_VVIX: {vvix:.1f} (ratio: {vvix_ratio:.1f})_")
        if prior and prior != regime:
            lines.insert(2, f"_(was {prior})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print(f"  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "data_date": latest["date"],
        "vix": round(vix, 2), "vix3m": round(vix3m, 2),
        "spread_30d_3m": round(s_30d_3m, 2),
        "composite_regime": regime,
        "regime_changed": prior != regime if prior else False,
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
