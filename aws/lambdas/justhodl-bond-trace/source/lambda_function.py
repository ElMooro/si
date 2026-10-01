"""Daily bond research: legacy ETF/OAS heuristic and FINRA aggregate observations.

These are descriptive, unqualified research inputs. FINRA aggregate reports
are not individual trades, executable quotes, fund flows or dealer positions.
Each source retains its own observation date and contract. Neither the legacy
score nor the aggregate layer has validated Calls or sizing authority.
The existing normal schedule and acquisition inputs are retained.
"""
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/bond-trace.json"
TRACE_KEY = "data/trace-bond-prints.json"
TRACE_HISTORY_KEY = "data/trace-bond-prints-history.json"  # legacy; not migrated
FINRA_HISTORY_KEY = "data/finra-aggregate-history-v1.json"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


def http_get(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[http] {e}")
        return None


def fetch_aggs(ticker, days_back=90):
    if not POLYGON_KEY: return None
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start}/{end}?adjusted=true&limit=200&apiKey={POLYGON_KEY}")
    data = http_get(url)
    if not data or "results" not in data: return None
    return data["results"]


def fred_get(series_id, limit=30):
    if not FRED_KEY: return None
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
            f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
            obs = data.get("observations", [])
            return [{"date": o["date"], "value": float(o["value"])}
                     for o in obs if o.get("value") and o["value"] != "."]
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
        return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=14400"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def _last_trade_date():
    """Return the most recent weekday (YYYY-MM-DD) for TRACE queries."""
    d = datetime.now(timezone.utc).date()
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d -= timedelta(days=1)
    return d.isoformat()


def _num(x):
    """Coerce to float, or None."""
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _finite_measurement(value, count=False):
    if type(value) not in (int, float):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    if count and (number > 9007199254740991 or not number.is_integer()):
        return None
    return int(number) if count else number


def _category_breadth(row):
    counts = [_finite_measurement(row.get(key), count=True)
              for key in ('advances', 'declines', 'unchanged')]
    if any(value is None for value in counts):
        return None
    total = sum(counts)
    if total <= 0 or total > 9007199254740991:
        return None
    return round((counts[0] - counts[1]) / total * 100, 2)


def _dated_history(history, key, observation, value, response_hash):
    """Only this definition and unique earlier dates enter a comparison.

    Historical packets from the former TRACE/dealer-positioning definition
    are neither read nor migrated. Same-date refreshes replace their entry.
    """
    valid = {}
    for row in history.get(key, []) if isinstance(history.get(key), list) else []:
        if not isinstance(row, dict) or row.get('contract_version') != 'finra-aggregates-v1':
            continue
        day = row.get('observation_date')
        try:
            if type(day) is not str or datetime.strptime(day, '%Y-%m-%d').date().isoformat() != day:
                continue
        except ValueError:
            continue
        number = row.get('value')
        if type(number) not in (int, float):
            continue
        try:
            number = float(number)
        except (ValueError, OverflowError):
            continue
        if not math.isfinite(number):
            continue
        if (key == 'treasury' and not 0 <= number <= 1) or (key == 'corporate' and not -100 <= number <= 100):
            continue
        # A duplicated date in imported history is ambiguous, not extra evidence.
        valid[day] = None if day in valid else dict(row)
    earlier = [valid[day] for day in sorted(valid) if day < observation and valid[day] is not None]
    prior_value = earlier[-1]['value'] if earlier else None
    latest = {
        'contract_version': 'finra-aggregates-v1', 'observation_date': observation,
        'value': value, 'response_sha256': response_hash,
    }
    # A late older response must not erase already retained newer observations.
    valid[observation] = latest if value is not None else None
    history[key] = [valid[day] for day in sorted(valid) if valid[day] is not None][-30:]
    return prior_value, len(earlier)


def build_trace_layer(prior):
    """Publish explicitly dated aggregates; unavailable per-print fields stay null."""
    try:
        import finra_trace
    except Exception as error:
        print(f'[finra-layer] helper unavailable: {type(error).__name__}')
        return None
    try:
        treasury = finra_trace.fetch_treasury_latest()
        breadth = finra_trace.fetch_corporate_breadth()
        if treasury is None and breadth is None:
            return None
        buckets = []
        shares = []
        for row in (treasury or {}).get('rows', []):
            dc = _finite_measurement(row.get('dealerCustomerVolume'))
            ats = _finite_measurement(row.get('atsInterdealerVolume'))
            complete = dc is not None and ats is not None
            total = dc + ats if complete else None
            if total is not None and not math.isfinite(total):
                total = None
            share = dc / total if total is not None and total > 0 else None
            benchmark = row.get('benchmark')
            category = row.get('productCategory')
            maturity = row.get('yearsToMaturity')
            scope_known = ((category in {'Bills', 'FRNs'} and benchmark is None and maturity is None) or
                           (category in {'Nominal Coupons', 'TIPS'} and benchmark in ('On-the-run', 'Off-the-run') and type(maturity) is str and bool(maturity.strip())))
            if not scope_known:
                share = None
            shares.append((dc, ats, total, scope_known))
            buckets.append({
                'product_category': category,
                'years_to_maturity': row.get('yearsToMaturity'),
                'observation_date': row['tradeDate'],
                'benchmark': benchmark,
                'vwap': _finite_measurement(row.get('volumeWeightedAveragePrice')),
                'dealer_customer_volume': dc,
                'ats_interdealer_volume': ats,
                'volume_unit': 'source_native_unverified',
                'dealer_customer_volume_share': round(share, 6) if share is not None else None,
                'dealer_share': round(share, 6) if share is not None else None,
                'vwap_dislocation': None,
                'source_row': row,
            })
        aggregate_share = None
        # Never sum unknown, missing or overlapping category identities. The
        # shared adapter already rejects duplicate category/maturity/benchmark.
        if shares and all(total is not None and known for dc, ats, total, known in shares):
            try:
                numerator = math.fsum(dc for dc, ats, total, known in shares)
                denominator = math.fsum(total for dc, ats, total, known in shares)
                if denominator > 0 and math.isfinite(numerator) and math.isfinite(denominator):
                    aggregate_share = round(numerator / denominator, 6)
            except (OverflowError, ValueError):
                pass
        categories = []
        all_breadth = None
        for row in (breadth or {}).get('rows', []):
            value = _category_breadth(row)
            categories.append({'product_category': row['productCategory'],
                               'observation_date': row['tradeReportDate'],
                               'net_advancing_pct': value, 'source_row': row})
            if row['productCategory'] == 'all securities':
                all_breadth = value
        history = get_s3_json(FINRA_HISTORY_KEY, {}) or {}
        if not isinstance(history, dict) or history.get('contract_version') != 'finra-aggregates-v1':
            history = {'contract_version': 'finra-aggregates-v1', 'treasury': [], 'corporate': []}
        breadth_momentum = None
        history_counts = {'treasury': 0, 'corporate': 0}
        if treasury:
            _, history_counts['treasury'] = _dated_history(history, 'treasury', treasury['observation_date'], aggregate_share, treasury['response_sha256'])
        if breadth:
            previous, history_counts['corporate'] = _dated_history(history, 'corporate', breadth['observation_date'], all_breadth, breadth['response_sha256'])
            if previous is not None and all_breadth is not None:
                breadth_momentum = round(all_breadth - previous, 2)
        unavailable = {
            'trace_prints': 'No documented per-print dataset or entitlement is configured; no endpoint is queried.',
            'vwap_dislocation': 'benchmark is an on/off-the-run classification, not a numerical reference price.',
            'dealer_positioning_z': 'Dealer-customer volume share is not dealer inventory or directional positioning.',
        }
        quality = {
            'status': 'unqualified', 'calls_eligible': False, 'sizing_eligible': False,
            'reason': 'Documented aggregate adapter; normal source delivery, expected coverage, source units and predictive validity remain unqualified.',
            'treasury_available': treasury is not None, 'corporate_available': breadth is not None,
            'unavailable': unavailable,
        }
        generated_at = datetime.now(timezone.utc).isoformat()
        standalone = {
            'schema_version': '2.0', 'contract_version': 'finra-aggregates-v1',
            'generated_at': generated_at, 'trade_date': (treasury or {}).get('observation_date'),
            'provenance': 'finra-daily-aggregates', 'quality': quality,
            'treasury': {'n_buckets': len(buckets), 'buckets': buckets,
                         'observation_date': (treasury or {}).get('observation_date'),
                         'dealer_customer_volume_share': aggregate_share,
                         'dealer_share': aggregate_share, 'vwap_dislocation': None,
                         'share_definition': 'dealerCustomerVolume / (dealerCustomerVolume + atsInterdealerVolume), over returned valid distinct buckets; not directional flow'},
            'corporate': {'breadth': breadth, 'categories': categories,
                          'observation_date': (breadth or {}).get('observation_date'),
                          'breadth_net_pct': all_breadth,
                          'definition': '100 * (advances - declines) / (advances + declines + unchanged), all securities row only'},
            'source_snapshots': {'treasury': treasury, 'corporate': breadth},
            'trace_aggregates': None,
            'stress_derived': {'dealer_positioning_z': None, 'breadth_momentum': breadth_momentum, 'vwap_dislocation': None},
            'history': {'key': FINRA_HISTORY_KEY, 'earlier_unique_observations': history_counts},
            'notes': 'Daily reported aggregates, not individual prints or executable quotes. Category subsets are never added to all securities. Per-leg observation dates remain separate.',
        }
        try:
            put_s3_json(FINRA_HISTORY_KEY, history, cache='no-cache')
            put_s3_json(TRACE_KEY, standalone)
        except Exception as error:
            print(f'[finra-layer] publication failed: {type(error).__name__}')
            return None
        return {
            'provenance': 'finra-daily-aggregates', 'contract_version': 'finra-aggregates-v1',
            'trade_date': (treasury or {}).get('observation_date'),
            'observation_dates': {'treasury': (treasury or {}).get('observation_date'), 'corporate': (breadth or {}).get('observation_date')},
            'quality': quality, 'dealer_positioning_z': None,
            'dealer_customer_volume_share': aggregate_share,
            'treasury_dealer_share': aggregate_share,
            'breadth_momentum': breadth_momentum, 'vwap_dislocation': None,
            'treasury_n_buckets': len(buckets), 'corporate_breadth_net_pct': all_breadth,
            'trace_n_prints': None, 'trace_total_volume': None, 'standalone_key': TRACE_KEY,
        }
    except Exception as error:
        print(f'[finra-layer] unavailable: {type(error).__name__}')
        return None


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}"); return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[bond-trace] starting")

    prior = get_s3_json(S3_KEY, {}) or {}

    # Fetch HYG (high yield), LQD (investment grade), JNK (HY alt), TLT (long Tsy)
    hyg = fetch_aggs("HYG", 90)
    lqd = fetch_aggs("LQD", 90)
    jnk = fetch_aggs("JNK", 90)
    tlt = fetch_aggs("TLT", 90)
    angl = fetch_aggs("ANGL", 90)  # fallen angels

    out = {
        "schema_version": "2.1",
        "method": "legacy_bond_proxy+documented_finra_aggregates",
        "call": None,
        "calls_eligible": False,
        "sizing_eligible": False,
        "quality": {"status": "unqualified", "reason": "Legacy ETF/OAS heuristic is not calibrated; time alignment, source units, missingness and normal delivery require qualification."},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    def closes(bars):
        return [b.get("c") for b in (bars or []) if b.get("c")]

    hyg_c = closes(hyg)
    lqd_c = closes(lqd)
    jnk_c = closes(jnk)
    tlt_c = closes(tlt)

    def pct_chg(arr, lookback):
        if not arr or len(arr) <= lookback: return None
        return round((arr[-1] / arr[-lookback-1] - 1) * 100, 2)

    out["hyg"] = {
        "price": hyg_c[-1] if hyg_c else None,
        "perf_5d_pct": pct_chg(hyg_c, 5),
        "perf_30d_pct": pct_chg(hyg_c, 30),
        "perf_90d_pct": pct_chg(hyg_c, 89) if hyg_c and len(hyg_c) >= 90 else None,
    }
    out["lqd"] = {
        "price": lqd_c[-1] if lqd_c else None,
        "perf_5d_pct": pct_chg(lqd_c, 5),
        "perf_30d_pct": pct_chg(lqd_c, 30),
        "perf_90d_pct": pct_chg(lqd_c, 89) if lqd_c and len(lqd_c) >= 90 else None,
    }

    # HYG/LQD ratio — falling ratio = credit underperforming = stress
    ratio_5d_pct = None
    ratio_30d_pct = None
    if hyg_c and lqd_c and len(hyg_c) >= 30 and len(lqd_c) >= 30:
        ratio_5d_pct = round((hyg_c[-1]/lqd_c[-1]) / (hyg_c[-6]/lqd_c[-6]) * 100 - 100, 2)
        ratio_30d_pct = round((hyg_c[-1]/lqd_c[-1]) / (hyg_c[-31]/lqd_c[-31]) * 100 - 100, 2) if len(hyg_c) > 30 and len(lqd_c) > 30 else None
    out["hyg_lqd_ratio"] = {
        "value": round(hyg_c[-1]/lqd_c[-1], 4) if (hyg_c and lqd_c) else None,
        "change_5d_pct": ratio_5d_pct,
        "change_30d_pct": ratio_30d_pct,
    }

    # JNK divergence vs HYG (both should move together; divergence = early stress)
    if jnk_c and hyg_c and len(jnk_c) >= 5 and len(hyg_c) >= 5:
        jnk_5 = pct_chg(jnk_c, 5)
        hyg_5 = pct_chg(hyg_c, 5)
        if jnk_5 is not None and hyg_5 is not None:
            out["jnk_hyg_divergence_5d_pct"] = round(jnk_5 - hyg_5, 3)

    # TLT (long Treasury) — if TLT rising while HYG falling = risk-off flight
    if tlt_c and len(tlt_c) >= 5:
        out["tlt_perf_5d_pct"] = pct_chg(tlt_c, 5)

    # Pull credit OAS from FRED (already used in cds-proxy)
    hy_oas = fred_get("BAMLH0A0HYM2", limit=30)
    if hy_oas and len(hy_oas) > 5:
        out["hy_oas_pct"] = round(hy_oas[0]["value"], 2)
        out["hy_oas_5d_change_bp"] = round((hy_oas[0]["value"] - hy_oas[5]["value"]) * 100, 1) if len(hy_oas) > 5 else None
        out["hy_oas_30d_change_bp"] = round((hy_oas[0]["value"] - hy_oas[29]["value"]) * 100, 1) if len(hy_oas) > 29 else None

    # Compute stress score
    score = 0
    reasons = []

    # 1. HYG falling materially
    if out["hyg"].get("perf_5d_pct") is not None and out["hyg"]["perf_5d_pct"] < -1.5:
        score += 25; reasons.append(f"HYG {out['hyg']['perf_5d_pct']:+.2f}% in 5d (credit selling)")
    elif out["hyg"].get("perf_5d_pct") is not None and out["hyg"]["perf_5d_pct"] < -0.5:
        score += 12

    # 2. HYG/LQD ratio collapsing
    if ratio_5d_pct is not None and ratio_5d_pct < -1:
        score += 25; reasons.append(f"HYG/LQD ratio {ratio_5d_pct:+.2f}% in 5d (credit under-perf IG)")
    elif ratio_30d_pct is not None and ratio_30d_pct < -2:
        score += 15; reasons.append(f"HYG/LQD 30d {ratio_30d_pct:+.2f}% (sustained credit selling)")

    # 3. HY OAS widening hard
    if out.get("hy_oas_5d_change_bp") is not None and out["hy_oas_5d_change_bp"] > 30:
        score += 25; reasons.append(f"HY OAS +{out['hy_oas_5d_change_bp']:.0f}bp in 5d (panic widening)")
    elif out.get("hy_oas_5d_change_bp") is not None and out["hy_oas_5d_change_bp"] > 15:
        score += 12

    # 4. TLT rising while HYG falling = risk-off rotation
    tp = out.get("tlt_perf_5d_pct"); hp = out["hyg"].get("perf_5d_pct")
    if tp is not None and hp is not None and tp > 1 and hp < -1:
        score += 15; reasons.append(f"TLT +{tp:.1f}% while HYG {hp:+.1f}% (risk-off rotation)")

    # 5. JNK-HYG divergence (JNK weaker = junk-specific stress)
    jh = out.get("jnk_hyg_divergence_5d_pct", 0)
    if jh < -0.5:
        score += 10; reasons.append(f"JNK-HYG divergence {jh:+.2f}% (lower-quality stress)")

    score = max(0, min(100, score))
    regime = ("BOND_PANIC" if score >= 70 else
                "STRESSED" if score >= 45 else
                "ELEVATED" if score >= 20 else
                "CALM")

    out["composite_stress"] = score
    out["regime"] = regime
    out["top_reasons"] = reasons
    out["interpretation"] = (
        "Legacy credit proxy exceeds its highest heuristic threshold; no equity drawdown probability is established." if score >= 70 else
        "Credit selling underway. Watch HYG/LQD ratio for stabilization." if score >= 45 else
        "Some credit weakness emerging. Monitor for acceleration." if score >= 20 else
        "Legacy credit proxy is below its first heuristic threshold; missing inputs and unvalidated calibration limit interpretation."
    )
    out["notes"] = ("Proxy from HYG/LQD/JNK/TLT ETFs + ICE BofA HY OAS. "
                     "The FINRA layer contains documented daily aggregates, not individual prints; source qualification remains open.")
    out["duration_s"] = round(time.time()-t0, 1)

    # Documented aggregate layer; the legacy proxy remains separate and unqualified.
    trace_layer = build_trace_layer(prior)
    if trace_layer is not None:
        out["trace"] = trace_layer
    else:
        out["trace"] = {
            "provenance": "proxy-fallback",
            "note": ("FINRA aggregate adapter unavailable; proxy fields above "
                     "remain an unqualified heuristic."),
        }

    put_s3_json(S3_KEY, out)
    print(f"[bond-trace] stress={score} regime={regime}")

    # Alerts
    try:
        prior_regime = prior.get("regime")
        if prior_regime and prior_regime != regime:
            maybe_telegram(
                f"📉 <b>BOND/CREDIT REGIME CHANGE</b>\n"
                f"{prior_regime} → <b>{regime}</b> · stress {score}\n"
                + ("\n".join(f"• {r}" for r in reasons[:4]))
            )

        # HY OAS panic
        if out.get("hy_oas_5d_change_bp", 0) and out["hy_oas_5d_change_bp"] > 30 \
            and (prior.get("hy_oas_5d_change_bp") or 0) <= 30:
            maybe_telegram(
                f"🚨 <b>HY OAS PANIC WIDENING</b>\n"
                f"+{out['hy_oas_5d_change_bp']:.0f}bp in 5d (now {out.get('hy_oas_pct')}%)\n"
                f"This is a heuristic threshold, not a validated equity drawdown probability."
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "stress": score, "regime": regime}),
    }
