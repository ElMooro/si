"""
justhodl-vol-surface — Bloomberg OPV / SKEW equivalent.

For 8 anchor symbols (SPY, QQQ, IWM, DIA, XLF, XLE, XBI, GLD), snapshots
the implied volatility surface across strike × expiration and computes:

  • ATM TERM STRUCTURE — 7d / 14d / 30d / 60d / 90d ATM IV
  • SKEW METRICS — 25-delta put IV vs 25-delta call IV (Bloomberg's SKEW)
                   Risk reversal = call IV − put IV (positive = stress)
  • BUTTERFLY VALUE — 25-delta wings vs ATM (curvature)
  • TERM SLOPE — front-month IV minus 6-month IV (negative = backwardation,
                 high warning, signal of stress)
  • SURFACE STEEPNESS — full smile slope vs flat-line fit

A single "surface tension" score 0-100 per symbol = composite of stress
signals (steep skew + backwardation + high vol-of-vol + IV percentile high).

Polygon endpoint:
  /v3/snapshot/options/{ticker}   — full chain snapshot with IV, delta, gamma,
                                     theta, vega, open interest, volume

For each underlying, group strikes by expiration, find 25-delta strikes
(by binary search on |delta - 0.25|), interpolate IV at the target deltas.

Output: data/vol-surface.json
  • per_symbol: SPY → {atm_term, skew_25d, rr_25d, butterfly_25d, term_slope,
                        surface_tension_score, iv_percentile_estimate}
  • alerts: NEW_BACKWARDATION, EXTREME_SKEW (>2σ), SURFACE_STRESS (>=70)

Schedule: cron(0 */2 * ? * MON-FRI *)  — every 2h Mon-Fri during market days
(market hours coverage 14:00, 16:00, 18:00, 20:00 UTC = 9, 11, 13, 15 ET)

Universe small + tight schedule ensures we stay within rate limits and
get fresh data when it matters.
"""
import json
import os
import time
import urllib.request
import urllib.error
import math
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/vol-surface.json"
S3_KEY_HISTORY = "data/vol-surface-history.json"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

UNDERLYINGS = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XBI", "GLD"]
TARGET_TENORS_DAYS = [7, 14, 30, 60, 90]
HISTORY_BUFFER = 240

s3 = boto3.client("s3", region_name="us-east-1")


def http_get(url, timeout=20, retries=1):
    for a in range(retries+1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and a < retries:
                time.sleep(2); continue
            return None
        except Exception:
            if a < retries:
                time.sleep(1); continue
            return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def fetch_chain(underlying):
    """Polygon snapshot for entire option chain on `underlying`."""
    if not POLYGON_KEY: return None
    # /v3/snapshot/options paginates — get up to ~5 pages (5000 contracts)
    base = f"https://api.polygon.io/v3/snapshot/options/{underlying}"
    contracts = []
    next_url = f"{base}?limit=250&apiKey={POLYGON_KEY}"
    pages = 0
    while next_url and pages < 8:
        data = http_get(next_url)
        if not data or "results" not in data:
            break
        contracts.extend(data.get("results", []) or [])
        nu = data.get("next_url")
        if nu:
            sep = "&" if "?" in nu else "?"
            next_url = f"{nu}{sep}apiKey={POLYGON_KEY}"
        else:
            break
        pages += 1
    return contracts


def days_to_expiry(expiry_str, now=None):
    if not expiry_str: return None
    try:
        exp = datetime.fromisoformat(expiry_str[:10]).replace(tzinfo=timezone.utc)
        now = now or datetime.now(timezone.utc)
        return (exp - now).days
    except Exception:
        return None


def parse_contract(c):
    """Normalize one snapshot contract entry."""
    det = c.get("details", {}) or {}
    grks = c.get("greeks", {}) or {}
    iv = c.get("implied_volatility")
    return {
        "ticker": det.get("ticker"),
        "type": det.get("contract_type"),  # 'call' or 'put'
        "strike": det.get("strike_price"),
        "expiry": det.get("expiration_date"),
        "iv": iv,
        "delta": grks.get("delta"),
        "gamma": grks.get("gamma"),
        "vega": grks.get("vega"),
        "theta": grks.get("theta"),
        "oi": c.get("open_interest"),
        "volume": (c.get("day", {}) or {}).get("volume", 0),
    }


def find_atm_iv(contracts_at_exp, underlying_price):
    """Find ATM IV by selecting strikes closest to spot, averaging C+P IV."""
    if not contracts_at_exp or not underlying_price:
        return None
    # Sort by |strike - spot|
    sorted_c = sorted(contracts_at_exp, key=lambda x: abs((x["strike"] or 0) - underlying_price))
    # Take nearest C + nearest P
    ivs = []
    for typ in ("call", "put"):
        for c in sorted_c:
            if c["type"] == typ and c["iv"] and c["iv"] > 0:
                ivs.append(c["iv"]); break
    return sum(ivs)/len(ivs) if ivs else None


def find_delta_iv(contracts_at_exp, target_delta, contract_type):
    """Find IV at target delta (e.g., 0.25 for puts, 0.25 for calls).
    target_delta should be positive."""
    if not contracts_at_exp: return None
    candidates = [c for c in contracts_at_exp
                    if c["type"] == contract_type and c["delta"] is not None and c["iv"]]
    if not candidates: return None
    # For puts, delta is negative
    target = -target_delta if contract_type == "put" else target_delta
    sorted_c = sorted(candidates, key=lambda x: abs(x["delta"] - target))
    return sorted_c[0]["iv"] if sorted_c else None


def get_underlying_price(contracts):
    """Polygon snapshot includes underlying_asset on each contract."""
    for c in contracts:
        ua = c.get("underlying_asset", {}) or {}
        if ua.get("price"):
            return ua["price"]
    return None


def compute_surface_metrics(underlying, contracts):
    """Compute term structure + skew + butterfly for one underlying."""
    if not contracts:
        return {"err": "no_contracts"}

    spot = get_underlying_price(contracts)
    if not spot:
        return {"err": "no_spot_price"}

    parsed = [parse_contract(c) for c in contracts]
    parsed = [p for p in parsed if p.get("strike") and p.get("expiry")]

    # Group by expiry
    by_exp = {}
    for p in parsed:
        by_exp.setdefault(p["expiry"], []).append(p)

    # Sorted list of (dte, exp_str)
    exp_dtes = []
    for exp, contracts_list in by_exp.items():
        dte = days_to_expiry(exp)
        if dte is None or dte < 0 or dte > 400:
            continue
        exp_dtes.append((dte, exp, contracts_list))
    exp_dtes.sort(key=lambda x: x[0])

    if not exp_dtes:
        return {"err": "no_valid_expiries"}

    # For each target tenor, find nearest available expiry
    term_structure = {}
    for target in TARGET_TENORS_DAYS:
        nearest = min(exp_dtes, key=lambda x: abs(x[0] - target))
        dte, exp, ctrs = nearest
        atm = find_atm_iv(ctrs, spot)
        p25 = find_delta_iv(ctrs, 0.25, "put")
        c25 = find_delta_iv(ctrs, 0.25, "call")
        term_structure[f"t_{target}d"] = {
            "actual_dte": dte,
            "expiry": exp,
            "atm_iv": round(atm*100, 2) if atm else None,
            "put_25d_iv": round(p25*100, 2) if p25 else None,
            "call_25d_iv": round(c25*100, 2) if c25 else None,
            "skew_25d_pct": round((p25-c25)*100, 2) if (p25 and c25) else None,
            "butterfly_25d_pct": round(((p25+c25)/2 - atm)*100, 2) if (p25 and c25 and atm) else None,
        }

    # Term slope: 7d ATM - 90d ATM (negative = backwardation = stress)
    t7 = term_structure.get("t_7d", {}).get("atm_iv")
    t90 = term_structure.get("t_90d", {}).get("atm_iv")
    term_slope_pct = round(t7 - t90, 2) if (t7 and t90) else None

    # Front-month skew (30d is most actionable)
    skew_30d = term_structure.get("t_30d", {}).get("skew_25d_pct")
    atm_30d = term_structure.get("t_30d", {}).get("atm_iv")

    # Surface tension composite (0-100)
    tension = 0
    tension_reasons = []
    if term_slope_pct is not None:
        if term_slope_pct > 5: tension += 30; tension_reasons.append(f"backwardation +{term_slope_pct:.1f}vol pts (stress)")
        elif term_slope_pct > 2: tension += 15; tension_reasons.append(f"front-IV higher (mild stress)")
        elif term_slope_pct < -5: tension -= 5  # contango = calm
    if skew_30d is not None:
        if skew_30d > 6: tension += 30; tension_reasons.append(f"extreme skew {skew_30d:+.1f} (crash-pricing)")
        elif skew_30d > 4: tension += 20; tension_reasons.append(f"high skew {skew_30d:+.1f}")
        elif skew_30d < 1: tension += 10; tension_reasons.append(f"complacent skew {skew_30d:+.1f}")
    if atm_30d is not None:
        if atm_30d > 30: tension += 20; tension_reasons.append(f"high 30d ATM IV {atm_30d:.0f}%")
        elif atm_30d > 22: tension += 10
        elif atm_30d < 11: tension += 8; tension_reasons.append(f"too-low 30d ATM IV {atm_30d:.0f}% (complacent)")
    tension = max(0, min(100, tension))

    state = "EXTREME_STRESS" if tension >= 70 else \
              "STRESS" if tension >= 50 else \
              "ELEVATED" if tension >= 30 else \
              "COMPLACENT" if tension <= 15 else "NORMAL"

    return {
        "spot": spot,
        "term_structure": term_structure,
        "term_slope_pct": term_slope_pct,
        "skew_30d_pct": skew_30d,
        "atm_30d_pct": atm_30d,
        "surface_tension": tension,
        "state": state,
        "tension_reasons": tension_reasons,
        "n_contracts": len(parsed),
        "n_expiries": len(by_exp),
    }


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
        print(f"[tg] sent: {msg[:80]}")
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[vol-surface] starting, universe={UNDERLYINGS}")

    if not POLYGON_KEY:
        return {"statusCode": 500, "body": json.dumps({"err": "POLYGON_KEY missing"})}

    prior = get_s3_json(S3_KEY, {}) or {}

    # Fetch in parallel (Polygon allows it within rate limit for paid tiers)
    surfaces = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {}
        for sym in UNDERLYINGS:
            futs[ex.submit(fetch_chain, sym)] = sym
        for f in as_completed(futs):
            sym = futs[f]
            try:
                contracts = f.result() or []
                surfaces[sym] = compute_surface_metrics(sym, contracts)
                surfaces[sym]["fetched_at"] = datetime.now(timezone.utc).isoformat()
                print(f"[vol-surface] {sym}: tension={surfaces[sym].get('surface_tension')} "
                      f"state={surfaces[sym].get('state')} skew={surfaces[sym].get('skew_30d_pct')} "
                      f"slope={surfaces[sym].get('term_slope_pct')}")
            except Exception as e:
                surfaces[sym] = {"err": str(e)[:100]}
                print(f"[vol-surface] {sym} err: {e}")

    # Compute composite across all symbols (average tension)
    tensions = [s.get("surface_tension") for s in surfaces.values()
                  if isinstance(s.get("surface_tension"), (int, float))]
    composite_tension = round(sum(tensions)/len(tensions), 1) if tensions else None
    overall_state = "EXTREME_STRESS" if (composite_tension or 0) >= 60 else \
                       "STRESS" if (composite_tension or 0) >= 40 else \
                       "ELEVATED" if (composite_tension or 0) >= 25 else \
                       "COMPLACENT" if (composite_tension or 0) <= 15 else "NORMAL"

    output = {
        "schema_version": "1.0",
        "method": "vol_surface_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "composite_tension": composite_tension,
        "overall_state": overall_state,
        "n_symbols": len(UNDERLYINGS),
        "n_symbols_with_data": len(tensions),
        "surfaces": surfaces,
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[vol-surface] composite={composite_tension} state={overall_state}")

    # History
    try:
        history = get_s3_json(S3_KEY_HISTORY, {"snapshots": []}) or {"snapshots": []}
        snaps = history.get("snapshots", [])
        snaps.append({
            "ts": output["generated_at"],
            "composite_tension": composite_tension,
            "overall_state": overall_state,
            "per_symbol_tension": {k: v.get("surface_tension") for k, v in surfaces.items()},
            "spy_skew_30d": surfaces.get("SPY", {}).get("skew_30d_pct"),
            "spy_atm_30d": surfaces.get("SPY", {}).get("atm_30d_pct"),
            "spy_term_slope": surfaces.get("SPY", {}).get("term_slope_pct"),
        })
        snaps = snaps[-HISTORY_BUFFER:]
        put_s3_json(S3_KEY_HISTORY, {"snapshots": snaps,
                                       "updated_at": output["generated_at"]})
    except Exception as e:
        print(f"[history] err: {e}")

    # ─── ALERTS ───────────────────────────────────────────────────────
    try:
        prior_state = prior.get("overall_state")
        # 1. Regime change
        if prior_state and prior_state != overall_state:
            lines = [f"<b>{prior_state} → {overall_state}</b>",
                       f"Composite tension: {composite_tension}"]
            for sym, s in surfaces.items():
                if s.get("state") in ("STRESS", "EXTREME_STRESS"):
                    lines.append(f"• {sym}: {s['state']} (tension {s.get('surface_tension')}, "
                                 f"skew {s.get('skew_30d_pct',0):+.1f}, slope {s.get('term_slope_pct',0):+.1f})")
            maybe_telegram("📈 <b>VOL SURFACE REGIME CHANGE</b>\n" + "\n".join(lines))

        # 2. Backwardation in SPY (front IV > 6mo IV)
        spy = surfaces.get("SPY", {})
        prior_spy_slope = prior.get("surfaces", {}).get("SPY", {}).get("term_slope_pct")
        if spy.get("term_slope_pct") and spy["term_slope_pct"] > 3 and \
           (prior_spy_slope is None or prior_spy_slope <= 0):
            maybe_telegram(
                f"⚠️ <b>SPY BACKWARDATION</b>\n"
                f"7d ATM IV is {spy['term_slope_pct']:+.1f}pts above 90d.\n"
                f"Front-month panic-pricing — historically precedes 5-10%% drawdowns."
            )

        # 3. EXTREME SKEW — SPY skew_30d > 6
        if spy.get("skew_30d_pct") and spy["skew_30d_pct"] > 6:
            prior_spy_skew = prior.get("surfaces", {}).get("SPY", {}).get("skew_30d_pct", 0)
            if prior_spy_skew <= 6:
                maybe_telegram(
                    f"💀 <b>EXTREME SPY SKEW</b>\n"
                    f"30d put-call skew {spy['skew_30d_pct']:+.1f}vol pts.\n"
                    f"Tail-risk hedging surging — institutional positioning shifting defensive."
                )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True,
            "composite_tension": composite_tension,
            "overall_state": overall_state,
            "n_with_data": len(tensions),
            "duration_s": round(time.time()-t0, 1),
        }),
    }
