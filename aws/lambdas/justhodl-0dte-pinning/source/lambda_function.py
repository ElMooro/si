"""
justhodl-0dte-pinning — 0DTE Options Pinning & Gamma Wall Engine (BUILD 13/15)

WHY THIS EXISTS
===============
0DTE (zero-days-to-expiry) options are 40%+ of total SPX volume and the
dominant intraday driver. Bloomberg's GEX terminal premium dashboards
($24k/yr) include 0DTE-specific pinning levels, max-pain, and gamma
walls. We build them from CBOE's free delayed-quote feed (same source
as the broader dealer-gex Lambda but filtered to today's expiry only).

Key insights this surfaces:
  • Pin candidate: where market gravitates as 0DTE OI collapses
  • Largest gamma walls (call resistance / put support)
  • Net 0DTE dealer gamma exposure
  • Skew (put/call premium imbalance)
  • Implied volatility for today's options

DATA SOURCE
===========
cdn.cboe.com/api/global/delayed_quotes/options/{SYM}.json
  Free · no auth · ~15-min delayed · returns full chain
  We filter to expiry == today's date.

UNIVERSE
========
SPY, QQQ, IWM — the only tickers where 0DTE volume is meaningful.

METRICS PER UNDERLYING
======================
spot price + spot price change
0DTE: n contracts, total OI, total volume, total notional USD
Net gamma exposure for 0DTE (calls minus puts, $-weighted)
Net delta exposure for 0DTE
Max pain (strike minimizing total option payout)
Largest gamma walls:
  call walls: top 3 strikes with most positive gamma (resistance)
  put walls: top 3 strikes with most negative gamma (support)
ATM IV + 25-delta skew
0DTE put/call ratio (volume + OI)

COMPOSITE REGIME PER UNDERLYING
================================
  PINNED_TIGHT      |spot − max_pain| < 0.3% (high gravity)
  PINNED_DRIFTING   |spot − max_pain| < 0.7%
  CALL_WALL_DOMINANT spot ≤ top call wall (resistance overhead)
  PUT_WALL_DOMINANT spot ≥ top put wall (support below)
  POSITIVE_GAMMA    dealers stabilize (low vol mode)
  NEGATIVE_GAMMA    dealers amplify (vol regime)

CROSS-UNDERLYING REGIME
========================
  Most underlyings POSITIVE_GAMMA + PINNED  → LOW_VOL_PINNED
  Mixed PUT_WALL_DOMINANT                    → SUPPORT_TESTED
  Mixed CALL_WALL_DOMINANT                   → RESISTANCE_TESTED
  Most NEGATIVE_GAMMA                        → VOL_REGIME

SCHEDULE
========
cron(5,35 13-21 ? * MON-FRI *) — every 30 min during US market hours
"""
import io, json, os, re, time, urllib.request
from datetime import datetime, timezone, date
import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/zerodte.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 25
UNIVERSE = ["SPY", "QQQ", "IWM"]

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# OCC SYMBOL PARSING
# ═══════════════════════════════════════════════════════════════════════════

# OCC: SYMBOL + YYMMDD + C/P + 8-digit strike (× 1000)
OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


def parse_occ(occ):
    m = OCC_RE.match(occ)
    if not m: return None
    sym, yymmdd, cp, strike_str = m.groups()
    year = 2000 + int(yymmdd[:2])
    month = int(yymmdd[2:4])
    day = int(yymmdd[4:6])
    strike = int(strike_str) / 1000.0
    return {
        "symbol": sym, "expiry": date(year, month, day),
        "is_call": cp == "C", "strike": strike,
    }


# ═══════════════════════════════════════════════════════════════════════════
# CBOE FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_cboe_chain(symbol):
    url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8"))


# ═══════════════════════════════════════════════════════════════════════════
# 0DTE METRICS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_0dte(symbol):
    result = {"symbol": symbol}
    try:
        raw = fetch_cboe_chain(symbol)
    except Exception as e:
        result["err"] = f"cboe fetch: {str(e)[:80]}"
        return result

    data = raw.get("data") or {}
    spot = data.get("current_price") or data.get("close")
    price_change_pct = data.get("price_change_percent")
    if not spot:
        result["err"] = "no spot price"
        return result

    options = data.get("options") or []
    if not options:
        result["err"] = "no options"
        return result

    # Determine today's date in NY tz (approx via UTC; market sets the expiry calendar)
    today = datetime.now(timezone.utc).date()

    contracts_today = []
    for opt in options:
        occ = opt.get("option")
        parsed = parse_occ(occ)
        if not parsed: continue
        if parsed["expiry"] != today: continue
        # Capture metrics
        oi = int(opt.get("open_interest") or 0)
        vol = int(opt.get("volume") or 0)
        gamma = float(opt.get("gamma") or 0)
        iv = float(opt.get("iv") or 0)
        delta = float(opt.get("delta") or 0)
        bid = float(opt.get("bid") or 0)
        ask = float(opt.get("ask") or 0)
        last = float(opt.get("last_trade_price") or 0)
        contracts_today.append({
            **parsed, "oi": oi, "vol": vol, "gamma": gamma, "iv": iv,
            "delta": delta, "bid": bid, "ask": ask, "last": last,
            "mid": (bid + ask) / 2 if (bid > 0 and ask > 0) else last,
        })

    if not contracts_today:
        result["err"] = "no 0DTE contracts (market closed or no same-day expiry)"
        result["spot"] = spot
        return result

    # Aggregate
    n_calls = sum(1 for c in contracts_today if c["is_call"])
    n_puts = sum(1 for c in contracts_today if not c["is_call"])
    total_oi = sum(c["oi"] for c in contracts_today)
    total_vol = sum(c["vol"] for c in contracts_today)
    total_call_oi = sum(c["oi"] for c in contracts_today if c["is_call"])
    total_put_oi = sum(c["oi"] for c in contracts_today if not c["is_call"])
    total_call_vol = sum(c["vol"] for c in contracts_today if c["is_call"])
    total_put_vol = sum(c["vol"] for c in contracts_today if not c["is_call"])

    pc_ratio_oi = (total_put_oi / total_call_oi) if total_call_oi else None
    pc_ratio_vol = (total_put_vol / total_call_vol) if total_call_vol else None

    # Dealer gamma exposure: dealer is short customer flow so short calls + long puts
    # Net gamma exposure = (call_gamma × OI × 100 × spot²) − (put_gamma × OI × 100 × spot²)
    # in $ per 1% move (gamma is per 1 unit underlying move)
    multiplier = 100  # standard options multiplier
    gex_call = sum(c["gamma"] * c["oi"] * multiplier * spot * spot * 0.01
                    for c in contracts_today if c["is_call"])
    gex_put = sum(c["gamma"] * c["oi"] * multiplier * spot * spot * 0.01
                   for c in contracts_today if not c["is_call"])
    net_gex_usd = gex_call - gex_put
    net_gex_billions = net_gex_usd / 1e9

    # Aggregate gamma by strike (for walls)
    gamma_by_strike = {}
    for c in contracts_today:
        k = c["strike"]
        g = c["gamma"] * c["oi"] * multiplier * spot * spot * 0.01
        if not c["is_call"]: g = -g
        gamma_by_strike[k] = gamma_by_strike.get(k, 0) + g
    # Top call walls (most positive gamma)
    sorted_walls = sorted(gamma_by_strike.items(), key=lambda x: -x[1])
    call_walls = [{"strike": k, "gamma_usd": round(v, 0)} for k, v in sorted_walls[:5] if v > 0]
    put_walls = [{"strike": k, "gamma_usd": round(v, 0)} for k, v in sorted_walls[-5:] if v < 0]
    put_walls.reverse()  # most negative first

    # Max pain: strike minimizing total cash payout to option holders
    strikes_sorted = sorted(set(c["strike"] for c in contracts_today))
    pain_by_strike = []
    for K in strikes_sorted:
        pain = 0
        for c in contracts_today:
            if c["is_call"]:
                pain += max(K - c["strike"], 0) * c["oi"]
            else:
                pain += max(c["strike"] - K, 0) * c["oi"]
        pain_by_strike.append((K, pain))
    max_pain_strike = min(pain_by_strike, key=lambda x: x[1])[0] if pain_by_strike else None
    pin_distance_pct = ((spot - max_pain_strike) / spot * 100) if max_pain_strike else None

    # ATM IV (closest strike to spot, both call and put)
    atm_contracts = sorted(contracts_today, key=lambda c: abs(c["strike"] - spot))[:6]
    atm_iv = sum(c["iv"] for c in atm_contracts if c["iv"]) / max(sum(1 for c in atm_contracts if c["iv"]), 1)

    # 25-delta skew (put 25-delta IV − call 25-delta IV)
    put25 = min((c for c in contracts_today
                  if not c["is_call"] and 0.20 <= abs(c["delta"]) <= 0.30),
                 key=lambda c: abs(abs(c["delta"]) - 0.25), default=None)
    call25 = min((c for c in contracts_today
                   if c["is_call"] and 0.20 <= c["delta"] <= 0.30),
                  key=lambda c: abs(c["delta"] - 0.25), default=None)
    skew_25d = (put25["iv"] - call25["iv"]) if (put25 and call25 and put25["iv"] and call25["iv"]) else None

    # Per-symbol regime
    abs_pin = abs(pin_distance_pct) if pin_distance_pct is not None else None
    if abs_pin is not None and abs_pin < 0.3:
        sym_regime = "PINNED_TIGHT"
    elif abs_pin is not None and abs_pin < 0.7:
        sym_regime = "PINNED_DRIFTING"
    elif net_gex_billions > 0.5:
        sym_regime = "POSITIVE_GAMMA"
    elif net_gex_billions < -0.5:
        sym_regime = "NEGATIVE_GAMMA"
    elif call_walls and spot <= call_walls[0]["strike"] * 1.005:
        sym_regime = "CALL_WALL_DOMINANT"
    elif put_walls and spot >= put_walls[0]["strike"] * 0.995:
        sym_regime = "PUT_WALL_DOMINANT"
    else:
        sym_regime = "MIXED"

    result.update({
        "spot": round(spot, 2),
        "price_change_pct": round(price_change_pct, 2) if price_change_pct is not None else None,
        "n_contracts_0dte": len(contracts_today),
        "n_calls": n_calls, "n_puts": n_puts,
        "total_oi_0dte": total_oi,
        "total_volume_0dte": total_vol,
        "total_call_oi": total_call_oi, "total_put_oi": total_put_oi,
        "total_call_volume": total_call_vol, "total_put_volume": total_put_vol,
        "put_call_ratio_oi": round(pc_ratio_oi, 3) if pc_ratio_oi else None,
        "put_call_ratio_volume": round(pc_ratio_vol, 3) if pc_ratio_vol else None,
        "net_gex_usd": round(net_gex_usd, 0),
        "net_gex_billions": round(net_gex_billions, 3),
        "max_pain_strike": max_pain_strike,
        "pin_distance_pct": round(pin_distance_pct, 3) if pin_distance_pct is not None else None,
        "call_walls": call_walls,
        "put_walls": put_walls,
        "atm_iv_pct": round(atm_iv * 100, 2) if atm_iv else None,
        "skew_25d_pp": round(skew_25d * 100, 2) if skew_25d is not None else None,
        "regime": sym_regime,
        "expiry_date": today.isoformat(),
    })
    return result


# ═══════════════════════════════════════════════════════════════════════════
# CROSS-UNDERLYING REGIME
# ═══════════════════════════════════════════════════════════════════════════

def market_regime(per_symbol):
    valid = [v for v in per_symbol.values() if not v.get("err")]
    if not valid:
        return "UNKNOWN", "No 0DTE data available (likely outside market hours or weekend)"

    regimes = [v.get("regime") for v in valid]
    n_pinned = sum(1 for r in regimes if r and "PINNED" in r)
    n_pos_gamma = sum(1 for r in regimes if r == "POSITIVE_GAMMA")
    n_neg_gamma = sum(1 for r in regimes if r == "NEGATIVE_GAMMA")
    n_call_wall = sum(1 for r in regimes if r == "CALL_WALL_DOMINANT")
    n_put_wall = sum(1 for r in regimes if r == "PUT_WALL_DOMINANT")

    if n_pinned >= 2:
        return "PINNED_MULTI", (
            f"{n_pinned}/{len(valid)} underlyings pinned tight to max-pain — low intraday range expected")
    if n_neg_gamma >= 2:
        return "VOL_REGIME", f"{n_neg_gamma}/{len(valid)} negative-gamma — dealer flow amplifies moves"
    if n_pos_gamma >= 2:
        return "LOW_VOL_PINNED", f"{n_pos_gamma}/{len(valid)} positive-gamma — dealers stabilize, low vol mode"
    if n_call_wall + n_put_wall >= 2:
        return "WALL_TESTED", f"Multiple underlyings testing 0DTE walls — directional bias possible"
    return "MIXED", "Mixed 0DTE signals; no dominant regime"


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

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
        print(f"  tg err: {str(e)[:80]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== 0dte-pinning v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    print(f"  universe: {UNIVERSE}")

    try:
        prior = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_regime = prior.get("market_regime")
    except Exception:
        prior_regime = None

    per_symbol = {}
    for sym in UNIVERSE:
        r = analyze_0dte(sym)
        per_symbol[sym] = r
        if r.get("err"):
            print(f"  ✗ {sym}: {r['err']}")
        else:
            print(f"  ✓ {sym} spot:${r['spot']} max_pain:${r['max_pain_strike']} "
                  f"pin:{r.get('pin_distance_pct',0):+.2f}% "
                  f"gex:{r.get('net_gex_billions'):+.2f}B "
                  f"pc:{r.get('put_call_ratio_oi')} regime:{r['regime']}")

    regime, signal = market_regime(per_symbol)

    # Total volume & OI
    total_vol = sum(v.get("total_volume_0dte", 0) for v in per_symbol.values() if not v.get("err"))
    total_oi = sum(v.get("total_oi_0dte", 0) for v in per_symbol.values() if not v.get("err"))
    total_net_gex = sum(v.get("net_gex_billions", 0) for v in per_symbol.values() if not v.get("err"))

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "cdn.cboe.com/api/global/delayed_quotes/options",
        "elapsed_seconds": round(time.time() - started, 2),
        "universe": UNIVERSE,
        "per_symbol": per_symbol,
        "market_regime": regime,
        "market_signal": signal,
        "total_volume_0dte": total_vol,
        "total_oi_0dte": total_oi,
        "total_net_gex_billions": round(total_net_gex, 3),
        "regime_changed_from_prior": (prior_regime != regime) if prior_regime else False,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=300")
        print(f"  ✓ zerodte.json written")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Telegram on regime change or extreme readings
    alert_sent = False
    if (prior_regime and prior_regime != regime) or regime in ("VOL_REGIME", "PINNED_MULTI"):
        lines = [f"⚡ *0DTE · {datetime.now(timezone.utc).strftime('%b %d %H:%M')} UTC*\n",
                  f"⚡ {regime}",
                  f"_{signal}_\n"]
        for sym in UNIVERSE:
            v = per_symbol.get(sym, {})
            if not v.get("err"):
                lines.append(f"  • {sym} ${v.get('spot'):.2f} · pin:{v.get('pin_distance_pct'):+.2f}% · "
                              f"gex:{v.get('net_gex_billions'):+.2f}B · {v.get('regime')}")
        if prior_regime and prior_regime != regime:
            lines.insert(2, f"_(was {prior_regime})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_underlyings": len(UNIVERSE),
        "n_with_data": sum(1 for v in per_symbol.values() if not v.get("err")),
        "market_regime": regime,
        "total_volume_0dte": total_vol,
        "total_oi_0dte": total_oi,
        "total_net_gex_billions": payload["total_net_gex_billions"],
        "regime_changed": payload["regime_changed_from_prior"],
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
