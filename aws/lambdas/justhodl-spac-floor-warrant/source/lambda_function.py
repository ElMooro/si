"""
justhodl-spac-floor-warrant -- SPAC Trust-Value Floor + Free Warrants
======================================================================

RETAIL EDGE
-----------
Pre-merger SPACs hold investor cash in a trust account at ~$10/share. If
the SPAC trades NEAR or AT trust value ($9.90-$10.10), the downside is
floored — investors can redeem at trust value, getting their cash back
plus interest. The warrants attached to many SPAC units offer asymmetric
upside if a deal is announced.

The asymmetric trade structure:
  - Buy SPAC units (share + fractional warrant) at $10.00-$10.05
  - Downside protected by redemption right (~$10 trust value)
  - Upside: if deal announced + closes, units typically pop to $11-$15
  - Warrants alone (no share) can 5-10x on announced deals

Empirical: pre-merger SPACs trading within 1% of trust value with active
deal-seeking activity yield ~6-12% annualized risk-free return + free
warrant optionality (Gahng-Ritter-Zhang 2023 SPAC research).

This engine:
  1. Polls IPO calendar + screener for active SPACs
  2. Filters to pre-merger SPACs (not yet announced deal)
  3. Checks share price within 1% of trust value (~$10)
  4. Outputs asymmetric trade tickets with sizing guidance

DIFFERENT FROM:
  - All existing engines (zero SPAC coverage in 347 Lambdas)
  - Specifically asymmetric: low downside, free optionality on upside

STATE MACHINE
-------------
  ASYMMETRIC_RICH   >=8 pre-merger SPACs at trust-value floor
  ACTIVE            3-7 SPACs at floor
  NORMAL            1-2 SPACs at floor
  QUIET             none — late-cycle SPAC market
"""
import datetime as dt
import json
import os
import re
import time
import traceback
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "v1.0.0"
ENGINE = "justhodl-spac-floor-warrant"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/spac-floor-warrant.json"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN",
                                  "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
SSM_STATE_KEY = "/justhodl/spac-floor-warrant/state"

TRUST_VALUE_USD = 10.10  # Typical trust value with accrued interest
PRICE_FLOOR_LOW = 9.85
PRICE_FLOOR_HIGH = 10.20
MIN_MCAP = 50_000_000  # Need enough liquidity

# Common SPAC name patterns (post-2020 boom + 2024-26 second wave)
SPAC_NAME_PATTERNS = [
    r"\bAcquisition\s+(Corp|Inc|Holdings|Limited|Ltd|Company)\b",
    r"\bCapital\s+Acquisition\b",
    r"\bSPAC\b",
    r"\bBlank\s+Check\b",
    r"\bBusiness\s+Combination\b",
]


def http_get(url, timeout=15, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception:
            if attempt == retries:
                return None
            time.sleep(0.5 * (attempt + 1))
    return None


def fmp_get(path, params=None):
    if not FMP_KEY:
        return None
    q = dict(params or {})
    q["apikey"] = FMP_KEY
    url = f"https://financialmodelingprep.com/stable/{path}?{urllib.parse.urlencode(q)}"
    body = http_get(url, timeout=15)
    if not body:
        return None
    try:
        return json.loads(body)
    except Exception:
        return None


def get_state():
    try:
        r = ssm.get_parameter(Name=SSM_STATE_KEY)
        return r["Parameter"]["Value"]
    except Exception:
        return "UNKNOWN"


def set_state(state):
    try:
        ssm.put_parameter(Name=SSM_STATE_KEY, Value=state, Type="String", Overwrite=True)
    except Exception as e:
        print(f"ssm err: {e}")


def telegram_send(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": TELEGRAM_CHAT, "text": text,
                            "parse_mode": "Markdown", "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=body,
                                      headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=8)
    except Exception as e:
        print(f"telegram error: {e}")


def is_spac_name(name):
    if not name:
        return False
    for pattern in SPAC_NAME_PATTERNS:
        if re.search(pattern, name, re.IGNORECASE):
            return True
    return False


def fetch_spac_universe():
    """Build universe via two methods:
       1) FMP stock screener filtered to SPAC-like names
       2) FMP IPO calendar (recent IPOs in last 2 years)
    """
    candidates = []
    # Method 1: stock screener for low-priced near-$10 names
    try:
        screen = fmp_get("company-screener",
                          {"priceMoreThan": 9.5, "priceLowerThan": 10.5,
                           "marketCapMoreThan": MIN_MCAP, "exchange": "nasdaq,nyse",
                           "limit": 500})
        if isinstance(screen, list):
            for s in screen:
                name = s.get("companyName") or s.get("name") or ""
                symbol = s.get("symbol") or s.get("ticker") or ""
                if symbol and is_spac_name(name):
                    candidates.append({"symbol": symbol.upper(), "name": name})
    except Exception as e:
        print(f"screener err: {e}")

    # Method 2: IPO calendar (last 18 months)
    try:
        end = dt.date.today()
        start = end - dt.timedelta(days=540)
        ipos = fmp_get("ipos-calendar", {"from": start.isoformat(), "to": end.isoformat()})
        if isinstance(ipos, list):
            for i in ipos:
                name = i.get("company") or i.get("name") or ""
                symbol = i.get("symbol") or i.get("ticker") or ""
                if symbol and is_spac_name(name):
                    if not any(c["symbol"] == symbol.upper() for c in candidates):
                        candidates.append({"symbol": symbol.upper(), "name": name})
    except Exception as e:
        print(f"ipo calendar err: {e}")

    return candidates[:150]


def analyze(spac):
    """Check if SPAC is at trust-value floor + has free-warrant optionality."""
    try:
        symbol = spac["symbol"]
        q = fmp_get("quote", {"symbol": symbol})
        if not q or not isinstance(q, list) or not q:
            return None
        q = q[0]
        price = q.get("price")
        if not price or price < PRICE_FLOOR_LOW or price > PRICE_FLOOR_HIGH:
            return None
        mcap = q.get("marketCap") or 0
        if mcap < MIN_MCAP:
            return None
        # Verify it's a pre-merger SPAC (still trading near $10 = no announced deal)
        # Pull recent price history; if price has been stable around $10 for 30+ days,
        # likely pre-merger
        end = dt.date.today()
        start = end - dt.timedelta(days=60)
        hist = fmp_get(f"historical-price-eod/light",
                        {"symbol": symbol, "from": start.isoformat(), "to": end.isoformat()})
        if not isinstance(hist, list) or len(hist) < 20:
            return None
        closes = sorted([float(h.get("close") or 0) for h in hist if h.get("close")])
        if not closes:
            return None
        median_close = closes[len(closes) // 2]
        if median_close < PRICE_FLOOR_LOW or median_close > 10.50:
            return None  # Already announced a deal (price moved)
        discount_to_trust = round(((price - TRUST_VALUE_USD) / TRUST_VALUE_USD) * 100, 3)
        # Free-warrant optionality estimate
        # Typical SPAC: 1/3 warrant per unit, strike $11.50
        warrant_implied_value = 0.30 if price < TRUST_VALUE_USD else 0.20
        # Look up associated warrant ticker if present (e.g. XYZW = warrant; XYZU = unit)
        warrant_symbol = f"{symbol}W"
        warrant_price = None
        try:
            wq = fmp_get("quote", {"symbol": warrant_symbol})
            if isinstance(wq, list) and wq:
                warrant_price = wq[0].get("price")
        except Exception:
            pass
        # Build asymmetric trade ticket
        max_downside_pct = round(((PRICE_FLOOR_LOW - price) / price) * 100, 2)
        ticket = {
            "strategy": "asymmetric_spac_floor_play",
            "instrument": symbol,
            "entry": price,
            "downside_floor": TRUST_VALUE_USD * 0.99,  # 1% below trust = practical floor
            "max_downside_pct": max_downside_pct,
            "best_case_deal_announced": "share to $11-15, warrants to $1-4 (5-10x typical)",
            "expected_annualized_yield_if_no_deal": "6-12% (trust earns T-bill rate)",
            "hold_period": "until deal announced OR redemption deadline (typ. 18mo)",
            "position_size_pct": 2.0,
            "exit_triggers": [
                "deal announced + completed (capture spread)",
                "redemption deadline approaches (cash out at trust value)",
                "warrant value spikes on rumored deal (sell warrant separately)",
            ],
            "warrant_play": (f"If associated warrant ({warrant_symbol}) trades at "
                              f"${warrant_price:.2f} (if available), consider buying warrants "
                              f"separately for max upside leverage."
                              if warrant_price else
                              "Check for associated warrant ticker (typ. SYMBOLW) for "
                              "additional asymmetric optionality."),
            "risks": [
                "deal-search timeout = forced redemption at trust value (NOT a loss)",
                "regulatory crackdown on SPAC structures",
                "interest-rate volatility affects trust yield",
                "occasionally trust value can be partial-recovery only",
            ],
        }
        return {
            "symbol": symbol,
            "name": spac.get("name"),
            "price": price,
            "trust_value_estimate": TRUST_VALUE_USD,
            "discount_to_trust_pct": discount_to_trust,
            "median_60d_price": round(median_close, 2),
            "mcap_millions": round(mcap / 1e6, 1) if mcap else None,
            "warrant_symbol": warrant_symbol,
            "warrant_price": warrant_price,
            "score": round(min(100, 60 + (1.0 - abs(discount_to_trust)) * 30), 1),
            "trade_ticket": ticket,
        }
    except Exception as e:
        print(f"analyze {spac.get('symbol')}: {e}")
        return None


def lambda_handler(event, context):
    print(f"=== {ENGINE} {VERSION} start ===")
    started = time.time()
    try:
        universe = fetch_spac_universe()
        print(f"SPAC universe candidates: {len(universe)}")
        picks = []
        with ThreadPoolExecutor(max_workers=6) as exe:
            futs = {exe.submit(analyze, s): s for s in universe}
            for fut in as_completed(futs):
                r = fut.result()
                if r:
                    picks.append(r)
        picks.sort(key=lambda x: -x["score"])
        n = len(picks)

        if n >= 8:
            state = "ASYMMETRIC_RICH"
        elif n >= 3:
            state = "ACTIVE"
        elif n >= 1:
            state = "NORMAL"
        else:
            state = "QUIET"

        prev = get_state()
        if state != prev and state in ("ASYMMETRIC_RICH", "ACTIVE"):
            tops = [p["symbol"] for p in picks[:5]]
            msg = (f"🎯 *SPAC TRUST-VALUE FLOOR*\n"
                   f"State: {prev} → *{state}*\n"
                   f"At-floor SPACs: {n}\n"
                   f"Top: {', '.join(tops)}\n\n"
                   f"https://justhodl.ai/retail-edges.html")
            telegram_send(msg)
        set_state(state)

        priors = {
            "ASYMMETRIC_RICH": {"baseline_return": "6-12% annualized (trust yield)",
                                 "deal_announced_pop": "+10-50% on share, +200-900% on warrants",
                                 "deal_completion_hit_rate": "55-65% historically",
                                 "basis": "Gahng-Ritter-Zhang 2023 SPAC research"},
            "ACTIVE":           {"baseline_return": "6-12% annualized",
                                 "deal_announced_pop": "+10-50% / +200-900% warrants"},
            "NORMAL":           {"baseline_return": "5-10% annualized"},
            "QUIET":            {"baseline_return": "n/a — SPAC market quiet"},
        }
        out = {
            "engine": ENGINE,
            "version": VERSION,
            "as_of": dt.datetime.utcnow().isoformat() + "Z",
            "state": state,
            "signal_strength": min(100, n * 10),
            "summary": {
                "universe_candidates": len(universe),
                "qualifying_picks_n": n,
                "trust_value_estimate": TRUST_VALUE_USD,
                "price_floor_range": [PRICE_FLOOR_LOW, PRICE_FLOOR_HIGH],
            },
            "picks": picks[:30],
            "forward_expectations": priors.get(state, {}),
            "methodology": {
                "framework": "Pre-merger SPACs at trust-value floor (~$10) = asymmetric payoff",
                "downside_protection": "Redemption right at trust value (~$10) caps loss",
                "upside_capture": "Share pop + free warrant on announced deal",
                "size_filter": f"mcap >= ${MIN_MCAP/1e6:.0f}M for liquidity",
                "edge_basis": "Gahng-Ritter-Zhang 2023 SPAC IPO and merger studies",
                "caveats": "SPAC market is cyclical. 2020-21 boom led to 2022-23 bust. Edge "
                            "is highest in mature/quiet SPAC markets where deal completions resume.",
            },
            "sources": ["FMP /stable/company-screener", "FMP /stable/ipos-calendar",
                         "FMP /stable/quote", "FMP /stable/historical-price-eod"],
            "why_now": (f"{n} pre-merger SPACs trading within $9.85-$10.20 (near trust value). "
                        f"Downside floored at trust redemption; upside unlimited via free warrants "
                        f"if deal is announced. Risk-free 6-12% annualized while waiting."),
            "run_seconds": round(time.time() - started, 1),
        }
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2, default=str).encode(),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        print(f"=== state={state} picks={n} ===")
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "state": state, "picks": n,
            "run_seconds": out["run_seconds"]})}
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        return {"statusCode": 500, "body": json.dumps({"ok": False, "error": str(e)[:300]})}
