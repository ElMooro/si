"""justhodl-coffee-can — the multibagger holding-discipline tracker.

The "coffee can portfolio" (Robert Kirby, 1984): you put great businesses in
a can and DON'T TOUCH THEM for a decade-plus. The hardest part of catching a
100-bagger is not finding it — it is HOLDING it. Chris Mayer's study of real
100-baggers found the median name endured multiple peak-to-trough drawdowns of
50%+ ALONG THE WAY. Investors who sold on the drawdowns never got the 100x.

This Lambda is the behavioral guardrail. For each committed holding it:
  • tracks return since entry, CAGR, time held, progress toward target multiple
  • measures the current drawdown from the holding's own peak
  • frames that drawdown against multibagger norms ("a -40% is NORMAL, not a sell")
  • re-checks the ORIGINAL THESIS against fresh fundamentals — and the ONLY
    legitimate sell signal is a BROKEN thesis (revenue contracting, ROIC
    collapsed, margins gone, balance sheet broken), never a price drop.

THESIS HEALTH:
  THESIS_INTACT     — revenue still growing, ROIC healthy, margins holding
  THESIS_WEAKENING  — one pillar deteriorating (watch, do not act yet)
  THESIS_BROKEN     — multiple pillars broken → the only valid exit trigger

EVENT API (invoke with a payload):
  {}                                  → refresh all holdings, write dashboard
  {"action":"add", "symbol":"XYZ", "entry_price":12.3, "thesis":"...",
   "target_multiple":25, "conviction":"high"}   → add a holding
  {"action":"remove", "symbol":"XYZ"} → remove a holding
  {"action":"note", "symbol":"XYZ", "note":"..."} → append a dated note

STORAGE:
  data/coffee-can-holdings.json  — the committed positions (input, editable)
  data/coffee-can.json           — the enriched dashboard (output)

Telegram fires ONLY on a thesis transition to THESIS_BROKEN — never on price.

Schedule: daily cron(0 13 ? * MON-FRI *).
"""
import json, os, time, math
from datetime import datetime, timezone, date
from urllib import request, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
HOLDINGS_KEY = "data/coffee-can-holdings.json"
DASHBOARD_KEY = "data/coffee-can.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

# Multibagger drawdown norms — from Chris Mayer "100 Baggers" study of the
# actual 100x names: nearly all endured several deep drawdowns en route.
DRAWDOWN_CONTEXT = [
    (0.20, "A drawdown under 20% is noise. 100-baggers have dozens of these."),
    (0.35, "A 20-35% drawdown is routine. Amazon had many of these on its way to 1000x."),
    (0.50, "A 35-50% drawdown is normal for a multibagger. Painful, not a sell signal."),
    (0.70, "A 50-70% drawdown is within multibagger norms — most 100x names had 2-4 of these. "
            "Sell only if the THESIS broke, never on price."),
    (1.00, "A 70%+ drawdown is severe. Re-examine the thesis carefully — but even Netflix "
            "and Apple had 70%+ drawdowns before going on to 100x+. Price alone is not the trigger."),
]


def _get_json(url, timeout=12, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-CoffeeCan/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.4 * (i + 1))
    return None


def fmp(path, symbol, limit=None):
    url = f"https://financialmodelingprep.com/stable/{path}?symbol={symbol}&apikey={FMP_KEY}"
    if limit:
        url += f"&limit={limit}"
    return _get_json(url)


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def get_s3_json(key, default):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def days_between(iso_a, iso_b=None):
    try:
        a = datetime.fromisoformat(str(iso_a)[:10]).date()
        b = (datetime.fromisoformat(str(iso_b)[:10]).date()
             if iso_b else datetime.now(timezone.utc).date())
        return (b - a).days
    except Exception:
        return None


def drawdown_note(dd_frac):
    for thresh, note in DRAWDOWN_CONTEXT:
        if dd_frac <= thresh:
            return note
    return DRAWDOWN_CONTEXT[-1][1]


# ───────────────────────── thesis health ─────────────────────────
def check_thesis(symbol):
    """Re-check multibagger thesis pillars against fresh fundamentals."""
    income = fmp("income-statement", symbol, limit=5) or []
    km = fmp("key-metrics", symbol, limit=5) or []
    ratios = fmp("ratios", symbol, limit=5) or []

    signals = []
    broken = 0
    weak = 0

    # Revenue trend — newest first
    revs = [s.get("revenue") for s in income if s.get("revenue") is not None]
    rev_state = "unknown"
    if len(revs) >= 3:
        if revs[0] > revs[1] > revs[2]:
            rev_state = "growing"
            signals.append("revenue still growing (3yr uptrend)")
        elif revs[0] < revs[1] and revs[1] < revs[2]:
            rev_state = "contracting"
            broken += 1
            signals.append("REVENUE CONTRACTING 2 consecutive years — thesis pillar broken")
        elif revs[0] < revs[1]:
            rev_state = "stalling"
            weak += 1
            signals.append("revenue dipped vs prior year — watch")
        else:
            rev_state = "mixed"
            signals.append("revenue choppy but not contracting")

    # ROIC health
    roic_state = "unknown"
    roics = [k.get("returnOnInvestedCapital") for k in km
             if k.get("returnOnInvestedCapital") is not None]
    if roics:
        latest = roics[0] * 100
        if latest >= 10:
            roic_state = "healthy"
            signals.append(f"ROIC healthy ({latest:.0f}%)")
        elif latest >= 4:
            roic_state = "soft"
            weak += 1
            signals.append(f"ROIC softening ({latest:.0f}%)")
        else:
            roic_state = "broken"
            broken += 1
            signals.append(f"ROIC collapsed ({latest:.0f}%) — compounding engine stalled")

    # Gross margin
    gm_state = "unknown"
    gms = [r.get("grossProfitMargin") for r in ratios
           if r.get("grossProfitMargin") is not None]
    if len(gms) >= 3:
        if gms[0] >= gms[-1] - 0.03:
            gm_state = "holding"
            signals.append("gross margin holding (moat intact)")
        else:
            gm_state = "eroding"
            weak += 1
            signals.append("gross margin eroding — moat under pressure")

    # Balance sheet
    bs_state = "unknown"
    if ratios:
        de = ratios[0].get("debtToEquityRatio")
        cr = ratios[0].get("currentRatio")
        if de is not None and de > 3.0:
            bs_state = "stressed"
            broken += 1
            signals.append(f"balance sheet stressed (D/E {de:.1f})")
        elif cr is not None and cr < 1.0:
            bs_state = "tight"
            weak += 1
            signals.append(f"liquidity tight (current ratio {cr:.1f})")
        else:
            bs_state = "ok"

    if broken >= 2:
        health = "THESIS_BROKEN"
    elif broken == 1:
        health = "THESIS_WEAKENING"
    elif weak >= 2:
        health = "THESIS_WEAKENING"
    else:
        health = "THESIS_INTACT"

    return {
        "health": health,
        "revenue": rev_state,
        "roic": roic_state,
        "gross_margin": gm_state,
        "balance_sheet": bs_state,
        "signals": signals,
        "latest_revenue": revs[0] if revs else None,
        "latest_roic_pct": round(roics[0] * 100, 1) if roics else None,
    }


def enrich_holding(h, prior_holding):
    """Compute live tracking + thesis health for one committed holding."""
    sym = h.get("symbol")
    quote = fmp("quote", sym) or []
    q = quote[0] if isinstance(quote, list) and quote else {}
    price = q.get("price")
    year_high = q.get("yearHigh")
    year_low = q.get("yearLow")

    entry_price = h.get("entry_price")
    entry_date = h.get("entry_date")
    target_mult = h.get("target_multiple") or 25

    # peak price ever seen by the tracker (persisted), updated with year_high
    peak = h.get("peak_price") or entry_price or price or 0
    if price:
        peak = max(peak, price)
    if year_high:
        peak = max(peak, year_high)

    ret_pct = None
    cagr_pct = None
    mult = None
    if entry_price and price and entry_price > 0:
        mult = price / entry_price
        ret_pct = (mult - 1) * 100
        d = days_between(entry_date)
        if d and d > 30:
            yrs = d / 365.25
            try:
                cagr_pct = ((mult) ** (1 / yrs) - 1) * 100
            except Exception:
                cagr_pct = None

    # current drawdown from peak
    dd_frac = None
    if peak and price and peak > 0:
        dd_frac = max(0.0, (peak - price) / peak)

    progress_pct = None
    if mult is not None and target_mult:
        progress_pct = min(100.0, mult / target_mult * 100)

    thesis = check_thesis(sym)

    # behavioral verdict
    if thesis["health"] == "THESIS_BROKEN":
        verdict = ("THESIS BROKEN — this is the one legitimate reason to exit. "
                   "Re-underwrite or sell. Not because of price.")
    elif dd_frac is not None and dd_frac >= 0.35:
        verdict = ("HOLD. " + drawdown_note(dd_frac) + " Thesis is "
                   + thesis["health"].replace("THESIS_", "").lower() + ".")
    elif thesis["health"] == "THESIS_WEAKENING":
        verdict = "HOLD but monitor — a thesis pillar is softening. No action yet."
    else:
        verdict = "HOLD. Thesis intact. Let the compounding work — time is the ally."

    return {
        "symbol": sym,
        "name": h.get("name") or q.get("name"),
        "entry_date": entry_date,
        "entry_price": entry_price,
        "current_price": price,
        "peak_price": round(peak, 4) if peak else None,
        "target_multiple": target_mult,
        "conviction": h.get("conviction", "medium"),
        "thesis_text": h.get("thesis"),
        "days_held": days_between(entry_date),
        "return_pct": round(ret_pct, 1) if ret_pct is not None else None,
        "current_multiple": round(mult, 2) if mult is not None else None,
        "cagr_pct": round(cagr_pct, 1) if cagr_pct is not None else None,
        "drawdown_from_peak_pct": round(dd_frac * 100, 1) if dd_frac is not None else None,
        "drawdown_context": drawdown_note(dd_frac) if dd_frac is not None else None,
        "progress_to_target_pct": round(progress_pct, 1) if progress_pct is not None else None,
        "thesis_health": thesis["health"],
        "thesis_detail": thesis,
        "verdict": verdict,
        "year_high": year_high,
        "year_low": year_low,
        "notes": h.get("notes", []),
        "_prior_health": (prior_holding or {}).get("thesis_health"),
    }


# ───────────────────────── event actions ─────────────────────────
def action_add(holdings, event):
    sym = (event.get("symbol") or "").upper().strip()
    if not sym:
        return holdings, "no symbol provided"
    holdings = [h for h in holdings if h.get("symbol") != sym]
    entry_price = event.get("entry_price")
    if entry_price is None:
        q = fmp("quote", sym) or []
        if isinstance(q, list) and q:
            entry_price = q[0].get("price")
    holdings.append({
        "symbol": sym,
        "name": event.get("name"),
        "entry_date": event.get("entry_date") or datetime.now(timezone.utc).date().isoformat(),
        "entry_price": entry_price,
        "target_multiple": event.get("target_multiple", 25),
        "conviction": event.get("conviction", "medium"),
        "thesis": event.get("thesis", ""),
        "notes": [],
    })
    return holdings, f"added {sym} at {entry_price}"


def action_remove(holdings, event):
    sym = (event.get("symbol") or "").upper().strip()
    before = len(holdings)
    holdings = [h for h in holdings if h.get("symbol") != sym]
    return holdings, f"removed {sym}" if len(holdings) < before else f"{sym} not found"


def action_note(holdings, event):
    sym = (event.get("symbol") or "").upper().strip()
    note = event.get("note", "")
    for h in holdings:
        if h.get("symbol") == sym:
            h.setdefault("notes", []).append({
                "date": datetime.now(timezone.utc).date().isoformat(), "note": note})
            return holdings, f"note added to {sym}"
    return holdings, f"{sym} not found"


def lambda_handler(event, context):
    t0 = time.time()
    event = event or {}
    action = event.get("action", "refresh")
    print(f"[coffee-can] starting action={action}")

    holdings = get_s3_json(HOLDINGS_KEY, {"holdings": []}).get("holdings", [])
    prior_dash = get_s3_json(DASHBOARD_KEY, {})
    prior_by_sym = {h.get("symbol"): h for h in prior_dash.get("holdings", [])}

    msg = None
    if action == "add":
        holdings, msg = action_add(holdings, event)
    elif action == "remove":
        holdings, msg = action_remove(holdings, event)
    elif action == "note":
        holdings, msg = action_note(holdings, event)

    if action in ("add", "remove", "note"):
        # persist holdings, carry forward peak prices
        put_s3_json(HOLDINGS_KEY, {"holdings": holdings,
                                    "updated_at": datetime.now(timezone.utc).isoformat()})
        print(f"[coffee-can] {msg}")

    if not FMP_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FMP_KEY not set"})}

    # Enrich every holding
    enriched = []
    for h in holdings:
        try:
            e = enrich_holding(h, prior_by_sym.get(h.get("symbol")))
            enriched.append(e)
            # persist updated peak back into holdings store
            h["peak_price"] = e.get("peak_price")
        except Exception as ex:
            print(f"[coffee-can] enrich {h.get('symbol')} err: {ex}")

    # Save holdings with refreshed peak prices
    put_s3_json(HOLDINGS_KEY, {"holdings": holdings,
                                "updated_at": datetime.now(timezone.utc).isoformat()})

    # Portfolio aggregates
    n = len(enriched)
    winners = [e for e in enriched if (e.get("return_pct") or 0) > 0]
    multibaggers = [e for e in enriched if (e.get("current_multiple") or 0) >= 2]
    broken = [e for e in enriched if e.get("thesis_health") == "THESIS_BROKEN"]
    weakening = [e for e in enriched if e.get("thesis_health") == "THESIS_WEAKENING"]
    avg_ret = (sum(e.get("return_pct") or 0 for e in enriched) / n) if n else 0
    best = max(enriched, key=lambda e: e.get("return_pct") or -1e9, default=None)

    # alerts: only thesis breaks (transition into BROKEN)
    alerts = []
    for e in enriched:
        if e.get("thesis_health") == "THESIS_BROKEN" and e.get("_prior_health") != "THESIS_BROKEN":
            alerts.append(f"{e['symbol']} — thesis BROKE: "
                          + "; ".join(e["thesis_detail"]["signals"][:3]))
        e.pop("_prior_health", None)

    out = {
        "schema_version": "1.0",
        "method": "coffee_can_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "last_action": msg,
        "n_holdings": n,
        "portfolio": {
            "n_winners": len(winners),
            "n_multibaggers_2x_plus": len(multibaggers),
            "n_thesis_broken": len(broken),
            "n_thesis_weakening": len(weakening),
            "avg_return_pct": round(avg_ret, 1),
            "best_performer": (best.get("symbol") if best else None),
            "best_return_pct": (best.get("return_pct") if best else None),
        },
        "holdings": sorted(enriched, key=lambda e: -(e.get("return_pct") or -1e9)),
        "discipline_reminder": (
            "The coffee-can rule: great businesses go IN and are not touched for "
            "a decade-plus. Real 100-baggers endured multiple 50%+ drawdowns en "
            "route. The ONLY valid sell trigger is a broken thesis — never price."
        ),
    }
    put_s3_json(DASHBOARD_KEY, out)

    if alerts:
        maybe_telegram("[coffee-can] <b>THESIS BREAK</b> — review required:\n"
                        + "\n".join(f"- {a}" for a in alerts[:5]))

    print(f"[coffee-can] done {out['elapsed_s']}s n={n} broken={len(broken)} "
          f"weakening={len(weakening)} avg_ret={avg_ret:.0f}%")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "action": action, "msg": msg,
        "n_holdings": n, "n_thesis_broken": len(broken)})}
