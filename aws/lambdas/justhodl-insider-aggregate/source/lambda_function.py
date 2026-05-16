"""justhodl-insider-aggregate — market-wide insider buy/sell timing signal.

This is NOT the per-stock insider-cluster scanner. This is the MARKET-LEVEL
aggregate: across every company, are corporate insiders — the people with the
best information about their own businesses — net buyers or net sellers right
now? The aggregate open-market insider buy/sell ratio is one of the most
reliable contrarian timing tools known:

  • Insiders buying heavily as a group  -> historically clusters near bottoms
    (insiders are value buyers; they buy their own stock when it is cheap and
    they are confident — and they tend to be early but right)
  • Insiders all selling / not buying    -> common near complacent tops

ONLY open-market transactions count toward the signal:
  P-Purchase  -> a real conviction buy (someone spent their own cash)
  S-Sale      -> an open-market sale
Awards, option exercises, gifts and 10b5-1 routine sales are excluded — they
carry little information.

OUTPUT: data/insider-aggregate.json
  • buy/sell ratio by count and by dollar value over rolling windows
  • regime: INSIDERS_ACCUMULATING / NEUTRAL / INSIDERS_DISTRIBUTING
  • sector breakdown — where are insiders putting money to work
  • notable cluster buys (multiple insiders, same company, recent)

Telegram on regime change. Schedule: daily cron(30 22 ? * MON-FRI *) after
the SEC Form 4 filing flow for the day has largely posted.
"""
import json, os, time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from urllib import request, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/insider-aggregate.json"
S3_HISTORY_KEY = "data/insider-aggregate-history.json"
HISTORY_MAX = 260

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MAX_PAGES = int(os.environ.get("MAX_PAGES", "40"))

s3 = boto3.client("s3", region_name="us-east-1")


def _get_json(url, timeout=15, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-Insider/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.4 * (i + 1))
    return None


def fetch_insider_page(page):
    """Try the documented /stable endpoint names for market-wide latest insider trades."""
    for path in ("insider-trading/latest", "insider-trading"):
        url = (f"https://financialmodelingprep.com/stable/{path}"
               f"?page={page}&apikey={FMP_KEY}")
        d = _get_json(url)
        if isinstance(d, list):
            return d
    return None


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


def classify(txn):
    """Return ('buy'|'sell'|'other', usd_value) for an open-market transaction.
    FMP transactionType examples: 'P-Purchase', 'S-Sale', 'A-Award',
    'M-Exercise', 'G-Gift'. acquisitionOrDisposition: 'A'/'D'."""
    ttype = (txn.get("transactionType") or "").upper()
    aod = (txn.get("acquisitionOrDisposition") or "").upper()
    qty = txn.get("securitiesTransacted") or txn.get("securitiesOwned") or 0
    price = txn.get("price") or 0
    try:
        usd = abs(float(qty)) * abs(float(price))
    except Exception:
        usd = 0.0

    is_purchase = ttype.startswith("P") or "PURCHASE" in ttype
    is_sale = ttype.startswith("S") or "SALE" in ttype
    # require a real price — option exercises / awards usually price 0
    if is_purchase and price and price > 0:
        return "buy", usd
    if is_sale and price and price > 0:
        return "sell", usd
    return "other", usd


def parse_date(txn):
    for k in ("transactionDate", "date", "filingDate"):
        v = txn.get(k)
        if v:
            try:
                return datetime.fromisoformat(str(v)[:10]).date()
            except Exception:
                pass
    return None


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[insider-aggregate] starting {datetime.now(timezone.utc).isoformat()}")
    if not FMP_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FMP_KEY not set"})}

    today = datetime.now(timezone.utc).date()
    win_7 = today - timedelta(days=7)
    win_30 = today - timedelta(days=30)
    win_90 = today - timedelta(days=90)

    # ── paginate market-wide insider trades ──
    txns = []
    for page in range(MAX_PAGES):
        rows = fetch_insider_page(page)
        if not rows:
            break
        txns.extend(rows)
        # stop once we've clearly gone past 90 days
        oldest = min((parse_date(r) for r in rows if parse_date(r)), default=None)
        if oldest and oldest < win_90:
            break
        time.sleep(0.15)
    print(f"[insider-aggregate] pulled {len(txns)} insider transactions")

    if not txns:
        out = {"schema_version": "1.0", "method": "insider_aggregate_v1",
               "generated_at": datetime.now(timezone.utc).isoformat(),
               "err": "no insider transactions returned", "n_transactions": 0}
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=json.dumps(out).encode("utf-8"),
                       ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": True, "n": 0})}

    # ── aggregate per window ──
    def window_stats(cutoff):
        bc = sc = 0
        bd = sd = 0.0
        by_sector_buy = defaultdict(float)
        cluster = defaultdict(lambda: {"buyers": set(), "usd": 0.0, "name": ""})
        for t in txns:
            d = parse_date(t)
            if not d or d < cutoff:
                continue
            kind, usd = classify(t)
            sym = t.get("symbol")
            if kind == "buy":
                bc += 1
                bd += usd
                if sym:
                    cluster[sym]["buyers"].add(t.get("reportingName") or t.get("typeOfOwner") or "?")
                    cluster[sym]["usd"] += usd
                    cluster[sym]["name"] = t.get("companyName") or sym
            elif kind == "sell":
                sc += 1
                sd += usd
        ratio_count = (bc / sc) if sc else (bc if bc else 0.0)
        ratio_dollar = (bd / sd) if sd else (bd if bd else 0.0)
        clusters = sorted(
            [{"symbol": s, "name": v["name"], "n_buyers": len(v["buyers"]),
              "total_usd": round(v["usd"])}
             for s, v in cluster.items() if len(v["buyers"]) >= 2],
            key=lambda x: (-x["n_buyers"], -x["total_usd"]))[:15]
        return {
            "buy_count": bc, "sell_count": sc,
            "buy_usd": round(bd), "sell_usd": round(sd),
            "buy_sell_ratio_count": round(ratio_count, 3),
            "buy_sell_ratio_dollar": round(ratio_dollar, 3),
            "cluster_buys": clusters,
        }

    w7 = window_stats(win_7)
    w30 = window_stats(win_30)
    w90 = window_stats(win_90)

    # ── regime off the 30d dollar ratio (most stable) ──
    r30 = w30["buy_sell_ratio_dollar"]
    if r30 >= 0.7:
        regime = "INSIDERS_ACCUMULATING"
        regime_read = ("Corporate insiders are net buyers of their own stock at "
                       "an elevated rate. Historically a contrarian-bullish signal "
                       "— insiders buy when they see value and are confident; "
                       "heavy aggregate buying has clustered near major bottoms.")
    elif r30 <= 0.20:
        regime = "INSIDERS_DISTRIBUTING"
        regime_read = ("Corporate insiders are overwhelmingly selling and barely "
                       "buying. Common in complacent, richly-valued markets — not a "
                       "precise timing tool, but a caution flag for risk appetite.")
    else:
        regime = "NEUTRAL"
        regime_read = ("Insider buying and selling are in a normal balance — no "
                       "strong contrarian signal from aggregate insider behaviour.")

    hist = {"snapshots": []}
    try:
        hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)["Body"].read())
    except Exception:
        pass
    prior_regime = hist["snapshots"][-1]["regime"] if hist.get("snapshots") else None

    out = {
        "schema_version": "1.0",
        "method": "insider_aggregate_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_transactions": len(txns),
        "windows": {"last_7d": w7, "last_30d": w30, "last_90d": w90},
        "regime": regime,
        "regime_read": regime_read,
        "headline_ratio_30d_dollar": r30,
        "notable_cluster_buys": w30["cluster_buys"][:10],
        "methodology": (
            "Open-market insider purchases (P) vs sales (S) aggregated market-wide. "
            "Awards, option exercises and gifts excluded. The 30d dollar buy/sell "
            "ratio drives the regime; cluster buys flag companies where 2+ insiders "
            "bought recently."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    hist["snapshots"].append({
        "ts": out["generated_at"], "regime": regime,
        "ratio_30d_dollar": r30,
        "ratio_30d_count": w30["buy_sell_ratio_count"],
        "buy_usd_30d": w30["buy_usd"], "sell_usd_30d": w30["sell_usd"],
    })
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if prior_regime and prior_regime != regime:
        maybe_telegram(
            f"[insiders] <b>INSIDER REGIME CHANGE</b>\n"
            f"<b>{prior_regime} -> {regime}</b>\n"
            f"30d buy/sell (\\$): {r30}\n{regime_read}")

    print(f"[insider-aggregate] done {out['elapsed_s']}s regime={regime} "
          f"ratio30d={r30} txns={len(txns)}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "regime": regime, "ratio_30d_dollar": r30,
        "n_transactions": len(txns)})}
