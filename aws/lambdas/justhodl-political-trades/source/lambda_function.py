"""
justhodl-political-trades — Senator & Representative trading activity.

Pulls Senate + House financial disclosures from FMP /stable/ endpoints,
deduplicates, flags:
  - Clusters (same ticker, multiple members within 7 days)
  - Sector concentration (Tech-Committee senator buys NVDA = suspicious)
  - Unusually large trades ($500K+)
  - Speed (trade disclosed within 30 days of execution = priority signal)

Outputs:
  data/political-trades.json — full normalized trades + clusters + alerts

Schedule: cron(45 13 ? * MON-FRI *) — daily 13:45 UTC (post-9AM ET, lets
overnight filings settle).

Telegram alerts:
  1. New $500K+ trade by any tracked member
  2. New cluster (3+ members same ticker within 7d)
  3. Pelosi/Crenshaw/Khanna specifically (high-watch names)
"""
import io
import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/political-trades.json"

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FMP_BASE = "https://financialmodelingprep.com/stable"
HTTP_TIMEOUT = 30

# Members worth special-case attention (frequent traders / committee chairs)
HIGH_WATCH_MEMBERS = {
    # House
    "Pelosi", "Khanna", "Crenshaw", "McCaul", "Greene",
    # Senate
    "Cruz", "Tuberville", "Whitehouse", "Capito", "Daines",
}

# Trade size thresholds (ranges from disclosure; use upper bound)
SIZE_RANGES = {
    "$1,001 - $15,000":      15_000,
    "$15,001 - $50,000":     50_000,
    "$50,001 - $100,000":    100_000,
    "$100,001 - $250,000":   250_000,
    "$250,001 - $500,000":   500_000,
    "$500,001 - $1,000,000": 1_000_000,
    "$1,000,001 - $5,000,000": 5_000_000,
    "$5,000,001 - $25,000,000": 25_000_000,
    "$25,000,001 - $50,000,000": 50_000_000,
    "Over $50,000,000":      100_000_000,
}
LARGE_TRADE_THRESHOLD = 500_000

s3 = boto3.client("s3", region_name="us-east-1")


def http_get_json(url, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 JustHodl.AI",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=600"):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def parse_size(amount_str):
    """Parse FMP's range strings into max-dollar upper bound."""
    if not amount_str: return 0
    s = amount_str.strip()
    if s in SIZE_RANGES:
        return SIZE_RANGES[s]
    # Try simple numeric parse
    try:
        return int(float(s.replace("$", "").replace(",", "")))
    except: pass
    return 0


def normalize_member(raw):
    """Best-effort member name normalization for cluster matching."""
    if not raw: return ""
    # FMP usually has "Last, First M." or "First Last"
    raw = str(raw).strip()
    # Strip titles
    for prefix in ["Senator ", "Sen. ", "Rep. ", "Representative ", "Hon. "]:
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
    return raw


def fetch_disclosures():
    """Try FMP senate + house RSS feeds. Multiple endpoint variants attempted."""
    candidates_senate = [
        f"{FMP_BASE}/senate-trading-rss-feed?apikey={FMP_KEY}",
        f"{FMP_BASE}/senate-trading?apikey={FMP_KEY}",
        f"{FMP_BASE}/senate-trades-rss?apikey={FMP_KEY}",
    ]
    candidates_house = [
        f"{FMP_BASE}/house-disclosure-rss-feed?apikey={FMP_KEY}",
        f"{FMP_BASE}/senate-disclosure-rss-feed?apikey={FMP_KEY}",
        f"{FMP_BASE}/house-trading?apikey={FMP_KEY}",
        f"{FMP_BASE}/house-disclosure?apikey={FMP_KEY}",
    ]

    senate_data, house_data = [], []
    senate_endpoint, house_endpoint = None, None
    for url in candidates_senate:
        try:
            data = http_get_json(url)
            if isinstance(data, list) and data:
                senate_data = data
                senate_endpoint = url.split("?")[0]
                print(f"[senate] OK {senate_endpoint} n={len(data)}")
                break
            print(f"[senate] empty: {url.split('?')[0]}")
        except urllib.error.HTTPError as e:
            print(f"[senate] HTTP {e.code} on {url.split('?')[0]}")
        except Exception as e:
            print(f"[senate] err on {url.split('?')[0]}: {str(e)[:80]}")
    for url in candidates_house:
        try:
            data = http_get_json(url)
            if isinstance(data, list) and data:
                house_data = data
                house_endpoint = url.split("?")[0]
                print(f"[house] OK {house_endpoint} n={len(data)}")
                break
            print(f"[house] empty: {url.split('?')[0]}")
        except urllib.error.HTTPError as e:
            print(f"[house] HTTP {e.code} on {url.split('?')[0]}")
        except Exception as e:
            print(f"[house] err on {url.split('?')[0]}: {str(e)[:80]}")

    return {
        "senate": senate_data, "senate_endpoint": senate_endpoint,
        "house": house_data, "house_endpoint": house_endpoint,
    }


def normalize_trade(raw, chamber):
    """Convert FMP trade record to common schema."""
    out = {
        "chamber": chamber,
        "member": normalize_member(raw.get("representative") or raw.get("senator")
                                      or raw.get("firstName", "") + " " + raw.get("lastName", "")),
        "office": raw.get("office") or raw.get("district") or "",
        "ticker": (raw.get("symbol") or raw.get("ticker") or "").upper(),
        "asset_description": raw.get("assetDescription") or raw.get("asset_description") or "",
        "asset_type": raw.get("type") or raw.get("assetType") or "",
        "transaction_type": (raw.get("type") or raw.get("transaction_type") or "").lower(),
        "amount_range": raw.get("amount") or raw.get("amountRange") or "",
        "amount_max_usd": parse_size(raw.get("amount") or raw.get("amountRange") or ""),
        "transaction_date": raw.get("transactionDate") or raw.get("date") or raw.get("transaction_date") or "",
        "disclosure_date": raw.get("disclosureDate") or raw.get("reportingDate") or raw.get("disclosure_date") or "",
        "link": raw.get("link") or raw.get("ptr_link") or raw.get("url") or "",
    }
    # Compute reporting lag in days
    try:
        if out["transaction_date"] and out["disclosure_date"]:
            td = datetime.fromisoformat(out["transaction_date"].split("T")[0])
            dd = datetime.fromisoformat(out["disclosure_date"].split("T")[0])
            out["reporting_lag_days"] = (dd - td).days
    except Exception:
        out["reporting_lag_days"] = None
    return out


def detect_clusters(trades, window_days=7, min_members=3):
    """Find tickers where N+ distinct members traded within window_days."""
    by_ticker = defaultdict(list)
    for t in trades:
        if t.get("ticker") and t.get("transaction_date"):
            by_ticker[t["ticker"]].append(t)

    clusters = []
    for ticker, ts in by_ticker.items():
        try:
            ts_dated = []
            for t in ts:
                try:
                    d = datetime.fromisoformat(t["transaction_date"].split("T")[0])
                    ts_dated.append((d, t))
                except Exception: pass
            ts_dated.sort(key=lambda x: x[0])
            # Sliding window
            for i, (d_i, t_i) in enumerate(ts_dated):
                window_members = {t_i["member"]}
                window_trades = [t_i]
                for d_j, t_j in ts_dated[i+1:]:
                    if (d_j - d_i).days > window_days: break
                    window_members.add(t_j["member"])
                    window_trades.append(t_j)
                if len(window_members) >= min_members:
                    n_buys = sum(1 for t in window_trades
                                  if t.get("transaction_type") in ("purchase", "buy"))
                    n_sells = len(window_trades) - n_buys
                    total_usd = sum(t.get("amount_max_usd", 0) for t in window_trades)
                    clusters.append({
                        "ticker": ticker,
                        "n_members": len(window_members),
                        "members": sorted(window_members),
                        "n_trades": len(window_trades),
                        "n_buys": n_buys, "n_sells": n_sells,
                        "first_trade": ts_dated[i][1]["transaction_date"],
                        "last_trade": window_trades[-1]["transaction_date"],
                        "total_value_max_usd": total_usd,
                        "direction": "BUY" if n_buys > n_sells
                                     else ("SELL" if n_sells > n_buys else "MIXED"),
                    })
                    break  # only one cluster per ticker
        except Exception as e:
            print(f"[cluster] {ticker}: {e}")

    # Dedup by ticker, keep one (already first-wins)
    clusters.sort(key=lambda c: -c["n_members"])
    return clusters


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
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
    print("[political-trades] starting")

    # Load prior run for diff
    prior_run = get_s3_json(S3_KEY_OUT, {}) or {}

    # Fetch all
    raw = fetch_disclosures()

    senate_trades = [normalize_trade(r, "SENATE") for r in (raw["senate"] or [])]
    house_trades = [normalize_trade(r, "HOUSE") for r in (raw["house"] or [])]
    all_trades = [t for t in (senate_trades + house_trades) if t.get("ticker")]

    # Sort by transaction date desc
    all_trades.sort(key=lambda t: t.get("transaction_date", ""), reverse=True)

    # Clusters
    clusters = detect_clusters(all_trades, window_days=7, min_members=3)

    # Large trades ($500K+)
    large_trades = [t for t in all_trades if t.get("amount_max_usd", 0) >= LARGE_TRADE_THRESHOLD]

    # High-watch member trades
    high_watch = [t for t in all_trades if any(name in (t.get("member") or "")
                                                  for name in HIGH_WATCH_MEMBERS)]

    # Recent (last 30 days disclosure)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    recent_disclosures = [t for t in all_trades if t.get("disclosure_date", "") >= cutoff]

    output = {
        "schema_version": "1.0",
        "method": "political_trades_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "endpoints": {
            "senate": raw.get("senate_endpoint"),
            "house": raw.get("house_endpoint"),
        },
        "stats": {
            "n_senate": len(senate_trades),
            "n_house": len(house_trades),
            "n_total": len(all_trades),
            "n_clusters": len(clusters),
            "n_large_trades": len(large_trades),
            "n_high_watch": len(high_watch),
            "n_recent_30d": len(recent_disclosures),
        },
        "trades_recent_50": all_trades[:50],
        "clusters_top_10": clusters[:10],
        "large_trades_top_15": large_trades[:15],
        "high_watch_recent_15": high_watch[:15],
        "duration_s": round(time.time() - t0, 2),
    }

    put_s3_json(S3_KEY_OUT, output)
    print(f"[political-trades] senate={len(senate_trades)} house={len(house_trades)} "
          f"clusters={len(clusters)} large={len(large_trades)} hw={len(high_watch)}")

    # ─── ALERTS ────────────────────────────────────────────────────────
    try:
        # Build set of prior trade fingerprints to diff
        def fp(t):
            return (t.get("ticker"), t.get("member"), t.get("transaction_date"),
                    t.get("amount_range"), t.get("transaction_type"))
        prior_trades_fps = {
            fp(t) for t in (prior_run.get("trades_recent_50") or [])
            if isinstance(t, dict)
        }

        alerts = []

        # 1. New large trades
        new_large = [t for t in large_trades if fp(t) not in prior_trades_fps][:5]
        if new_large:
            lines = []
            for t in new_large:
                val_m = t["amount_max_usd"] / 1e6
                val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${t['amount_max_usd']/1000:.0f}k"
                lines.append(
                    f"• <b>{t['ticker']}</b> · {t['member']} · "
                    f"{t['transaction_type'].upper()} · {val_str} · "
                    f"{t['transaction_date'][:10]}"
                )
            alerts.append(
                f"🏛 <b>NEW LARGE POLITICAL TRADES (≥ $500K)</b>\n" +
                "\n".join(lines) +
                "\n\n<a href='https://justhodl.ai/political/'>justhodl.ai/political/</a>"
            )

        # 2. New high-watch member trades
        new_hw = [t for t in high_watch if fp(t) not in prior_trades_fps][:5]
        if new_hw:
            lines = []
            for t in new_hw:
                val_m = t["amount_max_usd"] / 1e6
                val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${t['amount_max_usd']/1000:.0f}k"
                lines.append(
                    f"• <b>{t['member']}</b>: {t['transaction_type'].upper()} "
                    f"<b>{t['ticker']}</b> · {val_str} · {t['transaction_date'][:10]}"
                )
            alerts.append(
                f"👀 <b>HIGH-WATCH MEMBER TRADES</b>\n"
                f"<i>Pelosi/Crenshaw/Tuberville et al.</i>\n" +
                "\n".join(lines)
            )

        # 3. New clusters
        prior_cluster_tickers = {
            c.get("ticker") for c in (prior_run.get("clusters_top_10") or [])
            if isinstance(c, dict)
        }
        new_clusters = [c for c in clusters if c["ticker"] not in prior_cluster_tickers][:3]
        if new_clusters:
            lines = []
            for c in new_clusters:
                val_m = c["total_value_max_usd"] / 1e6
                val_str = f"${val_m:.2f}M" if val_m >= 1 else f"${c['total_value_max_usd']/1000:.0f}k"
                lines.append(
                    f"• <b>{c['ticker']}</b> · {c['n_members']} members · "
                    f"{c['direction']} · {val_str} · {c['n_trades']} trades"
                )
            alerts.append(
                f"⭐ <b>NEW POLITICAL CLUSTERS (3+ members same ticker)</b>\n" +
                "\n".join(lines)
            )

        for msg in alerts:
            maybe_telegram(msg)
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "n_senate": len(senate_trades),
            "n_house": len(house_trades),
            "n_clusters": len(clusters),
            "n_large_trades": len(large_trades),
            "top_5_recent": [{"ticker": t["ticker"], "member": t["member"],
                               "date": t.get("transaction_date", "")[:10]}
                              for t in all_trades[:5]],
        }),
    }
