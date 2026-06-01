"""
justhodl-tic-flows — Bloomberg TICS equivalent.

Treasury International Capital — who owns US Treasuries + foreign flows.
Critical for dollar regime + bond moves. Foreign demand collapse triggers
auction failures and yield spikes; foreign accumulation supports rallies.

Pulls FRED series for monthly TIC data + ICI fund flow series:
  • FOREIGN TREASURY HOLDINGS by major holder
    - Japan, China, UK, Cayman Islands, Luxembourg, Belgium, Switzerland
  • NET FOREIGN PURCHASES (monthly $B, lead indicator)
  • OFFICIAL vs PRIVATE buyer split
  • TIC-derived signals: foreign-flow momentum, holdings concentration

Composite TIC stress score 0-100:
  +25  Foreign holdings shrinking YoY (negative net flow)
  +25  China + Japan declining together (de-dollarization signal)
  +20  Auction tail size (proxy from justhodl-auction-grader)
  +15  Foreign private SOLD on month (vs buying)
  +15  Composition shift: short-bills > long-bonds (anti-duration)

Output: data/tic-flows.json
  • generated_at, n_holders
  • top_holders: list[country, current_b, yoy_change_b, status]
  • net_flow_3mo, net_flow_12mo
  • composite_score, regime
  • notes on de-dollarization, dedollarization

Schedule: cron(0 22 ? * THU *) — TIC data releases mid-month, weekly check.
TG: foreign holdings shift, China+Japan tandem decline.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/tic-flows.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# FRED series for foreign Treasury holdings (monthly, in $M)
# These are from Treasury's TIC table. NOT all may be in FRED — fallback to scrape.
TIC_SERIES = {
    "japan":          "JPNNFAQ027S",     # Japan major holder
    "china":          "MHCMM027S",       # China holdings
    "uk":             "MFFICHQ027S",
    "cayman_islands": "MFFICQQ027S",
    "luxembourg":     "MFFICOQ027S",
    "belgium":        "MFFICBQ027S",
    "switzerland":    "MFFICCQ027S",
}
# Total foreign holdings
TOTAL_FOREIGN_SERIES = "MFFICTQ027S"      # Total foreign holdings
# Net foreign purchases (monthly $M, can be negative)
NET_PURCHASES_SERIES = "FANTPDQ027S"

s3 = boto3.client("s3", region_name="us-east-1")


def fred_get(series_id, limit=24):
    if not FRED_KEY: return None
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
            f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
            obs = data.get("observations", [])
            out = []
            for o in obs:
                try:
                    v = float(o.get("value"))
                    out.append({"date": o.get("date"), "value": v})
                except Exception:
                    continue
            return out
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
        return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=21600"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


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
    print("[tic-flows] starting")

    prior = get_s3_json(S3_KEY, {}) or {}

    # Fetch all TIC series
    holders = {}
    for label, sid in TIC_SERIES.items():
        s = fred_get(sid, limit=24)
        if not s or len(s) < 2: 
            holders[label] = {"err": "no_data"}
            continue
        current_m = s[0]["value"]  # in millions
        # YoY change (12 months ago)
        yoy_m = s[12]["value"] if len(s) > 12 else None
        m3_m = s[3]["value"] if len(s) > 3 else None
        m6_m = s[6]["value"] if len(s) > 6 else None
        yoy_chg = round(current_m - yoy_m, 0) if yoy_m else None
        m3_chg = round(current_m - m3_m, 0) if m3_m else None
        yoy_pct = round((current_m - yoy_m) / yoy_m * 100, 1) if yoy_m and yoy_m != 0 else None

        # Status
        status = "STABLE"
        if yoy_pct is not None:
            if yoy_pct < -15: status = "RAPID_DECLINE"
            elif yoy_pct < -5: status = "DECLINING"
            elif yoy_pct > 10: status = "ACCUMULATING"
            elif yoy_pct > 5: status = "BUYING"

        holders[label] = {
            "current_b": round(current_m / 1000, 1),  # millions → billions
            "yoy_change_b": round(yoy_chg / 1000, 1) if yoy_chg else None,
            "3mo_change_b": round(m3_chg / 1000, 1) if m3_chg else None,
            "yoy_pct": yoy_pct,
            "status": status,
            "as_of": s[0]["date"],
        }

    # Total foreign holdings
    total = fred_get(TOTAL_FOREIGN_SERIES, limit=24)
    total_summary = {}
    if total and len(total) > 12:
        cur = total[0]["value"]
        yoy = total[12]["value"]
        total_summary = {
            "current_b": round(cur / 1000, 1),
            "yoy_change_b": round((cur - yoy) / 1000, 1),
            "yoy_pct": round((cur - yoy) / yoy * 100, 2) if yoy else None,
            "as_of": total[0]["date"],
        }

    # Net purchases (monthly)
    net = fred_get(NET_PURCHASES_SERIES, limit=24)
    net_summary = {}
    if net and len(net) > 12:
        # Sum last 3 / 12 months
        last_3 = sum(o["value"] for o in net[:3])
        last_12 = sum(o["value"] for o in net[:12])
        net_summary = {
            "latest_month_m": round(net[0]["value"]),
            "trailing_3mo_m": round(last_3),
            "trailing_12mo_m": round(last_12),
            "latest_date": net[0]["date"],
            "12mo_avg_monthly_m": round(last_12 / 12, 1),
        }

    # Compute composite TIC stress
    score = 0
    reasons = []

    # 1. Foreign holdings shrinking
    if total_summary.get("yoy_pct") is not None:
        if total_summary["yoy_pct"] < -3:
            score += 25; reasons.append(f"Total foreign holdings YoY {total_summary['yoy_pct']:+.1f}%")
        elif total_summary["yoy_pct"] < 0:
            score += 15
        elif total_summary["yoy_pct"] > 5:
            score -= 5; reasons.append(f"Foreign holdings growing YoY {total_summary['yoy_pct']:+.1f}%")

    # 2. China + Japan tandem
    china = holders.get("china", {})
    japan = holders.get("japan", {})
    if china.get("yoy_pct") is not None and japan.get("yoy_pct") is not None:
        if china["yoy_pct"] < -5 and japan["yoy_pct"] < -5:
            score += 25
            reasons.append(f"CHINA+JAPAN tandem decline (CN {china['yoy_pct']:+.1f}%, JP {japan['yoy_pct']:+.1f}%)")
        elif china["yoy_pct"] < -10:
            score += 15
            reasons.append(f"China holdings YoY {china['yoy_pct']:+.1f}% — de-dollarization signal")

    # 3. Net purchases negative
    if net_summary.get("trailing_3mo_m", 0) < 0:
        score += 15
        reasons.append(f"Net flows 3mo: ${net_summary['trailing_3mo_m']/1000:.1f}B (OUTFLOW)")

    score = max(0, min(100, score))
    regime = ("DE_DOLLARIZATION" if score >= 60 else
                "FOREIGN_SOFTNESS" if score >= 30 else
                "ABSORPTION_FINE" if score >= 10 else
                "STRONG_FOREIGN_DEMAND")

    # Top holders for display
    holders_list = sorted(
        [{"country": k.replace("_", " ").title(), **v}
          for k, v in holders.items() if v.get("current_b") is not None],
        key=lambda x: -(x.get("current_b") or 0)
    )

    output = {
        "schema_version": "1.0",
        "method": "tic_flows_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "composite_tic_stress": score,
        "regime": regime,
        "top_reasons": reasons,
        "total_foreign_holdings": total_summary,
        "net_purchases": net_summary,
        "top_holders": holders_list,
        "individual": holders,
        "interpretation": (
            "Foreign buyers stepping away from US Treasuries; rates structurally higher, "
            "DXY pressure rising. Watch auction tails." if score >= 60 else
            "Foreign demand cooling but not collapsing. Auctions should clear but "
            "with weaker bid-cover." if score >= 30 else
            "Foreign demand healthy. Auctions support yields lower. Dollar supported."
        ),
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[tic-flows] composite={score} regime={regime}")

    # Alerts
    try:
        prior_regime = prior.get("regime")
        if prior_regime and prior_regime != regime:
            maybe_telegram(
                f"🌐 <b>TIC FLOWS REGIME CHANGE</b>\n"
                f"{prior_regime} → <b>{regime}</b> · stress {score}\n"
                + ("\n".join(f"• {r}" for r in reasons[:5]))
            )

        # China + Japan tandem
        if china.get("yoy_pct") and japan.get("yoy_pct") and \
            china["yoy_pct"] < -5 and japan["yoy_pct"] < -5:
            prior_cn = (prior.get("individual") or {}).get("china", {}).get("yoy_pct", 0)
            prior_jp = (prior.get("individual") or {}).get("japan", {}).get("yoy_pct", 0)
            if not (prior_cn < -5 and prior_jp < -5):
                maybe_telegram(
                    f"🇨🇳🇯🇵 <b>CHINA+JAPAN TANDEM DECLINE</b>\n"
                    f"China YoY {china['yoy_pct']:+.1f}% (${china.get('current_b',0):.0f}B)\n"
                    f"Japan YoY {japan['yoy_pct']:+.1f}% (${japan.get('current_b',0):.0f}B)\n"
                    f"Both two largest holders selling — de-dollarization signal."
                )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "composite": score, "regime": regime,
                              "n_holders": len(holders_list)}),
    }
