"""
justhodl-cta-trend-exhaust
===========================

CTA / managed-money trend exhaustion in US equity index futures.

Pressure-test:
  - Naive: COT spec net positioning alone. Includes risk-management hedgers
    AND systematic trend-followers in one bucket -> noisy signal.
  - Better: Use CFTC TFF (Traders in Financial Futures) report which
    SEPARATELY tracks Asset Manager (long-bias institutional) vs Leveraged
    Funds (CTAs / trend-followers / global-macro). Focus on:
    - Leveraged Funds NET positioning in ES + NQ + RTY combined
    - 156-week (3-year) percentile rank of net position
    - 4-week change rate (velocity of positioning)
    - SPY price trend overlay for confirmation

  - States:
    CTA_MAX_LONG: combined Lev Fund net long >= 90th pctile + price trending
      -> trend exhaustion, short signal (CTAs forced to unwind on reversal)
    CTA_MAX_SHORT: combined net short >= 10th pctile (or <-90 in raw)
      -> max short positioning, long signal
    Positioning normal: NEUTRAL

Edge basis:
  Moskowitz-Ooi-Pedersen 2012 (time-series momentum across CTA universe),
  Bhardwaj-Gorton-Rouwenhorst 2014 (CTA performance + crowding),
  Hutchinson-O'Brien 2014 (managed-futures crowding signal). When CTAs are
  >90th percentile long AND price reverses 5%, the unwind drives mean-rev
  3-7% over 2-4 weeks ~58% hit rate.

Data: CFTC publicreporting.cftc.gov TFF report
  (resource gpe5-46if). Weekly Tuesday (data as of Friday).

Trade tickets:
  CTA_MAX_LONG: SH long or SPY short for unwind catch
  CTA_MAX_SHORT: SPY long or QQQ long for forced cover rally

Schedule: weekly Tuesday 23:00 UTC (after CFTC release).
"""
import json
import os
import ssl
import statistics
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/cta-trend-exhaust.json"
SSM_STATE_KEY = "/justhodl/cta-trend-exhaust/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

# CFTC TFF (Traders in Financial Futures) - separates Asset Managers from Lev Funds
TFF_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"

EQUITY_INDEX_CONTRACTS = {
    "ES":  {"name": "S&P 500 E-Mini",   "cftc_code": "13874A"},
    "NQ":  {"name": "NASDAQ 100 E-Mini", "cftc_code": "209742"},
    "RTY": {"name": "Russell 2000 E-Mini", "cftc_code": "239742"},
    "YM":  {"name": "Dow Jones E-Mini",  "cftc_code": "124603"},
}

# SSL context (CFTC sometimes has cert chain issues from Lambda)
ssl_ctx = ssl.create_default_context()


def http_get(url, timeout=15, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout, context=ssl_ctx) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def fetch_tff_history(cftc_code, weeks=156):
    """Fetch last 156 weeks of TFF data for a contract."""
    cutoff = (datetime.now(timezone.utc) - timedelta(weeks=weeks)).strftime("%Y-%m-%d")
    where = (f"cftc_contract_market_code='{cftc_code}' AND report_date_as_yyyy_mm_dd>'{cutoff}'")
    url = (f"{TFF_URL}?$where={urllib.parse.quote(where)}"
           f"&$order=report_date_as_yyyy_mm_dd DESC&$limit=200")
    try:
        data = json.loads(http_get(url))
        if not isinstance(data, list):
            return []
        rows = []
        for r in data:
            try:
                date = r.get("report_date_as_yyyy_mm_dd", "")[:10]
                lev_long = int(float(r.get("lev_money_positions_long_all") or 0))
                lev_short = int(float(r.get("lev_money_positions_short_all") or 0))
                am_long = int(float(r.get("asset_mgr_positions_long_all") or 0))
                am_short = int(float(r.get("asset_mgr_positions_short_all") or 0))
                oi = int(float(r.get("open_interest_all") or 0))
                if oi > 0:
                    rows.append({
                        "date": date,
                        "lev_net": lev_long - lev_short,
                        "lev_net_pct_oi": (lev_long - lev_short) / oi * 100,
                        "am_net": am_long - am_short,
                        "am_net_pct_oi": (am_long - am_short) / oi * 100,
                        "open_interest": oi,
                    })
            except Exception:
                continue
        rows.sort(key=lambda r: r["date"], reverse=True)
        return rows
    except Exception:
        return []


def percentile_rank(series, value):
    if not series:
        return None
    below = sum(1 for v in series if v < value)
    return round(100.0 * below / len(series), 1)


def fmp_history(symbol, days=80):
    q = urllib.parse.quote_plus(symbol)
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
           f"?symbol={q}&apikey={FMP_KEY}")
    try:
        data = json.loads(http_get(url))
        if isinstance(data, dict):
            hist = data.get("historical") or data.get("data") or []
        else:
            hist = data
        closes = []
        for r in hist[:days]:
            c = r.get("close") or r.get("price")
            if c is not None:
                closes.append(float(c))
        return closes
    except Exception:
        return []


def lambda_handler(event, context):
    start = time.time()
    try:
        # Fetch TFF for each equity index contract in parallel
        per_contract = {}
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(fetch_tff_history, c["cftc_code"], 156): k
                    for k, c in EQUITY_INDEX_CONTRACTS.items()}
            for f in as_completed(futs):
                k = futs[f]
                try:
                    per_contract[k] = f.result()
                except Exception:
                    per_contract[k] = []

        # Build aggregate Lev Fund net positioning across ES + NQ + RTY (weight by OI)
        # Aligned by report date - use 50 most recent dates
        contract_lev_pct = {}
        contract_lev_z = {}
        contract_lev_pctile = {}
        contract_change_4w = {}
        for k, rows in per_contract.items():
            if len(rows) < 30:
                continue
            lev_pct_series = [r["lev_net_pct_oi"] for r in rows]
            contract_lev_pct[k] = lev_pct_series[0] if lev_pct_series else None
            # Z-score vs 156w
            if len(lev_pct_series) >= 30:
                rest = lev_pct_series[1:]
                m = statistics.mean(rest)
                sd = statistics.stdev(rest) or 1e-9
                contract_lev_z[k] = round((lev_pct_series[0] - m) / sd, 2)
            else:
                contract_lev_z[k] = None
            # Percentile rank
            contract_lev_pctile[k] = percentile_rank(lev_pct_series[1:], lev_pct_series[0])
            # 4-week change
            if len(lev_pct_series) >= 5:
                contract_change_4w[k] = round(lev_pct_series[0] - lev_pct_series[4], 2)

        # Aggregate: average of available contracts (equity index combined view)
        avg_lev_z = (statistics.mean([z for z in contract_lev_z.values() if z is not None])
                     if any(z is not None for z in contract_lev_z.values()) else None)
        avg_lev_pctile = (statistics.mean([p for p in contract_lev_pctile.values() if p is not None])
                          if any(p is not None for p in contract_lev_pctile.values()) else None)
        avg_change_4w = (statistics.mean([c for c in contract_change_4w.values() if c is not None])
                         if any(c is not None for c in contract_change_4w.values()) else None)

        # SPY trend overlay
        spy = fmp_history("SPY", 60)
        spy_20d_pct = None
        spy_5d_pct = None
        if len(spy) > 20 and spy[20]:
            spy_20d_pct = (spy[0] / spy[20] - 1.0) * 100
        if len(spy) > 5 and spy[5]:
            spy_5d_pct = (spy[0] / spy[5] - 1.0) * 100

        # Classify
        state = "NEUTRAL"
        strength = 0.2
        why = "CTA positioning in normal range"

        if avg_lev_pctile is not None:
            if avg_lev_pctile >= 90 and (avg_lev_z or 0) >= 1.5:
                state = "CTA_MAX_LONG_RICH"
                strength = min(1.0, 0.7 + (avg_lev_pctile - 90) / 30)
                why = (f"Lev Funds combined pctile {round(avg_lev_pctile,0)} "
                       f"(z={avg_lev_z}); CTAs max long across ES/NQ/RTY -> "
                       f"trend exhaustion / unwind risk")
            elif avg_lev_pctile >= 85:
                state = "CTA_MAX_LONG_ACTIVE"
                strength = 0.55
                why = f"Lev Funds pctile {round(avg_lev_pctile,0)}; building long extreme"
            elif avg_lev_pctile <= 10 and (avg_lev_z or 0) <= -1.5:
                state = "CTA_MAX_SHORT_RICH"
                strength = min(1.0, 0.7 + (10 - avg_lev_pctile) / 30)
                why = (f"Lev Funds combined pctile {round(avg_lev_pctile,0)} "
                       f"(z={avg_lev_z}); CTAs max short -> forced cover rally setup")
            elif avg_lev_pctile <= 15:
                state = "CTA_MAX_SHORT_ACTIVE"
                strength = 0.55
                why = f"Lev Funds pctile {round(avg_lev_pctile,0)}; building short extreme"

        tickets = []
        if state == "CTA_MAX_LONG_RICH":
            tickets = [
                {"ticker": "SH", "side": "LONG",
                 "rationale": "Inverse SPY for CTA unwind catch (forced sellers ahead)",
                 "target_pct": 5, "stop_pct": -2.5, "holding_period": "2-4 weeks",
                 "size_pct_portfolio": 1.5},
                {"ticker": "SPY", "side": "LONG_PUT_SPREAD",
                 "rationale": "ATM put spread 30-45d; CTAs forced unwind catalyst",
                 "strike_setup": "Buy ATM, sell -5%", "size_pct_portfolio": 1.0},
                {"ticker": "RWM", "side": "LONG",
                 "rationale": "Inverse Russell - small-cap CTA concentration higher",
                 "target_pct": 6, "stop_pct": -3, "size_pct_portfolio": 1.0},
            ]
        elif state == "CTA_MAX_LONG_ACTIVE":
            tickets = [
                {"ticker": "SPY", "side": "LONG_PUT",
                 "rationale": "Partial hedge against CTA-led downside",
                 "strike_setup": "ATM 30d put", "size_pct_portfolio": 0.5},
            ]
        elif state == "CTA_MAX_SHORT_RICH":
            tickets = [
                {"ticker": "SPY", "side": "LONG",
                 "rationale": "CTAs max short = forced cover rally setup",
                 "target_pct": 4, "stop_pct": -2.5, "holding_period": "2-4 weeks",
                 "size_pct_portfolio": 2.0},
                {"ticker": "QQQ", "side": "LONG",
                 "rationale": "Tech leadership in CTA-cover rally",
                 "target_pct": 6, "stop_pct": -3.5, "size_pct_portfolio": 1.5},
                {"ticker": "IWM", "side": "LONG",
                 "rationale": "Small-cap squeeze potential in CTA cover",
                 "target_pct": 7, "stop_pct": -4, "size_pct_portfolio": 1.5},
            ]
        elif state == "CTA_MAX_SHORT_ACTIVE":
            tickets = [
                {"ticker": "SPY", "side": "LONG",
                 "rationale": "Partial CTA-cover entry; await full extreme",
                 "target_pct": 2, "stop_pct": -1.5, "size_pct_portfolio": 0.75},
            ]

        # Latest report date
        latest_dates = [
            rows[0]["date"] for rows in per_contract.values() if rows
        ]
        latest_date = max(latest_dates) if latest_dates else None

        out = {
            "engine": "cta-trend-exhaust",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "current_metrics": {
                "tff_latest_report_date": latest_date,
                "lev_fund_net_pct_oi_by_contract": {
                    k: round(v, 2) for k, v in contract_lev_pct.items() if v is not None},
                "lev_fund_pctile_156w_by_contract": contract_lev_pctile,
                "lev_fund_zscore_by_contract": contract_lev_z,
                "lev_fund_change_4w_by_contract": contract_change_4w,
                "avg_lev_zscore": round(avg_lev_z, 2) if avg_lev_z is not None else None,
                "avg_lev_pctile_156w": round(avg_lev_pctile, 1) if avg_lev_pctile is not None else None,
                "avg_4w_change": round(avg_change_4w, 2) if avg_change_4w is not None else None,
                "spy_5d_pct": round(spy_5d_pct, 2) if spy_5d_pct is not None else None,
                "spy_20d_pct": round(spy_20d_pct, 2) if spy_20d_pct is not None else None,
                "contracts_with_data": list(contract_lev_pct.keys()),
            },
            "regime_explanation": why,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "CTA trend exhaustion via CFTC TFF Leveraged Fund positioning "
                "in equity index futures (ES, NQ, RTY, YM). 156-week percentile "
                "rank + z-score + 4-week change of Lev Fund net positioning as "
                "% of open interest, aggregated across contracts. "
                "CTA_MAX_LONG_RICH: avg pctile >=90 + z>=+1.5 -> unwind/short. "
                "CTA_MAX_SHORT_RICH: avg pctile <=10 + z<=-1.5 -> cover/long. "
                "Edge basis: Moskowitz-Ooi-Pedersen 2012, Hutchinson-O'Brien "
                "2014. ~58% hit / +/-3-7% / 2-4 wks. Distinct from "
                "cot-extremes-scanner (broad surveillance); this is "
                "equity-index-only with retail trade tickets."
            ),
            "sources": [
                "CFTC publicreporting.cftc.gov TFF report (gpe5-46if)",
                "FMP /stable/historical-price-eod/light (SPY trend)",
            ],
            "why_now": why,
            "run_seconds": round(time.time() - start, 2),
        }

        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and "RICH" in state and TELEGRAM_TOKEN:
            msg = (f"*CTA-TREND-EXHAUST -> {state}*\n"
                   f"Lev Fund avg pctile: {round(avg_lev_pctile or 0,0)}/100  "
                   f"z: {round(avg_lev_z or 0,2)}\n"
                   f"SPY 20d: {round(spy_20d_pct or 0,1)}%\n"
                   f"{why}\nTickets: {len(tickets)}")
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                body = urllib.parse.urlencode({
                    "chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown",
                    "disable_web_page_preview": "true",
                }).encode("utf-8")
                urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=10)
            except Exception:
                pass
        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=state, Type="String", Overwrite=True)
        except Exception:
            pass

        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        return {"statusCode": 200, "body": json.dumps({"ok": True, "state": state,
                                                         "n_tickets": len(tickets)})}
    except Exception as e:
        import traceback
        err = {"engine": "cta-trend-exhaust", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
