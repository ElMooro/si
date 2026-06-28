"""justhodl-crypto-cot · v1.0 — CME Bitcoin & Ether COT (institutional positioning).

You track Deribit/OKX perps (leverage of the crypto-native crowd) but nothing showed REGULATED
institutional positioning. The CFTC Commitments of Traders (Traders in Financial Futures report)
breaks CME BTC/ETH futures into:

  - ASSET MANAGERS / institutional   — real-money directional positioning
  - LEVERAGED FUNDS / hedge funds     — much of which is the basis trade (short CME vs long spot/ETF)
  - DEALERS / intermediaries

The high-signal read is the SPLIT: asset managers net long while leveraged funds net short is the
classic ETF-era cash-and-carry footprint (institutions accumulating, hedge funds harvesting basis,
NOT directionally bearish). Each cohort's net is percentile-ranked over ~3y to flag extremes.

SOURCE: CFTC public reporting (Socrata, free) — same TFF dataset the COT-extremes scanner uses.
Weekly data; self-history + central FDR ledger registration of the asset-manager directional lean.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-cot.json"
HIST_KEY = "data/crypto-cot-history.json"
TFF = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
CONTRACTS = {"BTC": "133741", "ETH": "146021"}


def _get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl Research raafouis@gmail.com"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _f(r, k):
    try:
        return float(r.get(k) or 0)
    except (TypeError, ValueError):
        return 0.0


def _pctile(v, arr):
    return round(100 * sum(1 for x in arr if x <= v) / len(arr)) if arr else None


def cot(code):
    params = {"cftc_contract_market_code": code,
              "$order": "report_date_as_yyyy_mm_dd DESC", "$limit": "160"}
    rows = _get(TFF + "?" + urllib.parse.urlencode(params))
    if not rows:
        return {"_err": "no rows"}
    am_hist = [_f(r, "asset_mgr_positions_long") - _f(r, "asset_mgr_positions_short") for r in rows]
    lf_hist = [_f(r, "lev_money_positions_long") - _f(r, "lev_money_positions_short") for r in rows]
    cur = rows[0]
    oi = _f(cur, "open_interest_all")
    am_net = am_hist[0]
    lf_net = lf_hist[0]
    dl_net = _f(cur, "dealer_positions_long_all") - _f(cur, "dealer_positions_short_all")
    am_chg = _f(cur, "change_in_asset_mgr_long") - _f(cur, "change_in_asset_mgr_short")
    lf_chg = _f(cur, "change_in_lev_money_long") - _f(cur, "change_in_lev_money_short")
    am_pct = _pctile(am_net, am_hist)
    lf_pct = _pctile(lf_net, lf_hist)

    def extreme(p):
        return "EXTREME LONG" if p is not None and p >= 90 else "EXTREME SHORT" if p is not None and p <= 10 else None

    divergence = None
    if am_net > 0 and lf_net < 0:
        divergence = ("Asset managers net LONG while leveraged funds net SHORT — the ETF-era "
                      "cash-and-carry footprint (institutions accumulating, hedge funds harvesting basis).")
    elif am_net < 0 and lf_net > 0:
        divergence = "Asset managers net SHORT while leveraged funds net LONG — institutions defensive, fast money long."

    return {
        "report_date": str(cur.get("report_date_as_yyyy_mm_dd"))[:10],
        "open_interest": round(oi),
        "asset_mgr": {"net": round(am_net), "long": round(_f(cur, "asset_mgr_positions_long")),
                      "short": round(_f(cur, "asset_mgr_positions_short")),
                      "pct_oi_long": _f(cur, "pct_of_oi_asset_mgr_long"),
                      "pct_oi_short": _f(cur, "pct_of_oi_asset_mgr_short"),
                      "net_pctile_3y": am_pct, "wk_change": round(am_chg), "extreme": extreme(am_pct),
                      "read": "net LONG" if am_net > 0 else "net SHORT"},
        "lev_funds": {"net": round(lf_net), "long": round(_f(cur, "lev_money_positions_long")),
                      "short": round(_f(cur, "lev_money_positions_short")),
                      "pct_oi_long": _f(cur, "pct_of_oi_lev_money_long"),
                      "pct_oi_short": _f(cur, "pct_of_oi_lev_money_short"),
                      "net_pctile_3y": lf_pct, "wk_change": round(lf_chg), "extreme": extreme(lf_pct),
                      "read": "net LONG" if lf_net > 0 else "net SHORT"},
        "dealers": {"net": round(dl_net)},
        "divergence": divergence,
    }


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    for sym, code in CONTRACTS.items():
        try:
            out[sym.lower()] = cot(code)
        except Exception as e:
            out[sym.lower()] = {"_err": str(e)[:120]}
            diag.append("%s:%s" % (sym, str(e)[:60]))

    btc = out.get("btc") or {}
    am = btc.get("asset_mgr") or {}
    lf = btc.get("lev_funds") or {}
    out["interpretation"] = btc.get("divergence") or (
        ("BTC asset managers %s (%sth pctile), leveraged funds %s." % (am.get("read"), am.get("net_pctile_3y"), lf.get("read")))
        if am.get("read") else None)

    try:
        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"series": []}
        ser = hist.get("series", [])
        rd = btc.get("report_date") or datetime.now(timezone.utc).date().isoformat()
        snap = {"date": rd, "btc_am_net": am.get("net"), "btc_am_pctile": am.get("net_pctile_3y"),
                "btc_lf_net": lf.get("net"), "eth_am_net": ((out.get("eth") or {}).get("asset_mgr") or {}).get("net")}
        ser = [x for x in ser if x.get("date") != rd] + [snap]
        ser = ser[-200:]
        hist["series"] = ser
        hist["updated_at"] = out["generated_at"]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        out["history_n"] = len(ser)
    except Exception as e:
        diag.append("hist:" + str(e)[:60])

    out["duration_s"] = round(time.time() - t0, 1)
    out["sources"] = ["CFTC TFF (publicreporting.cftc.gov) — CME Bitcoin 133741 / Ether 146021"]
    if diag:
        out["_diag"] = diag
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({"btc_am": am.get("read"), "btc_am_pctile": am.get("net_pctile_3y"),
                                                    "btc_lf": lf.get("read")})}
