"""justhodl-earnings-blackout — the corporate-bid switch, institutional flow regime.

Buybacks are the largest single source of US equity demand; issuers go dark in
the pre-earnings blackout. Desks (GS buyback desk convention) proxy the window
as [earnings_date - 30d, earnings_date + 2d]. This engine computes, for the
S&P 500 weighted by market cap:
  - blackout share NOW,
  - a 6-week DAILY forward curve of blackout share,
  - daily reporting intensity (share of cap printing that day),
and accumulates its own history. Proxy convention is stated honestly — real
windows are issuer-policy; this is the standard street approximation.

Sources: FMP /stable/sp500-constituent + /stable/earnings-calendar (dates),
FinViz universe (market caps, $M). Output data/earnings-blackout.json.
Consumers: signal-board ("Buyback Blackout"), blackout.html.
"""
import json, urllib.request
from datetime import datetime, timezone, timedelta, date
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/earnings-blackout.json"
HIST = "data/history/blackout.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3 = boto3.client("s3", region_name="us-east-1")
PRE, POST = 30, 2   # street proxy window


def _get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "jh/1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _s3json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def lambda_handler(event=None, context=None):
    members = {(r.get("symbol") or "").upper()
               for r in _get("https://financialmodelingprep.com/stable/sp500-constituent?apikey=" + FMP)}
    members.discard("")
    print("[blackout] sp500 members:", len(members))

    uni = (_s3json("data/finviz-universe.json", {}) or {}).get("by_ticker", {})
    caps = {t: (uni.get(t) or {}).get("market_cap") for t in members}
    caps = {t: c for t, c in caps.items() if isinstance(c, (int, float)) and c > 0}
    total_cap = sum(caps.values())
    print("[blackout] caps matched: %d/%d  total $%.1fT" % (len(caps), len(members), total_cap / 1e6))

    today = datetime.now(timezone.utc).date()
    frm, to = today - timedelta(days=PRE + 6), today + timedelta(days=45)
    # FMP silently truncates long from/to ranges — chunk in 7-day windows.
    cal, cur = [], frm
    while cur <= to:
        nxt = min(cur + timedelta(days=6), to)
        j = _get("https://financialmodelingprep.com/stable/earnings-calendar?from=%s&to=%s&limit=3000&apikey=%s"
                 % (cur.isoformat(), nxt.isoformat(), FMP))
        if isinstance(j, list):
            cal.extend(j)
        cur = nxt + timedelta(days=1)
    print("[blackout] calendar rows fetched (chunked):", len(cal))
    ed = {}
    for r in cal if isinstance(cal, list) else []:
        t = (r.get("symbol") or "").upper()
        d = r.get("date")
        if t in caps and d:
            ed.setdefault(t, []).append(d)
    for t in ed:
        ed[t] = sorted(set(ed[t]))
    print("[blackout] members w/ earnings date in window:", len(ed))

    def day_state(day):
        bo_cap = rep_cap = 0.0
        for t, ds in ed.items():
            c = caps[t]
            for dstr in ds:
                d0 = date.fromisoformat(dstr)
                if d0 - timedelta(days=PRE) <= day <= d0 + timedelta(days=POST):
                    bo_cap += c
                    break
            if day.isoformat() in ds:
                rep_cap += c
        return (100 * bo_cap / total_cap, 100 * rep_cap / total_cap) if total_cap else (None, None)

    curve = []
    for i in range(42):
        d = today + timedelta(days=i)
        b, r = day_state(d)
        curve.append({"date": d.isoformat(), "value": round(b, 1), "reporting_pct": round(r, 2)})
    now_pct = curve[0]["value"]
    rep14 = round(sum(c["reporting_pct"] for c in curve[:14]), 1)
    peak = max(curve, key=lambda c: c["value"])
    trough = min(curve, key=lambda c: c["value"])

    hist = _s3json(HIST, {}) or {}
    hist[today.isoformat()] = now_pct
    hist = dict(sorted(hist.items())[-500:])
    s3.put_object(Bucket=BUCKET, Key=HIST, Body=json.dumps(hist, separators=(",", ":")).encode(),
                  ContentType="application/json")

    state = ("DEEP_BLACKOUT" if now_pct >= 70 else "HEAVY_BLACKOUT" if now_pct >= 50
             else "WINDOW_OPEN" if now_pct <= 20 else "MIXED")
    doc = {"engine": "justhodl-earnings-blackout", "version": "1.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "now": {"blackout_mktcap_pct": now_pct, "state": state,
                   "n_members": len(members), "n_caps_matched": len(caps),
                   "n_with_dates": len(ed), "total_cap_t": round(total_cap / 1e6, 2)},
           "next_14d": {"reporting_mktcap_pct": rep14},
           "curve": curve,
           "peak": peak, "trough": trough,
           "history": [{"date": k, "value": v} for k, v in sorted(hist.items())][-260:],
           "method": ("Street-proxy blackout [T-%dd, T+%dd] around each S&P 500 member's next "
                      "earnings date (FMP calendar), market-cap weighted (FinViz caps). Real "
                      "windows are issuer policy; this is the standard desk approximation. "
                      "High blackout = the corporate bid is largely switched off." % (PRE, POST))}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print("[blackout] now=%.1f%% state=%s rep14=%.1f%% peak=%s(%.0f%%)"
          % (now_pct, state, rep14, peak["date"], peak["value"]))
    return {"ok": True, "now_pct": now_pct, "state": state, "matched": len(caps)}
