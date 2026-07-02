"""
justhodl-margin-lending — NYSE margin debt + tri-party repo collateral + securities lending.

Complements existing justhodl-repo-monitor (which covers SOFR/RRP/funding rates)
by adding the user-portfolio-relevant leverage and squeeze indicators:

  1. NYSE MARGIN DEBT
       FRED: BOGZ1FL663067003Q (US Brokers + Dealers margin loans, quarterly)
       Convert to absolute level + as % of S&P 500 market cap
       Danger zone: > 2.5% = 2000-tech / 2007-housing top conditions

  2. TRI-PARTY REPO COLLATERAL MIX (proxy via FRED for clean tri-party)
       FRED RPONTSYAWARD = on-the-run Treasury repo award
       FRED RRPONTSYD = ON RRP take by counterparties
       Compute weekly direction of Treasury collateral demand

  3. CONSUMER CREDIT MOMENTUM
       FRED TOTALSL — total revolving + non-revolving consumer credit
       Acceleration → margin debt parallel for retail leverage

  4. SQUEEZE RISK SCORE 0-100
       +30 margin debt as % of cap > 2.5%
       +25 margin debt 6mo growth > 25%
       +20 SOFR-EFFR spread > 10bps (funding stress)
       +15 consumer credit YoY > 8%
       +10 RRP take falling fast (drainage)

Outputs:
  data/margin-lending.json
    - margin_debt: {absolute_usd, as_pct_of_sp500_cap, 6mo_growth_pct, status}
    - repo_collateral_proxy: {rrp_award, rrp_direction, sofr_rate, sofr_direction}
    - consumer_credit: {total_outstanding, yoy_pct, momentum}
    - squeeze_risk_score: 0-100
    - interpretation: narrative

Schedule: cron(0 14 ? * MON-FRI *) — daily 14:00 UTC (after FRED morning refresh)

Telegram alerts:
  - Margin debt % of cap crosses 2.5% from below
  - Squeeze risk score >= 65 (was below 50 last run)
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/margin-lending.json"

FRED_API_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# FRED series we'll pull
# 2026-05-15 FRED ID audit: replaced 4 dead/renamed series.
#   BOGZ1FL663068005Q → dropped (margin_debt already covers it).
#   SOFRVOLUME → SOFR (rate). The "volume" series was retired; daily rate is
#     the standard reference.
#   WILL5000PR → WILL5000IND (Wilshire 5000 index level, used as market cap
#     proxy by sister Lambda justhodl-repo-lending).
#   RIFSPBLP → dropped (use repo-lending's H.4.1 reverse-repo data instead).
FRED_SERIES = {
    "margin_debt":          "BOGZ1FL663067003Q",   # quarterly, $B (verified working)
    "consumer_credit":      "TOTALSL",             # monthly, $B
    "revolving_credit":     "REVOLSL",
    "sofr_rate":            "SOFR",                # daily, % (replaces dead SOFRVOLUME)
    "rrp_award":            "RRPONTSYD",           # daily, $B
    "wilshire_5000":        "WILL5000IND",         # market cap proxy (replaces dead WILL5000PR)
    "sp500_close":          "SP500",
    "tga":                  "WTREGEN",             # treasury balance
}

s3 = boto3.client("s3", region_name="us-east-1")


def http_get_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_fred_series(series_id, limit=80):
    url = (f"https://api.stlouisfed.org/fred/series/observations?"
           f"series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
           f"&limit={limit}&sort_order=desc")
    try:
        d = http_get_json(url)
        obs = d.get("observations", [])
        cleaned = []
        for o in obs:
            try:
                val = float(o["value"])
                cleaned.append({"date": o["date"], "value": val})
            except (ValueError, KeyError):
                continue
        return cleaned
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
        return []


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception: return default


def put_s3_json(key, body):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=900")


def estimate_market_cap_trillions(series_data):
    """Estimate US equity market cap in trillions $.
    Try Wilshire 5000 IND; fall back to SP500 × heuristic.
    Same approach as sister Lambda justhodl-repo-lending."""
    wil = series_data.get("wilshire_5000") or []
    if wil:
        # WILL5000IND ≈ total US market cap in $B; convert to trillions
        return round(wil[0]["value"] / 1000, 2)
    sp = series_data.get("sp500_close") or []
    if sp:
        # Heuristic: S&P represents ~80% of US market cap.
        # At SP500 ~5800 ≈ $50T total US equity cap → multiplier ≈ 0.0095
        return round(sp[0]["value"] * 0.0095, 2)
    return None


def compute_margin_debt_pct_of_cap(margin_debt_b, market_cap_t):
    """margin_debt in $B, market_cap in $T → pct."""
    if not margin_debt_b or not market_cap_t or market_cap_t <= 0:
        return None
    return round(100 * (margin_debt_b / 1000) / market_cap_t, 3)


def compute_growth(history, periods_back):
    if not history or len(history) < periods_back + 1: return None
    latest = history[0].get("value")
    earlier = history[periods_back].get("value")
    if latest is None or earlier is None or earlier == 0: return None
    return round(100 * (latest - earlier) / earlier, 2)


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




# ═══════════════ LEVERAGE MONITOR v2 (ops 2707) ═══════════════
# Fills the true institutional gaps the v1 (quarterly Z.1) engine couldn't:
# FINRA MONTHLY margin debt (the canonical retail-leverage series), Chicago
# Fed NFCI Leverage subindex (weekly), OFR hedge-fund leverage (probe-gated),
# plus synthesis of the fleet's own leveraged-ETF tilt + crypto OI/funding.
import re as _re, zipfile as _zf, statistics as _st
FINRA_HIST_KEY = "data/history/finra-margin.json"
FINRA_URLS = [
    "https://www.finra.org/investors/investing/investment-products/stocks/margin-statistics",
    "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics",
]
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}


def _ua_get(url, timeout=25, binary=False):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) jh/1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "ignore")


def _xlsx_rows_coord(blob):
    """Coordinate-aware xlsx reader (NAAIM lesson: doc-order shifts on empty cells)."""
    z = _zf.ZipFile(io.BytesIO(blob))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        sx = z.read("xl/sharedStrings.xml").decode("utf-8", "ignore")
        shared = [_re.sub(r"<[^>]+>", "", m) for m in _re.findall(r"<si>(.*?)</si>", sx, _re.S)]
    sheet = next((n for n in z.namelist() if _re.match(r"xl/worksheets/sheet1\.xml", n)), None)
    if not sheet:
        return []
    xml = z.read(sheet).decode("utf-8", "ignore")
    rows = []
    for rxml in _re.findall(r"<row[^>]*>(.*?)</row>", xml, _re.S):
        row = {}
        for ref, ctype, cv in _re.findall(r'<c[^>]*?r="([A-Z]+)\d+"[^>]*?(?:t="(\w+)")?[^>]*>.*?<v>(.*?)</v>', rxml, _re.S):
            if ctype == "s":
                try:
                    cv = shared[int(cv)]
                except Exception:
                    pass
            col = 0
            for ch in ref:
                col = col * 26 + (ord(ch) - 64)
            row[col - 1] = cv
        if row:
            rows.append([row.get(i, "") for i in range(max(row) + 1)])
    return rows


def _rows_to_finra(rows):
    """rows (xlsx or html-table cells) -> {YYYY-MM: debit_$M}. Debit balances
    live in the 100,000-2,000,000 $M band; month token or excel serial keys."""
    out = {}
    for row in rows:
        ym = None
        for cell in row[:3]:
            c = str(cell).strip()
            m = _re.match(r"([A-Za-z]{3,9})[\s\-/,]*'?(\d{2,4})$", c)
            if m and m.group(1)[:3].lower() in _MONTHS:
                y = int(m.group(2)); y += 2000 if y < 50 else 1900 if y < 100 else 0
                ym = "%04d-%02d" % (y, _MONTHS[m.group(1)[:3].lower()]); break
            try:
                n = float(c)
                if 30000 < n < 60000:
                    d = datetime(1899, 12, 30) + timedelta(days=n)
                    ym = d.strftime("%Y-%m"); break
            except Exception:
                pass
        if not ym:
            continue
        for cell in row:
            try:
                v = float(str(cell).replace(",", "").replace("$", "").strip())
            except Exception:
                continue
            if 100_000 <= v <= 2_000_000:
                out[ym] = round(v, 0); break
    return out


def _finra_live():
    """Best-effort live pull: discover xlsx (full history) else page table."""
    for url in FINRA_URLS:
        try:
            html = _ua_get(url)
        except Exception as e:
            print(f"[lev] finra page {url[:60]}: {str(e)[:60]}"); continue
        got = {}
        for href in _re.findall(r'href="([^"]+\.xlsx[^"]*)"', html, _re.I):
            if "margin" not in href.lower():
                continue
            u = href if href.startswith("http") else "https://www.finra.org" + href
            try:
                got = _rows_to_finra(_xlsx_rows_coord(_ua_get(u, binary=True, timeout=40)))
                if got:
                    print(f"[lev] finra xlsx parsed: {len(got)} months from {u[:70]}"); return got
            except Exception as e:
                print(f"[lev] finra xlsx fail: {str(e)[:60]}")
        rows = [[_re.sub(r"<[^>]+>", " ", c).strip() for c in _re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, _re.S)]
                for tr in _re.findall(r"<tr[^>]*>(.*?)</tr>", html, _re.S)]
        got = _rows_to_finra(rows)
        if got:
            print(f"[lev] finra html table parsed: {len(got)} months"); return got
    return {}


def _finra_layer(market_cap_t):
    hist = get_s3_json(FINRA_HIST_KEY, {}) or {}
    hist = {k: v for k, v in hist.items() if _re.match(r"\d{4}-\d{2}$", str(k))}
    live = {}
    try:
        live = _finra_live()
    except Exception as e:
        print(f"[lev] finra live err {str(e)[:60]}")
    src = "live+stored" if live else "stored_only"
    hist.update(live)
    if hist:
        put_s3_json(FINRA_HIST_KEY, hist)
    ser = sorted(hist.items())
    if not ser:
        return {"status": "UNAVAILABLE", "source": src}, []
    ym, vM = ser[-1]
    vB = round(vM / 1000.0, 1)
    def _at(back):
        return ser[-1 - back][1] if len(ser) > back else None
    yoy = round(100 * (vM / _at(12) - 1), 1) if _at(12) else None
    m6 = round(100 * (vM / _at(6) - 1), 1) if _at(6) else None
    yoys = []
    for i in range(12, len(ser)):
        p = ser[i - 12][1]
        if p:
            yoys.append(100 * (ser[i][1] / p - 1))
    z = round((yoy - _st.fmean(yoys)) / _st.pstdev(yoys), 2) if yoy is not None and len(yoys) > 24 and _st.pstdev(yoys) else None
    pct_cap = round(100 * vB / (market_cap_t * 1000), 2) if market_cap_t else None
    if yoy is not None and yoy <= -15:
        st = "FORCED_DELEVERAGING"
    elif (z is not None and z >= 1.5) or (yoy is not None and yoy >= 45):
        st = "EXCESSIVE_BUILD"
    elif yoy is not None and yoy >= 15:
        st = "BUILDING"
    elif yoy is not None and yoy <= -5:
        st = "CONTRACTING"
    else:
        st = "NORMAL"
    layer = {"latest_b": vB, "latest_month": ym, "yoy_pct": yoy, "m6_pct": m6,
             "yoy_z": z, "pct_of_market_cap": pct_cap, "months_n": len(ser),
             "status": st, "source": src, "provisional": len(ser) < 24,
             "note": "FINRA monthly debit balances in customers' securities margin accounts"}
    chart = [{"date": k + "-01", "value": round(v / 1000.0, 1)} for k, v in ser[-300:]]
    return layer, chart


def _deep_nums(doc, keys, out=None, depth=0):
    out = [] if out is None else out
    if depth > 8:
        return out
    if isinstance(doc, dict):
        for k, v in doc.items():
            if isinstance(v, (int, float)) and any(t in str(k).lower() for t in keys):
                out.append(float(v))
            else:
                _deep_nums(v, keys, out, depth + 1)
    elif isinstance(doc, list):
        for v in doc[:400]:
            _deep_nums(v, keys, out, depth + 1)
    return out


def _ofr_hf_layer():
    for u in ("https://www.financialresearch.gov/hedge-fund-monitor/api/series/leverage.json",
              "https://www.financialresearch.gov/hedge-fund-monitor/data/leverage.json",
              "https://www.financialresearch.gov/hedge-fund-monitor/chart-data/gross-leverage.json"):
        try:
            j = json.loads(_ua_get(u, timeout=15))
            nums = _deep_nums(j, ("gross", "leverage", "value"))
            if nums:
                return {"status": "OK", "latest": nums[-1], "source": u}
        except Exception:
            continue
    return {"status": "UNAVAILABLE",
            "note": "OFR HF monitor exposes charts, no stable public JSON found; layer excluded from composite (weight renormalized)"}


def _clamp_z(z, lim=2.5):
    return max(-lim, min(lim, z))


def lambda_handler(event, context):
    t0 = time.time()
    print("[margin-lending] starting")

    # Fetch all series
    series_data = {}
    for name, sid in FRED_SERIES.items():
        series_data[name] = get_fred_series(sid, limit=40)

    # ─── 1. Margin Debt ──────────────────────────────────────────────
    md_hist = series_data["margin_debt"]
    # FRED reports BOGZ1FL663067003Q in MILLIONS — convert to billions
    md_now = (md_hist[0]["value"] / 1000) if md_hist else None
    md_date = md_hist[0]["date"] if md_hist else None
    md_yoy = compute_growth(md_hist, 4)  # quarterly, 4 = 1 year
    md_2q = compute_growth(md_hist, 2)   # 6 months

    market_cap_t = estimate_market_cap_trillions(series_data)
    md_pct_of_cap = compute_margin_debt_pct_of_cap(md_now, market_cap_t)

    if md_pct_of_cap is None:
        md_status = "DATA_MISSING"
    elif md_pct_of_cap >= 2.5:
        md_status = "DANGER"
    elif md_pct_of_cap >= 2.0:
        md_status = "ELEVATED"
    elif md_pct_of_cap >= 1.5:
        md_status = "NORMAL"
    else:
        md_status = "LOW"

    # ─── 2. Repo Collateral Proxy ────────────────────────────────────
    rrp_hist = series_data["rrp_award"]
    rrp_now = rrp_hist[0]["value"] if rrp_hist else None
    rrp_5d_avg = (sum(o["value"] for o in rrp_hist[:5]) / 5) if len(rrp_hist) >= 5 else None
    rrp_30d_avg = (sum(o["value"] for o in rrp_hist[:30]) / 30) if len(rrp_hist) >= 30 else None
    rrp_direction = None
    if rrp_5d_avg is not None and rrp_30d_avg is not None and rrp_30d_avg > 0:
        chg_pct = 100 * (rrp_5d_avg - rrp_30d_avg) / rrp_30d_avg
        if chg_pct > 15: rrp_direction = "INCREASING_TAKE"
        elif chg_pct < -15: rrp_direction = "DRAINAGE"
        else: rrp_direction = "STABLE"

    # SOFR rate (replaces dead SOFRVOLUME — we now track the funding rate itself)
    sofr_rate_obs = series_data["sofr_rate"]
    sofr_rate_now = sofr_rate_obs[0]["value"] if sofr_rate_obs else None

    # Compute SOFR 5d vs 30d average for funding-stress direction
    sofr_5d_avg = sum(o["value"] for o in sofr_rate_obs[:5]) / 5 if len(sofr_rate_obs) >= 5 else None
    sofr_30d_avg = sum(o["value"] for o in sofr_rate_obs[:30]) / 30 if len(sofr_rate_obs) >= 30 else None
    sofr_direction = None
    if sofr_5d_avg is not None and sofr_30d_avg is not None:
        bps_chg = (sofr_5d_avg - sofr_30d_avg) * 100
        if bps_chg > 5: sofr_direction = "RISING"
        elif bps_chg < -5: sofr_direction = "FALLING"
        else: sofr_direction = "STABLE"

    # ─── 3. Consumer Credit ──────────────────────────────────────────
    cc = series_data["consumer_credit"]
    cc_now = cc[0]["value"] if cc else None
    cc_yoy = compute_growth(cc, 12)
    rev = series_data["revolving_credit"]
    rev_now = rev[0]["value"] if rev else None
    rev_yoy = compute_growth(rev, 12)

    cc_momentum = None
    if cc_yoy is not None:
        if cc_yoy > 8: cc_momentum = "HOT"
        elif cc_yoy > 5: cc_momentum = "WARM"
        elif cc_yoy > 2: cc_momentum = "NORMAL"
        elif cc_yoy > -2: cc_momentum = "COOLING"
        else: cc_momentum = "CONTRACTING"

    # ─── 4. Squeeze Risk Composite ───────────────────────────────────
    squeeze_score = 0
    squeeze_reasons = []

    if md_pct_of_cap is not None and md_pct_of_cap > 2.5:
        squeeze_score += 30
        squeeze_reasons.append(f"Margin debt {md_pct_of_cap}% of cap (>2.5% = 2000/2007 zone)")
    elif md_pct_of_cap is not None and md_pct_of_cap > 2.0:
        squeeze_score += 15
        squeeze_reasons.append(f"Margin debt {md_pct_of_cap}% of cap (elevated)")

    if md_2q is not None and md_2q > 25:
        squeeze_score += 25
        squeeze_reasons.append(f"Margin debt up {md_2q:+.1f}% in 6mo (frothy)")
    elif md_2q is not None and md_2q > 15:
        squeeze_score += 10
        squeeze_reasons.append(f"Margin debt up {md_2q:+.1f}% in 6mo")

    if cc_yoy is not None and cc_yoy > 8:
        squeeze_score += 15
        squeeze_reasons.append(f"Consumer credit YoY {cc_yoy:+.1f}% (high)")

    if rrp_direction == "DRAINAGE":
        squeeze_score += 10
        squeeze_reasons.append("RRP drainage — bank reserves under pressure")

    # SOFR rising materially is the funding-stress equivalent of the dead RIFSPBLP series
    if sofr_direction == "RISING" and sofr_5d_avg is not None and sofr_30d_avg is not None:
        bps = (sofr_5d_avg - sofr_30d_avg) * 100
        if bps > 10:
            squeeze_score += 20
            squeeze_reasons.append(f"SOFR up {bps:+.1f}bp (5d vs 30d) — funding tightening")

    squeeze_score = min(100, squeeze_score)
    if squeeze_score >= 65: squeeze_band = "HIGH"
    elif squeeze_score >= 35: squeeze_band = "ELEVATED"
    elif squeeze_score >= 15: squeeze_band = "NORMAL"
    else: squeeze_band = "LOW"

    # Interpretation
    if squeeze_band == "HIGH":
        interp = ("Late-cycle leverage build-up. Margin debt elevated, consumer credit "
                  "hot, funding markets tight. Position for volatility spike + deleveraging risk.")
    elif squeeze_band == "ELEVATED":
        interp = ("Leverage measures running above neutral. Watch trajectory — sustained "
                  "elevation typical of late-cycle. Maintain hedges.")
    elif squeeze_band == "NORMAL":
        interp = ("Leverage indicators in normal historical range. No squeeze setup. "
                  "Risk-on environment for measured positioning.")
    else:
        interp = ("Leverage deeply suppressed. Often follows deleveraging events. "
                  "Bottoming conditions if other oversold indicators align.")


    # ═══════════ LEVERAGE MONITOR v2 assembly (ops 2707) ═══════════
    finra, finra_chart = _finra_layer(market_cap_t)

    nfci = get_fred_series("NFCILEVERAGE", limit=900)  # weekly; + = tighter, - = leverage building
    nfci_layer = {"status": "UNAVAILABLE"}
    nfci_z = None
    if nfci:
        vals = [o["value"] for o in nfci]
        cur = vals[0]
        mu, sd = _st.fmean(vals), _st.pstdev(vals)
        nfci_z = round((cur - mu) / sd, 2) if sd else None
        d4 = round(cur - vals[4], 3) if len(vals) > 4 else None
        st = ("DELEVERAGING_STRESS" if cur >= 0.6 else "TIGHTENING" if cur >= 0.15
              else "LEVERAGE_HOT" if cur <= -0.6 else "BUILDING" if cur <= -0.15 else "NEUTRAL")
        nfci_layer = {"latest": round(cur, 3), "z": nfci_z, "chg_4w": d4,
                      "date": nfci[0]["date"], "status": st,
                      "note": "Chicago Fed NFCI Leverage subindex; NEGATIVE = looser = leverage building"}

    hf_layer = _ofr_hf_layer()

    # spec-ETF (ops 2708 precise): radar complexes carry bull/bear leveraged 5d
    # flows explicitly — no z is precomputed for the LEVERAGED slice, so we
    # accumulate our own daily net-tilt history and z it (provisional < 40 obs).
    etf_layer = {"status": "UNAVAILABLE"}
    radar = get_s3_json("data/capital-flow-radar.json", {}) or {}
    cx = radar.get("complexes") or []
    rows = [(c.get("complex") or c.get("name") or "?",
             (c.get("bull_lev_flow_5d") or 0) - (c.get("bear_lev_flow_5d") or 0))
            for c in cx if isinstance(c, dict)
            and (c.get("bull_lev_flow_5d") is not None or c.get("bear_lev_flow_5d") is not None)]
    if rows:
        net = round(sum(v for _, v in rows), 0)
        pos_share = round(100 * sum(1 for _, v in rows if v > 0) / len(rows), 1)
        hist_key = "data/history/lev-etf-tilt.json"
        th = get_s3_json(hist_key, {}) or {}
        th[datetime.now(timezone.utc).strftime("%Y-%m-%d")] = net
        th = dict(sorted(th.items())[-400:])
        put_s3_json(hist_key, th)
        tv = [v for _, v in sorted(th.items())]
        tz = round((net - _st.fmean(tv)) / _st.pstdev(tv), 2) if len(tv) >= 40 and _st.pstdev(tv) else None
        top3 = sorted(rows, key=lambda x: -abs(x[1]))[:3]
        etf_layer = {"status": "OK", "net_lev_5d_usd": net, "pct_complexes_bull": pos_share,
                     "n_complexes": len(rows), "tilt_z": tz, "tilt_history_n": len(tv),
                     "provisional": len(tv) < 40,
                     "top3": [{"complex": n, "net_5d_usd": round(v, 0)} for n, v in top3],
                     "note": "bull-minus-bear leveraged 5d ETF flow per complex (capital-flow-radar); z from own accumulated daily tilt history"}
    elif radar.get("leveraged_positioning") is not None:
        nets = _deep_nums(radar.get("leveraged_positioning"), ("net",))
        etf_layer = {"status": "OK", "net_lev_5d_usd": round(sum(nets), 0) if nets else None,
                     "n_complexes": len(nets), "tilt_z": None,
                     "note": "fallback deep-scan of leveraged_positioning board"}

    # crypto (ops 2708 precise): crypto-funding rows are per-asset OKX perps
    # with funding_z_score + oi_usd; aggregate across all asset rows and label
    # the OI scope honestly (OKX perps, not all-exchange).
    cr_layer = {"status": "UNAVAILABLE"}
    cf = get_s3_json("data/crypto-funding.json", {}) or {}
    def _asset_rows(doc):
        out = []
        stack = [doc]
        while stack:
            d = stack.pop()
            if isinstance(d, dict):
                if "funding_z_score" in d or "oi_usd" in d:
                    out.append(d)
                else:
                    stack.extend(d.values())
            elif isinstance(d, list):
                stack.extend(d[:120])
        return out
    ar = _asset_rows(cf)
    fz = [r["funding_z_score"] for r in ar if isinstance(r.get("funding_z_score"), (int, float))]
    oi = [r["oi_usd"] for r in ar if isinstance(r.get("oi_usd"), (int, float))]
    if ar:
        cr_layer = {"status": "OK", "assets_n": len(ar),
                    "funding_z_med": round(_st.median(fz), 2) if fz else None,
                    "funding_z_max": round(max(fz), 2) if fz else None,
                    "okx_perp_oi_usd_b": round(sum(oi) / 1e9, 1) if oi else None,
                    "note": "per-asset OKX perp rows from crypto-funding: median/max funding z (crowding) + summed OI (OKX scope)"}

    comps = []
    if finra.get("yoy_z") is not None:
        comps.append((0.30, _clamp_z(finra["yoy_z"])))
    elif finra.get("yoy_pct") is not None:
        comps.append((0.30, _clamp_z(finra["yoy_pct"] / 20.0)))
    if nfci_z is not None:
        comps.append((0.25, _clamp_z(-nfci_z)))          # building = negative NFCI = +leverage
    if isinstance(hf_layer.get("latest"), (int, float)):
        comps.append((0.15, 0.0))                          # level w/o history: neutral until series accumulates
    if etf_layer.get("tilt_z") is not None:
        comps.append((0.15, _clamp_z(etf_layer["tilt_z"])))
    if cr_layer.get("funding_z_med") is not None:
        comps.append((0.15, _clamp_z(cr_layer["funding_z_med"])))
    tw = sum(w for w, _ in comps) or 1.0
    cycle = round(max(0, min(100, 50 + 20 * sum(w * z for w, z in comps) / tw)), 1)

    prior_lm = (get_s3_json(S3_KEY_OUT, {}) or {}).get("leverage_monitor") or {}
    d_cycle = round(cycle - prior_lm["cycle_score"], 1) if isinstance(prior_lm.get("cycle_score"), (int, float)) else None
    fy = finra.get("yoy_pct")
    if fy is not None and fy <= -15:
        phase = "FORCED_DELEVERAGING"
    elif cycle >= 70:
        phase = "EXCESSIVE_ROLLING" if (d_cycle is not None and d_cycle < -1.5) else "EXCESSIVE_BUILDING"
    elif cycle >= 55:
        phase = "BUILDING"
    elif cycle <= 35:
        phase = "REBUILDING" if (d_cycle is not None and d_cycle > 1.5) else "LOW"
    else:
        phase = "COOLING" if (d_cycle is not None and d_cycle < -1.5) else "NEUTRAL"

    LM = {"version": "2.0.0", "cycle_score": cycle, "phase": phase, "delta_vs_prior": d_cycle,
          "n_layers_live": sum(1 for L in (finra, nfci_layer, hf_layer, etf_layer, cr_layer)
                               if L.get("status") not in (None, "UNAVAILABLE")),
          "layers": {"retail_finra": dict(finra, history=finra_chart),
                     "system_nfci": nfci_layer, "hedge_funds_ofr": hf_layer,
                     "spec_etf": etf_layer, "crypto": cr_layer},
          "method": ("Composite of clamped z-scores, weights renormalized over live layers "
                     "(retail .30 / NFCI .25 / HF .15 / lev-ETF .15 / crypto .15); "
                     "phase = level x momentum quadrant + FINRA forced-deleveraging override")}
    print(f"[lev] cycle={cycle} phase={phase} finra={finra.get('latest_b')}B "
          f"yoy={fy} nfci={nfci_layer.get('latest')} layers={LM['n_layers_live']}/5")

    output = {
        "schema_version": "2.0",
        "method": "leverage_monitor_v2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "margin_debt": {
            "absolute_usd_b": md_now,
            "as_pct_of_market_cap": md_pct_of_cap,
            "yoy_pct": md_yoy,
            "growth_6mo_pct": md_2q,
            "latest_date": md_date,
            "status": md_status,
            "interpretation": (
                f"Margin debt at ${md_now:.0f}B = {md_pct_of_cap}% of est. US market cap. "
                f"{'⚠️ Danger zone (>2.5%).' if md_pct_of_cap and md_pct_of_cap >= 2.5 else 'Normal range.' if md_pct_of_cap else ''}"
                if md_now else "Margin debt data missing"
            ),
        },
        "repo_collateral_proxy": {
            "rrp_award_b": rrp_now,
            "rrp_5d_avg_b": round(rrp_5d_avg, 2) if rrp_5d_avg else None,
            "rrp_30d_avg_b": round(rrp_30d_avg, 2) if rrp_30d_avg else None,
            "rrp_direction": rrp_direction,
            "sofr_rate_pct": sofr_rate_now,
            "sofr_5d_avg_pct": round(sofr_5d_avg, 3) if sofr_5d_avg else None,
            "sofr_30d_avg_pct": round(sofr_30d_avg, 3) if sofr_30d_avg else None,
            "sofr_direction": sofr_direction,
            "interpretation": (
                f"RRP {rrp_direction or 'unknown'}. "
                f"SOFR {sofr_rate_now}% ({sofr_direction or 'unknown'})."
                if sofr_rate_now else "Repo data partial."
            ),
        },
        "consumer_credit": {
            "total_outstanding_b": cc_now,
            "yoy_pct": cc_yoy,
            "revolving_outstanding_b": rev_now,
            "revolving_yoy_pct": rev_yoy,
            "momentum": cc_momentum,
            "interpretation": (
                f"Total ${cc_now:.0f}B (YoY {cc_yoy:+.1f}%). "
                f"Revolving ${rev_now:.0f}B (YoY {rev_yoy:+.1f}%). "
                f"Momentum: {cc_momentum}."
                if cc_now else "Consumer credit data missing"
            ),
        },
        "squeeze_risk": {
            "score": squeeze_score,
            "band": squeeze_band,
            "reasons": squeeze_reasons,
            "interpretation": interp,
        },
        "leverage_monitor": LM,
        "duration_s": round(time.time() - t0, 2),
    }

    prior_run = get_s3_json(S3_KEY_OUT, {}) or {}
    put_s3_json(S3_KEY_OUT, output)
    put_s3_json("data/leverage-monitor.json", output)   # page + fleet alias

    print(f"[margin-lending] md={md_pct_of_cap}% squeeze={squeeze_score}({squeeze_band})")

    # Alerts
    try:
        prior_md_pct = (prior_run.get("margin_debt") or {}).get("as_pct_of_market_cap")
        prior_squeeze = (prior_run.get("squeeze_risk") or {}).get("score", 0)

        if md_pct_of_cap is not None and md_pct_of_cap >= 2.5 and \
           (prior_md_pct is None or prior_md_pct < 2.5):
            maybe_telegram(
                f"🚨 <b>MARGIN DEBT DANGER ZONE</b>\n"
                f"Margin debt now <b>{md_pct_of_cap}%</b> of estimated market cap.\n"
                f"<i>Historical 2000-tech top at 2.7%; 2007-housing at 2.9%.</i>\n"
                f"6mo growth: {md_2q:+.1f}%   YoY: {md_yoy:+.1f}%"
            )

        if squeeze_score >= 65 and prior_squeeze < 50:
            maybe_telegram(
                f"⚠️ <b>SQUEEZE RISK ELEVATED: {squeeze_score}/100</b>\n"
                f"<i>was: {prior_squeeze}/100</i>\n"
                f"<b>{squeeze_band}</b> — {interp[:200]}\n\n"
                f"Reasons:\n" + "\n".join(f"• {r}" for r in squeeze_reasons[:4])
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "margin_debt_pct_of_cap": md_pct_of_cap,
            "margin_status": md_status,
            "squeeze_score": squeeze_score,
            "squeeze_band": squeeze_band,
        }),
    }
