"""
justhodl-crisis-canaries v3.0 — Funding-Plumbing Early Warning
==============================================================
Items 1/2/4/5: crisis starts in collateral and bank funding, weeks before equities.

  C1 SOFR tail        — NY Fed official API: 99th-pct − volume-wtd median, z (daily)
  C2 Repo volumes     — OFR STFM API probe (segment volumes)
  C3 Discount window  — H.4.1 primary credit (FRED weekly), level z + WoW jump
  C4 Bank deposits    — H.8 small domestically chartered, WoW outflow z
  C5 Auction slope    — 3-obs slope of platform auction-crisis composite
                        (self-bootstrapping history at data/_canaries/history.json)
  C6 Revision nowcast — ALFRED initial-release vs latest PAYEMS revisions, 6m slope z

Composite 0-100 over AVAILABLE canaries (coverage-honest). Score ≥70 logs a
crisis_canary DOWN signal to the closed loop vs SPY.
"""
import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crisis-canaries.json"
HIST_KEY = "data/_canaries/history.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
UA = {"User-Agent": "JustHodl Research admin@justhodl.ai"}
VERSION = "3.0.0"


def hj(url, timeout=30):
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout).read())
    except Exception as e:
        print(f"[http] {url[:70]}: {str(e)[:60]}")
        return None


def fred(sid, start="2018-01-01", extra=""):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY, "file_type": "json",
                                   "observation_start": start, "limit": 100000}) + extra)
    j = hj(u, 40)
    if not j:
        return []
    return [(o["date"], float(o["value"])) for o in j.get("observations", [])
            if o.get("value") not in (".", "", None)]


def zlast(vals, look=252):
    if len(vals) < 20:
        return None
    w = vals[-look:]
    m, sd = mean(w), (stdev(w) if len(w) > 1 else 0)
    return round((vals[-1] - m) / sd, 2) if sd else 0.0



def poly_closes(t, days=1500):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    for back in (0, 3, 9):
        if back:
            time.sleep(back)
        try:
            j = hj(u, timeout=45)
            rows = [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(),
                      float(r["c"])) for r in (j.get("results") or [])]
            if rows:
                return rows
        except Exception:
            pass
    return []


def fred_ladder(sids, start="2015-01-01"):
    for sid in sids:
        try:
            o = fred(sid, start)
            if len(o) > 8:
                return sid, o
        except Exception:
            continue
    return None, []


def pct(a, b):
    return round((a / b - 1) * 100, 2) if (a is not None and b) else None


def _mk(name, family, value, unit, signal, detail, as_of, z=None, lead=None, src=None):
    return {"name": name, "family": family, "value": value, "unit": unit,
             "signal": signal,
             "status": {1: "RED", 0: "AMBER", -1: "GREEN"}.get(signal, "GREEN"),
             "detail": detail, "as_of": as_of, "z": z, "lead": lead, "source": src}


def global_canaries(canaries, avail, alerts):
    """v3.0 — 31 early-warning canaries across 7 families."""
    G = {}

    def put(key, fn):
        try:
            r = fn()
            if r:
                cut = (datetime.now(timezone.utc) - timedelta(days=400)).date().isoformat()
                if str(r.get("as_of", ""))[:10] < cut:
                    avail[key] = "stale"
                    print(f"[g:{key}] STALE {r.get('as_of')} — excluded")
                    return
                G[key] = r; avail[key] = True
                if r["signal"] == 1:
                    alerts.append(f"{r['name']}: {r['detail'][:120]}")
            else:
                avail[key] = False
        except Exception as e:
            avail[key] = False
            print(f"[g:{key}] {str(e)[:60]}")

    # LABOR
    def f_sahm():
        sid, o = fred_ladder(["SAHMREALTIME", "SAHMCURRENT"], "2019-01-01")
        if not o: return None
        v = o[-1][1]
        sig = 1 if v >= 0.50 else 0 if v >= 0.30 else -1
        return _mk("Sahm Rule (real-time)", "labor", round(v, 2), "pp vs 12m low", sig,
                    f"{v:.2f} (trigger 0.50; called every recession since 1970)", o[-1][0],
                    lead="0-2m", src=sid)
    put("sahm_rule", f_sahm)

    def f_claims():
        o = fred("ICSA", "2015-01-01")
        if len(o) < 60: return None
        vals = [v for _, v in o]
        a4 = [sum(vals[i-4:i]) / 4 for i in range(4, len(vals) + 1)]
        yoy = pct(a4[-1], a4[-53]) if len(a4) > 53 else None
        z = zlast(a4, 156)
        sig = 1 if (yoy or 0) >= 15 and (z or 0) >= 1.2 else 0 if (yoy or 0) >= 8 else -1
        return _mk("Initial claims (4-wk avg)", "labor", round(a4[-1] / 1000, 0), "k", sig,
                    f"4wk {a4[-1]/1000:.0f}k, YoY {(yoy or 0):+.1f}%", o[-1][0], z=z,
                    lead="1-3m", src="ICSA")
    put("claims_4wk", f_claims)

    def f_continuing():
        o = fred("CCSA", "2015-01-01")
        if len(o) < 60: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-53]) if len(vals) > 53 else None
        sig = 1 if (yoy or 0) >= 12 else 0 if (yoy or 0) >= 5 else -1
        return _mk("Continuing claims", "labor", round(vals[-1] / 1e6, 2), "mn", sig,
                    f"YoY {(yoy or 0):+.1f}% — the hiring-freeze read", o[-1][0],
                    lead="1-3m", src="CCSA")
    put("continuing_claims", f_continuing)

    def f_quits():
        o = fred("JTSQUR", "2015-01-01")
        if len(o) < 26: return None
        vals = [v for _, v in o]
        yoy = round(vals[-1] - vals[-13], 2) if len(vals) > 13 else None
        sig = 1 if vals[-1] <= 1.9 and (yoy or 0) < 0 else 0 if (yoy or 0) <= -0.2 else -1
        return _mk("Quits rate (JOLTS)", "labor", vals[-1], "%", sig,
                    f"{vals[-1]}% ({(yoy or 0):+.2f}pp YoY) — workers stop quitting before layoffs",
                    o[-1][0], lead="3-6m", src="JTSQUR")
    put("quits_rate", f_quits)

    def f_overtime():
        o = fred("AWOTMAN", "2015-01-01")
        if len(o) < 26: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-13])
        sig = 1 if (yoy or 0) <= -8 else 0 if (yoy or 0) <= -3 else -1
        return _mk("Mfg overtime hours", "labor", vals[-1], "hrs", sig,
                    f"YoY {(yoy or 0):+.1f}% — overtime cut before headcount", o[-1][0],
                    lead="2-4m", src="AWOTMAN")
    put("overtime_mfg", f_overtime)

    # REAL ECONOMY
    def f_trucks():
        o = fred("HTRUCKSSAAR", "2014-01-01")
        if len(o) < 26: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-13])
        sig = 1 if (yoy or 0) <= -12 else 0 if (yoy or 0) <= -5 else -1
        return _mk("Heavy truck sales", "real_economy", round(vals[-1] * 1000, 0), "k units SAAR", sig,
                    f"YoY {(yoy or 0):+.1f}% — legendary capex/freight lead", o[-1][0],
                    lead="6-12m", src="HTRUCKSSAAR")
    put("heavy_trucks", f_trucks)

    def f_permits():
        o = fred("PERMIT", "2014-01-01")
        if len(o) < 26: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-13])
        sig = 1 if (yoy or 0) <= -12 else 0 if (yoy or 0) <= -5 else -1
        return _mk("Building permits", "real_economy", vals[-1], "k SAAR", sig,
                    f"YoY {(yoy or 0):+.1f}% — housing leads the cycle", o[-1][0],
                    lead="6-12m", src="PERMIT")
    put("building_permits", f_permits)

    def f_tonnage():
        sid, o = fred_ladder(["TRUCKD11"], "2014-01-01")
        if not o: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-13]) if len(vals) > 13 else None
        sig = 1 if (yoy or 0) <= -4 else 0 if (yoy or 0) <= -1 else -1
        return _mk("Truck tonnage", "real_economy", round(vals[-1], 1), "idx", sig,
                    f"YoY {(yoy or 0):+.1f}% — goods-economy pulse", o[-1][0],
                    lead="2-4m", src=sid)
    put("truck_tonnage", f_tonnage)

    def f_rail():
        sid, o = fred_ladder(["RAILFRTCARLOADSD11", "RAILFRTINTERMODALD11",
                                "FRGSHPUSM649NCIS"], "2014-01-01")
        if not o: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-13]) if len(vals) > 13 else None
        sig = 1 if (yoy or 0) <= -6 else 0 if (yoy or 0) <= -2 else -1
        return _mk("Rail/freight volumes", "real_economy", round(vals[-1] / 1000, 0), "k carloads", sig,
                    f"YoY {(yoy or 0):+.1f}%", o[-1][0], lead="1-3m", src=sid)
    put("rail_freight", f_rail)

    def f_wei():
        o = fred("WEI", "2019-01-01")
        if len(o) < 30: return None
        vals = [v for _, v in o]
        d13 = round(vals[-1] - vals[-14], 2) if len(vals) > 14 else None
        sig = 1 if vals[-1] < 0.5 else 0 if vals[-1] < 1.5 or (d13 or 0) <= -1 else -1
        return _mk("Weekly Economic Index (NY Fed)", "real_economy", round(vals[-1], 2),
                    "GDP-scaled", sig,
                    f"{vals[-1]:.2f} ({(d13 or 0):+.2f} vs 13wk) — real-time activity",
                    o[-1][0], lead="0-1m", src="WEI")
    put("wei", f_wei)

    # CREDIT QUALITY
    def f_ccc_bb():
        c = dict(fred("BAMLH0A3HYC", "2018-01-01"))
        b = dict(fred("BAMLH0A1HYBB", "2018-01-01"))
        ks = sorted(set(c) & set(b))
        if len(ks) < 60: return None
        sp = [(c[k] - b[k]) * 100 for k in ks]
        z = zlast(sp, 504)
        ch3m = round(sp[-1] - sp[-64], 0) if len(sp) > 64 else None
        sig = 1 if (z or 0) >= 1.5 or (ch3m or 0) >= 150 else 0 if (z or 0) >= 0.7 else -1
        return _mk("CCC−BB spread (stealth credit)", "credit", round(sp[-1], 0), "bp", sig,
                    f"{sp[-1]:.0f}bp ({(ch3m or 0):+.0f} 3m) — junk's junk cracks first",
                    ks[-1], z=z, lead="2-6m", src="BAMLH0A3HYC−BAMLH0A1HYBB")
    put("ccc_vs_bb", f_ccc_bb)

    def f_ci():
        o = fred("BUSLOANS", "2014-01-01")
        if len(o) < 26: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-13])
        sig = 1 if (yoy or 0) < 0 else 0 if (yoy or 0) < 2 else -1
        return _mk("C&I loan growth", "credit", yoy, "% YoY", sig,
                    f"{(yoy or 0):+.1f}% YoY — contraction = crunch", o[-1][0],
                    lead="0-3m", src="BUSLOANS")
    put("ci_loans", f_ci)

    def f_bankcred():
        o = fred("TOTBKCR", "2014-01-01")
        if len(o) < 60: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-53]) if len(vals) > 53 else None
        sig = 1 if (yoy or 0) < 0.5 else 0 if (yoy or 0) < 2.5 else -1
        return _mk("Total bank credit", "credit", yoy, "% YoY", sig,
                    f"{(yoy or 0):+.1f}% YoY (weekly H.8)", o[-1][0],
                    lead="0-3m", src="TOTBKCR")
    put("bank_credit", f_bankcred)

    def f_sloos():
        o = fred("DRTSCILM", "2014-01-01")
        if len(o) < 8: return None
        v = o[-1][1]
        sig = 1 if v >= 20 else 0 if v >= 8 else -1
        return _mk("SLOOS net tightening (C&I)", "credit", v, "% net", sig,
                    f"{v:+.1f}% net tightening (quarterly; ≥20 preceded every modern recession)",
                    o[-1][0], lead="3-9m", src="DRTSCILM")
    put("sloos", f_sloos)

    def f_ccdelinq():
        o = fred("DRCCLACBS", "2012-01-01")
        if len(o) < 10: return None
        vals = [v for _, v in o]
        ch4q = round(vals[-1] - vals[-5], 2) if len(vals) > 5 else None
        sig = 1 if (ch4q or 0) >= 0.6 else 0 if (ch4q or 0) >= 0.25 else -1
        return _mk("Credit-card delinquency", "credit", vals[-1], "%", sig,
                    f"{vals[-1]}% ({(ch4q or 0):+.2f}pp YoY) — consumer stress build",
                    o[-1][0], lead="3-6m", src="DRCCLACBS")
    put("cc_delinquency", f_ccdelinq)

    # RATES REGIME
    def f_uninvert():
        o = fred("T10Y2Y", "2021-06-01")
        if len(o) < 200: return None
        vals = [v for _, v in o]
        deep = min(vals[-504:]) if len(vals) >= 504 else min(vals)
        now = vals[-1]
        ch3m = round(now - vals[-64], 2) if len(vals) > 64 else None
        was_deep = deep <= -0.40
        sig = 1 if (was_deep and now >= -0.10 and (ch3m or 0) >= 0.35) else               0 if (was_deep and (ch3m or 0) >= 0.20) else -1
        return _mk("Un-inversion trigger (2s10s)", "rates_regime", now, "%", sig,
                    f"2s10s {now:+.2f}% ({(ch3m or 0):+.2f} 3m; trough {deep:+.2f}). Recessions "
                    f"start when the curve RE-steepens, not when it inverts.", o[-1][0],
                    lead="0-3m", src="T10Y2Y")
    put("uninversion_trigger", f_uninvert)

    def f_breadth():
        ids = ["DGS3MO", "DGS2", "DGS5", "DGS10", "DGS30"]
        d = {}
        for s in ids:
            o = fred(s, "2024-09-01")
            if o:
                d[s] = o[-1][1]
        if len(d) < 5: return None
        pairs = [("DGS3MO", "DGS10"), ("DGS3MO", "DGS30"), ("DGS2", "DGS10"),
                  ("DGS2", "DGS30"), ("DGS2", "DGS5"), ("DGS5", "DGS30")]
        inv = sum(1 for a, b in pairs if d[a] > d[b])
        pv = round(inv / len(pairs) * 100, 0)
        sig = 1 if pv >= 67 else 0 if pv >= 34 else -1
        return _mk("Curve inversion breadth", "rates_regime", pv, "% pairs inverted", sig,
                    f"{inv}/{len(pairs)} maturity pairs inverted",
                    datetime.now(timezone.utc).date().isoformat(),
                    lead="6-18m", src="DGS*")
    put("inversion_breadth", f_breadth)

    def f_breakevens():
        o = fred("T5YIE", "2022-01-01")
        if len(o) < 80: return None
        vals = [v for _, v in o]
        ch3m = round((vals[-1] - vals[-64]) * 100, 0)
        sig = 1 if ch3m <= -40 else 0 if ch3m <= -20 else -1
        return _mk("5y breakevens (growth scare)", "rates_regime", vals[-1], "%", sig,
                    f"{vals[-1]:.2f}% ({ch3m:+.0f}bp 3m) — fast BE collapse = deflation impulse",
                    o[-1][0], lead="0-2m", src="T5YIE")
    put("breakevens", f_breakevens)

    def f_2y():
        o = fred("DGS2", "2022-01-01")
        if len(o) < 80: return None
        vals = [v for _, v in o]
        ch3m = round((vals[-1] - vals[-64]) * 100, 0)
        sig = 1 if ch3m <= -60 else 0 if ch3m <= -30 else -1
        return _mk("2y yield collapse", "rates_regime", vals[-1], "%", sig,
                    f"{vals[-1]:.2f}% ({ch3m:+.0f}bp 3m) — front-end smells breakage first",
                    o[-1][0], lead="0-2m", src="DGS2")
    put("twoyr_collapse", f_2y)

    # GLOBAL / FX / EM
    def f_cu_au():
        cu = dict(poly_closes("CPER"))
        au = dict(poly_closes("GLD"))
        ks = sorted(set(cu) & set(au))
        if len(ks) < 300: return None
        r = [cu[k] / au[k] for k in ks]
        z = zlast(r, 756)
        ch6m = pct(r[-1], r[-127]) if len(r) > 127 else None
        cu3 = pct(cu[ks[-1]], cu[ks[-64]])
        au3 = pct(au[ks[-1]], au[ks[-64]])
        driver = ("copper-driven (STAGE-2: real demand cracking)" if (cu3 or 0) <= -5
                   else "gold-driven (stage-1: fear bid, demand intact)" if (au3 or 0) >= 5
                   else "mixed")
        sig = 1 if ((z or 0) <= -1.3 and (cu3 or 0) <= -5) else 0 if (z or 0) <= -0.8 else -1
        return _mk("Copper/Gold ratio", "global_fx", round(r[-1], 4), "CPER/GLD", sig,
                    f"z {z} · 6m {(ch6m or 0):+.1f}% · Cu3m {(cu3 or 0):+.1f}% vs Au3m "
                    f"{(au3 or 0):+.1f}% → {driver}. 2026 regime: ratio at ~175-year lows.",
                    ks[-1], z=z, lead="2-6m", src="CPER/GLD (Polygon)")
    put("copper_gold", f_cu_au)

    def f_audjpy():
        o = poly_closes("C:AUDJPY", 900)
        if len(o) < 120: return None
        vals = [v for _, v in o]
        ch3m = pct(vals[-1], vals[-64])
        sig = 1 if (ch3m or 0) <= -6 else 0 if (ch3m or 0) <= -3 else -1
        return _mk("AUD/JPY (risk FX)", "global_fx", round(vals[-1], 2), "", sig,
                    f"3m {(ch3m or 0):+.1f}% — THE carry/risk barometer; fast falls precede "
                    f"equity stress", o[-1][0], lead="0-2m", src="C:AUDJPY")
    put("aud_jpy", f_audjpy)

    def f_clp():
        o = poly_closes("C:USDCLP", 900)
        if len(o) < 120: return None
        vals = [v for _, v in o]
        ch3m = pct(vals[-1], vals[-64])
        sig = 1 if (ch3m or 0) >= 8 else 0 if (ch3m or 0) >= 4 else -1
        return _mk("USD/CLP (copper currency)", "global_fx", round(vals[-1], 1), "", sig,
                    f"3m {(ch3m or 0):+.1f}% — the Chilean peso IS the copper economy; CLP "
                    f"breakdowns lead industrial downturns", o[-1][0], lead="1-4m",
                    src="C:USDCLP")
    put("usd_clp", f_clp)

    def f_emfx():
        o = fred("DTWEXEMEGS", "2019-01-01")
        if len(o) < 120: return None
        vals = [v for _, v in o]
        ch3m = pct(vals[-1], vals[-64])
        sig = 1 if (ch3m or 0) >= 5 else 0 if (ch3m or 0) >= 2.5 else -1
        return _mk("USD vs EM (broad)", "global_fx", round(vals[-1], 1), "idx", sig,
                    f"3m {(ch3m or 0):+.1f}% — EM FX stress = dollar-shortage transmission",
                    o[-1][0], lead="1-3m", src="DTWEXEMEGS")
    put("em_fx_stress", f_emfx)

    def f_chile_ip():
        sid, o = fred_ladder(["CHLPROINDMISMEI", "PRINTO01CLQ661S",
                                "CHLPRMNTO01IXOBM"], "2015-01-01")
        if not o: return None
        vals = [v for _, v in o]
        yoy = pct(vals[-1], vals[-13]) if len(vals) > 13 else None
        sig = 1 if (yoy or 0) <= -3 else 0 if (yoy or 0) <= 0 else -1
        return _mk("Chile industrial production", "global_fx", round(vals[-1], 1), "idx", sig,
                    f"YoY {(yoy or 0):+.1f}% — copper-economy real output", o[-1][0],
                    lead="2-5m", src=sid)
    put("chile_ip", f_chile_ip)

    # MARKET INTERNALS
    def ratio_canary(num, den, name, red, amber, invert, extra, lead="1-3m"):
        a = dict(poly_closes(num, 900))
        b = dict(poly_closes(den, 900))
        ks = sorted(set(a) & set(b))
        if len(ks) < 120: return None
        r = [a[k] / b[k] for k in ks]
        ch3m = pct(r[-1], r[-64])
        x = -(ch3m or 0) if invert else (ch3m or 0)
        sig = 1 if x >= red else 0 if x >= amber else -1
        return _mk(name, "internals", round(r[-1], 4), f"{num}/{den}", sig,
                    f"3m {(ch3m or 0):+.1f}% — {extra}", ks[-1], lead=lead,
                    src=f"{num}/{den} (Polygon)")
    put("transports_rel", lambda: ratio_canary("IYT", "SPY", "Transports vs SPY", 8, 4,
        True, "Dow-theory non-confirmation when falling"))
    put("defensives_bid", lambda: ratio_canary("XLP", "SPY", "Defensives bid (XLP/SPY)",
        5, 2.5, False, "staples outperforming = de-risking under the hood"))
    put("regional_banks_rel", lambda: ratio_canary("KRE", "SPY", "Regional banks vs SPY",
        10, 5, True, "the SVB-class canary"))
    put("korea_beta", lambda: ratio_canary("EWY", "SPY", "Korea (EWY) vs SPY", 10, 5,
        True, "global-trade beta, priced daily"))
    put("small_large_breadth", lambda: ratio_canary("IWM", "SPY", "Russell 2000 vs S&P 500", 8, 4,
        True, "small-caps lagging large-caps = narrowing breadth, risk appetite fading under the surface"))

    # PLUMBING EXTRAS
    def f_swaps():
        sid, o = fred_ladder(["SWPT", "WLCFLSCBLS"], "2019-01-01")
        if not o: return None
        v = o[-1][1] / 1000.0   # FRED SWPT reported in $millions
        sig = 1 if v >= 5 else 0 if v >= 0.5 else -1
        return _mk("Fed FX swap lines", "plumbing_extra", round(v, 2), "$bn", sig,
                    f"${v:.2f}bn outstanding — ANY real usage (>$0.5bn) = global dollar shortage",
                    o[-1][0], lead="0m (acute)", src=sid)
    put("swap_lines", f_swaps)

    def f_foreign_repo():
        sid, o = fred_ladder(["WLRRAFOIAL", "WREPOFOR"], "2019-01-01")
        if not o: return None
        vals = [v for _, v in o]
        ch13 = round(vals[-1] - vals[-14], 0) if len(vals) > 14 else None
        d13 = [vals[i] - vals[i - 13] for i in range(13, len(vals))]
        z = zlast(d13, 156) if d13 else None
        sig = 1 if (z or 0) >= 2 else 0 if (z or 0) >= 1.2 else -1
        return _mk("Foreign repo pool (Fed)", "plumbing_extra", round(vals[-1] / 1000, 0), "$bn",
                    sig, f"${vals[-1]/1000:.0f}bn ({(ch13 or 0)/1000:+.0f}bn 13wk) — foreign officials "
                    f"hoarding dollars", o[-1][0], z=z, lead="0-2m", src=sid)
    put("foreign_repo_pool", f_foreign_repo)

    def f_realm2():
        md = dict(fred("M2SL", "2014-01-01"))
        cd = dict(fred("CPIAUCSL", "2014-01-01"))
        ks = sorted(set(md) & set(cd))
        if len(ks) < 26: return None
        real = [md[k] / cd[k] for k in ks]
        yoy = pct(real[-1], real[-13])
        sig = 1 if (yoy or 0) < -1 else 0 if (yoy or 0) < 0.5 else -1
        return _mk("Real M2 growth", "plumbing_extra", yoy, "% YoY", sig,
                    f"{(yoy or 0):+.1f}% YoY — real money contraction starves risk assets",
                    ks[-1], lead="6-12m", src="M2SL/CPIAUCSL")
    put("real_m2", f_realm2)

    return G


def lambda_handler(event=None, context=None):
    t0 = time.time()
    avail, canaries, alerts = {}, {}, []

    # ── C1: SOFR distribution tail (NY Fed) ──
    j = hj("https://markets.newyorkfed.org/api/rates/secured/sofr/last/120.json")
    rows = (j or {}).get("refRates") or []
    avail["sofr_tail"] = len(rows) > 30
    if avail["sofr_tail"]:
        rows = sorted(rows, key=lambda r: r.get("effectiveDate", ""))
        tails = [(r["effectiveDate"], float(r["percentPercentile99"]) - float(r["percentRate"]))
                 for r in rows if r.get("percentPercentile99") is not None and r.get("percentRate") is not None]
        vals = [t for _, t in tails]
        canaries["sofr_tail"] = {"as_of": tails[-1][0], "tail_bp": round(vals[-1] * 100, 1),
                                 "z": zlast(vals, 120),
                                 "vol_bn": rows[-1].get("volumeInBillions")}
        if (canaries["sofr_tail"]["z"] or 0) >= 2:
            alerts.append(f"SOFR 99th-pct tail {canaries['sofr_tail']['tail_bp']}bp (z {canaries['sofr_tail']['z']}) — collateral stress")

    # ── C2: OFR repo volumes (probe) ──
    ofr = None
    for mn in ("REPO-TRI_TV_TOT-P", "REPO-DVP_TV_TOT-P", "FNYR-BGCR-A"):
        o = hj(f"https://data.financialresearch.gov/v1/series/full?mnemonic={mn}", 30)
        ts = (((o or {}).get(mn) or {}).get("timeseries") or {}).get("aggregation") \
            if isinstance(o, dict) else None
        if isinstance(ts, list) and len(ts) > 30:
            vals = [float(v) for _, v in ts[-500:] if v is not None]
            ofr = {"mnemonic": mn, "as_of": ts[-1][0], "level": vals[-1], "z": zlast(vals)}
            break
    avail["ofr_repo"] = bool(ofr)
    if ofr:
        canaries["ofr_repo"] = ofr

    # ── C3: discount window (H.4.1) ──
    dw = None
    for sid in ("WLCFLPCL", "WLCFLL", "TOTBORR"):
        s = fred(sid, "2019-01-01")
        if len(s) > 30:
            vals = [v for _, v in s]
            wow = vals[-1] - vals[-2]
            dw = {"series": sid, "as_of": s[-1][0], "level_mn": round(vals[-1], 0),
                  "wow_chg": round(wow, 0), "z": zlast(vals, 156),
                  "wow_z": zlast([vals[i] - vals[i - 1] for i in range(1, len(vals))], 156)}
            break
    avail["discount_window"] = bool(dw)
    if dw:
        canaries["discount_window"] = dw
        if (dw.get("wow_z") or 0) >= 2.5:
            alerts.append(f"Discount-window borrowings jumped (WoW z {dw['wow_z']}) — pre-SVB pattern")

    # ── C4: small-bank deposits (H.8) ──
    dep = None
    for sid in ("DPSSCBW027SBOG", "DPSACBW027SBOG"):
        s = fred(sid, "2019-01-01")
        if len(s) > 30:
            vals = [v for _, v in s]
            d1 = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
            dep = {"series": sid, "as_of": s[-1][0], "level_bn": round(vals[-1], 1),
                   "wow_chg_bn": round(d1[-1], 1), "outflow_z": zlast(d1, 156)}
            break
    avail["bank_deposits"] = bool(dep)
    if dep:
        canaries["bank_deposits"] = dep
        if (dep.get("outflow_z") or 0) <= -2.5:
            alerts.append(f"Small-bank deposit outflow z {dep['outflow_z']} — funding flight")

    # FHLB discount-note issuance: no free machine-readable feed — explicit gap.
    avail["fhlb_dn"] = False

    # ── C5: auction-crisis slope (self-bootstrapping history) ──
    auc = None
    try:
        a = json.loads(S3.get_object(Bucket=BUCKET, Key="data/auction-crisis.json")["Body"].read())
        comp = a.get("composite_score") or a.get("score") or a.get("composite")
        if comp is not None:
            auc = float(comp)
    except Exception:
        pass
    hist = {}
    try:
        hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        hist = {"rows": []}
    today = datetime.now(timezone.utc).date().isoformat()
    row = {"date": today,
           "auction": auc,
           "sofr_tail_bp": (canaries.get("sofr_tail") or {}).get("tail_bp"),
           "dw_level": (canaries.get("discount_window") or {}).get("level_mn"),
           "dep_wow": (canaries.get("bank_deposits") or {}).get("wow_chg_bn")}
    if not hist["rows"] or hist["rows"][-1]["date"] != today:
        hist["rows"] = (hist["rows"] + [row])[-260:]
        S3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist).encode(),
                      ContentType="application/json")
    aser = [r["auction"] for r in hist["rows"] if r.get("auction") is not None][-5:]
    avail["auction_slope"] = len(aser) >= 3
    if avail["auction_slope"]:
        sl = (aser[-1] - aser[0]) / (len(aser) - 1)
        canaries["auction_slope"] = {"composite_now": aser[-1], "slope_per_obs": round(sl, 2),
                                     "n_obs": len(aser),
                                     "deteriorating": sl > 3}
        if sl > 5:
            alerts.append(f"Auction composite deteriorating {sl:+.1f}/obs over {len(aser)} obs")
    else:
        canaries["auction_slope"] = {"status": f"warming up ({len(aser)}/3 obs)", "composite_now": auc}

    # ── C6: ALFRED revision nowcast (PAYEMS initial vs latest) ──
    rev = None
    try:
        start = (datetime.now(timezone.utc) - timedelta(days=420)).date().isoformat()
        init = dict(fred("PAYEMS", start, "&output_type=4&realtime_start=2015-01-01&realtime_end=9999-12-31"))
        latest = dict(fred("PAYEMS", start))
        common = sorted(set(init) & set(latest))[:-1]
        revs = [(d, latest[d] - init[d]) for d in common][-8:]
        if len(revs) >= 4:
            rv = [r for _, r in revs]
            rev = {"last_obs": revs[-1][0], "revisions_k": [[d, round(r, 0)] for d, r in revs],
                   "mean_rev_k": round(mean(rv), 1),
                   "slope_k_per_m": round((rv[-1] - rv[0]) / (len(rv) - 1), 1),
                   "all_negative_last4": all(r < 0 for r in rv[-4:])}
            if rev["all_negative_last4"]:
                alerts.append("Payroll revisions systematically NEGATIVE 4 straight months — pre-recession signature")
    except Exception as e:
        print(f"[alfred] {str(e)[:70]}")
    avail["revision_nowcast"] = bool(rev)
    if rev:
        canaries["revision_nowcast"] = rev


    # ── BRAIN-GAP CANARIES (v2.0, from Khalid's brain audit ops-1580) ──
    # C7: MOVE-proxy — brain 74×: "key thing to watch if the Fed is gonna intervene
    #     is the MOVE index and overall treasury liquidity conditions". No free MOVE
    #     feed → honest proxy: 20d realized vol of the 10y yield, 3y z.
    try:
        g10 = fred("DGS10", "2018-01-01")
        if len(g10) > 60:
            ch = [g10[i][1] - g10[i - 1][1] for i in range(1, len(g10))]
            rv = []
            for i in range(20, len(ch)):
                w = ch[i - 20:i]
                m_ = mean(w)
                rv.append((sum((x - m_) ** 2 for x in w) / 20) ** 0.5 * 15.87)  # ~annualized bp
            canaries["treasury_vol_proxy"] = {
                "rv20_bp_ann": round(rv[-1] * 100, 1), "z": zlast(rv, 756),
                "as_of": g10[-1][0],
                "note": "MOVE-proxy: 20d realized vol of DGS10 (no free MOVE feed)"}
            avail["treasury_vol_proxy"] = True
            if (canaries["treasury_vol_proxy"]["z"] or 0) >= 2:
                alerts.append(f"Treasury vol proxy z {canaries['treasury_vol_proxy']['z']} — "
                              "Fed-intervention watch (brain: MOVE spike precedes backstops)")
    except Exception as e:
        avail["treasury_vol_proxy"] = False; print(f"[c7] {str(e)[:50]}")

    # C8: CP−bill spread — brain 37×: the 2008-style wholesale-funding canary.
    try:
        cp = dict(fred("DCPF3M", "2019-01-01") or fred("CPF3M", "2019-01-01"))
        tb = dict(fred("DTB3", "2019-01-01"))
        sprd = [(d_, (cp[d_] - tb[d_]) * 100) for d_ in sorted(set(cp) & set(tb))]
        if len(sprd) > 60:
            vals = [v for _, v in sprd]
            canaries["cp_bill_spread"] = {"spread_bp": round(vals[-1], 1),
                                           "z": zlast(vals, 504), "as_of": sprd[-1][0]}
            avail["cp_bill_spread"] = True
            if (canaries["cp_bill_spread"]["z"] or 0) >= 2.5:
                alerts.append(f"CP−bill spread {vals[-1]:.0f}bp (z) — wholesale funding stress")
    except Exception as e:
        avail["cp_bill_spread"] = False; print(f"[c8] {str(e)[:50]}")

    # C9: MMF assets — brain 56×: flight-to-cash / wholesale lenders' war chest.
    try:
        mm = None
        cutoff = (datetime.now(timezone.utc) - timedelta(days=200)).date().isoformat()
        for sid in ("MMMFFAQ027S", "WRMFSL", "WIMFSL"):
            o = fred(sid, "2019-01-01")
            if len(o) > 12 and o[-1][0] >= cutoff:   # reject discontinued series
                mm = (sid, o); break
        if mm:
            sid, o = mm
            vals = [v for _, v in o]
            d4 = [vals[i] - vals[i - 4] for i in range(4, len(vals))]
            canaries["mmf_assets"] = {"series": sid, "level_bn": round(vals[-1], 0),
                                       "chg_4obs": round(d4[-1], 0),
                                       "surge_z": zlast(d4, 156), "as_of": o[-1][0]}
            avail["mmf_assets"] = True
            if (canaries["mmf_assets"]["surge_z"] or 0) >= 2.5:
                alerts.append("MMF asset surge — flight to cash underway")
    except Exception as e:
        avail["mmf_assets"] = False; print(f"[c9] {str(e)[:50]}")

    # C10: Floor spreads — SOFR−IORB & EFFR−SOFR (free slice of the brain's
    #      67×-mentioned dollar-shortage / xccy-basis complex).
    try:
        sofr = dict(fred("SOFR", "2021-01-01")); iorb = dict(fred("IORB", "2021-01-01"))
        effr = dict(fred("EFFR", "2021-01-01"))
        si = [(d_, (sofr[d_] - iorb[d_]) * 100) for d_ in sorted(set(sofr) & set(iorb))]
        es = [(d_, (effr[d_] - sofr[d_]) * 100) for d_ in sorted(set(effr) & set(sofr))]
        if si:
            v1 = [v for _, v in si]; v2 = [v for _, v in es]
            canaries["floor_spreads"] = {"sofr_iorb_bp": round(v1[-1], 1),
                                          "sofr_iorb_z": zlast(v1, 504),
                                          "effr_sofr_bp": round(v2[-1], 1) if v2 else None,
                                          "as_of": si[-1][0]}
            avail["floor_spreads"] = True
            if v1[-1] >= 5:
                alerts.append(f"SOFR−IORB +{v1[-1]:.0f}bp — repo pressing through the floor "
                              "(Sept-2019 signature)")
    except Exception as e:
        avail["floor_spreads"] = False; print(f"[c10] {str(e)[:50]}")

    # C11: Bank reserves — brain 86×: "great barometer for liquidity".
    try:
        wr = fred("WRESBAL", "2018-01-01")
        if len(wr) > 30:
            vals = [v for _, v in wr]
            d13 = [vals[i] - vals[i - 13] for i in range(13, len(vals))]
            canaries["bank_reserves"] = {"level_bn": round(vals[-1], 0),
                                          "chg_13w_bn": round(d13[-1], 0),
                                          "drain_z": zlast(d13, 260), "as_of": wr[-1][0]}
            avail["bank_reserves"] = True
            if (canaries["bank_reserves"]["drain_z"] or 0) <= -2:
                alerts.append(f"Bank reserves draining (13w z {canaries['bank_reserves']['drain_z']}) "
                              "— the liquidity barometer is falling")
    except Exception as e:
        avail["bank_reserves"] = False; print(f"[c11] {str(e)[:50]}")

    # C12: Primary-dealer UST fails — brain 26×: collateral-scarcity tell.
    #      NY Fed PD API, series discovered at runtime (probe-tolerant).
    try:
        ts = hj("https://markets.newyorkfed.org/api/pd/list/timeseries.json", 30)
        rows = (ts or {}).get("pd", {}).get("timeseries", []) if isinstance(ts, dict) else []
        cand = [r.get("keyid") for r in rows
                if "fail" in str(r.get("description", "")).lower()
                and "deliver" in str(r.get("description", "")).lower()
                and "treasur" in str(r.get("description", "")).lower()][:2]
        pdser = None
        for kid in cand:
            j2 = hj(f"https://markets.newyorkfed.org/api/pd/get/{kid}.json", 30)
            obs = (j2 or {}).get("pd", {}).get("timeseries", [])
            pts = [(o_.get("asofdate"), float(o_.get("value")))
                   for o_ in obs if o_.get("value") not in (None, "", "*")]
            if len(pts) > 30:
                pdser = (kid, sorted(pts)); break
        if pdser:
            kid, pts = pdser
            vals = [v for _, v in pts]
            canaries["pd_fails"] = {"series": kid, "level_mn": round(vals[-1], 0),
                                     "z": zlast(vals, 156), "as_of": pts[-1][0]}
            avail["pd_fails"] = True
            if (canaries["pd_fails"]["z"] or 0) >= 2.5:
                alerts.append("Primary-dealer UST fails spiking — collateral scarcity")
    except Exception as e:
        avail["pd_fails"] = False; print(f"[c12] {str(e)[:50]}")

    # ── v3.0 GLOBAL CANARIES (31 metrics, 7 families) ──
    gcs = global_canaries(canaries, avail, alerts)
    canaries.update(gcs)
    fam_scores = {}
    for fam in ('labor', 'real_economy', 'credit', 'rates_regime', 'global_fx',
                 'internals', 'plumbing_extra'):
        sigs = [v['signal'] for v in gcs.values()
                 if isinstance(v, dict) and v.get('family') == fam]
        if sigs:
            fam_scores[fam] = {'n': len(sigs), 'red': sigs.count(1),
                                'amber': sigs.count(0), 'green': sigs.count(-1),
                                'score': round(sum((s + 1) * 50 for s in sigs) / len(sigs), 0)}
    red_total = sum(f['red'] for f in fam_scores.values())
    n_total = sum(f['n'] for f in fam_scores.values())
    global_score = (round(sum(f['score'] for f in fam_scores.values())
                           / len(fam_scores), 1) if fam_scores else None)
    grid_score = None
    try:
        gj_ = json.loads(S3.get_object(Bucket=BUCKET, Key='data/canary-grid.json')['Body'].read())
        for k_ in ('early_warning_level', 'composite_score', 'early_warning_score', 'composite', 'score'):
            v_ = gj_.get(k_)
            if isinstance(v_, (int, float)):
                grid_score = round(float(v_), 1); break
            if isinstance(v_, dict):
                vv = v_.get('score')
                if isinstance(vv, (int, float)):
                    grid_score = round(float(vv), 1); break
    except Exception as e_:
        print(f'[grid-ingest] {str(e_)[:50]}')

    # ── composite (coverage-honest) ──
    parts = []
    st = (canaries.get("sofr_tail") or {}).get("z")
    if st is not None:
        parts.append(("sofr", max(0, st)))
    dwz = (canaries.get("discount_window") or {}).get("wow_z")
    if dwz is not None:
        parts.append(("dw", max(0, dwz)))
    dz = (canaries.get("bank_deposits") or {}).get("outflow_z")
    if dz is not None:
        parts.append(("dep", max(0, -dz)))
    if avail["auction_slope"] and canaries["auction_slope"].get("deteriorating"):
        parts.append(("auc", 1.5))
    if rev and rev["all_negative_last4"]:
        parts.append(("rev", 1.5))
    for ck, fld, inv in (("treasury_vol_proxy", "z", False), ("cp_bill_spread", "z", False),
                          ("mmf_assets", "surge_z", False), ("bank_reserves", "drain_z", True),
                          ("pd_fails", "z", False)):
        zz = (canaries.get(ck) or {}).get(fld)
        if zz is not None:
            parts.append((ck[:6], max(0, -zz if inv else zz)))
    fs = (canaries.get("floor_spreads") or {}).get("sofr_iorb_bp")
    if fs is not None:
        parts.append(("floor", max(0, fs / 3.0)))
    score = round(min(100, max(0, (sum(v for _, v in parts) / max(1, len(parts))) * 28)), 1) if parts else None
    level = ("ACUTE" if (score or 0) >= 70 else "ELEVATED" if (score or 0) >= 45
             else "WATCH" if (score or 0) >= 25 else "CALM")

    n_logged = 0
    if (score or 0) >= 70:
        try:
            end = datetime.now(timezone.utc).date().isoformat()
            jq = hj(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/"
                    f"{(datetime.now(timezone.utc)-timedelta(days=7)).date().isoformat()}/{end}"
                    f"?adjusted=true&sort=asc&limit=10&apiKey={POLY_KEY}")
            px0 = (jq.get("results") or [{}])[-1].get("c")
            if px0:
                nowt = datetime.now(timezone.utc)
                DDB.Table("justhodl-signals").put_item(Item={
                    "signal_id": f"crisis-canary#USD#{today}", "signal_type": "crisis_canary",
                    "signal_value": str(score), "predicted_direction": "DOWN",
                    "confidence": Decimal("0.60"), "measure_against": "ticker",
                    "baseline_price": str(px0), "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat() for w in (5, 21, 63)},
                    "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                    "logged_epoch": int(nowt.timestamp()), "status": "pending", "schema_version": "2",
                    "horizon_days_primary": 21, "regime_at_log": level,
                    "ttl": int(nowt.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "crisis-canaries", "v": VERSION, "score": str(score)},
                    "rationale": f"Funding canary composite {score} ({level}): " + "; ".join(alerts[:3])})
                n_logged = 1
        except Exception as e:
            print(f"[signals] {str(e)[:80]}")

    out = {"engine": "crisis-canaries", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "availability": avail, "canaries": canaries,
           "composite_score": score, "level": level, "alerts": alerts,
           "signals_logged": n_logged,
           "families": fam_scores, "red_count": red_total, "n_global": n_total,
           "global_score": global_score, "grid_score_ingested": grid_score,
           "composite_v3": (round(0.35 * (score or 0) + 0.45 * (global_score or 0)
                                    + 0.20 * (grid_score if grid_score is not None
                                               else (global_score or 0)), 1)
                             if (global_score is not None or score is not None) else None),
           "level_v3": None,
           "known_gaps": ["FHLB discount-note issuance has no free machine-readable feed (KHALID_ACTIONS)"],
           "methodology": ("Coverage-honest composite over live funding canaries: SOFR 99th-pct tail z, "
                           "H.4.1 discount-window WoW z, H.8 small-bank deposit outflow z, auction-composite "
                           "slope (self-bootstrapping 3-obs min), ALFRED payroll first-release-vs-latest "
                           "revision signature, OFR repo volume z. Score≥70 logs crisis_canary DOWN vs SPY "
                           "to the closed loop.")}
    cv3 = out.get("composite_v3")
    out["level_v3"] = ("ACUTE" if (cv3 or 0) >= 70 else "ELEVATED" if (cv3 or 0) >= 45
                        else "WATCH" if (cv3 or 0) >= 25 else "CALM") if cv3 is not None else None
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[canaries] score={score} level={level} avail={avail} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"score": score, "level": level})}
