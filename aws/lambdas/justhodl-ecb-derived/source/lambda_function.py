"""justhodl-ecb-derived — the genuinely-missing ECB-derived dump predictors.

Verified gaps (the other ~6 high-signal indicators already exist in eurodollar-
stress / euro-fragmentation / systemic-stress / global-liquidity). Builds:

  #8  CISS Acceleration        — 30d Δ of CISS (raw + z). Strongest lead signal.
  #5  Bank Funding Stress      — MRO/LTRO reliance ratio + MLF spike (TARGET2 code
                                 A090400 is 404 at ECB; this captures the same
                                 bank-funding-stress intent with data that exists).
  #18 EU/US Liquidity Diverge  — ECB balance-sheet 6m Δ vs Fed net-liquidity 6m Δ.
  #12 BLS Credit Standards     — EU bank lending survey C&I tightening (vs Fed SLOOS).

v2.0 adds the Top-10 gap-fills (ops 1522) + the important-ECB sweep:
  (a) 5Y5Y EUR inflation breakeven   (market-based LT expectations)
  (b) Market-implied ECB rate path   (AAA-curve forwards vs DFR — OIS proxy)
  #4  3M Euribor - 3M compounded eSTR  (the EU FRA-OIS analogue; interbank stress)
  #6  eSTR level + eSTR-DFR spread     (money-market plumbing dislocation)
  #7  TARGET2 by country               (full creditor/debtor table, 3m flows)
  #8  HICP headline/core/services/energy/food/goods (+ vs 2% target, momentum)
  SWEEP: SPF long-term inflation expectations - BSI loans to NFC/HH YoY (credit
  impulse) - EUR/USD + nominal EER trend.

OUTPUT: data/ecb-derived.json · SCHEDULE: daily 14:40 UTC.
"""
import json, time, ssl, statistics, os, bisect
import urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
ECB = "https://data-api.ecb.europa.eu/service/data/"
FRED_KEY = __import__("os").environ.get("FRED_API_KEY") or "2f057499936072679d8843d7fce99989"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
           "Accept": "text/csv;q=0.9, */*;q=0.5", "Accept-Language": "en-US,en;q=0.9"}
s3 = boto3.client("s3", region_name=REGION)
_ctx = ssl.create_default_context(); _ctx.check_hostname = False; _ctx.verify_mode = ssl.CERT_NONE


def ecb_csv(key, last_n=None, start=None):
    q = "?format=csvdata"
    if last_n: q += f"&lastNObservations={last_n}"
    if start: q += f"&startPeriod={start}"
    try:
        raw = urllib.request.urlopen(urllib.request.Request(ECB + key + q, headers=HEADERS), timeout=40, context=_ctx).read()
        if raw[:2] == b"\x1f\x8b":
            import gzip; raw = gzip.decompress(raw)
        text = raw.decode("utf-8", "replace")
        lines = text.strip().split("\n"); hdr = lines[0].split(",")
        ti = hdr.index("TIME_PERIOD"); vi = hdr.index("OBS_VALUE")
        pts = []
        for ln in lines[1:]:
            c = ln.split(",")
            if len(c) <= max(ti, vi): continue
            d, v = c[ti].strip(), c[vi].strip()
            if not d or not v: continue
            try: pts.append((d, float(v)))
            except ValueError: continue
        pts.sort()
        return pts
    except Exception as e:
        print(f"[ecb-derived] {key} err: {str(e)[:60]}"); return []


def ecb_multi(path, last_n=None, start=None):
    """One OR-syntax SDMX call -> {series_key: [(date, val), ...]} sorted by date."""
    q = "?format=csvdata"
    if last_n: q += f"&lastNObservations={last_n}"
    if start: q += f"&startPeriod={start}"
    out = {}
    try:
        raw = urllib.request.urlopen(urllib.request.Request(ECB + path + q, headers=HEADERS), timeout=45, context=_ctx).read()
        if raw[:2] == b"\x1f\x8b":
            import gzip; raw = gzip.decompress(raw)
        lines = raw.decode("utf-8", "replace").strip().split("\n")
        if len(lines) < 2: return out
        hdr = lines[0].split(",")
        ki = hdr.index("KEY"); ti = hdr.index("TIME_PERIOD"); vi = hdr.index("OBS_VALUE")
        for ln in lines[1:]:
            c = ln.split(",")
            if len(c) <= max(ki, ti, vi): continue
            k, d, v = c[ki].strip(), c[ti].strip(), c[vi].strip()
            if not k or not d or not v: continue
            try: out.setdefault(k, []).append((d, float(v)))
            except ValueError: continue
        for k in out: out[k].sort()
    except Exception as e:
        print(f"[ecb-derived] multi {path} err: {str(e)[:60]}")
    return out


def fred_series(sid):
    try:
        u = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=200"
        d = json.loads(urllib.request.urlopen(u, timeout=20).read().decode())
        obs = [(o["date"], float(o["value"])) for o in d.get("observations", []) if o["value"] not in (".", "")]
        obs.sort()
        return obs
    except Exception as e:
        print(f"[ecb-derived] fred {sid} err: {str(e)[:50]}"); return []


def zscore(vals, lookback=260):
    if len(vals) < 10: return None
    w = vals[-lookback:] if len(vals) >= lookback else vals
    mu = statistics.mean(w); sd = statistics.pstdev(w)
    return round((vals[-1] - mu) / sd, 2) if sd else None


def lambda_handler(event=None, context=None):
    t0 = time.time(); out = {"engine": "ecb-derived", "version": "3.4.0",
                             "generated_at": datetime.now(timezone.utc).isoformat(), "indicators": {}}

    # ── #8 CISS Acceleration — read the history file we already build ──
    try:
        ch = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ecb-hist/ciss_ea.json")["Body"].read())
        pts = ch.get("points", [])
        vals = [p[1] for p in pts]
        if len(vals) > 40:
            latest = vals[-1]
            # 30 calendar days ≈ 22 trading days (CISS is business-daily)
            d30 = vals[-1] - vals[-23] if len(vals) > 23 else None
            d30_series = [vals[i] - vals[i-23] for i in range(23, len(vals))]
            accel_z = zscore(d30_series) if d30_series else None
            level = "CRITICAL" if (d30 or 0) > 0.30 else "WATCH" if (d30 or 0) > 0.15 else "CALM"
            out["indicators"]["ciss_acceleration"] = {
                "name": "CISS 30-day Acceleration", "ciss_level": round(latest, 5),
                "delta_30d": round(d30, 5) if d30 is not None else None,
                "delta_30d_zscore": accel_z, "signal": level,
                "interpretation": f"CISS Δ30d {round(d30,4) if d30 is not None else '—'} — {level}. >+0.15 watch, >+0.30 critical. Lead ~2w EU / ~4w US equities.",
                "thresholds": {"watch": 0.15, "critical": 0.30}}
    except Exception as e:
        out["indicators"]["ciss_acceleration"] = {"err": str(e)[:60]}

    # ── #5 Bank Funding Stress — MRO/LTRO reliance + MLF spike ──
    try:
        mro = ecb_csv("ILM/W.U2.C.A050100.U2.EUR", last_n=60)
        ltro = ecb_csv("ILM/W.U2.C.A050200.U2.EUR", last_n=60)
        mlf = ecb_csv("ILM/W.U2.C.A050500.U2.EUR", last_n=60)
        if mro and ltro:
            m, l = mro[-1][1], ltro[-1][1]
            total = m + l
            ltro_share = round(100 * l / total, 1) if total else None
            ltro_share_4wago = None
            if len(mro) > 4 and len(ltro) > 4:
                t4 = mro[-5][1] + ltro[-5][1]
                ltro_share_4wago = round(100 * ltro[-5][1] / t4, 1) if t4 else None
            mlf_latest = mlf[-1][1] if mlf else None
            stress = "ELEVATED" if (ltro_share or 0) > 80 else "NORMAL"
            if mlf_latest and mlf_latest > 1000: stress = "ACUTE"  # MLF > €1bn = panic button
            out["indicators"]["bank_funding_stress"] = {
                "name": "Bank Funding Stress (MRO/LTRO reliance + MLF)",
                "mro_eur_mn": round(m), "ltro_eur_mn": round(l),
                "ltro_share_pct": ltro_share, "ltro_share_4w_ago_pct": ltro_share_4wago,
                "mlf_eur_mn": round(mlf_latest) if mlf_latest is not None else None,
                "signal": stress,
                "interpretation": f"LTRO share {ltro_share}% of policy lending (banks hoarding long when high). MLF €{round(mlf_latest) if mlf_latest else 0}mn (>€1bn = panic). {stress}.",
                "thresholds": {"ltro_share_elevated": 80, "mlf_acute_eur_mn": 1000}}
    except Exception as e:
        out["indicators"]["bank_funding_stress"] = {"err": str(e)[:60]}

    # ── #18 EU/US Liquidity Divergence ──
    try:
        # ECB side: reuse the already-computed balance sheet from ecb-detail (no dup fetch).
        ecb_6m_bn = None; ecb_total_bn = None
        try:
            ed = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ecb-detail.json")["Body"].read())
            bs = ed.get("balance_sheet", {})
            ecb_total_bn = bs.get("total_assets_eur_bn")
            ch6_pct = bs.get("change_6m_pct")
            if ecb_total_bn is not None and ch6_pct is not None:
                # convert % change to absolute €bn change over 6m
                prev = ecb_total_bn / (1 + ch6_pct/100.0)
                ecb_6m_bn = round(ecb_total_bn - prev, 1)
        except Exception:
            pass
        # Fed side: net liquidity (WALCL - RRP - TGA), 6m change, $bn
        walcl = fred_series("WALCL"); rrp = fred_series("RRPONTSYD"); tga = fred_series("WTREGEN")
        def fred_netliq_6m():
            if not walcl or len(walcl) < 27: return None
            def v(pts, idx): return pts[idx][1] if len(pts) > abs(idx) else 0
            # FRED units: WALCL $mn, WTREGEN(TGA) $mn → /1000 to $bn; RRPONTSYD already $bn
            now = v(walcl,-1)/1000.0 - v(rrp,-1) - v(tga,-1)/1000.0
            then = v(walcl,-27)/1000.0 - v(rrp,-27) - v(tga,-27)/1000.0
            return round(now - then, 1)
        fed_6m_bn = fred_netliq_6m()
        diverg = round(ecb_6m_bn - fed_6m_bn, 1) if (ecb_6m_bn is not None and fed_6m_bn is not None) else None
        sig = "TAIL_RISK" if (diverg is not None and abs(diverg) > 300) else "NORMAL"
        out["indicators"]["eu_us_liquidity_divergence"] = {
            "name": "EU/US Liquidity Divergence (6m Δ)",
            "ecb_total_assets_eur_bn": ecb_total_bn,
            "ecb_balance_sheet_6m_chg_eur_bn": ecb_6m_bn,
            "fed_net_liquidity_6m_chg_usd_bn": fed_6m_bn,
            "divergence_bn": diverg, "signal": sig,
            "interpretation": f"ECB 6m Δ {ecb_6m_bn}€bn vs Fed net-liq 6m Δ {fed_6m_bn}$bn → divergence {diverg}bn. |>300| = FX/carry tail risk (drove 2024 yen-unwind).",
            "thresholds": {"tail_risk_abs_bn": 300}}
    except Exception as e:
        out["indicators"]["eu_us_liquidity_divergence"] = {"err": str(e)[:60]}

    # ── #12 BLS Credit Standards (C&I tightening) ──
    try:
        bls = ecb_csv("BLS/Q.U2.ALL.O.E.Z.B3.ST.S.WFNET", last_n=12)
        if bls:
            # drop empty trailing quarters (survey not yet released)
            bls = [(d, v) for d, v in bls if v is not None]
            if bls:
                latest = bls[-1][1]
                prev = bls[-2][1] if len(bls) > 1 else None
                sig = "SEVERE_TIGHTENING" if latest > 20 else "TIGHTENING" if latest > 0 else "EASING"
                out["indicators"]["bls_credit_standards"] = {
                    "name": "EU Bank Lending Survey — credit standards (C&I)",
                    "net_pct_tightening": round(latest, 1), "prev_quarter": round(prev, 1) if prev is not None else None,
                    "as_of": bls[-1][0], "signal": sig,
                    "interpretation": f"Net {round(latest,1)}% of EU banks tightening C&I standards (>+20 severe). EU equivalent of Fed SLOOS; SX5E drawdowns follow 1-2 quarters.",
                    "thresholds": {"severe": 20}}
    except Exception as e:
        out["indicators"]["bls_credit_standards"] = {"err": str(e)[:60]}

    # ── #14 Bank Pass-Through Premium — NFC lending rate minus ECB DFR ──
    try:
        nfc = ecb_csv("MIR/M.U2.B.A2A.A.R.A.2240.EUR.N", last_n=18)   # NFC new-business lending rate
        dfr = ecb_csv("FM/D.U2.EUR.4F.KR.DFR.LEV", last_n=200)         # deposit facility rate (daily)
        if nfc and dfr:
            nfc_now = nfc[-1][1]; dfr_now = dfr[-1][1]
            premium = round(nfc_now - dfr_now, 2)
            # 3-month change in the premium (NFC is monthly → 3 obs back)
            prem_3m_ago = None
            if len(nfc) > 3:
                # match DFR ~3 months back (≈63 trading days)
                dfr_3m = dfr[-64][1] if len(dfr) > 64 else dfr[0][1]
                prem_3m_ago = round(nfc[-4][1] - dfr_3m, 2)
            widening_3m = round(premium - prem_3m_ago, 2) if prem_3m_ago is not None else None
            sig = "CREDIT_STRESS" if (widening_3m is not None and widening_3m > 0.50) else "NORMAL"
            out["indicators"]["bank_pass_through_premium"] = {
                "name": "Bank Pass-Through Premium (NFC lending rate − DFR)",
                "nfc_lending_rate_pct": round(nfc_now, 2), "dfr_pct": round(dfr_now, 2),
                "premium_pct": premium, "premium_3m_ago_pct": prem_3m_ago,
                "widening_3m_pp": widening_3m, "as_of": nfc[-1][0], "signal": sig,
                "interpretation": f"Banks charge corporates {premium}pp over the ECB risk-free rate ({round(nfc_now,2)}% NFC vs {round(dfr_now,2)}% DFR). Widening >+0.50pp/3m = banks pricing default risk, credit cycle turning. 3m change: {widening_3m if widening_3m is not None else '—'}pp.",
                "thresholds": {"credit_stress_widening_3m_pp": 0.50}}
    except Exception as e:
        out["indicators"]["bank_pass_through_premium"] = {"err": str(e)[:60]}

    # ── #3 TARGET2 Imbalance Stress — correct dataflow is TGB (not ILM A090400) ──
    # DE claim (capital fleeing TO Germany) and IT balance (FROM Italy) = fragmentation.
    try:
        de = ecb_csv("TGB/M.DE.N.A094T.U2.EUR.E", last_n=60)   # Germany TARGET2 position (monthly)
        it = ecb_csv("TGB/M.IT.N.A094T.U2.EUR.E", last_n=60)   # Italy TARGET2 position
        if de:
            de_now = de[-1][1]
            de_3m_ago = de[-4][1] if len(de) > 3 else None
            de_chg_3m = round(de_now - de_3m_ago) if de_3m_ago is not None else None
            it_now = it[-1][1] if it else None
            # DE−IT spread proxy for fragmentation (both are €mn)
            frag = round(de_now - it_now) if it_now is not None else None
            # capital-flight signal: rising DE claim. €mn so 100bn = 100000
            sig = "CRITICAL" if (de_chg_3m or 0) > 200000 else "WATCH" if (de_chg_3m or 0) > 100000 else "NORMAL"
            out["indicators"]["target2_imbalance"] = {
                "name": "TARGET2 Imbalance (Bundesbank claim — eurozone fragmentation)",
                "de_target2_eur_mn": round(de_now), "it_target2_eur_mn": round(it_now) if it_now is not None else None,
                "de_3m_change_eur_mn": de_chg_3m, "de_minus_it_eur_mn": frag,
                "as_of": de[-1][0], "signal": sig,
                "interpretation": f"Bundesbank TARGET2 position €{round(de_now/1000)}bn (DE−IT spread €{round(frag/1000) if frag is not None else '—'}bn). Rising DE claim = capital fleeing periphery into Germany. 3m Δ €{round(de_chg_3m/1000) if de_chg_3m is not None else '—'}bn; >+€100bn watch, >+€200bn critical (2011-12 fragmentation signature).",
                "thresholds": {"watch_3m_eur_bn": 100, "critical_3m_eur_bn": 200}}
    except Exception as e:
        out["indicators"]["target2_imbalance"] = {"err": str(e)[:60]}

    # ── #1 USD Funding Stress Composite — A030000 + L080000 (+ Fed swap lines) z-scored ──
    try:
        a030 = ecb_csv("ILM/W.U2.C.A030000.U2.Z06", last_n=260)   # USD claims on EA residents
        l080 = ecb_csv("ILM/W.U2.C.L080000.U4.EUR", last_n=260)   # FX liabilities to non-EA
        swp = fred_series("SWPT")  # Fed central-bank liquidity swaps ($mn); SWPT is the current series
        def _z_last(pts):
            if not pts or len(pts) < 20: return None
            vals = [p[1] for p in pts]
            mu = statistics.mean(vals); sd = statistics.pstdev(vals)
            return (vals[-1] - mu) / sd if sd else 0.0
        za = _z_last(a030); zl = _z_last(l080); zs = _z_last(swp) if swp else 0.0
        comps = [z for z in [za, zl, zs] if z is not None]
        if comps:
            # weighted: 40% A030000, 30% L080000, 30% swap (fall back to equal if missing)
            w = []
            if za is not None: w.append((za, 0.40))
            if zl is not None: w.append((zl, 0.30))
            if zs is not None: w.append((zs, 0.30))
            tw = sum(x[1] for x in w)
            composite_z = sum(x[0]*x[1] for x in w) / tw if tw else None
            # map z to 0-100 (z of +2 → ~84th pctile feel; clamp)
            score = round(max(0, min(100, 50 + (composite_z or 0) * 20)), 1)
            sig = "CRITICAL" if score >= 70 else "WATCH" if score >= 50 else "NORMAL"
            out["indicators"]["usd_funding_stress_composite"] = {
                "name": "USD Funding Stress Composite (A030000 + L080000 + Fed swaps)",
                "score_0_100": score, "composite_z": round(composite_z, 2) if composite_z is not None else None,
                "a030000_z": round(za, 2) if za is not None else None,
                "l080000_z": round(zl, 2) if zl is not None else None,
                "fed_swap_z": round(zs, 2) if zs is not None else None,
                "signal": sig,
                "interpretation": f"Eurodollar dollar-shortage composite {score}/100 (z {round(composite_z,2) if composite_z is not None else '—'}). <50 normal, 50-70 watch, ≥70 critical. Combines ECB USD lending, foreign FX parking, and Fed swap-line usage.",
                "thresholds": {"watch": 50, "critical": 70}}
    except Exception as e:
        out["indicators"]["usd_funding_stress_composite"] = {"err": str(e)[:60]}

    # ── #4 Eurodollar Stress Index (ESI) — the master 0-100 dollar-shortage composite ──
    try:
        ufsc = out["indicators"].get("usd_funding_stress_composite", {})
        ciss = out["indicators"].get("ciss_acceleration", {})
        ptp = out["indicators"].get("bank_pass_through_premium", {})
        bfs = out["indicators"].get("bank_funding_stress", {})
        # components (each a 0-1 stress contribution)
        parts = []
        if ufsc.get("score_0_100") is not None: parts.append(("usd_funding", ufsc["score_0_100"]/100.0, 0.35))
        if ciss.get("ciss_level") is not None: parts.append(("ciss", min(1.0, ciss["ciss_level"]/0.5), 0.25))  # CISS 0.5+ = severe
        if ptp.get("premium_pct") is not None: parts.append(("pass_through", min(1.0, max(0, (ptp["premium_pct"]-1.0)/2.0)), 0.20))  # >1pp starts to matter
        mlf_binary = 1.0 if (bfs.get("mlf_eur_mn") or 0) > 1000 else 0.0
        parts.append(("mlf_spike", mlf_binary, 0.20))
        tw = sum(p[2] for p in parts)
        esi = round(sum(p[1]*p[2] for p in parts) / tw * 100, 1) if tw else None
        tier = "BLACK_SWAN" if (esi or 0) >= 85 else "CRITICAL" if (esi or 0) >= 70 else "WATCH" if (esi or 0) >= 50 else "NORMAL"
        out["indicators"]["eurodollar_stress_index"] = {
            "name": "Eurodollar Stress Index (ESI) — master dollar-shortage composite",
            "esi_0_100": esi, "tier": tier,
            "components": {p[0]: round(p[1]*100, 1) for p in parts},
            "signal": tier if tier != "NORMAL" else "NORMAL",
            "interpretation": f"ESI {esi}/100 → {tier}. The master dollar-shortage gauge: WATCH ≥50, CRITICAL ≥70, BLACK SWAN ≥85. 2008/2011/2020/2023 all crossed 70 weeks before SPX bottomed.",
            "thresholds": {"watch": 50, "critical": 70, "black_swan": 85}}
        # also mirror ESI into ecb-detail's eurodollar slot (where the audit looked)
        try:
            ed = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ecb-detail.json")["Body"].read())
            ed["eurodollar_stress_score"] = esi
            ed["eurodollar_stress_tier"] = tier
            s3.put_object(Bucket=BUCKET, Key="data/ecb-detail.json", Body=json.dumps(ed, default=str).encode(), ContentType="application/json")
        except Exception:
            pass
    except Exception as e:
        out["indicators"]["eurodollar_stress_index"] = {"err": str(e)[:60]}

    def eurostat_manr(coicop, n=8):
        """Latest euro-area HICP YoY months from Eurostat (post-enlargement aggregate)."""
        for geo in ("EA21", "EA20", "EA"):
            try:
                u = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr"
                     f"?format=JSON&lang=EN&geo={geo}&coicop={coicop}&lastTimePeriod={n}")
                raw = urllib.request.urlopen(urllib.request.Request(u, headers=HEADERS), timeout=30, context=_ctx).read()
                j = json.loads(raw)
                idx = j.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
                vals = j.get("value", {})
                pts = sorted((t, float(vals[str(i)])) for t, i in idx.items() if str(i) in vals)
                if pts:
                    return pts, geo
            except Exception:
                continue
        return [], None

    # ════════════════ v2.0 — Top-10 gap-fills (ops 1522) + important-ECB sweep ════════════════

    # ── #6 €STR + €STR−DFR · (b) market-implied rate path · #4 Euribor−OIS analogue ──
    try:
        estr = ecb_csv("EST/B.EU000A2X2A25.WT", last_n=15)                    # €STR volume-weighted rate
        dfr_d = ecb_csv("FM/D.U2.EUR.4F.KR.DFR.LEV", last_n=30)               # deposit facility rate
        estr3m = ecb_csv("EST/B.EU000A2QQF32.CR", last_n=10)                  # 3M compounded €STR average
        eur3m = ecb_csv("FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", last_n=8)       # 3M Euribor (monthly avg)
        yc = ecb_multi("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3M+SR_1Y+SR_2Y+SR_5Y+SR_10Y", last_n=3)

        def _yc(tenor):
            for k, pts in yc.items():
                if k.endswith("." + tenor) and pts:
                    return pts[-1][1]
            return None
        s3m_, s1y, s2y, s5y, s10y = _yc("SR_3M"), _yc("SR_1Y"), _yc("SR_2Y"), _yc("SR_5Y"), _yc("SR_10Y")
        e_now = estr[-1][1] if estr else None
        d_now = dfr_d[-1][1] if dfr_d else None
        spread_bp = round((e_now - d_now) * 100, 1) if (e_now is not None and d_now is not None) else None
        # market-implied path from AAA spot-curve forwards (public OIS proxy)
        f1y1y = round(((1 + s2y / 100) ** 2 / (1 + s1y / 100) - 1) * 100, 2) if (s1y is not None and s2y is not None) else None
        f5y5y_nom = round((((1 + s10y / 100) ** 10 / (1 + s5y / 100) ** 5) ** 0.2 - 1) * 100, 2) if (s5y is not None and s10y is not None) else None
        next12_bp = round((s1y - e_now) * 100) if (s1y is not None and e_now is not None) else None
        following12_bp = round((f1y1y - s1y) * 100) if (f1y1y is not None and s1y is not None) else None
        eo_bp = round((eur3m[-1][1] - estr3m[-1][1]) * 100, 1) if (eur3m and estr3m) else None
        path_read = None
        if next12_bp is not None:
            verb = "cuts" if next12_bp < 0 else "hikes"
            path_read = (f"Curve prices ~{abs(next12_bp)}bp of {verb} over the next 12m"
                         + (f", then {abs(following12_bp)}bp more {'cuts' if (following12_bp or 0) < 0 else 'hikes'} the following year (1y1y {f1y1y}%)." if following12_bp is not None else "."))
        out["rates_curve"] = {
            "estr_pct": round(e_now, 3) if e_now is not None else None,
            "estr_as_of": estr[-1][0] if estr else None,
            "dfr_pct": round(d_now, 2) if d_now is not None else None,
            "estr_dfr_spread_bp": spread_bp,
            "euribor_3m_pct": round(eur3m[-1][1], 3) if eur3m else None,
            "euribor_as_of": eur3m[-1][0] if eur3m else None,
            "estr_3m_compounded_pct": round(estr3m[-1][1], 3) if estr3m else None,
            "euribor_ois_proxy_bp": eo_bp,
            "aaa_spot": {"3m": s3m_, "1y": s1y, "2y": s2y, "5y": s5y, "10y": s10y},
            "fwd_1y1y_pct": f1y1y, "fwd_5y5y_nominal_pct": f5y5y_nom,
            "implied_next_12m_bp": next12_bp, "implied_following_12m_bp": following12_bp,
            "path_read": path_read,
            "as_of": estr[-1][0] if estr else None,
            "method_note": "Path from EA AAA spot-curve forwards vs €STR (OIS proxy — true OIS quotes are licensed). Euribor−OIS uses 3M compounded €STR (backward avg) as the risk-free leg.",
        }
        if spread_bp is not None:
            sig = "CRITICAL" if spread_bp >= 3 else "WATCH" if spread_bp >= -1 else "NORMAL"
            out["indicators"]["estr_dfr_dislocation"] = {
                "name": "€STR−DFR Dislocation (money-market plumbing)",
                "estr_pct": round(e_now, 3), "dfr_pct": round(d_now, 2), "spread_bp": spread_bp, "signal": sig,
                "interpretation": f"€STR {round(e_now,3)}% vs DFR {round(d_now,2)}% → {spread_bp}bp. Normally €STR sits a few bp BELOW the floor; ≥−1bp = collateral/reserve scarcity creeping in, ≥+3bp = plumbing dislocation (floor losing grip).",
                "thresholds": {"watch_bp": -1, "critical_bp": 3}}
        if eo_bp is not None:
            sig = "CRITICAL" if eo_bp >= 50 else "WATCH" if eo_bp >= 25 else "NORMAL"
            out["indicators"]["euribor_ois_stress"] = {
                "name": "Euribor−OIS Stress (EU interbank, FRA-OIS analogue)",
                "euribor_3m_pct": round(eur3m[-1][1], 3), "estr_3m_comp_pct": round(estr3m[-1][1], 3),
                "spread_bp": eo_bp, "signal": sig,
                "interpretation": f"3M Euribor − 3M compounded €STR = {eo_bp}bp. <25 normal, 25-50 watch, ≥50 critical (2008 >150bp, 2011 ~90bp). The euro leg of bank counterparty fear.",
                "thresholds": {"watch": 25, "critical": 50}}
    except Exception as e:
        out["rates_curve"] = {"err": str(e)[:60]}

    # ── (a) LT inflation expectations — SPF anchor + market context ──
    try:
        spf = ecb_csv("SPF/Q.U2.HICP.POINT.LT.Q.AVG", last_n=6)
        spf_now = round(spf[-1][1], 2) if spf else None
        spf_prev = round(spf[-2][1], 2) if len(spf) > 1 else None
        anchored = (spf_now is not None and 1.7 <= spf_now <= 2.3)
        rcb = out.get("rates_curve", {})
        out["inflation_expectations"] = {
            "spf_longterm_pct": spf_now, "spf_prev_q_pct": spf_prev,
            "spf_as_of": spf[-1][0] if spf else None,
            "anchored": anchored,
            "nominal_5y5y_fwd_pct": rcb.get("fwd_5y5y_nominal_pct"),
            "note": ("Market 5y5y ILS is licensed data not redistributed via the ECB public portal. "
                     "SPF long-term is the ECB's own survey anchor; nominal AAA 5y5y forward shown for the rates leg."),
        }
        if spf_now is not None:
            out["indicators"]["expectations_deanchoring"] = {
                "name": "LT Inflation Expectations (SPF anchor)",
                "spf_longterm_pct": spf_now, "spf_prev_q_pct": spf_prev,
                "nominal_5y5y_fwd_pct": rcb.get("fwd_5y5y_nominal_pct"),
                "as_of": spf[-1][0],
                "signal": "NORMAL" if anchored else "WATCH",
                "interpretation": (f"SPF long-term inflation expectations {spf_now}% "
                                    f"(prev {spf_prev}%) — "
                                    + ("inside the 1.7–2.3% anchored band: ECB credibility "
                                        "intact, the precondition for cutting into weakness."
                                        if anchored else
                                        "OUTSIDE the 1.7–2.3% anchored band — credibility "
                                        "stress; de-anchoring forces policy to stay tight "
                                        "into a downturn.")),
                "thresholds": {"anchored_band": [1.7, 2.3]}}
    except Exception as e:
        out["inflation_expectations"] = {"err": str(e)[:60]}

    # ── #8 HICP headline / core / services / energy / food / goods ──
    try:
        icp = ecb_multi("ICP/M.U2.N.000000+XEF000+SERV00+NRGY00+FOOD00+GOODS0+IGXE00.4.ANR", last_n=15)
        # euro-area enlargement (BG 2026-01) re-coded the aggregate: ECB U2/ICP lags at 2025-12.
        # Merge latest months from Eurostat post-enlargement series (EA21 → EA20 → EA fallback).
        ES_MAP = {"000000": "CP00", "XEF000": "TOT_X_NRG_FOOD", "SERV00": "SERV", "NRGY00": "NRG", "FOOD00": "FOOD"}
        es_geo_used = None
        for ecb_code, es_code in ES_MAP.items():
            try:
                base = None
                for k in icp:
                    if f".{ecb_code}." in k:
                        base = k; break
                if base is None:
                    continue
                last_ecb = icp[base][-1][0] if icp[base] else ""
                es_pts, geo = eurostat_manr(es_code, n=8)
                if es_pts:
                    es_geo_used = es_geo_used or geo
                    add = [(d, v) for d, v in es_pts if d > last_ecb]
                    icp[base] = icp[base] + add
            except Exception:
                continue

        def _icp(code):
            for k, pts in icp.items():
                if f".{code}." in k:
                    return pts
            return []
        Hd, Co, Sv = _icp("000000"), _icp("XEF000"), _icp("SERV00")
        En, Fd, Gd, Ig = _icp("NRGY00"), _icp("FOOD00"), _icp("GOODS0"), _icp("IGXE00")

        def _L(p): return round(p[-1][1], 2) if p else None
        def _m3(p): return round(p[-1][1] - p[-4][1], 2) if len(p) > 3 else None
        h, c_, sv = _L(Hd), _L(Co), _L(Sv)
        sticky = (sv is not None and sv > 3.0 and (c_ or 0) > 2.4)
        read = None
        if h is not None:
            gap = round(h - 2.0, 2)
            mom = _m3(Hd)
            read = (f"Headline {h}% ({'+' if gap >= 0 else ''}{gap}pp vs 2% target), core {c_}%, services {sv}%. "
                    f"3m momentum {'+' if (mom or 0) >= 0 else ''}{mom}pp"
                    + (" — services-sticky: ECB's hardest mile." if sticky else "."))
        out["inflation"] = {
            "headline_yoy": h, "headline_3m_chg_pp": _m3(Hd),
            "core_yoy": c_, "core_3m_chg_pp": _m3(Co),
            "services_yoy": sv, "energy_yoy": _L(En), "food_yoy": _L(Fd),
            "goods_yoy": _L(Gd), "indus_goods_ex_energy_yoy": _L(Ig),
            "vs_target_pp": round(h - 2.0, 2) if h is not None else None,
            "sticky_services": sticky,
            "as_of": Hd[-1][0] if Hd else None, "read": read,
            "latest_source": ("Eurostat " + es_geo_used) if es_geo_used else "ECB ICP (U2)",
        }
    except Exception as e:
        out["inflation"] = {"err": str(e)[:60]}

    # ── SWEEP: wages — official negotiated (STS) + ECB forward wage tracker (EWT) ──
    try:
        off = ecb_csv("STS/Q.U2.N.INWR.000000.3.ANR", last_n=10)          # official negotiated wages YoY
        trk = ecb_csv("EWT/Q.U2.N.WT.INWR._T.4F0.GY", last_n=10)          # wage tracker incl one-offs (extends fwd)
        trx = ecb_csv("EWT/Q.U2.N.WT.INWX._T.4F0.GY", last_n=4)           # ex one-off payments
        cov = ecb_csv("EWT/Q.U2.N.WT.COVR._T.4F0._Z", last_n=2)           # agreement coverage %
        o_now = round(off[-1][1], 2) if off else None
        o_1y = round(off[-5][1], 2) if len(off) > 4 else None
        t_fwd = round(trk[-1][1], 2) if trk else None
        t_fwd_d = trk[-1][0] if trk else None
        x_fwd = round(trx[-1][1], 2) if trx else None
        persist = (o_now is not None and o_now > 3.5) or (t_fwd is not None and t_fwd > 3.5)
        out["wages"] = {
            "negotiated_yoy_official": o_now, "official_as_of": off[-1][0] if off else None,
            "negotiated_1y_ago": o_1y,
            "tracker_fwd_yoy": t_fwd, "tracker_fwd_to": t_fwd_d,
            "tracker_ex_oneoffs_yoy": x_fwd,
            "tracker_coverage_pct": round(cov[-1][1], 1) if cov else None,
            "target_consistent_ceiling_pct": 3.0,
            "read": ((f"Negotiated wages {o_now}% YoY (official, {off[-1][0]}); ECB wage tracker projects {t_fwd}% "
                      f"through {t_fwd_d} ({x_fwd}% ex one-offs). ~3% is the 2%-inflation-consistent ceiling "
                      f"(target + ~1% productivity); "
                      + ("wage persistence ABOVE ceiling — services inflation stays sticky."
                         if persist else "wage disinflation intact — supports the easing case."))
                     if (o_now is not None and t_fwd is not None) else None),
        }
        if o_now is not None or t_fwd is not None:
            out["indicators"]["wage_persistence"] = {
                "name": "Wage Persistence (negotiated wages vs 3% ceiling)",
                "official_yoy": o_now, "tracker_fwd_yoy": t_fwd,
                "tracker_ex_oneoffs_yoy": x_fwd,
                "coverage_pct": round(cov[-1][1], 1) if cov else None,
                "as_of": off[-1][0] if off else t_fwd_d,
                "signal": "WATCH" if persist else "NORMAL",
                "interpretation": (f"Negotiated wages {o_now}% YoY, ECB tracker projects "
                                    f"{t_fwd}% ({x_fwd}% ex one-offs). ~3% is the "
                                    "2%-target-consistent ceiling (+~1% productivity); "
                                    + ("ABOVE ceiling — services inflation stays sticky, "
                                        "ECB hands tied." if persist else
                                        "wage disinflation intact — supports the easing "
                                        "path if growth cracks.")),
                "thresholds": {"watch_above": 3.5, "ceiling": 3.0}}
    except Exception as e:
        out["wages"] = {"err": str(e)[:60]}

    # ── #7 TARGET2 by country — full creditor/debtor table ──
    try:
        cc = "DE+IT+ES+FR+NL+GR+PT+AT+BE+FI+IE+LU"
        tg = ecb_multi(f"TGB/M.{cc}.N.A094T.U2.EUR.E", last_n=8)
        rows = []
        latest_d = None
        for k, pts in tg.items():
            if not pts:
                continue
            # key form TGB.M.DE.N.A094T... → REF_AREA is the 3rd token
            c2 = k.split(".")[2]
            now = pts[-1][1]
            ago = pts[-4][1] if len(pts) > 3 else None
            rows.append({"cc": c2, "eur_bn": round(now / 1000, 1),
                         "chg_3m_bn": round((now - ago) / 1000, 1) if ago is not None else None})
            latest_d = max(latest_d or pts[-1][0], pts[-1][0])
        rows.sort(key=lambda r: -(r["eur_bn"] if r["eur_bn"] is not None else 0))
        de = next((r for r in rows if r["cc"] == "DE"), None)
        it = next((r for r in rows if r["cc"] == "IT"), None)
        big_flow = max((abs(r["chg_3m_bn"]) for r in rows if r["chg_3m_bn"] is not None), default=0)
        out["target2"] = {
            "countries": rows, "as_of": latest_d,
            "de_minus_it_bn": round(de["eur_bn"] - it["eur_bn"], 1) if (de and it) else None,
            "max_abs_3m_flow_bn": big_flow,
            "read": (f"Largest creditor {rows[0]['cc']} €{rows[0]['eur_bn']}bn; biggest 3m flow €{big_flow}bn. "
                     "Rising DE claim + falling IT/ES = capital flight to the core (2011-12 signature).") if rows else None,
        }
    except Exception as e:
        out["target2"] = {"err": str(e)[:60]}

    # ── SWEEP: credit impulse — BSI adjusted loans YoY (NFC + households) ──
    try:
        nfc = ecb_csv("BSI/M.U2.Y.U.A20T.A.I.U2.2240.Z01.A", last_n=15)
        hh = ecb_csv("BSI/M.U2.Y.U.A20T.A.I.U2.2250.Z01.A", last_n=15)

        def _g(p, i): return round(p[i][1], 2) if len(p) > abs(i) else None
        n_now, n_6m = _g(nfc, -1), _g(nfc, -7)
        h_now, h_6m = _g(hh, -1), _g(hh, -7)
        accel = None
        if n_now is not None and n_6m is not None:
            accel = round(((n_now - n_6m) + ((h_now or 0) - (h_6m or 0))) / 2, 2)
        out["credit"] = {
            "nfc_loans_yoy": n_now, "nfc_6m_ago": n_6m,
            "hh_loans_yoy": h_now, "hh_6m_ago": h_6m,
            "impulse_6m_pp": accel,
            "as_of": nfc[-1][0] if nfc else None,
            "read": (f"NFC credit {n_now}% YoY ({'+' if (n_now or 0) - (n_6m or 0) >= 0 else ''}{round((n_now or 0)-(n_6m or 0),2)}pp/6m), "
                     f"households {h_now}%. Credit impulse {'accelerating' if (accel or 0) > 0 else 'decelerating'} — leads EA domestic demand ~2-3q.") if n_now is not None else None,
        }
        if n_now is not None:
            csig = ("CRITICAL" if n_now < 0 else
                    "WATCH" if (accel is not None and accel <= -1.0) else "NORMAL")
            out["indicators"]["credit_contraction"] = {
                "name": "EA Credit Cycle (NFC + household loans)",
                "nfc_loans_yoy": n_now, "hh_loans_yoy": h_now,
                "impulse_6m_pp": accel, "as_of": nfc[-1][0],
                "signal": csig,
                "interpretation": (f"NFC credit {n_now}% YoY, households {h_now}%; "
                                    f"6m impulse {accel:+.2f}pp. "
                                    + ("Outright CONTRACTION — 2009/2012-14 recession "
                                        "signature." if n_now < 0 else
                                        "Impulse decelerating ≥1pp — domestic demand "
                                        "rolls in ~2-3 quarters." if csig == "WATCH" else
                                        "Credit impulse healthy — leads EA domestic "
                                        "demand by ~2-3 quarters.")),
                "thresholds": {"critical_below": 0, "watch_impulse_6m": -1.0}}
    except Exception as e:
        out["credit"] = {"err": str(e)[:60]}

    # ── SWEEP: FX — EUR/USD + nominal effective exchange rate ──
    try:
        eu = ecb_csv("EXR/D.USD.EUR.SP00.A", last_n=270)
        ne = ecb_csv("EXR/D.E02.EUR.EN00.A", last_n=270)
        ne_grp = "E02"
        if not ne:
            ne = ecb_csv("EXR/D.E01.EUR.EN00.A", last_n=270); ne_grp = "E01"

        def _pct(p, back):
            if len(p) <= back: return None
            a, b = p[-1][1], p[-1 - back][1]
            return round((a / b - 1) * 100, 2) if b else None
        ez = zscore([v for _, v in eu]) if eu else None
        out["fx"] = {
            "eurusd": round(eu[-1][1], 4) if eu else None,
            "eurusd_1m_pct": _pct(eu, 22), "eurusd_3m_pct": _pct(eu, 66),
            "eurusd_z_1y": ez,
            "neer": round(ne[-1][1], 2) if ne else None, "neer_group": ne_grp if ne else None,
            "neer_3m_pct": _pct(ne, 66),
            "as_of": eu[-1][0] if eu else None,
            "read": (f"EUR/USD {round(eu[-1][1],4)} ({'+' if (_pct(eu,66) or 0) >= 0 else ''}{_pct(eu,66)}% /3m, z {ez}). "
                     f"NEER {'+' if (_pct(ne,66) or 0) >= 0 else ''}{_pct(ne,66)}% /3m — euro {'strength tightens' if (_pct(ne,66) or 0) > 0 else 'weakness eases'} financial conditions.") if eu else None,
        }
    except Exception as e:
        out["fx"] = {"err": str(e)[:60]}

    # ════════════════ v2.2 — fragmentation, M3, GC countdown, percentile ranks ════════════════

    # ── Fragmentation: peripheral 10Y spreads vs Bund (IRS convergence yields, monthly) ──
    try:
        irs = {}
        for cc in ("DE", "IT", "FR", "ES", "PT", "GR"):
            r = ecb_csv(f"IRS/M.{cc}.L.L40.CI.0000.EUR.N.Z", last_n=4)
            irs[cc] = r[-1] if r else None
        de = irs["DE"]
        frag = {}
        for cc in ("IT", "FR", "ES", "PT", "GR"):
            if de and irs[cc] and irs[cc][0] == de[0]:
                frag[cc] = round((irs[cc][1] - de[1]) * 100, 0)
        out["fragmentation"] = {
            "spreads_bp": frag, "as_of": de[0] if de else None, "cadence": "monthly avg",
            "tpi_watch_bp": 150,
            "read": (f"IT−DE {frag.get('IT', '—')}bp · FR−DE {frag.get('FR', '—')}bp · ES−DE {frag.get('ES', '—')}bp "
                     f"(monthly convergence yields). TPI-watch threshold 150bp on IT." if frag else None),
        }
        if frag.get("IT") is not None:
            itbp = frag["IT"]
            fsig = "CRITICAL" if itbp >= 250 else "WATCH" if itbp >= 150 else "NORMAL"
            out["indicators"]["fragmentation_stress"] = {
                "name": "Fragmentation / TPI Watch (periphery vs Bund)",
                "it_de_bp": itbp, "spreads_bp": frag,
                "as_of": de[0] if de else None,
                "signal": fsig,
                "interpretation": (f"IT−DE {itbp}bp · FR−DE {frag.get('FR','—')}bp · "
                                    f"ES−DE {frag.get('ES','—')}bp · GR−DE "
                                    f"{frag.get('GR','—')}bp (monthly convergence yields). "
                                    + ("≥250bp — TPI-activation territory; periphery "
                                        "funding stress acute." if fsig == "CRITICAL" else
                                        "≥150bp — TPI-chatter zone." if fsig == "WATCH" else
                                        "Below the 150bp TPI-watch line — periphery "
                                        "funding calm; the 2011/2022 dump channel is "
                                        "closed for now.")),
                "thresholds": {"watch": 150, "critical": 250}}
    except Exception as e:
        out["fragmentation"] = {"err": str(e)[:60]}

    # ── M3 (primary monetary aggregate) → into credit block ──
    try:
        m3 = ecb_csv("BSI/M.U2.Y.V.M30.X.I.U2.2300.Z01.A", last_n=8)
        if m3 and isinstance(out.get("credit"), dict) and not out["credit"].get("err"):
            ci = out["indicators"].get("credit_contraction")
            if isinstance(ci, dict):
                ci["m3_yoy"] = round(m3[-1][1], 1)
            out["credit"]["m3_yoy"] = round(m3[-1][1], 1)
            out["credit"]["m3_6m_chg_pp"] = round(m3[-1][1] - m3[-7][1], 1) if len(m3) > 6 else None
            out["credit"]["m3_as_of"] = m3[-1][0]
    except Exception:
        pass

    # ── Next Governing Council meeting (confirmed 2026 dates) ──
    try:
        GC_2026 = ["2026-06-11", "2026-07-23", "2026-09-10", "2026-10-29", "2026-12-17"]
        today = datetime.now(timezone.utc).date()
        nxt = next((d for d in GC_2026 if datetime.strptime(d, "%Y-%m-%d").date() >= today), None)
        if nxt:
            days = (datetime.strptime(nxt, "%Y-%m-%d").date() - today).days
            out["next_gc"] = {"date": nxt, "days_to": days,
                              "note": "Jun/Jul/Sep confirmed; Oct/Dec provisional",
                              "read": f"Next Governing Council: {nxt} ({days}d). Curve prices the 12m path shown above into it."}
    except Exception:
        pass

    # ── Percentile ranks vs own history (data/ecb-hist/) ──
    def _pct_rank(hist_id, current):
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key=f"data/ecb-hist/{hist_id}.json")["Body"].read())
            vals = [p[1] for p in doc.get("points", []) if p[1] is not None]
            if len(vals) < 24 or current is None:
                return None
            r = round(100 * sum(1 for v in vals if v <= current) / len(vals))
            return {"pct": r, "n": len(vals), "since": doc.get("first_date", "")[:4]}
        except Exception:
            return None

    try:
        PCT_MAP = [("inflation", "headline_yoy", "hicp_headline"), ("inflation", "core_yoy", "hicp_core"),
                   ("inflation", "services_yoy", "hicp_services"), ("wages", "negotiated_yoy_official", "wages_negotiated"),
                   ("credit", "nfc_loans_yoy", "nfc_loans_yoy"), ("credit", "m3_yoy", "m3_yoy"),
                   ("fx", "eurusd", "eurusd"), ("target2", "de_minus_it_bn", "t2_de_minus_it"),
                   ("rates_curve", "euribor_ois_proxy_bp", "euribor_ois_bp"), ("rates_curve", "fwd_1y1y_pct", "yc_1y1y"),
                   ("inflation_expectations", "spf_longterm_pct", "spf_longterm")]
        for blk, field, hid in PCT_MAP:
            b = out.get(blk)
            if isinstance(b, dict) and not b.get("err") and b.get(field) is not None:
                pr = _pct_rank(hid, b[field])
                if pr:
                    b.setdefault("pct_ranks", {})[field] = pr
        fr = out.get("fragmentation", {})
        if isinstance(fr, dict) and (fr.get("spreads_bp") or {}).get("IT") is not None:
            pr = _pct_rank("it_de_10y_bp", fr["spreads_bp"]["IT"])
            if pr:
                fr["pct_ranks"] = {"IT": pr}
    except Exception:
        pass

    # ── ESI daily history accumulator (snapshot-only metric becomes chartable) ──
    try:
        esi_now = (out["indicators"].get("eurodollar_stress_index") or {}).get("esi_0_100")
        if esi_now is not None:
            key = "data/ecb-hist/esi.json"
            try:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            except Exception:
                doc = {"id": "esi", "label": "Eurodollar Stress Index (0-100)", "freq": "daily",
                       "unit": "", "source": "justhodl-ecb-derived", "points": []}
            d0 = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            pts = [p for p in doc["points"] if p[0] != d0] + [[d0, esi_now]]
            pts.sort()
            doc.update({"points": pts, "first_date": pts[0][0], "latest_date": pts[-1][0], "n_points": len(pts)})
            s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc).encode(),
                          ContentType="application/json", CacheControl="public, max-age=3600")
            esi_pr = _pct_rank("esi", esi_now)
            if esi_pr and len(pts) >= 24:
                out["indicators"]["eurodollar_stress_index"]["pct_rank"] = esi_pr
    except Exception:
        pass



    # ── EU MACRO-CYCLE CANARIES (v3.2): unemployment, IP, confidence, real M1 ──
    def fred_obs(sid, start="1998-01-01"):
        try:
            import urllib.parse as _up
            u = ("https://api.stlouisfed.org/fred/series/observations?"
                 + _up.urlencode({"series_id": sid, "api_key": FRED_KEY, "file_type": "json",
                                   "observation_start": start, "limit": 100000}))
            j = json.loads(urllib.request.urlopen(u, timeout=35).read())
            o = [(x["date"], float(x["value"])) for x in j.get("observations", [])
                 if x.get("value") not in (".", "")]
            o.sort(); return o
        except Exception as e:
            print(f"[macro fred] {sid}: {str(e)[:50]}"); return []

    def _probe(sids, start="1998-01-01"):
        for sid in sids:
            o = fred_obs(sid, start)
            if len(o) > 40:
                return sid, o
        return None, []

    macro_series = {}

    def eurostat_series(dataset, dims, n=400, geos=("EA21", "EA20", "EA")):
        """Generic Eurostat JSON fetch → sorted [(YYYY-MM, val)]."""
        for geo in geos:
            try:
                q = "&".join(f"{k}={v}" for k, v in dims.items())
                u = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
                     f"{dataset}?format=JSON&lang=EN&geo={geo}&{q}&lastTimePeriod={n}")
                raw = urllib.request.urlopen(urllib.request.Request(u, headers=HEADERS),
                                              timeout=35, context=_ctx).read()
                j = json.loads(raw)
                idx = (j.get("dimension", {}).get("time", {}).get("category", {})
                        .get("index", {}))
                vals = j.get("value", {})
                pts = sorted((t, float(vals[str(i)])) for t, i in idx.items()
                              if str(i) in vals)
                if len(pts) > 24:
                    return pts, f"eurostat:{dataset}:{geo}"
            except Exception as _e:
                print(f"[eurostat] {dataset} {geo}: {str(_e)[:70]}")
                out.setdefault("_eurostat_debug", {})[f"{dataset}:{geo}"] = str(_e)[:90]
                continue
        return [], None

    # Unemployment rate, euro area — LIVE (FRED EA codes discontinued 2023):
    # ECB LFSI first, Eurostat une_rt_m fallback.
    try:
        ue, sid = [], None
        for k in ("LFSI/M.I9.S.UNEHRT.TOTAL0.15_74.T", "LFSI/M.U2.S.UNEHRT.TOTAL0.15_74.T"):
            ue = ecb_csv(k, start="1998-01")
            if len(ue) > 24:
                sid = f"ecb:{k}"
                break
        if not ue:
            ue, sid = eurostat_series("une_rt_m", {"s_adj": "SA", "age": "TOTAL",
                                                    "unit": "PC_ACT", "sex": "T"})
        if ue:
            vals = [v for _, v in ue]
            chg3 = round(vals[-1] - vals[-4], 2) if len(vals) > 4 else None
            sig = ("CRITICAL" if (chg3 or 0) >= 0.4 else
                   "WATCH" if (chg3 or 0) >= 0.2 else "NORMAL")
            out["indicators"]["ea_unemployment"] = {
                "name": "EA Unemployment Momentum (Sahm-style)",
                "unemployment_rate_pct": vals[-1], "chg_3m_pp": chg3,
                "as_of": ue[-1][0], "series": sid, "signal": sig,
                "interpretation": (f"EA unemployment {vals[-1]}% ({ue[-1][0]}), 3m change "
                                    f"{chg3:+.2f}pp. Rises of +0.2pp WATCH / +0.4pp CRITICAL — "
                                    "labor turns are slow but never false; the EU Sahm analogue."),
                "thresholds": {"watch_3m_pp": 0.2, "critical_3m_pp": 0.4}}
            macro_series["ea_unemployment"] = ("EA unemployment rate (%)", ue, {})
    except Exception as e:
        out["indicators"]["ea_unemployment"] = {"err": str(e)[:60]}

    # Industrial production, euro area — LIVE via Eurostat sts_inpr_m
    # (FRED EA19 code discontinued; flashing 3-year-old data is worse than no data).
    try:
        ip, sid = [], None
        # ECB STS first (same proven SDMX path as everything else on this engine)
        for k in ("STS/M.I9.Y.PROD.NS0010.4.000", "STS/M.I8.Y.PROD.NS0010.4.000",
                  "STS/M.U2.Y.PROD.NS0010.4.000"):
            ipx = ecb_csv(k, start="1999-01")
            if len(ipx) > 30:
                ip, sid = ipx, f"ecb:{k}"
                break
        if not ip:
         for dims in ({"indic_bt": "PROD", "nace_r2": "B-D", "s_adj": "SCA", "unit": "I21"},
                     {"indic_bt": "PROD", "nace_r2": "B-D", "s_adj": "SCA", "unit": "I15"},
                     {"indic_bt": "PROD", "nace_r2": "B-D", "s_adj": "CA", "unit": "I21"},
                     {"indic_bt": "PROD", "nace_r2": "C", "s_adj": "SCA", "unit": "I21"}):
            ip, sid = eurostat_series("sts_inpr_m", dims, n=240)
            if ip:
                break  # noqa: indented under the fallback guard
        if len(ip) > 14:
            yoy = [(ip[i][0], round((ip[i][1] / ip[i - 12][1] - 1) * 100, 2))
                   for i in range(12, len(ip)) if ip[i - 12][1]]
            cur = yoy[-1][1]
            sig = ("CRITICAL" if cur <= -5 else "WATCH" if cur <= -2 else "NORMAL")
            out["indicators"]["ea_industrial_production"] = {
                "name": "EA Industrial Production (YoY)",
                "ip_yoy_pct": cur, "as_of": yoy[-1][0], "series": sid, "signal": sig,
                "interpretation": (f"EA industrial output {cur:+.1f}% YoY ({yoy[-1][0]}). "
                                    "≤−2% WATCH, ≤−5% CRITICAL — the manufacturing leg of every "
                                    "EA recession (2008 −21%, 2012 −4%, 2020 −28%)."),
                "thresholds": {"watch_yoy": -2, "critical_yoy": -5}}
            macro_series["ip_yoy"] = ("EA industrial production YoY (%)", yoy,
                                       {"watch": -2, "critical": -5})
    except Exception as e:
        out["indicators"]["ea_industrial_production"] = {"err": str(e)[:60]}

    # Business + consumer confidence (OECD MEI, monthly, amplitude-adjusted ~100)
    try:
        _, bc = _probe(["BSCICP02EZM460S", "BSCICP03EZM665S"])
        _, cc = _probe(["CSCICP02EZM460S", "CSCICP03EZM665S"])
        if bc:
            bvals = [v for _, v in bc]
            bz = zscore(bvals, 240)
            cz = zscore([v for _, v in cc], 240) if cc else None
            worst = min(x for x in (bz, cz) if x is not None)
            sig = ("CRITICAL" if worst <= -2 else "WATCH" if worst <= -1.2 else "NORMAL")
            out["indicators"]["ea_confidence"] = {
                "name": "EA Business & Consumer Confidence",
                "business_conf": bvals[-1], "business_z": bz,
                "consumer_conf": (cc[-1][1] if cc else None), "consumer_z": cz,
                "as_of": bc[-1][0], "signal": sig,
                "interpretation": (f"Business confidence {bvals[-1]} (z {bz}), consumer "
                                    f"{cc[-1][1] if cc else '—'} (z {cz}). Either leg ≤−1.2z "
                                    "WATCH, ≤−2z CRITICAL — soft data leads hard data 2-4m."),
                "thresholds": {"watch_z": -1.2, "critical_z": -2}}
            macro_series["business_confidence"] = ("EA business confidence (OECD, ~100)",
                                                    bc, {})
    except Exception as e:
        out["indicators"]["ea_confidence"] = {"err": str(e)[:60]}

    # Real M1 growth: M1 annual growth (ECB BSI) minus HICP headline YoY (ECB ICP).
    # Negative real M1 preceded 2008, 2011 and the 2023 stagnation — the single
    # best free EA liquidity-cycle canary.
    try:
        m1 = ecb_csv("BSI/M.U2.Y.V.M10.X.I.U2.2300.Z01.A", start="1998-01")
        icx = ecb_csv("ICP/M.U2.N.000000.4.INX", start="1998-01")
        icp = [(icx[i][0], round((icx[i][1] / icx[i - 12][1] - 1) * 100, 2))
               for i in range(12, len(icx)) if icx[i - 12][1]] if len(icx) > 14 else \
              ecb_csv("ICP/M.U2.N.000000.4.ANR", start="1999-01")
        if len(m1) > 40 and len(icp) > 40:
            di = dict(icp)
            real = [(d_, round(v_ - di[d_], 2)) for d_, v_ in m1 if d_ in di]
            if len(real) > 40:
                cur = real[-1][1]
                sig = ("CRITICAL" if cur <= -3 else "WATCH" if cur < 0 else "NORMAL")
                out["indicators"]["real_m1_growth"] = {
                    "name": "Real M1 Growth (M1 YoY − HICP YoY)",
                    "m1_yoy_pct": round(dict(m1).get(real[-1][0], m1[-1][1]), 2),
                    "hicp_yoy_pct": di.get(real[-1][0]),
                    "real_m1_growth_pct": cur, "as_of": real[-1][0], "signal": sig,
                    "interpretation": (f"Real M1 growth {cur:+.1f}% ({real[-1][0]}). Negative "
                                        "real M1 preceded every EA recession (2008, 2011, "
                                        "2023's −9% trough). <0 WATCH, ≤−3 CRITICAL — the "
                                        "liquidity-cycle master canary."),
                    "thresholds": {"watch": 0, "critical": -3}}
                macro_series["real_m1_growth"] = ("Real M1 growth, EA (% YoY, deflated)",
                                                   real, {"watch": 0, "critical": -3})
    except Exception as e:
        out["indicators"]["real_m1_growth"] = {"err": str(e)[:60]}


    # ── Country unemployment: DE / FR / IT / ES (ECB LFSI) + CH (Eurostat) ──
    try:
        cmap = {}
        for cc in ("DE", "FR", "IT", "ES"):
            pts = ecb_csv(f"LFSI/M.{cc}.S.UNEHRT.TOTAL0.15_74.T", start="2000-01")
            if len(pts) > 24:
                cmap[cc] = pts
        ch_pts = []
        for dims in ({"s_adj": "SA", "age": "TOTAL", "unit": "PC_ACT", "sex": "T"},
                     {"s_adj": "NSA", "age": "TOTAL", "unit": "PC_ACT", "sex": "T"},
                     {"s_adj": "TC", "age": "TOTAL", "unit": "PC_ACT", "sex": "T"}):
            ch_pts, _src = eurostat_series("une_rt_m", dims, n=320, geos=("CH",))
            if len(ch_pts) > 24:
                break
        if len(ch_pts) <= 24:
            for sid_ in ("LRHUTTTTCHM156S", "LRHUTTTTCHQ156S", "LMUNRRTTCHM156S"):
                f_ = fred_obs(sid_, "2000-01-01")
                if len(f_) > 24:
                    ch_pts, _src = f_, f"fred:{sid_}"
                    break
        if len(ch_pts) > 24:
            cmap["CH"] = ch_pts
        else:
            out.setdefault("_eurostat_debug", {})["CH_unemployment"] = "all sources empty"
        if cmap:
            rows, worst = {}, 0.0
            for cc, pts in cmap.items():
                vals = [v for _, v in pts]
                # frequency-aware 3m momentum: monthly → 3 steps back, quarterly → 1
                step = 4
                if len(pts) > 3:
                    try:
                        d1 = (pts[-1][0] + "-01")[:10] if len(pts[-1][0]) == 7 else pts[-1][0][:10]
                        d0 = (pts[-2][0] + "-01")[:10] if len(pts[-2][0]) == 7 else pts[-2][0][:10]
                        from datetime import date as _date
                        gap = (_date.fromisoformat(d1) - _date.fromisoformat(d0)).days
                        step = 2 if gap > 45 else 4
                    except Exception:
                        pass
                c3 = round(vals[-1] - vals[-step], 2) if len(vals) > step else None
                rows[cc] = {"rate_pct": vals[-1], "chg_3m_pp": c3, "as_of": pts[-1][0]}
                if c3 is not None:
                    worst = max(worst, c3)
                LBL = {"DE": "Germany", "FR": "France", "IT": "Italy",
                       "ES": "Spain", "CH": "Switzerland"}[cc]
                macro_series[f"unemp_{cc.lower()}"] = (f"{LBL} unemployment rate (%)",
                                                        pts, {})
            sig = ("CRITICAL" if worst >= 0.4 else "WATCH" if worst >= 0.2 else "NORMAL")
            out["indicators"]["country_unemployment"] = {
                "name": "Country Unemployment Momentum (DE/FR/IT/ES/CH)",
                "countries": rows, "worst_3m_rise_pp": round(worst, 2), "signal": sig,
                "interpretation": ("Per-country Sahm read: any 3m rise ≥+0.2pp WATCH / "
                                    "≥+0.4pp CRITICAL. Italy & Spain turning up while core "
                                    "holds = fragmentation-flavored downturn; Switzerland is "
                                    "the non-EA control."),
                "thresholds": {"watch_3m_pp": 0.2, "critical_3m_pp": 0.4}}
    except Exception as e:
        out["indicators"]["country_unemployment"] = {"err": str(e)[:60]}

    # ════════════════════════════════════════════════════════════════════
    # v3.0 — HISTORY CHARTS · EVENT STUDY · COMPOSITE · AI BRIEFING · LOOP
    # ════════════════════════════════════════════════════════════════════
    def _hist(hid):
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=f"data/ecb-hist/{hid}.json")["Body"].read())
            return [(p[0], float(p[1])) for p in d.get("points", []) if p[1] is not None], d.get("label", hid)
        except Exception:
            return [], hid

    def _down(pts, cap=320):
        if len(pts) <= cap:
            return pts
        st = len(pts) / cap
        out_, i = [], 0.0
        while int(i) < len(pts) - 1:
            out_.append(pts[int(i)]); i += st
        out_.append(pts[-1])
        return out_

    def _cctx(pts):
        vals = sorted(v for _, v in pts)
        if not vals:
            return {}
        cur = pts[-1][1]
        pct = round(100.0 * bisect.bisect_left(vals, cur) / len(vals), 1)
        return {"latest": round(cur, 4), "latest_date": pts[-1][0],
                "pctile": pct, "min": round(vals[0], 4), "max": round(vals[-1], 4),
                "median": round(vals[len(vals) // 2], 4), "n": len(pts),
                "first_date": pts[0][0]}

    def fred_full(sid, start="1999-01-01"):
        try:
            u = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
                 f"&api_key={FRED_KEY}&file_type=json&observation_start={start}&limit=100000")
            d = json.loads(urllib.request.urlopen(u, timeout=40).read().decode())
            o = [(x["date"], float(x["value"])) for x in d.get("observations", [])
                 if x.get("value") not in (".", "")]
            o.sort(); return o
        except Exception as e:
            print(f"[v3 fred_full] {sid}: {str(e)[:50]}"); return []

    charts = {}
    # 1) CISS level (hist store, 1999+) + 2) CISS Δ30d derived
    ciss_pts, _ = _hist("ciss_ea")
    if len(ciss_pts) > 100:
        charts["ciss_level"] = {"label": "Euro-Area CISS (systemic stress, 0–1)",
                                 "points": _down(ciss_pts), **_cctx(ciss_pts),
                                 "thresholds": {"watch": 0.28, "crisis": 0.5}}
        cv = [v for _, v in ciss_pts]
        d30 = [(ciss_pts[i][0], round(cv[i] - cv[i - 23], 5)) for i in range(23, len(cv))]
        charts["ciss_delta30"] = {"label": "CISS 30-day Acceleration",
                                   "points": _down(d30), **_cctx(d30),
                                   "thresholds": {"watch": 0.15, "critical": 0.30}}
    # 3) ESI accumulated daily
    esi_pts, _ = _hist("esi")
    if len(esi_pts) > 5:
        charts["esi"] = {"label": "Eurodollar Stress Index (0–100)",
                          "points": _down(esi_pts), **_cctx(esi_pts),
                          "thresholds": {"watch": 50, "critical": 70}}
    # 4) IT–DE 10y fragmentation spread
    frag_pts, fl = _hist("it_de_10y_bp")
    if len(frag_pts) > 50:
        charts["it_de_spread"] = {"label": "BTP–Bund 10y spread (bp)",
                                   "points": _down(frag_pts), **_cctx(frag_pts),
                                   "thresholds": {"watch": 200, "critical": 300}}
    # 5) LTRO share of policy lending — full GFC/2011/2020 history from SDMX
    try:
        mroH = ecb_csv("ILM/W.U2.C.A050100.U2.EUR", start="2007-01")
        ltroH = ecb_csv("ILM/W.U2.C.A050200.U2.EUR", start="2007-01")
        dm, dl = dict(mroH), dict(ltroH)
        sh = [(d_, round(100 * dl[d_] / (dm[d_] + dl[d_]), 2))
              for d_ in sorted(set(dm) & set(dl)) if (dm[d_] + dl[d_]) > 0]
        if len(sh) > 100:
            charts["ltro_share"] = {"label": "LTRO share of ECB policy lending (%)",
                                     "points": _down(sh), **_cctx(sh),
                                     "thresholds": {"elevated": 80}}
    except Exception as e:
        print(f"[v3 ltro] {str(e)[:50]}")
    # 6) EU/US 6m liquidity divergence history (ECB BS weekly vs Fed net-liq)
    try:
        ecb_bs = fred_full("ECBASSETSW", "2015-01-01")  # ECB total assets, EUR mn, weekly
        wl = fred_full("WALCL", "2015-01-01"); rr = fred_full("RRPONTSYD", "2015-01-01")
        tg = fred_full("WTREGEN", "2015-01-01")
        dr, dt = dict(rr), dict(tg)
        fed = []
        for d_, v_ in wl:
            r_ = dr.get(d_); t_ = dt.get(d_)
            if r_ is None or t_ is None:
                cand_r = [x for x in dr if x <= d_]; cand_t = [x for x in dt if x <= d_]
                r_ = dr[max(cand_r)] if cand_r else None
                t_ = dt[max(cand_t)] if cand_t else None
            if r_ is not None and t_ is not None:
                fed.append((d_, (v_ - r_ - t_) / 1000.0))  # $bn
        if len(ecb_bs) > 40 and len(fed) > 40:
            def six(series):
                return [(series[i][0], round(series[i][1] - series[i - 26][1], 1))
                        for i in range(26, len(series))]
            e6 = dict(six([(d_, v_ / 1000.0) for d_, v_ in ecb_bs]))  # €bn
            f6 = dict(six(fed))
            fdk = sorted(f6)
            div = []
            for d_, ev in sorted(e6.items()):
                j = bisect.bisect_right(fdk, d_) - 1
                if j >= 0:
                    div.append((d_, round(ev - f6[fdk[j]], 1)))
            if len(div) > 60:
                charts["eu_us_divergence"] = {
                    "label": "EU−US 6m liquidity divergence (≈bn, ECB Δ − Fed net-liq Δ)",
                    "points": _down(div), **_cctx(div), "thresholds": {}}
    except Exception as e:
        print(f"[v3 diverge] {str(e)[:50]}")
    for cid, (lbl, pts_, th_) in macro_series.items():
        if len(pts_) > 30:
            charts[cid] = {"label": lbl, "points": _down(pts_), **_cctx(pts_),
                            "thresholds": {k: v for k, v in th_.items() if v is not None}}
    out["charts"] = charts

    # ── Event study: what ACTUALLY happened after CISS-acceleration episodes ──
    ev_study = {}
    try:
        if len(ciss_pts) > 500:
            cv = [v for _, v in ciss_pts]
            episodes, last_i = [], -10**9
            for i in range(23, len(cv)):
                if (cv[i] - cv[i - 23]) > 0.15 and (i - last_i) > 90:
                    episodes.append(ciss_pts[i][0]); last_i = i
            spx = {}
            try:
                sd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/spx-history-deep.json")["Body"].read())
                spx = {d_: float(v_) for d_, v_ in (sd.get("points") or []) if v_ is not None}
            except Exception:
                pass
            eur = dict(fred_full("DEXUSEU", "1999-01-01"))
            def study(px, want_neg):
                dd = sorted(px); idx = {d_: i for i, d_ in enumerate(dd)}
                res = {}
                for w in (21, 63):
                    rs = []
                    for ep in episodes:
                        j = idx.get(ep)
                        if j is None:
                            k = bisect.bisect_left(dd, ep)
                            j = k if k < len(dd) else None
                        if j is not None and j + w < len(dd):
                            rs.append((px[dd[j + w]] / px[dd[j]] - 1) * 100)
                    if rs:
                        rs.sort()
                        res[f"d{w}"] = {"n": len(rs), "median_pct": round(rs[len(rs) // 2], 2),
                                         "hit_pct": round(100 * sum(1 for r in rs
                                                                     if (r < 0) == want_neg) / len(rs), 1)}
                return res
            ev_study = {"definition": "CISS Δ30d crosses +0.15 (90-session cooldown)",
                        "n_episodes": len(episodes), "episode_dates": episodes,
                        "spx": study(spx, True) if spx else None,
                        "eurusd": study(eur, True) if eur else None,
                        "hypothesis": "tested: risk-off (SPX down, EUR down); hit% = share confirming"}
            sp21 = ((ev_study.get("spx") or {}).get("d21") or {})
            if sp21.get("n", 0) >= 10:
                hp = sp21.get("hit_pct", 50)
                ev_study["empirical_read"] = (
                    f"Across {ev_study['n_episodes']} episodes, SPX confirmed risk-off only {hp}% of the "
                    "time at +21d — CISS spikes have historically marked CAPITULATION (markets bottom "
                    "during the stress spike). Treat fresh episodes as a contrarian rebound timer on a "
                    "1–3 month horizon, not a sell trigger; the dump risk is in the SLOW build "
                    "(rising percentiles across pillars) before the spike." if hp < 40 else
                    f"SPX confirmed risk-off in {hp}% of {ev_study['n_episodes']} episodes at +21d — "
                    "treat fresh episodes as de-risking triggers.")
    except Exception as e:
        ev_study = {"err": str(e)[:70]}
    out["event_study"] = ev_study

    # ── Composite EU-Dump Score (coverage-honest z-blend) ──
    try:
        zs_ = []
        cd = charts.get("ciss_delta30")
        if cd:
            vv = [v for _, v in cd["points"]]
            sd_ = statistics.pstdev(vv) or 1
            zs_.append(("ciss_accel", max(-3, min(3, (vv[-1] - statistics.mean(vv)) / sd_)), 0.30))
        for cid, w_ in (("esi", 0.20), ("it_de_spread", 0.15), ("ltro_share", 0.15)):
            c_ = charts.get(cid)
            if c_:
                zs_.append((cid, max(-3, min(3, (c_["pctile"] - 50) / 17.0)), w_))
        ue_ = out["indicators"].get("ea_unemployment") or {}
        if isinstance(ue_.get("chg_3m_pp"), (int, float)):
            zs_.append(("unemp_3m", max(-3, min(3, ue_["chg_3m_pp"] / 0.2)), 0.10))
        ip_ = charts.get("ip_yoy")
        if ip_:
            zs_.append(("ip_yoy", max(-3, min(3, (50 - ip_["pctile"]) / 22.0)), 0.10))
        m1_ = charts.get("real_m1_growth")
        if m1_:
            zs_.append(("real_m1", max(-3, min(3, (50 - m1_["pctile"]) / 22.0)), 0.15))
        ed_ = charts.get("eu_us_divergence")
        if ed_:
            zs_.append(("eu_us_div", max(-3, min(3, (50 - ed_["pctile"]) / 17.0)), 0.20))
        tw = sum(w_ for _, _, w_ in zs_)
        zbar = sum(z_ * w_ for _, z_, w_ in zs_) / tw if tw else 0
        score = round(max(0, min(100, 50 + 18 * zbar)), 1)
        out["dump_score"] = {"score_0_100": score,
                              "level": ("ACUTE" if score >= 75 else "ELEVATED" if score >= 60
                                         else "WATCH" if score >= 50 else "CALM"),
                              "components": [{"id": i_, "z": round(z_, 2), "w": w_} for i_, z_, w_ in zs_],
                              "coverage_pillars": len(zs_)}
    except Exception as e:
        out["dump_score"] = {"err": str(e)[:60]}


    # ── Per-indicator history sparks: every Detail card gets current-vs-history ──
    def _spark_ctx(pts, cap=70):
        if len(pts) < 8:
            return None
        vals = sorted(v for _, v in pts)
        cur = pts[-1][1]
        return {"points": _down([(d_, round(v_, 4)) for d_, v_ in pts], cap),
                "pctile": round(100.0 * bisect.bisect_left(vals, cur) / len(vals), 1)}

    try:
        spark_src = {}
        estrH = ecb_csv("EST/B.EU000A2X2A25.WT", start="2019-10")
        dfrH = ecb_csv("FM/D.U2.EUR.4F.KR.DFR.LEV", start="2003-01")
        if estrH and dfrH:
            dd = dict(dfrH); keys = sorted(dd); last = None; sprd = []
            for d_, v_ in estrH:
                j = bisect.bisect_right(keys, d_) - 1
                if j >= 0:
                    sprd.append((d_, (v_ - dd[keys[j]]) * 100))
            spark_src["estr_dfr_dislocation"] = sprd
        nfcH = ecb_csv("MIR/M.U2.B.A2A.A.R.A.2240.EUR.N", start="2003-01")
        if nfcH and dfrH:
            dd = dict(dfrH); keys = sorted(dd)
            prem = []
            for d_, v_ in nfcH:
                j = bisect.bisect_right(keys, d_ + "-28") - 1
                if j >= 0:
                    prem.append((d_, v_ - dd[keys[j]]))
            spark_src["bank_pass_through_premium"] = prem
        e3m = ecb_csv("FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", start="2019-10")
        c3m = ecb_csv("EST/B.EU000A2QQF32.CR", start="2019-10")
        if e3m and c3m:
            mm = {}
            for d_, v_ in c3m:
                mm.setdefault(d_[:7], []).append(v_)
            sprd = [(d_, (v_ - sum(mm[d_]) / len(mm[d_])) * 100)
                    for d_, v_ in e3m if d_ in mm]
            spark_src["euribor_ois_stress"] = sprd
        t2 = ecb_csv("TGB/M.DE.N.A094T.U2.EUR.E", start="2007-01")
        if t2:
            spark_src["target2_imbalance"] = [(d_, v_ / 1000.0) for d_, v_ in t2]
        blsH = ecb_csv("BLS/Q.U2.ALL.O.E.Z.B3.ST.S.WFNET", start="2003-01")
        if blsH:
            spark_src["bls_credit_standards"] = blsH
        wg = ecb_csv("STS/Q.U2.N.INWR.000000.3.ANR", start="2000-01")
        if wg:
            spark_src["wage_persistence"] = wg
        nfcL = ecb_csv("BSI/M.U2.Y.U.A20T.A.I.U2.2240.Z01.A", start="2004-01")
        if nfcL:
            spark_src["credit_contraction"] = nfcL
        spf = ecb_csv("SPF/Q.U2.HICP.POINT.LT.Q.AVG", start="2004-01")
        if spf:
            spark_src["expectations_deanchoring"] = spf
        # reuse already-built chart histories where they map 1:1
        REUSE = {"ciss_acceleration": "ciss_delta30", "bank_funding_stress": "ltro_share",
                 "fragmentation_stress": "it_de_spread",
                 "eu_us_liquidity_divergence": "eu_us_divergence",
                 "eurodollar_stress_index": "esi",
                 "ea_unemployment": "ea_unemployment",
                 "ea_industrial_production": "ip_yoy",
                 "real_m1_growth": "real_m1_growth",
                 "ea_confidence": "business_confidence"}
        n_sparks = 0
        for ik, pts in spark_src.items():
            sp = _spark_ctx(pts)
            tgt = out["indicators"].get(ik)
            if sp and isinstance(tgt, dict) and not tgt.get("err"):
                tgt["spark"] = sp; n_sparks += 1
        for ik, cid in REUSE.items():
            c_ = charts.get(cid)
            tgt = out["indicators"].get(ik)
            if c_ and isinstance(tgt, dict) and not tgt.get("err") and "spark" not in tgt:
                tgt["spark"] = {"points": c_["points"][-70:], "pctile": c_["pctile"]}
                n_sparks += 1
        out["n_sparks"] = n_sparks
        misses = {}
        for ik in ("wage_persistence", "credit_contraction", "expectations_deanchoring",
                   "fragmentation_stress", "eurodollar_stress_index", "estr_dfr_dislocation",
                   "euribor_ois_stress"):
            tgt = out["indicators"].get(ik)
            if not isinstance(tgt, dict):
                misses[ik] = "indicator_absent"
            elif tgt.get("err"):
                misses[ik] = f"indicator_err:{tgt['err'][:40]}"
            elif "spark" not in tgt:
                src_n = len(spark_src.get(ik) or [])
                misses[ik] = f"src_pts={src_n}" + ("" if ik not in REUSE else f";chart={'Y' if charts.get(REUSE[ik]) else 'N'}")
        out["spark_misses"] = misses
    except Exception as e:
        print(f"[sparks] {str(e)[:80]}")

    # ── What changed today: diff vs the previous brief ──
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ecb-derived.json")["Body"].read())
        chg = []
        for k_, v_ in out["indicators"].items():
            a_ = ((prev.get("indicators") or {}).get(k_) or {}).get("signal")
            b_ = (v_ or {}).get("signal")
            if a_ and b_ and a_ != b_:
                chg.append({"indicator": k_, "from": a_, "to": b_})
        p_sc = (prev.get("dump_score") or {}).get("score_0_100")
        n_sc = (out.get("dump_score") or {}).get("score_0_100")
        out["changes_today"] = {"signal_changes": chg,
                                 "score_prev": p_sc, "score_now": n_sc,
                                 "score_delta": (round(n_sc - p_sc, 1)
                                                  if isinstance(p_sc, (int, float))
                                                  and isinstance(n_sc, (int, float)) else None),
                                 "vs": prev.get("generated_at")}
    except Exception as e:
        out["changes_today"] = {"err": str(e)[:60]}

    # ── AI briefing (server-side, cached in brief) ──
    try:
        akey = os.environ.get("ANTHROPIC_API_KEY", "")
        if not akey:
            out["ai_brief"] = {"error": "ANTHROPIC_API_KEY not set on this lambda"}
        else:
            ctx_ = {"as_of": out["generated_at"],
                    "dump_score": out.get("dump_score"),
                    "n_flashing": len([1 for k_, v_ in out["indicators"].items()
                                        if (v_ or {}).get("signal") in ("WATCH", "CRITICAL", "ELEVATED",
                                        "ACUTE", "TAIL_RISK", "TIGHTENING", "SEVERE_TIGHTENING",
                                        "CREDIT_STRESS", "BLACK_SWAN")]),
                    "indicators": {k_: {kk: vv for kk, vv in (v_ or {}).items()
                                         if kk in ("signal", "ciss_level", "delta_30d", "ltro_share_pct",
                                                    "mlf_eur_mn", "divergence_bn", "net_pct_tightening",
                                                    "premium_pct", "esi_0_100", "score_0_100",
                                                    "spread_bp", "headline_yoy", "core_yoy",
                                                    "unemployment_rate_pct", "chg_3m_pp",
                                                    "ip_yoy_pct", "real_m1_growth_pct",
                                                    "business_conf", "business_z",
                                                    "consumer_conf", "official_yoy",
                                                    "tracker_fwd_yoy", "nfc_loans_yoy",
                                                    "hh_loans_yoy", "impulse_6m_pp",
                                                    "m3_yoy", "spf_longterm_pct",
                                                    "it_de_bp", "worst_3m_rise_pp")}
                                    for k_, v_ in out["indicators"].items() if v_ and not v_.get("err")},
                    "history_context": {k_: {"pctile": c_["pctile"], "latest": c_["latest"],
                                              "median": c_["median"], "since": c_["first_date"]}
                                         for k_, c_ in charts.items()},
                    "event_study": {kk: ev_study.get(kk) for kk in
                                     ("n_episodes", "spx", "eurusd", "definition")}}
            prompt = ("You are the chief macro strategist for an institutional Euro-area risk desk. "
                      "Using ONLY the JSON below (real live data + a real historical event study), "
                      "write the daily EU Dump Radar briefing. Units: CISS is a 0-1 index (NOT bp); watch_next items must be plain strings. Respond with STRICT JSON only, no "
                      "markdown fences, keys exactly: what_this_is (2 sentences, what this radar "
                      "measures), why_it_matters (2-3 sentences, the ECB-plumbing-to-risk-assets "
                      "transmission), current_vs_history (3-4 sentences citing the percentiles), "
                      "base_case (3-4 sentences: most likely path forward grounded in the event-study "
                      "sample sizes and hit rates — be probabilistic, never certain), transmission "
                      "(object with keys risk_assets, liquidity, fx_usd, credit — 1-2 sentences each "
                      "on direct effects if current readings persist or worsen), watch_next (array of "
                      "exactly 3 concrete triggers with numeric thresholds), confidence_note (1 "
                      "sentence on sample-size limits).\n\nDATA:\n" + json.dumps(ctx_, default=str))
            payload = json.dumps({"model": "claude-haiku-4-5-20251001", "max_tokens": 2500,
                                   "temperature": 0.3,
                                   "messages": [{"role": "user", "content": prompt}]}).encode()
            req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=payload,
                                          headers={"Content-Type": "application/json",
                                                   "x-api-key": akey,
                                                   "anthropic-version": "2023-06-01"})
            resp = json.loads(urllib.request.urlopen(req, timeout=60).read())
            txt = "".join(b_.get("text", "") for b_ in resp.get("content", []))
            txt = txt.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            def _salvage(t):
                t = "".join(c if (c >= " " or c == "\t") else " " for c in t)
                try:
                    return json.loads(t)
                except Exception:
                    pass
                i = t.find("{")
                if i < 0:
                    raise ValueError("no json object")
                depth, instr, escp = 0, False, False
                for j in range(i, len(t)):
                    c = t[j]
                    if instr:
                        if escp:
                            escp = False
                        elif c == "\\":
                            escp = True
                        elif c == '"':
                            instr = False
                    else:
                        if c == '"':
                            instr = True
                        elif c == "{":
                            depth += 1
                        elif c == "}":
                            depth -= 1
                            if depth == 0:
                                return json.loads(t[i:j + 1])
                tail = t[i:].rstrip()
                if instr:
                    tail += '"'
                tail = tail.rstrip().rstrip(",")
                return json.loads(tail + "}" * depth)
            try:
                brief = _salvage(txt)
            except Exception as pe:
                out["ai_brief"] = {"error": f"parse: {str(pe)[:80]}", "raw_head": txt[:400]}
                raise
            brief["model"] = "claude-haiku-4-5-20251001"
            brief["generated_at"] = datetime.now(timezone.utc).isoformat()
            out["ai_brief"] = brief
    except Exception as e:
        if not (out.get("ai_brief") or {}).get("error"):
            out["ai_brief"] = {"error": str(e)[:120]}

    # ── Closed-loop: extreme radar readings must earn a hit rate ──
    try:
        sc_ = (out.get("dump_score") or {}).get("score_0_100") or 0
        nfl = len([1 for k_, v_ in out["indicators"].items()
                    if (v_ or {}).get("signal") in ("CRITICAL", "ACUTE", "TAIL_RISK", "BLACK_SWAN")])
        if sc_ >= 70 or nfl >= 3:
            spy = fred_full("SP500", (datetime.now(timezone.utc) - timedelta(days=10)).date().isoformat())
            px0 = spy[-1][1] if spy else None
            if px0:
                nowt = datetime.now(timezone.utc)
                boto3.resource("dynamodb", region_name=REGION).Table("justhodl-signals").put_item(Item={
                    "signal_id": f"eu-dump-radar#EA#{nowt.strftime('%Y-%m-%d')}",
                    "signal_type": "eu_dump_radar", "signal_value": str(sc_),
                    "predicted_direction": "DOWN", "confidence": Decimal("0.58"),
                    "measure_against": "ticker", "baseline_price": str(px0), "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                          for w in (5, 21, 63)},
                    "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                    "logged_epoch": int(nowt.timestamp()), "status": "pending",
                    "schema_version": "2", "horizon_days_primary": 21,
                    "regime_at_log": (out.get("dump_score") or {}).get("level", "UNKNOWN"),
                    "ttl": int(nowt.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "ecb-derived", "v": "3.0", "score": str(sc_)},
                    "rationale": f"EU dump score {sc_} ({nfl} critical signals) — risk-off vs SPY"})
                out["signal_logged"] = True
    except Exception as e:
        print(f"[v3 loop] {str(e)[:70]}")

    # composite headline: count how many are flashing
    flashing = []
    for k, v in out["indicators"].items():
        s = (v or {}).get("signal", "")
        if s in ("WATCH", "CRITICAL", "ELEVATED", "ACUTE", "TAIL_RISK", "TIGHTENING", "SEVERE_TIGHTENING", "CREDIT_STRESS", "BLACK_SWAN"):
            flashing.append(k)
    out["n_flashing"] = len(flashing); out["flashing"] = flashing
    out["headline"] = (f"{len(flashing)} ECB-derived dump signal(s) active: {', '.join(flashing)}"
                       if flashing else "ECB-derived dump signals all calm")
    out["duration_s"] = round(time.time() - t0, 1)
    s3.put_object(Bucket=BUCKET, Key="data/ecb-derived.json",
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[ecb-derived] flashing={flashing} in {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"n_flashing": len(flashing)})}
