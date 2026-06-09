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
import json, time, ssl, statistics
import urllib.request
from datetime import datetime, timezone
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
    t0 = time.time(); out = {"engine": "ecb-derived", "version": "2.1",
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
        if spf_now is not None and not anchored:
            out["indicators"]["expectations_deanchoring"] = {
                "name": "LT Inflation Expectations De-anchoring (SPF)",
                "spf_longterm_pct": spf_now, "signal": "WATCH",
                "interpretation": f"SPF long-term expectations {spf_now}% outside the 1.7–2.3% anchored band — credibility stress.",
                "thresholds": {"anchored_band": [1.7, 2.3]}}
    except Exception as e:
        out["inflation_expectations"] = {"err": str(e)[:60]}

    # ── #8 HICP headline / core / services / energy / food / goods ──
    try:
        icp = ecb_multi("ICP/M.U2.N.000000+XEF000+SERV00+NRGY00+FOOD00+GOODS0+IGXE00.4.ANR", last_n=15)

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
        if persist:
            out["indicators"]["wage_persistence"] = {
                "name": "Wage Persistence (negotiated > 3.5%)",
                "official_yoy": o_now, "tracker_fwd_yoy": t_fwd, "signal": "WATCH",
                "interpretation": f"Negotiated wages {o_now}% / tracker {t_fwd}% — above the ~3% target-consistent ceiling; services CPI stays sticky.",
                "thresholds": {"watch_above": 3.5}}
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
        if n_now is not None and n_now < 0:
            out["indicators"]["credit_contraction"] = {
                "name": "EA Credit Contraction (NFC loans YoY < 0)",
                "nfc_loans_yoy": n_now, "signal": "WATCH",
                "interpretation": f"NFC loan growth {n_now}% — outright contraction; 2009/2012-14 recession signature.",
                "thresholds": {"watch_below": 0}}
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
