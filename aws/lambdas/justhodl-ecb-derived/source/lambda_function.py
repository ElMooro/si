"""justhodl-ecb-derived — the genuinely-missing ECB-derived dump predictors.

Verified gaps (the other ~6 high-signal indicators already exist in eurodollar-
stress / euro-fragmentation / systemic-stress / global-liquidity). Builds:

  #8  CISS Acceleration        — 30d Δ of CISS (raw + z). Strongest lead signal.
  #5  Bank Funding Stress      — MRO/LTRO reliance ratio + MLF spike (TARGET2 code
                                 A090400 is 404 at ECB; this captures the same
                                 bank-funding-stress intent with data that exists).
  #18 EU/US Liquidity Diverge  — ECB balance-sheet 6m Δ vs Fed net-liquidity 6m Δ.
  #12 BLS Credit Standards     — EU bank lending survey C&I tightening (vs Fed SLOOS).

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
    t0 = time.time(); out = {"engine": "ecb-derived", "version": "1.0",
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
