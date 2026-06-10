"""
justhodl-crisis-canaries v1.0 — Funding-Plumbing Early Warning
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
VERSION = "2.0.1"


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
           "known_gaps": ["FHLB discount-note issuance has no free machine-readable feed (KHALID_ACTIONS)"],
           "methodology": ("Coverage-honest composite over live funding canaries: SOFR 99th-pct tail z, "
                           "H.4.1 discount-window WoW z, H.8 small-bank deposit outflow z, auction-composite "
                           "slope (self-bootstrapping 3-obs min), ALFRED payroll first-release-vs-latest "
                           "revision signature, OFR repo volume z. Score≥70 logs crisis_canary DOWN vs SPY "
                           "to the closed loop.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[canaries] score={score} level={level} avail={avail} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"score": score, "level": level})}
