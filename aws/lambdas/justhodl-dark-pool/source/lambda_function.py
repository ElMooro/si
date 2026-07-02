"""
justhodl-dark-pool — PER-NAME DARK-POOL / OFF-EXCHANGE ACCUMULATION (FINRA ATS transparency)
═══════════════════════════════════════════════════════════════════════════════════════════
Institutions execute large blocks OFF the lit tape — in ATS dark pools and via
wholesaler/internalizer (non-ATS OTC) flow — to avoid moving price. A rising
off-exchange share of a name's volume, especially while price stays flat, is
quiet accumulation that often PRECEDES the move. FINRA publishes this free,
weekly, by security (~2-3wk lag). This is distinct from justhodl-dix (market-
level SqueezeMetrics DIX) — it is per-name.

DATA (FINRA OTC Transparency, free, no auth)
  api.finra.org/data/group/otcMarket/name/weeklySummary
    summaryTypeCode ATS_W_SMBL  → ATS (true dark-pool) weekly shares per symbol
    summaryTypeCode OTC_W_SMBL  → non-ATS off-exchange (wholesaler/internalizer) per symbol
  Polygon grouped daily aggs → total consolidated weekly volume (for the %).

PER NAME (latest reported week):
  • ats_shares, offex_shares (ATS+OTC), dark_pool_pct = ATS/total, offex_pct = offex/total
  • dark_accel = latest ATS vs trailing-4wk average (rising = building)
  • price action over the week → ACCUMULATION (dark rising + price flat/up) vs
    DISTRIBUTION (dark rising + price down) vs NEUTRAL
  • dark_accumulation_score 0-100

top_picks = ACCUMULATION names (quiet build, price not yet moved) → signal-harvester
(eng:dark-pool) MEASURE-BEFORE-TRUST forward-excess-vs-SPY grading. Also feeds
justhodl-ignition's P4 dark-share dimension (previously dead — FINRA filter bug).
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/dark-pool.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", region_name=REGION)


def _get_json(key, default=None):
    """v2: S3 JSON reader (v1 fetched FINRA/Polygon directly and had none)."""
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default
FINRA = "https://api.finra.org/data/group/otcMarket/name/weeklySummary"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
MIN_WEEKLY_VOL = 2_000_000   # liquidity gate (shares/week)


def finra_post(body):
    try:
        req = urllib.request.Request(FINRA, data=json.dumps(body).encode(),
                                     headers={**UA, "Content-Type": "application/json",
                                              "Accept": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=40) as r:
            j = json.loads(r.read())
            return j if isinstance(j, list) else (j.get("data") or j.get("results") or [])
    except Exception as e:
        print(f"[dark-pool] FINRA fail: {str(e)[:120]}")
        return []


def poly_get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=30) as r:
            return json.loads(r.read())
    except Exception:
        return None


def fetch_equity_set():
    """Polygon reference → set of real common stocks + ADRs (CS, ADRC). Excludes ETFs/ETNs/
    funds/warrants/units, which structurally have huge off-exchange % (not alpha-relevant)."""
    eq = set()
    for typ in ("CS", "ADRC"):
        url = (f"https://api.polygon.io/v3/reference/tickers?type={typ}&market=stocks"
               f"&active=true&limit=1000&apiKey={POLY}")
        for _ in range(8):
            j = poly_get(url)
            for r in (j or {}).get("results") or []:
                t = r.get("ticker")
                if t:
                    eq.add(t.upper())
            nxt = (j or {}).get("next_url")
            if not nxt:
                break
            url = nxt + f"&apiKey={POLY}"
    return eq


def fetch_offexchange(weeks_back=45):
    """Pull ATS_W_SMBL + OTC_W_SMBL for the last ~6 weeks; index {sym: {week: {ats, otc}}}."""
    end = date.today()
    start = end - timedelta(days=weeks_back)
    out = {}
    for code, key in (("ATS_W_SMBL", "ats"), ("OTC_W_SMBL", "otc")):
        offset = 0
        for _ in range(8):
            rows = finra_post({
                "limit": 5000, "offset": offset,
                "compareFilters": [{"fieldName": "summaryTypeCode", "compareType": "EQUAL", "fieldValue": code}],
                "dateRangeFilters": [{"fieldName": "weekStartDate", "startDate": start.isoformat(), "endDate": end.isoformat()}],
            })
            if not rows:
                break
            for r in rows:
                sym = (r.get("issueSymbolIdentifier") or "").upper().strip()
                wk = r.get("weekStartDate")
                qty = r.get("totalWeeklyShareQuantity")
                if not sym or not wk or qty is None:
                    continue
                d = out.setdefault(sym, {}).setdefault(wk, {"ats": 0.0, "otc": 0.0})
                d[key] += float(qty)
            if len(rows) < 5000:
                break
            offset += 5000
    return out


def week_trading_days(week_start):
    d0 = date.fromisoformat(week_start)
    return [(d0 + timedelta(days=i)).isoformat() for i in range(5)]  # Mon-Fri


def fetch_total_volume(days):
    """Polygon grouped daily → {sym: total volume} summed across the week's trading days."""
    vol = {}
    closes = {}
    for ds in days:
        j = poly_get(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{ds}?adjusted=true&apiKey={POLY}")
        for r in (j or {}).get("results") or []:
            t = r.get("T")
            if not t:
                continue
            vol[t] = vol.get(t, 0.0) + (r.get("v") or 0.0)
            closes.setdefault(t, []).append((ds, r.get("c")))
    return vol, closes


def lambda_handler(event=None, context=None):
    t0 = time.time()
    offex = fetch_offexchange()
    if not offex:
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps({"engine": "justhodl-dark-pool", "ok": False,
                                       "error": "FINRA weeklySummary empty",
                                       "generated_at": datetime.now(timezone.utc).isoformat()}).encode(),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False})}

    all_weeks = sorted({w for sym in offex for w in offex[sym]})
    latest_week = all_weeks[-1]
    prior_weeks = all_weeks[-5:-1]   # up to 4 prior weeks
    vol, closes = fetch_total_volume(week_trading_days(latest_week))
    equity_set = fetch_equity_set()   # real common stocks + ADRs only (drop ETFs/funds)
    print(f"[dark-pool] equity_set={len(equity_set)} symbols")

    rows = []
    for sym, wk in offex.items():
        if equity_set and sym not in equity_set:
            continue   # exclude ETFs/ETNs/funds — structural off-exchange %, not alpha
        lw = wk.get(latest_week)
        if not lw:
            continue
        ats = lw["ats"]; otc = lw["otc"]; off = ats + otc
        tvol = vol.get(sym)
        if not tvol or tvol < MIN_WEEKLY_VOL:
            continue
        # trailing ATS average (prior weeks present)
        prior_ats = [wk[w]["ats"] for w in prior_weeks if w in wk]
        avg_prior = sum(prior_ats) / len(prior_ats) if prior_ats else None
        dark_accel = round(ats / avg_prior - 1, 3) if (avg_prior and avg_prior > 0) else None
        dark_pct = round(ats / tvol * 100, 2)
        offex_pct = round(off / tvol * 100, 2)
        # price action over the week
        cl = sorted(closes.get(sym, []))
        wk_ret = None
        if len(cl) >= 2 and cl[0][1]:
            wk_ret = round((cl[-1][1] / cl[0][1] - 1) * 100, 2)
        # classify
        rising = dark_accel is not None and dark_accel > 0.15
        if rising and wk_ret is not None and wk_ret >= -1.0:
            state = "ACCUMULATION"
        elif rising and wk_ret is not None and wk_ret < -1.0:
            state = "DISTRIBUTION"
        else:
            state = "NEUTRAL"
        # score 0-100
        score = 0.0
        score += 30 * min(1.0, dark_pct / 40)              # dark-pool share of volume
        score += 25 * min(1.0, offex_pct / 65)             # total off-exchange share
        if dark_accel is not None:
            score += 30 * min(1.0, max(0.0, dark_accel / 0.6))   # acceleration
        if wk_ret is not None:
            score += 15 * (1.0 if -1.0 <= wk_ret <= 6.0 else 0.3)  # quiet (not yet moved / mild up)
        score = round(score, 1)
        rows.append({"ticker": sym, "state": state, "score": score,
                     "dark_pool_pct": dark_pct, "offex_pct": offex_pct,
                     "dark_accel": dark_accel, "week_return_pct": wk_ret,
                     "ats_shares_wk": int(ats), "offex_shares_wk": int(off),
                     "total_vol_wk": int(tvol)})
    rows.sort(key=lambda r: r["score"], reverse=True)

    accumulation = [r for r in rows if r["state"] == "ACCUMULATION"]
    distribution = [r for r in rows if r["state"] == "DISTRIBUTION"]
    top_picks = [{"ticker": r["ticker"], "score": r["score"], "direction": "long",
                  "state": r["state"], "dark_pool_pct": r["dark_pool_pct"],
                  "dark_accel": r["dark_accel"], "offex_pct": r["offex_pct"],
                  "week_return_pct": r["week_return_pct"]}
                 for r in accumulation
                 if r["score"] >= 45
                 and r["week_return_pct"] is not None and -2.0 <= r["week_return_pct"] <= 8.0][:20]

    # ── dark map for justhodl-ignition (fixes its dead P4 dark-share dimension) ──
    # {ticker: latest-week ATS shares} so ignition can compute dark_to_adv.
    # ═══ v2 WEEKLY-DEGRADATION GUARD ═══
    # This exact failure was observed live: a flaked FINRA weekly pull produced an
    # all-NEUTRAL 0.0% board that would poison 7 downstream consumers. If the fresh
    # weekly side is degraded, carry the prior published board (marked) — daily
    # regsho fusion + own-DIX below still refresh on top of carried rows.
    weekly_source = "FRESH"
    _good = sum(1 for r in rows if (r.get("dark_pool_pct") or 0) > 0)
    if _good < 200:
        prev = _get_json(OUT_KEY) or {}
        prows = prev.get("board") or []
        if sum(1 for r in prows if (r.get("dark_pool_pct") or 0) > 0) >= 200:
            print("[guard] weekly ATS degraded (%d good) — carrying prior board (%d rows, week %s)" % (
                _good, len(prows), prev.get("latest_week")))
            rows = prows
            latest_week = prev.get("latest_week", latest_week)
            accumulation = [r for r in rows if r.get("state") == "ACCUMULATION"]
            distribution = [r for r in rows if r.get("state") == "DISTRIBUTION"]
            top_picks = prev.get("top_picks") or top_picks
            weekly_source = "CARRIED_STALE"

    # ═══ v2 LAYER D — DAILY regsho fusion (finra-short: short + TOTAL TRF volume) ═══
    # Weekly ATS lags 2-3wk; the daily off-exchange tape is already owned. Fusing gives a
    # daily pulse per name (short%% of off-exchange = sell-side pressure; LOW = DIX-style
    # buying) and our OWN market-level DIX proxy, independent of SqueezeMetrics.
    fs = _get_json("data/finra-short.json") or {}
    fh = _get_json("data/finra-short-history.json") or {}
    def _fs_rows(doc):
        for cand in ("rows","tickers","data","board"):
            v=doc.get(cand)
            if isinstance(v,list) and v and isinstance(v[0],dict): return v
            if isinstance(v,dict): return [{"ticker":k,**x} for k,x in v.items() if isinstance(x,dict)]
        return [{"ticker":k,**x} for k,x in doc.items() if isinstance(x,dict) and ("short_vol" in x or "short_volume" in x)]
    daily={}
    import statistics as _st
    # universe-wide z computed from finra-short's rolling history store
    # ({"tickers":{SYM:[series]}}; entries may be floats or {svr:..} dicts).
    def _svr_series(rec):
        if isinstance(rec,list):
            out=[]
            for x in rec:
                if isinstance(x,(int,float)): out.append(float(x))
                elif isinstance(x,dict):
                    v=x.get("svr") if isinstance(x.get("svr"),(int,float)) else x.get("svr_pct")
                    if isinstance(v,(int,float)): out.append(float(v)/100.0 if v>1.5 else float(v))
            return out
        if isinstance(rec,dict):
            for k in ("svr","svr_history","series","values"):
                if isinstance(rec.get(k),list): return _svr_series(rec[k])
        return []
    zmap={}
    for t,rec in ((fh.get("tickers") or {}) if isinstance(fh,dict) else {}).items():
        ser=_svr_series(rec)[-40:]
        if len(ser)>=15 and _st.pstdev(ser):
            zmap[t.upper()]=(_st.fmean(ser),_st.pstdev(ser))
    print("[v2] history z-map names:",len(zmap))
    _diag={"fh_type":type(fh).__name__,"fh_tickers":len((fh.get("tickers") or {}) if isinstance(fh,dict) else {}),"zmap_n":len(zmap)}
    for r0 in _fs_rows(fs):
        t=(r0.get("ticker") or r0.get("symbol") or "").upper()
        sv=r0.get("short_vol") or r0.get("short_volume"); tv=r0.get("total_vol") or r0.get("total_volume")
        svr=r0.get("svr"); z=r0.get("z_score") if isinstance(r0.get("z_score"),(int,float)) else None
        if t and isinstance(tv,(int,float)) and tv>0:
            pct=round(100*svr,1) if isinstance(svr,(int,float)) and svr<=1.5 else (round(100*sv/tv,1) if isinstance(sv,(int,float)) else None)
            if pct is not None:
                if z is None and t in zmap:
                    mu,sd=zmap[t]
                    z=round((pct/100.0-mu)/sd,2)
                daily[t]={"short_pct":pct,"off_exch_vol":tv,"short_z":z}
    joined=0
    for r in rows:
        d=daily.get(r["ticker"])
        if d:
            r["daily_short_pct"]=d["short_pct"]; r["daily_short_z"]=d.get("short_z")
            r["daily_off_exch_vol"]=d["off_exch_vol"]; joined+=1
            if d.get("short_z") is not None:
                if r.get("state")=="ACCUMULATION" and d["short_z"]<-0.5: r["conviction"]="HIGH"
                if (r.get("price_5d_pct") or 0)>2 and d["short_z"]>0.8: r["flag"]="DISTRIBUTION_INTO_STRENGTH"
    _diag["daily_n"]=len(daily); _diag["daily_z_n"]=sum(1 for v in daily.values() if v.get("short_z") is not None)
    _diag["joined_all"]=joined
    _diag["z_all"]=sum(1 for r in rows if r.get("daily_short_z") is not None)
    hi_all=[r["ticker"] for r in rows if r.get("conviction")=="HIGH"]
    dis_all=[r["ticker"] for r in rows if r.get("flag")=="DISTRIBUTION_INTO_STRENGTH"]
    print("[v2] daily regsho joined %d/%d names"%(joined,len(rows)),_diag)

    wsum=vsum=0.0
    for t,d in daily.items():
        w=d["off_exch_vol"]; wsum+=w*(100-d["short_pct"]); vsum+=w
    own_dix=round(wsum/vsum,2) if vsum else None
    dixdoc=_get_json("data/dix.json") or {}
    sq_dix=None
    for cand in (dixdoc.get("dix"),dixdoc.get("current"),
                 (dixdoc.get("current") or {}).get("dix") if isinstance(dixdoc.get("current"),dict) else None,
                 (dixdoc.get("dix") or {}).get("value") if isinstance(dixdoc.get("dix"),dict) else None):
        if isinstance(cand,(int,float)): sq_dix=cand; break
    own_hist=_get_json("data/history/dark-pool-dix.json") or {}
    _today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if own_dix is not None:
        own_hist[_today]=own_dix
        own_hist=dict(sorted(own_hist.items())[-500:])
        S3.put_object(Bucket=BUCKET,Key="data/history/dark-pool-dix.json",
                      Body=json.dumps(own_hist,separators=(",",":")).encode(),ContentType="application/json")
    ser=[v for v in own_hist.values()]
    own_dix_z=round((own_dix-_st.fmean(ser))/_st.pstdev(ser),2) if own_dix is not None and len(ser)>=20 and _st.pstdev(ser) else None
    dix_block={"own_dix_pct":own_dix,"own_dix_z":own_dix_z,"squeezemetrics_dix":sq_dix,
               "read":("BUYING PRESSURE" if own_dix and own_dix>=57 else "SELLING PRESSURE" if own_dix and own_dix<52 else "NEUTRAL") if own_dix else "N/A",
               "history":[{"date":k,"value":v} for k,v in sorted(own_hist.items())][-260:],
               "method":"$vol-weighted (1 - short%%) across daily FINRA regsho TRF tape (%d names) — own DIX-style proxy, no third-party dependency"%len(daily)}

    # ═══ v2.4 LAYER M — FINRA MONTHLY (FIRM×SYMBOL: wholesaler concentration) ═══
    # Discovery (ops 2719): the monthly dataset is firm-level (OTC_M_SMBL_FIRM /
    # ATS_M_SMBL_FIRM) with totalMonthlyShareQuantity + totalNotionalSum per
    # market participant per symbol — i.e. WHO internalizes each name and how
    # concentrated. Aggregated per symbol into share/top-firm/concentration.
    monthly={"status":"UNAVAILABLE"}
    spec=_get_json("data/config/finra-monthly-spec.json") or {}
    if spec.get("codes"):
        try:
            qf=spec.get("qty_field","totalMonthlyShareQuantity")
            nf=spec.get("notional_field","totalNotionalSum")
            df=spec.get("date_field","monthStartDate")
            sf=spec.get("sym_field","issueSymbolIdentifier")
            ff=spec.get("firm_field","marketParticipantName")
            want=sorted({r0["ticker"] for r0 in rows})
            agg={}; mo=None
            murl=FINRA.replace("weeklySummary","monthlySummary")
            for code in spec["codes"]:
                for i0 in range(0,len(want),200):
                    chunk=want[i0:i0+200]
                    mb={"limit":12000,
                        "compareFilters":[{"compareType":"EQUAL","fieldName":"summaryTypeCode","fieldValue":code}],
                        "domainFilters":[{"fieldName":sf,"values":chunk}]}
                    mreq=urllib.request.Request(murl,data=json.dumps(mb).encode(),
                          headers={**UA,"Content-Type":"application/json","Accept":"application/json"})
                    with urllib.request.urlopen(mreq,timeout=45) as r:
                        mrows=json.loads(r.read())
                    for m0 in (mrows if isinstance(mrows,list) else []):
                        t=(m0.get(sf) or "").upper(); q=m0.get(qf)
                        if not t or q is None: continue
                        mo=mo or m0.get(df)
                        e=agg.setdefault(t,{"sh":0,"ntl":0.0,"firms":{}})
                        e["sh"]+=q; e["ntl"]+=(m0.get(nf) or 0)
                        fm=m0.get(ff) or "?"
                        e["firms"][fm]=e["firms"].get(fm,0)+q
                    time.sleep(0.25)
            smap={}
            for t,e in agg.items():
                if not e["sh"]: continue
                topf,topq=max(e["firms"].items(),key=lambda kv:kv[1])
                smap[t]={"sh":e["sh"],"ntl":round(e["ntl"]/1e6,1),
                         "top_firm":topf[:28],"top_pct":round(100*topq/e["sh"],1),
                         "n_firms":len(e["firms"])}
            monthly={"status":"OK","month":mo,"joined":len(smap),"share_map":smap,
                     "codes":spec["codes"],
                     "note":"firm-level internalization: per-name monthly off-exch shares, $ notional, top wholesaler + concentration"}
        except Exception as e:
            monthly={"status":"ERR","err":str(e)[:110],"spec":spec}
    # ═══ v2.3 QUIVER OFF-EXCHANGE — gated enrichment (flips on when token exists) ═══
    qcfg=_get_json("data/config/quiver-offexchange.json") or {}
    if qcfg.get("enabled"):
        try:
            import boto3 as _b3
            _tok=_b3.client("ssm",region_name=REGION).get_parameter(
                Name="/justhodl/quiver/token",WithDecryption=True)["Parameter"]["Value"]
            qreq=urllib.request.Request("https://api.quiverquant.com/beta/live/offexchange",
                  headers={**UA,"Authorization":"Bearer "+_tok,"Accept":"application/json"})
            with urllib.request.urlopen(qreq,timeout=25) as r:
                qrows=json.loads(r.read())
            qmap={}
            for q0 in (qrows if isinstance(qrows,list) else [])[:8000]:
                t=(q0.get("Ticker") or q0.get("ticker") or "").upper()
                v=q0.get("DPI") or q0.get("Dpi") or q0.get("dpi")
                if t and v is not None: qmap[t]=v
            joinedq=0
            for r0 in rows:
                if r0["ticker"] in qmap: r0["qv_dpi"]=qmap[r0["ticker"]]; joinedq+=1
            monthly["quiver_dpi_joined"]=joinedq
        except Exception as e:
            qcfg={"enabled":True,"err":str(e)[:80]}
    print("[v2] monthly:",monthly.get("status"),"| own_dix:",own_dix,"vs sq:",sq_dix)

    # ═══ v2.4 LAYER Q — Quiver off-exchange DPI (env-keyed; definitive entitlement) ═══
    quiver={"status":"NO_TOKEN"}
    qtok=next((v for k,v in os.environ.items() if "QUIVER" in k.upper() and v),None)
    if qtok:
        qrows=None; code=None
        for auth in ("Bearer "+qtok,"Token "+qtok):
            try:
                qreq=urllib.request.Request("https://api.quiverquant.com/beta/live/offexchange",
                                            headers={**UA,"Authorization":auth,"Accept":"application/json"})
                with urllib.request.urlopen(qreq,timeout=25) as r:
                    qrows=json.loads(r.read()); break
            except urllib.error.HTTPError as he:
                code=he.code
            except Exception as e:
                code=str(e)[:40]
        if isinstance(qrows,list) and qrows:
            dpi={}
            for r0 in qrows:
                t=(r0.get("Ticker") or r0.get("ticker") or "").upper()
                v=r0.get("DPI") if r0.get("DPI") is not None else r0.get("Dpi")
                if t and isinstance(v,(int,float)): dpi[t]=round(v,4)
            quiver={"status":"OK","n":len(dpi),
                    "top_dpi":dict(sorted(dpi.items(),key=lambda kv:-kv[1])[:30])}
            for r in rows:
                if r["ticker"] in dpi: r["quiver_dpi"]=dpi[r["ticker"]]
        else:
            quiver={"status":"NOT_ENTITLED" if code in (401,402,403) else "ERROR","code":code}
    print("[v2.4] quiver:",quiver.get("status"),quiver.get("n") or quiver.get("code") or "")

    dark_map = {r["ticker"]: r["ats_shares_wk"] for r in rows}
    xray_map = {r["ticker"]: {"dp": r.get("dark_pool_pct"), "acc": r.get("dark_accel"),
                              "st": r.get("state"), "sz": r.get("daily_short_z"),
                              "cv": r.get("conviction"), "fl": r.get("flag"),
                              "dv": r.get("daily_off_exch_vol")} for r in rows}

    payload = {
        "engine": "justhodl-dark-pool", "version": "2.4.1", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Per-name off-exchange accumulation from FINRA ATS transparency. Rising "
                   "dark-pool share of volume while price stays flat = quiet institutional "
                   "build that often precedes the move. Distinct from market-level DIX."),
        "latest_week": latest_week, "weekly_source": weekly_source, "n_scored": len(rows),
        "distribution": {"accumulation": len(accumulation), "distribution": len(distribution)},
        "board": rows[:60],
        "top_picks": top_picks,
        "top_accumulation": accumulation[:20],
        "top_distribution": distribution[:12],
        "dark_map": dark_map,
        "xray_map": xray_map,
        "daily_fusion": {"joined": joined, "of": len(rows), "z_all": _diag["z_all"],
                         "source": "finra-short daily regsho (short + total TRF vol) + rolling-history z (11.9k names)"},
        "high_conviction": hi_all[:40], "distribution_into_strength": dis_all[:40],
        "dix": dix_block, "monthly_ats": monthly, "quiver": quiver,
        "data_source": "FINRA OTC Transparency weeklySummary (ATS+OTC) + Polygon grouped daily volume",
        "caveats": [
            "FINRA ATS/OTC transparency lags ~2-3 weeks (Tier 1 NMS weekly); this is a "
            "positioning/accumulation read, not intraday.",
            "Off-exchange % rising + price flat = accumulation inference; pairing with price "
            "disambiguates accumulation from off-exchange distribution.",
            "MEASURE-BEFORE-TRUST: top_picks → signal-harvester (eng:dark-pool) graded forward "
            "vs SPY; NOT in decision engines until alpha-proven. Also feeds ignition P4.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[dark-pool] week={latest_week} scored={len(rows)} accum={len(accumulation)} "
          f"picks={len(top_picks)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "latest_week": latest_week, "n_scored": len(rows),
        "n_accumulation": len(accumulation), "n_picks": len(top_picks),
        "top": [(r["ticker"], r["score"], r["state"], r["dark_pool_pct"], r["dark_accel"]) for r in rows[:8]], "diag": _diag})}
