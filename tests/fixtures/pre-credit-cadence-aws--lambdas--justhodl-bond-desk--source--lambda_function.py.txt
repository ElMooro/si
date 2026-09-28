"""justhodl-bond-desk v3.2.1 — dated research and preserved publications.

Cohort arithmetic is accepted against original issuer evidence. All forecasts
remain unqualified, and the old regional/analog code below is retained context.
No issuer-flow difference is treated as a transfer or a validated stress vote.

Historical implementation notes (not current qualification claims):

v3 additions on top of the v2 five-region blend:
  WORLD MAP    per-country anxiety tiles for every region with real data —
               rich regions (US/EU/JP/CN-HK/EM) from owned engines; the rest
               of the developed world from FRED OECD monthly 10y yields
               (6m repricing z = duration-shock anxiety, the JGB logic
               applied everywhere). ~14 tiles, geographically arranged.
  CRISIS ANALOGS  deterministic fingerprint of TODAY vs 10 named crises
               (2008 GFC .. 2024 yen-unwind) on full-history FRED series
               (BAA10Y credit, DGS10 6m repricing, VIX pctile, 2s10s,
               dollar 63d — BAML series only serve ~3y so they can't reach
               2008). Similarity %% + measured forward returns (SPY/TLT/
               GLD/HYG/BTC at +21/63/126d) from FMP history. Cached 30d.
  AI BRIEF     llm_router tier=reason (GLM-5.1, Claude fallback) reads the
               full world state + analogs -> interpretation, crisis
               comparison, per-asset projections. Provider-outage-honest:
               ai_status reported; quant analogs carry the page regardless.


v1 read ~30% of what the fleet owns and only the US. v2 synthesizes EVERY
owned FI surface into one world view:

  US-FLOWS   ETF duration ladder + credit-appetite + equity->bond (etf-flows/
             daily.json + etf-true-flows) + ICI industry-wide weekly bond
             fund flows (mutual+ETF — the broad layer ETFs alone miss).
  US-CREDIT  owned justhodl-credit-stress ICE-BofA ladder (ccc_minus_bb,
             hy_minus_ig, bbb_minus_aaa, composite_regime) + FRED CCC-BB
             percentile/delta micro (kept for the 900-obs history + chart).
  US-STRESS  bond-vol composite percentile, auction regime, settlement-fails
             pctile, ACM term-premium impulse, NY-Fed dealer survey (opt).
  GLOBAL-FUNDING  eurodollar-plumbing: health/severity, Fed CB swap usage,
             CNH escape-valve — the world's USD-funding anxiety.
  EUROPE     euro-fragmentation score/regime + BTP-Bund spread & 1m change;
             systemic-stress composite (opt).
  JAPAN      yen-carry: JGB 10y 6m/12m change + carry-unwind stress label —
             the global duration anchor.
  EM         credit-stress em_hy_minus_us_hy differential + EM-debt ETF flows.

World anxiety 0-100 = freshness-gated, weight-renormalized regional blend
(US .40 · funding .20 · Europe .15 · Japan .15 · EM .10). Chart = CCC-BB
weekly 5y (renders day one); own anxiety history still accumulates.
Output data/bond-desk.json. Consumers: signal-board, bond-desk.html.
"""
import json, time, urllib.request, statistics as st
from datetime import datetime, timezone, timedelta
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
try:
    from llm_router import complete as _llm
except Exception:
    _llm = None

BUCKET, OUT = "justhodl-dashboard-live", "data/bond-desk.json"
HIST = "data/history/bond-desk.json"
FRED_KEY = managed_secret(('FRED_KEY', 'FRED_API_KEY'), ("/justhodl/fred/api-key",))
FMP = managed_secret(('FMP', 'FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
ANALOG_KEY = "data/history/bond-crisis-analogs.json"

# world tiles: rich regions resolved from owned engines at runtime; the rest
# from FRED OECD monthly 10y (IRLTLT01<cc>M156N) 6m repricing.
WORLD_TILES = [
 ("us","🇺🇸 United States","RICH",None),("ca","🇨🇦 Canada","FRED","IRLTLT01CAM156N"),
 ("mx","🇲🇽 Mexico","FRED","IRLTLT01MXM156N"),("gb","🇬🇧 United Kingdom","FRED","IRLTLT01GBM156N"),
 ("ez","🇪🇺 Eurozone","RICH",None),("ch","🇨🇭 Switzerland","FRED","IRLTLT01CHM156N"),
 ("se","🇸🇪 Sweden","FRED","IRLTLT01SEM156N"),("za","🇿🇦 South Africa","FRED","IRLTLT01ZAM156N"),
 ("jp","🇯🇵 Japan","RICH",None),("cn","🇨🇳 China / HK","RICH",None),
 ("kr","🇰🇷 South Korea","FRED","IRLTLT01KRM156N"),("au","🇦🇺 Australia","FRED","IRLTLT01AUM156N"),
 ("em","🌏 EM Composite","RICH",None),("us_fund","💵 USD Funding","RICH",None),
]
CRISES = [
 ("GFC / Lehman","2008-10-10"),("Eurozone crisis","2011-11-25"),
 ("Taper tantrum","2013-06-24"),("China deval","2015-08-24"),
 ("2016 credit scare","2016-02-11"),("Volmageddon Q4","2018-12-24"),
 ("COVID crash","2020-03-23"),("UK gilt / inflation shock","2022-09-28"),
 ("SVB bank stress","2023-03-13"),("Yen-carry unwind","2024-08-05"),
]
s3 = boto3.client("s3", region_name="us-east-1")

BUCKETS = {
 "front_gov":  ["SHY","BIL","SGOV","VGSH","SHV"],
 "belly_gov":  ["IEI","IEF","VGIT","GOVT"],
 "long_gov":   ["TLT","VGLT","EDV","ZROZ","SPTL"],
 "tips":       ["TIP","SCHP","VTIP","STIP","LTPZ"],
 "ig_credit":  ["LQD","VCIT","VCSH","IGSB","USIG"],
 "hy_credit":  ["HYG","JNK","SJNK","USHY","SHYG"],
 "loans":      ["BKLN","SRLN"],
 "em_debt":    ["EMB","EMLC","VWOB"],
 "aggregate":  ["AGG","BND","BNDX"],
 "muni":       ["MUB","VTEB"],
 "equity_core":["SPY","IVV","VOO","QQQ","IWM","VTI","RSP"],
}

def _s3json(k, d=None):
    try: return __import__("provider_flow_research").guard(k, json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()))
    except Exception: return d

def _fred(series, limit=900):
    # observation_start makes the URL unique -> bypasses the fleet _fred_shim
    # same-URL cache that was serving stale short payloads.
    start=(datetime.now(timezone.utc).date()-timedelta(days=int(limit*1.55))).isoformat()
    url=("https://api.stlouisfed.org/fred/series/observations?series_id=%s"
         "&api_key=%s&file_type=json&sort_order=desc&limit=%d&observation_start=%s"
         %(series,FRED_KEY,limit,start))
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            obs=json.loads(r.read()).get("observations",[])
        return [(o["date"],float(o["value"])) for o in obs if o.get("value") not in (".",None)]
    except Exception as e:
        print("[fred]",series,str(e)[:60]); return []

def _first(doc, keys, want=(int,float), depth=0):
    if depth>7: return None
    if isinstance(doc,dict):
        for k in keys:
            v=doc.get(k)
            if isinstance(v,want): return v
        for v in doc.values():
            r=_first(v,keys,want,depth+1)
            if r is not None: return r
    elif isinstance(doc,list):
        for v in doc[:60]:
            r=_first(v,keys,want,depth+1)
            if r is not None: return r
    return None

def _fresh(doc, days=7):
    ts=_first(doc,("generated_at","as_of","updated_at","as_of_date"),(str,))
    if not ts: return True
    try:
        d=datetime.fromisoformat(str(ts).replace("Z","+00:00").split("T")[0])
        return (datetime.now(timezone.utc).date()-d.date()).days<=days
    except Exception: return True

def _ticker_map(doc):
    out={}
    def visit(d):
        if isinstance(d,dict):
            if "flow_5d_usd" in d and ("symbol" in d or "ticker" in d):
                out[(d.get("symbol") or d.get("ticker")).upper()]=d
            else:
                for k,v in d.items():
                    if isinstance(v,dict) and "flow_5d_usd" in v and len(k)<=6 and k.isupper():
                        out[k]=v
                    else: visit(v)
        elif isinstance(d,list):
            for v in d[:2000]: visit(v)
    visit(doc); return out

def _sub(z): return round(max(0,min(100,50+20*max(-2.5,min(2.5,z)))),1)

def _bps(v):
    """credit-stress + EM diffs are stored as PERCENT; normalize to bps."""
    if isinstance(v,(int,float)):
        return round(v*100,1) if abs(v)<50 else round(v,1)
    return None

def _fred_hist(series, start="2006-01-01"):
    url=("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
         "&file_type=json&sort_order=asc&observation_start=%s&limit=100000"%(series,FRED_KEY,start))
    for attempt in (0,1):
        try:
            time.sleep(0.5)  # throttle: five full-history pulls back-to-back can 429
            with urllib.request.urlopen(url,timeout=25) as r:
                obs=json.loads(r.read()).get("observations",[])
            rows=[(o["date"],float(o["value"])) for o in obs if o.get("value") not in (".",None)]
            if rows: return rows
        except Exception as e:
            print("[fredh]",series,"try%d"%attempt,str(e)[:60])
        time.sleep(2.5)
    return []

def _fmp_hist(sym, frm="2007-01-01"):
    url=("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=%s&from=%s&apikey=%s"%(sym,frm,FMP))
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh/1"}),timeout=30) as r:
            j=json.loads(r.read())
        rows=j if isinstance(j,list) else (j.get("historical") or [])
        return sorted([(x["date"],float(x["close"])) for x in rows if x.get("close")])
    except Exception as e:
        print("[fmph]",sym,str(e)[:60]); return []

def _vec_at(seriesmap, date):
    """6-dim anxiety fingerprint at a date from full-history FRED series."""
    def at(nm, back=0):
        ser=seriesmap[nm]
        idx=max(0,min(len(ser)-1,next((i for i,(d,_) in enumerate(ser) if d>=date),len(ser)-1)))
        j=max(0,idx-back)
        return ser[idx][1], ser[j][1]
    try:
        baa,_=at("BAA10Y"); _,baa63=at("BAA10Y",63)
        dgs,_=at("DGS10"); _,dgs126=at("DGS10",126)
        vix,_=at("VIXCLS")
        vser=[v for d,v in seriesmap["VIXCLS"] if d<=date][-756:]
        vpct=100*sum(1 for v in vser if v<=vix)/max(1,len(vser))
        cur,_=at("T10Y2Y"); dol,_=at("DTWEXBGS"); _,dol63=at("DTWEXBGS",63)
        return [baa, baa-baa63, abs(dgs-dgs126), vpct/25, -cur, 100*(dol/dol63-1)/3]
    except Exception as e:
        print("[vec]",date,str(e)[:50]); return None

def _crisis_analogs():
    cached=_s3json(ANALOG_KEY)
    if cached and (datetime.now(timezone.utc)-datetime.fromisoformat(cached["computed_at"])).days<30:
        return cached
    t0=time.time()
    sm={nm:_fred_hist(nm) for nm in ("BAA10Y","DGS10","VIXCLS","T10Y2Y","DTWEXBGS")}
    px={sym:dict(_fmp_hist(sym)) for sym in ("SPY","TLT","GLD","HYG","BTCUSD")}
    def fwd(sym, date):
        ser=sorted(px[sym].items())
        i=next((k for k,(d,_) in enumerate(ser) if d>=date),None)
        if i is None or i>=len(ser): return {}
        base=ser[i][1]; out={}
        for lbl,n in (("d21",21),("d63",63),("d126",126)):
            if i+n<len(ser): out[lbl]=round(100*(ser[i+n][1]/base-1),1)
        return out
    lib={"computed_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),"crises":[]}
    for name,date in CRISES:
        v=_vec_at(sm,date)
        if not v: continue
        lib["crises"].append({"name":name,"date":date,"vector":[round(x,3) for x in v],
            "fwd":{sym:fwd(sym,date) for sym in px if fwd(sym,date)}})
    if len(lib["crises"])<4:
        raise RuntimeError("crisis vectors thin (%d) — FRED history pulls empty (likely 429)"%len(lib["crises"]))
    lib["norm"]=[max(0.25,st.pstdev([c["vector"][k] for c in lib["crises"]]) or 1) for k in range(6)]
    lib["elapsed_s"]=round(time.time()-t0,1)
    s3.put_object(Bucket=BUCKET,Key=ANALOG_KEY,Body=json.dumps(lib,separators=(",",":")).encode(),
                  ContentType="application/json")
    return lib

def lambda_handler(event=None, context=None):
    from bond_publication import begin as publication_begin, publish as publication_publish
    publication_snapshot=publication_begin(s3,BUCKET,datetime.now(timezone.utc).isoformat())
    # ─── US FLOWS ───
    # Preserve the provider-context read; its guard never grants issuer-flow authority.
    provider_context=_s3json("etf-flows/daily.json",{}) or {}
    from bond_flow_store import collect as collect_flow, compatibility as flow_compatibility
    flow_research=None;flow_status="unavailable"
    try:
        flow_research=collect_flow(s3,BUCKET,datetime.now(timezone.utc).isoformat())
        flow_status="dated_issuer_cohorts"
    except Exception as error:
        flow_status="withheld_"+type(error).__name__
    B=flow_compatibility(flow_research)
    matched=sum(row["n"] for row in B.values())
    # Issuance estimates do not identify transfers or validate directional models.
    duration_tilt=appetite=eq_to_bond=None

    ici=_s3json("data/ici-flows.json",{}) or {}
    ici_bond=_first(ici.get("latest") or ici,("bond",))
    ici_wk=_first(ici,("week","date","week_ending"),(str,))
    us_flows={"buckets":B,"matched_tickers":matched,"duration_tilt":duration_tilt,
              "credit_appetite_5d_usd":appetite,"equity_to_bond_5d_usd":eq_to_bond,
              "ici_bond_weekly_usd_m":ici_bond,"ici_week":ici_wk,
              "note":"Dated issuer net-issuance cohorts; partial coverage is not a complete total. ICI remains separate.",
              "flow_research":flow_research,"flow_status":flow_status,"calls_eligible":False}

    # ─── US CREDIT (owned ICE ladder + FRED micro/chart) ───
    from bond_credit_store import collect as collect_credit
    credit_research=None;credit_status="unavailable"
    try:
        credit_research=collect_credit(s3,BUCKET,datetime.now(timezone.utc).isoformat())
        credit_status="dated_native_comparisons"
    except Exception as error:
        credit_status="withheld_"+type(error).__name__
    credit_fields=(credit_research or {}).get("legacy_fields",{})
    ccc=_fred("BAMLH0A3HYC",1560); bb=_fred("BAMLH0A1HYBB",1560)
    micro={"status":"UNAVAILABLE"}; chart=[]
    if ccc and bb:
        bbm=dict(bb)
        cb=[(d,round(c*100-bbm[d]*100,1)) for d,c in ccc if d in bbm]
        cur=cb[0][1]; histv=[v for _,v in cb]
        pct=round(100*sum(1 for v in histv if v<=cur)/len(histv),1)
        d21=round(cur-cb[21][1],1) if len(cb)>21 else None
        micro={"status":"OK","ccc_bb_bps":cur,"pctile":pct,"d21_bps":d21}
    # chart: server-side WEEKLY aggregation (frequency=w) — deterministic length
    def _fred_w(series,n=330):
        start=(datetime.now(timezone.utc).date()-timedelta(days=int(n*7.6))).isoformat()
        url=("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
             "&file_type=json&sort_order=asc&frequency=w&aggregation_method=eop"
             "&observation_start=%s&limit=%d"%(series,FRED_KEY,start,n+40))
        try:
            with urllib.request.urlopen(url,timeout=20) as r:
                obs=json.loads(r.read()).get("observations",[])
            return {o["date"]:float(o["value"]) for o in obs if o.get("value") not in (".",None)}
        except Exception as e:
            print("[fredw]",series,str(e)[:60]); return {}
    cw,bw=_fred_w("BAMLH0A3HYC"),_fred_w("BAMLH0A1HYBB")
    chart=[{"date":d,"value":round(cw[d]*100-bw[d]*100,1)} for d in sorted(cw) if d in bw]
    chart_src="fred_weekly(%d)"%len(chart)
    if len(chart)<180:
        # extend from OWNED credit-stress-history archive (values stored as %, ->bps)
        csh=_s3json("data/credit-stress-history.json",{}) or {}
        rows=[]
        it=csh.items() if isinstance(csh,dict) else [( (r.get("date") or ""),r) for r in csh if isinstance(r,dict)]
        for d,r in it:
            v=_first(r,("ccc_minus_bb",)) if isinstance(r,dict) else None
            if v is not None and len(str(d))>=10:
                rows.append((str(d)[:10],round(v*100,1) if abs(v)<50 else round(v,1)))
        rows.sort()
        wk=None; hist_pts=[]
        for d,v in rows:
            key="%d-%02d"%datetime.strptime(d,"%Y-%m-%d").isocalendar()[:2]
            if key!=wk: hist_pts.append({"date":d,"value":v}); wk=key
        have={p["date"] for p in chart}
        merged=sorted(hist_pts+[p for p in chart if p["date"] not in {h["date"] for h in hist_pts}],
                      key=lambda p:p["date"])
        if len(merged)>len(chart): chart=merged; chart_src+="+cs_history(%d)"%len(hist_pts)
    print("[desk] chart source:",chart_src,"n=",len(chart))
    us_credit={"source":"Native credit research; older micro/chart context remains separately unqualified",
               "ccc_minus_bb_bps":credit_fields.get("ccc_minus_bb_bps"),
               "hy_minus_ig_bps":credit_fields.get("hy_minus_ig_bps"),
               "bbb_minus_aaa_bps":credit_fields.get("bbb_minus_aaa_bps"),
               "composite_regime":"RESEARCH_ONLY",
               "credit_research":credit_research,"credit_status":credit_status,"calls_eligible":False,
               "fred_micro":micro}
    cz=max(-2.5,min(2.5,((micro.get("d21_bps") or 0)/25)+max(0,(micro.get("pctile") or 50)-85)/10))

    # ─── US STRESS ───
    bv=_s3json("data/bond-vol.json",{}) or {}
    au=_s3json("data/auction-crisis.json",{}) or {}
    sf=_s3json("data/settlement-fails.json",{}) or {}
    tp=_s3json("data/term-premium.json",{}) or {}
    ds=_s3json("data/dealer-survey.json",{}) or {}
    us_stress={"bond_vol_regime":bv.get("composite_regime") or bv.get("regime"),
               "bond_vol_pctile":bv.get("composite_percentile") or _first(bv,("composite_percentile",)),
               "auction_regime":au.get("regime") or _first(au,("regime",),(str,)),
               "fails_pctile":_first(sf,("pctile","percentile")),
               "acm_tp10_d21_bps":(tp.get("deltas_bps") or {}).get("d21"),
               "dealer_survey":_first(ds,("stance","positioning","summary","headline","regime"),(str,))}
    sz=(((us_stress["bond_vol_pctile"] or 50)-50)/25*0.5
        +((us_stress["fails_pctile"] or 50)-50)/25*0.3
        +max(0,(us_stress["acm_tp10_d21_bps"] or 0))/30*0.2)

    US={"score":None,"flows":us_flows,"credit":us_credit,"stress":us_stress,
        "fresh":False,"status":"unqualified_flow_forecast","calls_eligible":False}

    # Missing model authority is not a neutral term-premium stress contribution.
    if tp.get('calls_eligible') is False:
        US.update(score=None, fresh=False, status='unqualified_term_premium_vote', source_replay=tp.get('replay'))

    # Source research without a qualified vote cannot become a neutral score.
    if bv.get('calls_eligible') is False:
        US.update(score=None, fresh=False, status='unqualified_bond_vol_vote', bond_vol_replay=bv.get('replay'))

    # ─── GLOBAL FUNDING (eurodollar plumbing) ───
    pl=_s3json("data/eurodollar-plumbing.json",{}) or {}
    sev=_first(pl,("severity",),(str,)) or "?"
    health=_first(pl,("plumbing_health","health","composite_score","score"))
    swaps=_first(pl,("fed_swaps","value"),(int,float))
    GF={"severity":sev,"health":health,"fed_swaps_chg_bn":swaps,
        "cnh_gap_pips":_first(pl,("gap_pips",)),
        "fresh":_fresh(pl)}
    GF["score"]=_sub({"CRITICAL":2.4,"ELEVATED":1.4,"MODERATE":0.4,
                      "COMFORTABLE":-0.5,"ABUNDANT":-0.9,"GREEN":-0.7}.get(sev,0.0)
                     +(0.6 if (swaps or 0)>10 else 0))
    # Descriptive original evidence is neither a neutral vote nor a stress forecast.
    if pl.get('calls_eligible') is False or health is None:
        GF.update(score=None, fresh=False, status='unqualified_funding_vote', source_replay=pl.get('replay'))

    # ─── EUROPE ───
    ef=_s3json("data/euro-fragmentation.json",{}) or {}
    ita=None
    def _find_italy(doc,depth=0):
        nonlocal ita
        if ita or depth>6: return
        if isinstance(doc,dict):
            if "spread_vs_bund_bp" in doc and any(
                    t in json.dumps({k:v for k,v in doc.items() if isinstance(v,str)}).lower()
                    for t in ("ital","btp","\"it\"")):
                ita=doc; return
            for v in doc.values(): _find_italy(v,depth+1)
        elif isinstance(doc,list):
            for v in doc[:40]: _find_italy(v,depth+1)
    _find_italy(ef)
    ss=_s3json("data/systemic-stress.json",{}) or {}
    frag=ef.get("fragmentation") if isinstance(ef.get("fragmentation"),dict) else ef
    EU={"fragmentation_score":_first(frag,("score","fragmentation_score","composite_score")),
        "regime":_first(frag,("regime","state"),(str,)) or _first(ef,("regime",),(str,)),
        "btp_bund_bp":_first(ita or ef,("spread_vs_bund_bp",)),
        "btp_chg_1m_bp":_first(ita or ef,("spread_change_1m_bp",)),
        "systemic_stress":_first(ss,("score","composite","level")),
        "fresh":_fresh(ef)}
    EU["score"]=_sub(((EU["fragmentation_score"] or 30)-30)/20
                     +max(0,(EU["btp_chg_1m_bp"] or 0))/25)

    # ─── JAPAN / CARRY ───
    yc=_s3json("data/yen-carry.json",{}) or {}
    JP={"jgb10_chg_6m_pp":_first(yc,("jgb_10y_chg_6m_pp","jgb_6m")),
        "jgb10_chg_12m_pp":_first(yc,("jgb_10y_chg_12m_pp",)),
        "carry_stress":_first(yc,("stress",),(str,)),
        "duration_carry_pp":_first(yc,("duration_carry_pp",)),
        "fresh":_fresh(yc)}
    JP["score"]=_sub({"RISING SHARPLY":2.0,"RISING":1.0}.get(JP["carry_stress"] or "",0)
                     +max(0,(JP["jgb10_chg_6m_pp"] or 0))*2)
    if yc.get('calls_eligible') is False or yc.get('unwind_risk_score') is None:
        JP.update(score=None, fresh=False, status='unqualified_yen_vote', source_replay=yc.get('replay'))

    # ─── EM ───
    EM={"em_hy_minus_us_hy_bps":credit_fields.get("em_hy_minus_us_hy_bps"),
        "em_debt_flow_5d_usd":B["em_debt"]["flow_5d_usd"],
        "fresh":True}
    EM.update(score=None,fresh=False,status="unqualified_flow_forecast",calls_eligible=False)

    regions={"us":US,"global_funding":GF,"europe":EU,"japan":JP,"em":EM}
    weights={"us":0.40,"global_funding":0.20,"europe":0.15,"japan":0.15,"em":0.10}
    live={k:v for k,v in regions.items() if v.get("fresh") and isinstance(v.get("score"),(int,float))}
    from required_region_composite import required_region_composite
    world, regime, hot = required_region_composite(regions, weights)

    # ─── WORLD MAP TILES ───
    tiles=[]; other_scores=[]
    rich={"us":US["score"],"ez":EU["score"],"jp":JP["score"],"em":EM["score"],"us_fund":GF["score"]}
    hk=_s3json("data/hkma.json",{}) or {}
    cn_sig=_first(hk,("peg_stress","hibor_ois_bp","stress_score")) or (GF.get("cnh_gap_pips") or 0)/40
    rich["cn"]=_sub(max(-2.5,min(2.5,(cn_sig or 0)/1.5)))
    for code,label,kind,sid in WORLD_TILES:
        if kind=="RICH":
            sc=rich.get(code)
            tiles.append({"code":code,"label":label,"score":sc,"metric":"engine composite","src":"owned"})
        else:
            ser=_fred(sid,400)  # monthly series: wide window (v3 observation_start math gave ~7 obs)
            if len(ser)>=7:
                cur=ser[0][1]; ago=ser[min(6,len(ser)-1)][1]; chg=round(cur-ago,2)
                sc=_sub(max(-2.5,min(2.5,abs(chg)*2.2+(0.4 if chg>0 else -0.2))))
                tiles.append({"code":code,"label":label,"score":sc,
                              "metric":"10y Δ6m %+.2fpp"%chg,"yield_10y":cur,"src":"FRED"})
                other_scores.append(sc)
            else:
                tiles.append({"code":code,"label":label,"score":None,"metric":"n/a","src":"FRED"})
    other_dm=round(st.fmean(other_scores),1) if other_scores else None

    # ─── CRISIS ANALOGS ───
    analogs={"status":"UNAVAILABLE"}
    try:
        lib=_crisis_analogs()
        smap={nm:_fred_hist(nm,start="2023-01-01") for nm in ("BAA10Y","DGS10","VIXCLS","T10Y2Y","DTWEXBGS")}
        smap={k:(v if v else _fred_hist(k)) for k,v in smap.items()}
        nowv=_vec_at(smap, datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if nowv and lib.get("crises"):
            norm=lib["norm"]; ranked=[]
            for c in lib["crises"]:
                d=(sum(((a-b)/n)**2 for a,b,n in zip(nowv,c["vector"],norm))/6)**0.5
                ranked.append({**{k:c[k] for k in ("name","date","fwd")},
                               "similarity_pct":round(100*max(0,1-d/2.6),0)})
            ranked.sort(key=lambda r:-r["similarity_pct"])
            analogs={"status":"OK","current_vector":[round(x,3) for x in nowv],
                     "vector_dims":["BAA10Y","ΔBAA 63d","|Δ10y| 126d","VIX pct/25","−2s10s","DXY 63d%/3"],
                     "top":ranked[:3],"all":ranked,
                     "note":"Fingerprint on full-history series (BAML OAS only serves ~3y, cannot reach 2008)."}
    except Exception as e:
        print("[analogs]",str(e)[:90]); analogs={"status":"ERROR","err":str(e)[:90]}

    publication_at=datetime.now(timezone.utc).isoformat()
    hist=dict(publication_snapshot["history"]["doc"]) if publication_snapshot["history"] else {}
    today=publication_at.split("T")[0]
    hist[today]={"anxiety":world,"appetite":appetite,"eqbond":eq_to_bond}
    hist=dict(sorted(hist.items()))  # Preserve every existing history entry.

    er=[("Hottest: %s %.0f"%(hot[0].upper().replace("_"," "),hot[1]["score"])) if hot else "Required regional votes are unqualified",
        "US credit micro CCC-BB %sbps p%s Δ21d %+.0f"%(micro.get("ccc_bb_bps"),micro.get("pctile"),micro.get("d21_bps") or 0),
        "Equity-to-bond transfer is unmeasured; dated cohort coverage is reported separately.",
        "USD funding %s · EU frag %s (BTP-Bund %s Δ1m %+dbp)"%(sev,EU.get("regime"),EU.get("btp_bund_bp"),int(EU.get("btp_chg_1m_bp") or 0)),
        "JGB anchor: %s (Δ6m %+.2fpp)"%(JP.get("carry_stress"),JP.get("jgb10_chg_6m_pp") or 0)]
    equity_read=("WAIT — required regional votes are unavailable; no world-anxiety or equity implication is qualified." if world is None else
        "GLOBAL FI %s FOR EQUITIES — "%("FLASHES ANXIETY" if world>=60 else "IS CALM" if world<45 else "IS MIXED")+" · ".join(str(x) for x in er))

    # ─── AI INTERPRETATION (tier=reason; provider-outage honest) ───
    ai_brief=None; ai_status="ROUTER_MISSING" if _llm is None else "PROVIDER_DOWN"
    if world is None:ai_status="UNQUALIFIED_REQUIRED_INPUT"
    if _llm is not None and world is not None:
        try:
            payload={"world_anxiety":world,"regime":regime,"regions":{k:{kk:vv for kk,vv in v.items() if kk!="flows" and not isinstance(vv,dict)} for k,v in regions.items()},
                     "us_credit_micro":micro,"eq_to_bond_5d_usd":eq_to_bond,
                     "jgb_shock_pp_6m":JP.get("jgb10_chg_6m_pp"),"crisis_analogs":analogs.get("top"),
                     "acm_tp_d21":us_stress.get("acm_tp10_d21_bps")}
            SYSTEM=("You are the head of fixed income at a global macro fund. Respond ONLY with a JSON object, no markdown: "
                    '{"interpretation": str (<=90 words, what global bond markets are pricing and why), '
                    '"crisis_comparison": str (<=60 words, honest read of the closest analogs incl how today DIFFERS), '
                    '"projections": {"equities": str, "duration": str, "credit": str, "gold": str, "crypto": str} (each <=25 words, conditional not certain), '
                    '"confidence": "LOW"|"MODERATE"|"HIGH", "watch": [3 short triggers]}')
            raw=_llm(json.dumps(payload,default=str), tier="reason", max_tokens=900, system=SYSTEM) or ""
            txt=raw.strip()
            if txt.startswith("```"): txt=txt.strip("`").replace("json","",1).strip()
            if txt:
                ai_brief=json.loads(txt); ai_status="LIVE"
        except Exception as e:
            print("[ai]",str(e)[:90]); ai_status="PROVIDER_DOWN"

    doc={"engine":"justhodl-bond-desk","version":"3.2.1",
         "generated_at":publication_at,
         "world_anxiety":world,"regime":regime,
         "hottest_region":{"region":hot[0],"score":hot[1]["score"]} if hot else None,
         "regions":regions,"weights":weights,"n_regions_live":len(live),
         "equity_read":equity_read,
         "world_map":tiles,"other_dm_score":other_dm,
         "crisis_analogs":analogs,"ai_brief":ai_brief,"ai_status":ai_status,
         "chart_ccc_bb":chart,
         "anxiety_history":[{"date":k,"value":v["anxiety"]} for k,v in sorted(hist.items())],
         "method":("Unqualified legacy regional research retained for review. Dated issuer cohorts use exact retained packets and independent arithmetic; no flow-derived forecast or transfer inference. Legacy model description: regional blend (US .40 flows/credit/stress · "
                   "USD-funding .20 via eurodollar-plumbing · Europe .15 via fragmentation+CISS · "
                   "Japan .15 via yen-carry/JGB · EM .10 via EM-HY differential + flows). US layer "
                   "fuses ETF ladder (%d matched, ramping), ICI industry flows, owned ICE-BofA "
                   "credit ladder, bond-vol/auctions/fails/ACM/dealer-survey. Chart = CCC-BB weekly 5y."%matched)}
    if world is None:
        doc.update(calls_eligible=False, sizing_eligible=False, execution_eligible=False,
            decision={"verb":"WAIT","meaning":"abstain"}, quality={"status":"unqualified_required_input",
            "missing_regions":[k for k in weights if k not in live]})
    doc=publication_publish(s3,BUCKET,publication_snapshot,doc,hist)
    print("[desk] world=%s %s | tiles=%d analogs=%s ai=%s"%(
        world,regime,sum(1 for t in tiles if t["score"] is not None),analogs.get("status"),ai_status))
    return {"ok":True,"world":world,"regime":regime,"regions_live":len(live),
            "tiles":sum(1 for t in tiles if t["score"] is not None),
            "analogs":analogs.get("status"),"ai":ai_status}
