"""justhodl-bond-desk v2.0 — GLOBAL fixed-income flow, credit & anxiety desk.

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
import json, urllib.request, statistics as st
from datetime import datetime, timezone, timedelta
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/bond-desk.json"
HIST = "data/history/bond-desk.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
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
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _fred(series, limit=900):
    url=("https://api.stlouisfed.org/fred/series/observations?series_id=%s"
         "&api_key=%s&file_type=json&sort_order=desc&limit=%d"%(series,FRED_KEY,limit))
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

def lambda_handler(event=None, context=None):
    # ─── US FLOWS ───
    tk=_ticker_map(_s3json("etf-flows/daily.json",{}) or {})
    tf=_s3json("data/etf-true-flows.json",{}) or {}
    for r in (tf.get("results") or tf.get("metrics") or []):
        t=(r.get("ticker") or r.get("symbol") or "").upper()
        if t and t not in tk and isinstance(r.get("net_flow_5d_usd"),(int,float)):
            tk[t]={"flow_5d_usd":r["net_flow_5d_usd"],"flow_21d_usd":r.get("net_flow_20d_usd"),
                   "flow_zscore_90d":r.get("flow_zscore_90d")}
    B={}
    for name,ts in BUCKETS.items():
        rows=[tk[t] for t in ts if t in tk]
        zs=[r["flow_zscore_90d"] for r in rows if isinstance(r.get("flow_zscore_90d"),(int,float))]
        B[name]={"flow_5d_usd":round(sum(r.get("flow_5d_usd") or 0 for r in rows),0),
                 "flow_21d_usd":round(sum(r.get("flow_21d_usd") or 0 for r in rows),0),
                 "avg_z90":round(st.fmean(zs),2) if zs else None,"n":len(rows)}
    matched=sum(b["n"] for b in B.values())
    dur_num=B["long_gov"]["flow_5d_usd"]+0.5*B["belly_gov"]["flow_5d_usd"]-B["front_gov"]["flow_5d_usd"]
    dur_den=sum(abs(B[k]["flow_5d_usd"]) for k in("long_gov","belly_gov","front_gov")) or 1
    duration_tilt=round(dur_num/dur_den,3)
    appetite=round(B["hy_credit"]["flow_5d_usd"]+B["loans"]["flow_5d_usd"]+B["em_debt"]["flow_5d_usd"]
                   -(B["front_gov"]["flow_5d_usd"]+B["belly_gov"]["flow_5d_usd"]+B["long_gov"]["flow_5d_usd"]),0)
    eq_to_bond=round(sum(B[k]["flow_5d_usd"] for k in B if k!="equity_core")-B["equity_core"]["flow_5d_usd"],0)

    ici=_s3json("data/ici-flows.json",{}) or {}
    ici_bond=_first(ici.get("latest") or ici,("bond",))
    ici_wk=_first(ici,("week","date","week_ending"),(str,))
    us_flows={"buckets":B,"matched_tickers":matched,"duration_tilt":duration_tilt,
              "credit_appetite_5d_usd":appetite,"equity_to_bond_5d_usd":eq_to_bond,
              "ici_bond_weekly_usd_m":ici_bond,"ici_week":ici_wk,
              "note":"ETF ladder + ICI industry-wide bond fund flows (mutual+ETF)"}

    # ─── US CREDIT (owned ICE ladder + FRED micro/chart) ───
    cs=_s3json("data/credit-stress.json",{}) or {}
    ccc=_fred("BAMLH0A3HYC",1560); bb=_fred("BAMLH0A1HYBB",1560)
    micro={"status":"UNAVAILABLE"}; chart=[]
    if ccc and bb:
        bbm=dict(bb)
        cb=[(d,round(c*100-bbm[d]*100,1)) for d,c in ccc if d in bbm]
        cur=cb[0][1]; histv=[v for _,v in cb]
        pct=round(100*sum(1 for v in histv if v<=cur)/len(histv),1)
        d21=round(cur-cb[21][1],1) if len(cb)>21 else None
        micro={"status":"OK","ccc_bb_bps":cur,"pctile":pct,"d21_bps":d21}
        wk=None
        for d,v in cb[::-1][-1560:]:
            iso=datetime.strptime(d,"%Y-%m-%d").isocalendar()
            key="%d-%02d"%(iso[0],iso[1])
            if key!=wk: chart.append({"date":d,"value":v}); wk=key
    us_credit={"source":"justhodl-credit-stress (ICE BofA ladder)" if cs else "FRED",
               "ccc_minus_bb_bps":_bps(_first(cs,("ccc_minus_bb",))) or micro.get("ccc_bb_bps"),
               "hy_minus_ig_bps":_bps(_first(cs,("hy_minus_ig",))),
               "bbb_minus_aaa_bps":_bps(_first(cs,("bbb_minus_aaa",))),
               "composite_regime":_first(cs,("composite_regime","hy_regime"),(str,)),
               "fred_micro":micro}
    cz=max(-2.5,min(2.5,((micro.get("d21_bps") or 0)/25)+max(0,(micro.get("pctile") or 50)-85)/10))

    # ─── US STRESS ───
    bv=_s3json("data/bond-vol.json",{}) or {}
    au=_s3json("data/auction-crisis.json",{}) or {}
    sf=_s3json("data/settlement-fails.json",{}) or {}
    tp=_s3json("data/term-premium.json",{}) or {}
    ds=_s3json("data/dealer-survey.json",{}) or {}
    us_stress={"bond_vol_regime":bv.get("composite_regime"),
               "bond_vol_pctile":bv.get("composite_percentile") or _first(bv,("composite_percentile",)),
               "auction_regime":au.get("regime") or _first(au,("regime",),(str,)),
               "fails_pctile":_first(sf,("pctile","percentile")),
               "acm_tp10_d21_bps":(tp.get("deltas_bps") or {}).get("d21"),
               "dealer_survey":_first(ds,("stance","regime","summary"),(str,))}
    sz=(((us_stress["bond_vol_pctile"] or 50)-50)/25*0.5
        +((us_stress["fails_pctile"] or 50)-50)/25*0.3
        +max(0,(us_stress["acm_tp10_d21_bps"] or 0))/30*0.2)

    fz=max(-2.5,min(2.5,-(appetite/1.5e9)*0.5+(eq_to_bond/6e9)*0.35
                    -((ici_bond or 0)/8000)*0.15))
    US={"score":_sub(0.45*fz+0.35*cz+0.20*sz),"flows":us_flows,"credit":us_credit,"stress":us_stress,
        "fresh":True}

    # ─── GLOBAL FUNDING (eurodollar plumbing) ───
    pl=_s3json("data/eurodollar-plumbing.json",{}) or {}
    sev=_first(pl,("severity",),(str,)) or "?"
    health=_first(pl,("health","health_score"))
    swaps=_first(pl,("fed_swaps","value"),(int,float))
    GF={"severity":sev,"health":health,"fed_swaps_chg_bn":swaps,
        "cnh_gap_pips":_first(pl,("gap_pips",)),
        "fresh":_fresh(pl)}
    GF["score"]=_sub({"CRITICAL":2.4,"ELEVATED":1.4,"MODERATE":0.4,
                      "COMFORTABLE":-0.5,"ABUNDANT":-0.9,"GREEN":-0.7}.get(sev,0.0)
                     +(0.6 if (swaps or 0)>10 else 0))

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
    EU={"fragmentation_score":_first(ef,("fragmentation_score","composite_score","frag_score","score")),
        "regime":_first(ef,("regime",),(str,)),
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

    # ─── EM ───
    EM={"em_hy_minus_us_hy_bps":_bps(_first(cs,("em_hy_minus_us_hy",))),
        "em_debt_flow_5d_usd":B["em_debt"]["flow_5d_usd"],
        "fresh":True}
    EM["score"]=_sub(((EM["em_hy_minus_us_hy_bps"] or 0))/120
                     -(EM["em_debt_flow_5d_usd"]/8e8))

    regions={"us":US,"global_funding":GF,"europe":EU,"japan":JP,"em":EM}
    weights={"us":0.40,"global_funding":0.20,"europe":0.15,"japan":0.15,"em":0.10}
    live={k:v for k,v in regions.items() if v.get("fresh") and isinstance(v.get("score"),(int,float))}
    tw=sum(weights[k] for k in live) or 1
    world=round(sum(weights[k]*live[k]["score"] for k in live)/tw,1)
    regime=("STRESS" if world>=75 else "ANXIOUS" if world>=60 else "UNEASY" if world>=45 else "CALM")
    hot=max(live.items(),key=lambda kv:kv[1]["score"])

    hist=_s3json(HIST,{}) or {}
    today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today]={"anxiety":world,"appetite":appetite,"eqbond":eq_to_bond}
    hist=dict(sorted(hist.items())[-500:])
    s3.put_object(Bucket=BUCKET,Key=HIST,Body=json.dumps(hist,separators=(",",":")).encode(),
                  ContentType="application/json")

    er=["Hottest: %s %.0f"%(hot[0].upper().replace("_"," "),hot[1]["score"]),
        "US credit micro CCC-BB %sbps p%s Δ21d %+.0f"%(micro.get("ccc_bb_bps"),micro.get("pctile"),micro.get("d21_bps") or 0),
        "Eq→bond $%.1fB/5d · ICI bond $%sM/wk"%(eq_to_bond/1e9,ici_bond),
        "USD funding %s · EU frag %s (BTP-Bund %s Δ1m %+dbp)"%(sev,EU.get("regime"),EU.get("btp_bund_bp"),int(EU.get("btp_chg_1m_bp") or 0)),
        "JGB anchor: %s (Δ6m %+.2fpp)"%(JP.get("carry_stress"),JP.get("jgb10_chg_6m_pp") or 0)]
    equity_read="GLOBAL FI %s FOR EQUITIES — "%("FLASHES ANXIETY" if world>=60 else "IS CALM" if world<45 else "IS MIXED")+" · ".join(str(x) for x in er)

    doc={"engine":"justhodl-bond-desk","version":"2.0.0",
         "generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "world_anxiety":world,"regime":regime,
         "hottest_region":{"region":hot[0],"score":hot[1]["score"]},
         "regions":regions,"weights":weights,"n_regions_live":len(live),
         "equity_read":equity_read,
         "chart_ccc_bb":chart[-300:],
         "anxiety_history":[{"date":k,"value":v["anxiety"]} for k,v in sorted(hist.items())][-260:],
         "method":("World anxiety = freshness-gated regional blend (US .40 flows/credit/stress · "
                   "USD-funding .20 via eurodollar-plumbing · Europe .15 via fragmentation+CISS · "
                   "Japan .15 via yen-carry/JGB · EM .10 via EM-HY differential + flows). US layer "
                   "fuses ETF ladder (%d matched, ramping), ICI industry flows, owned ICE-BofA "
                   "credit ladder, bond-vol/auctions/fails/ACM/dealer-survey. Chart = CCC-BB weekly 5y."%matched)}
    s3.put_object(Bucket=BUCKET,Key=OUT,Body=json.dumps(doc,separators=(",",":")).encode(),
                  ContentType="application/json",CacheControl="public, max-age=1800")
    print("[desk] world=%.0f %s | US %.0f GF %.0f EU %.0f JP %.0f EM %.0f | live %d/5"%(
        world,regime,US["score"],GF["score"],EU["score"],JP["score"],EM["score"],len(live)))
    return {"ok":True,"world":world,"regime":regime,"regions_live":len(live)}
