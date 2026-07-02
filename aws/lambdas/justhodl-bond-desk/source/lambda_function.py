"""justhodl-bond-desk v2.0 — WORLD fixed-income flow, credit & anxiety desk.

v1 covered US ETF flows + one credit-micro pair. v2 is the full institutional
desk across FOUR REGIONS, built entirely on owned fleet feeds + FRED/ICE:

  US  — ETF duration/credit flows (etf-flows/daily + true-flows, ramping),
        FULL RATINGS LADDER (AAA..CCC OAS: level/Δ/percentile + spreads-of-
        spreads BBB−A, B−BB, CCC−B), ICI bond FUND flows (weekly, z from own
        fleet history), COT treasury spec positioning (tolerant), TIC foreign
        demand, cross-checks (bond-vol, auctions, fails, ACM Δ).
  EU  — Euro HY OAS + Euro−US HY differential, sovereign fragmentation
        (OAT−Bund, core avg, most-stressed, ECB stance) from owned engine.
  JP  — yen-carry regime + BOJ stance (owned engine).
  EM  — EM corporate/HY OAS + EM−US differential, EM debt ETF flows,
        dollar-radar regime as pressure.

WORLD anxiety = availability-renormalized regional blend (US .45 EU .22
JP .13 EM .20) + cross-region DIVERGENCE flag + ranked drivers + equity read.
Top-level anxiety_score/regime stay world-level (board-compatible).
Output data/bond-desk.json. Page bond-desk.html.
"""
import json, urllib.request, statistics as st
from datetime import datetime, timezone
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/bond-desk.json"
HIST = "data/history/bond-desk.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
s3 = boto3.client("s3", region_name="us-east-1")

BUCKETS = {
 "front_gov":["SHY","BIL","SGOV","VGSH","SHV"], "belly_gov":["IEI","IEF","VGIT","GOVT"],
 "long_gov":["TLT","VGLT","EDV","ZROZ","SPTL"], "tips":["TIP","SCHP","VTIP","STIP","LTPZ"],
 "ig_credit":["LQD","VCIT","VCSH","IGSB","USIG"], "hy_credit":["HYG","JNK","SJNK","USHY","SHYG"],
 "loans":["BKLN","SRLN"], "em_debt":["EMB","EMLC","VWOB"], "aggregate":["AGG","BND","BNDX"],
 "muni":["MUB","VTEB"], "equity_core":["SPY","IVV","VOO","QQQ","IWM","VTI","RSP"],
}
LADDER = {"AAA":"BAMLC0A1CAAA","AA":"BAMLC0A2CAA","A":"BAMLC0A3CA","BBB":"BAMLC0A4CBBB",
          "IG":"BAMLC0A0CM","BB":"BAMLH0A1HYBB","B":"BAMLH0A2HYB","CCC":"BAMLH0A3HYC","HY":"BAMLH0A0HYM2"}
GLOBAL_OAS = {"EURO_HY":"BAMLHE00EHYIOAS","EM_CORP":"BAMLEMCBPIOAS","EM_HY":"BAMLEMHBHYCRPIOAS"}

def _s3json(k,d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET,Key=k)["Body"].read())
    except Exception: return d

def _fred(series, limit=950):
    url=("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
         "&file_type=json&sort_order=desc&limit=%d"%(series,FRED_KEY,limit))
    try:
        with urllib.request.urlopen(url,timeout=20) as r:
            obs=json.loads(r.read()).get("observations",[])
        return [(o["date"],float(o["value"])) for o in obs if o.get("value") not in (".",None)]
    except Exception as e:
        print("[fred]",series,str(e)[:50]); return []

def _series_stats(ser, scale=100.0):
    """ser newest-first (date,val %) -> bps stats."""
    if not ser: return None
    v=[x*scale for _,x in ser]; cur=v[0]
    d5=round(cur-v[5],1) if len(v)>5 else None
    d21=round(cur-v[21],1) if len(v)>21 else None
    pct=round(100*sum(1 for x in v if x<=cur)/len(v),1)
    return {"bps":round(cur,1),"d5":d5,"d21":d21,"pctile":pct,"date":ser[0][0]}

def _first_num(doc, keys, depth=0):
    if depth>6: return None
    if isinstance(doc,dict):
        for k in keys:
            if isinstance(doc.get(k),(int,float)): return doc[k]
        for v in doc.values():
            r=_first_num(v,keys,depth+1)
            if r is not None: return r
    elif isinstance(doc,list):
        for v in doc[:60]:
            r=_first_num(v,keys,depth+1)
            if r is not None: return r
    return None

def _first_str(doc, keys, depth=0):
    if depth>6: return None
    if isinstance(doc,dict):
        for k in keys:
            if isinstance(doc.get(k),str): return doc[k]
        for v in doc.values():
            r=_first_str(v,keys,depth+1)
            if r is not None: return r
    elif isinstance(doc,list):
        for v in doc[:60]:
            r=_first_str(v,keys,depth+1)
            if r is not None: return r
    return None

def _clamp(z,l=2.5): return max(-l,min(l,z))

def _ticker_map():
    tk={}
    doc=_s3json("etf-flows/daily.json",{}) or {}
    for m in (doc.get("metrics") or []):
        t=(m.get("ticker") or "").upper()
        if t and not m.get("error"): tk[t]=m
    tf=_s3json("data/etf-true-flows.json",{}) or {}
    for r in (tf.get("results") or tf.get("metrics") or []):
        t=(r.get("ticker") or r.get("symbol") or "").upper()
        if t and t not in tk and isinstance(r.get("net_flow_5d_usd"),(int,float)):
            tk[t]={"flow_5d_usd":r["net_flow_5d_usd"],"flow_21d_usd":r.get("net_flow_20d_usd"),
                   "flow_zscore_90d":r.get("flow_zscore_90d")}
    return tk

def lambda_handler(event=None, context=None):
    now=datetime.now(timezone.utc)

    # ═══ US: flows ═══
    tk=_ticker_map(); print("[desk] flow tickers:",len(tk))
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
    risk_credit=B["hy_credit"]["flow_5d_usd"]+B["loans"]["flow_5d_usd"]+B["em_debt"]["flow_5d_usd"]
    safe_gov=B["front_gov"]["flow_5d_usd"]+B["belly_gov"]["flow_5d_usd"]+B["long_gov"]["flow_5d_usd"]
    appetite_5d=round(risk_credit-safe_gov,0)
    fi_total=sum(B[k]["flow_5d_usd"] for k in B if k!="equity_core")
    eq_to_bond=round(fi_total-B["equity_core"]["flow_5d_usd"],0)

    # ═══ US: full ratings ladder ═══
    ladder={}
    for rat,sid in LADDER.items():
        stt=_series_stats(_fred(sid))
        if stt: ladder[rat]=stt
    sos={}
    def _pair(a,b):
        if a in ladder and b in ladder:
            return {"bps":round(ladder[a]["bps"]-ladder[b]["bps"],1),
                    "d21":round((ladder[a]["d21"] or 0)-(ladder[b]["d21"] or 0),1)}
        return None
    for name,(a,b) in {"BBB_minus_A":("BBB","A"),"B_minus_BB":("B","BB"),
                       "CCC_minus_B":("CCC","B"),"CCC_minus_BB":("CCC","BB")}.items():
        p=_pair(a,b)
        if p: sos[name]=p

    # ═══ US: ICI bond FUND flows (weekly, z from fleet's own history) ═══
    ici_latest=_s3json("data/ici-flows.json",{}) or {}
    ici_hist=_s3json("data/history/ici-flows.json",{}) or {}
    def _bond_vals(doc):
        out=[]
        def walk(d,depth=0):
            if depth>6: return
            if isinstance(d,dict):
                for k,v in d.items():
                    if "bond" in str(k).lower() and isinstance(v,(int,float)): out.append(v)
                    else: walk(v,depth+1)
            elif isinstance(d,list):
                for v in d[:600]: walk(v,depth+1)
        walk(doc); return out
    cur_bond=_bond_vals(ici_latest)
    hist_bond=_bond_vals(ici_hist)
    ici={"status":"UNAVAILABLE"}
    if cur_bond:
        v=cur_bond[0]
        z=None
        if len(hist_bond)>=30 and st.pstdev(hist_bond):
            z=round((v-st.fmean(hist_bond))/st.pstdev(hist_bond),2)
        ici={"status":"OK","bond_flow_latest_b":round(v,2),"z":z,"hist_n":len(hist_bond),
             "note":"ICI weekly bond MUTUAL+ETF fund flows ($B) — the big-money channel beyond ETFs"}

    # ═══ US: COT treasury positioning (tolerant) + TIC foreign demand ═══
    cot_doc=_s3json("data/cftc-all-cache.json") or _s3json("data/cot-tracker.json") or {}
    cot={"status":"UNAVAILABLE"}
    try:
        rows=[]
        def cwalk(d,depth=0):
            if depth>7: return
            if isinstance(d,dict):
                nm=str(d.get("name") or d.get("market") or d.get("contract") or "")
                if any(k in nm.upper() for k in("TREASURY","T-NOTE","10-YEAR","ULTRA","2-YEAR","5-YEAR","30-YEAR","LONG BOND")):
                    rows.append(d)
                for v in d.values(): cwalk(v,depth+1)
            elif isinstance(d,list):
                for v in d[:300]: cwalk(v,depth+1)
        cwalk(cot_doc)
        zs=[_first_num(r,("z_score","net_z","zscore","z")) for r in rows]
        zs=[z for z in zs if isinstance(z,(int,float))]
        nets=[_first_num(r,("net_speculator","net_managed_money","net_position")) for r in rows]
        nets=[n for n in nets if isinstance(n,(int,float))]
        if rows:
            cot={"status":"OK","contracts_n":len(rows),
                 "avg_spec_z":round(st.fmean(zs),2) if zs else None,
                 "net_spec_sum":round(sum(nets),0) if nets else None,
                 "note":"treasury futures spec positioning across curve (fleet COT cache)"}
    except Exception as e:
        print("[cot]",str(e)[:60])
    sov=_s3json("data/sovereign-fiscal.json",{}) or {}
    tic={"status":"UNAVAILABLE"}
    th=sov.get("tic_holders") or _first_num(sov,("tic",))
    tot=_first_num(sov,("foreign_total_b","total_foreign_b","grand_total_b","total_b"))
    if th is not None or tot is not None:
        tic={"status":"OK","foreign_ust_total_b":tot,
             "as_of":_first_str(sov,("tic_as_of","as_of")),
             "note":"foreign official+private UST holdings (TIC via sovereign-fiscal)"}

    # ═══ US cross-checks ═══
    bv=_s3json("data/bond-vol.json",{}) or {}; au=_s3json("data/auction-crisis.json",{}) or {}
    sf=_s3json("data/settlement-fails.json",{}) or {}; tp=_s3json("data/term-premium.json",{}) or {}
    xchk={"bond_vol_regime":bv.get("composite_regime") or bv.get("regime"),
          "bond_vol_pctile":_first_num(bv,("composite_percentile","percentile")),
          "auction_regime":au.get("regime") or (au.get("latest") or {}).get("regime"),
          "fails_pctile":_first_num(sf,("pctile","percentile")),
          "acm_tp10_d21_bps":(tp.get("deltas_bps") or {}).get("d21")}

    # ═══ EU pillar ═══
    ef=_s3json("data/euro-fragmentation.json",{}) or {}
    euro_hy=_series_stats(_fred(GLOBAL_OAS["EURO_HY"]))
    eu={"status":"UNAVAILABLE"}
    if euro_hy or ef:
        e_us=(round(euro_hy["bps"]-ladder["HY"]["bps"],1)
              if euro_hy and "HY" in ladder else None)
        eu={"status":"OK","euro_hy":euro_hy,"euro_minus_us_hy_bps":e_us,
            "sovereign":{"core_avg_spread_bp":_first_num(ef,("core_avg_spread_bp",)),
                         "oat_bund_bp":_first_num(ef,("oat_bund_spread_bp","france_oat_bund_bp")),
                         "bund_10y_pct":_first_num(ef,("bund_benchmark_10y_pct",)),
                         "most_stressed":ef.get("most_stressed_top3"),
                         "core_stress_flag":ef.get("core_stress_flag"),
                         "ecb_stance":_first_str(ef,("ecb_stance",))}}
    # ═══ JP pillar ═══
    yc_=_s3json("data/yen-carry.json",{}) or {}
    jp={"status":"UNAVAILABLE"}
    if yc_:
        jp={"status":"OK","carry_regime":_first_str(yc_,("carry_regime","carry_conditions")),
            "boj_stance":_first_str(yc_,("boj_stance_label",)),
            "boj_injection_score":_first_num(yc_,("boj_injection_score",)),
            "carry_attractiveness":_first_num(yc_,("carry_attractiveness","carry_width"))}
    # ═══ EM pillar ═══
    em_corp=_series_stats(_fred(GLOBAL_OAS["EM_CORP"]))
    em_hy=_series_stats(_fred(GLOBAL_OAS["EM_HY"]))
    dr=_s3json("data/dollar-radar.json",{}) or {}
    em={"status":"UNAVAILABLE"}
    if em_corp or em_hy:
        em={"status":"OK","em_corp":em_corp,"em_hy":em_hy,
            "em_minus_us_hy_bps":(round(em_hy["bps"]-ladder["HY"]["bps"],1)
                                  if em_hy and "HY" in ladder else None),
            "em_etf_flow_5d_usd":B["em_debt"]["flow_5d_usd"],
            "dollar_regime":_first_str(dr,("regime","regime_note")),
            "note":"strong-dollar regimes tighten EM FI conditions"}

    # ═══ regional anxiety scores ═══
    def score(comps):
        live=[(w,_clamp(z)) for w,z in comps if z is not None]
        if not live: return None
        tw=sum(w for w,_ in live)
        return round(max(0,min(100,50+20*sum(w*z for w,z in live)/tw)),1)
    hist=_s3json(HIST,{}) or {}
    def z_own(key,val):
        ser=[v[key] for v in hist.values() if isinstance(v.get(key),(int,float))]
        return round((val-st.fmean(ser))/st.pstdev(ser),2) if len(ser)>=40 and st.pstdev(ser) else None
    app_z=z_own("appetite",appetite_5d); eqb_z=z_own("eqbond",eq_to_bond)
    app_zc=app_z if app_z is not None else _clamp(appetite_5d/1.5e9)
    eqb_zc=eqb_z if eqb_z is not None else _clamp(eq_to_bond/3e9)
    us_comps=[(0.20,-app_zc),(0.12,eqb_zc)]
    if sos.get("CCC_minus_BB"): us_comps.append((0.16,_clamp(sos["CCC_minus_BB"]["d21"]/25)))
    if sos.get("BBB_minus_A"):  us_comps.append((0.10,_clamp(sos["BBB_minus_A"]["d21"]/8)))
    if ladder.get("HY"):        us_comps.append((0.10,_clamp((ladder["HY"]["pctile"]-50)/25)))
    if ici.get("z") is not None: us_comps.append((0.12,-_clamp(ici["z"])))   # fund OUTflows = anxiety
    if isinstance(xchk["bond_vol_pctile"],(int,float)): us_comps.append((0.10,(xchk["bond_vol_pctile"]-50)/25))
    if isinstance(xchk["fails_pctile"],(int,float)):    us_comps.append((0.10,(xchk["fails_pctile"]-50)/25))
    us_anx=score(us_comps)
    eu_comps=[]
    if euro_hy: eu_comps+= [(0.4,_clamp((euro_hy["pctile"]-50)/25)),(0.25,_clamp((euro_hy["d21"] or 0)/30))]
    ca=eu.get("sovereign",{}).get("core_avg_spread_bp") if eu.get("status")=="OK" else None
    if isinstance(ca,(int,float)): eu_comps.append((0.35,_clamp((ca-35)/25)))
    eu_anx=score(eu_comps)
    jp_map={"CRISIS":2.2,"STRESS":1.6,"UNWIND":1.6,"TIGHT":1.0,"NEUTRAL":0,"SUPPORTIVE":-0.8,"ATTRACTIVE":-1.0,"WIDE":-1.0}
    jz=None
    if jp.get("status")=="OK":
        key=(jp.get("carry_regime") or "").upper()
        jz=next((v for k,v in jp_map.items() if k in key),0)
    jp_anx=score([(1.0,jz)]) if jz is not None else None
    em_comps=[]
    if em_hy: em_comps+=[(0.35,_clamp((em_hy["pctile"]-50)/25)),(0.25,_clamp((em_hy["d21"] or 0)/30))]
    elif em_corp: em_comps+=[(0.35,_clamp((em_corp["pctile"]-50)/25)),(0.25,_clamp((em_corp["d21"] or 0)/30))]
    em_comps.append((0.20,-_clamp(B["em_debt"]["flow_5d_usd"]/4e8)))
    if "STRONG" in str(em.get("dollar_regime","")).upper(): em_comps.append((0.20,1.0))
    em_anx=score(em_comps)

    regions={"US":{"anxiety":us_anx,"flows":{"buckets":B,"matched_tickers":matched,
                    "duration_tilt":duration_tilt,"credit_appetite_5d_usd":appetite_5d,
                    "equity_to_bond_5d_usd":eq_to_bond,
                    "tips_net_flow_5d_usd":B["tips"]["flow_5d_usd"],
                    "appetite_z":app_z,"eqbond_z":eqb_z,"provisional":app_z is None},
                   "ratings_ladder":ladder,"spreads_of_spreads":sos,
                   "ici_fund_flows":ici,"cot_duration":cot,"tic_foreign":tic,
                   "stress_crosschecks":xchk},
             "EU":dict(eu,anxiety=eu_anx),"JP":dict(jp,anxiety=jp_anx),"EM":dict(em,anxiety=em_anx)}
    ws=[(0.45,us_anx),(0.22,eu_anx),(0.13,jp_anx),(0.20,em_anx)]
    live=[(w,a) for w,a in ws if a is not None]
    world=round(sum(w*a for w,a in live)/sum(w for w,_ in live),1) if live else 50.0
    regime=("STRESS" if world>=75 else "ANXIOUS" if world>=60 else "UNEASY" if world>=45 else "CALM")
    avail={k:v["anxiety"] for k,v in regions.items() if v.get("anxiety") is not None}
    div=None
    if len(avail)>=2:
        hi=max(avail,key=avail.get); lo=min(avail,key=avail.get)
        if avail[hi]-avail[lo]>=18:
            div={"flag":True,"stressed":hi,"calm":lo,"gap":round(avail[hi]-avail[lo],1),
                 "note":"%s FI stressed (%.0f) while %s calm (%.0f) — regional divergence"%(hi,avail[hi],lo,avail[lo])}
    drivers=[]
    if sos.get("CCC_minus_BB"): drivers.append(("US","CCC−BB %.0fbps p%.0f Δ21 %+.0f"%(sos["CCC_minus_BB"]["bps"],ladder["CCC"]["pctile"],sos["CCC_minus_BB"]["d21"]),abs(sos["CCC_minus_BB"]["d21"]/25)))
    drivers.append(("US","equity→bond $%.1fB/5d"%(eq_to_bond/1e9),abs(eqb_zc)))
    if ici.get("z") is not None: drivers.append(("US","ICI bond funds $%.1fB (z %+.2f)"%(ici["bond_flow_latest_b"],ici["z"]),abs(ici["z"])))
    if euro_hy: drivers.append(("EU","Euro HY %.0fbps p%.0f"%(euro_hy["bps"],euro_hy["pctile"]),abs((euro_hy["pctile"]-50)/25)))
    if em_hy: drivers.append(("EM","EM HY %.0fbps p%.0f"%(em_hy["bps"],em_hy["pctile"]),abs((em_hy["pctile"]-50)/25)))
    if jz: drivers.append(("JP","carry %s"%(jp.get("carry_regime")),abs(jz)))
    drivers=sorted(drivers,key=lambda x:-x[2])[:6]
    er="WORLD BONDS %s (%.0f): "%(regime,world)+" · ".join("%s %s"%(r,t) for r,t,_ in drivers[:4])

    hist[now.strftime("%Y-%m-%d")]={"anxiety":world,"appetite":appetite_5d,"eqbond":eq_to_bond,
                                    "us":us_anx,"eu":eu_anx,"jp":jp_anx,"em":em_anx}
    hist=dict(sorted(hist.items())[-500:])
    s3.put_object(Bucket=BUCKET,Key=HIST,Body=json.dumps(hist,separators=(",",":")).encode(),ContentType="application/json")

    doc={"engine":"justhodl-bond-desk","version":"2.0.0",
         "generated_at":now.isoformat(timespec="seconds"),
         "anxiety_score":world,"regime":regime,
         "world":{"anxiety":world,"regime":regime,"weights":"US .45 EU .22 JP .13 EM .20 (renorm)",
                  "regional":avail,"divergence":div,
                  "drivers":[{"region":r,"driver":t} for r,t,_ in drivers]},
         "regions":regions,
         "flows":regions["US"]["flows"],"credit_micro":{"status":"OK","ccc_bb_bps":sos.get("CCC_minus_BB",{}).get("bps"),
              "pctile":ladder.get("CCC",{}).get("pctile"),"d21_bps":sos.get("CCC_minus_BB",{}).get("d21"),
              "hy_oas_pct":round(ladder["HY"]["bps"]/100,2) if ladder.get("HY") else None,
              "read":"junk-within-junk "+("WIDENING" if sos.get("CCC_minus_BB",{}).get("d21",0)>15 else "TIGHTENING" if sos.get("CCC_minus_BB",{}).get("d21",0)<-15 else "stable")},
         "stress_crosschecks":xchk,
         "equity_read":er,
         "history":[{"date":k,"value":v["anxiety"]} for k,v in sorted(hist.items())][-260:],
         "method":("WORLD desk v2: US = ETF duration/credit flows (ramping via true-flows expansion) + "
                   "full AAA..CCC OAS ladder + spreads-of-spreads + ICI bond fund flows (z on fleet history) "
                   "+ COT curve positioning + TIC foreign demand + owned stress engines. EU = Euro HY + "
                   "sovereign fragmentation (owned). JP = yen-carry/BOJ (owned). EM = EM corp/HY OAS + "
                   "EM ETF flows + dollar regime. World anxiety = availability-renormalized regional blend; "
                   "divergence flags cross-region splits ≥18pts.")}
    s3.put_object(Bucket=BUCKET,Key=OUT,Body=json.dumps(doc,separators=(",",":")).encode(),
                  ContentType="application/json",CacheControl="public, max-age=1800")
    print("[desk] world=%.0f %s | US=%s EU=%s JP=%s EM=%s | ladder=%d ici=%s"%(
        world,regime,us_anx,eu_anx,jp_anx,em_anx,len(ladder),ici.get("status")))
    return {"ok":True,"world":world,"regime":regime,"regions":avail,"ladder_n":len(ladder)}
