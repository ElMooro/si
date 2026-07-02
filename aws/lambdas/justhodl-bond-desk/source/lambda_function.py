"""justhodl-bond-desk — the fixed-income FLOW & CREDIT-APPETITE desk.

The fleet already measures bond STRESS deeply (bond-vol v2 10-channel, auction
crisis, fails, ACM). What was missing is the FLOW side — where fixed-income
money is actually moving (duration ladder, credit risk vs safety, TIPS,
equity->bond rotation) and the credit MICRO (CCC-vs-BB, junk-within-junk) —
synthesized into one anxiety read that tells the equity book what bonds are
pricing. Sources: OWNED feeds (etf-fund-flows per-ticker, bond-vol, auction-
crisis, settlement-fails, term-premium) + 2 new FRED OAS series.
Output data/bond-desk.json. Consumers: signal-board "Bond Desk", bond-desk.html.
"""
import json, urllib.request, statistics as st
from datetime import datetime, timezone
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

def _ticker_map(doc):
    """Tolerant per-ticker extractor from etf-fund-flows feed."""
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

def lambda_handler(event=None, context=None):
    flows_doc = _s3json("data/etf-fund-flows.json", {}) or {}
    tk = _ticker_map(flows_doc)
    print("[desk] tickers in flows feed:", len(tk))

    B={}
    for name,ts in BUCKETS.items():
        rows=[tk[t] for t in ts if t in tk]
        f5=sum(r.get("flow_5d_usd") or 0 for r in rows)
        f21=sum(r.get("flow_21d_usd") or 0 for r in rows)
        zs=[r["flow_zscore_90d"] for r in rows if isinstance(r.get("flow_zscore_90d"),(int,float))]
        B[name]={"flow_5d_usd":round(f5,0),"flow_21d_usd":round(f21,0),
                 "avg_z90":round(st.fmean(zs),2) if zs else None,"n":len(rows)}
    matched=sum(b["n"] for b in B.values())

    dur_num=B["long_gov"]["flow_5d_usd"]+0.5*B["belly_gov"]["flow_5d_usd"]-B["front_gov"]["flow_5d_usd"]
    dur_den=sum(abs(B[k]["flow_5d_usd"]) for k in("long_gov","belly_gov","front_gov")) or 1
    duration_tilt=round(dur_num/dur_den,3)  # +1 extending, -1 hiding in bills
    risk_credit=B["hy_credit"]["flow_5d_usd"]+B["loans"]["flow_5d_usd"]+B["em_debt"]["flow_5d_usd"]
    safe_gov=B["front_gov"]["flow_5d_usd"]+B["belly_gov"]["flow_5d_usd"]+B["long_gov"]["flow_5d_usd"]
    appetite_5d=round(risk_credit-safe_gov,0)
    fi_total=sum(B[k]["flow_5d_usd"] for k in B if k!="equity_core")
    eq_total=B["equity_core"]["flow_5d_usd"]
    eq_to_bond=round(fi_total-eq_total,0)
    tips_share=round(B["tips"]["flow_5d_usd"]/(abs(B["belly_gov"]["flow_5d_usd"])+abs(B["tips"]["flow_5d_usd"])+1),3)

    ccc=_fred("BAMLH0A3HYC"); bb=_fred("BAMLH0A1HYBB"); hy=_fred("BAMLH0A0HYM2",200)
    micro={"status":"UNAVAILABLE"}
    if ccc and bb:
        cb=[(d,c*100-dict(bb).get(d,0)*100) for d,c in ccc if d in dict(bb)]
        cur=cb[0][1]; hist=[v for _,v in cb]
        pct=round(100*sum(1 for v in hist if v<=cur)/len(hist),1)
        d21=round(cur-cb[21][1],1) if len(cb)>21 else None
        micro={"status":"OK","ccc_bb_bps":round(cur,1),"pctile":pct,"d21_bps":d21,
               "hy_oas_pct":hy[0][1] if hy else None,
               "hy_d5_bps":round((hy[0][1]-hy[5][1])*100,1) if len(hy)>5 else None,
               "read":"junk-within-junk "+("WIDENING" if (d21 or 0)>15 else "TIGHTENING" if (d21 or 0)<-15 else "stable")}

    bv=_s3json("data/bond-vol.json",{}) or {}
    au=_s3json("data/auction-crisis.json",{}) or {}
    sf=_s3json("data/settlement-fails.json",{}) or {}
    tp=_s3json("data/term-premium.json",{}) or {}
    xchk={"bond_vol_regime":bv.get("composite_regime") or bv.get("regime"),
          "bond_vol_pctile":bv.get("composite_percentile") or bv.get("percentile"),
          "auction_regime":au.get("regime") or (au.get("latest") or {}).get("regime"),
          "fails_pctile":(sf.get("latest") or sf).get("percentile") if sf else None,
          "acm_tp10_d21_bps":(tp.get("deltas_bps") or {}).get("d21")}

    hist=_s3json(HIST,{}) or {}
    today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    def z_own(key,val):
        ser=[v[key] for v in hist.values() if isinstance(v.get(key),(int,float))]
        if len(ser)>=40 and st.pstdev(ser): return round((val-st.fmean(ser))/st.pstdev(ser),2)
        return None
    app_z=z_own("appetite",appetite_5d) if hist else None
    eqb_z=z_own("eqbond",eq_to_bond) if hist else None
    app_zc=app_z if app_z is not None else max(-2.5,min(2.5,appetite_5d/1.5e9))
    eqb_zc=eqb_z if eqb_z is not None else max(-2.5,min(2.5,eq_to_bond/3e9))

    comps=[]
    comps.append((0.25,-app_zc))                                   # credit outflow = anxiety+
    if micro.get("d21_bps") is not None: comps.append((0.20,max(-2.5,min(2.5,micro["d21_bps"]/25))))
    flight=(1 if duration_tilt>0.15 and appetite_5d<0 else -1 if duration_tilt<-0.15 and appetite_5d>0 else 0)
    comps.append((0.15,flight*1.2))
    if isinstance(xchk["bond_vol_pctile"],(int,float)): comps.append((0.15,(xchk["bond_vol_pctile"]-50)/25))
    if isinstance(xchk["fails_pctile"],(int,float)): comps.append((0.10,(xchk["fails_pctile"]-50)/25))
    comps.append((0.15,max(-2.5,min(2.5,eqb_zc))))                 # rush into bonds from eq = anxiety+
    tw=sum(w for w,_ in comps) or 1
    anxiety=round(max(0,min(100,50+20*sum(w*z for w,z in comps)/tw)),1)
    regime=("STRESS" if anxiety>=75 else "ANXIOUS" if anxiety>=60 else
            "UNEASY" if anxiety>=45 else "CALM")

    hist[today]={"anxiety":anxiety,"appetite":appetite_5d,"eqbond":eq_to_bond}
    hist=dict(sorted(hist.items())[-500:])
    s3.put_object(Bucket=BUCKET,Key=HIST,Body=json.dumps(hist,separators=(",",":")).encode(),
                  ContentType="application/json")

    er=[]
    er.append("Credit appetite %s ($%.1fB risk-credit vs govvies 5d)"%("NEGATIVE" if appetite_5d<0 else "positive",appetite_5d/1e9))
    if micro.get("status")=="OK": er.append("CCC-BB %.0fbps (p%.0f, Δ21d %+.0f) — %s"%(micro["ccc_bb_bps"],micro["pctile"],micro["d21_bps"] or 0,micro["read"]))
    er.append("Duration tilt %+.2f (%s)"%(duration_tilt,"extending" if duration_tilt>0.15 else "hiding in bills" if duration_tilt<-0.15 else "neutral"))
    er.append("Equity→bond rotation $%.1fB/5d"%(eq_to_bond/1e9))
    equity_read=("BONDS %s FOR EQUITIES: "%("FLASH ANXIETY" if anxiety>=60 else "CALM" if anxiety<45 else "MIXED"))+" · ".join(er)

    doc={"engine":"justhodl-bond-desk","version":"1.0.0",
         "generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "anxiety_score":anxiety,"regime":regime,
         "flows":{"buckets":B,"matched_tickers":matched,
                  "duration_tilt":duration_tilt,"credit_appetite_5d_usd":appetite_5d,
                  "equity_to_bond_5d_usd":eq_to_bond,"tips_share_of_belly":tips_share,
                  "appetite_z":app_z,"eqbond_z":eqb_z,
                  "provisional":app_z is None},
         "credit_micro":micro,"stress_crosschecks":xchk,
         "equity_read":equity_read,
         "history":[{"date":k,"value":v["anxiety"]} for k,v in sorted(hist.items())][-260:],
         "method":("Duration ladder + credit-appetite + equity->bond rotation from owned "
                   "per-ticker ETF flows (%d matched); CCC-vs-BB micro from FRED; anxiety "
                   "composite cross-checked vs bond-vol/fails/auctions/ACM. Flow z self-"
                   "accumulates (activates @40 obs)."%matched)}
    s3.put_object(Bucket=BUCKET,Key=OUT,Body=json.dumps(doc,separators=(",",":")).encode(),
                  ContentType="application/json",CacheControl="public, max-age=1800")
    print("[desk] anxiety=%.0f %s app=$%.1fB dur=%+.2f eq→bond=$%.1fB ccc-bb=%s"%(
        anxiety,regime,appetite_5d/1e9,duration_tilt,eq_to_bond/1e9,micro.get("ccc_bb_bps")))
    return {"ok":True,"anxiety":anxiety,"regime":regime,"matched":matched}
