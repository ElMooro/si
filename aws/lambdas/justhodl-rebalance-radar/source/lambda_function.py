"""justhodl-rebalance-radar — quarter/month-end institutional flow regime.

Three institutional layers:
 1. EVENT STUDY (measured, not lore): mean/median/hit-rate returns at business-
    day offsets T-5..T+5 around every quarter-end over ~10y, per asset proxy
    (SPY QQQ SMH IWM TLT AGG GLD BTCUSD) from FMP full history. Cached 30d.
 2. LIVE WINDOW FORENSICS: mechanical-rebalance pressure model (QTD outperf vs
    SPY -> expected trim/add) vs OBSERVED 5d complex flows (capital-flow-radar)
    -> classifies each move as MECHANICAL_CONFIRMED / EXCESS_ROTATION.
 3. ROTATION_RISK flag: leadership complex (top QTD) seeing real outflows+price
    weakness while crypto leg accelerates inside the window -> regime-risk
    evidence list (the exact pattern Khalid observed at Q2-end).
Output data/rebalance-radar.json + board "Rebalance Window" + page.
"""
import json, urllib.request
from datetime import datetime, timezone, timedelta, date
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/rebalance-radar.json"
ES_KEY = "data/history/rebalance-eventstudy.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
PROXIES = {"SPY":"S&P 500","QQQ":"Nasdaq","SMH":"Semiconductors/AI","IWM":"Small Caps",
           "TLT":"Long Treasuries","AGG":"Agg Bonds","GLD":"Gold","BTCUSD":"Bitcoin"}
COMPLEX_PROXY = {"Semiconductors":"SMH","Nasdaq":"QQQ","Technology":"QQQ","S&P":"SPY",
                 "Small":"IWM","Bond":"AGG","Treasur":"TLT","Gold":"GLD","Crypto":"BTCUSD",
                 "Bitcoin":"BTCUSD","Energy":"XLE","Financ":"XLF"}
s3 = boto3.client("s3", region_name="us-east-1")

def _get(url,timeout=30):
    req=urllib.request.Request(url,headers={"User-Agent":"jh/1"})
    with urllib.request.urlopen(req,timeout=timeout) as r: return json.loads(r.read())

def _s3json(k,d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET,Key=k)["Body"].read())
    except Exception: return d

def _hist(sym, days=2900):
    # FMP full-history SILENTLY defaults to ~5y without from= -> pass 11y explicitly
    frm=(datetime.now(timezone.utc).date()-timedelta(days=4050)).isoformat()
    j=_get("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=%s&from=%s&apikey=%s"%(sym,frm,FMP))
    rows=j if isinstance(j,list) else (j.get("historical") or [])
    out=sorted([(r["date"],float(r["close"])) for r in rows if r.get("close")],key=lambda x:x[0])
    return out[-days:]

def _qends(dates):
    qs=[]; byq={}
    for d in dates:
        y,m=int(d[:4]),int(d[5:7]); q=(y,(m-1)//3)
        byq.setdefault(q,[]).append(d)
    for q in sorted(byq): qs.append(max(byq[q]))
    return qs[:-1]  # drop current (incomplete) quarter

def _event_study():
    cached=_s3json(ES_KEY)
    if cached and (datetime.now(timezone.utc)-datetime.fromisoformat(cached["computed_at"])).days<30:
        return cached
    study={"computed_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),"assets":{},"n_quarters":0}
    for sym,label in PROXIES.items():
        try: px=_hist(sym)
        except Exception as e: print("[es]",sym,str(e)[:60]); continue
        dates=[d for d,_ in px]; close={d:c for d,c in px}
        idx={d:i for i,d in enumerate(dates)}
        qe=_qends(dates); rows={o:[] for o in range(-5,6)}
        for q in qe:
            i=idx[q]
            for o in range(-5,6):
                j,k=i+o-1,i+o
                if 0<=j<len(dates) and 0<k<len(dates):
                    rows[o].append(100*(close[dates[k]]/close[dates[j]]-1))
        table=[]
        for o in range(-5,6):
            r=rows[o]
            if r:
                sr=sorted(r)
                table.append({"offset":o,"mean_pct":round(sum(r)/len(r),3),
                              "median_pct":round(sr[len(sr)//2],3),
                              "hit_up":round(100*sum(1 for x in r if x>0)/len(r),0),"n":len(r)})
        study["assets"][sym]={"label":label,"table":table}
        study["n_quarters"]=max(study["n_quarters"],len(qe))
    s3.put_object(Bucket=BUCKET,Key=ES_KEY,Body=json.dumps(study,separators=(",",":")).encode(),
                  ContentType="application/json")
    return study

def _bd_dist(a,b):
    d,n=a,0; step=timedelta(days=1)
    while d<b:
        d+=step
        if d.weekday()<5: n+=1
    return n

def lambda_handler(event=None, context=None):
    today=datetime.now(timezone.utc).date()
    y,m=today.year,today.month
    qm=((m-1)//3)*3+3
    qe=date(y,qm,1)+timedelta(days=31); qe=qe.replace(day=1)-timedelta(days=1)
    while qe.weekday()>=5: qe-=timedelta(days=1)
    prev_qe=date(y if m>3 else y-1, qm-3 if qm>3 else 12,1)+timedelta(days=31)
    prev_qe=prev_qe.replace(day=1)-timedelta(days=1)
    while prev_qe.weekday()>=5: prev_qe-=timedelta(days=1)
    dist_next=_bd_dist(today,qe) if today<qe else 0
    dist_prev=_bd_dist(prev_qe,today)
    in_window=(today<=qe and dist_next<=5) or (today>prev_qe and dist_prev<=3) or today in(qe,prev_qe)
    anchor_qe = prev_qe if (today>prev_qe and dist_prev<=3) else qe
    cal={"today":today.isoformat(),"quarter_end":qe.isoformat(),"prev_quarter_end":prev_qe.isoformat(),
         "bdays_to_qtr_end":dist_next,"bdays_since_prev_qtr_end":dist_prev,
         "in_rebalance_window":in_window,"window_anchor":anchor_qe.isoformat(),
         "window_def":"T-5..T+3 business days around quarter-end"}

    study=_event_study()

    q_start=date(anchor_qe.year,((anchor_qe.month-1)//3)*3+1,1)
    qtd={}
    for sym in ("SPY","QQQ","SMH","IWM","TLT","AGG","GLD","BTCUSD","XLE","XLF"):
        try:
            px=_hist(sym,140)
            base=next((c for d,c in px if d>=q_start.isoformat()),None)
            if base: qtd[sym]={"qtd_pct":round(100*(px[-1][1]/base-1),1),
                               "d5_pct":round(100*(px[-1][1]/px[-6][1]-1),1) if len(px)>6 else None}
        except Exception as e: print("[qtd]",sym,str(e)[:50])
    spy_qtd=(qtd.get("SPY") or {}).get("qtd_pct") or 0

    radar=_s3json("data/capital-flow-radar.json",{}) or {}
    cx=radar.get("complexes") or []
    ranked=[]
    for c in cx:
        nm=c.get("complex") or c.get("name") or "?"
        f5=c.get("net_flow_5d_usd")
        if not isinstance(f5,(int,float)): continue
        proxy=next((v for k,v in COMPLEX_PROXY.items() if k.lower() in nm.lower()),None)
        rel=(qtd.get(proxy) or {}).get("qtd_pct")
        mech=None
        if rel is not None:
            gap=rel-spy_qtd
            mech="TRIM" if gap>4 else "ADD" if gap<-4 else "NEUTRAL"
        cls=None
        if mech=="TRIM" and f5<0: cls="MECHANICAL_CONFIRMED_SELL"
        elif mech=="ADD" and f5>0: cls="MECHANICAL_CONFIRMED_BUY"
        elif mech=="TRIM" and f5>0: cls="EXCESS_BUYING_INTO_STRENGTH"
        elif mech=="ADD" and f5<0: cls="EXCESS_SELLING_INTO_WEAKNESS"
        ranked.append({"complex":nm,"net_flow_5d_usd":round(f5,0),
                       "price_5d_pct":c.get("price_5d_pct"),"proxy":proxy,
                       "qtd_vs_spy_pp":round(rel-spy_qtd,1) if rel is not None else None,
                       "mechanical_expectation":mech,"classification":cls})
    ranked.sort(key=lambda r:r["net_flow_5d_usd"])
    outflows=ranked[:8]; inflows=ranked[-8:][::-1]

    def _leg(keys): 
        return round(sum(r["net_flow_5d_usd"] for r in ranked
                     if any(k.lower() in r["complex"].lower() for k in keys)),0)
    legs={"ai_semis_5d_usd":_leg(["Semiconductor","Technology","Nasdaq","AI"]),
          "bonds_5d_usd":_leg(["Bond","Treasur","Credit","Fixed"]),
          "crypto_5d_usd":_leg(["Crypto","Bitcoin","Ethereum","Digital"]),
          "gold_5d_usd":_leg(["Gold","Precious"])}

    lead=[r for r in ranked if (r["qtd_vs_spy_pp"] or 0)>6]
    lead_out=[r for r in lead if r["net_flow_5d_usd"]<-2e8 and (r["price_5d_pct"] or 0)<-1]
    crypto_in=legs["crypto_5d_usd"]>1.5e8
    rotation_risk=bool(in_window and lead_out and crypto_in)
    sev="HIGH" if rotation_risk and legs["ai_semis_5d_usd"]<-1e9 else "ELEVATED" if rotation_risk else "NONE"
    evidence=[]
    if in_window: evidence.append("Inside %s window (anchor %s)"%(cal["window_def"],anchor_qe))
    for r in lead_out[:3]:
        evidence.append("%s: QTD %+.0fpp vs SPY, 5d flow $%.1fB, px %+.1f%% -> %s"%(
            r["complex"],r["qtd_vs_spy_pp"],r["net_flow_5d_usd"]/1e9,r["price_5d_pct"] or 0,r["classification"]))
    evidence.append("Crypto leg 5d: $%+.2fB %s"%(legs["crypto_5d_usd"]/1e9,"(accelerating)" if crypto_in else ""))
    spy_tbl={r["offset"]:r for r in (study["assets"].get("SPY") or {}).get("table",[])}
    post=[spy_tbl[o]["mean_pct"] for o in (1,2,3) if o in spy_tbl]
    hist_note=("Event study (%d quarters): SPY mean T+1..T+3 after quarter-end = %+.2f%% cum, "
               "i.e. month-end pressure typically REVERSES early in the new quarter."%(
               study["n_quarters"],sum(post))) if post else ""

    doc={"engine":"justhodl-rebalance-radar","version":"1.0.0",
         "generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "calendar":cal,"event_study":study,
         "qtd_proxies":qtd,
         "window_forensics":{"top_outflows":outflows,"top_inflows":inflows,
                             "cross_asset_legs":legs,"n_complexes":len(ranked)},
         "rotation_risk":{"flag":rotation_risk,"severity":sev,"evidence":evidence,
                          "read":("LEADERSHIP DE-RISKING INTO CRYPTO inside the rebalance window — "
                                  "treat as regime-risk until flows normalize post-window." if rotation_risk
                                  else "No leadership-rotation signature in the current window."),
                          "historical_context":hist_note},
         "method":("Layer 1: measured T-5..T+5 event study, %d quarters, cached 30d. Layer 2: "
                   "mechanical pressure = QTD outperf vs SPY (>|4pp|) vs observed radar 5d flows -> "
                   "MECHANICAL vs EXCESS classification. Layer 3: rotation-risk = in-window + "
                   "leadership (QTD>+6pp) real outflow>$200M w/ price<-1%% + crypto leg>+$150M."%study["n_quarters"])}
    s3.put_object(Bucket=BUCKET,Key=OUT,Body=json.dumps(doc,separators=(",",":")).encode(),
                  ContentType="application/json",CacheControl="public, max-age=1800")
    print("[rebal] window=%s risk=%s ai=$%.1fB crypto=$%.2fB quarters=%d"%(
        in_window,sev,legs["ai_semis_5d_usd"]/1e9,legs["crypto_5d_usd"]/1e9,study["n_quarters"]))
    return {"ok":True,"in_window":in_window,"rotation_risk":rotation_risk,"severity":sev}
