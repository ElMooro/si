
import json,boto3,uuid,time,urllib.request
from datetime import datetime,timezone,timedelta
from decimal import Decimal

dynamodb=boto3.resource("dynamodb",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1")
SIGNALS_TABLE="justhodl-signals"
S3_BUCKET="justhodl-dashboard-live"
CFTC_URL="https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/"

def f2d(obj):
    if isinstance(obj,float): return Decimal(str(round(obj,6)))
    if isinstance(obj,dict): return {k:f2d(v) for k,v in obj.items()}
    if isinstance(obj,list): return [f2d(v) for v in obj]
    return obj

def fs3(key):
    try:
        obj=s3.get_object(Bucket=S3_BUCKET,Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}"); return {}

def lget(url,path=""):
    try:
        full=url.rstrip("/")+("/"+path.lstrip("/") if path else "")
        req=urllib.request.Request(full,headers={"Content-Type":"application/json"})
        with urllib.request.urlopen(req,timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"[LAMBDA] {e}"); return {}

def log_sig(stype,val,pred,conf,against,windows,price=None,meta=None,bench=None):
    table=dynamodb.Table(SIGNALS_TABLE)
    now=datetime.now(timezone.utc)
    sid=str(uuid.uuid4())
    ts={f"day_{d}":(now+timedelta(days=d)).isoformat() for d in windows}
    item={"signal_id":sid,"signal_type":stype,"signal_value":str(val),
          "predicted_direction":pred,"confidence":f2d(float(conf)),
          "measure_against":against,"baseline_price":f2d(float(price)) if price else None,
          "benchmark":bench,"check_windows":[str(d) for d in windows],
          "check_timestamps":ts,"outcomes":{},"accuracy_scores":{},
          "logged_at":now.isoformat(),"logged_epoch":int(now.timestamp()),
          "status":"pending","metadata":f2d(meta or {}),
          "ttl":int((now+timedelta(days=365)).timestamp())}
    table.put_item(Item=item)
    print(f"[LOG] {stype}={val} {pred} conf={conf:.2f}")
    return sid

def dir_score(s,lo=40,hi=60):
    return "DOWN" if s>=hi else "UP" if s<=lo else "NEUTRAL"

def conf_ext(s,c=50,r=50):
    return min(1.0,abs(s-c)/r)

def lambda_handler(event,context):
    logged=[]
    # data.json
    d=fs3("data/report.json")
    ki=d.get("khalid_index")
    if ki is not None:
        ki=float(ki)
        val="HIGH_RISK" if ki>=70 else "ELEVATED" if ki>=55 else "MODERATE" if ki>=40 else "LOW_RISK"
        logged.append(log_sig("khalid_index",val,dir_score(ki,35,65),conf_ext(ki),"SPY",[7,14,30],meta={"score":ki,"regime":d.get("regime")}))
    regime=d.get("regime","")
    if regime:
        rm={"BULL":"UP","RECOVERY":"UP","RISK_ON":"UP","BEAR":"DOWN","CRISIS":"DOWN","CORRECTION":"DOWN","NEUTRAL":"NEUTRAL","UNKNOWN":"NEUTRAL"}
        logged.append(log_sig("edge_regime",regime,rm.get(regime.upper(),"NEUTRAL"),0.70,"SPY",[14,30],meta={"regime":regime}))
    for t in (d.get("buys") or [])[:3]:
        if isinstance(t,str): logged.append(log_sig("screener_buy",t,"UP",0.72,t,[14,30],bench="SPY",meta={"signal":"buy"}))
    for t in (d.get("sells") or [])[:3]:
        if isinstance(t,str): logged.append(log_sig("screener_sell",t,"DOWN",0.72,t,[14,30],bench="SPY",meta={"signal":"sell"}))
    # crypto-intel.json
    c=fs3("crypto-intel.json")
    fg=c.get("fear_greed",{})
    fgs=fg.get("current")
    if fgs is not None:
        fgs=float(fgs)
        v,p,cf=("EXTREME_FEAR","UP",0.80) if fgs<=20 else ("FEAR","UP",0.60) if fgs<=35 else ("EXTREME_GREED","DOWN",0.80) if fgs>=80 else ("GREED","DOWN",0.60) if fgs>=65 else ("NEUTRAL","NEUTRAL",0.40)
        logged.append(log_sig("crypto_fear_greed",v,p,cf,"BTC-USD",[3,7,14],meta={"score":fgs,"label":fg.get("label")}))
    rs=c.get("risk_score",{})
    rv=rs.get("score")
    if rv is not None:
        rv=float(rv)
        logged.append(log_sig("crypto_risk_score",rs.get("regime","?"),dir_score(rv,35,65),conf_ext(rv),"BTC-USD",[3,7,14],meta={"score":rv,"action":rs.get("action")}))
    tech=c.get("technicals",{})
    btc=tech.get("BTC",tech.get("bitcoin",{}))
    if isinstance(btc,dict):
        bs=btc.get("signal") or btc.get("trend")
        bp=btc.get("price"); br=btc.get("rsi")
        if bs:
            p2="UP" if any(x in str(bs).upper() for x in ["BUY","BULL","UP"]) else "DOWN" if any(x in str(bs).upper() for x in ["SELL","BEAR","DOWN"]) else "NEUTRAL"
            cf2=0.85 if br and (float(br)<=30 or float(br)>=70) else 0.72
            logged.append(log_sig("crypto_btc_signal",bs,p2,cf2,"BTC-USD",[3,7,14],price=bp,meta={"rsi":br,"price":bp}))
    oc=c.get("onchain_ratios",{})
    mvrv=oc.get("mvrv") or oc.get("MVRV")
    if mvrv is not None:
        mvrv=float(mvrv)
        v2,p2,cf2=("UNDERVALUED","UP",0.80) if mvrv<1.0 else ("OVERVALUED","DOWN",0.80) if mvrv>3.5 else ("ELEVATED","DOWN",0.65) if mvrv>2.5 else ("FAIR","NEUTRAL",0.40)
        logged.append(log_sig("btc_mvrv",v2,p2,cf2,"BTC-USD",[14,30,60],meta={"mvrv":mvrv}))
    # edge-data.json
    e=fs3("edge-data.json")
    es=e.get("composite_score")
    if es is not None:
        es=float(es)
        logged.append(log_sig("edge_composite",str(es),dir_score(es,35,65),conf_ext(es),"SPY",[7,14],meta={"score":es,"regime":e.get("regime")}))
    for tk,chg in (e.get("correlation",{}).get("changes",{}) or {}).items():
        if chg is None: continue
        chg=float(chg); p3="UP" if chg>0.5 else "DOWN" if chg<-0.5 else "NEUTRAL"; cf3=min(0.80,abs(chg)/3.0)
        if cf3>=0.3: logged.append(log_sig(f"momentum_{tk.lower()}",f"{chg:+.2f}%",p3,cf3,tk,[1,3,7],meta={"change":chg}))
    # repo-data.json
    r=fs3("repo-data.json")
    st=r.get("stress",{})
    sc=st.get("score")
    if sc is not None:
        sc=float(sc)
        v3,p3,cf3=("HIGH_STRESS","DOWN",0.80) if sc>=60 else ("ELEVATED","DOWN",0.65) if sc>=40 else ("MODERATE","NEUTRAL",0.50) if sc>=20 else ("NORMAL","UP",0.55)
        logged.append(log_sig("plumbing_stress",v3,p3,cf3,"SPY",[7,14,30],meta={"score":sc,"status":st.get("status"),"red_flags":st.get("red_flags")}))
    # intelligence-report.json
    ir=fs3("intelligence-report.json")
    sc2=ir.get("scores",{})
    for k2,against2,wins in [("ml_risk_score","SPY",[7,14,30]),("carry_risk_score","SPY",[14,30])]:
        v4=sc2.get(k2)
        if v4 is not None:
            v4=float(v4)
            logged.append(log_sig(k2.replace("_score",""),str(v4),dir_score(v4,35,65),conf_ext(v4),against2,wins,meta={"score":v4}))
    ph=ir.get("phase","")
    pm={"CRISIS":("DOWN",0.90),"PRE-CRISIS":("DOWN",0.75),"RECOVERY":("UP",0.70),"EXPANSION":("UP",0.65),"STABLE":("NEUTRAL",0.50)}
    if ph.upper() in pm:
        p4,cf4=pm[ph.upper()]
        logged.append(log_sig("market_phase",ph,p4,cf4,"SPY",[14,30,60],meta={"phase":ph}))
    # valuations-data.json
    vd=fs3("valuations-data.json")
    cape=vd.get("cape") or vd.get("CAPE")
    if cape is not None:
        cape=float(cape)
        v5,p5,cf5=("EXTREMELY_EXPENSIVE","DOWN",0.80) if cape>35 else ("EXPENSIVE","DOWN",0.65) if cape>28 else ("CHEAP","UP",0.65) if cape<15 else ("FAIR","NEUTRAL",0.45)
        logged.append(log_sig("cape_ratio",v5,p5,cf5,"SPY",[30,60,90],meta={"cape":cape}))
    buffett=vd.get("buffett_indicator") or vd.get("market_cap_gdp")
    if buffett is not None:
        buffett=float(buffett)
        v6,p6,cf6=("EXTREMELY_OVERVALUED","DOWN",0.80) if buffett>200 else ("OVERVALUED","DOWN",0.65) if buffett>150 else ("UNDERVALUED","UP",0.65) if buffett<100 else ("FAIR","NEUTRAL",0.45)
        logged.append(log_sig("buffett_indicator",v6,p6,cf6,"SPY",[30,60,90],meta={"buffett":buffett}))
    # screener
    sc3=fs3("screener/data.json")
    for i,st2 in enumerate(sc3.get("stocks",[])[:15]):
        tk2=st2.get("symbol") or st2.get("ticker")
        pr2=st2.get("price") or st2.get("currentPrice")
        pi=st2.get("piotroskiScore") or st2.get("piotroski",5)
        if tk2: logged.append(log_sig("screener_top_pick","TOP_10" if i<10 else "TOP_25","OUTPERFORM",min(0.92,float(pi)/9.0) if pi else 0.60,tk2,[30,60,90],price=float(pr2) if pr2 else None,bench="SPY",meta={"rank":i+1,"piotroski":pi}))
    # CFTC
    try:
        sigs=lget(CFTC_URL,"signals")
        items=sigs if isinstance(sigs,list) else sigs.get("signals",[])
        tmap={"GOLD":("GLD",[14,30,60]),"S&P 500":("SPY",[7,14,30]),"NASDAQ":("QQQ",[7,14,30]),"BITCOIN":("BTC-USD",[7,14,30]),"CRUDE OIL":("USO",[14,30,60]),"NATURAL GAS":("UNG",[14,30]),"EUR/USD":("FXE",[7,14,30]),"TREASURY":("TLT",[14,30,60]),"COPPER":("CPER",[14,30]),"SILVER":("SLV",[14,30,60])}
        for item in items:
            contract=str(item.get("contract") or item.get("name") or "").upper()
            sig5=str(item.get("signal") or item.get("direction") or "NEUTRAL").upper()
            cf5=float(item.get("confidence") or item.get("strength") or 0.65)
            tk3,wins3="SPY",[14,30,60]
            for key,(t3,w3) in tmap.items():
                if key in contract: tk3,wins3=t3,w3; break
            p5="UP" if any(x in sig5 for x in ["BUY","BULL","LONG"]) else "DOWN" if any(x in sig5 for x in ["SELL","BEAR","SHORT"]) else "NEUTRAL"
            stype2=f"cftc_{contract.lower().replace(' ','_').replace('/','_')[:25]}"
            logged.append(log_sig(stype2,sig5,p5,cf5,tk3,wins3,meta={"contract":contract,"net_pos":item.get("netPosition")}))
    except Exception as ex: print(f"[CFTC] {ex}")
    # save summary
    s3.put_object(Bucket=S3_BUCKET,Key="learning/last_log_run.json",Body=json.dumps({"logged_at":datetime.now(timezone.utc).isoformat(),"count":len([l for l in logged if l]),"action":event.get("action","auto")}),ContentType="application/json")
    total=len([l for l in logged if l])
    print(f"[DONE] Logged {total} signals")
    return {"statusCode":200,"body":json.dumps({"logged":total})}
