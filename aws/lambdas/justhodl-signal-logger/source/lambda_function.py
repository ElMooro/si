
import json,boto3,uuid,time,urllib.request,urllib.error
from datetime import datetime,timezone,timedelta
from decimal import Decimal

# Phase 2 KA rebrand — recursive khalid_* → ka_* alias helper.
try:
    from ka_aliases import add_ka_aliases
except Exception as _e:
    print(f"WARN: ka_aliases unavailable: {_e}")
    def add_ka_aliases(obj, **_kwargs):
        return obj

dynamodb=boto3.resource("dynamodb",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1")
SIGNALS_TABLE="justhodl-signals"
S3_BUCKET="justhodl-dashboard-live"
CFTC_URL="https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/"

# Same keys outcome-checker uses
POLYGON_KEY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_KEY="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

# Cache prices within a single Lambda invocation (one fetch per ticker)
_PRICE_CACHE={}

def _polygon_prev(ticker):
    """Free-tier-friendly previous close."""
    url=f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
    try:
        with urllib.request.urlopen(url,timeout=8) as r:
            d=json.loads(r.read().decode())
            res=d.get("results") or []
            if res: return float(res[0].get("c") or 0)
    except Exception as e: print(f"[PRICE] Polygon {ticker}: {e}")
    return None

def _fmp_stable(ticker):
    """Modern FMP /stable/quote endpoint."""
    url=f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={FMP_KEY}"
    try:
        with urllib.request.urlopen(url,timeout=8) as r:
            d=json.loads(r.read().decode())
            if d and isinstance(d,list) and len(d)>0:
                p=d[0].get("price")
                if p is not None: return float(p)
    except Exception as e: print(f"[PRICE] FMP {ticker}: {e}")
    return None

def _coingecko(ticker):
    """Free crypto fallback."""
    cmap={"BTC-USD":"bitcoin","BTC":"bitcoin","ETH-USD":"ethereum","ETH":"ethereum",
          "SOL-USD":"solana","SOL":"solana"}
    cg=cmap.get(ticker.upper())
    if not cg: return None
    url=f"https://api.coingecko.com/api/v3/simple/price?ids={cg}&vs_currencies=usd"
    try:
        with urllib.request.urlopen(url,timeout=8) as r:
            d=json.loads(r.read().decode())
            return float(d.get(cg,{}).get("usd") or 0)
    except Exception as e: print(f"[PRICE] CoinGecko {ticker}: {e}")
    return None

def get_baseline_price(ticker):
    """Get current price for a ticker — same fallback chain as outcome-checker.
    Cached within Lambda invocation to avoid duplicate fetches."""
    if not ticker: return None
    if ticker in _PRICE_CACHE: return _PRICE_CACHE[ticker]
    p=None
    if ticker.upper() in ("BTC-USD","ETH-USD","SOL-USD","BTC","ETH","SOL"):
        p=_coingecko(ticker)
    if not p: p=_fmp_stable(ticker)
    if not p: p=_polygon_prev(ticker)
    _PRICE_CACHE[ticker]=p
    return p

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

# Captures Khalid regime once per Lambda invocation; populated by lambda_handler
_REGIME_SNAPSHOT={"regime":None,"khalid_score":None}

def _capture_regime_snapshot():
    """Read data/report.json once, capture regime + khalid_score for this run.
    Called from lambda_handler at start of invocation."""
    try:
        d=fs3("data/report.json")
        ki=d.get("khalid_index")
        if isinstance(ki,dict):
            _REGIME_SNAPSHOT["khalid_score"]=int(float(ki.get("score",0))) if ki.get("score") is not None else None
            _REGIME_SNAPSHOT["regime"]=ki.get("regime") or d.get("regime")
        elif ki is not None:
            _REGIME_SNAPSHOT["khalid_score"]=int(float(ki))
            _REGIME_SNAPSHOT["regime"]=d.get("regime")
        else:
            _REGIME_SNAPSHOT["regime"]=d.get("regime")
        print(f"[REGIME] snapshot: regime={_REGIME_SNAPSHOT['regime']}, score={_REGIME_SNAPSHOT['khalid_score']}")
    except Exception as e:
        print(f"[REGIME] snapshot failed (non-fatal): {e}")

def log_sig(stype,val,pred,conf,against,windows,price=None,meta=None,bench=None,
            magnitude=None,target_price=None,rationale=None,supporting=None):
    table=dynamodb.Table(SIGNALS_TABLE)
    now=datetime.now(timezone.utc)
    sid=str(uuid.uuid4())
    ts={f"day_{d}":(now+timedelta(days=d)).isoformat() for d in windows}

    # Auto-fetch baseline_price if not explicitly passed
    if price is None and against:
        price=get_baseline_price(against)
    # Auto-fetch benchmark price for relative-comparison signals (OUTPERFORM/UNDERPERFORM)
    bench_price=None
    if bench and pred in ("OUTPERFORM","UNDERPERFORM"):
        bench_price=get_baseline_price(bench)

    # Compute predicted_target_price from magnitude × baseline (Q1.2 — both)
    computed_target=None
    if target_price is not None:
        computed_target=float(target_price)
    elif magnitude is not None and price:
        # +X% magnitude → target_price = baseline × (1 + X/100)
        # NEUTRAL/0 magnitude leaves target = baseline
        computed_target=float(price)*(1.0+float(magnitude)/100.0)

    horizon_primary=max(windows) if windows else None

    item={"signal_id":sid,"signal_type":stype,"signal_value":str(val),
          "predicted_direction":pred,"confidence":f2d(float(conf)),
          "measure_against":against,"baseline_price":f2d(float(price)) if price else None,
          "baseline_benchmark_price":f2d(float(bench_price)) if bench_price else None,
          "benchmark":bench,"check_windows":[str(d) for d in windows],
          "check_timestamps":ts,"outcomes":{},"accuracy_scores":{},
          "logged_at":now.isoformat(),"logged_epoch":int(now.timestamp()),
          "status":"pending","metadata":f2d(meta or {}),
          "ttl":int((now+timedelta(days=365)).timestamp()),
          # ─── Schema v2 fields (Week 2A) ────────────────────
          "schema_version":"2",
          "predicted_magnitude_pct":f2d(float(magnitude)) if magnitude is not None else None,
          "predicted_target_price":f2d(float(computed_target)) if computed_target is not None else None,
          "horizon_days_primary":int(horizon_primary) if horizon_primary else None,
          "regime_at_log":_REGIME_SNAPSHOT.get("regime"),
          "khalid_score_at_log":_REGIME_SNAPSHOT.get("khalid_score"),
          "rationale":str(rationale) if rationale else None,
          "supporting_signals":list(supporting) if supporting else None,
          }
    # Phase 2 dual-write — add ka_score_at_log alongside khalid_score_at_log
    item = add_ka_aliases(item)
    table.put_item(Item=item)
    bp_str=f" baseline=${price:.2f}" if price else " baseline=None"
    print(f"[LOG] {stype}={val} {pred} conf={conf:.2f}{bp_str}")
    return sid

def dir_score(s,lo=40,hi=60):
    return "DOWN" if s>=hi else "UP" if s<=lo else "NEUTRAL"

def conf_ext(s,c=50,r=50):
    return min(1.0,abs(s-c)/r)

def lambda_handler(event,context):
    # Capture Khalid regime once for this invocation; every log_sig() call
    # reads from _REGIME_SNAPSHOT to populate regime_at_log + khalid_score_at_log
    _capture_regime_snapshot()
    logged=[]
    # data.json
    d=fs3("data/report.json")
    ki=d.get("khalid_index")
    if ki is not None:
        if isinstance(ki, dict): ki=float(ki.get("score", 0))
        else: ki=float(ki)
        val="HIGH_RISK" if ki>=70 else "ELEVATED" if ki>=55 else "MODERATE" if ki>=40 else "LOW_RISK"
        ki_rat=f"Khalid Index {ki:.0f} = {val} ({d.get('regime') or 'unknown'} regime)"
        logged.append(log_sig("khalid_index",val,dir_score(ki,35,65),conf_ext(ki),"SPY",[7,14,30],meta={"score":ki,"regime":d.get("regime")},rationale=ki_rat))
    regime=d.get("regime","") or (d.get("khalid_index",{}).get("regime","") if isinstance(d.get("khalid_index"), dict) else "")
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
        fg_rat=f"Fear & Greed {int(fgs)} ({fg.get('label') or v}) — contrarian {p} signal"
        logged.append(log_sig("crypto_fear_greed",v,p,cf,"BTC-USD",[1,3,7,14],meta={"score":fgs,"label":fg.get("label")},rationale=fg_rat))
    rs=c.get("risk_score",{})
    rv=rs.get("score")
    if rv is not None:
        rv=float(rv)
        logged.append(log_sig("crypto_risk_score",rs.get("regime","?"),dir_score(rv,35,65),conf_ext(rv),"BTC-USD",[1,3,7,14],meta={"score":rv,"action":rs.get("action")}))
    tech=c.get("technicals",{})
    btc=tech.get("BTC",tech.get("bitcoin",{}))
    if isinstance(btc,dict):
        bs=btc.get("signal") or btc.get("trend")
        bp=btc.get("price"); br=btc.get("rsi")
        if bs:
            p2="UP" if any(x in str(bs).upper() for x in ["BUY","BULL","UP"]) else "DOWN" if any(x in str(bs).upper() for x in ["SELL","BEAR","DOWN"]) else "NEUTRAL"
            cf2=0.85 if br and (float(br)<=30 or float(br)>=70) else 0.72
            logged.append(log_sig("crypto_btc_signal",bs,p2,cf2,"BTC-USD",[1,3,7,14],price=bp,meta={"rsi":br,"price":bp}))
    oc=c.get("onchain_ratios",{})
    mvrv=oc.get("mvrv") or oc.get("MVRV")
    if mvrv is not None:
        mvrv=float(mvrv)
        v2,p2,cf2=("UNDERVALUED","UP",0.80) if mvrv<1.0 else ("OVERVALUED","DOWN",0.80) if mvrv>3.5 else ("ELEVATED","DOWN",0.65) if mvrv>2.5 else ("FAIR","NEUTRAL",0.40)
        mvrv_mag=10.0 if mvrv<0.8 else (-15.0 if mvrv>3.5 else (-8.0 if mvrv>2.5 else 0))
        mvrv_rat=f"MVRV {mvrv:.2f} = {v2} (historic UP at <1, DOWN at >3)"
        logged.append(log_sig("btc_mvrv",v2,p2,cf2,"BTC-USD",[14,30,60],meta={"mvrv":mvrv},magnitude=mvrv_mag,rationale=mvrv_rat))
    # edge-data.json
    e=fs3("edge-data.json")
    es=e.get("composite_score")
    if es is not None:
        es=float(es)
        logged.append(log_sig("edge_composite",str(es),dir_score(es,35,65),conf_ext(es),"SPY",[1,7,14],meta={"score":es,"regime":e.get("regime")}))
    for tk,chg in (e.get("correlation",{}).get("changes",{}) or {}).items():
        if chg is None: continue
        chg=float(chg); p3="UP" if chg>0.5 else "DOWN" if chg<-0.5 else "NEUTRAL"; cf3=min(0.80,abs(chg)/3.0)
        if cf3>=0.3:
            mom_mag=chg if abs(chg)<10 else (10 if chg>0 else -10)  # cap at ±10% to filter outliers
            mom_rat=f"{tk} momentum: {chg:+.2f}% recent change → {p3} {abs(chg):.1f}% over 1-7d"
            logged.append(log_sig(f"momentum_{tk.lower()}",f"{chg:+.2f}%",p3,cf3,tk,[1,3,7],meta={"change":chg},magnitude=mom_mag,rationale=mom_rat))
    # repo-data.json
    r=fs3("repo-data.json")
    st=r.get("stress",{})
    sc=st.get("score")
    if sc is not None:
        sc=float(sc)
        v3,p3,cf3=("HIGH_STRESS","DOWN",0.80) if sc>=60 else ("ELEVATED","DOWN",0.65) if sc>=40 else ("MODERATE","NEUTRAL",0.50) if sc>=20 else ("NORMAL","UP",0.55)
        plumb_rat=f"Plumbing stress {sc:.0f} = {v3} ({st.get('status') or '?'}); red_flags={st.get('red_flags') or 0}"
        logged.append(log_sig("plumbing_stress",v3,p3,cf3,"SPY",[1,7,14,30],meta={"score":sc,"status":st.get("status"),"red_flags":st.get("red_flags")},rationale=plumb_rat))
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
        cape_mag=-8.0 if cape>35 else (-4.0 if cape>28 else (5.0 if cape<15 else 0))
        cape_rat=f"Shiller CAPE {cape:.1f} = {v5} (historical avg ~17, frothy >28)"
        logged.append(log_sig("cape_ratio",v5,p5,cf5,"SPY",[30,60,90],meta={"cape":cape},magnitude=cape_mag,rationale=cape_rat))
    buffett=vd.get("buffett_indicator") or vd.get("market_cap_gdp")
    if buffett is not None:
        buffett=float(buffett)
        v6,p6,cf6=("EXTREMELY_OVERVALUED","DOWN",0.80) if buffett>200 else ("OVERVALUED","DOWN",0.65) if buffett>150 else ("UNDERVALUED","UP",0.65) if buffett<100 else ("FAIR","NEUTRAL",0.45)
        buff_mag=-10.0 if buffett>200 else (-5.0 if buffett>150 else (5.0 if buffett<100 else 0))
        buff_rat=f"Buffett Indicator (Mkt Cap/GDP) {buffett:.0f}% = {v6} — historic balance at ~100%"
        logged.append(log_sig("buffett_indicator",v6,p6,cf6,"SPY",[30,60,90],meta={"buffett":buffett},magnitude=buff_mag,rationale=buff_rat))
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

    # ─── Phase 9.6 — Crisis & Plumbing signals (Loop 1 calibrator integration) ───
    # Pulls data/crisis-plumbing.json (Phase 9.3) and data/correlation-breaks.json
    # (Phase 9.5) and emits signals into the calibrator. Each non-NORMAL signal
    # bucket maps to a directional prediction on a relevant outcome ticker.
    # When the calibrator runs Sundays it will weight these by realized accuracy.
    try:
        cp=fs3("data/crisis-plumbing.json")
        if cp:
            # Confidence ladder: CRISIS=0.85, ELEVATED=0.75, WATCH=0.65, NORMAL=0.45
            CONF={"CRISIS":0.85,"ELEVATED":0.75,"WATCH":0.65,"NORMAL":0.45}
            # Direction: most stress signals predict equities DOWN
            def stress_sig(name,sig,bucket_val,against,wins,meta=None,rat=None):
                """Helper for stress-direction signals (DOWN on stress, NEUTRAL on normal)."""
                pred="DOWN" if sig in ("CRISIS","ELEVATED","WATCH") else "NEUTRAL"
                cf=CONF.get(sig,0.45)
                logged.append(log_sig(name,bucket_val,pred,cf,against,wins,meta=meta,rationale=rat))

            # Funding & Credit Signals (6)
            fcs=cp.get("funding_credit_signals",{}) or {}
            sofr=fcs.get("SOFR_IORB_SPREAD",{})
            if sofr.get("available"):
                sig=sofr.get("signal","NORMAL")
                rat=f"SOFR-IORB spread {sofr.get('spread_bps')}bps z_1y={sofr.get('z_score_1y')} → {sig} repo stress"
                stress_sig("crisis_sofr_iorb",sig,f"{sofr.get('spread_bps')}bps","SPY",[3,7,14],
                           meta={"spread_bps":sofr.get("spread_bps"),"z_1y":sofr.get("z_score_1y"),"signal":sig},rat=rat)
            hy=fcs.get("HY_OAS",{})
            if hy.get("available"):
                sig=hy.get("signal","NORMAL")
                rat=f"HY OAS {hy.get('latest_value'):.0f}bps z_1y={hy.get('z_score_1y')} → {sig} credit fear"
                # Two simultaneous predictions: HYG itself + broader SPY
                stress_sig("crisis_hy_oas_vs_hyg",sig,f"{hy.get('latest_value'):.0f}bps","HYG",[3,7,14],
                           meta={"oas_bps":hy.get("latest_value"),"z_1y":hy.get("z_score_1y"),"signal":sig},rat=rat)
                stress_sig("crisis_hy_oas_vs_spy",sig,f"{hy.get('latest_value'):.0f}bps","SPY",[7,14,30],
                           meta={"oas_bps":hy.get("latest_value"),"signal":sig},rat=rat)
            ig=fcs.get("IG_BBB_OAS",{})
            if ig.get("available"):
                sig=ig.get("signal","NORMAL")
                rat=f"IG BBB OAS {ig.get('latest_value'):.0f}bps → {sig}"
                stress_sig("crisis_ig_bbb_oas",sig,f"{ig.get('latest_value'):.0f}bps","SPY",[14,30],
                           meta={"oas_bps":ig.get("latest_value"),"signal":sig},rat=rat)
            t10yie=fcs.get("T10YIE",{})
            if t10yie.get("available"):
                lv=float(t10yie.get("latest_value") or 0)
                # T10YIE has 'extremes' direction — both very low (deflation) and very high (unanchored) are stress
                if lv<1.5 or lv>3.0:
                    pred="DOWN"; cf=0.65; sig="ELEVATED"
                else:
                    pred="NEUTRAL"; cf=0.45; sig="NORMAL"
                rat=f"10Y breakeven inflation {lv:.2f}% → {sig} (extremes flagged: <1.5% deflation / >3% unanchored)"
                logged.append(log_sig("crisis_t10yie_extreme",f"{lv:.2f}%",pred,cf,"SPY",[30,60],
                                      meta={"breakeven_pct":lv,"signal":sig},rationale=rat))
            dfii=fcs.get("DFII10",{})
            if dfii.get("available"):
                sig=dfii.get("signal","NORMAL")
                rat=f"10Y real rate {dfii.get('latest_value'):.2f}% → {sig} (high real rates pressure equities + gold)"
                stress_sig("crisis_dfii10_vs_spy",sig,f"{dfii.get('latest_value'):.2f}%","SPY",[14,30,60],
                           meta={"real_rate_pct":dfii.get("latest_value"),"signal":sig},rat=rat)
                # high real rates also pressure gold
                stress_sig("crisis_dfii10_vs_gld",sig,f"{dfii.get('latest_value'):.2f}%","GLD",[14,30,60],
                           meta={"real_rate_pct":dfii.get("latest_value"),"signal":sig},rat=rat)
            sloos=fcs.get("SLOOS_TIGHTEN",{})
            if sloos.get("available"):
                sig=sloos.get("signal","NORMAL")
                rat=f"SLOOS C&I tightening {sloos.get('latest_value'):.1f}% → {sig} (banks pulling credit, slow signal)"
                stress_sig("crisis_sloos_tighten",sig,f"{sloos.get('latest_value'):.1f}%","SPY",[60,90],
                           meta={"net_pct_tightening":sloos.get("latest_value"),"signal":sig},rat=rat)

            # Cross-currency / offshore dollar funding (4)
            xcc=cp.get("xcc_basis_proxy",{}) or {}
            for k,against,wins in (("rate_diff_jpy_3m","SPY",[7,14]),
                                    ("rate_diff_eur_3m","SPY",[7,14])):
                rd=xcc.get(k,{})
                if rd.get("available"):
                    sig=rd.get("signal","NORMAL")
                    rat=f"{k} {rd.get('current_pct'):.2f}% z_1y={rd.get('z_score_1y')} → {sig} USD funding stress proxy"
                    stress_sig(f"crisis_{k}",sig,f"{rd.get('current_pct'):.2f}%",against,wins,
                               meta={"diff_pct":rd.get("current_pct"),"z_1y":rd.get("z_score_1y"),"signal":sig},rat=rat)
            bd=xcc.get("broad_dollar_index",{})
            if bd.get("available"):
                sig=bd.get("signal","NORMAL")
                # High dollar = global stress = SPY/EEM down
                rat=f"Broad USD index {bd.get('level')} z_1y={bd.get('z_score_1y')} → {sig}"
                stress_sig("crisis_broad_dollar_vs_spy",sig,f"{bd.get('level')}","SPY",[7,14],
                           meta={"level":bd.get("level"),"z_1y":bd.get("z_score_1y"),"signal":sig},rat=rat)
                stress_sig("crisis_broad_dollar_vs_eem",sig,f"{bd.get('level')}","EEM",[7,14,30],
                           meta={"level":bd.get("level"),"signal":sig},rat=rat)
            ob=xcc.get("obfr_iorb_spread",{})
            if ob.get("available"):
                sig=ob.get("signal","NORMAL")
                rat=f"OBFR-IORB spread {ob.get('spread_bps')}bps → {sig} unsecured cash hoarding"
                stress_sig("crisis_obfr_iorb",sig,f"{ob.get('spread_bps')}bps","SPY",[3,7],
                           meta={"spread_bps":ob.get("spread_bps"),"signal":sig},rat=rat)

            # MMF flight-to-quality
            mmf=cp.get("mmf_composition") or {}
            if mmf and mmf.get("flight_to_quality") is not None:
                ftq=bool(mmf.get("flight_to_quality"))
                pred="DOWN" if ftq else "NEUTRAL"
                cf=0.75 if ftq else 0.45
                rat=f"MMF flight to quality: prime share Δ30d={mmf.get('prime_share_change_30d_pp')}pp → "+("FLIGHT" if ftq else "stable")
                # Regional banks (KRE) are most affected by deposit flight
                logged.append(log_sig("crisis_mmf_flight_to_quality",
                                      "FLIGHT" if ftq else "STABLE",pred,cf,"KRE",[14,30],
                                      meta={"prime_share_change_pp":mmf.get("prime_share_change_30d_pp"),
                                            "flight":ftq},rationale=rat))

            # Official Crisis Indices (NFCI / STLFSI4 / ANFCI / KCFSI)
            ci=cp.get("crisis_indices",{}) or {}
            for sid,data in ci.items():
                if not data.get("available"): continue
                stressed=bool(data.get("is_stressed"))
                pct=data.get("pct_rank")
                pred="DOWN" if stressed else "NEUTRAL"
                if pct is not None and pct>=90: cf=0.80
                elif stressed: cf=0.70
                else: cf=0.45
                rat=f"{sid} latest={data.get('latest_value')} pct_rank={pct} → "+("STRESSED" if stressed else "NORMAL")
                logged.append(log_sig(f"crisis_index_{sid.lower()}",
                                      "STRESSED" if stressed else "NORMAL",pred,cf,"SPY",[14,30],
                                      meta={"latest":data.get("latest_value"),"pct_rank":pct,
                                            "is_stressed":stressed},rationale=rat))
    except Exception as ex: print(f"[CRISIS-PLUMBING signals] {ex}")

    # Phase 9.5 — correlation break detector
    try:
        cb=fs3("data/correlation-breaks.json")
        if cb and cb.get("status")!="warming_up":
            sig=cb.get("signal","NORMAL")
            fz=cb.get("frobenius_z_score_1y")
            n2=cb.get("n_pairs_above_2sigma",0)
            # Composite correlation break → VIX UP + SPY DOWN
            CONF={"CRISIS":0.85,"ELEVATED":0.75,"WATCH":0.65,"NORMAL":0.45}
            cf=CONF.get(sig,0.45)
            pred_spy="DOWN" if sig in ("CRISIS","ELEVATED","WATCH") else "NEUTRAL"
            pred_vix="UP" if sig in ("CRISIS","ELEVATED","WATCH") else "NEUTRAL"
            rat=f"Correlation break composite z={fz}, {n2} pairs >2σ → {sig} regime change signal"
            logged.append(log_sig("corr_break_composite_vs_spy",sig,pred_spy,cf,"SPY",[3,5,14],
                                  meta={"fro_z":fz,"n_pairs_2sigma":n2,"signal":sig},rationale=rat))
            logged.append(log_sig("corr_break_composite_vs_vxx",sig,pred_vix,cf,"VXX",[3,5,14],
                                  meta={"fro_z":fz,"n_pairs_2sigma":n2,"signal":sig},rationale=rat))

            # Top breaking pair — if |z|>=2 it's worth tracking individually
            tops=cb.get("top_breaking_pairs") or []
            if tops:
                top=tops[0]
                z1=top.get("z_score") or 0
                if abs(z1)>=2:
                    pname=f"{top['pair'][0]}_{top['pair'][1]}".lower()[:30]
                    pred="DOWN" if abs(z1)>=2 else "NEUTRAL"
                    cf2=min(0.85,0.5+abs(z1)*0.1)
                    rat=f"Top breaking pair {top['pair'][0]}↔{top['pair'][1]}: z={z1}, current={top.get('current_corr')}, baseline={top.get('baseline_corr')}"
                    logged.append(log_sig(f"corr_break_top_pair",pname,pred,cf2,"SPY",[5,14],
                                          meta={"pair":top['pair'],"z":z1,
                                                "current_corr":top.get('current_corr'),
                                                "baseline_corr":top.get('baseline_corr'),
                                                "context":top.get('context')},rationale=rat))
    except Exception as ex: print(f"[CORRELATION-BREAK signals] {ex}")
    # ─── End Phase 9.6 additions ───

    # save summary
    s3.put_object(Bucket=S3_BUCKET,Key="learning/last_log_run.json",Body=json.dumps({"logged_at":datetime.now(timezone.utc).isoformat(),"count":len([l for l in logged if l]),"action":event.get("action","auto")}),ContentType="application/json")
    total=len([l for l in logged if l])
    print(f"[DONE] Logged {total} signals")
    return {"statusCode":200,"body":json.dumps({"logged":total})}
