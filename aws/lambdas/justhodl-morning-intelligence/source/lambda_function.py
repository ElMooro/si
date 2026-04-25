import os
import json,boto3,urllib.request,time,math
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from collections import defaultdict
from boto3.dynamodb.conditions import Attr

# Calibration helper — Loop 1: weight signals by historical accuracy
try:
    from calibration import blend_score, get_calibration
    _CALIBRATION_AVAILABLE = True
except Exception as _e:
    print(f"WARN: calibration module unavailable: {_e}")
    _CALIBRATION_AVAILABLE = False
    def blend_score(scores, default_weight=1.0):
        if not scores: return {"value": 0.0, "raw_value": 0.0, "contributions": [],
                                "total_weight": 0.0, "is_calibrated": False, "n_calibrated": 0}
        n = len(scores)
        avg = sum(float(v) for v in scores.values() if v is not None) / n if n else 0.0
        return {"value": avg, "raw_value": avg, "contributions": [],
                "total_weight": float(n), "is_calibrated": False, "n_calibrated": 0}
    def get_calibration():
        class _C:
            is_meaningful = False
            weights = {}
            accuracy = {}
            def weight(self, _): return 1.0
            def is_signal_calibrated(self, _): return False
        return _C()

TELEGRAM_TOKEN="8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_API="https://api.telegram.org/bot"+TELEGRAM_TOKEN
ANTHROPIC_KEY=os.environ.get('ANTHROPIC_KEY', '')
S3_BUCKET="justhodl-dashboard-live"
CHAT_ID_PARAM="/justhodl/telegram/chat_id"
WEIGHTS_PARAM="/justhodl/calibration/weights"
ACCURACY_PARAM="/justhodl/calibration/accuracy"
TEMPLATES_KEY="learning/prompt_templates.json"
IMPROVEMENTS_KEY="learning/improvement_log.json"

dynamodb=boto3.resource("dynamodb",region_name="us-east-1")
ssm=boto3.client("ssm",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1")

def d2f(obj):
    if isinstance(obj,Decimal): return float(obj)
    if isinstance(obj,dict): return {k:d2f(v) for k,v in obj.items()}
    if isinstance(obj,list): return [d2f(v) for v in obj]
    return obj

def gp(name,default=None):
    try: return ssm.get_parameter(Name=name,WithDecryption=True)["Parameter"]["Value"]
    except: return default

def fs3(key):
    try:
        obj=s3.get_object(Bucket=S3_BUCKET,Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print("[S3] "+key+": "+str(e)); return {}

def stg(chat_id,text):
    url=TELEGRAM_API+"/sendMessage"
    for pm in ["Markdown",None]:
        try:
            payload={"chat_id":chat_id,"text":text[:4096],"disable_web_page_preview":True}
            if pm: payload["parse_mode"]=pm
            body=json.dumps(payload).encode()
            req=urllib.request.Request(url,data=body,headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(req,timeout=10) as r:
                resp=json.loads(r.read().decode())
                if resp.get("ok"): return resp
        except Exception as e: print("[TG] "+str(e))

def ai(prompt,max_tokens=800):
    try:
        body=json.dumps({"model":"claude-haiku-4-5-20251001","max_tokens":max_tokens,"messages":[{"role":"user","content":prompt}]}).encode()
        req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=body,
            headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"})
        with urllib.request.urlopen(req,timeout=30) as r:
            return json.loads(r.read().decode())["content"][0]["text"].strip()
    except Exception as e: print("[AI] "+str(e)); return None

def load_weights():
    raw=gp(WEIGHTS_PARAM)
    if raw:
        try: return json.loads(raw)
        except: pass
    return {}

def load_accuracy():
    raw=gp(ACCURACY_PARAM)
    if raw:
        try: return json.loads(raw)
        except: pass
    return {}

def load_templates():
    defaults={
        "morning_brief":(
            "You are JustHodlAI, institutional-grade autonomous financial intelligence. "
            "Generate a morning Telegram brief max 380 words using the live data provided. "
            "Requirements: 1) Use REAL numbers only - no placeholders. "
            "2) Lead with Khalid Index and calibrated weight. "
            "3) Flag top 2-3 signals by calibrated weight. "
            "4) Include BTC price, MVRV, funding sentiment. "
            "5) ONE clear actionable takeaway at end. "
            "6) Emphasize signals system trusts most. "
            "Format with emojis and Markdown bold headers."
        ),
        "signal_analysis":(
            "You are a quant analyst reviewing JustHodlAI prediction failures. "
            "Identify exact conditions causing each failure. "
            "Suggest which signals to weight differently."
        ),
        "improvement_writer":(
            "Improve the JustHodlAI morning brief prompt based on empirical failures. "
            "Keep under 250 words. Return ONLY the new prompt text."
        )
    }
    stored=fs3(TEMPLATES_KEY)
    if stored: defaults.update(stored)
    return defaults

def save_templates(t):
    s3.put_object(Bucket=S3_BUCKET,Key=TEMPLATES_KEY,Body=json.dumps(t,indent=2),ContentType="application/json")

def load_all():
    keys={
        "main":"data/report.json",
        "intel":"intelligence-report.json",
        "crypto":"crypto-intel.json",
        "edge":"edge-data.json",
        "repo":"repo-data.json",
        "flow":"flow-data.json",
        "screener":"screener/data.json",
        "predictions":"predictions.json",
        "valuations":"valuations-data.json",
        "bond_regime":"regime/current.json",
        "divergence":"divergence/current.json"
    }
    return {k:fs3(v) for k,v in keys.items()}

def extract_metrics(data,weights):
    d=data.get("main",{})
    intel=data.get("intel",{})
    crypto=data.get("crypto",{})
    edge=data.get("edge",{})
    repo=data.get("repo",{})
    flow=data.get("flow",{})
    vals=data.get("valuations",{})
    scores=intel.get("scores",{})
    regime_d=intel.get("regime",{})
    stress=repo.get("stress",{})
    fg=crypto.get("fear_greed",{})
    rs=crypto.get("risk_score",{})
    corr=edge.get("correlation",{}).get("changes",{})
    coins=crypto.get("top_coins",{}).get("coins",[])
    btc=next((c for c in coins if c.get("symbol")=="BTC"),{})
    eth=next((c for c in coins if c.get("symbol")=="ETH"),{})
    oc=crypto.get("onchain_ratios",{})
    fund_rates=crypto.get("funding",{}).get("rates",[])
    btc_fund=next((r for r in fund_rates if r.get("symbol")=="BTC"),{})
    eth_fund=next((r for r in fund_rates if r.get("symbol")=="ETH"),{})
    ki=d.get("khalid_index") or scores.get("khalid_index",0)
    # Loop 1: use shared calibration helper instead of raw weights dict.
    # Helper applies the is_meaningful gate (≥30 scored outcomes per
    # signal); falls back to 1.0 when calibrator data is sparse, so
    # we don't apply noisy 0.5 default weights from a calibrator that
    # has 0 scored outcomes today.
    _cal = get_calibration() if _CALIBRATION_AVAILABLE else None
    kw = _cal.weight("khalid_index") if _cal is not None else 1.0
    picks=[s.get("symbol","?")+"(P:"+str(s.get("piotroskiScore","?"))+")" for s in data.get("screener",{}).get("stocks",[])[:5]]
    alerts=[str(a.get("message",a))[:80] for a in edge.get("alerts",[])[:3]]
    top_w=sorted([(k,v) for k,v in weights.items()],key=lambda x:x[1],reverse=True)[:5]
    return {
        "khalid_raw":ki,
        "khalid_weight":kw,
        "khalid_adj":round(float(ki["score"] if isinstance(ki, dict) else ki)*kw,1) if ki else 0,
        "khalid_regime":(ki.get("regime") if isinstance(ki, dict) else None) or d.get("regime") or regime_d.get("khalid","UNKNOWN"),
        "edge_score":edge.get("composite_score","N/A"),
        "edge_regime":edge.get("regime","N/A"),
        "ml_risk":scores.get("ml_risk_score","N/A"),
        "carry_risk":scores.get("carry_risk_score","N/A"),
        "crisis_dist":scores.get("crisis_distance","N/A"),
        "phase":intel.get("phase","UNKNOWN"),
        "forecast":intel.get("forecast","")[:120],
        "spy":corr.get("SPY"),
        "tlt":corr.get("TLT"),
        "gld":corr.get("GLD"),
        "qqq":corr.get("QQQ"),
        "uup":corr.get("UUP"),
        "stress_score":stress.get("score","N/A"),
        "stress_status":stress.get("status","N/A"),
        "stress_phase":repo.get("summary",{}).get("phase","N/A"),
        "red_flags":stress.get("red_flags",0),
        "flags":[str(f)[:50] for f in stress.get("flags",[])[:3]],
        "fg":fg.get("current","N/A"),
        "fg_label":fg.get("label","N/A"),
        "crypto_risk":rs.get("score","N/A"),
        # ─── Loop 1: calibration-weighted multi-signal composite ───
        # Blends khalid_index + plumbing_stress + ml_risk + carry_risk
        # weighted by historical accuracy. is_calibrated is True only
        # after the calibrator has scored ≥30 outcomes for at least
        # one signal (~ early May 2026 onward).
        **(lambda inputs=({k:v for k,v in [
            ("khalid_index", float(ki["score"]) if isinstance(ki, dict) and ki.get("score") is not None
                              else (float(ki) if isinstance(ki, (int, float)) and ki else None)),
            ("plumbing_stress", float(stress.get("score")) if stress.get("score") not in (None, "N/A") else None),
            ("ml_risk", float(scores.get("ml_risk_score")) if scores.get("ml_risk_score") not in (None, "N/A") else None),
            ("carry_risk", float(scores.get("carry_risk_score")) if scores.get("carry_risk_score") not in (None, "N/A") else None),
        ] if v is not None}): {
            "blended_composite": round(blend_score(inputs)["value"], 2) if inputs else None,
            "raw_composite": round(blend_score(inputs)["raw_value"], 2) if inputs else None,
            "calibration_active": blend_score(inputs)["is_calibrated"] if inputs else False,
            "calibration_n_signals": len(inputs),
        })(),
        "crypto_regime":rs.get("regime","N/A"),
        "crypto_action":rs.get("action","N/A"),
        # ─── Phase 1A bond regime + Phase 1B divergence — added 2026-04-25 ───
        "bond_regime":(data.get("bond_regime") or {}).get("regime","UNKNOWN"),
        "bond_regime_strength":(data.get("bond_regime") or {}).get("regime_strength"),
        "bond_extreme_count":(data.get("bond_regime") or {}).get("indicators_extreme",0),
        "bond_total_count":(data.get("bond_regime") or {}).get("indicators_total",0),
        "bond_n_off":(data.get("bond_regime") or {}).get("n_risk_off",0),
        "bond_n_on":(data.get("bond_regime") or {}).get("n_risk_on",0),
        "bond_changed":(data.get("bond_regime") or {}).get("regime_changed",False),
        "bond_extreme_signals":[
            (s.get("name"),s.get("z"),s.get("direction"))
            for s in ((data.get("bond_regime") or {}).get("signals") or [])
            if s.get("extreme")
        ][:5],
        "divergence_extreme_count":((data.get("divergence") or {}).get("summary") or {}).get("n_extreme",0),
        "divergence_alert_count":((data.get("divergence") or {}).get("summary") or {}).get("n_alert_worthy",0),
        "divergence_top":[
            (rel.get("name"),rel.get("z_score"),rel.get("mispricing"))
            for rel in ((data.get("divergence") or {}).get("relationships") or [])
            if rel.get("status")=="ok" and rel.get("extreme")
        ][:3],
        "btc_price":btc.get("price"),
        "btc_24h":btc.get("change_24h"),
        "btc_7d":btc.get("change_7d"),
        "btc_ath_chg":btc.get("ath_change"),
        "btc_funding_pct":btc_fund.get("funding_rate_pct"),
        "btc_funding_annual":btc_fund.get("annualized_pct"),
        "btc_sentiment":btc_fund.get("sentiment"),
        "eth_price":eth.get("price"),
        "eth_24h":eth.get("change_24h"),
        "eth_sentiment":eth_fund.get("sentiment"),
        "mvrv":oc.get("mvrv_approx"),
        "onchain_signal":oc.get("signal"),
        "onchain_momentum":oc.get("momentum_30d"),
        "cape":vals.get("cape") or vals.get("CAPE"),
        "buffett":vals.get("buffett_indicator") or vals.get("market_cap_gdp"),
        "pc":flow.get("put_call_ratio") or flow.get("pc_ratio"),
        "options_bias":flow.get("bias") or flow.get("overall_bias"),
        "picks":picks,
        "alerts":alerts,
        "top_weights":top_w
    }

def get_outcomes(days=7):
    table=dynamodb.Table("justhodl-outcomes")
    cutoff=(datetime.now(timezone.utc)-timedelta(days=days)).isoformat()
    try:
        res=table.scan(FilterExpression=Attr("checked_at").gte(cutoff))
        items=res.get("Items",[])
        while "LastEvaluatedKey" in res:
            res=table.scan(FilterExpression=Attr("checked_at").gte(cutoff),ExclusiveStartKey=res["LastEvaluatedKey"])
            items+=res.get("Items",[])
        return [d2f(i) for i in items]
    except Exception as e: print("[OUT] "+str(e)); return []

def perf_summary(outcomes):
    if not outcomes: return None
    by=defaultdict(lambda:{"c":0,"w":0,"r":[]})
    for o in outcomes:
        t=o.get("signal_type","?")
        ok=o.get("correct")
        ret=float(o.get("outcome",{}).get("return_pct",o.get("outcome",{}).get("excess_return",0)) or 0)
        if ok is True: by[t]["c"]+=1
        elif ok is False: by[t]["w"]+=1
        by[t]["r"].append(ret)
    out={}
    for t,v in by.items():
        n=v["c"]+v["w"]
        if not n: continue
        avg=sum(v["r"])/len(v["r"]) if v["r"] else 0
        sharpe=None
        if len(v["r"])>=3:
            std=math.sqrt(sum((r-avg)**2 for r in v["r"])/len(v["r"]))
            sharpe=round(avg/std,2) if std>0 else None
        out[t]={"accuracy":round(v["c"]/n,3),"n":n,"correct":v["c"],"wrong":v["w"],"avg_return":round(avg,2),"sharpe":sharpe}
    return out

def self_improve(outcomes,templates,accuracy):
    # Loop 3: this DAILY function is now a no-op. Prompt iteration
    # moved to weekly justhodl-prompt-iterator Lambda which has safety
    # guardrails (length validation, content checks, version tracking).
    # The old daily iteration ran on noise (most outcomes have
    # correct=None today) and could randomly degrade brief quality.
    return templates, None
    # ─── DISABLED CODE BELOW (preserved for reference) ───────────────
    wrong=sorted([o for o in outcomes if o.get("correct") is False],
                 key=lambda x:x.get("checked_at",""),reverse=True)[:8]
    if not wrong: return templates,None
    lines=[]
    for o in wrong:
        ret=float(o.get("outcome",{}).get("return_pct",o.get("outcome",{}).get("excess_return",0)) or 0)
        lines.append("- "+str(o.get("signal_type"))+": pred="+str(o.get("predicted_dir"))+" actual="+str(o.get("outcome",{}).get("actual_direction","?"))+" ("+str(round(ret,1))+"%) ["+str(o.get("window_key"))+"]")
    err="\n".join(lines)
    worst=sorted([(k,v) for k,v in accuracy.items() if v.get("n",0)>=3],key=lambda x:x[1].get("accuracy",1))[:3]
    worst_txt="\n".join(["- "+s+": "+str(round(v.get("accuracy",0)*100))+"% ("+str(v.get("n"))+" outcomes)" for s,v in worst]) or "No data yet"
    analysis=ai("You are a quant analyst reviewing JustHodlAI prediction failures.\n\nWrong predictions:\n"+err+"\n\nWorst signals:\n"+worst_txt+"\n\nIn 3 sentences: what caused failures and what to weight differently?",max_tokens=250)
    new_prompt=ai("Improve the JustHodlAI morning brief prompt based on empirical failures. Keep under 250 words. Return ONLY the new prompt text.\n\nCurrent prompt:\n"+templates["morning_brief"]+"\n\nFailures:\n"+err+"\n\nAnalysis: "+str(analysis)+"\n\nWorst signals: "+worst_txt+"\n\nWrite improved prompt:",max_tokens=300)
    if new_prompt and len(new_prompt)>60:
        old=templates["morning_brief"]
        templates["morning_brief"]=new_prompt
        save_templates(templates)
        try:
            existing=fs3(IMPROVEMENTS_KEY)
            if not isinstance(existing,list): existing=[]
            existing.append({"date":datetime.now(timezone.utc).isoformat(),"errors":err,"analysis":analysis,"old":old,"new":new_prompt,"v":len(existing)+1})
            s3.put_object(Bucket=S3_BUCKET,Key=IMPROVEMENTS_KEY,Body=json.dumps(existing[-90:],indent=2),ContentType="application/json")
        except Exception as e: print("[LOG] "+str(e))
    return templates,analysis

def build_brief(templates,m,perf,err_analysis,weights,accuracy):
    now_et=datetime.now(timezone(timedelta(hours=-5)))
    date_str=now_et.strftime("%a %b %d, %Y")
    pl=[]
    if perf:
        for t,v in sorted(perf.items(),key=lambda x:x[1]["accuracy"],reverse=True)[:8]:
            em="OK" if v["accuracy"]>=0.65 else "~" if v["accuracy"]>=0.50 else "X"
            sh=" S:"+str(v["sharpe"]) if v.get("sharpe") else ""
            wt=weights.get(t,1.0)
            ws=" w:"+str(round(wt,1)) if wt!=1.0 else ""
            pl.append(em+" "+t.replace("_"," ").title()[:22]+": "+str(round(v["accuracy"]*100))+"% ("+str(v["n"])+sh+ws+") avg:"+str(v["avg_return"])+"%")
    perf_txt="\n".join(pl) or "Building baseline"
    tw="\n".join(["* "+k.replace("_"," ")+": w="+str(round(v,2)) for k,v in m.get("top_weights",[])]) or "Default weights"
    learned="SYSTEM LEARNED: "+str(err_analysis) if err_analysis else ""
    parts=[
        templates["morning_brief"],
        "",
        "=== LIVE DATA "+date_str+" ===",
        "KHALID: "+str(m["khalid_raw"])+"/100 weight:"+str(round(m["khalid_weight"],2))+"x adj:"+str(m["khalid_adj"])+" REGIME:"+str(m["khalid_regime"])+" PHASE:"+str(m["phase"]),
        "EDGE: "+str(m["edge_score"])+"/100 ("+str(m["edge_regime"])+") ML_RISK:"+str(m["ml_risk"])+" CARRY:"+str(m["carry_risk"])+" CRISIS_DIST:"+str(m["crisis_dist"])+"pts",
        # ─── Phase 1A: Bond regime (added 2026-04-25) ───
        "BOND_REGIME: "+str(m["bond_regime"])+" strength="+str(m["bond_regime_strength"])+"/100 extreme="+str(m["bond_extreme_count"])+"/"+str(m["bond_total_count"])+" (risk_off:"+str(m["bond_n_off"])+" risk_on:"+str(m["bond_n_on"])+")"+(" REGIME_CHANGED" if m["bond_changed"] else "")+(" extremes:"+",".join([s[0]+"("+("+" if s[1]>=0 else "")+str(round(s[1],1))+")" for s in m["bond_extreme_signals"]]) if m["bond_extreme_signals"] else ""),
        # ─── Phase 1B: Cross-asset divergence ───
        "DIVERGENCE: "+str(m["divergence_extreme_count"])+" pairs >2σ, "+str(m["divergence_alert_count"])+" >3σ alerts"+(" TOP:"+";".join([d[0]+"("+("+" if d[1]>=0 else "")+str(round(d[1],1))+")" for d in m["divergence_top"]]) if m["divergence_top"] else ""),
        "FORECAST: "+str(m["forecast"]),
        "MOVES: SPY:"+str(m["spy"])+"% TLT:"+str(m["tlt"])+"% GLD:"+str(m["gld"])+"% QQQ:"+str(m["qqq"])+"% DXY:"+str(m["uup"])+"%",
        "PLUMBING: Stress:"+str(m["stress_score"])+"/100 ("+str(m["stress_status"])+") Phase:"+str(m["stress_phase"])+" RedFlags:"+str(m["red_flags"])+" Flags:"+str(", ".join(m["flags"]) or "none"),
        "CRYPTO: F&G:"+str(m["fg"])+"/100 ("+str(m["fg_label"])+") CryptoRisk:"+str(m["crypto_risk"])+"/100 ("+str(m["crypto_regime"])+") Action:"+str(m["crypto_action"]),
        "BTC: $"+str(m["btc_price"])+" 24h:"+str(m["btc_24h"])+"% 7d:"+str(m["btc_7d"])+"% ATH_down:"+str(m["btc_ath_chg"])+"%",
        "BTC_FUNDING: "+str(m["btc_funding_pct"])+"% ("+str(m["btc_funding_annual"])+"% annual) Sentiment:"+str(m["btc_sentiment"]),
        "ETH: $"+str(m["eth_price"])+" 24h:"+str(m["eth_24h"])+"% Sentiment:"+str(m["eth_sentiment"]),
        "ONCHAIN: MVRV:"+str(m["mvrv"])+" Signal:"+str(m["onchain_signal"])+" Momentum30d:"+str(m["onchain_momentum"]),
        "VALUATIONS: CAPE:"+str(m["cape"])+" Buffett:"+str(m["buffett"])+"%",
        "OPTIONS: Bias:"+str(m["options_bias"])+" P/C:"+str(m["pc"]),
        "PICKS: "+str(", ".join(m["picks"])),
        "ALERTS: "+str(", ".join(m["alerts"]) or "none"),
        "TOP TRUSTED SIGNALS:",
        tw,
        "SIGNAL ACCURACY 7d:",
        perf_txt,
        learned,
        "Write the morning brief using real numbers above. Use emojis and Markdown bold."
    ]
    prompt="\n".join(parts)
    brief=ai(prompt,max_tokens=600)
    if not brief:
        em="HIGH RISK" if float(m["khalid_raw"] or 0)>=70 else "ELEVATED" if float(m["khalid_raw"] or 0)>=40 else "LOW RISK"
        brief=("JustHodl Brief "+date_str+"\n\n"
               "Khalid: "+str(m["khalid_raw"])+"/100 ("+str(m["khalid_regime"])+") "+em+"\n"
               "Edge: "+str(m["edge_score"])+" Phase: "+str(m["phase"])+"\n"
               "SPY:"+str(m["spy"])+"% TLT:"+str(m["tlt"])+"% GLD:"+str(m["gld"])+"%\n"
               "BTC $"+str(m["btc_price"])+" ("+str(m["btc_24h"])+"% 24h) Funding:"+str(m["btc_sentiment"])+"\n"
               "MVRV:"+str(m["mvrv"])+" ("+str(m["onchain_signal"])+")\n"
               "F&G:"+str(m["fg"])+"/100 ("+str(m["fg_label"])+")\n"
               "Plumbing:"+str(m["stress_score"])+"/100\n"
               "Picks: "+str(", ".join(m["picks"][:3])))
    return brief

def format_accuracy(perf,accuracy,weights):
    if not perf:
        run=fs3("learning/last_log_run.json")
        return ("Signal Accuracy\n\nBuilding baseline - first results in 7 days\n"
                "Signals logged this run: "+str(run.get("count",0))+"\n"
                "Sources: Khalid, CFTC(29 contracts), Screener(503 stocks), Crypto, Edge, Repo, Valuations, Options, ML")
    lines=["Signal Accuracy - Last 7 Days\n"]
    for t,v in sorted(perf.items(),key=lambda x:x[1]["accuracy"],reverse=True)[:12]:
        em="OK" if v["accuracy"]>=0.65 else "~" if v["accuracy"]>=0.50 else "X"
        sh=" S:"+str(v["sharpe"]) if v.get("sharpe") else ""
        wt=weights.get(t,1.0)
        ws=" w:"+str(round(wt,1)) if wt!=1.0 else ""
        lines.append(em+" "+t.replace("_"," ").title()[:25]+": "+str(round(v["accuracy"]*100))+"% ("+str(v["n"])+sh+ws+") "+str(v["avg_return"])+"%")
    c=sum(v["correct"] for v in perf.values())
    w2=sum(v["wrong"] for v in perf.values())
    if c+w2>0:
        lines.append("\nOverall: "+str(round(c/(c+w2)*100))+"% ("+str(c)+" correct "+str(w2)+" wrong across "+str(len(perf))+" signal types)")
    return "\n".join(lines)

def lambda_handler(event,context):
    print("[START] morning-intelligence v3 action="+str(event.get("action","morning")))
    chat_id=gp(CHAT_ID_PARAM)
    if not chat_id:
        print("[ERROR] No chat ID in SSM at "+CHAT_ID_PARAM)
        return {"statusCode":500,"body":"No chat ID"}
    weights=load_weights()
    accuracy=load_accuracy()
    templates=load_templates()
    print("[WEIGHTS] "+str(len(weights))+" loaded")
    all_data=load_all()
    m=extract_metrics(all_data,weights)
    print("[DATA] Khalid="+str(m["khalid_raw"])+" BTC=$"+str(m["btc_price"])+" FG="+str(m["fg"])+" MVRV="+str(m["mvrv"]))
    outcomes=get_outcomes(7)
    perf=perf_summary(outcomes)
    wrong_count=sum(1 for o in outcomes if o.get("correct") is False)
    err_analysis=None
    if wrong_count>=2:
        templates,err_analysis=self_improve(outcomes,templates,accuracy)
    brief=build_brief(templates,m,perf,err_analysis,weights,accuracy)
    stg(chat_id,brief)
    time.sleep(1)
    stg(chat_id,format_accuracy(perf,accuracy,weights))
    time.sleep(1)
    if err_analysis:
        imp=fs3(IMPROVEMENTS_KEY)
        v=len(imp) if isinstance(imp,list) else 0
        stg(chat_id,"System Self-Improved (v"+str(v)+")\n\n"+str(err_analysis)+"\n\nPrompt updated. Log: S3/learning/improvement_log.json")
    s3.put_object(Bucket=S3_BUCKET,Key="learning/morning_run_log.json",
        Body=json.dumps({"run_at":datetime.now(timezone.utc).isoformat(),"outcomes":len(outcomes),"wrong":wrong_count,"improved":err_analysis is not None,"weights":len(weights),"khalid":m["khalid_raw"],"regime":m["khalid_regime"]}),
        ContentType="application/json")
    print("[DONE] Sent. Khalid="+str(m["khalid_raw"])+" BTC="+str(m["btc_price"])+" outcomes="+str(len(outcomes)))
    return {"statusCode":200,"body":json.dumps({"success":True,"khalid":m["khalid_raw"],"khalid_adj":m["khalid_adj"],"regime":m["khalid_regime"],"btc":m["btc_price"],"outcomes":len(outcomes),"improved":err_analysis is not None,"weights_active":len(weights)})}
