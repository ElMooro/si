import os
import json,urllib3,boto3,os,time
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime
http=urllib3.PoolManager()
s3_client=boto3.client("s3",region_name="us-east-1")
FMP_KEY=os.environ.get("FMP_KEY","wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
ANT_KEY=os.environ.get("ANTHROPIC_KEY",os.environ.get('ANTHROPIC_API_KEY', ''))
BUCKET="justhodl-dashboard-live"
FMP_BASE="https://financialmodelingprep.com"
ANT_URL="https://api.anthropic.com/v1/messages"
AGENTS={
  "buffett":{"name":"Warren Buffett","title":"The Oracle of Omaha","icon":"B","color":"#F59E0B","philosophy":"You are Warren Buffett. Focus on owner earnings, ROE>15%, low debt, wide margins, margin of safety. Hold forever. Love simple durable high-margin businesses with pricing power.","key_metrics":["ownerEarningsYield","roe","netMargin","debtEquity","currentRatio","pb","pe","fcfYield"]},
  "munger":{"name":"Charlie Munger","title":"Buffetts Partner","icon":"M","color":"#10B981","philosophy":"You are Charlie Munger. Only buy wonderful businesses with ROIC>20%, wide moats, exceptional management at fair prices. Gross margins reveal the moat. Second-order thinking always.","key_metrics":["roic","grossMargin","netMargin","roe","pe","pb","debtEquity"]},
  "burry":{"name":"Michael Burry","title":"The Big Short","icon":"R","color":"#EF4444","philosophy":"You are Michael Burry. Deep contrarian value investor. Obsess over FCF yield, P/B, Altman Z-Score, balance sheet stress. Comfortable being early. Look for catalysts to unlock hidden value.","key_metrics":["pb","pfcf","altmanZ","piotroski","currentRatio","debtEquity","priceChange1Y","fcfYield"]},
  "druckenmiller":{"name":"Stanley Druckenmiller","title":"Macro Legend","icon":"D","color":"#3B82F6","philosophy":"You are Stanley Druckenmiller. Combine macro with earnings momentum and price action. Size aggressively when macro tailwind plus fundamentals plus price trend align. Never fight the Fed.","key_metrics":["priceChange1M","priceChange3M","priceChange1Y","revenueGrowth","epsGrowth","analystUpside","pe"]},
  "lynch":{"name":"Peter Lynch","title":"The Growth Hunter","icon":"L","color":"#8B5CF6","philosophy":"You are Peter Lynch. PEG below 1.0 is a bargain above 2.0 overpriced. Want consistent earnings growth 10-25%, reasonable P/E, simple understandable business Wall Street ignores.","key_metrics":["peg","pe","revenueGrowth","epsGrowth","netMargin","priceToSales","priceChange1Y"]},
  "wood":{"name":"Cathie Wood","title":"Innovation Visionary","icon":"W","color":"#EC4899","philosophy":"You are Cathie Wood. Disruptive innovation with 5-year horizons. Accelerating revenue growth, network effects, large TAM being disrupted. High P/S acceptable if growth is 40%+.","key_metrics":["revenueGrowth","epsGrowth","priceToSales","priceChange1Y","grossMargin","dcfUpside","analystUpside"]}
}
SIGNALS=["STRONG BUY","BUY","HOLD","SELL","STRONG SELL"]

def fmp(endpoint,params=None,retries=2):
    p=dict(params or {})
    p["apikey"]=FMP_KEY
    url=FMP_BASE+"/"+endpoint+"?"+("&".join(str(k)+"="+str(v) for k,v in p.items()))
    for attempt in range(retries+1):
        try:
            r=http.request("GET",url,timeout=8)
            if r.status==200:
                return json.loads(r.data.decode("utf-8"))
            if r.status==429:
                time.sleep(1.5)
        except Exception as e:
            if attempt==retries:
                print("FMP error "+endpoint+": "+str(e))
    return None

def get_stock_data(ticker):
    ticker=ticker.upper().strip()
    results={}
    endpoints={
        "profile":("stable/profile",{"symbol":ticker}),
        "ratios":("stable/ratios-ttm",{"symbol":ticker}),
        "scores":("stable/scores",{"symbol":ticker}),
        "owner_earn":("stable/owner-earnings",{"symbol":ticker}),
        "price_chg":("stable/stock-price-change",{"symbol":ticker}),
        "pt_consensus":("stable/price-target-consensus",{"symbol":ticker}),
        "grades":("stable/grades-consensus",{"symbol":ticker}),
        "dcf":("stable/discounted-cash-flow",{"symbol":ticker}),
        "income_growth":("stable/income-statement-growth",{"symbol":ticker,"limit":"4"})
    }
    def fetch_one(key,ep,params):
        return key,fmp(ep,params)
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures={ex.submit(fetch_one,k,ep,pm):k for k,(ep,pm) in endpoints.items()}
        for f in as_completed(futures):
            try:
                key,data=f.result()
                results[key]=data
            except Exception as e:
                print("Fetch error: "+str(e))
    return results

def get_macro_context():
    try:
        obj=s3_client.get_object(Bucket=BUCKET,Key="data/report.json")
        data=json.loads(obj["Body"].read().decode("utf-8"))
        ki=data.get("khalidIndex",{})
        regime=data.get("macroRegime",{})
        return{"khalid_score":ki.get("score",50),"regime":regime.get("name","STABLE"),"fed_rate":data.get("fedFundsRate",{}).get("value","N/A"),"inflation":data.get("cpi",{}).get("value","N/A")}
    except Exception as e:
        print("Macro error: "+str(e))
        return{"khalid_score":50,"regime":"UNKNOWN","fed_rate":"N/A","inflation":"N/A"}

def normalize_metrics(raw,ticker):
    def safe(d,default=None):
        if not d:return default
        return d[0] if isinstance(d,list) and d else (d if isinstance(d,dict) else default)
    profile=safe(raw.get("profile"),{})
    ratios=safe(raw.get("ratios"),{})
    scores=safe(raw.get("scores"),{})
    oe=safe(raw.get("owner_earn"),{})
    pc=safe(raw.get("price_chg"),{})
    pt=safe(raw.get("pt_consensus"),{})
    grades=safe(raw.get("grades"),{})
    dcf=safe(raw.get("dcf"),{})
    ig=raw.get("income_growth",[]) or []
    price=profile.get("price",0) or 0
    dcf_val=dcf.get("dcf",0) or 0
    dcf_up=min(round(((dcf_val-price)/price)*100,1) if price>0 and dcf_val>0 else 0,300)
    sb=grades.get("strongBuy",0) or 0
    bg=grades.get("buy",0) or 0
    hg=grades.get("hold",0) or 0
    sg=grades.get("sell",0) or 0
    ss=grades.get("strongSell",0) or 0
    tg=sb+bg+hg+sg+ss
    buy_pct=round((sb+bg)/tg*100) if tg>0 else 0
    ptt=pt.get("targetConsensus",0) or 0
    aup=round(((ptt-price)/price)*100,1) if price>0 and ptt>0 else 0
    oe_ps=oe.get("ownersEarningsPerShare",0) or 0
    oe_y=round((oe_ps/price)*100,2) if price>0 and oe_ps>0 else 0
    rg=[g.get("revenueGrowth",0) or 0 for g in ig[:4] if isinstance(g,dict)]
    eg=[g.get("epsgrowth",0) or g.get("netIncomeGrowth",0) or 0 for g in ig[:4] if isinstance(g,dict)]
    rev_g=round(sum(rg)/len(rg)*100,1) if rg else 0
    eps_g=round(sum(eg)/len(eg)*100,1) if eg else 0
    pe=ratios.get("peRatioTTM",0) or 0
    peg=round(pe/eps_g,2) if eps_g>0 and pe>0 else None
    return{"ticker":ticker,"name":profile.get("companyName",ticker),"sector":profile.get("sector","N/A"),"industry":profile.get("industry","N/A"),"price":price,"mktCap":round((profile.get("mktCap",0) or 0)/1e9,1),"pe":round(pe,1),"pb":round(ratios.get("priceToBookRatioTTM",0) or 0,2),"priceToSales":round(ratios.get("priceToSalesRatioTTM",0) or 0,2),"pfcf":round(ratios.get("priceToFreeCashFlowsRatioTTM",0) or 0,1),"peg":peg,"dcfUpside":dcf_up,"analystUpside":aup,"buyPct":buy_pct,"roe":round((ratios.get("returnOnEquityTTM",0) or 0)*100,1),"roic":round((ratios.get("returnOnCapitalEmployedTTM",0) or ratios.get("returnOnInvestedCapitalTTM",0) or 0)*100,1),"netMargin":round((ratios.get("netProfitMarginTTM",0) or 0)*100,1),"grossMargin":round((ratios.get("grossProfitMarginTTM",0) or 0)*100,1),"fcfYield":round((ratios.get("freeCashFlowYieldTTM",0) or 0)*100,2),"debtEquity":round(ratios.get("debtEquityRatioTTM",0) or 0,2),"currentRatio":round(ratios.get("currentRatioTTM",0) or 0,2),"piotroski":scores.get("piotroskiScore","N/A"),"altmanZ":round(scores.get("altmanZScore",0) or 0,2),"ownerEarningsYield":oe_y,"ownerEarningsPS":round(oe_ps,2),"revenueGrowth":rev_g,"epsGrowth":eps_g,"priceChange1D":round(pc.get("1D",0) or 0,2),"priceChange1M":round(pc.get("1M",0) or 0,2),"priceChange3M":round(pc.get("3M",0) or 0,2),"priceChange6M":round(pc.get("6M",0) or 0,2),"priceChange1Y":round(pc.get("1Y",0) or 0,2),"priceChangeYTD":round(pc.get("YTD",0) or 0,2)}

def run_investor_agent(agent_key,agent_cfg,metrics,macro):
    try:
        relevant={k:metrics.get(k,"N/A") for k in agent_cfg["key_metrics"]}
        parts=[
            "Analyze "+metrics["ticker"]+" ("+metrics["name"]+") "+metrics["sector"],
            "Price: $"+str(metrics["price"])+" MktCap: $"+str(metrics["mktCap"])+"B",
            "KEY METRICS: "+json.dumps(relevant),
            "ALL METRICS: "+json.dumps({k:v for k,v in metrics.items() if k not in ["ticker","name","sector","industry","price","mktCap"]}),
            "MACRO: Khalid="+str(macro.get("khalid_score","N/A"))+"/100 Regime="+str(macro.get("regime","N/A"))+" Fed="+str(macro.get("fed_rate","N/A"))+"% CPI="+str(macro.get("inflation","N/A"))+"%",
            'Respond ONLY as valid JSON: {"signal":"<STRONG BUY|BUY|HOLD|SELL|STRONG SELL>","conviction":<1-10>,"thesis":"<2-3 sentences citing numbers>","bull_case":"<1 sentence>","bear_case":"<1 sentence>","key_metric":"<most important metric>"}'
        ]
        uc=" ".join(parts)
        body=json.dumps({"model":"claude-haiku-4-5-20251001","max_tokens":600,"system":agent_cfg["philosophy"],"messages":[{"role":"user","content":uc}]})
        r=http.request("POST",ANT_URL,body=body.encode("utf-8"),headers={"Content-Type":"application/json","x-api-key":ANT_KEY,"anthropic-version":"2023-06-01"},timeout=25)
        if r.status!=200:
            raise Exception("Anthropic "+str(r.status)+": "+r.data[:500].decode("utf-8"))
        resp=json.loads(r.data.decode("utf-8"))
        text=resp["content"][0]["text"].strip()
        if text.startswith("```"):
            text=text.split("\n",1)[1].rsplit("```",1)[0].strip()
        verdict=json.loads(text)
        verdict.update({"agent":agent_key,"name":agent_cfg["name"],"title":agent_cfg["title"],"icon":agent_cfg["icon"],"color":agent_cfg["color"]})
        verdict["signal"]=verdict.get("signal","HOLD").upper()
        verdict["conviction"]=max(1,min(10,int(verdict.get("conviction",5))))
        return verdict
    except Exception as e:
        return{"agent":agent_key,"name":agent_cfg["name"],"title":agent_cfg["title"],"icon":agent_cfg["icon"],"color":agent_cfg["color"],"signal":"HOLD","conviction":5,"thesis":"Unavailable: "+str(e)[:80],"bull_case":"N/A","bear_case":"N/A","key_metric":"N/A"}

def build_consensus(verdicts,metrics,macro):
    w={"STRONG BUY":2,"BUY":1,"HOLD":0,"SELL":-1,"STRONG SELL":-2}
    counts={s:0 for s in SIGNALS}
    ts=0
    tc=0
    for v in verdicts:
        sig=v.get("signal","HOLD")
        conv=v.get("conviction",5)
        ts+=w.get(sig,0)*conv
        tc+=conv
        counts[sig]=counts.get(sig,0)+1
    avg=ts/tc if tc>0 else 0
    if avg>1.2:sig="STRONG BUY"
    elif avg>0.4:sig="BUY"
    elif avg>-0.4:sig="HOLD"
    elif avg>-1.2:sig="SELL"
    else:sig="STRONG SELL"
    conv=max(1,min(10,round(abs(avg)/2*10)))
    bulls=counts["STRONG BUY"]+counts["BUY"]
    bears=counts["STRONG SELL"]+counts["SELL"]
    return{"signal":sig,"conviction":conv,"bulls":bulls,"bears":bears,"holds":counts["HOLD"],"signal_breakdown":counts,"score":round(avg,2),"summary":str(bulls)+" of 6 legends bullish on "+metrics["ticker"]+". Regime: "+str(macro.get("regime","STABLE"))+" Khalid: "+str(macro.get("khalid_score","N/A"))+"/100. Consensus: "+sig+" conviction "+str(conv)+"/10."}

def lambda_handler(event,context):
    cors={"Access-Control-Allow-Origin":"*","Access-Control-Allow-Headers":"Content-Type","Access-Control-Allow-Methods":"POST,OPTIONS"}
    if event.get("requestContext",{}).get("http",{}).get("method")=="OPTIONS":
        return{"statusCode":200,"headers":cors,"body":""}
    try:
        body=event.get("body","{}")
        if isinstance(body,str):
            body=json.loads(body)
        ticker=(body.get("ticker") or "").upper().strip()
        if not ticker:
            return{"statusCode":400,"headers":cors,"body":json.dumps({"error":"ticker required"})}
        print("[InvestorAgents] Analyzing "+ticker)
        start=time.time()
        raw=get_stock_data(ticker)
        metrics=normalize_metrics(raw,ticker)
        macro=get_macro_context()
        verdicts=[]
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures={ex.submit(run_investor_agent,k,cfg,metrics,macro):k for k,cfg in AGENTS.items()}
            for f in as_completed(futures,timeout=30):
                try:
                    verdicts.append(f.result())
                except Exception as e:
                    print("Verdict error: "+str(e))
        verdicts.sort(key=lambda x:x.get("conviction",0),reverse=True)
        consensus=build_consensus(verdicts,metrics,macro)
        result={"ticker":ticker,"name":metrics["name"],"sector":metrics["sector"],"price":metrics["price"],"metrics":metrics,"macro":macro,"agents":verdicts,"consensus":consensus,"generated":datetime.utcnow().isoformat()+"Z","elapsed":round(time.time()-start,1)}
        try:
            s3_client.put_object(Bucket=BUCKET,Key="investor-analysis/"+ticker+".json",Body=json.dumps(result).encode("utf-8"),ContentType="application/json",CacheControl="max-age=3600")
        except Exception as e:
            print("S3 error: "+str(e))
        print("[InvestorAgents] "+ticker+" done in "+str(result["elapsed"])+"s - "+consensus["signal"])
        return{"statusCode":200,"headers":{**cors,"Content-Type":"application/json"},"body":json.dumps(result)}
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return{"statusCode":500,"headers":cors,"body":json.dumps({"error":str(e)})}
