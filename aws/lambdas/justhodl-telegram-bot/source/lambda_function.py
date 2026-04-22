import json,os,re,boto3,urllib.request,traceback
from datetime import datetime,timezone

TELEGRAM_TOKEN=os.environ.get("TELEGRAM_TOKEN","")
ANTHROPIC_KEY=os.environ.get("ANTHROPIC_API_KEY","")
S3_BUCKET="justhodl-dashboard-live"
TELEGRAM_API=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
STOCK_URL="https://enxmdjjowjfwiydpslyykokjee0qdvml.lambda-url.us-east-1.on.aws/"
CFTC_URL="https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/"
EDGE_URL="https://vsxv2775x5aojiuwaoqb7wipam0rmuln.lambda-url.us-east-1.on.aws/"

def http_get(url,timeout=15):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"JustHodl-Bot/1.0"})
        with urllib.request.urlopen(req,timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"error":str(e)}

def http_post(url,payload,timeout=25):
    try:
        data=json.dumps(payload).encode()
        req=urllib.request.Request(url,data=data,headers={"Content-Type":"application/json"})
        with urllib.request.urlopen(req,timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"error":str(e)}

def send_message(chat_id,text,parse_mode="Markdown",reply_markup=None):
    payload={"chat_id":chat_id,"text":text[:4096],"parse_mode":parse_mode,"disable_web_page_preview":True}
    if reply_markup: payload["reply_markup"]=reply_markup
    result=http_post(f"{TELEGRAM_API}/sendMessage",payload)
    if isinstance(result,dict) and result.get("error"):
        payload2={"chat_id":chat_id,"text":text[:4096],"disable_web_page_preview":True}
        if reply_markup: payload2["reply_markup"]=reply_markup
        return http_post(f"{TELEGRAM_API}/sendMessage",payload2)
    return result
def send_typing(chat_id):
    http_post(f"{TELEGRAM_API}/sendChatAction",{"chat_id":chat_id,"action":"typing"})

def n(val,dec=1):
    if val is None or val=="N/A": return "N/A"
    try: return f"{float(val):,.{dec}f}"
    except: return str(val)

def regime_emoji(r):
    r=str(r).upper()
    if "BULL" in r: return "[BULL]"
    if "BEAR" in r: return "[BEAR]"
    if "CRISIS" in r: return "[CRISIS]"
    if "CAUTION" in r: return "[CAUTION]"
    return "[NEUTRAL]"

def fear_label(s):
    try:
        v=int(s)
        if v<20: return "Extreme Fear"
        if v<40: return "Fear"
        if v<60: return "Neutral"
        if v<80: return "Greed"
        return "Extreme Greed"
    except: return str(s)

def get_s3_json(key):
    try:
        s3=boto3.client("s3",region_name="us-east-1")
        obj=s3.get_object(Bucket=S3_BUCKET,Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return {}

def get_report():
    return get_s3_json("data/report.json")

def get_crypto_intel():
    return get_s3_json("crypto-intel.json")

def parse_report(r):
    out={}
    if not isinstance(r,dict): return out
    ki=r.get("khalid_index",{})
    if isinstance(ki,dict):
        out["score"]=ki.get("score")
        out["regime"]=ki.get("regime")
        out["signals"]=ki.get("signals",[])
    rd=r.get("risk_dashboard",{})
    if isinstance(rd,dict):
        out["risk_credit"]=rd.get("credit")
        out["risk_liquidity"]=rd.get("liquidity")
        out["risk_market"]=rd.get("market")
    cr=r.get("crypto",{})
    if isinstance(cr,dict):
        btc=cr.get("BTC",{})
        eth=cr.get("ETH",{})
        out["btc"]=btc.get("price") if isinstance(btc,dict) else None
        out["eth"]=eth.get("price") if isinstance(eth,dict) else None
    cg=r.get("crypto_global",{})
    if isinstance(cg,dict):
        out["total_mcap"]=cg.get("total_mcap")
        out["btc_dom"]=cg.get("btc_dom")
    fred=r.get("fred",{})
    if isinstance(fred,dict):
        for sec in fred.values():
            if isinstance(sec,dict):
                for k,v in sec.items():
                    if "vix" in k.lower() and isinstance(v,dict) and v.get("current"):
                        out["vix"]=v["current"]
        treas=fred.get("treasury",{})
        if isinstance(treas,dict):
            t10=treas.get("DGS10",{}); t2=treas.get("DGS2",{})
            v10=t10.get("current") if isinstance(t10,dict) else None
            v2=t2.get("current") if isinstance(t2,dict) else None
            if v10: out["t10y"]=v10
            if v10 and v2:
                try: out["yield_curve"]=round(float(v10)-float(v2),3)
                except: pass
        ice=fred.get("ice_bofa",{})
        if isinstance(ice,dict):
            for k,v in ice.items():
                if "hy" in k.lower() and isinstance(v,dict) and v.get("current"):
                    raw=float(v["current"])
                    out["hy_spread"]=round(raw*100,0) if raw<20 else raw; break
        dxy=fred.get("dxy",{})
        if isinstance(dxy,dict):
            for k,v in dxy.items():
                if isinstance(v,dict) and v.get("current"):
                    out["dxy"]=v["current"]; break
    cftc=r.get("cftc_positioning",{})
    if isinstance(cftc,dict):
        out["cftc_score"]=cftc.get("positioning_score")
        out["cftc_appetite"]=cftc.get("risk_appetite")
        out["cftc_crisis"]=cftc.get("crisis_score")
        out["cftc_smart_money"]=str(cftc.get("smart_money",""))
        out["cftc_crisis_level"]=str(cftc.get("crisis_level",""))
        out["cftc_summary"]=str(cftc.get("summary",""))
        sp=cftc.get("sector_positioning",{})
        if isinstance(sp,dict):
            out["cftc_signals"]=list(sp.items())[:6]
    nl=r.get("net_liquidity",{})
    if isinstance(nl,dict):
        out["net_liquidity"]=nl.get("net")
        out["fed_balance"]=nl.get("fed")
    stk=r.get("stocks",{})
    if isinstance(stk,dict):
        spy=stk.get("SPY",{}); qqq=stk.get("QQQ",{})
        out["spy"]=spy.get("price") if isinstance(spy,dict) else None
        out["qqq"]=qqq.get("price") if isinstance(qqq,dict) else None
    sigs=r.get("signals",{})
    if isinstance(sigs,dict):
        out["buys"]=sigs.get("buys",[])[:5]
        out["sells"]=sigs.get("sells",[])[:5]
        out["warnings"]=sigs.get("warnings",[])[:3]
    return out

def enrich_with_crypto_intel(d):
    ci=get_crypto_intel()
    if not ci: return d
    fg=ci.get("fear_greed",{})
    if isinstance(fg,dict) and fg.get("current"):
        d["fear_greed"]=fg["current"]
        d["fear_greed_label"]=fg.get("label","")
    tvl=ci.get("tvl",{})
    if isinstance(tvl,dict) and tvl.get("total"):
        v=float(tvl["total"])
        d["defi_tvl"]=round(v/1e9,1) if v>1e6 else v
    eg=ci.get("eth_gas",{})
    if isinstance(eg,dict):
        d["eth_gas"]=eg.get("fast",eg.get("standard","N/A"))
    rs=ci.get("risk_score",{})
    if isinstance(rs,dict) and rs.get("regime"):
        d["ml_regime"]=rs["regime"]+" -> "+rs.get("action","")
    fr=ci.get("funding",{}).get("rates",[])
    if isinstance(fr,list):
        d["funding_rates"]=fr[:5]
    oi=ci.get("onchain_ratios",{})
    if isinstance(oi,dict):
        d["mvrv"]=oi.get("mvrv_approx")
        d["onchain_signal"]=oi.get("signal")
    return d

def ask_claude(question,context=None):
    system="You are JustHodl.AI institutional market intelligence. Be concise, use bullets with -, bold with *. Max 400 chars. No disclaimers."
    user=question
    if context:
        ctx={k:v for k,v in context.items() if k not in ("signals","cftc_signals","buys","sells","warnings","funding_rates")}
        user=f"Live data: {json.dumps(ctx,default=str)[:1500]}\n\nQuestion: {question}"
    payload={"model":"claude-haiku-4-5-20251001","max_tokens":400,"system":system,"messages":[{"role":"user","content":user}]}
    data=json.dumps(payload).encode()
    req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=data,
        headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"})
    try:
        with urllib.request.urlopen(req,timeout=25) as r:
            return json.loads(r.read().decode())["content"][0]["text"]
    except Exception as e:
        return f"AI unavailable: {e}"

def get_alert_state():
    try:
        s3=boto3.client("s3",region_name="us-east-1")
        obj=s3.get_object(Bucket=S3_BUCKET,Key="telegram/alert_state.json")
        return json.loads(obj["Body"].read())
    except:
        return {"subscribers":[],"last_regime":None,"last_khalid":None,"last_fear":None}

def save_alert_state(state):
    s3=boto3.client("s3",region_name="us-east-1")
    s3.put_object(Bucket=S3_BUCKET,Key="telegram/alert_state.json",
        Body=json.dumps(state),ContentType="application/json")

def cmd_start(chat_id):
    text=("*JustHodl.AI Intelligence Bot*\n"
          "---\n"
          "Institutional-grade market intelligence.\n\n"
          "*Commands:*\n"
          "- /briefing - Full morning briefing\n"
          "- /khalid - Khalid Index + regime\n"
          "- /crypto - Crypto intelligence\n"
          "- /cftc - CFTC positioning\n"
          "- /edge - Edge engine signals\n"
          "- /stock [TICKER] - Full stock analysis\n"
          "- /breadth - Market breadth + movers\n"
          "- /ask [question] - AI query\n"
          "- /subscribe - Proactive alerts ON\n"
          "- /unsubscribe - Alerts OFF\n"
          "- /status - System health\n\n"
          "Or just type any market question.")
    kb={"inline_keyboard":[
        [{"text":"Morning Briefing","callback_data":"briefing"},{"text":"Khalid Index","callback_data":"khalid"}],
        [{"text":"Crypto","callback_data":"crypto"},{"text":"CFTC","callback_data":"cftc"}],
        [{"text":"Edge Engine","callback_data":"edge"},{"text":"Subscribe Alerts","callback_data":"subscribe"}]]}
    send_message(chat_id,text,reply_markup=kb)

def cmd_briefing(chat_id):
    send_typing(chat_id)
    send_message(chat_id,"Loading intelligence from all systems...")
    d=enrich_with_crypto_intel(parse_report(get_report()))
    score=d.get("score","N/A"); regime=d.get("regime","Unknown")
    rc=d.get("risk_credit","N/A"); rl=d.get("risk_liquidity","N/A"); rm=d.get("risk_market","N/A")
    vix=d.get("vix","N/A"); yc=d.get("yield_curve","N/A"); hy=d.get("hy_spread","N/A")
    btc=d.get("btc","N/A"); fear=d.get("fear_greed","N/A"); defi=d.get("defi_tvl","N/A")
    cftc_score=d.get("cftc_score","N/A"); spy=d.get("spy","N/A"); nl=d.get("net_liquidity","N/A")
    now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    text=(f"*JustHodl.AI Morning Briefing*\n"
          f"{now}\n\n"
          f"*Khalid Index: {score}/100*\n"
          f"Regime: *{regime}*\n\n"
          f"*Risk Dashboard*\n"
          f"- Credit: {n(rc,0)}/100  Liquidity: {n(rl,0)}/100  Market: {n(rm,0)}/100\n\n"
          f"*Key Rates*\n"
          f"- VIX: {n(vix,1)}\n"
          f"- Yield Curve (10y-2y): {n(yc,3)}%\n"
          f"- HY Spread: {n(hy,0)}bps\n\n"
          f"*Crypto*\n"
          f"- BTC: ${n(btc,0)}\n"
          f"- Fear & Greed: {fear_label(fear)} ({fear})\n"
          f"- DeFi TVL: ${n(defi,1)}B\n\n"
          f"*Markets*\n"
          f"- SPY: ${n(spy,2)}\n"
          f"- CFTC Score: {n(cftc_score,1)}\n"
          f"- Net Liquidity: ${n(nl,0)}M\n\n"
          f"_/cftc /crypto /edge for deep dives_")
    send_message(chat_id,text)
    ctx={k:d[k] for k in ["score","regime","vix","btc","fear_greed","yield_curve","hy_spread","spy"] if k in d}
    ai=ask_claude("2-sentence institutional macro outlook based on this data. Be direct.",ctx)
    send_message(chat_id,f"*AI Macro Outlook*\n\n{ai}")

def cmd_khalid(chat_id):
    send_typing(chat_id)
    d=parse_report(get_report())
    score=d.get("score","N/A"); regime=d.get("regime","N/A")
    try: filled=int(float(score)/10); bar="["+"#"*filled+"-"*(10-filled)+"]"; lbl=f"{float(score):.0f}/100"
    except: bar="[----------]"; lbl=str(score)
    signals=d.get("signals",[])
    sig_text="\n".join(f"  - {s}" for s in signals[:5]) if signals else "  - No signals"
    text=(f"*Khalid Index*\n\n"
          f"`{bar}` *{lbl}*\n"
          f"Regime: *{regime}*\n\n"
          f"*Risk Components*\n"
          f"- Credit: {n(d.get('risk_credit'),0)}/100\n"
          f"- Liquidity: {n(d.get('risk_liquidity'),0)}/100\n"
          f"- Market: {n(d.get('risk_market'),0)}/100\n\n"
          f"*Key Rates*\n"
          f"- VIX: {n(d.get('vix'),1)}\n"
          f"- Yield Curve: {n(d.get('yield_curve'),3)}%\n"
          f"- HY Spread: {n(d.get('hy_spread'),0)}bps\n\n"
          f"*Signals*\n{sig_text}")
    send_message(chat_id,text)

def cmd_crypto(chat_id):
    send_typing(chat_id)
    d=enrich_with_crypto_intel(parse_report(get_report()))
    btc=d.get("btc","N/A"); eth=d.get("eth","N/A")
    fear=d.get("fear_greed","N/A"); defi=d.get("defi_tvl","N/A")
    gas=d.get("eth_gas","N/A"); ml=d.get("ml_regime","N/A")
    mcap=d.get("total_mcap"); mcap_str=f"${round(float(mcap)/1e12,2)}T" if mcap else "N/A"
    mvrv=d.get("mvrv"); oc_sig=d.get("onchain_signal","")
    fund_text=""
    for item in d.get("funding_rates",[])[:5]:
        sym=item.get("symbol",""); pct=item.get("funding_rate_pct",0); sent=item.get("sentiment","")
        sign="+" if float(pct)>=0 else ""
        fund_text+=f"  - {sym}: {sign}{float(pct):.4f}% ({sent})\n"
    if not fund_text: fund_text="  - See justhodl.ai/crypto-intel.html\n"
    text=(f"*Crypto Intelligence*\n\n"
          f"- BTC: ${n(btc,0)}\n"
          f"- ETH: ${n(eth,0)}\n"
          f"- Total Mcap: {mcap_str}\n"
          f"- Fear & Greed: {fear_label(fear)} ({fear})\n"
          f"- DeFi TVL: ${n(defi,1)}B\n"
          f"- ETH Gas: {n(gas,2)} Gwei\n"
          f"- ML Regime: {ml}\n"
          f"- MVRV: {n(mvrv,2)} ({oc_sig})\n\n"
          f"*Funding Rates*\n{fund_text}\n"
          f"_Full terminal: justhodl.ai/crypto-intel.html_")
    send_message(chat_id,text)

def cmd_cftc(chat_id):
    send_typing(chat_id)
    d=parse_report(get_report())
    cftc_score=d.get("cftc_score","N/A")
    appetite=d.get("cftc_appetite","N/A")
    crisis=d.get("cftc_crisis","N/A")
    sm=d.get("cftc_smart_money","")
    cl=d.get("cftc_crisis_level","")
    summary=d.get("cftc_summary","")
    lines=["*CFTC Futures Positioning*",""]
    lines.append(f"Positioning Score: *{n(cftc_score,1)}*")
    lines.append(f"Risk Appetite: *{n(appetite,1)}*")
    lines.append(f"Crisis Score: *{n(crisis,0)}*")
    if sm: lines.append(f"Smart Money: *{sm}*")
    if cl: lines.append(f"Crisis Level: {cl}")
    lines.append("")
    lines.append("*Sector Signals*")
    for sec,val in d.get("cftc_signals",[])[:6]:
        if isinstance(val,dict):
            bias=val.get("bias","N/A")
            b=val.get("bullish",""); br=val.get("bearish","")
            detail=f"{b}up/{br}dn" if b!="" else ""
            lines.append(f"  - {sec}: {bias}" + (f" ({detail})" if detail else ""))
    if summary: lines.append(f"\n_{summary[:100]}_")
    lines.append("")
    lines.append("_29 contracts - 7 sectors - Weekly COT_")
    lines.append("_Full: justhodl.ai/positioning/_")
    send_message(chat_id,"\n".join(lines))

def cmd_edge(chat_id):
    send_typing(chat_id)
    edge=http_get(EDGE_URL,timeout=20)
    if not isinstance(edge,dict) or "error" in edge:
        send_message(chat_id,f"Edge engine unavailable: {str(edge)[:100]}")
        return
    score=edge.get("composite_score",edge.get("edge_score","N/A"))
    alloc=edge.get("tactical_allocation",edge.get("recommendation","N/A"))
    regime=edge.get("market_regime",edge.get("regime","N/A"))
    engines=edge.get("engines",edge.get("engine_scores",{}))
    etxt=""
    if isinstance(engines,dict):
        for nm,dat in list(engines.items())[:5]:
            s=dat.get("score",dat.get("value","N/A")) if isinstance(dat,dict) else dat
            etxt+=f"  - {nm}: {s}\n"
    if not etxt: etxt="  - See justhodl.ai/edge.html\n"
    text=(f"*Edge Intelligence Engine*\n\n"
          f"Composite Score: *{score}*\n"
          f"Regime: {regime}\n"
          f"Allocation Signal: *{alloc}*\n\n"
          f"*Engine Breakdown*\n{etxt}\n"
          f"_Updated every 6h - justhodl.ai/edge.html_")

def build_chart_url(chart_config):
    """Build QuickChart.io URL for a Chart.js config"""
    import urllib.parse, json as _json
    cfg_str = _json.dumps(chart_config, separators=(',',':'))
    params = urllib.parse.urlencode({"c": cfg_str, "w": 800, "h": 400, "bkg": "#0a0e1a", "f": "png"})
    return f"https://quickchart.io/chart?{params}"

def send_chart(chat_id, title, chart_config):
    """Send a chart image to Telegram"""
    try:
        chart_url = build_chart_url(chart_config)
        payload = {"chat_id": chat_id, "photo": chart_url, "caption": title, "parse_mode": "Markdown"}
        http_post(f"{TELEGRAM_API}/sendPhoto", payload)
    except Exception as e:
        send_message(chat_id, f"Chart error: {str(e)[:80]}")

def send_stock_charts(chat_id, ticker, hist):
    """Generate and send 5 historical charts for a stock"""
    if not hist or not hist.get("labels"):
        send_message(chat_id, "_No historical data available for charts_")
        return

    labels = hist.get("labels", [])
    DARK = "#0a0e1a"
    GRID = "#2d3748"
    TEXT = "#a0aec0"

    def clean(arr):
        return [v if v is not None else None for v in arr]

    # Chart 1: Revenue & Net Income
    send_chart(chat_id, f"*{ticker}* Revenue & Net Income ($B) — Annual", {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [
                {"label": "Revenue ($B)", "data": clean(hist.get("revenue",[])),
                 "backgroundColor": "rgba(49,130,206,0.8)", "borderRadius": 3},
                {"label": "Net Income ($B)", "data": clean(hist.get("net_income",[])),
                 "backgroundColor": "rgba(72,187,120,0.8)", "borderRadius": 3}
            ]
        },
        "options": {
            "plugins": {"legend": {"labels": {"color": TEXT}}},
            "scales": {
                "x": {"ticks": {"color": TEXT}, "grid": {"color": GRID}},
                "y": {"ticks": {"color": TEXT, "callback": "function(v){return '$'+v+'B'}"}, "grid": {"color": GRID}}
            }
        }
    })

    # Chart 2: EPS History
    send_chart(chat_id, f"*{ticker}* Earnings Per Share (EPS) — Annual", {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [
                {"label": "EPS ($)", "data": clean(hist.get("eps",[])),
                 "backgroundColor": "rgba(159,122,234,0.8)", "borderRadius": 3}
            ]
        },
        "options": {
            "plugins": {"legend": {"labels": {"color": TEXT}}},
            "scales": {
                "x": {"ticks": {"color": TEXT}, "grid": {"color": GRID}},
                "y": {"ticks": {"color": TEXT, "callback": "function(v){return '$'+v}"}, "grid": {"color": GRID}}
            }
        }
    })

    # Chart 3: P/E Ratio History
    pe_data = clean(hist.get("pe",[]))
    send_chart(chat_id, f"*{ticker}* Price-to-Earnings (P/E) Ratio — Annual", {
        "type": "line",
        "data": {
            "labels": labels,
            "datasets": [
                {"label": "P/E Ratio", "data": pe_data,
                 "borderColor": "#ed8936", "backgroundColor": "rgba(237,137,54,0.1)",
                 "borderWidth": 2, "pointRadius": 4, "fill": True, "tension": 0.3}
            ]
        },
        "options": {
            "plugins": {"legend": {"labels": {"color": TEXT}}},
            "scales": {
                "x": {"ticks": {"color": TEXT}, "grid": {"color": GRID}},
                "y": {"ticks": {"color": TEXT}, "grid": {"color": GRID}}
            }
        }
    })

    # Chart 4: Margins & ROE
    send_chart(chat_id, f"*{ticker}* Margins & ROE (%) — Annual", {
        "type": "line",
        "data": {
            "labels": labels,
            "datasets": [
                {"label": "Gross Margin %", "data": clean(hist.get("gross_margin",[])),
                 "borderColor": "#48bb78", "borderWidth": 2, "pointRadius": 3, "fill": False, "tension": 0.3},
                {"label": "Net Margin %", "data": clean(hist.get("net_margin",[])),
                 "borderColor": "#3182ce", "borderWidth": 2, "pointRadius": 3, "fill": False, "tension": 0.3},
                {"label": "ROE %", "data": clean(hist.get("roe",[])),
                 "borderColor": "#f56565", "borderWidth": 2, "pointRadius": 3, "fill": False, "tension": 0.3}
            ]
        },
        "options": {
            "plugins": {"legend": {"labels": {"color": TEXT}}},
            "scales": {
                "x": {"ticks": {"color": TEXT}, "grid": {"color": GRID}},
                "y": {"ticks": {"color": TEXT, "callback": "function(v){return v+'%'}"}, "grid": {"color": GRID}}
            }
        }
    })

    # Chart 5: Annual Dividends
    div_labels = hist.get("div_labels", [])
    div_data = hist.get("div_annual", [])
    if div_labels and any(v and v > 0 for v in div_data):
        send_chart(chat_id, f"*{ticker}* Annual Dividends ($) — History", {
            "type": "bar",
            "data": {
                "labels": div_labels,
                "datasets": [
                    {"label": "Annual Dividend ($)", "data": div_data,
                     "backgroundColor": "rgba(72,187,120,0.8)", "borderRadius": 3}
                ]
            },
            "options": {
                "plugins": {"legend": {"labels": {"color": TEXT}}},
                "scales": {
                    "x": {"ticks": {"color": TEXT}, "grid": {"color": GRID}},
                    "y": {"ticks": {"color": TEXT, "callback": "function(v){return '$'+v}"}, "grid": {"color": GRID}}
                }
            }
        })
    else:
        send_message(chat_id, f"_{ticker} does not pay dividends_")

def build_chart_url(chart_config):
    import urllib.parse, json as _json
    cfg_str = _json.dumps(chart_config, separators=(",",":"))
    params = urllib.parse.urlencode({"c":cfg_str,"w":800,"h":400,"bkg":"#0a0e1a","f":"png"})
    return f"https://quickchart.io/chart?{params}"

def send_chart(chat_id, title, chart_config):
    try:
        chart_url = build_chart_url(chart_config)
        payload = {"chat_id":chat_id,"photo":chart_url,"caption":title,"parse_mode":"Markdown"}
        http_post(f"{TELEGRAM_API}/sendPhoto", payload)
    except Exception as e:
        send_message(chat_id, f"Chart error: {str(e)[:80]}")

def send_stock_charts(chat_id, ticker, hist):
    if not hist or not hist.get("labels"):
        send_message(chat_id, "_No historical data for charts_"); return
    labels = hist.get("labels",[])
    GRID = "#2d3748"; TEXT = "#a0aec0"
    def cl(arr): return [v if v is not None else None for v in arr]

    send_chart(chat_id, f"*{ticker}* Revenue & Net Income ($B) - Annual", {
        "type":"bar","data":{"labels":labels,"datasets":[
            {"label":"Revenue ($B)","data":cl(hist.get("revenue",[])),"backgroundColor":"rgba(49,130,206,0.8)","borderRadius":3},
            {"label":"Net Income ($B)","data":cl(hist.get("net_income",[])),"backgroundColor":"rgba(72,187,120,0.8)","borderRadius":3}
        ]},"options":{"plugins":{"legend":{"labels":{"color":TEXT}}},"scales":{
            "x":{"ticks":{"color":TEXT},"grid":{"color":GRID}},
            "y":{"ticks":{"color":TEXT},"grid":{"color":GRID}}}}})

    send_chart(chat_id, f"*{ticker}* Earnings Per Share (EPS) - Annual", {
        "type":"bar","data":{"labels":labels,"datasets":[
            {"label":"EPS ($)","data":cl(hist.get("eps",[])),"backgroundColor":"rgba(159,122,234,0.8)","borderRadius":3}
        ]},"options":{"plugins":{"legend":{"labels":{"color":TEXT}}},"scales":{
            "x":{"ticks":{"color":TEXT},"grid":{"color":GRID}},
            "y":{"ticks":{"color":TEXT},"grid":{"color":GRID}}}}})

    send_chart(chat_id, f"*{ticker}* Price-to-Earnings (P/E) Ratio - Annual", {
        "type":"line","data":{"labels":labels,"datasets":[
            {"label":"P/E Ratio","data":cl(hist.get("pe",[])),"borderColor":"#ed8936","backgroundColor":"rgba(237,137,54,0.1)","borderWidth":2,"pointRadius":4,"fill":True,"tension":0.3}
        ]},"options":{"plugins":{"legend":{"labels":{"color":TEXT}}},"scales":{
            "x":{"ticks":{"color":TEXT},"grid":{"color":GRID}},
            "y":{"ticks":{"color":TEXT},"grid":{"color":GRID}}}}})

    send_chart(chat_id, f"*{ticker}* Gross Margin, Net Margin & ROE (%) - Annual", {
        "type":"line","data":{"labels":labels,"datasets":[
            {"label":"Gross Margin %","data":cl(hist.get("gross_margin",[])),"borderColor":"#48bb78","borderWidth":2,"pointRadius":3,"fill":False,"tension":0.3},
            {"label":"Net Margin %","data":cl(hist.get("net_margin",[])),"borderColor":"#3182ce","borderWidth":2,"pointRadius":3,"fill":False,"tension":0.3},
            {"label":"ROE %","data":cl(hist.get("roe",[])),"borderColor":"#f56565","borderWidth":2,"pointRadius":3,"fill":False,"tension":0.3}
        ]},"options":{"plugins":{"legend":{"labels":{"color":TEXT}}},"scales":{
            "x":{"ticks":{"color":TEXT},"grid":{"color":GRID}},
            "y":{"ticks":{"color":TEXT},"grid":{"color":GRID}}}}})

    div_labels = hist.get("div_labels",[])
    div_data = hist.get("div_annual",[])
    if div_labels and any(v and v>0 for v in div_data):
        send_chart(chat_id, f"*{ticker}* Annual Dividends ($) - History", {
            "type":"bar","data":{"labels":div_labels,"datasets":[
                {"label":"Annual Dividend ($)","data":div_data,"backgroundColor":"rgba(72,187,120,0.8)","borderRadius":3}
            ]},"options":{"plugins":{"legend":{"labels":{"color":TEXT}}},"scales":{
                "x":{"ticks":{"color":TEXT},"grid":{"color":GRID}},
                "y":{"ticks":{"color":TEXT},"grid":{"color":GRID}}}}})
    else:
        send_message(chat_id, f"_{ticker} does not pay dividends_")
def cmd_stock(chat_id,ticker):
    if not ticker:
        send_message(chat_id,"Usage: /stock AAPL\nExample: /stock NVDA"); return
    send_message(chat_id,f"Analyzing {ticker.upper()}... please wait")
    try:
        req=urllib.request.Request(f"{STOCK_URL}?ticker={ticker.upper()}",headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(req,timeout=55) as r:
            d=json.loads(r.read().decode())
    except Exception as e:
        send_message(chat_id,f"Error fetching {ticker.upper()}: {str(e)[:100]}"); return
    # Fetch ticker-specific news from FMP
    try:
        FMP_KEY="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
        news_req=urllib.request.Request(f"https://financialmodelingprep.com/stable/news/stock?symbols={ticker.upper()}&limit=5&apikey={FMP_KEY}",headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(news_req,timeout=10) as nr:
            fmp_news=json.loads(nr.read().decode())
        if isinstance(fmp_news,list) and fmp_news:
            d["news"]=[{"title":n.get("title",""),"published":(n.get("publishedDate") or n.get("date",""))[:10],"source":n.get("site") or n.get("source","")} for n in fmp_news[:5]]
    except: pass
    tech=d.get("technicals",{});fund=d.get("fundamentals",{})
    earn=d.get("earnings",[]);divs=d.get("dividends",[])
    news=d.get("news",[]);cf=d.get("cash_flow",{});bs=d.get("balance_sheet",{})
    name=d.get("name") or ticker.upper()
    sector=fund.get("sector") or "N/A"
    def fmt(v,pre="$",suf="",dec=2):
        if v is None: return "N/A"
        try:
            fv=float(v)
            if abs(fv)>=1e12: return f"{pre}{fv/1e12:.2f}T{suf}"
            if abs(fv)>=1e9:  return f"{pre}{fv/1e9:.2f}B{suf}"
            if abs(fv)>=1e6:  return f"{pre}{fv/1e6:.2f}M{suf}"
            return f"{pre}{fv:.{dec}f}{suf}"
        except: return str(v)
    def pct(v): return f"{float(v):+.1f}%" if v is not None else "N/A"
    def num(v,dec=2): return f"{float(v):.{dec}f}" if v is not None else "N/A"
    price=tech.get("price");rsi=tech.get("rsi");macd=tech.get("macd")
    sma50=tech.get("sma50");sma200=tech.get("sma200")
    bb_u=tech.get("bb_upper");bb_l=tech.get("bb_lower")
    stoch=tech.get("stoch_rsi");atr=tech.get("atr");sigs=tech.get("signals","NEUTRAL").replace("_"," ")
    rsi_label="NEUTRAL"
    if rsi:
        if rsi<30: rsi_label="OVERSOLD"
        elif rsi>70: rsi_label="OVERBOUGHT"
        elif rsi<45: rsi_label="BEARISH"
        elif rsi>55: rsi_label="BULLISH"
    msg=f"*{name} ({ticker.upper()})*\n_{sector}_\n"
    msg+=f"CEO: {fund.get('ceo','N/A')} | Staff: {fund.get('employees','N/A')}\n\n"
    msg+="*PRICE ACTION*\n"
    msg+=f"Price: *{fmt(price)}* | Beta: {num(fund.get('beta'),2)}\n"
    msg+=f"52W: {fmt(tech.get('wk52_high'))} H / {fmt(tech.get('wk52_low'))} L\n"
    msg+=f"Vs 50MA: {pct(tech.get('dist_50ma_pct'))} | Vs 200MA: {pct(tech.get('dist_200ma_pct'))}\n"
    msg+=f"1M: {pct(tech.get('pct_1m'))} | 3M: {pct(tech.get('pct_3m'))} | 6M: {pct(tech.get('pct_6m'))} | 1Y: {pct(tech.get('pct_1y'))}\n\n"
    msg+="*TECHNICALS*\n"
    msg+=f"RSI(14): *{num(rsi,1)}* ({rsi_label}) | StochRSI: {num(stoch,1)}\n"
    msg+=f"MACD: {num(macd,3)} | ATR: {fmt(atr)} | Vol Ratio: {num(tech.get('vol_ratio'),2)}x\n"
    msg+=f"SMA20: {fmt(tech.get('sma20'))} | SMA50: {fmt(sma50)} | SMA200: {fmt(sma200)}\n"
    msg+=f"BB: {fmt(bb_u)} / {fmt(bb_l)}\n"
    msg+=f"Signals: _{sigs}_\n\n"
    msg+="*VALUATION*\n"
    msg+=f"Mkt Cap: *{fmt(fund.get('market_cap'),'$','')}* | EV: {fmt(fund.get('ev'),'$','')}\n"
    msg+=f"P/E: {num(fund.get('pe_ttm'),1)} | P/B: {num(fund.get('pb_ttm'),1)} | P/S: {num(fund.get('ps_ttm'),1)} | PEG: {num(fund.get('peg'),2)}\n"
    msg+=f"EV/EBITDA: {num(fund.get('ev_ebitda'),1)} | FCF Yield: {num(fund.get('fcf_yield'),2)}%\n\n"
    msg+="*PROFITABILITY*\n"
    msg+=f"Gross Margin: {num(fund.get('gross_margin'),1)}% | Net Margin: {num(fund.get('profit_margin'),1)}%\n"
    msg+=f"ROE: {num(fund.get('roe'),1)}% | ROA: {num(fund.get('roa'),1)}%\n"
    msg+=f"D/E: {num(fund.get('debt_equity'),2)} | Current Ratio: {num(fund.get('current_ratio'),2)}\n\n"
    msg+="*ANALYST TARGETS*\n"
    msg+=f"Consensus: {fund.get('pt_consensus','N/A')} | Mean: {fmt(fund.get('pt_mean'))} | Analysts: {fund.get('pt_analysts','N/A')}\n"
    msg+=f"Range: {fmt(fund.get('pt_low'))} - {fmt(fund.get('pt_high'))}\n\n"
    if earn:
        msg+="*EARNINGS (Last 4Q)*\n"
        for e in earn[:4]:
            surp=f"{e.get('surprise_pct'):+.1f}%" if e.get('surprise_pct') is not None else "N/A"
            msg+=f"{str(e.get('date',''))[:7]}: EPS {num(e.get('eps_act'),2)} vs {num(e.get('eps_est'),2)} ({surp})\n"
        msg+="\n"
    if divs and divs[0].get('dividend'):
        msg+="*DIVIDENDS*\n"
        last_div = divs[0].get('dividend',0)
        annual_div = float(last_div)*4 if last_div else 0
        div_yield = round(annual_div/float(price)*100,2) if price and annual_div else None
        msg+=f"Yield: {num(div_yield,2)}% | Annual: ${num(annual_div,2)} | Payout: {num(fund.get('payout_ratio'),1)}%\n"
        for dv in divs[:3]: msg+=f"{str(dv.get('date',''))[:10]}: {fmt(dv.get('dividend'))}\n"
        msg+="\n"
    if cf:
        msg+="*CASH FLOW*\n"
        msg+=f"Operating: {fmt(cf.get('operating_cf'))} | FCF: {fmt(cf.get('free_cf'))}\n"
        msg+=f"CapEx: {fmt(cf.get('capex'))} | Buybacks: {fmt(cf.get('buybacks'))}\n\n"
    if bs:
        msg+="*BALANCE SHEET*\n"
        msg+=f"Cash: {fmt(bs.get('cash'))} | Debt: {fmt(bs.get('total_debt'))} | Net Debt: {fmt(bs.get('net_debt'))}\n\n"
    if news:
        msg+="*LATEST NEWS*\n"
        for n in news[:3]:
            msg+=f"- {str(n.get('title',''))[:65]}\n  _{n.get('source','')} {n.get('published','')}_\n"
    send_message(chat_id,msg)
    # Send historical charts
    hist = d.get("historical",{})
    if hist and hist.get("labels"):
        send_message(chat_id,"_Generating historical charts..._")
        send_stock_charts(chat_id, ticker.upper(), hist)

def cmd_breadth(chat_id):
    send_message(chat_id,"Fetching market breadth data...")
    try:
        req=urllib.request.Request(f"{STOCK_URL}?breadth=true",headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(req,timeout=30) as r:
            d=json.loads(r.read().decode())
    except Exception as e:
        send_message(chat_id,f"Error: {str(e)[:100]}"); return
    msg="*MARKET BREADTH*\n\n"
    msg+=f"Above 200MA: *{d.get('above_200ma','N/A')}* / {d.get('total_tracked','N/A')} stocks\n"
    msg+=f"At ATH: {d.get('at_ath','N/A')} | Near ATH: {d.get('near_ath','N/A')}\n"
    msg+=f"ATH Breakouts: {d.get('ath_breakouts','N/A')}\n"
    msg+=f"Regime: *{d.get('market_regime','N/A')}* | Khalid: {d.get('khalid_score','N/A')}/100\n\n"
    gainers=d.get('top_gainers',[])
    if gainers:
        msg+="*TOP GAINERS*\n"
        for g in gainers: msg+=f"{g.get('symbol')}: {g.get('chg_pct',0):+.1f}% @ ${g.get('price')}\n"
        msg+="\n"
    losers=d.get('top_losers',[])
    if losers:
        msg+="*TOP LOSERS*\n"
        for l in losers: msg+=f"{l.get('symbol')}: {l.get('chg_pct',0):+.1f}% @ ${l.get('price')}\n"
    send_message(chat_id,msg)
    send_message(chat_id,text)

def cmd_ask(chat_id,question):
    if not question:
        send_message(chat_id,"Usage: /ask What is the current market regime?"); return
    send_typing(chat_id)
    send_message(chat_id,"Querying live data + AI...")
    d=enrich_with_crypto_intel(parse_report(get_report()))
    answer=ask_claude(question,d)
    send_message(chat_id,f"*JustHodl AI*\n\n{answer}")

def cmd_status(chat_id):
    send_typing(chat_id)
    checks=[("report.json","data/report.json"),("crypto-intel","crypto-intel.json"),("CFTC Lambda",None),("Edge Lambda",None)]
    lines=["*System Health*",""]
    s3=boto3.client("s3",region_name="us-east-1")
    for name,key in checks[:2]:
        try: s3.head_object(Bucket=S3_BUCKET,Key=key); lines.append(f"OK - {name}")
        except: lines.append(f"FAIL - {name}")
    r=http_get(CFTC_URL+"health",timeout=8)
    lines.append(f"{'OK' if 'error' not in r else 'FAIL'} - CFTC Lambda")
    r=http_get(EDGE_URL+"health",timeout=8)
    lines.append(f"{'OK' if 'error' not in r else 'FAIL'} - Edge Lambda")
    send_message(chat_id,"\n".join(lines))

def cmd_subscribe(chat_id):
    state=get_alert_state()
    if chat_id not in state["subscribers"]:
        state["subscribers"].append(chat_id); save_alert_state(state)
        send_message(chat_id,"*Alerts enabled!*\n\n- Daily briefing 7AM UTC\n- Regime change alerts\n- Extreme fear/greed alerts\n- Khalid extremes\n\n/unsubscribe to turn off.")
    else:
        send_message(chat_id,"Already subscribed.")

def cmd_unsubscribe(chat_id):
    state=get_alert_state()
    if chat_id in state["subscribers"]:
        state["subscribers"].remove(chat_id); save_alert_state(state)
        send_message(chat_id,"Unsubscribed.")
    else:
        send_message(chat_id,"Not subscribed.")

def cmd_debug(chat_id):
    send_typing(chat_id)
    d=enrich_with_crypto_intel(parse_report(get_report()))
    skip={"signals","cftc_signals","buys","sells","warnings","funding_rates"}
    lines=[f"  {k}: {str(v)[:60]}" for k,v in d.items() if k not in skip]
    send_message(chat_id,"*Debug - Parsed Fields*\n"+"\n".join(lines))

def run_proactive_alerts():
    state=get_alert_state(); subs=state.get("subscribers",[])
    if not subs: return {"sent":0}
    d=enrich_with_crypto_intel(parse_report(get_report())); sent=0; alerts=[]
    now_hour=datetime.now(timezone.utc).hour
    if now_hour==7:
        for cid in subs: cmd_briefing(cid); sent+=1
        state["last_briefing"]=datetime.now(timezone.utc).isoformat()
        save_alert_state(state)
        return {"sent":sent,"type":"morning_briefing"}
    regime=d.get("regime","")
    if regime and regime!=state.get("last_regime","") and state.get("last_regime"):
        alerts.append(f"*REGIME CHANGE*\n\nPrev: {state['last_regime']}\nNew: *{regime}*\nKhalid: {d.get('score','N/A')}/100\n\n_/khalid for details_")
    state["last_regime"]=regime
    fear=d.get("fear_greed")
    try:
        fi=int(fear); lf=state.get("last_fear")
        was_x=lf is not None and (int(lf)<20 or int(lf)>80)
        is_x=fi<20 or fi>80
        if is_x and not was_x:
            alerts.append(f"*EXTREME SENTIMENT*\n\nFear & Greed: *{fi}* - {fear_label(fi)}\n\n_Historically significant signal_")
        state["last_fear"]=fear
    except: pass
    ki=d.get("score")
    try:
        k=float(ki); lk=state.get("last_khalid")
        was_x=lk is not None and (float(lk)<15 or float(lk)>85)
        is_x=k<15 or k>85
        if is_x and not was_x:
            lbl="CRISIS ZONE" if k<15 else "EUPHORIA ZONE"
            alerts.append(f"*KHALID EXTREME*\n\nScore: *{k:.0f}/100*\n{lbl}\n\n_/khalid for full analysis_")
        state["last_khalid"]=ki
    except: pass
    save_alert_state(state)
    for a in alerts:
        for cid in subs: send_message(cid,a); sent+=1
    return {"sent":sent,"alerts":len(alerts)}

def route(chat_id,text_raw):
    text=(text_raw or "").strip()
    if not text: return
    m=re.match(r"^/(\w+)(?:@\S+)?(?:\s+(.*))?$",text,re.DOTALL)
    if m:
        cmd=m.group(1).lower(); args=(m.group(2) or "").strip()
        d={"start":lambda:cmd_start(chat_id),"help":lambda:cmd_start(chat_id),
           "briefing":lambda:cmd_briefing(chat_id),"khalid":lambda:cmd_khalid(chat_id),
           "crypto":lambda:cmd_crypto(chat_id),"cftc":lambda:cmd_cftc(chat_id),
           "edge":lambda:cmd_edge(chat_id),"status":lambda:cmd_status(chat_id),"stock":lambda:cmd_stock(chat_id,args),"breadth":lambda:cmd_breadth(chat_id),
           "ask":lambda:cmd_ask(chat_id,args),"subscribe":lambda:cmd_subscribe(chat_id),
           "unsubscribe":lambda:cmd_unsubscribe(chat_id),"debug":lambda:cmd_debug(chat_id)}
        h=d.get(cmd)
        if h: h()
        else: send_message(chat_id,f"Unknown: /{cmd} - Send /start for commands.")
    else:
        lower=text.lower()
        if any(w in lower for w in ["btc","bitcoin","eth","crypto"]): cmd_crypto(chat_id)
        elif any(w in lower for w in ["khalid","regime","risk","index"]): cmd_khalid(chat_id)
        elif any(w in lower for w in ["cftc","futures","cot","positioning"]): cmd_cftc(chat_id)
        elif any(w in lower for w in ["brief","morning","summary","market"]): cmd_briefing(chat_id)
        else:
            send_typing(chat_id)
            d=enrich_with_crypto_intel(parse_report(get_report()))
            send_message(chat_id,f"*JustHodl AI*\n\n{ask_claude(text,d)}")

def lambda_handler(event,context):
    try:
        if event.get("source")=="aws.events" or event.get("proactive_alerts"):
            return {"statusCode":200,"body":json.dumps(run_proactive_alerts())}
        body=event.get("body","{}")
        if isinstance(body,str): update=json.loads(body)
        else: update=body or {}
        if "callback_query" in update:
            cq=update["callback_query"]; cid=cq["message"]["chat"]["id"]
            http_post(f"{TELEGRAM_API}/answerCallbackQuery",{"callback_query_id":cq["id"]})
            route(cid,"/"+cq.get("data",""))
            return {"statusCode":200,"body":"ok"}
        msg=update.get("message",{})
        if not msg: return {"statusCode":200,"body":"no message"}
        route(msg["chat"]["id"],msg.get("text",""))
        return {"statusCode":200,"body":"ok"}
    except Exception as e:
        print(traceback.format_exc())
        return {"statusCode":200,"body":json.dumps({"error":str(e)})}
