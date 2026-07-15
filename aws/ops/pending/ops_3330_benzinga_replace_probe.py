"""ops 3330 — capture FMP /stable field shapes to rebuild benzinga-news-agent
(page benzinga.html) off already-paid FMP feeds instead of the dead Benzinga
key. Page JS reads: analyst_ratings[{date,ticker,analyst,action_company,
rating_current,pt_current}], earnings_calendar[{date,ticker,name,eps_est,time}],
economic_events[{date,event_name,actual,consensus,prior}], market_news[{title,
author,created}]. Probe the FMP replacements + record exact fields."""
import json, urllib.request, urllib.error
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1")
def key():
    c=LAM.get_function_configuration(FunctionName="justhodl-analyst-consensus")
    return ((c.get("Environment") or {}).get("Variables") or {}).get("FMP_KEY")
def g(path,params,k):
    p={**params,"apikey":k}
    url=f"https://financialmodelingprep.com/stable/{path}?"+"&".join(f"{a}={b}" for a,b in p.items())
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh-3330"}),timeout=25) as r:
            b=json.loads(r.read())
            if isinstance(b,list): return {"http":r.status,"n":len(b),"fields":list(b[0].keys()) if b else [],"sample":b[0] if b else None}
            return {"http":r.status,"keys":list(b.keys())[:8]}
    except urllib.error.HTTPError as e:
        try: d=e.read().decode()[:100]
        except Exception: d=""
        return {"http":e.code,"err":d}
    except Exception as e:
        return {"err":type(e).__name__}
with report("3330_benzinga_replace_probe") as rep:
    k=key()
    rep.section("EARNINGS CALENDAR")
    rep.kv(earnings_calendar=g("earnings-calendar",{"from":"2026-07-15","to":"2026-07-25"},k))
    rep.section("ECONOMIC CALENDAR")
    rep.kv(economic_calendar=g("economic-calendar",{"from":"2026-07-15","to":"2026-07-25"},k))
    rep.section("NEWS")
    rep.kv(news_general_latest=g("news/general-latest",{"page":0,"limit":20},k))
    rep.kv(news_stock_latest=g("news/stock-latest",{"page":0,"limit":20},k))
    rep.section("RATINGS (reconfirm)")
    rep.kv(grades_latest=g("grades-latest-news",{"limit":30},k),
           pt_latest=g("price-target-latest-news",{"limit":30},k))
    rep.section("DIVIDENDS")
    rep.kv(dividends_calendar=g("dividends-calendar",{"from":"2026-07-15","to":"2026-07-25"},k))
    rep.kv(RESULT="DONE")
