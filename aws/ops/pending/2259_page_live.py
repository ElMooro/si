import urllib.request, json, time
def get(u,t=30):
    req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Origin":"https://justhodl.ai"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","replace")
s,html=get("https://justhodl.ai/why.html")
print("why.html ->",s,
      "| renderForwardModel:", "renderForwardModel" in html,
      "| renderIndustryComparison:", "renderIndustryComparison" in html,
      "| AI Forward Model:", "AI Forward Model" in html,
      "| Industry Comparison hdr:", "Industry Comparison" in html)
# data carries the fields for render
s2,b=get("https://justhodl-data-proxy.raafouis.workers.dev/equity-research/LDOS.json?v=%d"%int(time.time()))
d=json.loads(b); fm=d.get("forward_model") or {}; ic=d.get("industry_comparison") or {}
print("LDOS data ->",s2,"| forward_model:",bool(fm.get("price_model")),"| industry_pe:",ic.get("industry_pe"),"| pe_vs_industry:",ic.get("pe_vs_industry_pct"))
print("DONE 2259")
