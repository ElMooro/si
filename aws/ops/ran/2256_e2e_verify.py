import urllib.request, json, time
def get(u,timeout=30):
    req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh-verify","Origin":"https://justhodl.ai"})
    with urllib.request.urlopen(req,timeout=timeout) as r: return r.status, r.read().decode("utf-8","replace")
# 1) live page carries the embedded devil's-advocate render code
s,html=get("https://justhodl.ai/why.html")
print("why.html ->",s,
      "| renders devils_advocate:", "devils_advocate" in html,
      "| kill points:", "Kill points" in html,
      "| bulls underestimate:", "what_bulls_underestimate" in html,
      "| non-destructive 502:", "stay silent on failure" in html)
# 2) live LDOS research via the proxy carries devils_advocate
s2,b=get("https://justhodl-data-proxy.raafouis.workers.dev/equity-research/LDOS.json?v=%d"%int(time.time()))
d=json.loads(b); da=d.get("devils_advocate") or {}; v=d.get("verdict") or {}
print("LDOS via proxy ->",s2,"| verdict:",v.get("rating"),v.get("conviction_grade"),
      "| devils_advocate:", bool(da), "| title:", da.get("title"))
print("  kill_points:", len(da.get("kill_points") or []), "| has bulls-underestimate:", bool(da.get("what_bulls_underestimate")))
# sections present
core=["executive_summary","investment_thesis","risk_factors","devils_advocate","valuation_assessment","scenarios","verdict"]
print("  core sections present:", {k:bool(d.get(k)) for k in core})
print("DONE 2256")
