import json, urllib.request, re
# 1) data feed has all new fields
u="https://justhodl-data-proxy.raafouis.workers.dev/data/bottleneck-boom-research.json"
d=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"x"}),timeout=20).read())
bt=d.get("by_ticker",{})
def cov(f): return sum(1 for v in bt.values() if v.get(f) is not None)
print("FEED fields live:")
print(f"  #1 price/mom: ret_1m {cov('ret_1m')} ret_3m {cov('ret_3m')} spark {sum(1 for v in bt.values() if v.get('price_spark'))}")
print(f"  #2 pe-range:  pe_pctile {cov('pe_pctile')}")
print(f"  #3 earnings:  beat_rate {cov('beat_rate')} nq_eps_est {cov('nq_eps_est')}")
# 2) page has the new UI code
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottleneck-boom.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={
 "#1 price header":"class=\"phdr\"" in p or "phdr" in p,
 "#2 pe own-range":"P/E own range" in p,
 "#3 beats badge":"Beats est" in p,
 "#4 filter chips":"fchips" in p and "Real only" in p,
 "#5 chart link":"chart-pro.html?ticker=" in p,
 "header fixed":"['ticker','Name','l']" in p,
}
print("\nPAGE live (justhodl.ai):")
for k,val in checks.items(): print(f"  {'OK' if val else 'MISSING'}  {k}")
