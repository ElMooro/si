"""ops 2067: INVESTIGATE Khalid's claim — does quarter-end institutional buying mean a sector booms
(momentum) or does rebalancing make it reverse? Test: rank sectors by the quarter just ended, then
measure forward EXCESS-vs-SPY of the quarter's WINNERS vs LOSERS at +5d (rebalancing window),
+21d (next month) and +63d (next quarter). Winners>losers = momentum persists (claim right);
losers>winners = rebalancing reversal (claim backwards)."""
import urllib.request, json
from datetime import datetime, timezone
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
SECT=["XLK","XLF","XLE","XLV","XLI","XLY","XLP","XLU","XLB"]  # 9 originals (full history since 1998)
ALL=SECT+["SPY"]
def agg(t):
    u=f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/2010-01-01/2026-06-20?adjusted=true&sort=asc&limit=5000&apiKey={POLY}"
    r=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())
    return {datetime.fromtimestamp(x["t"]/1000,timezone.utc).strftime("%Y-%m-%d"):x["c"] for x in r.get("results",[]) if x.get("c")}
data={t:agg(t) for t in ALL}
common=set(data["SPY"])
for t in ALL: common&=set(data[t])
dates=sorted(common)
idx={d:i for i,d in enumerate(dates)}
# quarter-end = last trading day whose month is 3,6,9,12 and next trading day is a new quarter
qends=[]
for i,d in enumerate(dates[:-1]):
    m=int(d[5:7]); nm=int(dates[i+1][5:7])
    if m in (3,6,9,12) and nm!=m:
        qends.append(i)
def ret(t,i,j):
    return data[t][dates[j]]/data[t][dates[i]]-1 if 0<=i<len(dates) and 0<=j<len(dates) else None
horizons={"+5d (rebal window)":5,"+21d (next month)":21,"+63d (next quarter)":63}
agg_res={h:{"win":[],"los":[],"win_gt_los":0,"n":0} for h in horizons}
NW=3  # top/bottom 3 baskets
for qi in qends:
    if qi-63<0 or qi+63>=len(dates): continue
    qret={t:ret(t,qi-63,qi) for t in SECT}  # quarter just ended
    ranked=sorted(SECT,key=lambda t:qret[t],reverse=True)
    winners,losers=ranked[:NW],ranked[-NW:]
    spyq=ret("SPY",qi-63,qi)
    for h,H in horizons.items():
        we=[ret(t,qi,qi+H)-ret("SPY",qi,qi+H) for t in winners]
        le=[ret(t,qi,qi+H)-ret("SPY",qi,qi+H) for t in losers]
        wm=sum(we)/len(we); lm=sum(le)/len(le)
        agg_res[h]["win"].append(wm); agg_res[h]["los"].append(lm)
        agg_res[h]["win_gt_los"]+= 1 if wm>lm else 0; agg_res[h]["n"]+=1
def med(x): 
    s=sorted(x); n=len(s); return (s[n//2] if n%2 else (s[n//2-1]+s[n//2])/2) if s else 0
print(f"Quarter-end rebalancing study | {len(agg_res['+5d (rebal window)']['win'])} quarter-ends 2010-2026 | 9 SPDR sectors")
print("Forward EXCESS-vs-SPY of the quarter's WINNERS (rebalancers SELL) vs LOSERS (rebalancers BUY):\n")
print(f"{'horizon':<22}{'WINNERS avg':>13}{'LOSERS avg':>12}{'spread(W-L)':>13}{'W>L freq':>10}")
for h in horizons:
    a=agg_res[h]; wm=sum(a['win'])/a['n']*100; lm=sum(a['los'])/a['n']*100
    print(f"{h:<22}{wm:>+12.2f}%{lm:>+11.2f}%{wm-lm:>+12.2f}%{100*a['win_gt_los']/a['n']:>9.0f}%")
print("\nmedians (robust):")
for h in horizons:
    a=agg_res[h]; print(f"  {h:<22} winners {med(a['win'])*100:+.2f}%  losers {med(a['los'])*100:+.2f}%  spread {(med(a['win'])-med(a['los']))*100:+.2f}%")
print("\nINTERPRETATION: spread>0 = momentum persists (winners keep winning); spread<0 = REBALANCING REVERSAL (the quarter's losers outperform — institutions buying the laggards).")
print("DONE 2067")
