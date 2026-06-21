"""ops 2068: DIRECT test of Khalid's claim using REAL ETF flow data.
When a sector ETF sees big INFLOWS (institutions buying), does it OUTPERFORM next month, or fade?
Signal = trailing 21d net fund_flow as % of AUM (persistent flow, per the engine's own thesis).
Forward = ETF 21-trading-day return minus SPY (excess). Quintile the events; compare top vs bottom.
IC = correlation(signal, forward excess). Positive = flows predict continuation (claim right);
negative/zero = inflows fade / no edge (claim wrong)."""
import urllib.request, json, time
from datetime import datetime, timezone, timedelta, date
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
SECT=["XLK","XLF","XLE","XLV","XLP","XLY","XLI","XLU","XLB","XLC","XLRE"]
def jget(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh-flowtest"}),timeout=25) as r:
                return json.loads(r.read())
        except Exception: time.sleep(2)
    return {}
def flows(etf):
    rows={}; lte=date.today()
    for _ in range(8):  # paginate back ~2.5y
        u=(f"https://api.polygon.io/etf-global/v1/fund-flows?composite_ticker={etf}"
           f"&processed_date.lte={lte.isoformat()}&order=desc&sort=processed_date&limit=120&apiKey={POLY}")
        res=jget(u).get("results",[])
        if not res: break
        for r in res:
            d=str(r.get("processed_date") or r.get("effective_date") or "")[:10]
            ff=r.get("fund_flow", r.get("fund_flow_daily"))
            nav=r.get("nav"); sh=r.get("shares_outstanding")
            if d and ff is not None and nav and sh:
                rows[d]={"flow":float(ff),"aum":float(nav)*float(sh)}
        earliest=min(str(r.get("processed_date"))[:10] for r in res)
        nxt=datetime.strptime(earliest,"%Y-%m-%d").date()-timedelta(days=1)
        if nxt>=lte or len(res)<120: break
        lte=nxt
    return rows
def closes(t):
    frm=(date.today()-timedelta(days=1000)).isoformat()
    u=f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{frm}/{date.today().isoformat()}?adjusted=true&sort=asc&limit=1000&apiKey={POLY}"
    return {datetime.fromtimestamp(x["t"]/1000,timezone.utc).strftime("%Y-%m-%d"):x["c"] for x in jget(u).get("results",[]) if x.get("c")}
spy=closes("SPY"); spd=sorted(spy)
events=[]  # (signal, fwd_excess)
per_etf_n={}
for etf in SECT:
    fl=flows(etf); px=closes(etf)
    if len(fl)<60 or len(px)<60: per_etf_n[etf]=0; continue
    pdates=sorted(set(px)&set(spy))
    pidx={d:i for i,d in enumerate(pdates)}
    fdates=sorted(fl)
    n=0
    # step every ~21 trading days through flow dates to limit overlap
    i=21
    while i<len(fdates):
        d=fdates[i]
        win=[fl[fdates[j]]["flow"] for j in range(i-20,i+1)]
        aum=fl[d]["aum"]
        if aum<=0: i+=21; continue
        sig=sum(win)/aum*100   # 21d net flow as % AUM
        # align to price date on/after d
        ad=next((x for x in pdates if x>=d),None)
        if not ad or ad not in pidx: i+=21; continue
        k=pidx[ad]
        if k+21>=len(pdates): i+=21; continue
        f1=pdates[k+21]
        er=(px[f1]/px[ad]-1)-(spy[f1]/spy[ad]-1)
        events.append((sig,er*100)); n+=1
        i+=21
    per_etf_n[etf]=n
print(f"ETF flow predictive test | {len(events)} non-overlapping events across {sum(1 for v in per_etf_n.values() if v)} sectors")
print("per-sector events:",{k:v for k,v in per_etf_n.items() if v})
if len(events)<40:
    print("INSUFFICIENT flow history from API (likely entitlement/limit). events=",len(events)); print("DONE 2068"); raise SystemExit
sig=[e[0] for e in events]; fwd=[e[1] for e in events]
n=len(events)
# Pearson IC
import statistics as st
ms,mf=st.mean(sig),st.mean(fwd); ss,sf=st.pstdev(sig),st.pstdev(fwd)
ic=sum((sig[i]-ms)*(fwd[i]-mf) for i in range(n))/n/(ss*sf) if ss and sf else 0
# quintiles by signal
order=sorted(events,key=lambda e:e[0]); qn=n//5
botq=order[:qn]; topq=order[-qn:]
def stats(b): 
    fs=[x[1] for x in b]; return st.mean(fs), (st.median(fs)), 100*sum(1 for v in fs if v>0)/len(fs)
tm,tmd,thr=stats(topq); bm,bmd,bhr=stats(botq)
print(f"\nsignal = trailing 21d net flow as % of AUM | forward = 21d excess vs SPY")
print(f"  Information Coefficient (corr signal→fwd excess): {ic:+.3f}")
print(f"  TOP quintile (biggest INFLOWS, n{len(topq)}):  fwd excess mean {tm:+.2f}% median {tmd:+.2f}%  win {thr:.0f}%")
print(f"  BOTTOM quintile (biggest OUTFLOWS, n{len(botq)}): fwd excess mean {bm:+.2f}% median {bmd:+.2f}%  win {bhr:.0f}%")
print(f"  SPREAD (inflows − outflows): {tm-bm:+.2f}% mean")
print(f"\n  >0 / IC>0 = inflows predict OUTPERFORMANCE (claim supported). <0 = inflows FADE (claim wrong).")
print("DONE 2068")
