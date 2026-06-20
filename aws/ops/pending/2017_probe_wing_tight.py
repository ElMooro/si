"""ops 2017: re-probe SPY/QQQ with money-centered window so the 10-25Δ put wing is clean."""
import os, json, calendar, urllib.request, urllib.error
from datetime import date
POLY=os.environ.get("POLYGON_KEY","zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
def get(u):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh/1"}),timeout=30) as r:
            return r.getcode(), json.loads(r.read().decode())
    except Exception as e: return None,{"err":str(e)[:90]}
def tf(n=4):
    out=[];t=date.today();y,m=t.year,t.month
    while len(out)<n+1:
        cal=calendar.monthcalendar(y,m);fr=[w[calendar.FRIDAY] for w in cal if w[calendar.FRIDAY]]
        d=date(y,m,fr[2])
        if d>=t:out.append(d.isoformat())
        m+=1
        if m>12:m=1;y+=1
    return out
def near(res,typ,td):
    best=None;bd=9
    for c in res:
        g=c.get("greeks") or {};d=g.get("delta");iv=c.get("implied_volatility")
        if d is None or iv is None or (c.get("details") or {}).get("contract_type")!=typ:continue
        if abs(abs(d)-abs(td))<bd:bd=abs(abs(d)-abs(td));best=(round(d,3),round(iv,4),(c.get("details") or {}).get("strike_price"))
    return best
for T in ["SPY","QQQ"]:
    c,j=get(f"https://api.polygon.io/v2/aggs/ticker/{T}/prev?adjusted=true&apiKey={POLY}")
    spot=(j.get("results") or [{}])[0].get("c")
    tgt=next(e for e in tf(4) if (date.fromisoformat(e)-date.today()).days>=25)
    lo,hi=round(spot*0.84,2),round(spot*1.10,2)   # money-centered: covers ~10Δ put → ~10Δ call
    cc,jj=get(f"https://api.polygon.io/v3/snapshot/options/{T}?expiration_date={tgt}&strike_price.gte={lo}&strike_price.lte={hi}&limit=250&apiKey={POLY}")
    res=jj.get("results") or []
    nIV=sum(1 for x in res if x.get("implied_volatility") and (x.get("greeks") or {}).get("delta") is not None)
    print(f"{T} spot={spot} exp={tgt} win[{lo},{hi}] contracts={len(res)} next={'Y' if jj.get('next_url') else 'n'} w/IV+Δ={nIV}")
    print(f"   put10Δ={near(res,'put',0.10)} put25Δ={near(res,'put',0.25)} atmP={near(res,'put',0.50)} call25Δ={near(res,'call',0.25)} call10Δ={near(res,'call',0.10)}")
print("DONE 2017")
