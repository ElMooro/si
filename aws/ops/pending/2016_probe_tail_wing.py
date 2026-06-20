"""ops 2016: probe SPY put-wing depth (delta to ~-0.10, IV coverage) for crash-prob/tail index."""
import os, json, calendar, urllib.request, urllib.error
from datetime import date, timedelta
POLY=os.environ.get("POLYGON_KEY","zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
def get(u):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh/1"}),timeout=30) as r:
            return r.getcode(), json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode())
        except: return e.code,{}
    except Exception as e: return None,{"err":str(e)[:90]}
def third_fridays(n=4):
    out=[];t=date.today();y,m=t.year,t.month
    while len(out)<n+1:
        cal=calendar.monthcalendar(y,m);fr=[w[calendar.FRIDAY] for w in cal if w[calendar.FRIDAY]]
        tf=date(y,m,fr[2])
        if tf>=t: out.append(tf.isoformat())
        m+=1
        if m>12:m=1;y+=1
    return out
for T in ["SPY","QQQ","IWM"]:
    c,j=get(f"https://api.polygon.io/v2/aggs/ticker/{T}/prev?adjusted=true&apiKey={POLY}")
    spot=(j.get("results") or [{}])[0].get("c")
    # nearest monthly >=25d for ~30d tail tenor
    exps=third_fridays(4); 
    tgt=None
    for e in exps:
        dte=(date.fromisoformat(e)-date.today()).days
        if dte>=25: tgt=e; break
    lo,hi=round(spot*0.62,2),round(spot*1.12,2)
    cc,jj=get(f"https://api.polygon.io/v3/snapshot/options/{T}?expiration_date={tgt}&strike_price.gte={lo}&strike_price.lte={hi}&limit=250&apiKey={POLY}")
    res=jj.get("results") or []
    puts=[c for c in res if (c.get("details") or {}).get("contract_type")=="put" and (c.get("greeks") or {}).get("delta") is not None and c.get("implied_volatility")]
    deltas=sorted([(c.get("greeks") or {}).get("delta") for c in puts])
    dte=(date.fromisoformat(tgt)-date.today()).days if tgt else None
    print(f"{T} spot={spot} exp={tgt} dte={dte} contracts={len(res)} next={'Y' if jj.get('next_url') else 'n'} | puts w/IV+delta={len(puts)} min_put_delta={deltas[0] if deltas else None} (need ~-0.10) max={deltas[-1] if deltas else None}")
    # show wing IVs near 10d/25d/ATM
    def near(typ,tgt_d):
        best=None;bd=9
        for c in res:
            g=c.get("greeks") or {};d=g.get("delta");iv=c.get("implied_volatility")
            if d is None or iv is None or (c.get("details") or {}).get("contract_type")!=typ:continue
            if abs(abs(d)-abs(tgt_d))<bd: bd=abs(abs(d)-abs(tgt_d));best=(round(d,3),round(iv,4),(c.get("details") or {}).get("strike_price"))
        return best
    print(f"   put10Δ={near('put',0.10)} put25Δ={near('put',0.25)} atm_put={near('put',0.50)} call25Δ={near('call',0.25)} call10Δ={near('call',0.10)}")
print("DONE 2016")
