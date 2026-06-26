import json, urllib.request, math, datetime
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"; FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def g(u):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=25).read())
    except Exception as e: return {"_err":str(e)[:60]}
def options_expectations(tkr):
    # spot + next earnings
    q=g(f"https://financialmodelingprep.com/stable/quote?symbol={tkr}&apikey={FMP}")
    spot=(q[0].get("price") if isinstance(q,list) and q else None)
    ec=g(f"https://financialmodelingprep.com/stable/earnings?symbol={tkr}&limit=8&apikey={FMP}")
    today=datetime.date.today()
    nexte=None
    if isinstance(ec,list):
        fut=sorted([e for e in ec if e.get("date") and e["date"]>=str(today)], key=lambda e:e["date"])
        nexte=fut[0]["date"] if fut else None
    if not spot: return {"_err":"no spot"}
    lo,hi=round(spot*0.80),round(spot*1.20)
    base=f"https://api.polygon.io/v3/snapshot/options/{tkr}?strike_price.gte={lo}&strike_price.lte={hi}&limit=250&apiKey={POLY}"
    if nexte: base+=f"&expiration_date.gte={nexte}"
    r=g(base); res=r.get("results") if isinstance(r,dict) else None
    if not res: return {"_err":"no options","_r":str(r)[:80]}
    # group by expiry
    exps={}
    for c in res:
        d=c.get("details") or {}; e=d.get("expiration_date")
        if e and c.get("implied_volatility"): exps.setdefault(e,[]).append(c)
    if not exps: return {"_err":"no IV contracts"}
    target=sorted(exps.keys())[0]   # nearest expiry after earnings
    chain=exps[target]
    # ATM = strike nearest spot; atm IV = avg of call+put IV at that strike
    def strike(c): return (c.get("details") or {}).get("strike_price")
    atm_k=min({strike(c) for c in chain if strike(c)}, key=lambda k:abs(k-spot))
    atm_ivs=[c["implied_volatility"] for c in chain if strike(c)==atm_k]
    atm_iv=sum(atm_ivs)/len(atm_ivs)
    days=(datetime.date.fromisoformat(target)-today).days or 1
    move_pct=atm_iv*math.sqrt(days/365.0)*100
    # skew: OTM put (~0.95) IV vs OTM call (~1.05) IV
    def near(c,mult): return abs(strike(c)-spot*mult)
    puts=[c for c in chain if (c.get("details") or {}).get("contract_type")=="put"]
    calls=[c for c in chain if (c.get("details") or {}).get("contract_type")=="call"]
    pskew=cskew=None
    if puts: p=min(puts,key=lambda c:near(c,0.95)); pskew=p["implied_volatility"]
    if calls: cc=min(calls,key=lambda c:near(c,1.05)); cskew=cc["implied_volatility"]
    skew=(pskew-cskew) if (pskew and cskew) else None
    # put/call OI ratio across the near-money chain
    poi=sum((c.get("open_interest") or 0) for c in res if (c.get("details") or {}).get("contract_type")=="put")
    coi=sum((c.get("open_interest") or 0) for c in res if (c.get("details") or {}).get("contract_type")=="call")
    return {"spot":spot,"next_earnings":nexte,"expiry":target,"days":days,
            "atm_iv_pct":round(atm_iv*100,1),"implied_move_pct":round(move_pct,1),
            "expected_low":round(spot*(1-move_pct/100),2),"expected_high":round(spot*(1+move_pct/100),2),
            "put_skew_pts":round(skew*100,1) if skew is not None else None,
            "pc_oi_ratio":round(poi/coi,2) if coi else None,"n_contracts":len(res)}
for t in ["AAPL","LDOS","NVDA"]:
    print(t,"->",json.dumps(options_expectations(t)))
print("DONE 2278")
