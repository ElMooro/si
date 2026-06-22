import json, urllib.request, time
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"; POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def gj(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(u,timeout=30) as r: return json.loads(r.read())
        except Exception as e: time.sleep(2); err=str(e)[:60]
    return {"_err":err}
def study(sym):
    print(f"\n{'='*60}\n{sym}  — earnings-inflection study\n{'='*60}")
    inc=gj(f"https://financialmodelingprep.com/stable/income-statement?symbol={sym}&period=quarter&limit=14&apikey={FMP}")
    if isinstance(inc,dict) and inc.get("_err"): print("  income err:",inc["_err"]); return
    if not inc: print("  no income data"); return
    rows=list(reversed(inc))  # oldest first
    # price history (3y daily)
    import datetime as dt
    to=dt.date.today().isoformat(); fr=(dt.date.today()-dt.timedelta(days=1100)).isoformat()
    px=gj(f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/{fr}/{to}?adjusted=true&sort=asc&limit=50000&apikey={POLY}")
    bars=px.get("results",[]) if isinstance(px,dict) else []
    def price_on(datestr):
        if not bars: return None
        tgt=dt.datetime.strptime(datestr,"%Y-%m-%d").timestamp()*1000
        best=None
        for b in bars:
            if b["t"]<=tgt+86400000*5: best=b["c"]
            else: break
        return best
    print(f"  {'qtr_end':<12}{'rev_$M':>9}{'gross%':>8}{'oper%':>8}{'EPS':>8}{'ttmEPS':>8}{'price':>9}{'P/E':>8}")
    epss=[]
    for r in rows:
        d=r.get("date","")[:10]; rev=r.get("revenue") or 0
        gp=r.get("grossProfit") or 0; oi=r.get("operatingIncome") or 0
        eps=r.get("epsdiluted") or r.get("eps") or 0
        epss.append(eps)
        ttm=sum(epss[-4:]) if len(epss)>=4 else None
        p=price_on(d)
        pe=(p/ttm) if (p and ttm and ttm>0) else None
        gm=(gp/rev*100) if rev else 0; om=(oi/rev*100) if rev else 0
        print(f"  {d:<12}{rev/1e6:>9.0f}{gm:>8.1f}{om:>8.1f}{eps:>8.2f}{(ttm if ttm else 0):>8.2f}{(p if p else 0):>9.2f}{(pe if pe else 0):>8.1f}")
    # find trough EPS and inflection
    if len(epss)>=4:
        ttm_series=[sum(epss[max(0,i-3):i+1]) for i in range(len(epss))]
        tr_i=min(range(len(ttm_series)),key=lambda i:ttm_series[i])
        print(f"  >> TTM-EPS trough at {rows[tr_i]['date'][:10]} (ttmEPS={ttm_series[tr_i]:.2f}); latest ttmEPS={ttm_series[-1]:.2f} -> {ttm_series[-1]/ttm_series[tr_i] if ttm_series[tr_i]>0 else float('inf'):.1f}x off trough" if ttm_series[tr_i]>0 else f"  >> trough ttmEPS was negative ({ttm_series[tr_i]:.2f}) at {rows[tr_i]['date'][:10]} -> swung positive")
    # price move
    if bars:
        lo=min(b["c"] for b in bars); hi=max(b["c"] for b in bars); last=bars[-1]["c"]
        print(f"  >> price 3y: low {lo:.2f} -> high {hi:.2f} ({hi/lo:.1f}x); last {last:.2f}")
for s in ["MU","SNDK"]: study(s)
print("\nDONE 2111")
