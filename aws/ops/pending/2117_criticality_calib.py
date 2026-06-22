import json, urllib.request, time, statistics, boto3
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def gj(u):
    for _ in range(3):
        try:
            with urllib.request.urlopen(u,timeout=25) as r: return json.loads(r.read())
        except Exception as e: time.sleep(2); err=str(e)[:40]
    return None

# supply-chain-graph centrality (how many edges route through this node as a hub/supplier)
cent={}
try:
    g=json.loads(s3.get_object(Bucket=B,Key="data/supply-chain-graph.json")["Body"].read())
    # count appearances as supplier (out-edges) — hub-ness
    for e in g.get("edges",[]) or []:
        sup=e.get("supplier") or e.get("source"); 
        if sup: cent[sup]=cent.get(sup,0)+1
    # also try nodes with centrality
    for n in g.get("nodes",[]) or []:
        s=n.get("symbol") or n.get("id")
        if s and isinstance(n.get("centrality"),(int,float)): cent[s]=max(cent.get(s,0),n["centrality"]*10)
except Exception as ex:
    print("supply-chain-graph read:",str(ex)[:50])

def metrics(sym):
    inc=gj(f"https://financialmodelingprep.com/stable/income-statement?symbol={sym}&period=annual&limit=10&apikey={FMP}")
    if not isinstance(inc,list) or len(inc)<4: return None
    gms=[]; oms=[]; rd=[]
    for r in inc:
        rev=r.get("revenue") or 0
        if rev<=0: continue
        gms.append((r.get("grossProfit") or 0)/rev*100)
        oms.append((r.get("operatingIncome") or 0)/rev*100)
        rd.append((r.get("researchAndDevelopmentExpenses") or 0)/rev*100)
    if len(gms)<4: return None
    rat=gj(f"https://financialmodelingprep.com/stable/ratios?symbol={sym}&period=annual&limit=1&apikey={FMP}")
    roic=None
    if isinstance(rat,list) and rat:
        roic=rat[0].get("returnOnInvestedCapital") or rat[0].get("returnOnCapitalEmployed") or rat[0].get("returnOnEquity")
        if isinstance(roic,(int,float)): roic=round(roic*100,1) if abs(roic)<3 else round(roic,1)
    return {
        "gm_level": round(statistics.mean(gms),1),
        "gm_stability": round(statistics.pstdev(gms),1),     # LOW = holds price through cycles
        "om_level": round(statistics.mean(oms),1),
        "rd_intensity": round(statistics.mean(rd),1),
        "roic": roic,
        "centrality": cent.get(sym,0),
    }

def crit_score(m):
    if not m: return 0
    gm = min(1.0, m["gm_level"]/70.0)                       # high margins
    stab = max(0.0, 1.0 - m["gm_stability"]/15.0)           # stable margins (pricing power through cycles)
    roic = min(1.0, (m["roic"] or 0)/30.0) if m["roic"] else 0.3
    rd = min(1.0, m["rd_intensity"]/20.0)                   # R&D barrier
    ctr = min(1.0, m["centrality"]/8.0)                     # supply-chain hub-ness
    return round(100*(0.30*gm + 0.22*stab + 0.20*roic + 0.13*rd + 0.15*ctr),1)

print("centrality hubs found:", dict(sorted(cent.items(),key=lambda x:-x[1])[:12]) or "(none — graph schema differs)")
print(f"\n{'sym':<6}{'crit':>6}{'gm_lvl':>8}{'gm_std':>8}{'om_lvl':>8}{'roic':>7}{'rd%':>6}{'ctr':>5}  [class]")
POS=["ASML","TSM","NVDA","V","MA","SNPS","KLAC","CDNS"]   # textbook chokepoints
NEG=["GT","UAL","DOW","X","GNK","F","CCL"]                # commodity / substitutable
rows=[]
for s in POS+NEG:
    m=metrics(s); sc=crit_score(m)
    rows.append((s,sc,m,"CHOKEPOINT" if s in POS else "commodity"))
for s,sc,m,cls in sorted(rows,key=lambda x:-x[1]):
    if not m: print(f"{s:<6} no data  [{cls}]"); continue
    print(f"{s:<6}{sc:>6}{m['gm_level']:>8}{m['gm_stability']:>8}{m['om_level']:>8}{str(m['roic']):>7}{m['rd_intensity']:>6}{m['centrality']:>5}  [{cls}]")
pos=[sc for s,sc,m,c in rows if c=='CHOKEPOINT' and m]; neg=[sc for s,sc,m,c in rows if c=='commodity' and m]
print(f"\nSEPARATION: chokepoints avg={round(statistics.mean(pos),1)} (min {min(pos)})  vs  commodity avg={round(statistics.mean(neg),1)} (max {max(neg)})")
print("VIABLE if chokepoint min > commodity max (clean separation)")
print("DONE 2117")
