"""ops 2057: TEST Khalid's bifurcation thesis — AI-led index melt-up over a broad risk-off tape.
Measure Oct 1 2025 → now: crypto crash? cap-weight SPY vs equal-weight RSP (concentration tell)?
AI/megacap (QQQ/NVDA/MAGS) vs small-caps IWM? = is the market actually TWO regimes at once?"""
import urllib.request, json
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def agg(t,cr=False):
    pre="X:" if cr else ""
    u=f"https://api.polygon.io/v2/aggs/ticker/{pre}{t}/range/1/day/2025-10-01/2026-06-20?adjusted=true&sort=asc&limit=400&apiKey={POLY}"
    r=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=25).read())
    res=r.get("results",[])
    if not res: return None
    o=res[0]["c"]; last=res[-1]["c"]; lo=min(x["c"] for x in res); hi=max(x["c"] for x in res)
    peak=res[0]["c"]; mdd=0
    for x in res:
        peak=max(peak,x["c"]); mdd=min(mdd,(x["c"]-peak)/peak)
    return {"oct":o,"now":last,"chg":(last/o-1)*100,"lo":lo,"hi":hi,"trough_from_oct":(lo/o-1)*100,"mdd":mdd*100}
names={"SPY (cap-weight S&P)":("SPY",0),"RSP (EQUAL-weight S&P)":("RSP",0),"QQQ (Nasdaq/AI-heavy)":("QQQ",0),
 "IWM (small caps)":("IWM",0),"NVDA":("NVDA",0),"MAGS (Mag-7 ETF)":("MAGS",0),"XLK (tech)":("XLK",0),
 "BTC":("BTCUSD",1),"ETH":("ETHUSD",1)}
print("=== Oct 1 2025 → Jun 2026 ===")
rows={}
for label,(t,cr) in names.items():
    try:
        d=agg(t,cr); rows[label]=d
        if d: print(f"  {label:<24} {d['chg']:+7.1f}%   (low {d['trough_from_oct']:+.0f}% from Oct, maxDD {d['mdd']:.0f}%)")
        else: print(f"  {label:<24} no data")
    except Exception as e: print(f"  {label:<24} ERR {str(e)[:40]}")
# the bifurcation verdict
spy=rows.get("SPY (cap-weight S&P)"); rsp=rows.get("RSP (EQUAL-weight S&P)"); iwm=rows.get("IWM (small caps)"); nvda=rows.get("NVDA"); btc=rows.get("BTC")
print("\n=== BIFURCATION TEST ===")
if spy and rsp:
    spread=spy["chg"]-rsp["chg"]
    print(f"SPY − RSP concentration spread: {spread:+.1f} pts  ({'CONCENTRATED megacap-led' if spread>4 else 'broad' if abs(spread)<2 else 'mild tilt'})")
if spy and iwm:
    print(f"SPY − IWM (large vs small): {spy['chg']-iwm['chg']:+.1f} pts")
if nvda and iwm:
    print(f"NVDA vs small caps: NVDA {nvda['chg']:+.0f}% vs IWM {iwm['chg']:+.0f}%")
if btc:
    print(f"Crypto BTC: {btc['chg']:+.0f}% (Khalid said ~−75%; trough {btc['trough_from_oct']:+.0f}% from Oct)")
print("VERDICT:", "BIFURCATED — index up on concentration while breadth/crypto weak (Khalid's thesis SUPPORTED)" if (spy and rsp and spy['chg']-rsp['chg']>3 and (not iwm or iwm['chg']<spy['chg'])) else "NOT clearly bifurcated — recheck")
print("DONE 2057")
