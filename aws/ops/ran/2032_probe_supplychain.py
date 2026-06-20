"""ops 2032: probe free supplier->customer relationship data feasibility (FMP peers, SEC EDGAR FTS)."""
import os, json, urllib.request, urllib.error
FMP=os.environ.get("FMP_KEY","wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
def get(u,hdr=None):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers=hdr or {"User-Agent":"jh/1"}),timeout=30) as r:
            return r.getcode(), r.read().decode()
    except urllib.error.HTTPError as e:
        try: return e.code, e.read().decode()[:300]
        except: return e.code,""
    except Exception as e: return None,str(e)[:150]

print("="*64);print("1) FMP relationship endpoints (peers / supply-chain guesses)");print("="*64)
for ep in ["stock-peers?symbol=NVDA","supply-chain?symbol=NVDA","company-supply-chain?symbol=NVDA",
           "key-customers?symbol=NVDA","stock-peers?symbol=AAPL"]:
    c,b=get(f"https://financialmodelingprep.com/stable/{ep}&apikey={FMP}" if "?" in ep else f"https://financialmodelingprep.com/stable/{ep}?apikey={FMP}")
    print(f"  /stable/{ep.split('?')[0]}: HTTP {c} {b[:160]}")

print("\n"+"="*64);print("2) SEC EDGAR full-text search — find filers naming a company as customer");print("="*64)
UA={"User-Agent":"JustHodl Research raafouis@gmail.com"}
# EDGAR FTS API: filers whose 10-K mentions 'NVIDIA' (candidate suppliers depending on NVDA)
for q in ['%22NVIDIA%22', '%22Apple+Inc%22']:
    c,b=get(f"https://efts.sec.gov/LATEST/search-index?q={q}&forms=10-K&dateRange=custom&startdt=2025-01-01&enddt=2026-06-20",UA)
    print(f"\n  FTS q={q}: HTTP {c}")
    try:
        j=json.loads(b); hits=j.get("hits",{}).get("hits",[])
        print("   total:",j.get("hits",{}).get("total",{}).get("value"),"| sample filers:")
        for h in hits[:6]:
            src=h.get("_source",{})
            print("     ",src.get("display_names"))
    except Exception as e: print("   parse:",str(e)[:120],"raw:",b[:160])
print("\nNOTE: precision question — does a 10-K mentioning 'NVIDIA' = NVIDIA is THEIR customer? Need 'customer'/'% of revenue' co-text.")
# refine: phrase search for customer-concentration language near the name
c,b=get('https://efts.sec.gov/LATEST/search-index?q=%22NVIDIA%22+%22of+our+revenue%22&forms=10-K',UA)
try:
    j=json.loads(b); print("\n  refined q='NVIDIA'+'of our revenue': total",j.get("hits",{}).get("total",{}).get("value"))
    for h in j.get("hits",{}).get("hits",[])[:6]: print("     ",h.get("_source",{}).get("display_names"))
except Exception as e: print("  refined parse:",str(e)[:100])
print("DONE 2032")
