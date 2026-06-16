import json, urllib.request, urllib.error, boto3
lam=boto3.client("lambda",region_name="us-east-1")
# get the real FMP key from a lambda env
fmpkey=None
for fn in ["justhodl-stock-valuations","justhodl-fundamentals-xray","justhodl-bottleneck-research"]:
    try:
        env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        if env.get("FMP_KEY"): fmpkey=env["FMP_KEY"]; print(f"FMP key from {fn} (len {len(fmpkey)})"); break
    except Exception as e: pass
if not fmpkey: fmpkey="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"; print("using fallback FMP key")

def get(url, hdr=None, t=30):
    req=urllib.request.Request(url, headers=hdr or {"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e: return e.code, ""
    except Exception as e: return None, str(e)[:80]

print("\n=== FMP /stable/ depth (AAPL) ===")
for per,lim in [("annual",40),("quarter",40)]:
    st,body=get(f"https://financialmodelingprep.com/stable/income-statement?symbol=AAPL&period={per}&limit={lim}&apikey={fmpkey}")
    try:
        j=json.loads(body); yrs=sorted({str(r.get('date') or r.get('fiscalYear') or r.get('period'))[:4] for r in j})
        print(f"  {per:8} http={st} periods={len(j)} earliest={yrs[0] if yrs else '?'} latest={yrs[-1] if yrs else '?'}")
    except Exception as e: print(f"  {per:8} http={st} parse-fail {str(e)[:50]} body={body[:90]}")

print("\n=== SEC EDGAR companyfacts (AAPL CIK 0000320193) ===")
st,body=get("https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json", hdr={"User-Agent":"JustHodl Research raafouis@gmail.com"})
try:
    j=json.loads(body); usg=j.get("facts",{}).get("us-gaap",{})
    rev=usg.get("Revenues") or usg.get("RevenueFromContractWithCustomerExcludingAssessedTax") or {}
    units=rev.get("units",{}).get("USD",[])
    yrs=sorted({str(u.get('fy')) for u in units if u.get('fy')})
    print(f"  http={st} us-gaap concepts={len(usg)} revenue datapoints={len(units)} fy range={yrs[0] if yrs else '?'}..{yrs[-1] if yrs else '?'}")
    # a few key line items available
    keys=[k for k in ("Revenues","RevenueFromContractWithCustomerExcludingAssessedTax","NetIncomeLoss","Assets","Liabilities","StockholdersEquity","CashAndCashEquivalentsAtCarryingValue","OperatingIncomeLoss","ResearchAndDevelopmentExpense","EarningsPerShareDiluted") if k in usg]
    print("  key line items present:", keys)
except Exception as e: print(f"  http={st} parse-fail {str(e)[:60]} body={body[:90]}")

print("\n=== Finviz financial statements (re-confirm) ===")
import os
tok=boto3.client("ssm",region_name="us-east-1").get_parameter(Name="/justhodl/finviz/auth-token",WithDecryption=True)["Parameter"]["Value"].strip()
for lab,p in [("statement IA","statement.ashx?t=AAPL&s=IA"),("quote financials","quote.ashx?t=AAPL&ty=l&ta=0&p=d")]:
    st,_=get(f"https://elite.finviz.com/{p}&auth={tok}")
    print(f"  {lab:18} http={st}")
