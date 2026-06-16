import time, json, boto3, urllib.request, urllib.error
s3=boto3.client("s3",region_name="us-east-1")
tok=boto3.client("ssm",region_name="us-east-1").get_parameter(Name="/justhodl/finviz/auth-token",WithDecryption=True)["Parameter"]["Value"].strip()

print("=== AUDIT: current Finviz feeds in S3 ===")
for k in ["finviz-universe","finviz-signals","finviz-short","finviz-heatmap","finviz-groups","finviz-news","finviz-earnings-calendar"]:
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=f"data/{k}.json")
        print(f"  data/{k}.json  {h['ContentLength']:>9}b  {h['LastModified'].strftime('%m-%d %H:%M')}")
    except Exception as e: print(f"  data/{k}.json  MISSING")

def get(path, sleep=5, label=""):
    url="https://elite.finviz.com/"+path+("&" if "?" in path else "?")+"auth="+tok
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req,timeout=45) as r: body=r.read().decode("utf-8","ignore"); st=r.status
    except urllib.error.HTTPError as e: st=e.code; body=""
    except Exception as e: st=None; body=str(e)[:60]
    time.sleep(sleep); return st, body

print("\n=== PROBE 1: extended custom columns (c=0..199) — anything beyond the 72 we capture? ===")
cols=",".join(str(i) for i in range(200))
st,body=get(f"export.ashx?v=152&c={cols}&f=sec_technology")
hdr=body.splitlines()[0] if body else ""
known={"No.","Ticker","Company","Sector","Industry","Country","Market Cap","P/E","Forward P/E","PEG","P/S","P/B","P/Cash","P/Free Cash Flow","Dividend Yield","Payout Ratio","EPS (ttm)","EPS Growth This Year","EPS Growth Next Year","EPS Growth Past 5 Years","EPS Growth Next 5 Years","Sales Growth Past 5 Years","EPS Growth Quarter Over Quarter","Sales Growth Quarter Over Quarter","Shares Outstanding","Shares Float","Insider Ownership","Insider Transactions","Institutional Ownership","Institutional Transactions","Short Float","Short Ratio","Return on Assets","Return on Equity","Return on Invested Capital","Current Ratio","Quick Ratio","LT Debt/Equity","Total Debt/Equity","Gross Margin","Operating Margin","Profit Margin","Performance (Week)","Performance (Month)","Performance (Quarter)","Performance (Half Year)","Performance (Year)","Performance (Year To Date)","Beta","Average True Range","Volatility (Week)","Volatility (Month)","20-Day Simple Moving Average","50-Day Simple Moving Average","200-Day Simple Moving Average","50-Day High","50-Day Low","52-Week High","52-Week Low","Relative Strength Index (14)","Change from Open","Gap","Analyst Recom","Average Volume","Relative Volume","Price","Change","Volume","Earnings Date","Target Price","IPO Date","After-Hours Close"}
cs=[c.strip().strip('"') for c in hdr.split(",")] if hdr else []
print(f"  http={st} total columns returned={len(cs)}")
new=[c for c in cs if c not in known]
print(f"  NEW columns beyond our 72: {len(new)}")
print("  ->", new)

print("\n=== PROBE 2: index-membership filters ===")
for lab,f in [("S&P 500","idx_sp500"),("Nasdaq 100","idx_ndx"),("DJIA","idx_dji")]:
    st,body=get(f"export.ashx?v=111&f={f}")
    rows=max(len(body.splitlines())-1,0) if "Ticker" in body[:50] else 0
    print(f"  {lab:12} f={f:12} http={st} rows={rows}")

print("\n=== PROBE 3: financial statements export (per-ticker) ===")
for lab,p in [("income annual","statement.ashx?t=AAPL&s=IA"),("balance annual","statement.ashx?t=AAPL&s=BA"),("cash annual","statement.ashx?t=AAPL&s=CA")]:
    st,body=get(p, sleep=4)
    print(f"  {lab:16} http={st} head={body[:70].replace(chr(10),' ')!r}")
