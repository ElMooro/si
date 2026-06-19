import os, urllib.request, boto3, subprocess
K=os.environ.get("MASSIVE_API_KEY",""); OLD="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def g(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=15)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e: return e.code, e.read().decode("utf-8","ignore")[:100]
    except Exception as e: return 0,str(e)[:90]
if not K:
    print("ERROR: MASSIVE_API_KEY env missing"); raise SystemExit(1)
print("new key received via secret (len %d)"%len(K))
ssm=boto3.client("ssm","us-east-1")
ssm.put_parameter(Name="/justhodl/massive-api-key",Value=K,Type="SecureString",Overwrite=True)
back=ssm.get_parameter(Name="/justhodl/massive-api-key",WithDecryption=True)["Parameter"]["Value"]
print("SSM /justhodl/massive-api-key  stored + readback match:", back==K)
B="https://api.massive.com"
def probe(name,path):
    c,b=g(B+path+("&" if "?" in path else "?")+"apiKey="+K)
    ok=c==200 and ('"results"' in b) and "NOT_AUTHORIZED" not in b
    print("  [%s] %-16s HTTP %s"%("UNLOCKED" if ok else "NO/LIMITED",name,c)); return ok
print("NEW KEY entitlements @ api.massive.com:")
probe("stocks","/v2/aggs/ticker/AAPL/prev")
probe("options","/v3/snapshot/options/AAPL?limit=2")
probe("fx","/v2/aggs/ticker/C:EURUSD/prev")
probe("futures","/v2/aggs/ticker/ES/range/1/day/2026-05-01/2026-06-18?limit=2")
probe("etf-fund-flows","/etf-global/v1/fund-flows?composite_ticker=SPY&limit=2")
probe("etf-constituents","/etf-global/v1/constituents?composite_ticker=SPY&limit=2")
c,b=g(B+"/v2/aggs/ticker/AAPL/prev?apiKey="+OLD)
print("OLD key (zvEY...): HTTP %s -> %s"%(c,"STILL LIVE" if (c==200 and '\"results\"' in b) else "DEAD / unauthorized"))
r=subprocess.run("grep -rl '%s' aws/ --include=*.py | wc -l"%OLD,shell=True,capture_output=True,text=True)
print("files still hardcoding OLD key:",r.stdout.strip())
print(subprocess.run("grep -rl '%s' aws/ --include=*.py | sed 's#aws/lambdas/##;s#/source.*##' | sort -u | head -40"%OLD,shell=True,capture_output=True,text=True).stdout)
