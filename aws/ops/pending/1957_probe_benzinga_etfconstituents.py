"""1957 — PROBE newly-subscribed entitlements before integrating (probe-first rule).
  (A) Benzinga Earnings  — estimates/actuals/surprises
  (B) ETF Global Constituents — holdings look-through
Tries multiple endpoint/param variants (exact paths unknown), reports which
return 200, the JSON schema, and coverage for a few tickers/ETFs.
No writes. Read-only reconnaissance.
"""
import os, json, urllib.request, urllib.error, boto3

def key():
    k=os.environ.get("MASSIVE_API_KEY")
    if not k:
        try: k=boto3.client("ssm","us-east-1").get_parameter(Name="/justhodl/massive-api-key",WithDecryption=True)["Parameter"]["Value"]
        except Exception: k=""
    return k
K=key()
print("key present:", bool(K), "len:", len(K))

def get(url):
    u=url+("&" if "?" in url else "?")+f"apiKey={K}"
    try:
        req=urllib.request.Request(u, headers={"User-Agent":"justhodl-probe/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            body=r.read().decode("utf-8","replace")
            return r.status, body
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8","replace")[:300]
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

def show(label, status, body):
    print(f"\n  [{label}] HTTP {status}")
    if status==200:
        try:
            j=json.loads(body)
            if isinstance(j,dict):
                print("   top keys:", list(j.keys())[:10])
                res=j.get("results") or j.get("data") or j.get("constituents") or j.get("holdings")
                if isinstance(res,list) and res:
                    print("   n_results:", len(res))
                    print("   sample record fields:", list(res[0].keys()) if isinstance(res[0],dict) else type(res[0]))
                    print("   sample[0]:", json.dumps(res[0])[:400])
                elif isinstance(res,list):
                    print("   results empty (entitled but no rows for this query)")
                else:
                    print("   body head:", json.dumps(j)[:400])
        except Exception as ex:
            print("   parse err:", ex, "| head:", body[:200])
    else:
        print("   body:", body[:260])

BASES=["https://api.polygon.io","https://api.massive.com"]

print("\n"+"="*64); print("(A) BENZINGA EARNINGS"); print("="*64)
for b in BASES:
    for url in [f"{b}/benzinga/v1/earnings?ticker=AAPL&limit=3",
                f"{b}/benzinga/v1/earnings?limit=3",
                f"{b}/benzinga/v1/full/earnings?ticker=AAPL&limit=3"]:
        s,body=get(url); 
        if s is not None: show(url.split(b)[-1].split('?')[0]+f" @{b.split('//')[1]}", s, body)
        if s==200: break

print("\n"+"="*64); print("(B) ETF GLOBAL CONSTITUENTS"); print("="*64)
for b in BASES:
    cands=[f"{b}/etf-global/v1/constituents?ticker=SPY&limit=5",
           f"{b}/etf-global/v1/constituents?composite_ticker=SPY&limit=5",
           f"{b}/etf-global/v1/constituents?etf=SPY&limit=5",
           f"{b}/etf-global/v1/holdings?ticker=SPY&limit=5",
           f"{b}/etf-global/v1/profile/constituents?ticker=SPY&limit=5"]
    hit=False
    for url in cands:
        s,body=get(url)
        if s is not None and s!=404:
            show(url.split(b)[-1].split('?')[0]+f" ({url.split('?')[1].split('&')[0]}) @{b.split('//')[1]}", s, body)
        if s==200: hit=True; break
    if hit: break

print("\nDONE 1957")
