import boto3, json, urllib.request, urllib.parse
lam=boto3.client("lambda","us-east-1")
fred_key=None; src=None
for fn in ["justhodl-global-liquidity","justhodl-sovereign-fiscal","justhodl-yield-curve","justhodl-macro-nowcast","justhodl-tic-flows","justhodl-fed-nlp"]:
    try:
        env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        if env.get("FRED_API_KEY"): fred_key=env["FRED_API_KEY"]; src=fn; break
    except Exception: pass
print("FRED key from:", src, "| have:", bool(fred_key))

def fred_search(q):
    u=(f"https://api.stlouisfed.org/fred/series/search?search_text={urllib.parse.quote(q)}"
       f"&api_key={fred_key}&file_type=json&limit=6&order_by=popularity&sort_order=desc")
    j=json.loads(urllib.request.urlopen(u,timeout=20).read())
    return [(s["id"],s["title"][:62],s.get("frequency_short"),s.get("units_short"),s.get("observation_end")) for s in j.get("seriess",[])]
def fred_latest(sid):
    u=(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={fred_key}"
       f"&file_type=json&sort_order=desc&limit=3")
    j=json.loads(urllib.request.urlopen(u,timeout=20).read())
    return [(o["date"],o["value"]) for o in j.get("observations",[]) if o["value"]!="."][:2]

if fred_key:
    for q in ["net foreign purchases long-term securities","treasury international capital",
              "net foreign acquisition US securities","foreign purchases US treasury securities",
              "net capital inflows United States","US net acquisition long-term securities TIC"]:
        print(f"\nSEARCH '{q}':")
        try:
            for r in fred_search(q): print("   ", r)
        except Exception as e: print("   ERR", str(e)[:60])
# test real treasury TIC transaction files
print("\n=== TREASURY ticdata files ===")
for u in ["https://ticdata.treasury.gov/Publish/slt3a.txt",
          "https://ticdata.treasury.gov/Publish/slt1d_globl.csv",
          "https://ticdata.treasury.gov/Publish/s1.csv",
          "https://ticdata.treasury.gov/Publish/ ","https://ticdata.treasury.gov/Publish/mfhhis01.txt"]:
    try:
        r=urllib.request.urlopen(urllib.request.Request(u.strip(),headers={"User-Agent":"Mozilla/5.0"}),timeout=15)
        b=r.read(); print(f"  {u.strip()} -> {r.status} len {len(b)} | head: {b[:80]}")
    except Exception as e: print(f"  {u.strip()} -> ERR {str(e)[:50]}")
print("DONE 2219")
