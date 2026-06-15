import os, json, boto3, urllib.request, urllib.error
ssm=boto3.client("ssm",region_name="us-east-1")
tok=ssm.get_parameter(Name="/justhodl/finviz/auth-token",WithDecryption=True)["Parameter"]["Value"].strip()
def fetch(params):
    url="https://elite.finviz.com/export.ashx?"+params+"&auth="+tok
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8","ignore")[:150]
    except Exception as e:
        return None, str(e)[:150]
def rowcount(params):
    st,body=fetch(params)
    lines=body.splitlines() if isinstance(body,str) else []
    ok = lines and ("Ticker" in lines[0])
    return st, (len(lines)-1 if ok else -1), (lines[0][:60] if lines else "")

print("=== FULL CUSTOM COLUMN SET (v=152&c=0..70) — the complete data surface ===")
cols=",".join(str(i) for i in range(0,72))
st,body=fetch("v=152&c="+cols+"&f=idx_sp500")
hdr=body.splitlines()[0] if isinstance(body,str) and body else ""
names=[c.strip().strip('"') for c in hdr.split(",")]
print(f"http={st} | {len(names)} columns available:")
for i,n in enumerate(names): print(f"   c={i}: {n}")

print("\n=== SIGNAL PRESETS (s=) — row counts ===")
for sig in ["ta_topgainers","ta_toplosers","ta_newhigh","ta_newlow","ta_mostvolatile","ta_mostactive","ta_unusualvolume","ta_overbought","ta_oversold","n_majornews","it_latestbuys","it_latestsales"]:
    st,n,_=rowcount("v=111&s="+sig)
    print(f"  s={sig:18} http={st} rows={n}")

print("\n=== TECHNICAL FILTERS (f=) — MA crossovers / position ===")
for f in ["ta_sma50_cross200a","ta_sma50_cross200b","ta_sma200_pa","ta_sma200_pb","ta_sma200_cross200a","ta_perf_4w30o","ta_rsi_os30","ta_rsi_ob70","ta_highlow52w_nh","sh_relvol_o2","ta_pattern_channelup"]:
    st,n,_=rowcount("v=111&f="+f)
    print(f"  f={f:22} http={st} rows={n}")

print("\n=== NEWS / OPTIONS availability ===")
st,body=fetch("v=3")  # news view?
print(f"  v=3 (news?) http={st} first60={ (body.splitlines()[0][:60] if isinstance(body,str) and body else '')}")
st,n,_=rowcount("v=111&f=sh_opt_option")  # optionable filter
print(f"  f=sh_opt_option (optionable) http={st} rows={n}")
