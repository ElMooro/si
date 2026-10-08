import os, time, boto3, urllib.request, urllib.error
ssm=boto3.client("ssm",region_name="us-east-1")
tok=ssm.get_parameter(Name="/justhodl/finviz/auth-token",WithDecryption=True)["Parameter"]["Value"].strip()

def probe(label, qs, sleep=5):
    url="https://elite.finviz.com/export.ashx?"+qs+"&auth="+tok
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            body=r.read().decode("utf-8","ignore"); st=r.status
    except urllib.error.HTTPError as e:
        st=e.code; body=""
    except Exception as e:
        st=None; body=str(e)[:60]
    lines=body.splitlines() if body else []
    hdr=lines[0] if lines else ""
    csv = ("," in hdr and ("Ticker" in hdr or "No." in hdr))
    rows=max(len(lines)-1,0) if csv else 0
    print(f"{label:34} http={st} rows={rows} csv={csv}")
    time.sleep(sleep)
    return st, rows, hdr

print("=== SIGNALS (s=) — each is a prebuilt screen ===")
for lab,s in [("most-active (volume)","ta_mostactive"),("unusual-volume","ta_unusualvolume"),
              ("most-volatile","ta_mostvolatile"),("top-gainers","ta_topgainers"),
              ("top-losers","ta_toplosers"),("new-high-52w","ta_newhigh"),
              ("new-low-52w","ta_newlow"),("overbought","ta_overbought"),
              ("oversold","ta_oversold"),("major-news","n_majornews"),
              ("insider-buys","it_latestbuys"),("insider-sales","it_latestsales"),
              ("unusual-volume-up","ta_unusualvolume")]:
    probe(lab, f"v=111&s={s}")

print("\n=== FILTERS (f=) — confirm technical/momentum/short/options filters ===")
for lab,f in [("golden-cross 50x200 up","ta_sma50_cross200a"),
              ("death-cross 50x200 dn","ta_sma50_cross200b"),
              ("price ABOVE sma200","ta_sma200_pa"),
              ("price BELOW sma200","ta_sma200_pb"),
              ("price x sma200 up","ta_sma200_cross200a"),
              ("RSI overbought >70","ta_rsi_ob70"),
              ("RSI oversold <30","ta_rsi_os30"),
              ("new high 52w (filt)","ta_highlow52w_nh"),
              ("rel-volume > 2x","sh_relvol_o2"),
              ("optionable (options)","sh_opt_option"),
              ("short float high","sh_short_high"),
              ("perf month up >20%","ta_perf_4w20o")]:
    probe(lab, f"v=111&f={f}")

print("\n=== CUSTOM COLUMNS (v=152&c=0..71) — full field set in one export ===")
cols=",".join(str(i) for i in range(72))
st,rows,hdr=probe("custom 72-col (tech)", f"v=152&c={cols}&f=sec_technology", sleep=4)
if hdr:
    cs=[c.strip().strip('"') for c in hdr.split(",")]
    print(f"  -> {len(cs)} columns returned")
    print("  cols:", cs)

print("\n=== NEWS export endpoint ===")
def news_probe(qs):
    url="https://elite.finviz.com/news_export.ashx?"+qs+"&auth="+tok
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body=r.read().decode("utf-8","ignore"); st=r.status
    except urllib.error.HTTPError as e: st=e.code; body=""
    except Exception as e: st=None; body=str(e)[:60]
    lines=body.splitlines() if body else []
    print(f"news_export v=3: http={st} rows={max(len(lines)-1,0)} hdr={lines[0][:80] if lines else ''}")
news_probe("v=3")
