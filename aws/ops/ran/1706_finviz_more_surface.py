import time, boto3, urllib.request, urllib.error
tok=boto3.client("ssm",region_name="us-east-1").get_parameter(Name="/justhodl/finviz/auth-token",WithDecryption=True)["Parameter"]["Value"].strip()
def probe(label, path, sleep=5):
    url="https://elite.finviz.com/"+path+("&" if "?" in path else "?")+"auth="+tok
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req,timeout=40) as r: body=r.read().decode("utf-8","ignore"); st=r.status
    except urllib.error.HTTPError as e: st=e.code; body=""
    except Exception as e: st=None; body=str(e)[:60]
    lines=body.splitlines() if body else []
    hdr=lines[0] if lines else ""
    is_csv = ("," in hdr and "<" not in hdr[:3] and len(hdr)<400)
    print(f"{label:30} http={st} rows={max(len(lines)-1,0) if is_csv else 0} csv={is_csv}")
    if is_csv: print("    cols:", hdr[:300])
    elif body: print("    (non-CSV head:", body[:70].replace(chr(10)," "), ")")
    time.sleep(sleep)

print("=== GROUPS export (sector / industry / country / market-cap aggregates) ===")
for lab,p in [("groups sector perf","grp_export.ashx?g=sector&v=140"),
              ("groups sector valuation","grp_export.ashx?g=sector&v=120"),
              ("groups industry perf","grp_export.ashx?g=industry&v=140"),
              ("groups country perf","grp_export.ashx?g=country&v=140"),
              ("groups mktcap perf","grp_export.ashx?g=capitalization&v=140")]:
    probe(lab,p)

print("\n=== NEWS / BLOGS export ===")
for lab,p in [("news (v=3)","news_export.ashx?v=3"),
              ("blogs (v=4)","news_export.ashx?v=4")]:
    probe(lab,p)

print("\n=== INSIDER detailed export (beyond screener signal) ===")
for lab,p in [("insider all","insidertrading_export.ashx?"),
              ("insider buys","insidertrading_export.ashx?tc=1"),
              ("insider o-value","insidertrading_export.ashx?o=-transactionValue")]:
    probe(lab,p)

print("\n=== FUTURES / FOREX / CRYPTO ===")
for lab,p in [("futures perf","future_export.ashx?"),
              ("forex perf","forex_export.ashx?"),
              ("crypto perf","crypto_export.ashx?")]:
    probe(lab,p)
