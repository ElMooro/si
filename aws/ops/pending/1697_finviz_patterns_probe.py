import time, boto3, urllib.request, urllib.error
tok=boto3.client("ssm",region_name="us-east-1").get_parameter(Name="/justhodl/finviz/auth-token",WithDecryption=True)["Parameter"]["Value"].strip()
def probe(label, f, sleep=5):
    url="https://elite.finviz.com/export.ashx?v=111&f="+f+"&auth="+tok
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req,timeout=40) as r: body=r.read().decode("utf-8","ignore"); st=r.status
    except urllib.error.HTTPError as e: st=e.code; body=""
    except Exception as e: st=None; body=str(e)[:50]
    lines=body.splitlines() if body else []; csv=("Ticker" in (lines[0] if lines else ""))
    print(f"{label:32} http={st} rows={max(len(lines)-1,0) if csv else 0} csv={csv}")
    time.sleep(sleep)
print("=== CHART PATTERNS (reversal/continuation) ===")
for lab,f in [("DOUBLE BOTTOM (bullish)","ta_pattern_doublebottom"),
              ("DOUBLE TOP (bearish)","ta_pattern_doubletop"),
              ("inverse H&S (bullish bottom)","ta_pattern_headandshouldersinv"),
              ("head&shoulders (bearish top)","ta_pattern_headandshoulders"),
              ("multiple bottom","ta_pattern_multiplebottom"),
              ("multiple top","ta_pattern_multipletop"),
              ("channel up","ta_pattern_channelup"),
              ("channel down","ta_pattern_channeldown"),
              ("triangle ascending","ta_pattern_triangleascending"),
              ("triangle descending","ta_pattern_triangledescending"),
              ("wedge up","ta_pattern_wedgeup"),
              ("wedge down","ta_pattern_wedgedown"),
              ("TL support","ta_pattern_tlsupport"),
              ("TL resistance","ta_pattern_tlresistance")]:
    probe(lab,f)
