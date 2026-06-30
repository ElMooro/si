import urllib.request, time
for a in range(5):
    time.sleep(38 if a else 12)
    try:
        html=urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}-{a}",headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
        live=('id="confT"' in html) and ('tbl(prep(S.breakout)' in html)
        print(f"attempt {a}: bytes={len(html)} layer_live={live}")
        if live:
            for n in ['id="confT"','Filter out unconfirmed','tbl(prep(S.breakout)','const conv=t=>',"'Conviction'",'#confT.on{','⚠ parabolic']:
                print(f"  {'OK' if n in html else 'MISS'} {n}")
            break
    except Exception as e: print(f"attempt {a}: {str(e)[:60]}")
print("DONE 2578")
