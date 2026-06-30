import urllib.request, time
for a in range(4):
    time.sleep(35 if a else 10)
    try:
        html=urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}-{a}",headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
        live=("'MB score'" in html) and ("'Rev gr'" in html)
        print(f"attempt {a}: bytes={len(html)} new_cols_live={live}")
        if live:
            for n in ["'Rev gr'","'⚙ Eng'","🧬 Setup","'MB score'","const TH=t=>","r.canslim?r.canslim.score"]:
                print(f"  {'OK' if n in html else 'MISS'} {n}")
            break
    except Exception as e: print(f"attempt {a}: {str(e)[:60]}")
print("DONE 2577")
