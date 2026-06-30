"""ops 2575 — final verify: hint + DNA + mbscore live; report cache-control + host."""
import urllib.request, time
r = urllib.request.urlopen(urllib.request.Request(
    f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}",
    headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}), timeout=25)
html = r.read().decode("utf-8","ignore"); h={k.lower():v for k,v in dict(r.headers).items()}
print("bytes:", len(html))
print("server:", h.get("server"), "| cache-control:", h.get("cache-control"), "| cf-cache:", h.get("cf-cache-status"))
for n in ['id="hint"','Click any ticker','dnaBanner','data-p="mbscore"','Multibagger Score','function openThesis']:
    print(f"  {'OK' if n in html else 'MISS'} {n}")
print("DONE 2575")
