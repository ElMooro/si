"""ops 2571 — verify upside-radar v2 decision panel live."""
import urllib.request, time
for attempt in range(4):
    time.sleep(40 if attempt else 20)
    url = f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}-{attempt}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Cache-Control": "no-cache"})
    try:
        html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
        v2 = "dnaBanner" in html
        print(f"attempt {attempt}: bytes={len(html)} v2_panel={v2}")
        if v2:
            for n in ['dnaBanner','DNA match','btarget','smart_money_read',"who's positioning",
                      'data-p="mbscore"','Multibagger Score','dstrip','Notable holders']:
                print(f"  {'OK' if n in html else 'MISS'} {n}")
            print("  no double-escape:", "&amp;amp;" not in html)
            break
    except Exception as e:
        print(f"attempt {attempt} err: {str(e)[:70]}")
print("DONE 2571")
