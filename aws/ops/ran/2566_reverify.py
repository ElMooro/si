"""ops 2566 — re-verify thesis modal live after Cloudflare propagation."""
import urllib.request, time
time.sleep(50)
hits = {}
for attempt in range(3):
    url = f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}-{attempt}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Cache-Control": "no-cache"})
    try:
        html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
        has_modal = "function openThesis" in html
        print(f"attempt {attempt}: bytes={len(html)} modal={has_modal}")
        if has_modal:
            for n in ['class="tkl"', "id=\"modal\"", "upside-theses", "CAN SLIM", "100-Bagger",
                      "Lynch Tenbagger", "Multibagger case", "fwScore", "what_breaks_it"]:
                print(f"  {'OK' if n in html else 'MISS'} {n}")
            print("  no double-escape:", "&amp;amp;" not in html)
            break
    except Exception as e:
        print(f"attempt {attempt} err: {str(e)[:80]}")
    time.sleep(40)
print("DONE 2566")
