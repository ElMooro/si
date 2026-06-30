"""ops 2574 — verify _headers cache fix + hint live."""
import urllib.request, time
for attempt in range(4):
    time.sleep(35 if attempt else 15)
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}-{attempt}",
            headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}), timeout=25)
        html = r.read().decode("utf-8","ignore"); hdr = dict(r.headers)
        cc = hdr.get("cache-control") or hdr.get("Cache-Control")
        hint = 'id="hint"' in html
        print(f"attempt {attempt}: bytes={len(html)} cache-control={cc!r} hint={hint}")
        if hint:
            print("  ✓ hint live · DNA banner present:", "dnaBanner" in html, "· mbscore tab:", 'data-p="mbscore"' in html)
            break
    except Exception as e:
        print(f"attempt {attempt}: {str(e)[:70]}")
print("DONE 2574")
