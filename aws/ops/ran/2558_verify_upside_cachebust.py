"""ops 2558 — re-verify upside-radar live WITH cache-buster (rule out CDN cache)."""
import urllib.request, time
time.sleep(20)
for label, q in [("no-buster", ""), ("cache-buster", f"?cb={int(time.time())}")]:
    url = f"https://justhodl.ai/upside-radar.html{q}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                                               "Cache-Control": "no-cache", "Pragma": "no-cache"})
    try:
        r = urllib.request.urlopen(req, timeout=25)
        html = r.read().decode("utf-8", "ignore")
        cf = r.headers.get("cf-cache-status", "?")
        new = 'data-p="confluence"' in html
        print(f"  [{label}] bytes={len(html)} cf-cache={cf} new_tabs={new}")
    except Exception as e:
        print(f"  [{label}] err: {str(e)[:80]}")
print("DONE 2558")
