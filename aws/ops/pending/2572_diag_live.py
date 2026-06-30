"""ops 2572 — diagnose why v2 features may not show: page JS, feed reachability, data path."""
import urllib.request, json, time
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Cache-Control": "no-cache"}
def get(url, tmo=25):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=tmo)
        return r.status, r.read().decode("utf-8", "ignore"), dict(r.headers)
    except Exception as e:
        body = ""
        try: body = e.read().decode("utf-8","ignore")[:200]
        except: pass
        return f"ERR {str(e)[:60]}", body, {}

# 1. live page: does it have the v2 JS + the feed wired?
st, html, hdr = get(f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}")
print(f"PAGE status={st} bytes={len(html)} cf-cache={hdr.get('cf-cache-status') or hdr.get('Cf-Cache-Status')}")
for n in ["function openThesis","th:'upside-theses'","class=\"tkl\"","data-p=\"mbscore\"","dnaBanner",
          "addEventListener('click'"]:
    print(f"   {'OK' if n in html else 'MISS'} {n}")

# 2. is the theses feed reachable on the SAME paths the page fetches?
print("\nFEED REACHABILITY (paths the page actually uses):")
for label, url in [
    ("justhodl.ai/data", "https://justhodl.ai/data/upside-theses.json"),
    ("proxy worker", "https://justhodl-data-proxy.raafouis.workers.dev/data/upside-theses.json"),
    ("control: upside-radar.json via justhodl.ai", "https://justhodl.ai/data/upside-radar.json"),
]:
    st, body, hdr = get(url + f"?t={int(time.time())}")
    ok_json = False; info = ""
    if isinstance(st, int) and st == 200:
        try:
            j = json.loads(body); ok_json = True
            if "theses" in j: info = f"v={j.get('version')} n_ai={j.get('n_ai')} n_cand={j.get('n_candidates')} top0={j.get('top_ranked',[''])[0]}"
            else: info = f"keys={list(j)[:5]}"
        except Exception as e: info = f"NOT JSON ({str(e)[:30]}) first80={body[:80]!r}"
    print(f"   [{label}] status={st} json={ok_json} {info}")
    ac = hdr.get('access-control-allow-origin') or hdr.get('Access-Control-Allow-Origin')
    print(f"      CORS allow-origin: {ac}  content-type: {hdr.get('content-type') or hdr.get('Content-Type')}")
print("DONE 2572")
