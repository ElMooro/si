"""ops 3274 — runtime diagnostic for Khalid's browser reality.
Server-side literal checks passed while his browser shows nothing, so
this ops tests what a BROWSER actually executes and fetches:

  1. SERVED jh-nav-drawer.js  → node --check (reskin post-processes it
     in _site; a mangled char = whole drawer dead = no favorites).
  2. SERVED chart-pro.html    → extract the ops-3273 script block →
     node --check (reskin typography pass may have corrupted it).
  3. DATA SOURCES both ways   → worker proxy vs same-origin /data/ for
     tv-watchlists.json + symbol-map.json (status, parse, list count).
  4. CACHE HEADERS as-a-user  → homepage + chart-pro WITHOUT busters:
     cf-cache-status, age, drawer-tag version actually served.
Pure evidence; fixes ship next ops from these facts.
"""
import json
import re
import subprocess
import sys
import tempfile
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "jh-ops-3274"}


def get(u, hdr=False):
    r = urllib.request.urlopen(
        urllib.request.Request(u, headers=UA), timeout=25)
    b = r.read().decode("utf-8", "replace")
    return (dict(r.headers), b) if hdr else b


def node_check(code, name):
    f = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False)
    f.write(code)
    f.close()
    p = subprocess.run(["node", "--check", f.name],
                       capture_output=True, text=True)
    return p.returncode == 0, (p.stderr or "").strip()[:220]


with report("3274_runtime_diag") as rep:
    fails, evid = [], []
    rep.section("1. Served drawer JS parses?")
    try:
        js = get("https://justhodl.ai/jh-nav-drawer.js?d=3274")
        ok, err = node_check(js, "drawer")
        rep.kv(drawer_bytes=len(js), drawer_parses=ok)
        if not ok:
            rep.log("  " + err)
            fails.append("SERVED drawer JS is syntax-broken")
    except Exception as e:
        fails.append(f"drawer fetch: {str(e)[:60]}")

    rep.section("2. Served chart-pro block parses?")
    try:
        h = get("https://justhodl.ai/chart-pro.html?d=3274")
        m = re.search(r"<script>\s*(/\* ops 3273.*?)</script>", h,
                      re.S)
        if not m:
            fails.append("ops-3273 block ABSENT from served page")
        else:
            ok, err = node_check(m.group(1), "wlblock")
            rep.kv(block_bytes=len(m.group(1)), block_parses=ok)
            if not ok:
                rep.log("  " + err)
                fails.append("SERVED watchlist block syntax-broken "
                             "(reskin mangling)")
    except Exception as e:
        fails.append(f"chart-pro fetch: {str(e)[:60]}")

    rep.section("3. Data sources — worker vs same-origin")
    for label, base in (
            ("worker",
             "https://justhodl-data-proxy.raafouis.workers.dev"),
            ("origin", "https://justhodl.ai")):
        for key in ("tv-watchlists.json", "symbol-map.json"):
            u = f"{base}/data/{key}?d=3274"
            try:
                hd, b = get(u, hdr=True)
                j = json.loads(b)
                n = len(j.get("lists") or j.get("map") or {})
                rep.log(f"  {label:<6} {key:<22} 200 "
                        f"{len(b):>9}B items={n}")
            except urllib.error.HTTPError as e:
                rep.log(f"  {label:<6} {key:<22} HTTP {e.code}")
            except Exception as e:
                rep.log(f"  {label:<6} {key:<22} ERR "
                        f"{str(e)[:50]}")

    rep.section("4. What a plain (unbusted) user request gets")
    for u in ("https://justhodl.ai/",
              "https://justhodl.ai/chart-pro.html"):
        try:
            hd, b = get(u, hdr=True)
            tag = re.search(r'jh-nav-drawer\.js[^"]*', b)
            rep.log(f"  {u.split('/')[-1] or 'home':<16} "
                    f"cf={hd.get('Cf-Cache-Status')} "
                    f"age={hd.get('Age')} "
                    f"cc={str(hd.get('Cache-Control'))[:28]} "
                    f"tag={tag.group(0)[:40] if tag else 'NONE'}")
        except Exception as e:
            rep.log(f"  {u}: {str(e)[:50]}")

    rep.kv(n_fails=len(fails),
           verdict="PASS" if not fails else "DIAGNOSED")
    for f in fails:
        rep.fail(f)
    if fails:
        sys.exit(1)
