"""ops 2576 — verify loader fix live (proxy-first gj + core-first batched load)."""
import urllib.request, time
for attempt in range(4):
    time.sleep(30 if attempt else 10)
    try:
        html = urllib.request.urlopen(urllib.request.Request(
            f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}-{attempt}",
            headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}), timeout=25).read().decode("utf-8","ignore")
        fixed = "core feed first and reliably" in html
        print(f"attempt {attempt}: bytes={len(html)} loader_fixed={fixed}")
        if fixed:
            for n in ["${PX}/${p}${u}","ent.slice(i,i+4)","render(); // render core immediately",
                      "D=await gj('data/upside-radar.json')"]:
                print(f"  {'OK' if n in html else 'MISS'} {n}")
            # also confirm proxy-first order: PX fetch appears before justhodl.ai in gj
            gj = html[html.find("async function gj("):html.find("async function gj(")+260]
            print("  proxy-first:", gj.find("${PX}") < gj.find("justhodl.ai"))
            break
    except Exception as e:
        print(f"attempt {attempt}: {str(e)[:70]}")
print("DONE 2576")
