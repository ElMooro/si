#!/usr/bin/env python3
from pathlib import Path
P = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
# parents: patchers -> ops -> aws -> repo root is parents[3]

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    if 'lastSource="unavailable"' in t:
        print("already clean")
        return 0
    old = '''    lastSource="synth";
    var fb=synth(t, 400);
    barCache[key]={d:fb, at:now, src:"synth"};
    return fb;'''
    new = '''    lastSource="unavailable";
    barCache[key]={d:[], at:now, src:"unavailable"};
    return [];'''
    if old not in t:
        raise SystemExit("synth fallback block drifted")
    t = t.replace(old, new, 1)
    t = t.replace(
        'if(st) st.textContent="v12 QR · "',
        'var cd=document.getElementById("cd"); if(cd) cd.textContent="v12 QR"; if(st) st.textContent="v12 QR · "',
        1,
    )
    oldu = '''    var urls=[
      "/api/klines?symbol="+encodeURIComponent(t)+"&interval="+encodeURIComponent(sp[0])+"&limit=1000",
      "/api/yahoo?ticker="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],'''
    newu = '''    var urls=[
      LIVE+"/data/series/"+encodeURIComponent(ys)+".json",
      LIVE+"/data/series/"+encodeURIComponent(t)+".json",
      "/api/klines?symbol="+encodeURIComponent(t)+"&interval="+encodeURIComponent(sp[0])+"&limit=1000",
      "/api/yahoo?ticker="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],'''
    if oldu not in t:
        raise SystemExit("url list drifted")
    t = t.replace(oldu, newu, 1)
    t = t.replace(
        "/* JustHodl Chart engine v12 — QR tape always on. Does not touch Chart Pro. */",
        "/* JustHodl Chart engine v12.1 — no synth candles; warehouse series first; footer v12 QR. Chart Pro untouched. */",
        1,
    )
    p.write_text(t)
    print("patched", p, "bytes", p.stat().st_size)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
