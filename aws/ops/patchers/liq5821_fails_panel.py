#!/usr/bin/env python3
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
TAG = '<script src="/jh-liq-fails-panel.js?v=5821"></script>\n'
for rel in ("liquidity.html", "ciss.html"):
    p = ROOT / rel
    if not p.exists():
        print("skip", rel); continue
    t = p.read_text()
    if rel == "liquidity.html":
        if "jh-liq-fails-panel.js" in t:
            print("already liq"); continue
        if "</body>" not in t:
            print("nobody liq"); continue
        p.write_text(t.replace("</body>", TAG + "</body>", 1))
        print("wired liquidity")
    else:
        extra = '<script src="/data/ciss-headline.json"></script>\n'  # placeholder avoided
        if "ciss-headline" in t:
            print("already ciss note"); continue
        if "</body>" not in t:
            print("nobody ciss"); continue
        js = '<script>(function(){fetch("/data/ciss-headline.json",{cache:"no-store"}).then(function(r){return r.ok?r.json():null}).then(function(d){if(!d)return;var n=document.getElementById("jh-ciss-headline");if(!n){n=document.createElement("div");n.id="jh-ciss-headline";n.style.cssText="margin:12px 0;padding:10px 12px;background:#12121a;border:1px solid #2a2a36;border-radius:8px;color:#e6ecf5;font:13px IBM Plex Mono,monospace";document.body.insertBefore(n,document.body.firstChild);}n.textContent="CISS EA "+(d.ea_composite!=null?d.ea_composite:"\u2014")+"  "+(d.ea_regime||"")+"  as of "+(d.observation_date||"")+"  (headline, not full series)";});})();</script>\n'
        p.write_text(t.replace("</body>", js + "</body>", 1))
        print("wired ciss headline")
