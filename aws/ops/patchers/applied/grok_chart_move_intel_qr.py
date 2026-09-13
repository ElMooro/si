#!/usr/bin/env python3
from pathlib import Path

def main():
    html = Path("chart.html")
    eng = Path("jh-chart-engine.js")
    if not html.exists():
        root = Path(__file__).resolve().parents[3]
        html, eng = root / "chart.html", root / "jh-chart-engine.js"
    h = html.read_text()
    if "desk-intel" not in h:
        h = h.replace(
            '<aside class="watch" id="watch">',
            '<aside class="watch" id="watch"><div id="desk-intel" style="font-size:10px;line-height:1.35;padding:6px 8px;border-bottom:1px solid var(--bd);color:var(--mut)"></div>',
            1,
        )
        print("desk-intel")
    if "#tape{display:none}" not in h and "id=\"tape\"" in h:
        h = h.replace("<head>", "<head><style>#tape{display:none!important}</style>", 1)
        print("hide mid-tape")
    html.write_text(h)
    t = eng.read_text()
    t = t.replace(
        "fillTape(document.getElementById(\"tape\"));",
        "/* tape strip above chart disabled */",
        1,
    )
    # daily blotter when no prints
    old = "Waiting for tape"
    if "daily warehouse blotter" not in t and "No public prints" in t:
        pass
    p = Path("jh-chart-engine.js") if Path("jh-chart-engine.js").exists() else eng
    t2 = t
    needle = "function fillTape(el){\n    if(!el) return;\n    var rows=tape.prints.slice().reverse();"
    insert = '''function fillTape(el){
    if(!el) return;
    if((!tape.prints || !tape.prints.length) && window.lastBars && lastBars.length){
      var lb=lastBars.slice(-40), i, html="<div class=qrbar><b>QR</b> "+active+" <span>daily warehouse — not SIP ticks</span></div><div class=qrbody>";
      for(i=lb.length-1;i>=0;i--){
        var b=lb[i], up=b.close>=b.open, d=new Date(b.time*1000);
        html+="<div style=display:flex;gap:8px;font-variant-numeric:tabular-nums><span>"+d.toISOString().slice(0,10)+"</span><span style=color:"+(up?"#089981":"#f23645")+">"+b.close.toFixed(2)+"</span><span>"+Math.round(b.volume||0).toLocaleString()+"</span></div>";
      }
      el.innerHTML=html+"</div>"; return;
    }
    var rows=tape.prints.slice().reverse();'''
    if needle in t2:
        t2 = t2.replace(needle, insert, 1)
        print("blotter")
    else:
        print("fillTape drifted")
    Path(eng).write_text(t2)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
