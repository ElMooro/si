#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    old = 'PROXY+"/ohlc?ticker="+encodeURIComponent(t),' 
    new = 'PROXY+"/ohlc?ticker="+encodeURIComponent(t)+"&span="+((/^[0-9]+[sm]$/.test(tfId)||tfId==="1m"||tfId==="3m"||tfId==="5m"||tfId==="15m"||tfId==="30m"||tfId==="45m")?"minute": (/h$/.test(tfId)?"hour":"day")),'
    if '"&span="' in t:
        print("span already")
    elif old in t:
        t = t.replace(old, new, 1)
        print("ohlc span by tf")
    else:
        print("ohlc url drifted")
    # cap minute flood
    cap = "if(d.length>=8){"
    capn = "if(d.length>8000) d=d.slice(-8000); if(d.length>=8){"
    if "d.slice(-8000)" not in t and cap in t:
        t = t.replace(cap, capn, 1)
        print("cap 8k bars")
    oldl = '''document.getElementById("legend").innerHTML="<div style='color:var(--fg);font-weight:500;margin-bottom:4px'>"+active+" · "+tf+(compare.length?" + "+compare.join(" "):"")+"</div>"+INDS.filter(function(i){return i.on;}).map(function(i){ return "<button style=color:"+i.c+" data-i='"+i.id+"'>"+i.n+v(i.id)+"</button>"; }).join("")'''
    newl = '''document.getElementById("legend").innerHTML="<div style='color:var(--fg);font-weight:500;margin-bottom:4px'>"+active+" · "+tf+" <button id=leghideall style=font-size:10px>hide all</button></div>"+INDS.map(function(i){ return "<button style=color:"+i.c+";opacity:"+(i.on?1:.35)+" data-i='"+i.id+"'>"+(i.on?"◉ ":"○ ")+i.n+v(i.id)+"</button>"; }).join("")'''
    if "leghideall" in t:
        print("legend already")
    elif "#legend" in t and "INDS.filter(function(i){return i.on;})" in t:
        t = t.replace(
            "INDS.filter(function(i){return i.on;}).map(function(i){ return \"<button style=color:\"+i.c+\" data-i='\"+i.id+\"'>\"+i.n+v(i.id)+\"</button>\"; }).join(\"\")",
            "INDS.map(function(i){ return \"<button style=color:\"+i.c+\";opacity:\"+(i.on?1:.35)+\" data-i='\"+i.id+\"'>\"+(i.on?\"◉ \":\"○ \")+i.n+v(i.id)+\"</button>\"; }).join(\"\")",
            1,
        )
        print("legend eyes")
    p.write_text(t)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
