#!/usr/bin/env python3
"""Open the chart on a readable window instead of fitContent of the whole archive.

Screen fit is about 7px per bar. Chips 6M / 1Y / 5Y / All stay one click away.
All uses log when every close is positive, and restores the previous scale on the
way out. Key 0 is the screen window. Shift+0 is all history. Volume profile and
high/low lines stay on. Does not touch sanitize, round, or the volume pane.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENG = ROOT / "jh-chart-engine.js"
CHART = ROOT / "chart.html"
MARKER = "/* screen-fit-20261112 */"

BLOCK = r"""  /* screen-fit-20261112 */
  var viewMode="screen";
  var scaleBeforeAll=null;
  function plotWidthPx(){
    var w=(host&&host.clientWidth)||640;
    var axis=76;
    try{ var pw=chart&&chart.priceScale("right").width(); if(pw>40) axis=pw; }catch(e){}
    return Math.max(240, w-axis);
  }
  function barTimeSec(b){
    if(!b) return null;
    var t=b.time;
    if(typeof t==="number" && isFinite(t)) return t;
    if(t && typeof t==="object" && t.year) return Date.UTC(t.year, (t.month||1)-1, t.day||1)/1000;
    if(typeof t==="string" && /^\d{4}-\d{2}-\d{2}/.test(t)){ var ms=Date.parse(t+"T00:00:00Z"); if(isFinite(ms)) return ms/1000; }
    return null;
  }
  function syncScale(){
    try{ chart.priceScale("right").applyOptions({ mode: rightScaleMode() }); }catch(e){}
  }
  function allScaleOk(bars){
    var i;
    for(i=0;i<bars.length;i++) if(!(bars[i]&&bars[i].close>0)) return false;
    return true;
  }
  function leaveAllScale(){
    if(scaleBeforeAll==null) return;
    scaleMode=scaleBeforeAll;
    scaleBeforeAll=null;
    syncScale();
  }
  function enterAllScale(){
    if(mode!=="price" || comparingOn() || scaleMode===1) return;
    if(!allScaleOk(lastBars||[])) return;
    scaleBeforeAll=scaleMode;
    scaleMode=1;
    syncScale();
  }
  function applyScreenFit(target){
    target=target||chart;
    var bars=lastBars||[];
    var n=bars.length;
    var barPx=7, rightPad=12;
    var slots=Math.max(rightPad+24, Math.floor(plotWidthPx()/barPx));
    var count=Math.min(n, Math.max(24, slots-rightPad));
    try{ target.timeScale().applyOptions({ barSpacing:barPx, rightOffset:rightPad, lockVisibleTimeRangeOnResize:false }); }catch(e){}
    if(n<8){ try{ target.timeScale().fitContent(); }catch(e2){} return; }
    try{ target.timeScale().setVisibleLogicalRange({ from:n-count, to:(n-1)+rightPad }); }
    catch(e3){ try{ target.timeScale().fitContent(); }catch(e4){} }
  }
  function applyRangeYears(years){
    var bars=lastBars||[];
    var n=bars.length;
    if(n<8 || !chart){ applyScreenFit(); return; }
    var last=barTimeSec(bars[n-1]);
    var from=0, i, ts;
    if(last!=null){
      var cut=last-years*365.25*86400;
      for(i=0;i<n;i++){ ts=barTimeSec(bars[i]); if(ts!=null && ts>=cut){ from=i; break; } }
    } else from=Math.max(0, n-Math.round(years*252));
    if(n-from<8) from=Math.max(0, n-8);
    try{ chart.timeScale().applyOptions({ rightOffset:8, lockVisibleTimeRangeOnResize:true }); }catch(e){}
    try{ chart.timeScale().setVisibleLogicalRange({ from:from, to:(n-1)+8 }); }
    catch(e2){ try{ chart.timeScale().fitContent(); }catch(e3){} }
  }
  function applyAllHistory(){
    try{ chart.timeScale().applyOptions({ lockVisibleTimeRangeOnResize:true }); }catch(e){}
    try{ chart.timeScale().fitContent(); }catch(e2){}
  }
  function applyView(){
    if(viewMode==="6m") return applyRangeYears(0.5);
    if(viewMode==="1y") return applyRangeYears(1);
    if(viewMode==="5y") return applyRangeYears(5);
    if(viewMode==="all"){
      if(scaleMode===1 && !allScaleOk(lastBars||[])){
        scaleMode=scaleBeforeAll!=null?scaleBeforeAll:0;
        syncScale();
      }
      return applyAllHistory();
    }
    return applyScreenFit();
  }
  function setViewMode(next){
    next=next||"screen";
    if(next!=="all") leaveAllScale();
    viewMode=next;
    if(viewMode==="all") enterAllScale();
    applyView();
    preserveView=true;
    try{ renderTf(); }catch(e){}
    try{ requestAnimationFrame(function(){ refreshHiLoVP(); }); }catch(e2){}
  }
  function viewChipHtml(){
    var views=[["screen","Screen","Readable window, about 7px per bar. Key 0"],["6m","6M","Six months"],["1y","1Y","One year"],["5y","5Y","Five years"],["all","All","All history, log when every close is positive. Shift+0"]];
    return views.map(function(v){
      return "<button class='"+(viewMode===v[0]?"on":"")+"' data-view='"+v[0]+"' title='"+v[2]+"'>"+v[1]+"</button>";
    }).join("")+"<span class=sep></span>";
  }
  function copyMainRange(target){
    try{
      var vr=chart&&chart.timeScale().getVisibleLogicalRange();
      if(vr&&vr.from!=null&&vr.to!=null){ target.timeScale().setVisibleLogicalRange(vr); return; }
    }catch(e){}
    try{ target.timeScale().fitContent(); }catch(e2){}
  }
  function showRecentBars(n){
"""

REPLACEMENTS = [
    (
        "timeScale:{ borderColor:p.border, borderVisible:true, timeVisible:true, secondsVisible:sec, rightOffset:12, ticksVisible:true, lockVisibleTimeRangeOnResize:true }",
        "timeScale:{ borderColor:p.border, borderVisible:true, timeVisible:true, secondsVisible:sec, rightOffset:12, ticksVisible:true, lockVisibleTimeRangeOnResize:false }",
        1,
    ),
    (
        "if(preserveView&&saved)chart.timeScale().setVisibleLogicalRange(saved);else chart.timeScale().fitContent();",
        "if(preserveView&&saved)chart.timeScale().setVisibleLogicalRange(saved);else { try{ applyView(); }catch(e){ chart.timeScale().fitContent(); } }",
        1,
    ),
    (
        """    } else {
      try{ chart.timeScale().fitContent(); }catch(e){}
    }
    try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}""",
        """    } else {
      try{ applyView(); }catch(e){ try{ chart.timeScale().fitContent(); }catch(e2){} }
    }
    try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}""",
        1,
    ),
    (
        "  function showRecentBars(n){",
        BLOCK,
        1,
    ),
    (
        "refs[i].timeScale().fitContent()",
        "copyMainRange(refs[i])",
        2,
    ),
    (
        """      tfHtml+
      "<button class=drop id=btn-tfmore title='All intervals'>▾</button>"+
      "<span class=sep></span>"+""",
        """      tfHtml+
      "<button class=drop id=btn-tfmore title='All intervals'>▾</button>"+
      "<span class=sep></span>"+
      viewChipHtml()+""",
        1,
    ),
    (
        """document.querySelectorAll("#tfbar [data-tf]").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });""",
        """document.querySelectorAll("#tfbar [data-tf]").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });
    document.querySelectorAll("#tfbar [data-view]").forEach(function(b){ b.onclick=function(){ setViewMode(b.dataset.view); }; });""",
        1,
    ),
    (
        """if(b.dataset.a==="rst"){ try{ chart.timeScale().fitContent(); }catch(e){} }""",
        """if(b.dataset.a==="rst"){ try{ setViewMode("screen"); }catch(e){} }""",
        1,
    ),
    (
        """if(e.key==="0"){ try{ chart.timeScale().fitContent(); }catch(err){} return; }""",
        """if(e.key==="0" || (e.shiftKey && e.code==="Digit0")){ e.preventDefault(); try{ setViewMode(e.shiftKey?"all":"screen"); }catch(err){ try{ chart.timeScale().fitContent(); }catch(e2){} } return; }""",
        1,
    ),
    (
        """["+ / − / 0","Zoom in / out / fit"]""",
        """["+ / − / 0","Zoom in / out / screen"],["Shift+0","All history"]""",
        1,
    ),
    (
        "v12.41",
        "v12.42",
        3,
    ),
]


def die(msg):
    raise SystemExit(msg)


def patch_engine(text):
    if MARKER in text:
        print("engine already has screen fit")
        return text, False
    for old, new, n in REPLACEMENTS:
        got = text.count(old)
        if got != n:
            die("engine needle count %s wanted %s: %s" % (got, n, old[:80].replace("\n", " ")))
    for old, new, n in REPLACEMENTS:
        text = text.replace(old, new, n)
    if text.count(MARKER) != 1:
        die("marker missing after rewrite")
    if text.count("lockVisibleTimeRangeOnResize:false") < 1:
        die("resize lock default not false")
    if text.count("lockVisibleTimeRangeOnResize:true") != 2:
        die("calendar lock count %s" % text.count("lockVisibleTimeRangeOnResize:true"))
    if "vpOn=true" not in text or "hiLo=true" not in text:
        die("profile or hi/lo default disappeared")
    return text, True


def patch_chart(text):
    old = "us10y=20261021c&ldr=20261105"
    new = "us10y=20261021c&fit=20261112&ldr=20261105"
    if new in text:
        print("chart cache-bust already present")
        return text, False
    if text.count(old) != 1:
        die("engine script cache-bust needle count %s" % text.count(old))
    text = text.replace(old, new, 1)
    if text.split('&ldr=20261105"').__len__() != 3:
        die("ldr cache-bust count changed; desk-engines.test.js would fail")
    return text, True


if __name__ == "__main__":
    if not ENG.is_file() or not CHART.is_file():
        die("chart files missing")
    eng, eng_changed = patch_engine(ENG.read_text())
    chart, chart_changed = patch_chart(CHART.read_text())
    if eng_changed:
        ENG.write_text(eng)
        print("patched", ENG)
    if chart_changed:
        CHART.write_text(chart)
        print("patched", CHART)
    if not eng_changed and not chart_changed:
        print("no changes")
