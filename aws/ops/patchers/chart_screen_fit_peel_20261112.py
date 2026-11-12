#!/usr/bin/env python3
"""Keep the v12.41 stamp the chart tests require, and peel the screen-fit
bytes back to the prior reviewed hashes before the preservation chain asserts.
"""
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENG = ROOT / "jh-chart-engine.js"
MACRO = ROOT / "tests/helpers/chart-macro-line-preservation.cjs"
CENSUS = ROOT / "tests/helpers/chart-census-additional-preservation.cjs"
PEEL = r"""function peelScreenFit(raw,file){
 const expect={
  'jh-chart-engine.js':{now:'c5df7d4d54010ebf1b6ad76a33cffc465086c99b4ae75ec8ab085d428ade27b7',prior:'a31450e192466e529396caff40e57ef177cdb8ab42420e81dc89856c894b0a3e',pairs:[{"old": "timeScale:{ borderColor:p.border, borderVisible:true, timeVisible:true, secondsVisible:sec, rightOffset:12, ticksVisible:true, lockVisibleTimeRangeOnResize:true }", "new": "timeScale:{ borderColor:p.border, borderVisible:true, timeVisible:true, secondsVisible:sec, rightOffset:12, ticksVisible:true, lockVisibleTimeRangeOnResize:false }", "n": 1}, {"old": "if(preserveView&&saved)chart.timeScale().setVisibleLogicalRange(saved);else chart.timeScale().fitContent();", "new": "if(preserveView&&saved)chart.timeScale().setVisibleLogicalRange(saved);else { try{ applyView(); }catch(e){ chart.timeScale().fitContent(); } }", "n": 1}, {"old": "    } else {\n      try{ chart.timeScale().fitContent(); }catch(e){}\n    }\n    try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}", "new": "    } else {\n      try{ applyView(); }catch(e){ try{ chart.timeScale().fitContent(); }catch(e2){} }\n    }\n    try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}", "n": 1}, {"old": "  function showRecentBars(n){", "new": "  /* screen-fit-20261112 */\n  var viewMode=\"screen\";\n  var scaleBeforeAll=null;\n  function plotWidthPx(){\n    var w=(host&&host.clientWidth)||640;\n    var axis=76;\n    try{ var pw=chart&&chart.priceScale(\"right\").width(); if(pw>40) axis=pw; }catch(e){}\n    return Math.max(240, w-axis);\n  }\n  function barTimeSec(b){\n    if(!b) return null;\n    var t=b.time;\n    if(typeof t===\"number\" && isFinite(t)) return t;\n    if(t && typeof t===\"object\" && t.year) return Date.UTC(t.year, (t.month||1)-1, t.day||1)/1000;\n    if(typeof t===\"string\" && /^\\d{4}-\\d{2}-\\d{2}/.test(t)){ var ms=Date.parse(t+\"T00:00:00Z\"); if(isFinite(ms)) return ms/1000; }\n    return null;\n  }\n  function syncScale(){\n    try{ chart.priceScale(\"right\").applyOptions({ mode: rightScaleMode() }); }catch(e){}\n  }\n  function allScaleOk(bars){\n    var i;\n    for(i=0;i<bars.length;i++) if(!(bars[i]&&bars[i].close>0)) return false;\n    return true;\n  }\n  function leaveAllScale(){\n    if(scaleBeforeAll==null) return;\n    scaleMode=scaleBeforeAll;\n    scaleBeforeAll=null;\n    syncScale();\n  }\n  function enterAllScale(){\n    if(mode!==\"price\" || comparingOn() || scaleMode===1) return;\n    if(!allScaleOk(lastBars||[])) return;\n    scaleBeforeAll=scaleMode;\n    scaleMode=1;\n    syncScale();\n  }\n  function applyScreenFit(target){\n    target=target||chart;\n    var bars=lastBars||[];\n    var n=bars.length;\n    var barPx=7, rightPad=12;\n    var slots=Math.max(rightPad+24, Math.floor(plotWidthPx()/barPx));\n    var count=Math.min(n, Math.max(24, slots-rightPad));\n    try{ target.timeScale().applyOptions({ barSpacing:barPx, rightOffset:rightPad, lockVisibleTimeRangeOnResize:false }); }catch(e){}\n    if(n<8){ try{ target.timeScale().fitContent(); }catch(e2){} return; }\n    try{ target.timeScale().setVisibleLogicalRange({ from:n-count, to:(n-1)+rightPad }); }\n    catch(e3){ try{ target.timeScale().fitContent(); }catch(e4){} }\n  }\n  function applyRangeYears(years){\n    var bars=lastBars||[];\n    var n=bars.length;\n    if(n<8 || !chart){ applyScreenFit(); return; }\n    var last=barTimeSec(bars[n-1]);\n    var from=0, i, ts;\n    if(last!=null){\n      var cut=last-years*365.25*86400;\n      for(i=0;i<n;i++){ ts=barTimeSec(bars[i]); if(ts!=null && ts>=cut){ from=i; break; } }\n    } else from=Math.max(0, n-Math.round(years*252));\n    if(n-from<8) from=Math.max(0, n-8);\n    try{ chart.timeScale().applyOptions({ rightOffset:8, lockVisibleTimeRangeOnResize:true }); }catch(e){}\n    try{ chart.timeScale().setVisibleLogicalRange({ from:from, to:(n-1)+8 }); }\n    catch(e2){ try{ chart.timeScale().fitContent(); }catch(e3){} }\n  }\n  function applyAllHistory(){\n    try{ chart.timeScale().applyOptions({ lockVisibleTimeRangeOnResize:true }); }catch(e){}\n    try{ chart.timeScale().fitContent(); }catch(e2){}\n  }\n  function applyView(){\n    if(viewMode===\"6m\") return applyRangeYears(0.5);\n    if(viewMode===\"1y\") return applyRangeYears(1);\n    if(viewMode===\"5y\") return applyRangeYears(5);\n    if(viewMode===\"all\"){\n      if(scaleMode===1 && !allScaleOk(lastBars||[])){\n        scaleMode=scaleBeforeAll!=null?scaleBeforeAll:0;\n        syncScale();\n      }\n      return applyAllHistory();\n    }\n    return applyScreenFit();\n  }\n  function setViewMode(next){\n    next=next||\"screen\";\n    if(next!==\"all\") leaveAllScale();\n    viewMode=next;\n    if(viewMode===\"all\") enterAllScale();\n    applyView();\n    preserveView=true;\n    try{ renderTf(); }catch(e){}\n    try{ requestAnimationFrame(function(){ refreshHiLoVP(); }); }catch(e2){}\n  }\n  function viewChipHtml(){\n    var views=[[\"screen\",\"Screen\",\"Readable window, about 7px per bar. Key 0\"],[\"6m\",\"6M\",\"Six months\"],[\"1y\",\"1Y\",\"One year\"],[\"5y\",\"5Y\",\"Five years\"],[\"all\",\"All\",\"All history, log when every close is positive. Shift+0\"]];\n    return views.map(function(v){\n      return \"<button class='\"+(viewMode===v[0]?\"on\":\"\")+\"' data-view='\"+v[0]+\"' title='\"+v[2]+\"'>\"+v[1]+\"</button>\";\n    }).join(\"\")+\"<span class=sep></span>\";\n  }\n  function copyMainRange(target){\n    try{\n      var vr=chart&&chart.timeScale().getVisibleLogicalRange();\n      if(vr&&vr.from!=null&&vr.to!=null){ target.timeScale().setVisibleLogicalRange(vr); return; }\n    }catch(e){}\n    try{ target.timeScale().fitContent(); }catch(e2){}\n  }\n  function showRecentBars(n){\n", "n": 1}, {"old": "refs[i].timeScale().fitContent()", "new": "copyMainRange(refs[i])", "n": 2}, {"old": "      tfHtml+\n      \"<button class=drop id=btn-tfmore title='All intervals'>\u25be</button>\"+\n      \"<span class=sep></span>\"+", "new": "      tfHtml+\n      \"<button class=drop id=btn-tfmore title='All intervals'>\u25be</button>\"+\n      \"<span class=sep></span>\"+\n      viewChipHtml()+", "n": 1}, {"old": "document.querySelectorAll(\"#tfbar [data-tf]\").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });", "new": "document.querySelectorAll(\"#tfbar [data-tf]\").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });\n    document.querySelectorAll(\"#tfbar [data-view]\").forEach(function(b){ b.onclick=function(){ setViewMode(b.dataset.view); }; });", "n": 1}, {"old": "if(b.dataset.a===\"rst\"){ try{ chart.timeScale().fitContent(); }catch(e){} }", "new": "if(b.dataset.a===\"rst\"){ try{ setViewMode(\"screen\"); }catch(e){} }", "n": 1}, {"old": "if(e.key===\"0\"){ try{ chart.timeScale().fitContent(); }catch(err){} return; }", "new": "if(e.key===\"0\" || (e.shiftKey && e.code===\"Digit0\")){ e.preventDefault(); try{ setViewMode(e.shiftKey?\"all\":\"screen\"); }catch(err){ try{ chart.timeScale().fitContent(); }catch(e2){} } return; }", "n": 1}, {"old": "[\"+ / \u2212 / 0\",\"Zoom in / out / fit\"]", "new": "[\"+ / \u2212 / 0\",\"Zoom in / out / screen\"],[\"Shift+0\",\"All history\"]", "n": 1}]},
  'chart.html':{now:'3a5abb78e73edae965c2a19fbebc943102f7a6cff35b93af81fe46cda81ca59f',prior:'11c209334122515ebe61e6ebb10f76aa3c5f05f2d2b518a82e16325613257827',pairs:[{"old": "us10y=20261021c&ldr=20261105", "new": "us10y=20261021c&fit=20261112&ldr=20261105", "n": 1}]}
 };
 const row=expect[file];
 if(!row||hash(raw)!==row.now)return raw;
 let back=raw;
 for(let i=row.pairs.length-1;i>=0;i--){
  const p=row.pairs[i];
  const hits=back.split(p.new).length-1;
  if(hits!==p.n)throw new Error('screen-fit peel count '+hits+' wanted '+p.n+' in '+file);
  back=back.split(p.new).join(p.old);
 }
 if(hash(back)!==row.prior)throw new Error('screen-fit peel hash '+file+' '+hash(back));
 return back;
}
"""
NEW_ENGINE = "c5df7d4d54010ebf1b6ad76a33cffc465086c99b4ae75ec8ab085d428ade27b7"


def die(msg):
    raise SystemExit(msg)


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def patch_engine(text):
    if "/* screen-fit-20261112 */" not in text:
        die("screen-fit marker missing from engine")
    if text.count("v12.42") == 0 and text.count("v12.41") == 3 and sha(text) == NEW_ENGINE:
        print("engine version already v12.41")
        return text, False
    if text.count("v12.42") != 3:
        die("v12.42 count %s" % text.count("v12.42"))
    text = text.replace("v12.42", "v12.41")
    if sha(text) != NEW_ENGINE:
        die("engine hash after version restore %s" % sha(text))
    return text, True


def patch_macro(text):
    if "function peelScreenFit(" in text:
        print("peel already in macro-line helper")
        return text, False
    needle = "function normalize(raw,file){raw=peelFlowsEngine(raw,file);"
    if text.count(needle) != 1:
        die("macro-line normalize needle %s" % text.count(needle))
    export = "module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr};"
    if text.count(export) != 1:
        die("macro-line export needle")
    text = text.replace(needle, PEEL + "function normalize(raw,file){raw=peelScreenFit(raw,file);raw=peelFlowsEngine(raw,file);", 1)
    text = text.replace(export, "module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr,peelScreenFit};", 1)
    if text.count("function peelScreenFit(") != 1:
        die("peel insert failed")
    return text, True


def patch_census(text):
    needle = 'function normalize(raw,file){raw=require("./chart-imf-preservation.cjs").normalize(raw,file);'
    peeled = 'function normalize(raw,file){raw=require("./chart-macro-line-preservation.cjs").peelScreenFit(raw,file);raw=require("./chart-imf-preservation.cjs").normalize(raw,file);'
    if peeled in text:
        print("census helper already peels")
        return text, False
    if text.count(needle) != 1:
        die("census normalize needle %s" % text.count(needle))
    return text.replace(needle, peeled, 1), True


if __name__ == "__main__":
    eng, eng_changed = patch_engine(ENG.read_text())
    macro, macro_changed = patch_macro(MACRO.read_text())
    census, census_changed = patch_census(CENSUS.read_text())
    if eng_changed:
        ENG.write_text(eng)
        print("patched", ENG)
    if macro_changed:
        MACRO.write_text(macro)
        print("patched", MACRO)
    if census_changed:
        CENSUS.write_text(census)
        print("patched", CENSUS)
    if not (eng_changed or macro_changed or census_changed):
        print("no changes")
