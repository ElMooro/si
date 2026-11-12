#!/usr/bin/env python3
"""Quiet the lines that cross candle bodies, and show each bar's change and volume.

Wicks stay. 5Y and All stay candles. Printed prices are not rewritten.
Screen-fit is unchanged. v12.41 stays.
"""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENG = ROOT / "jh-chart-engine.js"
HTML = ROOT / "chart.html"
MACRO = ROOT / "tests/helpers/chart-macro-line-preservation.cjs"
MARKER = "candle-clarity-20261112"
WICK_Q = "us10y=20261021c&fit=20261112&wick=20261112&ldr=20261105"
HTML_OLD = "us10y=20261021c&fit=20261112&ldr=20261105"
ENG_NOW = "5532845c393c4561aab72b10f095961a0c01346fdaff2ca475c47594e638ba20"
HTML_NOW = "7422d4985604ff15f771b05b44bf20239328b03720cb88516028e720a005d8b6"
DECL = "function peelScreenFit(raw,file){"
NORM = "function normalize(raw,file){raw=peelScreenFit(raw,file);"
NORM_NEW = "function normalize(raw,file){raw=peelCandleClarity(raw,file);raw=peelScreenFit(raw,file);"
EXP = "module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr,peelScreenFit};"
EXP_NEW = "module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr,peelScreenFit,peelCandleClarity};"

PAIRS = json.loads(r"""{"eng": [["grid:\"rgba(42,46,57,0.70)\",gridH:\"#2a2e39\"", "grid:\"rgba(42,46,57,0.70)\",gridH:\"rgba(42,46,57,0.70)\""], ["grid:\"#f0f3fa\",gridH:\"#e8ecf5\"", "grid:\"#f0f3fa\",gridH:\"#f0f3fa\""], ["var pl=mainSeries.createPriceLine({ price:+px, color:color||\"#787b86\", lineWidth:1, lineStyle:style==null?2:style, axisLabelVisible:true, title:title||\"\" });", "var quiet=key===\"H\"||key===\"L\"||key===\"POC\"||key===\"VAH\"||key===\"VAL\";\n      var pl=mainSeries.createPriceLine({ price:+px, color:color||\"#787b86\", lineWidth:1, lineStyle:style==null?2:style, lineVisible:!quiet, axisLabelVisible:true, title:title||\"\" });"], ["try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}", "try{ requestAnimationFrame(function(){ if(current()){ refreshHiLoVP(); try{ barReadout(null); }catch(eBr){} } }); }catch(eR){}"], ["try{ requestAnimationFrame(function(){ refreshHiLoVP(); }); }catch(e2){}", "try{ requestAnimationFrame(function(){ refreshHiLoVP(); try{ barReadout(null); }catch(eBr){} }); }catch(e2){}"], ["  chart.subscribeCrosshairMove(function(param){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)){if(hud){hud.style.display=\"\";hud.textContent=observationText(lastBars,param&&param.time);}if(ohlc)ohlc.textContent=observationText(lastBars,param&&param.time);renderDwin(param);return;}\n    if(!param||!param.time||!mainSeries){ hud.style.display=\"none\"; if(dwinOn){ var dw=document.getElementById(\"dwin\"); if(dw) dw.style.opacity=\".55\"; } return; }\n    var d=param.seriesData.get(mainSeries); if(!d){ hud.style.display=\"none\"; return; }\n    var o=d.open!=null?d.open:d.value, h=d.high!=null?d.high:d.value, l=d.low!=null?d.low:d.value, c=d.close!=null?d.close:d.value;\n    var extra=\"\", iH, bar=null, ix=-1;\n    if(lastBars && lastBars.length){\n      for(iH=0;iH<lastBars.length;iH++) if(lastBars[iH].time===param.time){ bar=lastBars[iH]; ix=iH; break; }\n    }\n    if(bar){\n      var rH=rvolAt(lastBars,ix,20);\n      var atrV=valAt(lastAtrPts, param.time);\n      var vwH=valAt(lastHudVwap, param.time);\n      extra=\"  ATR \"+(atrV!=null?fmt(atrV):\"\u2014\")+\"  RVOL \"+(rH!==null?(rH>0&&rH<0.01?\"&lt;0.01\":rH.toFixed(2))+\"x\":\"Unavailable\");\n      if(vwH!=null && c) extra+=\"  VWAP \"+((c-vwH)>=0?\"+\":\"\")+((c-vwH)/vwH*100).toFixed(2)+\"%\";\n      if(lastVP.poc!=null) extra+=\"  POC \"+fmt(lastVP.poc)+(c>lastVP.vah?\" \u2191VA\": c<lastVP.val?\" \u2193VA\":\" inVA\");\n      if(lastAdr && lastAdr.adr) extra+=\"  ADR \"+lastAdr.used.toFixed(0)+\"%\";\n    }\n    hud.style.display=\"block\";\n    hud.innerHTML=new Date((param.time+tzOff*3600)*1000).toISOString().slice(0,16).replace(\"T\",\" \")+\" \"+tzName+\"  O \"+fmt(o)+\" H \"+fmt(h)+\" L \"+fmt(l)+\" C \"+fmt(c)+extra;\n    if(ohlc) ohlc.innerHTML=\"O \"+fmt(o)+\"  H <span class=up>\"+fmt(h)+\"</span>  L <span class=dn>\"+fmt(l)+\"</span>  C \"+fmt(c)+extra;\n    renderLegend(param.time);\n    renderDwin(param);\n  });\n", "  /* candle-clarity-20261112 */\n  function barReadout(time, seriesPt){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)) return;\n    if(!lastBars||!lastBars.length) return;\n    var ix=-1, bar=null, iH;\n    if(time!=null){\n      for(iH=0;iH<lastBars.length;iH++) if(lastBars[iH].time===time){ bar=lastBars[iH]; ix=iH; break; }\n    }\n    var pin=!bar;\n    if(pin){ bar=lastBars[lastBars.length-1]; ix=lastBars.length-1; time=bar.time; seriesPt=null; }\n    var o,h,l,c;\n    if(seriesPt){\n      o=seriesPt.open!=null?seriesPt.open:seriesPt.value;\n      h=seriesPt.high!=null?seriesPt.high:seriesPt.value;\n      l=seriesPt.low!=null?seriesPt.low:seriesPt.value;\n      c=seriesPt.close!=null?seriesPt.close:seriesPt.value;\n    } else {\n      o=bar.open; h=bar.high; l=bar.low; c=bar.close;\n    }\n    var extra=\"\", prev=ix>0?lastBars[ix-1]:null;\n    var curR=roundTick(c), prevR=prev?roundTick(prev.close):null;\n    if(prevR!=null && isFinite(prevR) && prevR!==0 && isFinite(curR)){\n      var dlt=curR-prevR, up=dlt>=0;\n      extra+=\"  <span class=\"+(up?\"up\":\"dn\")+\">\"+(up?\"+\":\"\")+fmt(dlt)+\" \"+fmtXs(dlt/prevR)+\"</span>\";\n    }\n    extra+=\"  Vol \"+fmtVol(reportedVolume(bar.volume));\n    var rH=rvolAt(lastBars,ix,20);\n    var atrV=valAt(lastAtrPts, time);\n    var vwH=valAt(lastHudVwap, time);\n    extra+=\"  ATR \"+(atrV!=null?fmt(atrV):\"\u2014\")+\"  RVOL \"+(rH!==null?(rH>0&&rH<0.01?\"<0.01\":rH.toFixed(2))+\"x\":\"Unavailable\");\n    if(vwH!=null && c) extra+=\"  VWAP \"+((c-vwH)>=0?\"+\":\"\")+((c-vwH)/vwH*100).toFixed(2)+\"%\";\n    if(lastVP.poc!=null) extra+=\"  POC \"+fmt(lastVP.poc)+(c>lastVP.vah?\" \u2191VA\": c<lastVP.val?\" \u2193VA\":\" inVA\");\n    if(lastAdr && lastAdr.adr) extra+=\"  ADR \"+lastAdr.used.toFixed(0)+\"%\";\n    var body=\"O \"+fmt(o)+\" H \"+fmt(h)+\" L \"+fmt(l)+\" C \"+fmt(c)+extra;\n    if(hud){\n      if(pin) hud.style.display=\"none\";\n      else {\n        hud.style.display=\"block\";\n        hud.innerHTML=new Date((time+tzOff*3600)*1000).toISOString().slice(0,16).replace(\"T\",\" \")+\" \"+tzName+\"  \"+body;\n      }\n    }\n    if(ohlc) ohlc.innerHTML=body;\n  }\n  chart.subscribeCrosshairMove(function(param){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)){if(hud){hud.style.display=\"\";hud.textContent=observationText(lastBars,param&&param.time);}if(ohlc)ohlc.textContent=observationText(lastBars,param&&param.time);renderDwin(param);return;}\n    if(!param||!param.time||!mainSeries){ try{ barReadout(null); }catch(e){} if(dwinOn){ var dw=document.getElementById(\"dwin\"); if(dw) dw.style.opacity=\".55\"; } return; }\n    var d=param.seriesData.get(mainSeries); if(!d){ try{ barReadout(null); }catch(e2){} return; }\n    try{ barReadout(param.time, d); }catch(e3){}\n    renderLegend(param.time);\n    renderDwin(param);\n  });\n"]], "html": ["us10y=20261021c&fit=20261112&ldr=20261105", "us10y=20261021c&fit=20261112&wick=20261112&ldr=20261105"], "eng_now": "5532845c393c4561aab72b10f095961a0c01346fdaff2ca475c47594e638ba20", "eng_prior": "c5df7d4d54010ebf1b6ad76a33cffc465086c99b4ae75ec8ab085d428ade27b7", "html_now": "7422d4985604ff15f771b05b44bf20239328b03720cb88516028e720a005d8b6", "html_prior": "3a5abb78e73edae965c2a19fbebc943102f7a6cff35b93af81fe46cda81ca59f"}""")
PEEL = r"""function peelCandleClarity(raw,file){
 const expect={
  'jh-chart-engine.js':{now:'5532845c393c4561aab72b10f095961a0c01346fdaff2ca475c47594e638ba20',prior:'c5df7d4d54010ebf1b6ad76a33cffc465086c99b4ae75ec8ab085d428ade27b7',pairs:[{"old": "grid:\"rgba(42,46,57,0.70)\",gridH:\"#2a2e39\"", "new": "grid:\"rgba(42,46,57,0.70)\",gridH:\"rgba(42,46,57,0.70)\"", "n": 1}, {"old": "grid:\"#f0f3fa\",gridH:\"#e8ecf5\"", "new": "grid:\"#f0f3fa\",gridH:\"#f0f3fa\"", "n": 1}, {"old": "var pl=mainSeries.createPriceLine({ price:+px, color:color||\"#787b86\", lineWidth:1, lineStyle:style==null?2:style, axisLabelVisible:true, title:title||\"\" });", "new": "var quiet=key===\"H\"||key===\"L\"||key===\"POC\"||key===\"VAH\"||key===\"VAL\";\n      var pl=mainSeries.createPriceLine({ price:+px, color:color||\"#787b86\", lineWidth:1, lineStyle:style==null?2:style, lineVisible:!quiet, axisLabelVisible:true, title:title||\"\" });", "n": 1}, {"old": "try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}", "new": "try{ requestAnimationFrame(function(){ if(current()){ refreshHiLoVP(); try{ barReadout(null); }catch(eBr){} } }); }catch(eR){}", "n": 1}, {"old": "try{ requestAnimationFrame(function(){ refreshHiLoVP(); }); }catch(e2){}", "new": "try{ requestAnimationFrame(function(){ refreshHiLoVP(); try{ barReadout(null); }catch(eBr){} }); }catch(e2){}", "n": 1}, {"old": "  chart.subscribeCrosshairMove(function(param){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)){if(hud){hud.style.display=\"\";hud.textContent=observationText(lastBars,param&&param.time);}if(ohlc)ohlc.textContent=observationText(lastBars,param&&param.time);renderDwin(param);return;}\n    if(!param||!param.time||!mainSeries){ hud.style.display=\"none\"; if(dwinOn){ var dw=document.getElementById(\"dwin\"); if(dw) dw.style.opacity=\".55\"; } return; }\n    var d=param.seriesData.get(mainSeries); if(!d){ hud.style.display=\"none\"; return; }\n    var o=d.open!=null?d.open:d.value, h=d.high!=null?d.high:d.value, l=d.low!=null?d.low:d.value, c=d.close!=null?d.close:d.value;\n    var extra=\"\", iH, bar=null, ix=-1;\n    if(lastBars && lastBars.length){\n      for(iH=0;iH<lastBars.length;iH++) if(lastBars[iH].time===param.time){ bar=lastBars[iH]; ix=iH; break; }\n    }\n    if(bar){\n      var rH=rvolAt(lastBars,ix,20);\n      var atrV=valAt(lastAtrPts, param.time);\n      var vwH=valAt(lastHudVwap, param.time);\n      extra=\"  ATR \"+(atrV!=null?fmt(atrV):\"—\")+\"  RVOL \"+(rH!==null?(rH>0&&rH<0.01?\"&lt;0.01\":rH.toFixed(2))+\"x\":\"Unavailable\");\n      if(vwH!=null && c) extra+=\"  VWAP \"+((c-vwH)>=0?\"+\":\"\")+((c-vwH)/vwH*100).toFixed(2)+\"%\";\n      if(lastVP.poc!=null) extra+=\"  POC \"+fmt(lastVP.poc)+(c>lastVP.vah?\" ↑VA\": c<lastVP.val?\" ↓VA\":\" inVA\");\n      if(lastAdr && lastAdr.adr) extra+=\"  ADR \"+lastAdr.used.toFixed(0)+\"%\";\n    }\n    hud.style.display=\"block\";\n    hud.innerHTML=new Date((param.time+tzOff*3600)*1000).toISOString().slice(0,16).replace(\"T\",\" \")+\" \"+tzName+\"  O \"+fmt(o)+\" H \"+fmt(h)+\" L \"+fmt(l)+\" C \"+fmt(c)+extra;\n    if(ohlc) ohlc.innerHTML=\"O \"+fmt(o)+\"  H <span class=up>\"+fmt(h)+\"</span>  L <span class=dn>\"+fmt(l)+\"</span>  C \"+fmt(c)+extra;\n    renderLegend(param.time);\n    renderDwin(param);\n  });\n", "new": "  /* candle-clarity-20261112 */\n  function barReadout(time, seriesPt){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)) return;\n    if(!lastBars||!lastBars.length) return;\n    var ix=-1, bar=null, iH;\n    if(time!=null){\n      for(iH=0;iH<lastBars.length;iH++) if(lastBars[iH].time===time){ bar=lastBars[iH]; ix=iH; break; }\n    }\n    var pin=!bar;\n    if(pin){ bar=lastBars[lastBars.length-1]; ix=lastBars.length-1; time=bar.time; seriesPt=null; }\n    var o,h,l,c;\n    if(seriesPt){\n      o=seriesPt.open!=null?seriesPt.open:seriesPt.value;\n      h=seriesPt.high!=null?seriesPt.high:seriesPt.value;\n      l=seriesPt.low!=null?seriesPt.low:seriesPt.value;\n      c=seriesPt.close!=null?seriesPt.close:seriesPt.value;\n    } else {\n      o=bar.open; h=bar.high; l=bar.low; c=bar.close;\n    }\n    var extra=\"\", prev=ix>0?lastBars[ix-1]:null;\n    var curR=roundTick(c), prevR=prev?roundTick(prev.close):null;\n    if(prevR!=null && isFinite(prevR) && prevR!==0 && isFinite(curR)){\n      var dlt=curR-prevR, up=dlt>=0;\n      extra+=\"  <span class=\"+(up?\"up\":\"dn\")+\">\"+(up?\"+\":\"\")+fmt(dlt)+\" \"+fmtXs(dlt/prevR)+\"</span>\";\n    }\n    extra+=\"  Vol \"+fmtVol(reportedVolume(bar.volume));\n    var rH=rvolAt(lastBars,ix,20);\n    var atrV=valAt(lastAtrPts, time);\n    var vwH=valAt(lastHudVwap, time);\n    extra+=\"  ATR \"+(atrV!=null?fmt(atrV):\"—\")+\"  RVOL \"+(rH!==null?(rH>0&&rH<0.01?\"<0.01\":rH.toFixed(2))+\"x\":\"Unavailable\");\n    if(vwH!=null && c) extra+=\"  VWAP \"+((c-vwH)>=0?\"+\":\"\")+((c-vwH)/vwH*100).toFixed(2)+\"%\";\n    if(lastVP.poc!=null) extra+=\"  POC \"+fmt(lastVP.poc)+(c>lastVP.vah?\" ↑VA\": c<lastVP.val?\" ↓VA\":\" inVA\");\n    if(lastAdr && lastAdr.adr) extra+=\"  ADR \"+lastAdr.used.toFixed(0)+\"%\";\n    var body=\"O \"+fmt(o)+\" H \"+fmt(h)+\" L \"+fmt(l)+\" C \"+fmt(c)+extra;\n    if(hud){\n      if(pin) hud.style.display=\"none\";\n      else {\n        hud.style.display=\"block\";\n        hud.innerHTML=new Date((time+tzOff*3600)*1000).toISOString().slice(0,16).replace(\"T\",\" \")+\" \"+tzName+\"  \"+body;\n      }\n    }\n    if(ohlc) ohlc.innerHTML=body;\n  }\n  chart.subscribeCrosshairMove(function(param){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)){if(hud){hud.style.display=\"\";hud.textContent=observationText(lastBars,param&&param.time);}if(ohlc)ohlc.textContent=observationText(lastBars,param&&param.time);renderDwin(param);return;}\n    if(!param||!param.time||!mainSeries){ try{ barReadout(null); }catch(e){} if(dwinOn){ var dw=document.getElementById(\"dwin\"); if(dw) dw.style.opacity=\".55\"; } return; }\n    var d=param.seriesData.get(mainSeries); if(!d){ try{ barReadout(null); }catch(e2){} return; }\n    try{ barReadout(param.time, d); }catch(e3){}\n    renderLegend(param.time);\n    renderDwin(param);\n  });\n", "n": 1}]},
  'chart.html':{now:'7422d4985604ff15f771b05b44bf20239328b03720cb88516028e720a005d8b6',prior:'3a5abb78e73edae965c2a19fbebc943102f7a6cff35b93af81fe46cda81ca59f',pairs:[{"old": "us10y=20261021c&fit=20261112&ldr=20261105", "new": "us10y=20261021c&fit=20261112&wick=20261112&ldr=20261105", "n": 1}]}
 };
 const row=expect[file];
 if(!row||hash(raw)!==row.now)return raw;
 let back=raw;
 for(let i=row.pairs.length-1;i>=0;i--){
  const p=row.pairs[i];
  const hits=back.split(p.new).length-1;
  if(hits!==p.n)throw new Error('candle-clarity peel count '+hits+' wanted '+p.n+' in '+file);
  back=back.split(p.new).join(p.old);
 }
 if(hash(back)!==row.prior)throw new Error('candle-clarity peel hash '+file+' '+hash(back));
 return back;
}
"""


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main():
    eng = ENG.read_text(encoding="utf-8")
    html = HTML.read_text(encoding="utf-8")
    macro = MACRO.read_text(encoding="utf-8")
    done_e = MARKER in eng
    done_h = WICK_Q in html
    done_m = "function peelCandleClarity(" in macro
    if done_e and done_h and done_m:
        print("candle clarity already present")
        return
    if done_e or done_h or done_m:
        raise SystemExit("partial candle clarity engine=%s html=%s macro=%s" % (done_e, done_h, done_m))
    if eng.count("v12.41") != 3 or "v12.42" in eng:
        raise SystemExit("version guard")
    if eng.count("wickVisible:true") < 1:
        raise SystemExit("wicks missing before edit")
    wicks = eng.count("wickVisible:true")
    for old, new in PAIRS["eng"]:
        n = eng.count(old)
        if n != 1:
            raise SystemExit("engine anchor %s for %s" % (n, old[:48]))
        eng = eng.replace(old, new, 1)
    if html.count(HTML_OLD) != 1:
        raise SystemExit("html anchor %s" % html.count(HTML_OLD))
    html = html.replace(HTML_OLD, WICK_Q, 1)
    if html.count('&ldr=20261105"') != 2:
        raise SystemExit("ldr count")
    if sha(eng) != ENG_NOW:
        raise SystemExit("engine hash " + sha(eng))
    if sha(html) != HTML_NOW:
        raise SystemExit("html hash " + sha(html))
    if eng.count("wickVisible:true") != wicks or eng.count("v12.41") != 3:
        raise SystemExit("wick or version changed")
    if macro.count(DECL) != 1 or macro.count(NORM) != 1 or macro.count(EXP) != 1:
        raise SystemExit("macro anchors")
    macro = macro.replace(DECL, PEEL + "\n" + DECL, 1)
    macro = macro.replace(NORM, NORM_NEW, 1)
    macro = macro.replace(EXP, EXP_NEW, 1)
    if macro.count("function peelCandleClarity(") != 1:
        raise SystemExit("peel insert")
    ENG.write_text(eng, encoding="utf-8")
    HTML.write_text(html, encoding="utf-8")
    MACRO.write_text(macro, encoding="utf-8")
    print("candle clarity applied")


if __name__ == "__main__":
    main()
