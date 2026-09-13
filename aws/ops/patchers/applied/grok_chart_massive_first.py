#!/usr/bin/env python3
"""Stocks: warehouse /ohlc (Massive daily bank then tv-bars) BEFORE Yahoo scrape.
Kill leftover synth on empty load. Volume: share-count field, never close price.
"""
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    old_urls = '''    var urls=[
      LIVE+"/data/series/"+encodeURIComponent(ys)+".json",
      LIVE+"/data/series/"+encodeURIComponent(t)+".json",
      "/api/klines?symbol="+encodeURIComponent(t)+"&interval="+encodeURIComponent(sp[0])+"&limit=1000",
      "/api/yahoo?ticker="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(t)+"&range="+sp[3]+"&interval="+sp[2],
      PROXY+"/ohlc?ticker="+encodeURIComponent(t),'''
    new_urls = '''    var urls=[
      PROXY+"/ohlc?ticker="+encodeURIComponent(t),
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],
      PROXY+"/yf-ohlc?symbol="+encodeURIComponent(t)+"&range="+sp[3]+"&interval="+sp[2],
      LIVE+"/data/series/"+encodeURIComponent(ys)+".json",
      LIVE+"/data/series/"+encodeURIComponent(t)+".json",
      "/api/klines?symbol="+encodeURIComponent(t)+"&interval="+encodeURIComponent(sp[0])+"&limit=1000",
      "/api/yahoo?ticker="+encodeURIComponent(ys)+"&range="+sp[3]+"&interval="+sp[2],'''
    if old_urls in t:
        t = t.replace(old_urls, new_urls, 1)
        print("ohlc warehouse first")
    else:
        print("url list drifted")
    old_ls = '''          lastSource=urls[i].indexOf("/api/klines")===0?"binance/local": urls[i].indexOf("/api/")===0?"local": urls[i].indexOf(PROXY)===0?"proxy": urls[i].indexOf(LIVE)===0?"warehouse":"feed";'''
    new_ls = '''          lastSource=(raw&& (raw.warehouse_key||raw.source)) || (urls[i].indexOf("/ohlc")>=0?"warehouse": urls[i].indexOf("/api/klines")===0?"binance": urls[i].indexOf(PROXY)===0?"proxy": "feed");'''
    if "var d=toBars(await fetchJson(urls[i]));" in t:
        t = t.replace("var d=toBars(await fetchJson(urls[i]));", "var raw=await fetchJson(urls[i]); var d=toBars(raw);", 1)
        print("capture raw source")
    if old_ls in t:
        t = t.replace(old_ls, new_ls, 1)
        print("lastSource from warehouse_key")
    t = t.replace('if(!d.length){ d=synth(bare(active),400); lastSource="synth"; }', 'if(!d.length){ lastSource="unavailable"; }')
    t = t.replace('var d2=synth(bare(active),400); lastSource="synth";', 'var d2=[]; lastSource="unavailable";')
    dirty = "volume:+(b.volume||b.v||b.value||b.vol||0)"
    clean = "volume:+(function(){ var px=c3; var cand=b.volume!=null?b.volume:(b.v!=null?b.v:(b.vol!=null?b.vol:b.value)); var n=+cand; if(!isFinite(n)||n<0) return 0; if(b.volume==null&&b.v==null&&b.vol==null&&n>0&&n<px*5) return 0; return n; })()"
    if dirty in t:
        t = t.replace(dirty, clean, 1)
        print("volume heuristic")
    p.write_text(t)
    print("patched", p)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
