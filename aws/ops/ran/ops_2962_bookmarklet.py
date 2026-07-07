#!/usr/bin/env python3
"""ops 2962 — Self-contained TV notes bookmarklet (fixes CSP block).

TradingView's Content Security Policy blocks loading external scripts, so the
loader bookmarklet (which injected a <script src="justhodl.ai/..."> tag)
silently failed — hence "nothing showed up".

Fix: a SELF-CONTAINED bookmarklet — all JavaScript inline, Lambda URL and
ingest token baked in by this script, no external loading of any kind. Runs
entirely in the browser context on tradingview.com, so:
  - Same-origin API calls to TV endpoints work with no CORS issues
  - Fetch/XHR interception captures notes as TV loads them
  - UPLOAD posts directly to our Lambda (Lambda CORS allows *)
  - No CSP violation possible (javascript: bookmarklets bypass script-src)

Also updates tv-notes.html with a clear 3-step guide and a pre-built,
one-click-copyable bookmarklet using the exact Lambda URL + token.
"""
import json
import sys
import time
import urllib.parse
from pathlib import Path

import boto3
from ops_report import report

LAM    = boto3.client("lambda", region_name="us-east-1")
SSM    = boto3.client("ssm",   region_name="us-east-1")
S3     = boto3.client("s3",    region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT   = Path(__file__).resolve().parents[2]

INGEST_FN = "justhodl-tv-notes-ingest"


def ssm_get(name):
    try:
        return SSM.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def build_bookmarklet(ingest_url, token):
    """Return the full javascript: bookmarklet URL — self-contained, CSP-safe."""
    # The core extractor inline — no external deps, no script tags
    js = r"""(async function(){
if(window.__JHN){var b=document.getElementById('__jhnBox');if(b)b.style.display='block';return;}
var IU='""" + ingest_url + r"""',TOK='""" + token + r"""';
var S=new Map(),T=new Set();
function hid(s,t,x){var h=0,str=s+'|'+t+'|'+(x+'').slice(0,80);for(var i=0;i<str.length;i++)h=(h*31+str.charCodeAt(i))>>>0;return 'tv-'+h.toString(36);}
function keep(sym,text,title,ts){text=(text||'').trim();if(text.length<2)return;sym=(sym||'UNTAGGED').toUpperCase().slice(0,30);ts=ts||Date.now();if(typeof ts==='string'){try{ts=new Date(ts).getTime()||Date.now();}catch(e){ts=Date.now();}}var id=hid(sym,ts,text);if(!S.has(id)){S.set(id,{symbol:sym,text:text.slice(0,7000),title:title||'',created:+ts});T.add(sym);repaint();}}
function mine(o,h,d){if(!o||d>7)return;if(Array.isArray(o)){o.forEach(function(x){mine(x,h,d+1);});return;}if(typeof o!='object')return;var sym=o.symbol||o.ticker||o.s||h,text=o.text||o.note||o.content||o.body;if(text&&typeof text=='string'&&text.length>1&&(o.id!=null||o.created!=null||o.created_at!=null||o.updated_at!=null)){keep(sym,text,o.title||o.name,new Date(o.created_at||o.created||0).getTime()||Date.now());}for(var k in o){if(o[k]&&typeof o[k]=='object')mine(o[k],sym||h,d+1);}}
var _f=window.fetch.bind(window);
window.fetch=function(u,i){var url=typeof u=='string'?u:(u&&u.url)||'',p=_f(u,i);if(/note/i.test(url)&&!/notif|notice/i.test(url)){p.then(function(r){try{r.clone().json().then(function(j){mine(j,null,0);}).catch(function(){});}catch(e){}}).catch(function(){});}return p;};
var _xo=XMLHttpRequest.prototype.open;XMLHttpRequest.prototype.open=function(m,url){if(/note/i.test(url)&&!/notif|notice/i.test(url)){this.addEventListener('load',function(){try{mine(JSON.parse(this.responseText),null,0);}catch(e){}});}return _xo.apply(this,arguments);};
['/note-manager/api/notes/?limit=5000','/api/v1/text_notes/?limit=5000','/textnotes/list/','/api/v2/notes/?limit=5000','/note-manager/api/notes/?page_size=5000'].forEach(function(ep){_f(ep,{credentials:'include'}).then(function(r){if(r.ok)r.json().then(function(j){mine(j,null,0);}).catch(function(){});}).catch(function(){});});
try{for(var k in localStorage){try{if(/note/i.test(k)&&k.length<60){mine(JSON.parse(localStorage[k]),null,0);}}catch(e){}}}catch(e){}
var box=document.createElement('div');box.id='__jhnBox';
box.style.cssText='position:fixed;z-index:2147483647;bottom:16px;right:16px;background:#0C0B09;color:#e8e2d4;border:2px solid #F0B429;border-radius:10px;padding:14px 16px;font:12px/1.5 IBM Plex Mono,monospace;width:310px;box-shadow:0 8px 32px rgba(0,0,0,.85);';
box.innerHTML='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px"><b style="color:#F0B429;font-size:13px">JustHodl \u00b7 TV Notes</b><button onclick="document.getElementById(\'__jhnBox\').style.display=\'none\'" style="background:none;border:none;color:#8a836f;cursor:pointer;font-size:18px;line-height:1">\u00d7</button></div><div id="__jhnC" style="color:#F0B429;font-weight:bold;font-size:14px;margin-bottom:4px">0 notes \u00b7 0 tickers</div><div style="color:#8a836f;font-size:10px;margin-bottom:10px">Probing TV API automatically\u2026 Also open Notes widget \u2192 All notes \u2192 click each symbol.</div><button id="__jhnUp" style="background:#F0B429;color:#0C0B09;border:none;border-radius:5px;padding:8px;font-weight:bold;cursor:pointer;width:100%;font-size:12px;margin-bottom:6px">UPLOAD 0 NOTES TO BRAIN</button><div id="__jhnM" style="font-size:11px;min-height:14px;color:#6fce8a"></div>';
document.body.appendChild(box);
function repaint(){var c=document.getElementById('__jhnC'),u=document.getElementById('__jhnUp');if(c)c.textContent=S.size+' notes \u00b7 '+T.size+' tickers';if(u)u.textContent='UPLOAD '+S.size+' NOTES TO BRAIN';}
async function upload(){var all=Array.from(S.values()),m=document.getElementById('__jhnM');if(!all.length){if(m)m.textContent='No notes yet \u2014 open All Notes and click each symbol';return;}if(m){m.style.color='#F0B429';m.textContent='Uploading '+all.length+' notes\u2026';}var ok=0,err=0;for(var i=0;i<all.length;i+=100){try{var r=await _f(IU,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({token:TOK,notes:all.slice(i,i+100)})});var d=await r.json();ok+=d.brain_upserted||0;err+=d.brain_errors||0;}catch(e){if(m){m.style.color='#E07A6A';m.textContent='Error: '+e;}return;}}if(m){m.style.color=err&&!ok?'#E07A6A':'#6fce8a';m.textContent=ok>0?'\u2705 DONE: '+ok+' notes in Brain ('+T.size+' tickers). Brain-compiler will route them to your engines.':(err?'No notes upserted \u2014 check Lambda logs':'0 notes (nothing captured yet)');}}
document.getElementById('__jhnUp').addEventListener('click',upload);
window.__JHN={show:function(){var b=document.getElementById('__jhnBox');if(b)b.style.display='block';}};
console.log('%cJH TV Notes armed \u2014 '+S.size+' notes so far. Open All Notes panel.','color:#F0B429;font-weight:bold;font-size:14px');
})();"""
    # Wrap as javascript: URL
    return "javascript:" + urllib.parse.quote(js, safe="!*()'")


def build_tv_notes_html(bookmarklet_url, ingest_url):
    """Build an updated tv-notes.html that shows the self-contained bookmarklet."""
    return """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradingView Notes \u2192 Brain \u00b7 JustHodl</title>
<style>
:root{--bg:#0C0B09;--panel:#12110C;--ink:#e8e2d4;--dim:#8a836f;--amber:#F0B429;--line:#2B2820;--green:#6fce8a;--red:#E07A6A}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.6 "IBM Plex Mono",monospace;padding:24px}
.wrap{max-width:820px;margin:0 auto}
h1{color:var(--amber);font-size:20px;margin:0 0 4px}
.sub{color:var(--dim);margin-bottom:24px;font-size:13px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px 20px;margin-bottom:16px}
.step{display:flex;gap:14px;margin-bottom:16px;align-items:flex-start}
.num{flex:0 0 28px;height:28px;border-radius:50%;background:var(--amber);color:var(--bg);display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:13px;margin-top:2px}
.step div{flex:1}
.bm-box{background:#080705;border:2px solid var(--amber);border-radius:6px;padding:14px;margin:12px 0;word-break:break-all;font-size:11px;color:var(--green);max-height:120px;overflow-y:auto;cursor:text;user-select:all}
button{background:var(--amber);color:var(--bg);border:0;border-radius:5px;padding:8px 16px;font-weight:bold;cursor:pointer;font-family:inherit;font-size:12px}
button.ghost{background:none;color:var(--amber);border:1px solid var(--amber)}
.kv{color:var(--dim)}.kv b{color:var(--ink)}
.stat{font-size:28px;color:var(--amber)}
.warn{color:var(--dim);font-size:12px;border-left:2px solid var(--line);padding-left:10px;margin-top:10px}
.big{font-size:15px;font-weight:bold;color:var(--amber);margin-bottom:8px}
a{color:var(--amber);text-decoration:none}a:hover{text-decoration:underline}
.copied{background:var(--green)!important;color:var(--bg)!important}
</style></head>
<body><div class="wrap">
<h1>TradingView Notes \u2192 Brain</h1>
<div class="sub">Harvest every note you&rsquo;ve written under any ticker and load them into your Brain \u2014 from your own logged-in browser, zero credentials shared.</div>

<div class="card">
  <div class="kv">Notes in mirror: <span class="stat" id="cnt">\u2026</span></div>
  <div class="kv" id="upd" style="font-size:12px;margin-top:4px"></div>
</div>

<div class="card">
  <div class="big">\u26a1 One-click setup</div>
  <div class="step">
    <div class="num">1</div>
    <div>
      Open <b>Chrome Bookmark Manager</b> with <kbd>Ctrl+Shift+O</kbd> (or \u2630 menu \u2192 Bookmarks \u2192 Bookmark manager).<br>
      Right-click <b>Bookmarks bar</b> in the left sidebar \u2192 <b>Add new bookmark</b>.
    </div>
  </div>
  <div class="step">
    <div class="num">2</div>
    <div>
      <b>Name:</b> <code>JH TV Notes</code><br>
      <b>URL:</b> click the box below to select all, then Ctrl+C to copy, paste as the bookmark URL.<br>
      <div class="bm-box" id="bmUrl" onclick="selectAll(this)" title="Click to select all"></div>
      <div style="margin-top:8px"><button onclick="copyBm()">Copy Bookmarklet URL</button></div>
      <div class="warn">\u26a0\ufe0f The URL starts with <code>javascript:</code> &mdash; that&rsquo;s correct. It&rsquo;s a bookmarklet, not a web address.</div>
    </div>
  </div>
  <div class="step">
    <div class="num">3</div>
    <div>
      Go to <a href="https://www.tradingview.com/chart/" target="_blank">tradingview.com</a> (logged in) and <b>click the "JH TV Notes" bookmark</b> in your bookmarks bar.<br>
      An amber panel appears bottom-right. It automatically probes your notes API. When done, open the <b>Notes widget \u2192 All Notes</b> and click through your symbols &mdash; every note that loads is captured. Press <b>UPLOAD TO BRAIN</b>.
    </div>
  </div>
</div>

<div class="card">
  <div class="kv" style="margin-bottom:8px;font-size:12px;color:var(--dim)">Ingest endpoint (for diagnostics): <b id="iurl">\u2026</b></div>
</div>

<div class="warn" style="margin-top:0">
Everything runs inside your own browser on tradingview.com. The bookmarklet is self-contained &mdash; no external scripts, no credentials exposed, no data sent until you press UPLOAD. Notes are tagged <code>[TV:SYMBOL]</code> and deduplicated. Brain-compiler routes them to every matching engine on its next run.
</div>
</div>

<script>
var BM = """ + json.dumps(bookmarklet_url) + """;
var IU = """ + json.dumps(ingest_url) + """;
document.getElementById('bmUrl').textContent = BM;
document.getElementById('iurl').textContent = IU;
function selectAll(el){var r=document.createRange();r.selectNodeContents(el);var s=window.getSelection();s.removeAllRanges();s.addRange(r);}
function copyBm(){navigator.clipboard.writeText(BM).then(function(){var b=document.querySelector('button');b.textContent='Copied!';b.classList.add('copied');setTimeout(function(){b.textContent='Copy Bookmarklet URL';b.classList.remove('copied');},1500);});}
var S3='https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com';
fetch(S3+'/data/tradingview-notes.json?t='+Date.now()).then(function(r){return r.ok?r.json():null;}).then(function(d){
  document.getElementById('cnt').textContent=d&&d.count!=null?d.count:0;
  if(d&&d.updated)document.getElementById('upd').innerHTML='Last upload: <b>'+new Date(d.updated).toLocaleString()+'</b>';
}).catch(function(){document.getElementById('cnt').textContent='0';});
</script>
</body></html>"""


def main():
    with report("2962_bookmarklet") as rep:
        fails = []

        # ── get Lambda URL + token ────────────────────────────────────
        rep.section("1. Get ingest credentials")
        token = ssm_get("/justhodl/tvnotes/ingest-token")
        ingest_url = None
        try:
            ingest_url = LAM.get_function_url_config(
                FunctionName=INGEST_FN)["FunctionUrl"].rstrip("/")
        except Exception as e:
            fails.append("Lambda URL: %s" % e)
        rep.kv(token_ok=bool(token), ingest_url=ingest_url)
        if not token or not ingest_url:
            for f in fails:
                rep.fail(f)
            sys.exit(1)

        # ── build bookmarklet ─────────────────────────────────────────
        rep.section("2. Build self-contained bookmarklet")
        bm = build_bookmarklet(ingest_url, token)
        rep.kv(bookmarklet_len=len(bm),
               starts_with_javascript=bm.startswith("javascript:"))
        # sanity: Chrome limit ~2MB, typical bookmarklet <50KB
        if len(bm) > 500_000:
            fails.append("bookmarklet too large: %d bytes" % len(bm))

        # ── write bookmarklet + HTML ──────────────────────────────────
        rep.section("3. Publish")
        S3.put_object(Bucket=BUCKET, Key="data/tv-bookmarklet.json",
                      Body=json.dumps({"bookmarklet": bm, "ingest_url": ingest_url,
                                       "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                       "name": "JH TV Notes",
                                       "instructions": "Save as a Chrome bookmark; click on tradingview.com"
                                       }).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        rep.ok("tv-bookmarklet.json published to S3")

        html = build_tv_notes_html(bm, ingest_url)
        tv_notes_path = ROOT / "tv-notes.html"
        tv_notes_path.write_text(html, encoding="utf-8")
        rep.ok("tv-notes.html rebuilt with self-contained bookmarklet")
        rep.kv(html_bytes=len(html))

        line = ("bookmarklet: len=%d ingest=%s token=%s"
                % (len(bm), bool(ingest_url), bool(token)))
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            sys.exit(1)
        rep.ok("self-contained bookmarklet built and published")


if __name__ == "__main__":
    main()
