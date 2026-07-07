#!/usr/bin/env python3
"""ops 2963 — Build and publish the JustHodl TradingView Notes Chrome Extension.

  1. Reads ingest Lambda URL + token from SSM.
  2. Bakes them into background.js (replaces placeholder strings).
  3. Packages the chrome-extension/ directory as a .zip.
  4. Copies zip + icons to tools/ (served by GitHub Pages at justhodl.ai/tools/).
  5. Regenerates tv-notes.html with install instructions pointing to the zip.

User installs by:
  1. Going to justhodl.ai/tv-notes.html → clicking "Download Extension"
  2. Extracting zip to a local folder
  3. chrome://extensions → Developer Mode ON → Load Unpacked → select folder
  4. Done — extension auto-harvests on every tradingview.com tab
"""
import json
import shutil
import sys
import time
import zipfile
from pathlib import Path

import boto3
from ops_report import report

LAM    = boto3.client("lambda", region_name="us-east-1")
SSM    = boto3.client("ssm",   region_name="us-east-1")
S3     = boto3.client("s3",    region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT   = Path(__file__).resolve().parents[3]          # repo root
EXT    = ROOT / "chrome-extension"
TOOLS  = ROOT / "tools"
INGEST_FN = "justhodl-tv-notes-ingest"


def ssm_get(name):
    try:
        return SSM.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def main():
    with report("2963_extension_build") as rep:
        fails = []

        # ── 1. Read credentials ───────────────────────────────────────
        rep.section("1. Credentials")
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

        # ── 2. Bake credentials into background.js ────────────────────
        rep.section("2. Bake credentials")
        bg_path = EXT / "background.js"
        bg_src  = bg_path.read_text(encoding="utf-8")
        bg_baked = (bg_src
                    .replace("INGEST_URL_PLACEHOLDER", ingest_url)
                    .replace("INGEST_TOKEN_PLACEHOLDER", token))
        assert "INGEST_URL_PLACEHOLDER"   not in bg_baked, "URL bake failed"
        assert "INGEST_TOKEN_PLACEHOLDER" not in bg_baked, "token bake failed"
        rep.ok("credentials baked into background.js")

        # ── 3. Package ZIP (don't commit the baked background.js) ─────
        rep.section("3. Package ZIP")
        TOOLS.mkdir(parents=True, exist_ok=True)
        zip_path = TOOLS / "jh-tv-extension.zip"
        files_packed = []
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in EXT.rglob("*"):
                if f.is_dir():
                    continue
                rel = f.relative_to(EXT)
                if f == bg_path:
                    zf.writestr(str(rel), bg_baked)
                else:
                    zf.write(f, str(rel))
                files_packed.append(str(rel))
        rep.kv(zip_size_kb=round(zip_path.stat().st_size / 1024, 1),
               files=len(files_packed))
        for fn in sorted(files_packed):
            rep.log("  " + fn)
        rep.ok("jh-tv-extension.zip built")

        # ── 4. Also publish zip to S3 for direct download ─────────────
        rep.section("4. Publish to S3")
        S3.put_object(
            Bucket=BUCKET, Key="tools/jh-tv-extension.zip",
            Body=zip_path.read_bytes(),
            ContentType="application/zip",
            ContentDisposition='attachment; filename="jh-tv-extension.zip"',
            CacheControl="max-age=300")
        rep.ok("zip published to S3 data/tools/jh-tv-extension.zip")

        # ── 5. Rebuild tv-notes.html ──────────────────────────────────
        rep.section("5. Rebuild tv-notes.html")
        html = build_install_page(ingest_url)
        (ROOT / "tv-notes.html").write_text(html, encoding="utf-8")
        rep.kv(html_bytes=len(html))
        rep.ok("tv-notes.html rebuilt with extension install guide")

        line = ("ext: zip=%dKB files=%d ingest_baked=%s"
                % (round(zip_path.stat().st_size / 1024, 1), len(files_packed), bool(ingest_url)))
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            sys.exit(1)
        rep.ok("extension built and published — ready to install")


def build_install_page(ingest_url):
    return '''<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradingView Notes Chrome Extension · JustHodl</title>
<style>
:root{--bg:#0C0B09;--panel:#12110C;--ink:#e8e2d4;--dim:#8a836f;--amber:#F0B429;--line:#2B2820;--green:#6fce8a;--red:#E07A6A}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.6 "IBM Plex Mono",monospace;padding:24px}
.wrap{max-width:820px;margin:0 auto}
h1{color:var(--amber);font-size:22px;margin:0 0 4px}
.sub{color:var(--dim);margin-bottom:24px;font-size:13px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px 20px;margin-bottom:16px}
.step{display:flex;gap:14px;margin-bottom:16px;align-items:flex-start}
.num{flex:0 0 30px;height:30px;border-radius:50%;background:var(--amber);color:var(--bg);display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:14px;margin-top:2px}
button{background:var(--amber);color:var(--bg);border:0;border-radius:5px;padding:10px 20px;font-weight:bold;cursor:pointer;font-family:inherit;font-size:14px}
button:hover{opacity:.9}
code{background:#080705;border:1px solid var(--line);border-radius:4px;padding:1px 6px;font-size:12px}
kbd{background:#1a1a14;border:1px solid var(--line);border-radius:4px;padding:2px 7px;font-size:12px}
.warn{color:var(--dim);font-size:12px;border-left:2px solid var(--line);padding-left:10px;margin:10px 0}
.big{font-size:15px;font-weight:bold;color:var(--amber);margin-bottom:12px}
.stat{font-size:28px;color:var(--amber);font-weight:bold}
a{color:var(--amber)}
.badge{display:inline-block;background:rgba(240,180,41,.15);border:1px solid var(--amber);border-radius:4px;padding:2px 8px;font-size:11px;color:var(--amber);margin-left:8px}
</style></head>
<body><div class="wrap">
<h1>TV Notes Chrome Extension <span class="badge">v1.0.0</span></h1>
<div class="sub">Fully autonomous — installs in Chrome, auto-harvests every note across all your watchlists, uploads to Brain with one click. No DevTools, no pasting, no F12.</div>

<div class="card" style="display:flex;align-items:center;justify-content:space-between">
  <div>
    <div style="color:var(--dim);font-size:12px">Notes in Brain mirror</div>
    <div class="stat" id="cnt">…</div>
    <div style="color:var(--dim);font-size:12px" id="upd"></div>
  </div>
  <button onclick="downloadExt()" style="font-size:16px;padding:12px 24px">⬇ Download Extension</button>
</div>

<div class="card">
  <div class="big">📦 Install once — works forever</div>

  <div class="step">
    <div class="num">1</div>
    <div>
      <b>Download the extension</b> — click the button above or <a id="dlLink" href="#">download directly</a>. Save the .zip file anywhere convenient (e.g. <code>Downloads</code>).<br>
      <div class="warn">⚠ Don't extract inside a cloud-synced folder (OneDrive/Google Drive) — Chrome may have trouble loading extensions from there.</div>
    </div>
  </div>

  <div class="step">
    <div class="num">2</div>
    <div>
      <b>Extract the zip.</b> Right-click the downloaded <code>jh-tv-extension.zip</code> → <b>Extract All</b> → choose a permanent location like <code>C:\\Users\\Adam\\jh-tv-extension\\</code> and click Extract.
    </div>
  </div>

  <div class="step">
    <div class="num">3</div>
    <div>
      <b>Load in Chrome.</b> In Chrome, go to <code>chrome://extensions</code>, enable <b>Developer mode</b> (toggle, top-right), then click <b>Load unpacked</b> and select the extracted folder.<br>
      You'll see the amber <b>JH</b> icon appear in your Chrome toolbar.
    </div>
  </div>

  <div class="step">
    <div class="num">4</div>
    <div>
      <b>Open TradingView.</b> Go to <a href="https://www.tradingview.com/chart/" target="_blank">tradingview.com</a> (logged in). The extension auto-starts immediately — an amber panel appears bottom-right showing harvest progress. When it stops counting up, press <b>UPLOAD TO BRAIN</b>.<br>
      <div class="warn">The extension also opens the Notes panel automatically and scrolls through your watchlists. Just let it run — it takes 1–5 minutes depending on how many notes you have.</div>
    </div>
  </div>
</div>

<div class="card">
  <div class="big" style="margin-bottom:8px">🔄 Ongoing auto-sync</div>
  <div style="color:var(--dim);font-size:13px">After install, every time you open TradingView the extension runs automatically. New notes you write get picked up within minutes. The Brain-compiler routes them to your fleet engines on each run.</div>
</div>

<div class="warn">The extension only reads your notes and sends them to your own JustHodl ingest endpoint. No data goes anywhere else. Token and URL are compiled into the extension at build time.</div>
</div>

<script>
var DL = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/tools/jh-tv-extension.zip";
document.getElementById("dlLink").href = DL;
function downloadExt(){ window.location.href = DL; }
var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
fetch(S3+"/data/tradingview-notes.json?t="+Date.now()).then(r=>r.ok?r.json():null).then(d=>{
  document.getElementById("cnt").textContent=d&&d.count!=null?d.count:0;
  if(d&&d.updated)document.getElementById("upd").textContent="Last sync: "+new Date(d.updated).toLocaleString();
}).catch(()=>{ document.getElementById("cnt").textContent="0"; });
</script>
</body></html>'''


if __name__ == "__main__":
    main()
