"""ops 3158 — TradingView pipeline v3: watchlists join the harvest.

Post-mortem of the "miserable" round: Lambda crawler = Cloudflare-
blocked (architectural, permanent); bookmarklet = page CSP; the console
script + extension shipped but (a) were never install-tested and
(b) STRUCTURALLY never uploaded watchlist membership — only notes.
Khalid's core ask (watchlists as predictors) was unserved by design.

V3 (this push):
  • chrome-extension 1.1.0: content.js captures every watchlist's full
    membership; background posts {token, notes, watchlists} (first
    chunk); popup reports lists synced. Extension context = no page
    CSP, TV cookies native.
  • tools/tv-export.js console fallback: same payload parity.
  • ingest lambda: _save_watchlists → data/tv-watchlists.json
    (merge-by-list-id, latest sync wins), lists-only syncs succeed,
    watchlists_saved in every response.

THIS OP: deploy ingest · E2E POST with real token (1 synthetic note +
2 synthetic watchlists) → assert mirror + watchlists doc → delete_ids
cleanup → strip e2e lists from the doc · package extension zip →
tools/ + S3 · verify zip + config on CDN.
"""

import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tv-notes-ingest"
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
AWS_DIR = HERE.parents[1]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3158_tv_pipeline_v3") as rep:
    fails, warns = [], []
    rep.heading("ops 3158 — TV pipeline v3 (notes + WATCHLISTS)")

    rep.section("1. Deploy ingest lambda")
    live = LAM.get_function_configuration(FunctionName=FN)
    cp = AWS_DIR / "lambdas" / FN / "config.json"
    cfg = json.loads(cp.read_text()) if cp.exists() else {
        "timeout": live.get("Timeout", 60),
        "memory": live.get("MemorySize", 256),
        "description": live.get("Description", "")}
    sched = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=(live.get("Environment") or {})
                  .get("Variables") or {},
                  eb_rule_name=(sched.get("name") or sched.get("rule_name")),
                  eb_schedule=(sched.get("expression") or sched.get("cron")),
                  timeout=cfg.get("timeout", 60),
                  memory=cfg.get("memory", 256),
                  description=(cfg.get("description") or "")[:250],
                  smoke=False)
    furl = LAM.get_function_url_config(FunctionName=FN)["FunctionUrl"]
    rep.kv(function_url=furl)

    rep.section("2. E2E: notes + watchlists through the real pipe")
    token = SSM.get_parameter(Name="/justhodl/tvnotes/ingest-token",
                              WithDecryption=True)["Parameter"]["Value"]
    stamp = int(time.time())
    payload = {
        "token": token,
        "notes": [{"symbol": "OPSTEST", "text": f"e2e-3158-{stamp}",
                   "title": "ops e2e", "created": stamp * 1000}],
        "watchlists": [
            {"id": f"e2e-a-{stamp}", "name": "E2E Momentum Test",
             "symbols": ["NASDAQ:NVDA", "NASDAQ:AMD", "NYSE:ANET"]},
            {"id": f"e2e-b-{stamp}", "name": "E2E Defensive Test",
             "symbols": ["NYSE:JNJ", "NYSE:PG"]},
        ],
    }
    req = urllib.request.Request(
        furl, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        resp = json.loads(r.read().decode())
    rep.kv(e2e_status=resp.get("ok"),
           brain_upserted=resp.get("brain_upserted"),
           mirror_added=resp.get("mirror_added"),
           watchlists_saved=resp.get("watchlists_saved"))
    if resp.get("watchlists_saved") != 2:
        fails.append(f"watchlists_saved={resp.get('watchlists_saved')} "
                     "(expected 2)")
    wdoc = s3_json("data/tv-watchlists.json")
    ids = {l["id"] for l in wdoc.get("lists") or []}
    if f"e2e-a-{stamp}" in ids and f"e2e-b-{stamp}" in ids:
        rep.ok(f"tv-watchlists.json live: {wdoc.get('n_lists')} lists, "
               "e2e lists present with full membership")
    else:
        fails.append("e2e lists missing from tv-watchlists.json")
    # cleanup: delete the note; strip e2e lists from the doc
    note_id = None
    m = s3_json("data/tradingview-notes.json")
    for n in m.get("notes") or []:
        if f"e2e-3158-{stamp}" in str(n.get("text")):
            note_id = n.get("id")
    if note_id:
        req = urllib.request.Request(
            furl, data=json.dumps({"token": token,
                                   "delete_ids": [note_id]}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=20).read()
        rep.log("e2e note deleted via delete_ids")
    wdoc["lists"] = [l for l in wdoc["lists"]
                     if not l["id"].startswith("e2e-")]
    wdoc["n_lists"] = len(wdoc["lists"])
    wdoc["generated_at"] = datetime.now(timezone.utc).isoformat()
    S3.put_object(Bucket=BUCKET, Key="data/tv-watchlists.json",
                  Body=json.dumps(wdoc).encode(),
                  ContentType="application/json")
    rep.log("e2e watchlists stripped — doc clean for the real sync")

    rep.section("3. Package extension 1.1.0")
    ext = ROOT / "chrome-extension"
    buf = io.BytesIO()
    n_files = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(ext.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(ext))
                n_files += 1
    data = buf.getvalue()
    (ROOT / "tools" / "jh-tv-extension.zip").write_bytes(data)
    S3.put_object(Bucket=BUCKET, Key="tools/jh-tv-extension.zip",
                  Body=data, ContentType="application/zip")
    rep.kv(zip_files=n_files, zip_kb=round(len(data) / 1024, 1))
    rep.ok("zip → repo tools/ + S3 tools/jh-tv-extension.zip")

    rep.section("4. Download path verification")
    for url in ("https://justhodl.ai/data/tv-ingest-config.json",
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com"
                "/tools/jh-tv-extension.zip"):
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 ops"}), timeout=15)
            rep.ok(f"{url.split('/')[-1]}: HTTP {r.status} "
                   f"({r.headers.get('Content-Length', '?')} bytes)")
        except Exception as e:
            warns.append(f"{url.split('/')[-1]}: {str(e)[:80]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
