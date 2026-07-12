"""ops 3161 — ingest hardened for Khalid's REAL harvest scale.

Live evidence from the browser: the v1.2 tap works — 1,983 notes captured
from tv/textnotes/getall/ (the real endpoint; note-manager was my wrong
host) and watchlists from symbols_list/custom/. The UPLOAD then failed.

Root causes fixed here:
  1. ingest lambda was 256MB/60s with SERIAL brain writes — fine for the
     1-note E2E, hopeless at 1,983. Now 1024MB/300s + ThreadPoolExecutor
     (8 workers, 100-note brain chunks).
  2. brain failure was FATAL to the whole response. Now the S3 mirror
     alone counts as success, and the first brain error returns verbatim
     (brain_error_sample) so the panel shows WHY instead of a blank
     "upload failed".
  3. extension (1.3.0): watchlists POST FIRST in their own request — they
     used to ride note-chunk 0, so a note failure silently killed them
     too. Notes then stream in 40-note chunks with live progress.
  4. note symbols normalized NASDAQ:AAPL -> AAPL so the brain-compiler
     routes them; replay now hits /textnotes/getall/.

GATE: 300 synthetic notes + 2 watchlists through the REAL Function URL in
one shot (the scale that broke it), then cleanup.
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tv-notes-ingest"
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def post(url, body, timeout=120):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read().decode())


with report("3161_ingest_scale") as rep:
    fails, warns = [], []
    rep.heading("ops 3161 — ingest at harvest scale")

    rep.section("1. Deploy hardened ingest (1024MB / 300s / parallel)")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    live = LAM.get_function_configuration(FunctionName=FN)
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=(live.get("Environment") or {})
                  .get("Variables") or {},
                  timeout=cfg.get("timeout", 300),
                  memory=cfg.get("memory", 1024),
                  description=(cfg.get("description") or "")[:250],
                  smoke=False)
    conf = LAM.get_function_configuration(FunctionName=FN)
    rep.kv(memory=conf["MemorySize"], timeout=conf["Timeout"])
    if conf["Timeout"] < 120:
        fails.append("timeout still low — config not applied")

    furl = LAM.get_function_url_config(FunctionName=FN)["FunctionUrl"]
    token = SSM.get_parameter(Name="/justhodl/tvnotes/ingest-token",
                              WithDecryption=True)["Parameter"]["Value"]

    rep.section("2. Watchlists-first request (the new order)")
    stamp = int(time.time())
    t0 = time.time()
    st, r1 = post(furl, {"token": token, "notes": [], "watchlists": [
        {"id": f"e2e-a-{stamp}", "name": "E2E Scale A",
         "symbols": ["NASDAQ:NVDA", "NASDAQ:AMD", "NYSE:ANET"]},
        {"id": f"e2e-b-{stamp}", "name": "E2E Scale B",
         "symbols": ["NYSE:JNJ", "NYSE:PG"]}]})
    rep.kv(wl_only_status=st, wl_saved=r1.get("watchlists_saved"),
           wl_secs=round(time.time() - t0, 1))
    if r1.get("watchlists_saved") != 2:
        fails.append(f"watchlists-first failed: {json.dumps(r1)[:160]}")
    else:
        rep.ok("watchlists land independently of notes (the fix that "
               "saves them when a note chunk dies)")

    rep.section("3. 300-note burst — the scale that broke it")
    notes = [{"symbol": ["NVDA", "AAPL", "MU", "STX"][i % 4],
              "text": f"ops3161 scale probe {stamp} #{i} — thesis line "
                      f"with enough body to be realistic for a note.",
              "title": "scale probe", "created": (stamp - i) * 1000}
             for i in range(300)]
    t1 = time.time()
    st, r2 = post(furl, {"token": token, "notes": notes})
    dt = round(time.time() - t1, 1)
    rep.kv(burst_status=st, burst_secs=dt,
           brain_upserted=r2.get("brain_upserted"),
           brain_failed=r2.get("brain_failed"),
           mirror_added=r2.get("mirror_added"),
           brain_error=str(r2.get("brain_error_sample"))[:80])
    if st != 200:
        fails.append(f"300-note burst HTTP {st}")
    elif not r2.get("ok"):
        fails.append(f"burst not ok: {json.dumps(r2)[:200]}")
    else:
        rep.ok(f"300 notes accepted in {dt}s "
               f"(brain {r2.get('brain_upserted')}, "
               f"mirror {r2.get('mirror_added')})")
        if r2.get("brain_failed"):
            warns.append(f"brain rejected {r2.get('brain_failed')} — "
                         f"sample: {str(r2.get('brain_error_sample'))[:120]} "
                         "(mirror still captured them; brain-compiler "
                         "reads the mirror on its next run)")
    est = round(dt / 300 * 1983, 1) if dt else 0
    rep.kv(projected_1983_notes_secs=est)
    if est > 240:
        warns.append(f"projected {est}s for the real 1,983 — extension "
                     "chunks at 40 so each request stays small; fine")

    rep.section("4. Cleanup e2e artifacts")
    # best-effort: cleanup must never fail a passing pipeline (50 serial
    # brain deletes were blowing the client read timeout)
    try:
        m = s3_json("data/tradingview-notes.json")
        ids = [n["id"] for n in (m.get("notes") or [])
               if f"ops3161 scale probe {stamp}" in str(n.get("text"))]
        done = 0
        for i in range(0, min(len(ids), 300), 10):
            try:
                post(furl, {"token": token,
                            "delete_ids": ids[i:i + 10]}, timeout=110)
                done += len(ids[i:i + 10])
            except Exception:
                break
        rep.log(f"probe notes deleted: {done}/{len(ids)}")
        if done < len(ids):
            warns.append(f"{len(ids) - done} probe notes left in mirror "
                         "(harmless; brain dedupes by id)")
    except Exception as e:
        warns.append(f"note cleanup skipped: {str(e)[:80]}")
    w = s3_json("data/tv-watchlists.json")
    w["lists"] = [l for l in (w.get("lists") or [])
                  if not str(l.get("id", "")).startswith("e2e-")]
    w["n_lists"] = len(w["lists"])
    w["generated_at"] = datetime.now(timezone.utc).isoformat()
    S3.put_object(Bucket=BUCKET, Key="data/tv-watchlists.json",
                  Body=json.dumps(w).encode(),
                  ContentType="application/json")
    rep.ok("e2e watchlists stripped — clean slate for the real sync")

    for w_ in warns:
        rep.warn(w_)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
