"""ops 3162 — the config was pointing at a DEAD function URL.

Khalid's panel: "❌ watchlists: TypeError: Failed to fetch" — a network
block, not a data problem. Root cause, verbatim from the artifacts:
  • manifest permitted nzoe4a43…lambda-url…on.aws
  • the LIVE ingest function URL is w4osrorys…lambda-url…on.aws
  → Chrome blocked every POST to an unpermitted host, and v1.2's manifest
    had also dropped the S3 host, so the extension could not even fetch
    its config to self-correct.

Extension 1.4.0 (this push): wildcard *.lambda-url.us-east-1.on.aws +
S3 + justhodl.ai host permissions; live URL baked in; storage-URL
validation; automatic retry on the baked URL if a stale one is cached.

THIS OP makes the published config authoritative:
  1. read the LIVE function URL + SSM token
  2. rewrite data/tv-ingest-config.json with them
  3. verify the public config is fetchable AND its url == the live url
  4. POST a watchlists-only probe to the URL FROM THE CONFIG (proving the
     exact path the extension takes), then clean up
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tv-notes-ingest"
CFG_KEY = "data/tv-ingest-config.json"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3162_ingest_config") as rep:
    fails, warns = [], []
    rep.heading("ops 3162 — authoritative ingest config")

    rep.section("1. Live URL vs published config")
    live = LAM.get_function_url_config(FunctionName=FN)["FunctionUrl"]
    try:
        old = s3_json(CFG_KEY)
    except Exception:
        old = {}
    rep.kv(live_url=live, config_url=old.get("ingest_url"),
           match=(str(old.get("ingest_url", "")).rstrip("/")
                  == live.rstrip("/")))
    if str(old.get("ingest_url", "")).rstrip("/") != live.rstrip("/"):
        rep.ok("CONFIRMED: published config pointed at a dead URL — this "
               "is exactly the 'Failed to fetch' Khalid saw")

    rep.section("2. Republish config")
    token = SSM.get_parameter(Name="/justhodl/tvnotes/ingest-token",
                              WithDecryption=True)["Parameter"]["Value"]
    cfg = {"ingest_url": live, "token": token,
           "updated_at": datetime.now(timezone.utc).isoformat(),
           "note": "written by ops 3162"}
    S3.put_object(Bucket=BUCKET, Key=CFG_KEY,
                  Body=json.dumps(cfg).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=60")
    rep.ok("config republished with the live URL + token")

    rep.section("3. Public fetch (the path the extension takes)")
    pub = None
    for url in (f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"
                f"{CFG_KEY}",
                f"https://justhodl.ai/{CFG_KEY}"):
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                url + f"?t={int(time.time())}",
                headers={"User-Agent": "Mozilla/5.0 ops"}), timeout=15)
            d = json.loads(r.read().decode())
            ok = d.get("ingest_url", "").rstrip("/") == live.rstrip("/")
            rep.ok(f"{url.split('/')[2][:28]}… HTTP {r.status} · "
                   f"url matches live: {ok}")
            if ok and pub is None:
                pub = d
        except Exception as e:
            warns.append(f"{url.split('/')[2][:28]}…: {str(e)[:70]}")
    if pub is None:
        fails.append("no public config source serves the live URL")

    rep.section("4. Watchlists-only probe through the config URL")
    if pub:
        stamp = int(time.time())
        body = {"token": pub["token"], "notes": [], "watchlists": [
            {"id": f"e2e-cfg-{stamp}", "name": "E2E Config Probe",
             "symbols": ["NASDAQ:NVDA", "NYSE:JNJ"]}]}
        req = urllib.request.Request(
            pub["ingest_url"], data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                res = json.loads(r.read().decode())
            rep.kv(probe_status=r.status,
                   watchlists_saved=res.get("watchlists_saved"))
            if res.get("watchlists_saved") == 1:
                rep.ok("END-TO-END: config URL accepts watchlists — the "
                       "extension's exact path is now clear")
            else:
                fails.append(f"probe rejected: {json.dumps(res)[:140]}")
        except Exception as e:
            fails.append(f"probe failed: {str(e)[:120]}")
        # strip probe
        try:
            w = s3_json("data/tv-watchlists.json")
            w["lists"] = [l for l in (w.get("lists") or [])
                          if not str(l.get("id", "")).startswith("e2e-")]
            w["n_lists"] = len(w["lists"])
            S3.put_object(Bucket=BUCKET, Key="data/tv-watchlists.json",
                          Body=json.dumps(w).encode(),
                          ContentType="application/json")
            rep.log("probe watchlist stripped")
        except Exception:
            pass

    for w_ in warns:
        rep.warn(w_)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
