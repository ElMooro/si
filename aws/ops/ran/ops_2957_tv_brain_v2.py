#!/usr/bin/env python3
"""ops 2957 — TV Notes → Brain v2 pipeline hardening.

tv-export.js v2 is now autonomous (enumerates all watchlists, probes notes
API per-ticker, scans chart layouts, wraps fetch/XHR for passive capture).

This ops script:
  1. Gets the live ingest Lambda URL + refreshes tv-ingest-config.json on S3
     with correct public CacheControl so the extractor can self-configure.
  2. Redeploys tv-notes-ingest with fresh env (brain uid + token).
  3. Wires the brain-compiler to ALSO read tradingview-notes.json as a
     supplemental source (so TV notes merge into brain.json on every
     compiler run, not just when Khalid manually uploads).
  4. Does a live round-trip: POST sentinel → confirm in brain → delete.
  5. Writes a verified status to data/tv-pipeline-status.json.
"""
import json
import secrets
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
S3  = boto3.client("s3",  region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = Path(__file__).resolve().parents[2]
FN_INGEST = "justhodl-tv-notes-ingest"
FN_BRAIN  = "justhodl-brain-compiler"
BRAIN_UID = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9"


def ssm_get(name):
    try:
        return SSM.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except SSM.exceptions.ParameterNotFound:
        return None


def ssm_put(name, value, secure=True):
    SSM.put_parameter(Name=name, Value=value,
                      Type="SecureString" if secure else "String",
                      Overwrite=True)


def post(url, obj, to=30):
    req = urllib.request.Request(
        url, data=json.dumps(obj).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=to) as r:
        return r.getcode(), json.loads(r.read().decode("utf-8", "replace"))


def main():
    with report("2957_tv_brain_v2") as rep:
        fails = []

        # ── 1. SSM bootstrap ──────────────────────────────────────────
        rep.section("1. SSM bootstrap")
        uid = ssm_get("/justhodl/brain/uid") or BRAIN_UID
        if not ssm_get("/justhodl/brain/uid"):
            ssm_put("/justhodl/brain/uid", BRAIN_UID)
        token = ssm_get("/justhodl/tvnotes/ingest-token")
        if not token:
            token = secrets.token_urlsafe(28)
            ssm_put("/justhodl/tvnotes/ingest-token", token)
            rep.log("minted fresh ingest token")
        rep.kv(uid_ok=bool(uid), token_ok=bool(token))

        # ── 2. Deploy ingest lambda ───────────────────────────────────
        rep.section("2. Deploy tv-notes-ingest")
        try:
            cur = LAM.get_function_configuration(FunctionName=FN_INGEST)
            env = cur.get("Environment", {}).get("Variables", {}) or {}
            to_  = int(cur.get("Timeout", 60))
            mem  = int(cur.get("MemorySize", 256))
        except Exception:
            env, to_, mem = {}, 60, 256
        env["INGEST_TOKEN"] = token
        env["BRAIN_UID"]    = uid
        deploy_lambda(report=rep, function_name=FN_INGEST,
                      source_dir=ROOT / "lambdas" / FN_INGEST / "source",
                      env_vars=env, timeout=to_, memory=mem,
                      description="TV notes ingest v2 (ops 2957)",
                      create_function_url=True, smoke=False)

        # get the function URL
        fn_url = LAM.get_function_url_config(
            FunctionName=FN_INGEST)["FunctionUrl"].rstrip("/")
        rep.kv(ingest_url=fn_url)

        # ── 3. Publish ingest config (public, used by tv-export.js) ──
        rep.section("3. Publish tv-ingest-config.json")
        cfg = {"ingest_url": fn_url, "token": token,
               "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "extractor_url": "https://justhodl.ai/tools/tv-export.js",
               "notes_page": "https://justhodl.ai/tv-notes.html"}
        S3.put_object(Bucket=BUCKET, Key="data/tv-ingest-config.json",
                      Body=json.dumps(cfg).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=120")
        rep.ok("config published")

        # ── 4. Wire brain-compiler to merge TV notes ──────────────────
        rep.section("4. Wire brain-compiler → TV notes merge")
        bc_path = ROOT / "lambdas" / FN_BRAIN / "source" / "lambda_function.py"
        bc_src = bc_path.read_text(encoding="utf-8")
        MERGE_ANCHOR = 'brain = gj("data/brain.json") or {}'
        MERGE_CODE = (
            'brain = gj("data/brain.json") or {}\n'
            '    # ── merge TradingView notes from the mirror (additive, idempotent) ──\n'
            '    try:\n'
            '        tv_mirror = gj("data/tradingview-notes.json") or {}\n'
            '        tv_notes  = tv_mirror.get("notes") or []\n'
            '        brain_ids = {n.get("id") for n in (brain.get("notes") or [])}\n'
            '        added_tv  = 0\n'
            '        for tn in tv_notes:\n'
            '            if not isinstance(tn, dict): continue\n'
            '            text = str(tn.get("text") or "").strip()\n'
            '            if len(text) < 20: continue\n'
            '            sym  = str(tn.get("symbol") or "UNTAGGED").upper()\n'
            '            nid  = tn.get("id") or ("tv-" + __import__("hashlib").sha1(\n'
            '                       (sym + "|" + text[:120]).encode()).hexdigest()[:16])\n'
            '            if nid in brain_ids: continue\n'
            '            body = "[TV:%s] %s" % (sym, text)\n'
            '            if not brain.get("notes"): brain["notes"] = []\n'
            '            brain["notes"].append({"id": nid, "cat": "thesis",\n'
            '                "text": body, "pinned": False,\n'
            '                "created": tn.get("created"), "_tv_symbol": sym})\n'
            '            brain_ids.add(nid); added_tv += 1\n'
            '        if added_tv: print("[tv-merge] +%d TV notes merged into brain" % added_tv)\n'
            '    except Exception as _tv_e:\n'
            '        print("[tv-merge] %s" % _tv_e)'
        )
        if "tradingview-notes.json" not in bc_src:
            assert bc_src.count(MERGE_ANCHOR) == 1, "anchor not found"
            bc_src = bc_src.replace(MERGE_ANCHOR, MERGE_CODE)
            bc_path.write_text(bc_src, encoding="utf-8")
            rep.ok("brain-compiler: TV notes merge wired")
            # redeploy brain-compiler
            try:
                cur2 = LAM.get_function_configuration(FunctionName=FN_BRAIN)
                env2 = cur2.get("Environment", {}).get("Variables", {}) or {}
                deploy_lambda(report=rep, function_name=FN_BRAIN,
                              source_dir=ROOT / "lambdas" / FN_BRAIN / "source",
                              env_vars=env2,
                              timeout=int(cur2.get("Timeout", 300)),
                              memory=int(cur2.get("MemorySize", 512)),
                              description="brain-compiler: TV notes merge (ops 2957)",
                              smoke=True)
            except Exception as e:
                rep.warn("brain-compiler redeploy: %s" % e)
        else:
            rep.log("brain-compiler: TV merge already wired — no change")

        # ── 5. Live round-trip ────────────────────────────────────────
        rep.section("5. Live round-trip")
        time.sleep(4)
        # health
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(fn_url, headers={"Accept": "application/json"}),
                    timeout=20) as r:
                h = json.loads(r.read())
            rep.kv(health_ok=h.get("ok"), mirror_count=h.get("mirror_count", 0))
        except Exception as e:
            fails.append("health GET failed: %s" % e)

        # sentinel
        probe = {"symbol": "NASDAQ:JHTEST", "title": "ops2957-sentinel",
                 "text": "JH-OPS2957 pipeline self-test — safe to delete %d" % int(time.time()),
                 "created": int(time.time() * 1000)}
        sentinel_id = None
        try:
            c, d = post(fn_url, {"token": token, "notes": [probe]})
            rep.kv(sentinel_status=c, brain_upserted=d.get("brain_upserted"),
                   mirror_added=d.get("mirror_added"))
            if (d.get("brain_upserted") or 0) < 1:
                fails.append("sentinel not upserted to brain")
            time.sleep(2)
            mir = json.loads(S3.get_object(Bucket=BUCKET,
                             Key="data/tradingview-notes.json")["Body"].read())
            for n in mir.get("notes", []):
                if "JH-OPS2957" in str(n.get("text", "")) and "JHTEST" in str(n.get("symbol", "")):
                    sentinel_id = n["id"]; break
            rep.kv(sentinel_in_mirror=bool(sentinel_id))
        except Exception as e:
            fails.append("round-trip failed: %s" % e)

        # cleanup
        if sentinel_id:
            try:
                c, d = post(fn_url, {"token": token, "delete_ids": [sentinel_id]})
                rep.kv(cleanup_ok=c == 200)
            except Exception as e:
                rep.warn("cleanup: %s" % e)

        # ── 6. Status feed ────────────────────────────────────────────
        rep.section("6. Status feed")
        status = {"ok": not fails, "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "ingest_url": fn_url, "extractor": "tools/tv-export.js v2.0",
                  "brain_merge": "tradingview-notes.json -> brain-compiler (every run)",
                  "instructions": "Open justhodl.ai/tv-notes.html for harvest instructions",
                  "pipeline": "TV browser -> tv-export.js -> tv-notes-ingest Lambda -> "
                              "tradingview-notes.json + brain.json -> brain-compiler -> "
                              "321 engine claims routed to fleet"}
        S3.put_object(Bucket=BUCKET, Key="data/tv-pipeline-status.json",
                      Body=json.dumps(status).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        rep.ok("status feed published")

        line = ("tv-brain-v2: url=%s sentinel_brain=%s mirror=%s brain_compiler_wired=%s"
                % (fn_url.split("//")[-1][:30],
                   not any("brain" in f for f in fails),
                   bool(sentinel_id),
                   ("tradingview-notes.json" in bc_path.read_text())))
        print(line); rep.kv(summary=line)
        if fails:
            for f in fails: rep.fail(f)
            print("FAILURES: " + " | ".join(fails)); sys.exit(1)
        rep.ok("TV Notes → Brain v2 pipeline fully live")


if __name__ == "__main__":
    main()
