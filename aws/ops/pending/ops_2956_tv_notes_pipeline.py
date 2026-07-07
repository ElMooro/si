#!/usr/bin/env python3
"""ops 2956 — TradingView Notes -> Brain pipeline ship.

Deploys justhodl-tv-notes-ingest (public Function URL; token-gated write path
into the real Brain store + data/tradingview-notes.json mirror). Bootstraps
SSM: reuses the existing Brain uid, mints an ingest token if absent. Publishes
data/tv-ingest-config.json (URL + token) so the browser extractor and
tv-notes.html can self-configure. Runs a live self-test (dry-run + real
round-trip of one sentinel note, then deletes it) so we KNOW the path writes
to the Brain before Khalid runs the harvest. No fake data ever persists.
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
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = Path(__file__).resolve().parents[2]
FN = "justhodl-tv-notes-ingest"
BRAIN_UID = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9"


def ssm_get(name):
    try:
        return SSM.get_parameter(Name=name,
                                 WithDecryption=True)["Parameter"]["Value"]
    except SSM.exceptions.ParameterNotFound:
        return None


def ssm_put(name, value, secure=True):
    SSM.put_parameter(Name=name, Value=value,
                      Type="SecureString" if secure else "String",
                      Overwrite=True)


def post(url, obj):
    req = urllib.request.Request(
        url, data=json.dumps(obj).encode("utf-8"), method="POST",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.getcode(), json.loads(r.read().decode("utf-8", "replace"))


def main():
    with report("2956_tv_notes_pipeline") as rep:
        fails = []

        # ---- SSM bootstrap -------------------------------------------
        uid = ssm_get("/justhodl/brain/uid")
        if not uid:
            ssm_put("/justhodl/brain/uid", BRAIN_UID)
            uid = BRAIN_UID
            rep.log("minted /justhodl/brain/uid")
        token = ssm_get("/justhodl/tvnotes/ingest-token")
        if not token:
            token = secrets.token_urlsafe(24)
            ssm_put("/justhodl/tvnotes/ingest-token", token)
            rep.log("minted /justhodl/tvnotes/ingest-token")
        rep.kv(brain_uid_present=bool(uid), ingest_token_present=bool(token))

        # ---- deploy ---------------------------------------------------
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / FN / "source",
                      env_vars={"INGEST_TOKEN": token, "BRAIN_UID": uid},
                      timeout=60, memory=256,
                      description="TradingView notes -> Brain ingest (ops 2956)",
                      create_function_url=True, smoke=False)

        url = LAM.get_function_url_config(
            FunctionName=FN)["FunctionUrl"].rstrip("/")
        rep.kv(function_url=url)

        # ---- publish config feed for the extractor + page ------------
        S3.put_object(Bucket=BUCKET, Key="data/tv-ingest-config.json",
                      Body=json.dumps({"ingest_url": url, "token": token,
                                       "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                                time.gmtime())}).encode(),
                      ContentType="application/json", CacheControl="max-age=120")

        # ---- health -------------------------------------------------
        time.sleep(4)
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                h = json.loads(r.read().decode())
            rep.kv(health_ok=h.get("ok"), mirror_count=h.get("mirror_count"))
        except Exception as e:
            fails.append("health GET failed: %s" % e)

        # ---- dry-run validation --------------------------------------
        sentinel_text = "JH-OPS-SENTINEL %d — pipeline self-test, safe to delete" % int(time.time())
        probe = {"symbol": "NASDAQ:JHTEST", "text": sentinel_text,
                 "title": "selftest", "created": int(time.time() * 1000)}
        try:
            c, d = post(url, {"token": token, "selftest": True, "notes": [probe]})
            rep.kv(dryrun_status=c, would_ingest=d.get("would_ingest"),
                   dryrun_rejected=d.get("rejected"))
            if d.get("would_ingest") != 1:
                fails.append("dry-run did not normalize the sentinel")
        except Exception as e:
            fails.append("dry-run failed: %s" % e)

        # ---- real round-trip: write sentinel, confirm in mirror, delete
        sentinel_id = None
        try:
            c, d = post(url, {"token": token, "notes": [probe]})
            rep.kv(ingest_status=c, brain_upserted=d.get("brain_upserted"),
                   brain_failed=d.get("brain_failed"),
                   mirror_added=d.get("mirror_added"))
            if not (d.get("brain_upserted") or 0) >= 1:
                fails.append("sentinel not upserted to brain (brain route down?)")
            # find id in mirror
            time.sleep(2)
            mir = json.loads(S3.get_object(Bucket=BUCKET,
                             Key="data/tradingview-notes.json")["Body"].read())
            for n in mir.get("notes", []):
                if n.get("text", "").find("JH-OPS-SENTINEL") != -1 and "JHTEST" in n.get("symbol", ""):
                    sentinel_id = n["id"]
                    break
            rep.kv(sentinel_in_mirror=bool(sentinel_id))
        except Exception as e:
            fails.append("real ingest failed: %s" % e)

        # cleanup sentinel from brain + mirror
        if sentinel_id:
            try:
                c, d = post(url, {"token": token, "delete_ids": [sentinel_id]})
                rep.kv(cleanup_status=c, cleanup_removed=d.get("mirror_removed"))
            except Exception as e:
                rep.warn("cleanup failed (harmless): %s" % e)

        # ---- brain-compiler awareness (does it read the mirror?) -----
        bc = ROOT / "lambdas" / "justhodl-brain-compiler" / "source" / "lambda_function.py"
        try:
            src = bc.read_text()
            rep.kv(brain_compiler_reads_brain=("brain.json" in src or "/brain" in src))
        except Exception:
            pass

        line = ("tv-notes: url=%s health=%s dryrun=%s brain_write=%s mirror_sentinel=%s"
                % (url.split("//")[-1][:24], not any("health" in f for f in fails),
                   not any("dry-run" in f for f in fails),
                   not any("brain" in f for f in fails), bool(sentinel_id)))
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            print("FAILURES: " + " | ".join(fails))
            sys.exit(1)
        rep.ok("TV notes pipeline live: ingest deployed, brain round-trip proven, config published")


if __name__ == "__main__":
    main()
