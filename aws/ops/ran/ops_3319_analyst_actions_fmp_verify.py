"""ops 3319 — finish Path 2: ensure justhodl-analyst-actions v2.0 (FMP-
sourced) has FMP_KEY in its env, then force-run and verify the feed
populates with real analyst signals so analyst-actions.html renders.

Steps:
  1. Copy FMP_KEY from justhodl-analyst-consensus env onto
     justhodl-analyst-actions (keep MASSIVE_API_KEY too, harmless).
  2. Wait until LastUpdateStatus=Successful AND the deployed code is v2.0
     (poll: invoke returns, then check the written doc.version == 2.0.0).
     Because deploy-lambdas may still be rolling, retry the invoke a few
     times until version flips to 2.0.0.
  3. Verify data/analyst-actions.json: version 2.0.0, fresh (<15m),
     non-zero counts (ratings_7d / upgrades / pt_raises), sane samples.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", REGION)
S3 = boto3.client("s3", REGION)
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/analyst-actions.json"
FN = "justhodl-analyst-actions"


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


with report("3319_analyst_actions_fmp_verify") as rep:
    fails = []

    # 1. ensure FMP_KEY on engine env
    rep.section("ENSURE FMP_KEY")
    try:
        donor = LAM.get_function_configuration(
            FunctionName="justhodl-analyst-consensus")
        fmp = ((donor.get("Environment") or {}).get("Variables") or {}
               ).get("FMP_KEY")
        cur = LAM.get_function_configuration(FunctionName=FN)
        env = (cur.get("Environment") or {}).get("Variables") or {}
        rep.kv(fmp_key_suffix=(fmp[-4:] if fmp else None),
               engine_had_fmp=("FMP_KEY" in env))
        if fmp and env.get("FMP_KEY") != fmp:
            env["FMP_KEY"] = fmp
            LAM.update_function_configuration(
                FunctionName=FN, Environment={"Variables": env})
            for _ in range(25):
                s = LAM.get_function_configuration(FunctionName=FN)
                if s.get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(2)
            rep.ok("FMP_KEY set on engine env")
        elif not fmp:
            fails.append("no FMP_KEY on donor justhodl-analyst-consensus")
        else:
            rep.ok("FMP_KEY already present")
    except Exception as e:
        fails.append(f"env step failed: {e}")

    # 2. force-run until code is v2.0 (deploy may still be rolling)
    rep.section("FORCE RUN (await v2.0)")
    doc = None
    got_v2 = False
    for attempt in range(1, 9):
        try:
            r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                           Payload=b"{}")
            resp = json.loads(r["Payload"].read().decode())
            fn_err = r.get("FunctionError")
            time.sleep(3)
            doc = s3_json(OUT_KEY)
            ver = (doc or {}).get("version")
            rep.kv(**{f"attempt_{attempt}": {
                "fn_error": fn_err, "resp": resp, "written_version": ver}})
            if ver == "2.0.0":
                got_v2 = True
                break
        except Exception as e:
            rep.kv(**{f"attempt_{attempt}": f"invoke err: {e}"})
        time.sleep(12)  # let deploy roll forward

    if not got_v2:
        fails.append("engine never wrote version 2.0.0 (deploy not rolled or "
                     "runtime error) — see attempts")

    # 3. verify populated
    rep.section("VERIFY FEED")
    if doc:
        counts = doc.get("counts", {})
        total = sum(counts.values()) if counts else 0
        mb = doc.get("most_bullish", [])
        rep.kv(version=doc.get("version"), generated_at=doc.get("generated_at"),
               data_source=doc.get("data_source"), counts=counts,
               total_signals=total, n_most_bullish=len(mb),
               n_top_picks=len(doc.get("top_picks", [])),
               sample_bullish=(mb[0] if mb else None))
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                doc["generated_at"].replace("Z", "+00:00"))).total_seconds()
            rep.kv(age_seconds=int(age))
            if age > 900:
                fails.append(f"feed stale (age {int(age)}s)")
        except Exception:
            pass
        if total == 0:
            fails.append("feed populated but 0 signals — FMP feed empty? "
                         "(check attempts/log)")
    else:
        fails.append("feed unreadable")

    rep.section("VERDICT")
    if fails:
        for f in fails:
            rep.fail(f)
        rep.kv(RESULT="FAIL", n_fails=len(fails))
        sys.exit(1)
    rep.ok("analyst-actions v2.0 LIVE on FMP data — page renders real analyst "
           "rating transitions + PT moves. Benzinga dependency retired.")
    rep.kv(RESULT="FIXED")
