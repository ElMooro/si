#!/usr/bin/env python3
"""ops 3034 -- Highest-conviction leads strip (Khalid). Warroom v10 embeds
barometer.conviction_leads = LEARNED mechanisms with hit_rate>=0.75 AND
false_alarm_rate<=0.20 from the live weights file (record + current
score); page renders the gold strip with printed evidence, or an honest
empty note. Expect exactly {vol, global_stress} to qualify today."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    with report("3034_conviction_strip") as rep:
        rep.section("1. Wait warroom settled (this push deploys it)")
        fresh = None
        for _ in range(24):
            try:
                c = LAM.get_function_configuration(
                    FunctionName="justhodl-canary-warroom")
                lm = datetime.fromisoformat(
                    c["LastModified"].replace("+0000", "+00:00"))
                age = (datetime.now(timezone.utc)
                       - lm).total_seconds() / 60.0
                if (age < 12 and c.get("LastUpdateStatus") in
                        (None, "Successful")):
                    time.sleep(8)
                    fresh = age
                    break
            except Exception:
                pass
            time.sleep(20)
        rep.kv(code_age_min=fresh)
        if fresh is None:
            fails.append("warroom not fresh")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        rep.section("2. Regenerate + assert strip")
        LAM.invoke(FunctionName="justhodl-canary-warroom",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = s3_json("data/canary-warroom.json")
        cv = (d.get("barometer") or {}).get("conviction_leads") or {}
        leads = cv.get("leads") or []
        rep.kv(n_qualifying=cv.get("n_qualifying"),
               leads=json.dumps([{k: l.get(k) for k in
                                  ("key", "hit_rate", "false_alarm_rate",
                                   "mean_lead_months", "score")}
                                 for l in leads]))
        if not leads:
            fails.append("no conviction leads embedded")
        for l in leads:
            if (l.get("hit_rate") or 0) < 0.75 or \
                    (l.get("false_alarm_rate") or 1) > 0.20:
                fails.append("gate leak: %s" % l.get("key"))
        keys = {l.get("key") for l in leads}
        if not {"vol", "global_stress"} & keys:
            warns.append("expected qualifiers absent: got %s" % keys)

        rep.section("3. Live page (warn-level)")
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/canaries.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3034"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            ok = "HIGHEST-CONVICTION LEADS" in page
            rep.kv(page_strip=ok)
            if not ok:
                warns.append("pages not propagated yet")
        except Exception as e:
            warns.append("page check: %s" % str(e)[:100])

        rep.section("verdict")
        _fin(rep, fails, warns, {"n_qualifying": cv.get("n_qualifying")})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- %s conviction leads live" % cv.get("n_qualifying"))


def _fin(rep, fails, warns, extra):
    payload = {"ops": 3034, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3034.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
