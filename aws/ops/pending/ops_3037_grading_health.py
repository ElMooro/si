#!/usr/bin/env python3
"""ops 3037 -- Push C+D (all-8 items 2,6,7,8): warroom v12 emits a
harvester-gradeable stance (earned<40 -> LONG SPY; >=55 -> LONG SH
inverse ETF, since the harvester grades all picks LONG); per-mechanism
feed_as_of/age/stale on cards; page: lead-time lane, card drill-down,
data-health chip, mobile stack. Verifies: stance emitted per band,
harvester writes eng:canary-warroom rows, freshness fields live."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=420,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def wait_fresh_settled(fn, max_min=8):
    for _ in range(int(max_min * 3)):
        try:
            c = LAM.get_function_configuration(FunctionName=fn)
            lm = datetime.fromisoformat(
                c["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds() / 60.0
            if age < 12 and c.get("LastUpdateStatus") in (None,
                                                          "Successful"):
                time.sleep(8)
                return age
        except Exception:
            pass
        time.sleep(20)
    return None


def main():
    fails, warns = [], []
    with report("3037_grading_health") as rep:
        rep.section("1. Warroom v12 regen")
        if wait_fresh_settled("justhodl-canary-warroom") is None:
            fails.append("warroom not fresh")
            _fin(rep, fails, warns)
            sys.exit(1)
        LAM.invoke(FunctionName="justhodl-canary-warroom",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = s3_json("data/canary-warroom.json")
        earned = ((d.get("barometer") or {}).get("views") or {}).get(
            "earned", {}).get("score")
        picks = d.get("top_picks")
        mechs = d.get("mechanisms") or []
        ages = {m.get("key"): m.get("feed_age_h") for m in mechs}
        stale = [m.get("key") for m in mechs if m.get("stale")]
        rep.kv(earned=earned, top_picks=json.dumps(picks),
               feed_ages_h=json.dumps(ages), stale=stale)
        if picks is None:
            fails.append("top_picks key missing")
        elif earned is not None and earned < 40 and (
                not picks or picks[0].get("ticker") != "SPY"):
            fails.append("risk-on band should emit LONG SPY")
        elif earned is not None and earned >= 55 and (
                not picks or picks[0].get("ticker") != "SH"):
            fails.append("risk-off band should emit LONG SH")
        elif earned is not None and 40 <= earned < 55 and picks:
            fails.append("FLAT band emitted a pick")
        if not any(isinstance(v, (int, float)) for v in ages.values()):
            fails.append("feed_age_h not populated")

        rep.section("2. Harvester ingest")
        r = LAM.invoke(FunctionName="justhodl-signal-harvester",
                       InvocationType="RequestResponse", Payload=b"{}")
        body = json.loads(r["Payload"].read() or b"{}")
        blob = json.dumps(body)[:400]
        rep.kv(harvester=blob)
        ok_row = "canary-warroom" in blob
        if not ok_row:
            ddb = boto3.resource("dynamodb", region_name="us-east-1")
            t = ddb.Table("justhodl-signals")
            sc = t.scan(FilterExpression=boto3.dynamodb.conditions.Attr(
                "signal_type").eq("eng:canary-warroom"), Limit=200,
                Select="COUNT")
            rep.kv(ddb_rows=sc.get("Count"))
            ok_row = (sc.get("Count") or 0) >= 1
        if picks and not ok_row:
            fails.append("no eng:canary-warroom row after harvest")
        rep.log("scorecard grading matures in ~3 weeks; alpha verdict "
                "will appear in engine-alpha.json automatically.")

        rep.section("3. Live page (warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/canaries.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3037"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            marks = {"lane": "FIRING CANARIES BY LEAD-TIME",
                     "health": "data health:",
                     "drill": "scrollIntoView"}
            res = {k: (v in page) for k, v in marks.items()}
            rep.kv(**res)
            if not all(res.values()):
                warns.append("pages lag: %s" % res)
        except Exception as e:
            warns.append("page: %s" % str(e)[:90])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    payload = {"ops": 3037, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    (AWS_DIR / "ops" / "reports" / "3037.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
