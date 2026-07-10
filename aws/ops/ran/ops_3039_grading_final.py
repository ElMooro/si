#!/usr/bin/env python3
"""ops 3039 -- grading FINAL. 3038: the harvester response field
"engines" is a COUNT (int), len() crashed. Definitive check now =
paginated DDB scan for eng:canary-warroom rows logged in the last
40 min (stops at first hit).

Prior: ops 3038 -- grading CLOSE. 3037 fail was a verification artifact: the
harvester response was string-truncated at 400 chars before the
membership check, then a Limit=200 filtered Scan on a huge table
guaranteed a miss. Now parses the response body JSON and checks
engines_hit membership directly.

Prior: ops 3037 -- Push C+D (all-8 items 2,6,7,8): warroom v12 emits a
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
    with report("3039_grading_final") as rep:
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

        rep.section("2. Harvester ingest (proper verification)")
        r = LAM.invoke(FunctionName="justhodl-signal-harvester",
                       InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(r["Payload"].read() or b"{}")
        body = raw.get("body")
        body = json.loads(body) if isinstance(body, str) else (body or raw)
        rep.kv(n_written=body.get("n_written"),
               engines_count=body.get("engines"))
        # definitive: recent eng:canary-warroom rows in the ledger
        from boto3.dynamodb.conditions import Attr
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        t = ddb.Table("justhodl-signals")
        cutoff = int(time.time()) - 2400
        cnt, lek = 0, None
        for _ in range(60):
            kw = {"Select": "COUNT",
                  "FilterExpression": Attr("signal_type").eq(
                      "eng:canary-warroom") & Attr("logged_epoch").gt(
                      cutoff)}
            if lek:
                kw["ExclusiveStartKey"] = lek
            pg = t.scan(**kw)
            cnt += pg.get("Count", 0)
            lek = pg.get("LastEvaluatedKey")
            if not lek or cnt:
                break
        rep.kv(recent_warroom_rows=cnt)
        if picks and cnt < 1:
            fails.append("no recent eng:canary-warroom ledger row")

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
    payload = {"ops": 3039, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    (AWS_DIR / "ops" / "reports" / "3039.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
