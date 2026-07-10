#!/usr/bin/env python3
"""ops 3036 -- Push B (items 4+1-replay): weights v3 multi-horizon
(3/6/12mo, best-horizon pick per mechanism, per-horizon records
published) + PROXY replay 1996-> (data/warroom-replay.json, equal +
earned monthly composites, crisis windows) + page chart. Asserts the
replay is CREDIBLE: earned composite >=60 inside the GFC window."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=480,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    with report("3036_multihorizon_replay") as rep:
        rep.section("1. Wait weights fn settled")
        ok = False
        for _ in range(24):
            try:
                c = LAM.get_function_configuration(
                    FunctionName="justhodl-warroom-weights")
                lm = datetime.fromisoformat(
                    c["LastModified"].replace("+0000", "+00:00"))
                age = (datetime.now(timezone.utc)
                       - lm).total_seconds() / 60.0
                if age < 12 and c.get("LastUpdateStatus") in (
                        None, "Successful"):
                    time.sleep(8)
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("weights fn not fresh")
            _fin(rep, fails, warns, {})
            sys.exit(1)

        rep.section("2. Re-study (3 horizons) + replay")
        r = LAM.invoke(FunctionName="justhodl-warroom-weights",
                       InvocationType="RequestResponse", Payload=b"{}")
        body = json.loads(r["Payload"].read() or b"{}")
        rep.kv(invoke=json.dumps(body)[:220])
        if body.get("errorMessage"):
            fails.append("crashed: %s" % body["errorMessage"][:140])
            _fin(rep, fails, warns, {})
            sys.exit(1)
        wj = s3_json("data/warroom-weights.json")
        mechs = wj.get("mechanisms") or {}
        learned = {k: v for k, v in mechs.items()
                   if v.get("status") == "LEARNED"}
        hz_ok = sum(1 for v in learned.values()
                    if v.get("best_horizon_months") in (3, 6, 12)
                    and v.get("horizons"))
        rep.kv(n_learned=len(learned), with_horizons=hz_ok,
               best_horizons=json.dumps({k: v.get("best_horizon_months")
                                         for k, v in learned.items()}),
               weights=json.dumps({k: v.get("weight")
                                   for k, v in mechs.items()}))
        if hz_ok < 10:
            fails.append("horizons on %d learned (<10)" % hz_ok)
        rp = s3_json("data/warroom-replay.json")
        M, EW = rp.get("months") or [], rp.get("earned") or []
        gfc = [EW[i] for i, mo in enumerate(M)
               if "2008-09" <= mo <= "2009-03" and EW[i] is not None]
        cov = [EW[i] for i, mo in enumerate(M)
               if "2020-02" <= mo <= "2020-04" and EW[i] is not None]
        rep.kv(replay_months=len(M), first=M[0] if M else None,
               gfc_peak=max(gfc) if gfc else None,
               covid_peak=max(cov) if cov else None)
        if len(M) < 300:
            fails.append("replay months=%d (<300)" % len(M))
        if not gfc or max(gfc) < 60:
            fails.append("replay NOT credible: GFC peak %s (<60)"
                         % (max(gfc) if gfc else None))
        if cov and max(cov) < 55:
            warns.append("COVID peak only %s" % max(cov))

        rep.section("3. Live page (warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/canaries.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3036"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            if "BAROMETER REPLAY" not in page:
                warns.append("pages not propagated yet")
            rep.kv(page_replay="BAROMETER REPLAY" in page)
        except Exception as e:
            warns.append("page: %s" % str(e)[:90])

        rep.section("verdict")
        _fin(rep, fails, warns, {"gfc_peak": max(gfc) if gfc else None,
                                 "n_learned": len(learned)})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns, extra):
    payload = {"ops": 3036, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3036.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
