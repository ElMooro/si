"""ops 6700 - justhodl-worldbank-full v1.1.0 first light: one asynchronous run (the same {} event as its daily
schedule; the drain has 2,870 indicators left, ops 6699) and the bytes it writes.  v1.0.0 wrote the 2.2 MB state twice
per indicator (56,690 versions / 143 GB in the live bucket since Oct 25; the archive keeps every one).  Reported: the
state's new encoding and size, the number and bytes of state versions this run wrote, the inflight markers, the
manifest's last_run counters, and the bytes per indicator - v1.0.0's ~4.4 MB against v1.1.0's.  No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"
PFX = "data/warm/worldbank-full/"
FN = "justhodl-worldbank-full"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=120)


def versions_since(s3, key, since):
    n, b, tok = 0, 0, {}
    while True:
        resp = s3.list_object_versions(Bucket=LIVE, Prefix=key, MaxKeys=1000, **tok)
        for v in resp.get("Versions", []):
            if v["Key"] == key and v["LastModified"] >= since:
                n += 1
                b += v["Size"]
        if not resp.get("IsTruncated"):
            break
        tok = {"KeyMarker": resp["NextKeyMarker"], "VersionIdMarker": resp["NextVersionIdMarker"]}
    return n, b


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    with report("ops_6700_worldbank_full_light") as r:
        r.heading("ops 6700 - justhodl-worldbank-full v1.1.0: one run, the bytes it writes")
        cfg = lam.get_function_configuration(FunctionName=FN)
        r.log("deployed %s | %s" % (cfg["LastModified"], cfg.get("Description", "")[:80]))
        if "v1.1.0" not in cfg.get("Description", ""):
            r.fail("v1.1.0 is not deployed")
            raise SystemExit(1)
        man0 = json.loads(s3.get_object(Bucket=LIVE, Key=PFX + "manifest.json")["Body"].read())
        r.log("manifest before: version %s, as_of %s, banked %s, queue_left %s" % (man0.get("version"), man0.get("as_of"), man0.get("banked"), man0.get("queue_left")))
        since = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("kicked (async) at %s" % since.isoformat()[:19])
        man, deadline = man0, time.time() + 1020
        while time.time() < deadline:
            time.sleep(30)
            man = json.loads(s3.get_object(Bucket=LIVE, Key=PFX + "manifest.json")["Body"].read())
            if man.get("version") == "1.1.0" and man.get("as_of") != man0.get("as_of"):
                break
        if man.get("version") != "1.1.0":
            r.fail("no v1.1.0 manifest within 17 min (lease held by another run, or the run failed) - manifest %s" % json.dumps(man)[:300])
            raise SystemExit(1)
        run = man.get("last_run") or {}
        r.ok("manifest after: as_of %s, banked %s, queue_left %s, phase %s, last_run %s" % (man.get("as_of"), man.get("banked"), man.get("queue_left"), man.get("phase"), run))
        head = s3.get_object(Bucket=LIVE, Key=PFX + "_state/state.json")
        body = head["Body"].read()
        r.log("state.json now %d bytes, gzip %s" % (len(body), body[:2] == b"\x1f\x8b"))
        n_state, b_state = versions_since(s3, PFX + "_state/state.json", since)
        n_mark, b_mark = versions_since(s3, PFX + "_state/inflight.json", since)
        done = (man0.get("queue_left") or 0) - (man.get("queue_left") or 0)
        r.log("this run: %d indicators off the queue (%s uploaded, %s unchanged); state versions %d / %.2f MB; inflight markers %d / %.1f KB" % (
            done, run.get("uploaded"), run.get("unchanged"), n_state, b_state / 1e6, n_mark, b_mark / 1e3))
        if done > 0:
            r.log("state bytes per indicator: v1.0.0 ~4.4 MB (2 x 2.2 MB) -> v1.1.0 %.3f MB" % (b_state / 1e6 / done))
        r.ok("ops 6700 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
