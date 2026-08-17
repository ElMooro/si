"""ops/4854 -- foreign-flows total-tile fix + global-flows v1.2
macro-only settle.
 (a) DIAGNOSE: print the current foreign-flows excluded/diag on
     record (release-night null total).
 (b) settle foreign-flows v1.1.1 (retry shell); re-invoke; assert
     flows_bn.total PRESENT numeric + latest_month 2026-06-01;
     print the June readout.
 (c) settle global-flows v1.2.0; invoke; assert taiwan macro LIVE
     + hot_money == MOVED pointer + peru intact.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=150,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def settle(fn, marker, rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=fn)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if marker in src:
                rep.ok("%s settled (attempt %d)" % (marker,
                                                    att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("%s never settled" % marker)
    FAILED.append("settle")
    return False


def fresh_invoke(fn, out_key, timeout_s, rep, payload=b"{}"):
    try:
        prev = sread(out_key).get("generated_at")
    except ClientError:
        prev = None
    lam.invoke(FunctionName=fn, InvocationType="Event",
               Payload=payload)
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        time.sleep(10)
        try:
            d = sread(out_key)
        except ClientError:
            continue
        if d.get("generated_at") != prev:
            rep.ok("%s fresh in %ds" % (fn,
                                        int(time.time() - t0)))
            return d
    rep.fail("%s: no fresh doc" % fn)
    FAILED.append("fresh_" + fn)
    return None


def main():
    with report("ops 4854 -- ff total fix + gf v1.2 settle") \
            as rep:
        rep.heading("a. diagnosis on record")
        try:
            cur = sread("data/foreign-flows.json")
            rep.log("  current excluded: %s"
                    % json.dumps(cur.get("excluded") or {})[:300])
            rep.log("  latest_month=%s new_release=%s"
                    % (cur.get("latest_month"),
                       cur.get("new_release")))
        except ClientError:
            rep.warn("  current doc unreadable")

        rep.heading("b. foreign-flows v1.1.1 + total verify")
        if not settle("justhodl-foreign-flows",
                      "foreign-flows v1.1.1", rep):
            sys.exit(1)
        d = fresh_invoke("justhodl-foreign-flows",
                         "data/foreign-flows.json", 240, rep)
        if not d:
            sys.exit(1)
        tot = (d.get("flows_bn") or {}).get("total") or {}
        if isinstance(tot.get("latest"), (int, float)) \
                and d.get("latest_month") == "2026-06-01":
            rep.ok("  TOTAL restored: %+.1fB @ %s (12m %+.1fB)"
                   % (tot["latest"], tot.get("latest_month"),
                      tot.get("sum_12m", 0)))
        else:
            rep.fail("  total still broken: %s excluded=%s"
                     % (json.dumps(tot)[:80], d.get("excluded")))
            FAILED.append("total")
        rep.log("  JUNE: treas %+.1fB eq %+.1fB corp %+.1fB "
                "agency %+.1fB tbills %+.1fB"
                % tuple((d["flows_bn"].get(k) or {})
                        .get("latest", 0)
                        for k in ("treas", "equity", "corp",
                                  "agency", "tbills")))

        rep.heading("c. global-flows v1.2 macro-only")
        if not settle("justhodl-global-flows",
                      "global-flows v1.2.0", rep):
            sys.exit(1)
        g = fresh_invoke("justhodl-global-flows",
                         "data/global-flows.json", 240, rep)
        if not g:
            sys.exit(1)
        tw = (g.get("countries") or {}).get("taiwan") or {}
        pe = (g.get("countries") or {}).get("peru") or {}
        if tw.get("status") == "LIVE" \
                and (tw.get("hot_money") or {}).get("status") \
                == "MOVED" and pe.get("status") == "LIVE":
            rep.ok("  taiwan macro LIVE, hot_money MOVED "
                   "pointer, peru intact")
        else:
            rep.fail("  tw=%s hm=%s pe=%s"
                     % (tw.get("status"),
                        (tw.get("hot_money") or {}).get("status"),
                        pe.get("status")))
            FAILED.append("gf")

        rep.heading("verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("total tile restored on June data; global-flows "
               "is macro-only")


if __name__ == "__main__":
    main()
