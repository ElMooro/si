"""ops/4814 -- spx-beaters v1.1.0 COMEBACK wing verify.
 (1) settle 'spx-beaters v1.1.0'; invoke; poll fresh doc.
 (2) comeback truths on the LIVE league:
     - bucket present; every row: dd_52w_pct <= -30, quality leg
       present, n_legs >= 3, score >= comeback_min, why cites the
       drawdown AND a quality line.
     - TRAP INTEGRITY: re-pull beneish red_flags, earnings-quality
       avoid, insider-sell clusters, comeback-screener DILUTION board
       live and assert ZERO overlap with listed comeback names.
     - momentum buckets unchanged (53w, 12-1 intact, contract clean).
 (3) full comeback readout (top 8 with why).
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

FN = "justhodl-spx-beaters"
B = "justhodl-dashboard-live"
OUT_KEY = "data/spx-beaters.json"
MARKER = "spx-beaters v1.1.0"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []
start_iso = __import__("datetime").datetime.now(
    __import__("datetime").timezone.utc).isoformat()


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def tick(r):
    for k in ("ticker", "symbol", "t"):
        if r.get(k):
            return str(r[k]).upper()
    return None


def main():
    with report("ops 4814 -- comeback wing verify") as rep:
        rep.heading("1. settle + invoke")
        ok = False
        for _ in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if MARKER in src:
                    ok = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not ok:
            rep.fail("marker never settled")
            sys.exit(1)
        rep.ok("marker settled")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 480:
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
                if d.get("marker") == MARKER and \
                        d.get("as_of", "") > start_iso:
                    doc = d
                    break
            except ClientError:
                pass
        if not doc:
            rep.fail("doc never refreshed to v1.1.0")
            sys.exit(1)
        rep.ok("  fresh v1.1.0 doc (~%ds)" % int(time.time() - t0))

        rep.heading("2. comeback truths")
        cb = (doc.get("buckets") or {}).get("comeback")
        if cb is None:
            rep.fail("  comeback bucket missing")
            sys.exit(1)
        cmin = doc.get("comeback_min") or 60
        rep.ok("  comeback qualifiers = %s (showing %d)"
               % ((doc.get("counts") or {}).get("comeback"), len(cb)))
        bad = 0
        for r in cb:
            probs = []
            if (r.get("dd_52w_pct") or 0) > -30:
                probs.append("dd")
            if "quality" not in (r.get("legs") or {}):
                probs.append("quality")
            if r.get("n_legs", 0) < 3:
                probs.append("n_legs")
            if r.get("score", 0) < cmin:
                probs.append("score")
            whys = " ".join(r.get("why") or [])
            if "52w high" not in whys:
                probs.append("why_dd")
            if "quality" not in whys.lower() and "screener" not in \
                    whys.lower():
                probs.append("why_quality")
            if probs:
                bad += 1
                rep.warn("    %s violates %s" % (r["t"], probs))
        (rep.ok if bad == 0 else rep.fail)(
            "  row contract violations = %d" % bad)
        if bad:
            FAILED.append("rows")

        rep.heading("2b. trap integrity (live cross-check)")
        traps = set()
        for key, cont in (("data/beneish.json", "red_flags"),
                          ("data/earnings-quality.json",
                           "top_10_low_quality_avoid"),
                          ("data/insider-sell-cluster.json",
                           "top_clusters")):
            try:
                for r in (sread(key).get(cont) or []):
                    t = tick(r)
                    if t:
                        traps.add(t)
            except ClientError:
                rep.warn("  %s unavailable" % key)
        try:
            for bname, rows in ((sread("data/comeback-screener.json")
                                 .get("boards")) or {}).items():
                if "DILUTION" in str(bname).upper():
                    for r in (rows or []):
                        t = tick(r)
                        if t:
                            traps.add(t)
        except ClientError:
            rep.warn("  comeback-screener unavailable")
        overlap = sorted({r["t"] for r in cb} & traps)
        (rep.ok if not overlap else rep.fail)(
            "  trap-set size %d, overlap with listed = %s"
            % (len(traps), overlap or "NONE"))
        if overlap:
            FAILED.append("trap_overlap")

        rep.heading("2c. momentum wings unchanged")
        led = doc.get("ledger") or {}
        okm = (led.get("weeks") == 53
               and (doc.get("mom_status") or {}).get("m12_1"))
        (rep.ok if okm else rep.fail)(
            "  53w + 12-1 intact = %s" % okm)
        if not okm:
            FAILED.append("mom")
        rep.kv(counts=json.dumps(doc.get("counts")))

        rep.heading("3. comeback readout")
        if not cb:
            rep.warn("  no comeback names cleared the bar this week "
                     "-- honest zero (quality + turn + cheap-vs-index "
                     "is a hard triple)")
        for r in cb[:8]:
            rep.ok("  %-6s %5.1f  dd %s%%  8w %s%%  scope=%s  [%s]"
                   % (r["t"], r["score"], r.get("dd_52w_pct"),
                      r.get("ret_8w_pct"), r.get("scope"),
                      r.get("bucket")))
            rep.log("      " + " | ".join((r.get("why") or [])[:4]))

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("comeback wing LIVE -- quality drawdown candidates "
               "with trap guards verified against live flag sets")


if __name__ == "__main__":
    main()
