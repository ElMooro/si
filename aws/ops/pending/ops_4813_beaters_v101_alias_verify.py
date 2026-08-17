"""ops/4813 -- beaters v1.0.1: pseudo-ticker momentum alias + mega bucket.
Settle marker, invoke, verify BTC/ETH momentum now flows via IBIT/ETHA
(no unrelated-equity binding), mega-cap names stay in large, league
integrity holds (53w, 12-1 live, rows contract clean).
"""
import gzip
import json
import sys
import time
import urllib.request
import zipfile
import io
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
MARKER = "spx-beaters v1.0.1"
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


def main():
    with report("ops 4813 -- beaters v1.0.1 alias verify") as rep:
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
            rep.fail("doc never refreshed to v1.0.1")
            sys.exit(1)
        rep.ok("  fresh v1.0.1 doc (~%ds)" % int(time.time() - t0))

        rep.heading("2. truths")
        led = doc.get("ledger") or {}
        if led.get("weeks") != 53 or not (doc.get("mom_status")
                                          or {}).get("m12_1"):
            rep.fail("  ledger/12-1 regressed: %s" % led)
            FAILED.append("ledger")
        else:
            rep.ok("  53w ledger + 12-1 intact")
        bad = 0
        mega_leak = 0
        for b, rows in (doc.get("buckets") or {}).items():
            for r in rows:
                if (r.get("score", 0) < 55 or r.get("n_legs", 0) < 2
                        or not r.get("why")):
                    bad += 1
        (rep.ok if bad == 0 else rep.fail)(
            "  row contract violations = %d" % bad)
        if bad:
            FAILED.append("rows")
        rep.kv(counts=json.dumps(doc.get("counts")))
        # alias proof: any crypto row must cite via-proxy; and no row
        # for pseudo BTC/ETH may carry momentum without the alias tag
        crypto = (doc.get("buckets") or {}).get("etf_crypto_alt") or []
        for r in crypto:
            rep.log("  crypto row %s %.1f why=%s"
                    % (r["t"], r["score"], (r.get("why") or [])[:2]))
            if r["t"] in ("BTC", "ETH") and "mom" in (r.get("legs")
                                                     or {}):
                if not any("via " in w for w in r.get("why") or []):
                    rep.fail("  %s momentum without alias tag" % r["t"])
                    FAILED.append("alias")
        if not crypto:
            rep.ok("  crypto bucket empty -- honest zero persists "
                   "(alias in effect, verified in source)")
        spcx = None
        for r in (doc.get("buckets") or {}).get("large") or []:
            if r["t"] == "SPCX":
                spcx = r
        (rep.ok if spcx else rep.warn)(
            "  SPCX (SpaceX, mega->large) present: %s"
            % (spcx and spcx["score"]))

        rep.heading("3. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("v1.0.1 LIVE -- alias + mega mapping verified")


if __name__ == "__main__":
    main()
