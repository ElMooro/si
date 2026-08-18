"""ops/4882 -- earnings v1.1 universe-join verify.
 (1) settle 'earnings v1.1.0'; invoke; poll v==1.1.0.
 (2) G0 field-level: beat_league[0] carries the bucket key.
 (3) truths: join accounting sane (matched>=30% of reporters);
     row0 join fields == independent read of data/universe.json
     for that symbol; by_bucket ns sum <= matched; picks carry
     join fields.
 (4) page bucket-filter served.
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
FN = "justhodl-earnings"
B = "justhodl-dashboard-live"
OUT_KEY = "data/earnings.json"
MARKER = "earnings v1.1.0"
PAGE = Path(__file__).resolve().parents[3] / "earnings.html"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4882 -- earnings v1.1 verify") as rep:
        rep.heading("1. settle + invoke")
        for _ in range(40):
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") \
                        != "InProgress":
                    break
            except ClientError:
                pass
            time.sleep(6)
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"],
                    timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if MARKER in src:
                    rep.ok("marker settled (attempt %d)"
                           % (att + 1))
                    settled = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not settled:
            rep.fail("no marker")
            sys.exit(1)
        prev = sread(OUT_KEY).get("generated_at")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 560:
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
            except (ClientError, KeyError):
                continue
            if d.get("generated_at") != prev \
                    and d.get("v") == "1.1.0":
                doc = d
                break
        if not doc:
            rep.fail("no fresh v1.1.0 doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("2. G0 + truths")
        lg = doc.get("beat_league") or []
        if lg and "bucket" in lg[0]:
            rep.ok("league[0] carries bucket key (%s %s)"
                   % (lg[0]["t"], lg[0].get("bucket")))
        else:
            rep.fail("bucket key missing")
            sys.exit(1)
        uj = doc.get("universe_join") or {}
        n = doc["stats"]["n_reporters"]
        if uj.get("status") == "LIVE" \
                and uj.get("matched", 0) >= 0.3 * n:
            rep.ok("join %d/%d reporters"
                   % (uj["matched"], n))
        else:
            rep.fail("join thin: %s" % uj)
            FAILED.append("join")
        uni = sread("data/universe.json")
        rows_u = uni.get("stocks") or uni.get("rows") \
            or uni.get("universe") or []
        idx = {str(r.get("symbol") or r.get("ticker")
                   or "").upper(): r for r in rows_u}
        sample = next((r for r in lg
                       if r.get("sector") is not None), None)
        if sample:
            u = idx.get(sample["t"]) or {}
            mc = u.get("market_cap")
            ok = (sample["sector"] == u.get("sector")
                  and sample["mcap_b"]
                  == (round(mc / 1e9, 2)
                      if isinstance(mc, (int, float)) and mc
                      else None))
            (rep.ok if ok else rep.fail)(
                "  %s join == independent universe read "
                "(%s, $%sB)" % (sample["t"],
                                sample["sector"],
                                sample["mcap_b"]))
            if not ok:
                FAILED.append("sample")
        bb = doc["stats"].get("by_bucket") or {}
        tot = sum(x["n"] for x in bb.values())
        if 0 < tot <= uj.get("matched", 0):
            rep.ok("  by_bucket %s (sum %d <= matched)"
                   % ({k: v["n"] for k, v in bb.items()},
                      tot))
        else:
            rep.fail("  by_bucket sums off: %d vs %s"
                     % (tot, uj.get("matched")))
            FAILED.append("bucket")
        picks = (doc.get("growth_calls") or {}).get(
            "picks") or []
        if picks and "mcap_b" in picks[0]:
            rep.ok("  picks carry join fields (top %s %s "
                   "$%sB)" % (picks[0]["t"],
                              picks[0].get("bucket"),
                              picks[0].get("mcap_b")))

        rep.heading("3. page")
        if 'id="bfilter"' in PAGE.read_text(
                encoding="utf-8"):
            rep.ok("  committed filter")
        else:
            rep.fail("  filter missing")
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/earnings.html?t=%d"
                    % int(time.time()),
                    headers={"User-Agent": "ops-4882",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) \
                        as r:
                    if 'id="bfilter"' in r.read().decode(
                            "utf-8", "replace"):
                        rep.ok("  SERVED (%ds)"
                               % int(time.time() - t0))
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        else:
            rep.fail("  not served")
            FAILED.append("served")

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("cap-aware earnings desk: joins recomputed, "
               "buckets honest, filters live")


if __name__ == "__main__":
    main()
