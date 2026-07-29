"""ops_4100 — settle source-map v2.2 (venue vocabulary), re-roll, verify
the NEW list is agency-pure (venue codes classified, not surfaced)."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-source-map"
MARK = "source-map engine v2.2 ops4100"
ADDED = ("cryptocap", "iceus", "ose", "comex", "gpw", "nymex", "ftse",
         "hose", "vie", "cselk", "pse", "sparks", "spcfd", "dj")


def main():
    with report("4100_venue_vocab") as rep:
        rep.heading("ops 4100 — venue vocabulary, agency-pure NEW list")
        checks = []
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src and "cryptocap" in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        ok = False
        for _ in range(24):
            try:
                lam.update_function_code(FunctionName=FN,
                                         ZipFile=buf.getvalue(), Publish=True)
            except Exception:
                pass
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") != "InProgress":
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=FN)["Code"]["Location"],
                    timeout=60).read())).read("lambda_function.py").decode()
                if MARK in dep:
                    ok = True
                    break
            time.sleep(8)
        checks.append(("v2.2 settled in deployed zip", ok))

        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b'{"source": "ops4100"}')
        checks.append(("engine invoke clean", not r.get("FunctionError")))

        sm = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/source-map.json")["Body"].read())
        new = sm.get("new_sources") or []
        rep.kv(marker=sm.get("marker"),
               symbols_with_source=sm.get("symbols_with_source"),
               n_new=len(new))
        leak = [x for x in new
                if str(x.get("source", "")).lower() in ADDED]
        rep.kv(venue_leaks=len(leak))
        for row in new[:25]:
            rep.log(f"  NEW {row.get('n_symbols'):5}  "
                    f"{str(row.get('source'))[:58]}   "
                    f"e.g. {', '.join(row.get('examples') or [])[:50]}")
        kf = sm.get("known_families") or {}
        rep.log("  KNOWN: " + ", ".join(
            f"{k}:{v}" for k, v in
            sorted(kf.items(), key=lambda x: -x[1])[:10]))
        checks.append(("no added-venue leaks in NEW", not leak))

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — map re-rolled agency-pure: {len(new)} NEW, "
               f"{sm.get('symbols_with_source')} sourced")


if __name__ == "__main__":
    main()
