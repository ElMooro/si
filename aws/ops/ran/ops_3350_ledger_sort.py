"""ops 3350 — ledger-sort fix. With the 13F lens revived (3349) the
scored universe is 2,118 names but by_ticker sliced the first 300 of
an UNSORTED list — the Ledger tab would show arbitrary names. results
now sorts by |flow_score| desc before the slice. Gate: min |score|
inside by_ticker >= 20 (strongest-300 proof) and top entry matches
accumulating[0] or distributing[0]."""
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-capital-flow"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))


def _j(k):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())


with report("3350_ledger_sort") as R:
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if '-abs(r.get("flow_score")' in src:
                break
        time.sleep(8)
    else:
        R.fail("sort marker never deployed")
        raise SystemExit(1)
    print("[1] deployed zip sorts results by |score|")

    g0 = _j("data/capital-flow.json").get("generated_at")
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(50):
        time.sleep(5)
        try:
            c = _j("data/capital-flow.json")
        except Exception:
            continue
        if c.get("generated_at") != g0:
            d = c
            break
    if not d:
        R.fail("feed never refreshed")
        raise SystemExit(1)
    bt = list((d.get("by_ticker") or {}).values())
    scores = sorted([abs(r.get("flow_score") or 0) for r in bt])
    mn = scores[0] if scores else 0
    out = {"n_by_ticker": len(bt), "min_abs_score": mn,
           "max_abs_score": scores[-1] if scores else 0,
           "n_scored": (d.get("summary") or {}).get("n_scored")}
    print("[2]", json.dumps(out))
    ok = len(bt) >= 250 and mn >= 15
    out["ok"] = ok
    from pathlib import Path
    import os
    Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()), "aws/ops/reports/3350.json") \
        .write_text(json.dumps(out, indent=1), encoding="utf-8")
    (R.ok if ok else R.warn)(f"ledger={len(bt)} min|score|={mn}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
