"""ops 3342 — flows_1y refire after the update race. ops 3341's invoke
landed at 20:05:03 while deploy-lambdas finished 20:05:16 — the warm/
in-progress function served the OLD code, so top_inflows/top_outflows
came back flows_1y=null. This waits LastUpdateStatus=Successful, proves
the DEPLOYED zip actually contains the flows_1y emit (no guessing),
re-invokes, and polls the feed until flows_1y is populated."""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-finviz-universe"
FEED = "data/finviz-etf-flows.json"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))

with report("3342_flows1y_refire") as R:
    out = {}

    # [1] function fully settled
    for i in range(30):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            break
        time.sleep(4)
    out["settled"] = {"LastUpdateStatus": st.get("LastUpdateStatus"), "State": st.get("State"),
                      "LastModified": st.get("LastModified")}
    print("[1] settled:", out["settled"])

    # [2] deployed package really has the new emit
    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
    blob = urllib.request.urlopen(loc, timeout=60).read()
    src = zipfile.ZipFile(io.BytesIO(blob)).read("lambda_function.py").decode("utf-8", "ignore")
    has_marker = '"flows_1y": r.get("flows_1y")' in src
    out["deployed_code_has_flows_1y"] = has_marker
    print("[2] deployed zip marker:", has_marker)
    if not has_marker:
        R.fail("deployed code lacks flows_1y — deploy-lambdas did not ship it")
        raise SystemExit(1)

    # [3] refire + poll for populated flows_1y
    before = json.loads(s3.get_object(Bucket=BUCKET, Key=FEED)["Body"].read())
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    print("[3] refired; prior gen", before.get("generated_at"))
    ok = False
    for i in range(90):
        time.sleep(6)
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=FEED)["Body"].read())
        except Exception:
            continue
        if d.get("generated_at") == before.get("generated_at"):
            continue
        rows = (d.get("top_inflows") or []) + (d.get("top_outflows") or [])
        n1y = sum(1 for x in rows if x.get("flows_1y") is not None)
        out["result"] = {"generated_at": d.get("generated_at"), "n_rows": len(rows),
                         "flows_1y_nonnull": n1y,
                         "sample": [{k: x.get(k) for k in ("ticker", "flows_1m", "flows_1y")}
                                    for x in (d.get("top_inflows") or [])[:3]]}
        ok = n1y > 0
        break
    print("[3]", json.dumps(out.get("result", {}), default=str))
    from pathlib import Path
    import os
    rep = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws/ops/reports/3342.json"
    rep.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"flows_1y_nonnull={out.get('result', {}).get('flows_1y_nonnull')}")
    print("VERDICT:", "PASS" if ok else "FAIL")

sys.exit(0)
