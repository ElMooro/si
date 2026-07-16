"""ops 3347 — event-study funnel fix verify. 3346 showed n_events=0
with no error: Polygon returns ~74 total rows (not 120), so the
len(arr)<85 gate silently zeroed the universe. This push relaxes gates
(len>=65, baseline>=35, t from 45), adds a self-explaining funnel diag
into the doc, and hard-skips corrupt (MUU-class) series where any
|daily flow| > 50% AUM. Gate: n_events>=10 and funnel.eligible>=60."""
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
FN = "justhodl-etf-fund-flows"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))


def _j(k):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())


with report("3347_event_funnel") as R:
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if '"funnel": diag' in src:
                break
        time.sleep(8)
    else:
        R.fail("funnel marker never deployed")
        raise SystemExit(1)
    print("[1] deployed zip carries funnel diag")

    try:
        g0 = _j("etf-flows/event-study.json").get("generated_at")
    except Exception:
        g0 = None
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    es = None
    for i in range(80):
        time.sleep(6)
        try:
            c = _j("etf-flows/event-study.json")
        except Exception:
            continue
        if c.get("generated_at") != g0 and (c.get("event_study") or {}).get("funnel"):
            es = c
            break
    if not es:
        R.fail("event-study never refreshed with funnel")
        raise SystemExit(1)
    st_ = es["event_study"]
    print("[2] funnel:", json.dumps(st_.get("funnel"), default=str))
    print("[2] n_events:", st_.get("n_events"), "bench:", st_.get("benchmark"))
    print("    overall:", json.dumps(st_.get("overall"), default=str))
    print("    by_dir:", json.dumps(st_.get("by_dir"), default=str))
    print("    by_quadrant:", json.dumps(st_.get("by_quadrant"), default=str))
    print("    smart:", json.dumps(st_.get("smart_money"), default=str),
          " retail:", json.dumps(st_.get("retail_favored"), default=str))
    print("    top3:", json.dumps((st_.get("top_events") or [])[:3], default=str))
    fn = st_.get("funnel") or {}
    n = st_.get("n_events") or 0
    ok = n >= 10 and (fn.get("eligible") or 0) >= 60
    out = {"ok": ok, "n_events": n, "funnel": fn, "overall": st_.get("overall"),
           "by_dir": st_.get("by_dir"), "by_quadrant": st_.get("by_quadrant"),
           "smart_money": st_.get("smart_money"), "retail_favored": st_.get("retail_favored"),
           "benchmark": st_.get("benchmark"), "top_events": (st_.get("top_events") or [])[:6]}
    from pathlib import Path
    import os
    Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()), "aws/ops/reports/3347.json") \
        .write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"events={n} eligible={fn.get('eligible')} "
                             f"z_fires={fn.get('z_fires')} nav_fail={fn.get('nav_guard_fail')}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
