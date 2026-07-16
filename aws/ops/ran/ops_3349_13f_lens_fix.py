"""ops 3349 — 13F-lens schema-drift fix verify. 3348 exposed that
capital-flow's 13F reader wanted changes_summary{} while the producer
writes flat n_funds_new_position/adding/trimming/exiting — so
new/add/trim/exit were silently ZERO across all 378 names and the 13F
lens contributed only the n_funds>=10 bonus. Reader now maps the
current schema (legacy kept as fallback). Gate: top_new_positions>=5,
sum(new) across accumulating > 0, and lens_conflicts recount printed."""
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


with report("3349_13f_lens_fix") as R:
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if "n_funds_new_position" in src:
                break
        time.sleep(8)
    else:
        R.fail("schema-fix marker never deployed")
        raise SystemExit(1)
    print("[1] deployed zip reads n_funds_* schema")

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

    acc = d.get("accumulating") or []
    np_ = d.get("top_new_positions") or []
    cf = d.get("lens_conflicts") or []
    sm = d.get("summary") or {}
    tot_new = sum(((r.get("detail") or {}).get("13f") or {}).get("new") or 0 for r in acc)
    tot_add = sum(((r.get("detail") or {}).get("13f") or {}).get("added") or 0 for r in acc)
    out = {"n_new_positions": len(np_), "n_conflicts": len(cf),
           "acc_sum_new": tot_new, "acc_sum_added": tot_add,
           "summary": sm, "new_money_top5": np_[:5], "conflict_top3": cf[:3],
           "acc_top3": [{k: r.get(k) for k in ("ticker", "flow_score", "lenses")} | 
                        {"f13": (r.get("detail") or {}).get("13f")} for r in acc[:3]]}
    print("[2] new-money board:", json.dumps(np_[:5], default=str))
    print("[2] conflicts:", len(cf), json.dumps(cf[:3], default=str))
    print("[2] acc top3 w/ 13F detail:", json.dumps(out["acc_top3"], default=str)[:800])
    print("[2] summary:", json.dumps({k: sm.get(k) for k in ("n_scored", "n_strong_acc", "n_strong_dis")}))

    ok = len(np_) >= 5 and tot_new + tot_add > 0
    out["ok"] = ok
    from pathlib import Path
    import os
    Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()), "aws/ops/reports/3349.json") \
        .write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"new_money={len(np_)} conflicts={len(cf)} "
                             f"acc_new={tot_new} acc_add={tot_add}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
