"""ops 3344 — flow event-study + leveraged risk-appetite arc.
Ships with this push: [engine justhodl-etf-fund-flows, additive]
build_event_study(): every |z|>=2 daily-flow spike (rolling 60d
baseline, 10d cooldown, leveraged/vol excluded) → forward 5d/21d NAV
return minus SPY, quadrant-classified (stealth_accum/chase/
distribution/capitulation), smart-vs-retail split → NEW output
etf-flows/event-study.json. build_leveraged_appetite(): bull vs bear
5d flows inside the leveraged complex → composite.leveraged_appetite
(RISK_SEEKING/NEUTRAL/RISK_AVERSE + pair detail). [page sectors.html,
additive] sections #flow-evidence (stat tiles by direction/quadrant/
cohort + largest-outcome table) and #lev-appetite (bull/bear gauge +
pair chips). Verify: 3342 race-safe pattern → refire → validate study
aggregates + composite key → bare-URL page markers."""
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
PAGE = "https://justhodl.ai/sectors.html"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))


def _j(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


with report("3344_event_study_lev") as R:
    out = {}

    # [1] settled + deployed marker
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if "build_event_study" in src and "leveraged_appetite" in src:
                break
        time.sleep(8)
    else:
        R.fail("deployed zip never showed event-study markers")
        raise SystemExit(1)
    out["deployed_marker"] = True
    print("[1] deployed zip carries build_event_study + leveraged_appetite")

    # [2] refire + poll for the new output
    try:
        before_gen = _j("etf-flows/event-study.json").get("generated_at")
    except Exception:
        before_gen = None
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    print("[2] refired; prior event-study gen:", before_gen)
    es = None
    for i in range(80):  # ~8 min; engine ~100 ETF parallel, timeout 300
        time.sleep(6)
        try:
            cand = _j("etf-flows/event-study.json")
        except Exception:
            continue
        if cand.get("generated_at") != before_gen and cand.get("event_study"):
            es = cand
            break
    if not es:
        R.fail("event-study.json never appeared fresh")
        raise SystemExit(1)
    st_ = es["event_study"]
    out["event_study"] = {k: st_.get(k) for k in ("n_events", "overall", "by_dir", "by_quadrant",
                                                  "smart_money", "retail_favored")}
    print("[2] study:", json.dumps(out["event_study"], default=str)[:900])
    print("    top:", json.dumps((st_.get("top_events") or [])[:3], default=str))

    # [3] composite carries the leveraged gauge
    co = _j("etf-flows/composite.json")
    la = ((co.get("composite") or {}).get("leveraged_appetite")) or {}
    out["leveraged_appetite"] = {k: la.get(k) for k in ("read", "bull_5d_usd", "bear_5d_usd",
                                                        "net_5d_usd")}
    out["leveraged_appetite"]["n_pairs"] = len(la.get("pairs") or [])
    print("[3] lev-appetite:", out["leveraged_appetite"])

    # [4] bare-URL page markers
    markers = ["flow-evidence", "lev-appetite", "event-study.json",
               "Largest 21d outcomes", "Leveraged risk appetite"]
    live = {}
    for i in range(30):
        try:
            req = urllib.request.Request(PAGE, headers={"User-Agent": "Mozilla/5.0 ops3344",
                                                        "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
            live = {m: (m in body) for m in markers}
            if all(live.values()):
                break
        except Exception as e:
            live = {"err": str(e)}
        time.sleep(10)
    out["live_page"] = live
    print("[4] live:", live)

    n_ev = st_.get("n_events") or 0
    ok = (n_ev >= 5 and la.get("read") in ("RISK_SEEKING", "NEUTRAL", "RISK_AVERSE")
          and out["leveraged_appetite"]["n_pairs"] >= 3
          and isinstance(live, dict) and all(live.get(m) for m in markers))
    out["ok"] = ok
    from pathlib import Path
    import os
    rep = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws/ops/reports/3344.json"
    rep.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"events={n_ev} lev={la.get('read')} pairs={out['leveraged_appetite']['n_pairs']} "
                             f"live_all={all(live.get(m) for m in markers) if isinstance(live, dict) else False}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
