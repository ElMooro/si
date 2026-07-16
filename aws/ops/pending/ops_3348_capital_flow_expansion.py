"""ops 3348 — capital-flow.html exponential expansion + full-data audit.
Audit found the page hid the feed's richest layers: detail.13f
(new/add/trim/exit funds, $Δ), detail.inst_change (QoQ shares %,
investor Δ), and the ENTIRE 300-name by_ticker ledger; ETF rows hid
dvol_z/5d/20d; rows didn't even pass the ticker on click. Shipped:
[engine v1.1 additive] summary (score distribution + top acc/dis
sectors), lens_conflicts (quarterly 13F vs fresher inst-QoQ sign
disagreement = footprint turning), top_new_positions (pure 13F
initiations). [page additive] every acc/dis row now shows all three
lens details + Whales-Q join (13f-flows-by-ticker wn + fund NAMES via
fb/fs) + ⚡ conflict chip + why.html?ticker links; summary strip; ETF
rows gain z/5d/20d; sector tab gains smart-money-footprint breadth
board; NEW Ledger tab (search over all 300 scored names); NEW
Conflicts & New Money tab. Verify: race-safe refire → intel keys
populated → whale join available for top names → bare-URL markers."""
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
PAGE = "https://justhodl.ai/capital-flow.html"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))


def _j(k):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())


with report("3348_capital_flow_expansion") as R:
    out = {}

    # [1] race-safe deploy wait
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if "lens_conflicts" in src and "top_new_positions" in src:
                break
        time.sleep(8)
    else:
        R.fail("deployed zip never showed intel markers")
        raise SystemExit(1)
    print("[1] deployed zip carries summary/lens_conflicts/top_new_positions")

    # [2] refire + poll
    try:
        g0 = _j("data/capital-flow.json").get("generated_at")
    except Exception:
        g0 = None
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(50):
        time.sleep(5)
        try:
            c = _j("data/capital-flow.json")
        except Exception:
            continue
        if c.get("generated_at") != g0 and c.get("summary"):
            d = c
            break
    if not d:
        R.fail("capital-flow.json never refreshed with summary")
        raise SystemExit(1)
    sm = d.get("summary") or {}
    cf = d.get("lens_conflicts") or []
    np_ = d.get("top_new_positions") or []
    bt = d.get("by_ticker") or {}
    acc = d.get("accumulating") or []
    out["feed"] = {"version": d.get("version"), "generated_at": d.get("generated_at"),
                   "summary": sm, "n_conflicts": len(cf), "n_new_positions": len(np_),
                   "n_by_ticker": len(bt), "n_acc": len(acc), "n_dis": len(d.get("distributing") or []),
                   "conflict_sample": cf[:3], "new_money_sample": np_[:3]}
    print("[2] feed:", json.dumps({k: out["feed"][k] for k in ("version", "summary", "n_conflicts",
                                                               "n_new_positions", "n_by_ticker")}, default=str))
    print("    conflicts:", json.dumps(cf[:3], default=str))
    print("    new money:", json.dumps(np_[:3], default=str))

    # [3] whale join availability for top accumulating names
    wt = (_j("data/13f-flows-by-ticker.json").get("t")) or {}
    top5 = [r.get("ticker") for r in acc[:5]]
    hits = {t: (t in wt) for t in top5}
    out["whale_join"] = {"top5": hits, "n_hit": sum(hits.values())}
    print("[3] whale join for top-5 acc:", hits)

    # [4] detail presence audit — the 'all data on page' contract
    n_det13 = sum(1 for r in acc if (r.get("detail") or {}).get("13f"))
    n_detic = sum(1 for r in acc if (r.get("detail") or {}).get("inst_change"))
    out["detail_coverage"] = {"acc_rows": len(acc), "with_13f": n_det13, "with_inst": n_detic}
    print("[4] detail coverage:", out["detail_coverage"])

    # [5] bare-URL live page markers
    markers = ['data-tab="ledger"', 'data-tab="intel"', "Smart-money footprint by sector",
               "lens_conflicts", "top_new_positions", "cf-q", "why.html?ticker="]
    live = {}
    for i in range(30):
        try:
            req = urllib.request.Request(PAGE, headers={"User-Agent": "Mozilla/5.0 ops3348",
                                                        "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
            live = {m: (m in body) for m in markers}
            if all(live.values()):
                break
        except Exception as e:
            live = {"err": str(e)}
        time.sleep(10)
    out["live_page"] = live
    print("[5] live:", live)

    ok = (d.get("version") == "1.1" and (sm.get("n_scored") or 0) >= 100
          and len(bt) >= 100 and len(np_) >= 5
          and n_det13 > 0 and n_detic > 0
          and isinstance(live, dict) and all(live.get(m) for m in markers))
    out["ok"] = ok
    from pathlib import Path
    import os
    Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()), "aws/ops/reports/3348.json") \
        .write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"v={d.get('version')} scored={sm.get('n_scored')} conflicts={len(cf)} "
                             f"new={len(np_)} ledger={len(bt)} whale_top5={out['whale_join']['n_hit']}/5 "
                             f"live_all={all(live.get(m) for m in markers) if isinstance(live, dict) else False}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
