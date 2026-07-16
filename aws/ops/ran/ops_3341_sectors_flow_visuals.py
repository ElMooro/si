"""ops 3341 — sectors.html fund-flows visual upgrade + engine audit.

Khalid: 'biggest inflow and outflows needs to be enhanced with some
visuals; also audit the engine and see if all the data is included on
this page'. Audit found the feed carries name/aum/flows_1m_pct/3m/ytd/
ret_1y/etf_type for 25+25 ETFs while the page showed 6 ticker chips per
side. Fix shipped in the same push: [engine] _ef now also emits
flows_1y (parser already had it — feed did not); [page] tornado bars
(flow-scaled, %-of-AUM badge, 3M/YTD/1Y term structure, 1y return,
type tag, why.html links), Σ summary strip, show-all-25 toggle, and a
'Most intense flows' board (|1M flow| % of AUM, ≥$500M funds) — the
data the $-ranking hides. This script: [1] waits for deploy-lambdas to
land the new finviz-universe code, [2] fires it and polls the feed for
a fresh generated_at carrying flows_1y, [3] reports per-field non-null
coverage across top_inflows+top_outflows so no rendered column is dead,
[4] polls the BARE live page for the new markers (verify-doctrine:
gate on client reality)."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-finviz-universe"
FEED = "data/finviz-etf-flows.json"
PAGE = "https://justhodl.ai/sectors.html"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))


def _get_feed():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=FEED)["Body"].read())
    except Exception:
        return None


with report("3341_sectors_flow_visuals") as R:
    t0 = datetime.now(timezone.utc)
    out = {"steps": {}}

    # ── [1] wait for deploy-lambdas to land the flows_1y code ──
    landed = False
    for i in range(40):  # ~5 min
        try:
            lm = lam.get_function_configuration(FunctionName=FN)["LastModified"]
            lm_dt = datetime.strptime(lm.split("+")[0], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
            if (t0 - lm_dt).total_seconds() < 420 or lm_dt > t0:
                landed = True
                break
        except Exception as e:
            print("  cfg poll err:", e)
        time.sleep(8)
    out["steps"]["deploy_landed"] = landed
    print(f"[1] deploy landed: {landed}")

    # ── [2] fire the engine, poll feed for fresh gen + flows_1y ──
    before = _get_feed() or {}
    before_gen = before.get("generated_at")
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    print(f"[2] invoked {FN} async; prior gen={before_gen}")
    fresh, doc = False, before
    for i in range(90):  # ~9 min — full 11.3k FinViz pull
        time.sleep(6)
        d = _get_feed()
        if d and d.get("generated_at") != before_gen:
            fresh, doc = True, d
            break
    ti = doc.get("top_inflows") or []
    to = doc.get("top_outflows") or []
    has_1y = any(x.get("flows_1y") is not None for x in ti + to)
    out["steps"]["feed_refreshed"] = {"fresh": fresh, "generated_at": doc.get("generated_at"),
                                      "n_etfs": doc.get("n_etfs"), "flows_1y_present": has_1y}
    print(f"    fresh={fresh} gen={doc.get('generated_at')} n_etfs={doc.get('n_etfs')} flows_1y={has_1y}")

    # ── [3] field-coverage audit: is every rendered column alive? ──
    rows = ti + to
    fields = ["ticker", "name", "aum", "flows_1m", "flows_1m_pct", "flows_3m",
              "flows_ytd", "flows_1y", "ret_1y", "etf_type", "expense", "n_holdings"]
    cov = {f: sum(1 for x in rows if x.get(f) is not None) for f in fields}
    out["steps"]["field_coverage"] = {"n_rows": len(rows), **cov}
    print(f"[3] coverage over {len(rows)} rows: " +
          " ".join(f"{f}={cov[f]}" for f in fields))
    sample = [{k: x.get(k) for k in ("ticker", "flows_1m", "flows_1m_pct", "flows_1y", "aum")}
              for x in ti[:3] + to[:3]]
    out["steps"]["sample"] = sample
    for r in sample:
        print("    ", r)
    # intensity-board sanity: entries with pct + AUM>=500M
    n_int = sum(1 for x in rows if x.get("flows_1m_pct") is not None and (x.get("aum") or 0) >= 5e8)
    out["steps"]["intensity_eligible"] = n_int
    print(f"    intensity-eligible (pct + aum>=500M): {n_int}")

    # ── [4] BARE live-page verify: new markers served to real clients ──
    markers = ["jh-fl-tgl", "Most intense flows", "jh-flmore"]
    live = {}
    for i in range(30):  # ~5 min for pages.yml + CDN
        try:
            req = urllib.request.Request(PAGE, headers={"User-Agent": "Mozilla/5.0 ops3341",
                                                        "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
            live = {m: (m in body) for m in markers}
            if all(live.values()):
                break
        except Exception as e:
            live = {"err": str(e)}
        time.sleep(10)
    out["steps"]["live_page"] = live
    print(f"[4] live page markers: {live}")

    ok = fresh and all(cov[f] > 0 for f in ("flows_1m", "flows_1m_pct", "name", "aum")) \
        and isinstance(live, dict) and all(live.get(m) for m in markers)
    out["ok"] = ok
    from pathlib import Path
    import os
    rep = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws/ops/reports/3341.json"
    rep.parent.mkdir(parents=True, exist_ok=True)
    rep.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"deploy={landed} fresh={fresh} flows_1y={has_1y} "
                             f"coverage(1m/pct/name)={cov['flows_1m']}/{cov['flows_1m_pct']}/{cov['name']} "
                             f"live={live}")
    print("VERDICT:", "PASS" if ok else "PARTIAL — see steps")

sys.exit(0)
