"""ops 3343 — sectors.html flow-intel arc (out-of-the-box upgrades).
Ships with this push: [engine finviz-universe] _ef += perf_m/perf_ytd;
NEW feed keys complex_nets (12 wrapper complexes — gross in/out/net 1M,
net YTD, member chips), type_flows (asset-class rollup over all ranked
ETFs), sector_breadth (per-GICS stock internals: % >200DMA, % >50DMA,
median 1M). [page] five additive sections: flow–price quadrant SVG map
(SPDRs, same-window 1M pair), wrapper-wars complex-net card, asset-class
rollup bars, whales-vs-crowd (13F Q net + whale net vs Finviz 1M crowd
flow, divergence tags), sector internals strip joined to SPDR flows.
Verify (3342 race-safe pattern): wait LastUpdateStatus=Successful +
deployed-zip marker → refire → assert new keys populated with sane
values → assert 13F map carries the SPDRs → bare-URL page markers."""
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
PAGE = "https://justhodl.ai/sectors.html"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))

with report("3343_flow_intel") as R:
    out = {}

    # [1] settled + deployed marker (never invoke a mid-update function)
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if "complex_nets" in src and '"perf_m": r.get("perf_m")' in src:
                break
        time.sleep(8)
    else:
        R.fail("deployed code never showed complex_nets marker")
        raise SystemExit(1)
    out["deployed_marker"] = True
    print("[1] settled + deployed zip carries complex_nets/perf_m")

    # [2] refire + poll for the new keys
    before = json.loads(s3.get_object(Bucket=BUCKET, Key=FEED)["Body"].read())
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    print("[2] refired; prior gen", before.get("generated_at"))
    d = None
    for i in range(90):
        time.sleep(6)
        try:
            cand = json.loads(s3.get_object(Bucket=BUCKET, Key=FEED)["Body"].read())
        except Exception:
            continue
        if cand.get("generated_at") != before.get("generated_at") and cand.get("complex_nets"):
            d = cand
            break
    if not d:
        R.fail("feed never refreshed with complex_nets")
        raise SystemExit(1)

    cx = d.get("complex_nets") or []
    tf = d.get("type_flows") or []
    sb = d.get("sector_breadth") or []
    secs = d.get("sector_etfs") or []
    n_perf = sum(1 for x in secs if x.get("perf_m") is not None)
    sp = next((c for c in cx if c["complex"] == "S&P 500"), {})
    out["feed"] = {
        "generated_at": d.get("generated_at"),
        "n_complexes": len(cx), "n_types": len(tf), "n_breadth_sectors": len(sb),
        "sector_perf_m_nonnull": f"{n_perf}/{len(secs)}",
        "sp500_complex": {k: sp.get(k) for k in ("net_1m", "gross_in_1m", "gross_out_1m", "net_ytd", "n")},
        "top_type": tf[0] if tf else None,
        "breadth_sample": sb[:2],
    }
    print("[2] feed:", json.dumps(out["feed"], default=str)[:900])

    # [3] 13F map carries the SPDRs (whales-vs-crowd join source)
    wt = json.loads(s3.get_object(Bucket=BUCKET, Key="data/13f-flows-by-ticker.json")["Body"].read()).get("t", {})
    spdrs = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC"]
    hit = {t: ({"n": wt[t].get("n"), "wn": wt[t].get("wn")} if t in wt else None) for t in spdrs}
    n_hit = sum(1 for v in hit.values() if v)
    out["whale_join"] = {"spdrs_in_13f": n_hit, "sample": {k: v for k, v in list(hit.items())[:4]}}
    print(f"[3] 13F join: {n_hit}/11 SPDRs present; sample {out['whale_join']['sample']}")

    # [4] bare-URL live page markers
    markers = ["quad-map", "complex-nets", "type-flows", "whale-crowd", "sec-breadth",
               "loadFlowIntel", "STEALTH ACCUMULATION"]
    live = {}
    for i in range(30):
        try:
            req = urllib.request.Request(PAGE, headers={"User-Agent": "Mozilla/5.0 ops3343",
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

    ok = (len(cx) >= 8 and len(tf) >= 4 and len(sb) >= 8 and n_perf >= 9
          and n_hit >= 8 and isinstance(live, dict) and all(live.get(m) for m in markers))
    out["ok"] = ok
    from pathlib import Path
    import os
    rep = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws/ops/reports/3343.json"
    rep.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"cx={len(cx)} types={len(tf)} breadth={len(sb)} perf={n_perf}/11 "
                             f"13F={n_hit}/11 live_all={all(live.get(m) for m in markers) if isinstance(live, dict) else False}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
