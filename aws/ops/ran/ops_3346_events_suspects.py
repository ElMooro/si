"""ops 3346 — closes the 3344/45 arc. This push: [etf-fund-flows]
event study restructured for Polygon's sparse NAV (flow z runs on the
full ~120d flow series; NAV guarded per event; cooldown on detection;
benchmark ladder >=55 NAV days) — probe showed SPY/IVV/VOO/QQQ carry
only 67-74 NAV days, which is why 3345 still returned zero events.
[finviz-universe] _ef gains suspect flag (|1M flow| > 150% AUM =
share-class conversion artifact — MUU +$19.5B on $5.0B AUM is the
live example, cross-confirmed by Polygon showing MUU $113.8B/5d).
[page] tornado rows render ⚠ + auto footnote; intensity board
excludes suspects. Verify: dual race-safe refire, gate on n_events,
MUU tagged, live markers."""
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
PAGE = "https://justhodl.ai/sectors.html"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, retries={"max_attempts": 0}))


def _j(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def _wait_marker(fn, markers, tries=45):
    for i in range(tries):
        st = lam.get_function_configuration(FunctionName=fn)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if all(m in src for m in markers):
                return True
        time.sleep(8)
    return False


with report("3346_events_suspects") as R:
    out = {}

    # ── [A] etf-fund-flows: refire + events ──
    if not _wait_marker("justhodl-etf-fund-flows", ["cooldown on detection", ">=55 NAV days"]):
        R.fail("etf-fund-flows deploy marker missing")
        raise SystemExit(1)
    try:
        g0 = _j("etf-flows/event-study.json").get("generated_at")
    except Exception:
        g0 = None
    lam.invoke(FunctionName="justhodl-etf-fund-flows", InvocationType="Event", Payload=b"{}")
    es = None
    for i in range(80):
        time.sleep(6)
        try:
            c = _j("etf-flows/event-study.json")
        except Exception:
            continue
        if c.get("generated_at") != g0 and c.get("event_study"):
            es = c
            break
    if not es:
        R.fail("event-study never refreshed")
        raise SystemExit(1)
    st_ = es["event_study"]
    out["event_study"] = {k: st_.get(k) for k in ("benchmark", "n_events", "overall", "by_dir",
                                                  "by_quadrant", "smart_money", "retail_favored",
                                                  "error", "probe")}
    print("[A] study:", json.dumps(out["event_study"], default=str)[:1200])
    print("    top3:", json.dumps((st_.get("top_events") or [])[:3], default=str))
    la = ((_j("etf-flows/composite.json").get("composite") or {}).get("leveraged_appetite")) or {}
    out["lev"] = {k: la.get(k) for k in ("read", "bull_5d_usd", "bear_5d_usd", "net_5d_usd",
                                         "n_suspect_excluded")}
    print("[A] lev:", out["lev"])

    # ── [B] finviz-universe: refire + suspect tag ──
    if not _wait_marker("justhodl-finviz-universe", ['"suspect": bool']):
        R.fail("finviz-universe deploy marker missing")
        raise SystemExit(1)
    g1 = _j("data/finviz-etf-flows.json").get("generated_at")
    lam.invoke(FunctionName="justhodl-finviz-universe", InvocationType="Event", Payload=b"{}")
    fz = None
    for i in range(90):
        time.sleep(6)
        try:
            c = _j("data/finviz-etf-flows.json")
        except Exception:
            continue
        if c.get("generated_at") != g1 and any("suspect" in x for x in (c.get("top_inflows") or [])[:1]):
            fz = c
            break
    if not fz:
        R.fail("finviz feed never refreshed with suspect field")
        raise SystemExit(1)
    sus = [x["ticker"] for x in (fz.get("top_inflows") or []) + (fz.get("top_outflows") or [])
           if x.get("suspect")]
    muu = next((x for x in fz.get("top_inflows") or [] if x["ticker"] == "MUU"), None)
    out["finviz"] = {"suspect_tickers": sus,
                     "muu": ({k: muu.get(k) for k in ("flows_1m", "aum", "suspect")} if muu else None)}
    print("[B] suspects:", out["finviz"])

    # ── [C] live page markers ──
    markers = ["jh-suspect-note", "conversion / merger artifact", "cursor:help"]
    live = {}
    for i in range(30):
        try:
            req = urllib.request.Request(PAGE, headers={"User-Agent": "Mozilla/5.0 ops3346",
                                                        "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
            live = {m: (m in body) for m in markers}
            if all(live.values()):
                break
        except Exception as e:
            live = {"err": str(e)}
        time.sleep(10)
    out["live_page"] = live
    print("[C] live:", live)

    n_ev = st_.get("n_events") or 0
    ok = (n_ev >= 10 and st_.get("benchmark") in ("SPY", "IVV", "VOO", "QQQ")
          and ("MUU" in sus if muu else True)
          and isinstance(live, dict) and all(live.get(m) for m in markers))
    out["ok"] = ok
    from pathlib import Path
    import os
    Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()), "aws/ops/reports/3346.json") \
        .write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"bench={st_.get('benchmark')} events={n_ev} "
                             f"suspects={sus} live_all={all(live.get(m) for m in markers) if isinstance(live, dict) else False}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
