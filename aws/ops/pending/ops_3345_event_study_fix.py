"""ops 3345 — 3344 fix cycle. Root causes addressed in this push:
[a] event study now uses a benchmark LADDER (SPY→IVV→VOO→QQQ, first
with >=80 NAV days) instead of assuming SPY's fetch succeeded, and the
error branch reports a per-candidate NAV-row probe; [b] leveraged
gauge drops data-suspect rows (|5d flow| > max($5B, 50% AUM) for a
leveraged fund) and reports them — $148B 'bull flow' was pollution,
not positioning; [c] page clamps pair chips to 12. This script prints
GROUND TRUTH first (SPY/IVV/VOO daily.json rows; top-10 leveraged by
|flow_5d|) so the polluter is on the record, then refires and gates on
sane magnitudes + a real event count."""
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


def _j(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


with report("3345_event_study_fix") as R:
    out = {}

    # [0] ground truth from the PREVIOUS run
    d0 = _j("etf-flows/daily.json")
    mets = d0.get("metrics") or []
    by_tk = {m.get("ticker"): m for m in mets if isinstance(m, dict)}
    out["bench_rows_prev"] = {t: {"label": (by_tk.get(t) or {}).get("signal_label"),
                                  "n_hist": (by_tk.get(t) or {}).get("n_history"),
                                  "flow_5d": (by_tk.get(t) or {}).get("flow_5d_usd")}
                              for t in ("SPY", "IVV", "VOO", "QQQ")}
    lev_prev = sorted([m for m in mets if m.get("flow_5d_usd") is not None],
                      key=lambda m: -abs(m.get("flow_5d_usd") or 0))[:10]
    out["top10_abs_flow5d_prev"] = [{"t": m.get("ticker"), "f5d": m.get("flow_5d_usd"),
                                     "aum": m.get("aum_usd"), "label": m.get("signal_label")}
                                    for m in lev_prev]
    print("[0] bench rows prev:", json.dumps(out["bench_rows_prev"], default=str))
    print("[0] top10 |flow_5d| prev:", json.dumps(out["top10_abs_flow5d_prev"], default=str))

    # [1] settled + deployed marker (ladder present)
    for i in range(45):
        st = lam.get_function_configuration(FunctionName=FN)
        if st.get("LastUpdateStatus", "Successful") == "Successful" and st.get("State") == "Active":
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())) \
                .read("lambda_function.py").decode("utf-8", "ignore")
            if 'for cand in ("SPY", "IVV", "VOO", "QQQ")' in src and "n_suspect_excluded" in src:
                break
        time.sleep(8)
    else:
        R.fail("deployed zip never showed the ladder/suspect markers")
        raise SystemExit(1)
    print("[1] deployed zip carries ladder + suspect guard")

    # [2] refire + poll
    try:
        before_gen = _j("etf-flows/event-study.json").get("generated_at")
    except Exception:
        before_gen = None
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    es = None
    for i in range(80):
        time.sleep(6)
        try:
            cand = _j("etf-flows/event-study.json")
        except Exception:
            continue
        if cand.get("generated_at") != before_gen and cand.get("event_study"):
            es = cand
            break
    if not es:
        R.fail("event-study.json never refreshed")
        raise SystemExit(1)
    st_ = es["event_study"]
    out["event_study"] = {k: st_.get(k) for k in ("benchmark", "n_events", "overall",
                                                  "by_dir", "by_quadrant",
                                                  "smart_money", "retail_favored", "error", "probe")}
    print("[2] study:", json.dumps(out["event_study"], default=str)[:1100])
    print("    top3:", json.dumps((st_.get("top_events") or [])[:3], default=str))

    # [3] leveraged gauge sanity
    la = ((_j("etf-flows/composite.json").get("composite") or {}).get("leveraged_appetite")) or {}
    out["leveraged_appetite"] = {k: la.get(k) for k in ("read", "bull_5d_usd", "bear_5d_usd",
                                                        "net_5d_usd", "n_suspect_excluded")}
    out["suspects"] = la.get("suspects")
    print("[3] lev:", json.dumps(out["leveraged_appetite"], default=str))
    print("    suspects:", json.dumps(out["suspects"], default=str))

    n_ev = st_.get("n_events") or 0
    sane = (abs(la.get("bull_5d_usd") or 0) < 2e10 and abs(la.get("bear_5d_usd") or 0) < 2e10)
    ok = n_ev >= 5 and st_.get("benchmark") in ("SPY", "IVV", "VOO", "QQQ") and sane \
        and la.get("read") in ("RISK_SEEKING", "NEUTRAL", "RISK_AVERSE")
    out["ok"] = ok
    from pathlib import Path
    import os
    rep = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws/ops/reports/3345.json"
    rep.write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    (R.ok if ok else R.warn)(f"bench={st_.get('benchmark')} events={n_ev} "
                             f"lev bull={la.get('bull_5d_usd')} bear={la.get('bear_5d_usd')} "
                             f"suspects={la.get('n_suspect_excluded')}")
    print("VERDICT:", "PASS" if ok else "PARTIAL")

sys.exit(0)
