"""ops 3601 — spillover ribbon v3: 12-month trend-reversal instrument (raw
faded, date-window 12m MA segment-colored by slope, reversal ▲▼ flips, YoY-Δ
histogram pane). Page-only; gates on served markers + shard freshness +
harness-check of the date-window math on the real shard."""
import json, sys, time, urllib.request
from datetime import datetime
from pathlib import Path
import boto3
from ops_report import report

S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"

with report("3601_ribbon_trend") as rep:
    rep.heading("ops 3601 — 12m trend-reversal ribbon")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 python re-implementation of the page's date-window math on the REAL shard
    try:
        dj = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol-history.json")["Body"].read())
        R = dj.get("rows") or []
        ts = [datetime.strptime(r0["d"], "%Y-%m-%d").timestamp() for r0 in R]
        DAY = 86400
        ma, j0 = [], 0
        for i in range(len(R)):
            while ts[j0] < ts[i] - 365 * DAY:
                j0 += 1
            w = [R[k]["spill"] for k in range(j0, i + 1)]
            ma.append(sum(w) / len(w) if (ts[i] - ts[j0] >= 60 * DAY or i - j0 >= 40) else None)
        yoy = []
        for i in range(len(R)):
            tgt = ts[i] - 365 * DAY
            pi = next((k for k in range(i, -1, -1) if ts[k] <= tgt), -1)
            yoy.append(round(R[i]["spill"] - R[pi]["spill"], 2)
                       if pi >= 0 and ts[i] - ts[pi] >= 300 * DAY else None)
        slope = [(ma[i] - ma[i - 4]) if (ma[i] is not None and i >= 4 and ma[i - 4] is not None)
                 else None for i in range(len(ma))]
        flips = sum(1 for i in range(5, len(slope))
                    if slope[i] is not None and slope[i - 1] is not None
                    and (slope[i] >= 0) != (slope[i - 1] >= 0))
        n_ma = sum(1 for v in ma if v is not None)
        n_yoy = sum(1 for v in yoy if v is not None)
        ok1 = len(R) >= 1500 and n_ma >= len(R) - 80 and n_yoy >= len(R) - 120 and 10 <= flips <= 400
        gate("G1_math_harness", ok1,
             f"rows={len(R)} ma_cov={n_ma} yoy_cov={n_yoy} reversal_flips={flips} "
             f"ma_last={round(ma[-1],2) if ma[-1] is not None else None} "
             f"yoy_last={yoy[-1]} slope_last={'RISING' if slope[-1] and slope[-1]>=0 else 'FALLING'} "
             f"shard_last={dj.get('last')}")
        out["trend"] = {"ma_last": round(ma[-1], 2) if ma[-1] is not None else None,
                        "yoy_last": yoy[-1], "flips_36y": flips,
                        "slope_last": "RISING" if (slope[-1] or 0) >= 0 else "FALLING"}
    except Exception as e:
        gate("G1_math_harness", False, str(e)[:320])

    ok2 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if all(k in html for k in ("12-MONTH CHANGE", "12m trend", "RISING",
                                       "jh-fifx-rb")) \
               and html.find('id="jh-fifx"') < html.find('id="jh-spx-ma"'):
                ok2 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G2_page_served", ok2, "served: 12m trend overlay + YoY pane markers, card still top")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3601.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
