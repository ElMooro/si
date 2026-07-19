"""ops 3503 — V0 probe fix (FMP /stable light returns a bare LIST) +
the exact ISO-week volume reconciliation V2 skipped.
"""
import json, sys, urllib.request
from datetime import datetime, date, timedelta
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3c = boto3.client("s3", region_name="us-east-1")

with report("3503_probe_recon") as rep:
    out = {"ops": 3503, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:440]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3503 — FMP volume probe + exact week reconciliation")
    req = urllib.request.Request(
        "https://financialmodelingprep.com/stable/historical-price-eod/"
        "light?symbol=AAPL&from=%s&apikey=%s"
        % ((date.today() - timedelta(days=200)).isoformat(), FMP_KEY),
        headers={"User-Agent": "ops-3503"})
    raw = json.loads(urllib.request.urlopen(req, timeout=40).read())
    hist = raw if isinstance(raw, list) else \
        (raw.get("historical") or raw.get("data") or [])
    rows = sorted((str(r["date"])[:10], r.get("volume")) for r in hist
                  if isinstance(r, dict) and r.get("date"))
    okv = sum(1 for _, v in rows[-120:]
              if isinstance(v, (int, float)) and v > 0)
    gate("W0_probe", len(rows) >= 100 and okv >= 0.99 * 120,
         {"n": len(rows), "with_volume_120": okv, "sample": rows[-2:]})

    doc = json.loads(s3c.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/fundgraph/cache/AAPL_quarter_v18.json")["Body"].read())
    vw = doc["points"]["volume_w"]
    byweek = {}
    for d, v in rows:
        if isinstance(v, (int, float)):
            k = datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
            byweek[k] = byweek.get(k, 0.0) + v
    wk_map = {datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]: v
              for d, v in vw[-40:]}
    common = sorted(set(byweek) & set(wk_map))[1:-1]
    k = common[-2]
    gate("W1_exact_week_recon", abs(byweek[k] - wk_map[k]) < 1.0,
         {"iso_week": k, "probe_sum": byweek[k], "engine_sum": wk_map[k]})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3503.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
