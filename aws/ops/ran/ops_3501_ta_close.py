"""ops 3501 — TA close: block relocated after P assembly (NameError: P
was initialized 217 lines below the compute site; T2's live gate caught
it in one cycle). v1.7.1 regate, tech.error surfaced if any remains.

  Z2 AAPL live v17: same consistency battery as 3500-T2 + tech.error
     printed on failure
"""
import json, sys, time, urllib.request
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")

with report("3501_ta_close") as rep:
    out = {"ops": 3501, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:460]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3501 — TA relocation close (v1.7.1)")
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.7.1 TA relocate (ops 3501)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL"], "periods": ["quarter"], "refresh": True}).encode())
    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v17.json")["Body"].read())
        te = doc.get("tech") or {}
        st = te.get("status") or {}
        ev = te.get("events") or []
        p200 = (doc.get("points") or {}).get("px_ma200") or []
        cons = st.get("above_200") == (st.get("last_close", 0)
                                       > st.get("ma200", 9e9))
        band = (st.get("ma20") is not None
                and (doc["points"].get("px_bb_up") or [[0, 0]])[-1][1]
                > st["ma20"]
                > (doc["points"].get("px_bb_dn") or [[0, 9e9]])[-1][1])
        gate("Z2_aapl_live",
             band and cons and 0 < (st.get("rsi14") or -1) < 100
             and abs(st.get("last_close", 0) / (st.get("ma200") or 1) - 1)
             < 0.4 and len(ev) >= 1 and ev == sorted(ev)
             and len(p200) >= 100,
             {"version": doc.get("version"),
              "tech_error": te.get("error"),
              "status": {k: st.get(k) for k in
                         ("last_close", "ma200", "pct_vs_200", "above_200",
                          "bull_stack", "rsi14", "bb_pos", "last_cross")},
              "n_events": len(ev), "n_px_ma200": len(p200),
              "recent_events": ev[-4:],
              "patterns": st.get("patterns")})
    except Exception as e:
        gate("Z2_aapl_live", False, str(e)[:320])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3501.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
