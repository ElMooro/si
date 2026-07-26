"""
ops_3923 — DEPLOY vault v2.0 (full-coverage ladder + brain-text registry)
and risk-gate v2.2 (phase-2 wires: vault-fed real MOVE + oil front-vs-spot
backwardation). Gates: both markers settle; vault coverage: every symbol
carries a definitive state (LIVE/DISCONTINUED/NO_FREE_SOURCE — zero bare
UNRESOLVED), LIVE >= 400 of the expanded registry, spot-checks CL1!/MOVE/
SPX/UNEMPLOY live + GE1! DISCONTINUED + JPLG documented; registry grew via
brain_text origin; gate carries the two new structure inputs.
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=880, retries={"max_attempts": 0}))
MARKS = {"justhodl-tradingview": "tradingview-vault v2.0 FULL-COVERAGE",
         "justhodl-risk-gate": "risk-gate v2.2 BRAIN-CONSTITUTIONAL FLEET-FUSED"}


def settle(fn, mark):
    for attempt in range(1, 41):
        try:
            loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
            blob = urllib.request.urlopen(loc, timeout=60).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if mark in z.read("lambda_function.py").decode("utf-8", "ignore"):
                    return attempt
        except Exception:
            pass
        time.sleep(15)
    return None


def wait_active(fn):
    for _ in range(25):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return c
        time.sleep(8)
    return lam.get_function_configuration(FunctionName=fn)


def main():
    with report("3923_vault_v2_gate_v22") as rep:
        rep.heading("ops 3923 — vault v2.0 full coverage + gate v2.2 phase-2 wires")
        checks = []
        for fn, mark in MARKS.items():
            a = settle(fn, mark)
            (rep.ok if a else rep.fail)(f"  {fn} settled: {a}")
            checks.append((f"{fn} settled", bool(a)))
        if not all(k for _, k in checks):
            rep.fail("settle failed"); sys.exit(1)
        for fn in MARKS:
            wait_active(fn)

        rep.section("vault v2.0 invoke (long: FRED aliases + Yahoo ladder)")
        r = lam.invoke(FunctionName="justhodl-tradingview",
                       InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"vault FunctionError: {json.dumps(raw)[:900]}"); sys.exit(1)
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/tradingview.json")["Body"].read())
        by_status = {}
        for row in doc.get("symbols") or []:
            by_status[row.get("status")] = by_status.get(row.get("status"), 0) + 1
        n_text = sum(1 for row in doc.get("symbols") or [] if row.get("origin") == "brain_text")
        rep.kv(n_symbols=doc.get("n_symbols"), n_live=doc.get("n_live"),
               coverage_pct=doc.get("coverage_pct"), statuses=str(by_status),
               brain_text_added=n_text)
        checks.append(("zero bare UNRESOLVED (every symbol has a definitive state)",
                       by_status.get("UNRESOLVED", 0) == 0))
        checks.append(("LIVE >= 400", (doc.get("n_live") or 0) >= 400))
        checks.append(("registry grew via brain-text scan", n_text >= 10))
        idx = {row["symbol"]: row for row in doc.get("symbols") or []}
        for s_, want in (("CL1!", "LIVE"), ("MOVE", "LIVE"), ("SPX", "LIVE"),
                         ("UNEMPLOY", "LIVE"), ("GE1!", "DISCONTINUED"),
                         ("JPLG", "NO_FREE_SOURCE")):
            got = (idx.get(s_) or {}).get("status")
            val = (idx.get(s_) or {}).get("value")
            rep.log(f"  {s_}: {got} value={val} src={(idx.get(s_) or {}).get('source')}")
            checks.append((f"{s_} = {want}", got == want))

        rep.section("gate v2.2 invoke — vault wires live")
        r = lam.invoke(FunctionName="justhodl-risk-gate",
                       InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"gate FunctionError: {json.loads(r['Payload'].read())}"); sys.exit(1)
        gd = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                      Key="data/risk-gate.json")["Body"].read())
        st = {x["input"]: x for x in (gd.get("legs", {}).get("structure", {})
                                      .get("fleet_inputs") or [])}
        mv, bw = st.get("move_index") or {}, st.get("oil_backwardation_front_vs_spot_pct") or {}
        rep.log(f"  move_index: {mv.get('status')} value={mv.get('value')} adj={mv.get('score_adj')}")
        rep.log(f"  oil_backwardation: {bw.get('status')} value={bw.get('value')} adj={bw.get('score_adj')}")
        checks.append(("gate consumes vault MOVE", mv.get("status") == "OK"
                       and isinstance(mv.get("value"), (int, float))))
        checks.append(("gate oil backwardation input present", "value" in bw))
        rep.kv(gate_posture=gd.get("posture"), gate_composite=gd.get("composite"))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — vault {doc.get('n_live')}/{doc.get('n_symbols')} live "
               f"({doc.get('coverage_pct')}%), statuses {by_status}; gate v2.2 vault-fed")


if __name__ == "__main__":
    main()
