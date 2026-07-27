"""
ops_3955 — JPLG COMES HOME: vault v3.6 boj adapter (official BOJ API,
DLCLAADBLTTO avg-outstanding loans, YoY from last non-null month vs -12) +
risk-gate v2.3 carry leg jplg_loan_growth_yoy (flash-warning rule, brain-
cited). Gates: both settle by STRING in zip; vault force -> JPLG LIVE via
bank-of-japan, yoy in [-10,15], asof 2026; gate invoke -> carry leg carries
jplg input OK numeric; n_live >= 454; zero UNRESOLVED; posture valid.
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))


def settle(fn, marks):
    for attempt in range(1, 41):
        try:
            loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
            blob = urllib.request.urlopen(loc, timeout=60).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if all(m in src for m in marks):
                    return attempt
        except Exception:
            pass
        time.sleep(15)
    return None


def wait_active(fn):
    for _ in range(25):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return
        time.sleep(8)


def get_doc(key):
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())


def main():
    with report("3955_jplg_wire") as rep:
        rep.heading("ops 3955 — JPLG comes home (vault v3.6 + gate v2.3)")
        checks = []
        a1 = settle("justhodl-tradingview",
                    ("tradingview-vault v3.6 BOJ-JPLG", '"JPLG": "boj:'))
        a2 = settle("justhodl-risk-gate",
                    ("risk-gate v2.3", "jplg_loan_growth_yoy"))
        (rep.ok if a1 else rep.fail)(f"  vault settled: {a1}")
        (rep.ok if a2 else rep.fail)(f"  gate settled: {a2}")
        checks += [("vault v3.6 settled", bool(a1)), ("gate v2.3 settled", bool(a2))]
        if not (a1 and a2):
            rep.fail("settle failed"); sys.exit(1)
        wait_active("justhodl-tradingview"); wait_active("justhodl-risk-gate")

        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName="justhodl-tradingview", InvocationType="Event",
                   Payload=json.dumps({"force": True}).encode())
        doc = None
        for i in range(60):
            time.sleep(15)
            d = get_doc("data/tradingview.json")
            if d.get("generated_at", "") > t_mark:
                doc = d; rep.ok(f"  vault refreshed ~{(i+1)*15}s"); break
        checks.append(("vault force wrote", doc is not None))
        if not doc: rep.fail("vault never wrote"); sys.exit(1)
        st_ = doc.get("status_counts") or {}
        jr = next((r for r in doc.get("symbols") or [] if r.get("symbol") == "JPLG"), {})
        rep.log(f"  JPLG: {jr.get('status')} value={jr.get('value')} prev={jr.get('prev')} "
                f"src={jr.get('source')} asof={jr.get('asof')}")
        checks.append(("JPLG LIVE via bank-of-japan",
                       jr.get("status") == "LIVE" and "bank-of-japan" in str(jr.get("source"))))
        checks.append(("JPLG yoy plausible [-10,15]",
                       isinstance(jr.get("value"), (int, float)) and -10 <= jr["value"] <= 15))
        checks.append(("JPLG asof 2026", "2026" in str(jr.get("asof"))))
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_))
        checks.append(("n_live >= 454", (doc.get("n_live") or 0) >= 454))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))

        r = lam.invoke(FunctionName="justhodl-risk-gate",
                       InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"gate error: {json.loads(r['Payload'].read())}"); sys.exit(1)
        gd = get_doc("data/risk-gate.json")
        ci = {x["input"]: x for x in (gd.get("legs", {}).get("carry", {})
                                      .get("fleet_inputs") or [])}
        jp = ci.get("jplg_loan_growth_yoy") or {}
        rep.log(f"  gate jplg: {jp.get('status')} value={jp.get('value')} adj={jp.get('score_adj')}")
        rep.kv(posture=gd.get("posture"), composite=gd.get("composite"))
        checks.append(("gate carry leg carries JPLG OK",
                       jp.get("status") == "OK" and isinstance(jp.get("value"), (int, float))))
        checks.append(("posture valid", gd.get("posture") in
                       ("RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE")))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — JPLG {jr.get('value')}% ({jr.get('asof')}) LIVE from the "
               f"Bank of Japan and firing in the risk-gate carry leg "
               f"(adj {jp.get('score_adj')}); vault {doc.get('n_live')} LIVE")


if __name__ == "__main__":
    main()
