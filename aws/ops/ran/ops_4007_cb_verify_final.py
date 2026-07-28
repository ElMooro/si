"""
ops_4007 — verify the central-bank expansion end to end: vault symbols LIVE
with sane values, barometers classify+sign them, risk-gate carry cites the
new JGB-10Y input.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
NEW = {"JP01Y": (0.3, 4), "JP05Y": (0.5, 5), "JP10Y": (1.0, 6),
       "JP20Y": (1.5, 7), "JP30Y": (1.5, 8), "NOINTR": (1.5, 8),
       "NO10Y": (1.5, 8), "PEINTR": (1.0, 10), "PENUSD": (2.0, 5.5),
       "PEFER": (100000, 900000)}


def settle(fn, mark, srcpath, rep):
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                lam.get_function(FunctionName=fn)["Code"]["Location"],
                timeout=60).read())).read("lambda_function.py").decode()
            if mark in dep:
                return True
            src = (ROOT / "lambdas" / fn / "source" / "lambda_function.py").read_text()
            buf = io.BytesIO()
            with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", src)
            try:
                lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue(),
                                         Publish=True)
                rep.log(f"  {fn}: pushed from runner")
            except lam.exceptions.ResourceConflictException:
                pass
        time.sleep(10)
    return False


def main():
    with report("4007_cb_verify_final") as rep:
        rep.heading("ops 4004 — CB expansion pipeline verify")
        checks = []

        rep.section("A. vault: poll for v3.9 write, then symbol sanity")
        base = json.loads(s3.get_object(Bucket=BUCKET,
                                        Key="data/tradingview.json")["Body"].read())
        g0 = base.get("generated_at")
        doc = base
        def live_count(d):
            ix = {r["symbol"]: r for r in d.get("symbols") or []}
            return sum(1 for s2, (lo, hi) in NEW.items()
                       if (ix.get(s2) or {}).get("status") == "LIVE"
                       and isinstance((ix.get(s2) or {}).get("value"), (int, float))
                       and lo <= ix[s2]["value"] <= hi)
        for i in range(20):
            doc = json.loads(s3.get_object(Bucket=BUCKET,
                                           Key="data/tradingview.json")["Body"].read())
            if "v3.9" in str(doc.get("marker")) and live_count(doc) >= 8:
                rep.ok(f"  v3.9 with >=8 new LIVE after ~{i*30}s")
                break
            if i % 4 == 3:
                rep.log(f"  [{i}] marker={str(doc.get('marker'))[:28]} "
                        f"new_live={live_count(doc)}")
            time.sleep(30)
        rep.kv(marker=doc.get("marker"), n_live=doc.get("n_live"),
               generated_at=doc.get("generated_at"))
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}
        live_n = 0
        for sym, (lo, hi) in NEW.items():
            r = idx.get(sym) or {}
            v = r.get("value")
            ok = r.get("status") == "LIVE" and isinstance(v, (int, float)) \
                and lo <= v <= hi
            live_n += bool(ok)
            rep.log(f"  {sym:7s} {str(r.get('status')):14s} v={v} "
                    f"src={str(r.get('source'))[:16]} sane={ok}")
        checks.append(("vault marker v3.9", "v3.9" in str(doc.get("marker"))))
        checks.append((">=8 of 10 new symbols LIVE and sane", live_n >= 8))
        curve = [idx.get(s, {}).get("value") for s in
                 ("JP01Y", "JP05Y", "JP10Y", "JP20Y", "JP30Y")]
        mono = all(isinstance(x, (int, float)) for x in curve) and \
            all(curve[i] <= curve[i + 1] + 0.05 for i in range(4))
        rep.kv(jgb_curve=curve, monotone=mono)
        checks.append(("JGB curve monotone (sanity)", mono))

        rep.section("B. barometers: classify + sign the new symbols")
        ok = settle("justhodl-domain-barometers",
                    "domain-barometers v1.3 ops4003 cb-polarities",
                    None, rep)
        checks.append(("barometers v1.3 settled", ok))
        r = lam.invoke(FunctionName="justhodl-domain-barometers",
                       InvocationType="RequestResponse",
                       Payload=json.dumps({"source": "ops4004"}).encode())
        rep.log(f"  invoke fnerr={r.get('FunctionError')}")
        checks.append(("barometers invoke clean", not r.get("FunctionError")))
        bar = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/domain-barometers.json")["Body"].read())
        bidx = {x["symbol"]: x for x in bar.get("symbols") or []}
        pol_ok = 0
        for sym in NEW:
            x = bidx.get(sym) or {}
            good = bool(x.get("polarity")) and x.get("polarity_basis") == "brain_note"
            pol_ok += good
            rep.log(f"  {sym:7s} dom={str(x.get('domain')):9s} pol={x.get('polarity')} "
                    f"basis={x.get('polarity_basis')} status={x.get('status')}")
        checks.append((">=8 new symbols carry a brain-note polarity", pol_ok >= 8))
        for d in ("MACRO", "LIQUIDITY", "RISK"):
            b = (bar.get("barometers") or {}).get(d) or {}
            rep.log(f"  {d:9s} {b.get('score_0_100')} {b.get('state')} "
                    f"voting={((b.get('coverage') or {}).get('voting'))}")

        rep.section("C. risk-gate: carry leg cites JGB 10Y")
        ok = settle("justhodl-risk-gate", "ops4003 jp10y-carry", None, rep)
        checks.append(("risk-gate jp10y patch settled", ok))
        r = lam.invoke(FunctionName="justhodl-risk-gate",
                       InvocationType="RequestResponse",
                       Payload=json.dumps({"source": "ops4004"}).encode())
        rep.log(f"  invoke fnerr={r.get('FunctionError')}")
        checks.append(("risk-gate invoke clean", not r.get("FunctionError")))
        gate = json.loads(s3.get_object(Bucket=BUCKET,
                                        Key="data/risk-gate.json")["Body"].read())
        gj = json.dumps(gate)
        has = "jgb10y_carry_cost" in gj
        i2 = gj.find("jgb10y_carry_cost")
        names = gj[max(0, i2 - 40):i2 + 220] if has else gj[:200]
        rep.log(f"  posture={gate.get('posture')} sizing={gate.get('sizing_multiplier')}")
        rep.log(f"  carry inputs head: {names[:260]}")
        checks.append(("carry leg includes jgb10y_carry_cost", has))
        checks.append(("posture valid", gate.get("posture") in
                       ("RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE")))

        failed = [l for l, ok2 in checks if not ok2]
        for l, ok2 in checks:
            (rep.ok if ok2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {live_n}/10 new CB indicators LIVE; barometers signed "
               f"{pol_ok}; risk-gate carry cites JGB 10Y; posture {gate.get('posture')}")


if __name__ == "__main__":
    main()

# rerun after race fix
