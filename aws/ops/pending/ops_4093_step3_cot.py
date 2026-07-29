"""ops_4093 — STEP 3: route the 340 COT/COT3 tickers to real CFTC data.

ops 4091 killed my assumption that this was a wiring job: the fleet's
cftc-all-cache holds 29 short-name contracts and shares ZERO keys with
these tickers. ops 4092 probed the CFTC's own Socrata publication and
found the TFF datasets carry exactly the taxonomy the ticker suffixes
encode (dealer / asset_mgr / lev_money / other_rept, each long/short/
spread).

So this ships two halves: the resolver decodes a suffix to a column and
VERIFIES it against a live row before aliasing, and the vault gains a
cftc: adapter that can fetch it. Any suffix without a matching, populated
column leaves its ticker unrouted — an alias that resolves to nothing is
worse than an honest gap.
"""
import io, json, sys, time, urllib.request, zipfile as zf
from collections import Counter
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"


def deploy(rep, fn, mark):
    src = (ROOT / "lambdas" / fn / "source" / "lambda_function.py").read_text()
    assert mark in src, f"{fn}: marker parity broken ({mark!r})"
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue(), Publish=True); break
        except lam.exceptions.ResourceConflictException:
            time.sleep(12)
    for a in range(24):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())
                                 ).read("lambda_function.py").decode()
                if mark in dep:
                    rep.log(f"  ✓ {fn} settled (attempt {a+1})"); return True
        except Exception as e:
            rep.log(f"  settle {a+1}: {str(e)[:55]}")
        time.sleep(10)
    return False


def poll(key, before_gen, rep, tries=42):
    for a in range(tries):
        time.sleep(20)
        try:
            cur = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            if cur.get("generated_at") != before_gen:
                rep.log(f"  ✓ {key} moved after {(a+1)*20}s"); return cur
        except Exception:
            pass
    return None


def main():
    with report("4093_step3_cot") as rep:
        rep.heading("ops 4093 — STEP 3: COT/COT3 → CFTC TFF, verified")
        checks = []

        rep.section("A. deploy resolver v3.0 + vault v3.13.0")
        ok1 = deploy(rep, "justhodl-symbol-resolver", "symbol-resolver v3.0 ops4093 step3-cot")
        ok2 = deploy(rep, "justhodl-tradingview", "tradingview-vault v3.13.0 ops4093 cftc-adapter")
        checks += [("resolver v3.0 settled", ok1), ("vault v3.13.0 settled", ok2)]
        if not (ok1 and ok2):
            rep.log("✗ stale artifact"); sys.exit(1)

        rep.section("B. resolve COT tickers (async + poll)")
        before = json.loads(s3.get_object(Bucket=BUCKET, Key="data/symbol-aliases.json")["Body"].read())
        r = lam.invoke(FunctionName="justhodl-symbol-resolver",
                       InvocationType="Event", Payload=b'{"source":"ops4093"}')
        checks.append(("resolver async accepted", r["StatusCode"] == 202))
        sa = poll("data/symbol-aliases.json", before.get("generated_at"), rep)
        if sa is None:
            rep.log("✗ resolver artifact never moved"); sys.exit(1)

        rep.kv(cot_total=sa.get("cot_total"), cot_aliased=sa.get("cot_aliased"),
               cot_skipped=sa.get("cot_skipped_this_run"),
               n_aliases=sa.get("n_aliases"))
        rep.log(f"  COT tickers {sa.get('cot_total')} · aliased "
                f"{sa.get('cot_aliased')} · skipped {sa.get('cot_skipped_this_run')}")
        rep.log(f"  by_route: {sa.get('by_route')}")
        det = sa.get("detail") or {}
        cot = [(k, v) for k, v in det.items() if v.get("route") == "cot-verified"]
        rep.section("Verified COT aliases (sample)")
        for k, v in cot[:14]:
            rep.log(f"  {k[:26]:26} → {v['alias'][:44]:44} asof {v.get('asof')}")
        checks.append(("COT aliases produced", len(cot) > 0))
        checks.append(("every COT alias points at a real CFTC column",
                       all(v["alias"].startswith("cftc:") and v["alias"].count(":") == 3
                           for _, v in cot)))
        checks.append(("every COT alias was verified against a live row",
                       all(v.get("asof") and v.get("confidence") == 1.0 for _, v in cot)))

        rep.section("C. vault fetches them for real")
        bv = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        b_live = len([x for x in (bv.get("symbols") or []) if str(x.get("status")).upper() == "LIVE"])
        r2 = lam.invoke(FunctionName="justhodl-tradingview",
                        InvocationType="Event", Payload=b'{"source":"ops4093"}')
        checks.append(("vault async accepted", r2["StatusCode"] == 202))
        av = poll("data/tradingview.json", bv.get("generated_at"), rep)
        if av is None:
            rep.log("✗ vault artifact never moved"); sys.exit(1)
        rows = av.get("symbols") or []
        live = [x for x in rows if str(x.get("status")).upper() == "LIVE"]
        cft = [x for x in live if str(x.get("resolved_via") or "").startswith("cftc:")
               and x.get("value") is not None]
        rep.kv(vault_rows=len(rows), vault_live=len(live), cftc_live=len(cft))
        rep.log(f"  vault rows {len(bv.get('symbols') or [])} → {len(rows)} · "
                f"LIVE {b_live} → {len(live)}")
        rep.section("CFTC-routed symbols carrying real values")
        for x in cft[:12]:
            rep.log(f"  {str(x.get('symbol'))[:26]:26} = {str(x.get('value'))[:12]:12} "
                    f"asof {x.get('asof')}")
        checks.append(("cftc: adapter returns real values", len(cft) > 0))
        checks.append(("pre-existing LIVE not regressed", len(live) >= b_live))

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — {sa.get('cot_aliased')} COT tickers verified against "
                f"the CFTC's own publication; {len(cft)} already pulling live in "
                f"the vault. Total aliases {sa.get('n_aliases')}.")


if __name__ == "__main__":
    main()
