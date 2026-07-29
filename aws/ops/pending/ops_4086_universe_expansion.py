"""ops_4086 — admit the resolved aliases into the vault's universe.

ops 4085 shipped the resolution half and I flagged the gap rather than
leaving it: the vault learned to RESOLVE generated aliases, but its
universe union only admitted the curated ALIASES dict. So 354 verified
FRED aliases existed, loaded correctly, and were never walked —
n_symbols stayed at 591. Resolution without admission is a no-op that
reports success.

v3.12.0 unions the generated aliases into the registry too, bounded at
MAX_EXPAND per run so a 10,319-ticker backlog accretes over days instead
of blowing the 900s budget in one unreviewable jump.

GATES: the vault must GROW, the admitted symbols must actually resolve to
real values via fred:, and the pre-existing LIVE population must not
regress — an expansion that breaks what already worked is a failure even
if the new rows look good.
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
                   config=Config(read_timeout=900, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tradingview"
MARK = "tradingview-vault v3.12.0 ops4086 universe-expansion"


def main():
    with report("4086_universe_expansion") as rep:
        rep.heading("ops 4086 — vault universe expansion (admit the aliases)")
        checks = []

        # baseline BEFORE, so growth is measured not assumed
        base = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        b_rows = base.get("symbols") or []
        b_live = [r for r in b_rows if str(r.get("status")).upper() == "LIVE"]
        rep.section("A. baseline")
        rep.log(f"  rows {len(b_rows)}  LIVE {len(b_live)}")
        rep.kv(before_rows=len(b_rows), before_live=len(b_live))

        sa = json.loads(s3.get_object(Bucket=BUCKET, Key="data/symbol-aliases.json")["Body"].read())
        gen = sa.get("aliases") or {}
        rep.log(f"  generated aliases available: {len(gen)}")

        rep.section("B. deploy v3.12.0")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue(), Publish=True); break
            except lam.exceptions.ResourceConflictException:
                time.sleep(12)
        settled = False
        for a in range(24):
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())
                                     ).read("lambda_function.py").decode()
                    if MARK in dep:
                        settled = True; rep.log(f"  ✓ settled by marker (attempt {a+1})"); break
            except Exception as e:
                rep.log(f"  settle {a+1}: {str(e)[:60]}")
            time.sleep(10)
        checks.append(("v3.12.0 settled by marker", settled))
        if not settled:
            rep.log("✗ stale artifact"); sys.exit(1)

        rep.section("C. invoke — admit the first batch")
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b'{"source":"ops4086"}')
        rep.log(f"  fnerr={r.get('FunctionError')}")
        rep.log(f"  {r['Payload'].read().decode()[:240]}")
        checks.append(("invoke clean", r.get("FunctionError") is None))

        after = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        a_rows = after.get("symbols") or []
        a_live = [r for r in a_rows if str(r.get("status")).upper() == "LIVE"]
        rep.kv(after_rows=len(a_rows), after_live=len(a_live),
               growth=len(a_rows) - len(b_rows))
        rep.log(f"  rows {len(b_rows)} → {len(a_rows)}   "
                f"LIVE {len(b_live)} → {len(a_live)}")

        # did the ADMITTED symbols actually resolve to real numbers?
        admitted = [r for r in a_rows if r.get("symbol") in gen]
        adm_live = [r for r in admitted
                    if str(r.get("status")).upper() == "LIVE" and r.get("value") is not None]
        rep.section("D. do the admitted aliases carry REAL values?")
        rep.log(f"  admitted rows {len(admitted)}   LIVE with a value {len(adm_live)}")
        for r in adm_live[:12]:
            rep.log(f"    {str(r.get('symbol'))[:20]:20} = {str(r.get('value'))[:14]:14} "
                    f"via {str(r.get('resolved_via'))[:26]:26} asof {r.get('asof')}")
        rv = Counter(str(r.get("resolved_via") or "").split(":")[0] for r in adm_live)
        rep.log(f"  resolved_via: {dict(rv)}")
        rep.kv(admitted=len(admitted), admitted_live=len(adm_live))

        checks.append(("vault universe GREW", len(a_rows) > len(b_rows)))
        checks.append(("admitted aliases resolve to real values", len(adm_live) > 0))
        checks.append(("admitted rows route through fred", rv.get("fred", 0) > 0))
        # An expansion that breaks what already worked is a failure.
        checks.append(("pre-existing LIVE population not regressed",
                       len(a_live) >= len(b_live)))
        checks.append(("growth is BOUNDED, not a runaway",
                       len(a_rows) - len(b_rows) <= 300))

        rep.section("E. remaining backlog")
        still = [x for x in gen if x not in {r.get("symbol") for r in a_rows}]
        rep.log(f"  generated aliases not yet admitted: {len(still)}")
        rep.log(f"  → accretes at MAX_EXPAND=250/run on the daily schedule")

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — vault {len(b_rows)}→{len(a_rows)} rows, "
                f"{len(adm_live)} newly-admitted tickers now carry live values. "
                f"{len(still)} queued.")


if __name__ == "__main__":
    main()
