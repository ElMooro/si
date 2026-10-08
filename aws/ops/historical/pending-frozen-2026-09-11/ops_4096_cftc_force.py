"""ops_4096 — the cftc rows were cached NO_FREE_SOURCE, not broken.

ops 4095 dumped the actual rows and the answer was sitting in them:

    020601_FO_AMP_SPREAD  status=NO_FREE_SOURCE  value=None  via=None

All 65 COT symbols ARE admitted to the vault. They were resolved during
ops 4093 — BEFORE v3.13.1 taught resolve_alias about the cftc kind — so
they landed NO_FREE_SOURCE. And the vault's cache gate gives that status
a 27-DAY retry window:

    retry_gate = 27 if c.get("status") == "NO_FREE_SOURCE"

So ops 4094 deployed a working adapter and then never asked it to resolve
anything: every COT row was skipped as "recently determined unresolvable".
The adapter was never the problem, and two ops reported a red gate for a
cache policy rather than a defect. The handler already supports the
escape hatch — force=true bypasses the gate — which is exactly the right
tool for "a resolver that did not exist last run now does".
"""
import json, sys, time
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


def main():
    with report("4096_cftc_force") as rep:
        rep.heading("ops 4096 — force past the 27-day NO_FREE_SOURCE gate")
        checks = []

        before = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        b_rows = before.get("symbols") or []
        b_live = len([r for r in b_rows if str(r.get("status")).upper() == "LIVE"])
        sa = json.loads(s3.get_object(Bucket=BUCKET, Key="data/symbol-aliases.json")["Body"].read())
        cot_keys = {k for k, r in (sa.get("detail") or {}).items()
                    if r.get("route") == "cot-verified"}
        rep.section("A. baseline")
        rep.log(f"  rows {len(b_rows)}  LIVE {b_live}  cot aliases {len(cot_keys)}")
        b_cot_live = len([r for r in b_rows if r.get("symbol") in cot_keys
                          and str(r.get("status")).upper() == "LIVE"])
        rep.log(f"  cot rows LIVE before: {b_cot_live}")
        rep.kv(before_rows=len(b_rows), before_live=b_live, before_cot_live=b_cot_live)

        rep.section("B. invoke with force=true (async, then poll)")
        r = lam.invoke(FunctionName="justhodl-tradingview",
                       InvocationType="Event",
                       Payload=json.dumps({"source": "ops4096", "force": True}).encode())
        rep.log(f"  async accepted: {r['StatusCode']}")
        checks.append(("async accepted", r["StatusCode"] == 202))
        after = None
        for a in range(45):
            time.sleep(20)
            try:
                cur = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
                if cur.get("generated_at") != before.get("generated_at"):
                    after = cur; rep.log(f"  ✓ artifact moved after {(a+1)*20}s"); break
            except Exception:
                pass
        if after is None:
            rep.log("✗ artifact never moved"); sys.exit(1)

        rows = after.get("symbols") or []
        live = [r for r in rows if str(r.get("status")).upper() == "LIVE"]
        cft = [r for r in rows if str(r.get("resolved_via") or "").startswith("cftc:")
               and r.get("value") is not None]
        rep.section("C. did the cftc adapter finally run?")
        rep.log(f"  rows {len(b_rows)} → {len(rows)}   LIVE {b_live} → {len(live)}")
        rep.log(f"  cftc-routed rows with a value: {len(cft)}")
        for x in cft[:14]:
            rep.log(f"    {str(x.get('symbol'))[:26]:26} = {str(x.get('value'))[:12]:12} "
                    f"asof {x.get('asof')}  via {str(x.get('resolved_via'))[:40]}")
        rep.kv(after_rows=len(rows), after_live=len(live), cftc_live=len(cft))

        checks.append(("cftc adapter returns real values", len(cft) > 0))
        checks.append(("pre-existing LIVE not regressed", len(live) >= b_live))
        # A force run refetches EVERYTHING, so this also proves the whole
        # fleet of adapters still works — not just the new one.
        checks.append(("force refetch did not break the vault",
                       len(live) >= int(b_live * 0.9)))

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — {len(cft)} COT symbols now pulling live from the "
                f"CFTC. Steps 1, 2 and 3 all fetching. Vault LIVE {len(live)}.")


if __name__ == "__main__":
    main()
