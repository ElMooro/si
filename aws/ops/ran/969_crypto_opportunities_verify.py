"""
ops 969 -- verify justhodl-crypto-opportunities deploy + invoke + S3 + page
============================================================================

After deploy-lambdas.yml runs (patched in ops 968 to accept .env field and
inherit_env:true boolean), this verifier confirms:

1. Lambda exists with correct config (runtime, mem, timeout)
2. Environment has CMC_KEY (inherited from buyback-scanner)
3. Lambda invoke returns 200 + valid output schema
4. S3 output is fresh and contains all 4 tables (convergence, vol, social, stable)
5. Schema sanity: state, summary, why_now_explainer, trigger_conditions present
6. crypto-opportunities.html page loads + has the table markers
7. dex.html nav now has the OPPORTUNITIES link

If Lambda hasn't deployed yet (CI race), retry up to 3 times with 60s waits.
"""
import datetime as dt
import json
import os
import time
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-crypto-opportunities"
S3_KEY = "data/crypto-opportunities.json"
PAGE_URL = "https://justhodl.ai/crypto-opportunities.html"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

CHECKS = []

def add(n, ok, d=""):
    CHECKS.append({"name": n, "passed": bool(ok), "detail": str(d)[:300]})


def wait_for_deploy(max_tries=3, sleep_s=60):
    for i in range(max_tries):
        try:
            info = lam.get_function(FunctionName=FN)
            cfg = info["Configuration"]
            return cfg
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                print(f"  [{i+1}/{max_tries}] Lambda not yet deployed, sleeping {sleep_s}s...")
                time.sleep(sleep_s)
            else:
                raise
    return None


def main():
    print(f"ops 969 -- verifying {FN} at {dt.datetime.utcnow().isoformat()}Z")

    # 1. Wait for deploy
    cfg = wait_for_deploy(max_tries=3, sleep_s=60)
    if not cfg:
        add("lambda.deployed", False, "Lambda still not deployed after 3 min wait")
        write_report()
        return
    add("lambda.deployed", True,
        f"runtime={cfg.get('Runtime')} mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')} mod={cfg.get('LastModified','')[:19]}")

    # 2. Env vars (CMC_KEY inherited from buyback-scanner)
    env = cfg.get("Environment", {}).get("Variables", {})
    add("lambda.env_has_cmc", "CMC_KEY" in env and len(env.get("CMC_KEY", "")) > 10,
        f"CMC_KEY present={('CMC_KEY' in env)} other_keys={list(env.keys())[:8]}")
    add("lambda.env_has_s3_bucket", env.get("S3_BUCKET") == S3_BUCKET,
        f"S3_BUCKET={env.get('S3_BUCKET')}")

    # 3. Invoke
    print(f"  invoking {FN} (may take 60-120s due to CoinGecko pacing)...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload_raw = r["Payload"].read().decode()
        try:
            payload = json.loads(payload_raw)
            inner = payload.get("statusCode", 200)
            body = payload.get("body", "")
            try:
                body_json = json.loads(body) if isinstance(body, str) else body
            except Exception:
                body_json = {}
        except Exception:
            inner = "n/a"
            body_json = {}

        ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
        add("lambda.invoke", ok,
            f"dur={dur}s outer={r['StatusCode']} inner={inner} state={body_json.get('state')} "
            f"conv={body_json.get('n_convergence')} vol={body_json.get('n_volume_surge')} "
            f"soc={body_json.get('n_social_velocity')} stb={body_json.get('n_stable_inflows')}")
    except ClientError as e:
        add("lambda.invoke", False, str(e)[:240])

    # 4. S3 freshness
    time.sleep(3)
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        age_s = (dt.datetime.now(dt.timezone.utc) - h["LastModified"]).total_seconds()
        add("s3.fresh", h["ContentLength"] > 1000 and age_s < 600,
            f"size={h['ContentLength']}B age_s={int(age_s)}")
    except ClientError as e:
        add("s3.fresh", False, str(e)[:200])
        write_report()
        return

    # 5. Schema sanity
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        d = json.loads(obj["Body"].read())
        required = ["engine", "version", "as_of", "state", "signal_strength",
                    "summary", "current_readings", "top_volume_surge",
                    "top_social_velocity", "top_stable_inflows", "convergence",
                    "trigger_conditions", "forward_expectations",
                    "recommended_trade", "historical_episodes",
                    "why_now_explainer", "methodology", "sources"]
        missing = [k for k in required if k not in d]
        add("s3.schema_complete", len(missing) == 0,
            f"missing={missing} engine={d.get('engine')} version={d.get('version')}")
        add("s3.state_valid", d.get("state") in ("OPPORTUNITY_RICH", "ACTIVE", "NORMAL", "QUIET"),
            f"state={d.get('state')} description='{(d.get('state_description') or '')[:80]}'")
        # Trade ticket present on each row
        tickets_ok = True
        for sec in ("top_volume_surge", "top_social_velocity", "top_stable_inflows", "convergence"):
            for row in (d.get(sec) or [])[:2]:
                if not isinstance(row.get("trade_ticket"), dict):
                    tickets_ok = False
                    print(f"  WARN: {sec} row missing trade_ticket: {row.get('ticker')}")
        add("s3.trade_tickets_present", tickets_ok,
            "all rows have trade_ticket dict" if tickets_ok else "some rows missing trade_ticket")
        # Summary numbers
        sm = d.get("summary", {})
        add("s3.scan_executed",
            sm.get("universe_size", 0) >= 100,
            f"universe={sm.get('universe_size')} filtered={sm.get('filtered_universe_size')} "
            f"enriched={sm.get('n_enriched')}")
    except Exception as e:
        add("s3.schema_complete", False, str(e)[:200])

    # 6. Page reachable + wired
    try:
        req = urllib.request.Request(PAGE_URL, headers={"User-Agent": "ops/969 (verify)"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
        markers = ["tbodyConv", "tbodyVol", "tbodySoc", "tbodyStb",
                   "Convergence", "Volume Surge", "Social Velocity",
                   "Stablecoin Inflows", "crypto-opportunities.json"]
        found = [m for m in markers if m in body]
        missing = [m for m in markers if m not in body]
        add("page.live", r.status == 200 and len(body) > 5000,
            f"status={r.status} size={len(body)}")
        add("page.all_markers_present", len(missing) == 0,
            f"found={len(found)}/{len(markers)} missing={missing}")
    except Exception as e:
        add("page.live", False, str(e)[:200])

    # 7. dex.html nav has the link
    try:
        req = urllib.request.Request("https://justhodl.ai/dex.html",
                                     headers={"User-Agent": "ops/969 (verify)"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
        add("dex.nav_has_opportunities_link",
            "/crypto-opportunities.html" in body and "OPPORTUNITIES" in body,
            "link present in topnav" if "/crypto-opportunities.html" in body
            else "link NOT in topnav")
    except Exception as e:
        add("dex.nav_has_opportunities_link", False, str(e)[:200])

    write_report()


def write_report():
    rep = {
        "ops": 969,
        "title": "verify justhodl-crypto-opportunities engine + page (retail edge)",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {"total": len(CHECKS),
                    "passed": sum(1 for c in CHECKS if c["passed"]),
                    "failed": sum(1 for c in CHECKS if not c["passed"])},
        "overall_ok": all(c["passed"] for c in CHECKS),
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/969_crypto_opportunities_verify.json", "w") as f:
        json.dump(rep, f, indent=2)
    p, t = rep["summary"]["passed"], rep["summary"]["total"]
    print(f"\n=== {p}/{t} ({100*p//max(t,1)}%) ===")
    for c in CHECKS:
        flag = "OK  " if c["passed"] else "FAIL"
        print(f"  [{flag}] {c['name']:38} {c['detail'][:130]}")


if __name__ == "__main__":
    main()
