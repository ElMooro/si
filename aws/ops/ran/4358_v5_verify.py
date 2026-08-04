"""ops 4358 — verify crypto-intel v5.0 end-to-end.

deploy-lambdas redeploys the engine from this same push; this script may start
first, so it polls: invoke -> read crypto-intel.json -> until version=='5.0'
(max 8 x 45s). Then asserts the v5 surface with real values:
  cryptoquant.status + n metrics + composite z, fleet ledger ok/stale/missing,
  prices_canonical authority + BTC price, source_health ok/total.
Report -> aws/ops/reports/4358_v5_verify.{json,md}
"""
import json, os, time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"; FN = "justhodl-crypto-intel"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)

R = {"ops": 4358, "started": datetime.now(timezone.utc).isoformat(), "attempts": []}
doc = None
for i in range(8):
    at = {"n": i + 1, "t": datetime.now(timezone.utc).isoformat()}
    try:
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        at["invoke"] = inv.get("StatusCode"); at["fn_err"] = inv.get("FunctionError")
        body = inv["Payload"].read().decode()
        at["payload"] = body[:220]
    except Exception as e:
        at["invoke_err"] = f"{type(e).__name__}: {e}"[:150]
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="crypto-intel.json")["Body"].read())
        at["version"] = d.get("version")
        if d.get("version") == "5.0":
            doc = d; R["attempts"].append(at); break
    except Exception as e:
        at["s3_err"] = f"{type(e).__name__}: {e}"[:120]
    R["attempts"].append(at)
    time.sleep(45)

if doc:
    q = doc.get("cryptoquant") or {}
    fl = doc.get("fleet") or {}
    pc = doc.get("prices_canonical") or {}
    sh = doc.get("source_health") or {}
    led = fl.get("ledger") or []
    R["verify"] = {
        "version": doc.get("version"),
        "generated_at": doc.get("generated_at"),
        "fetch_time_s": doc.get("fetch_time"),
        "cryptoquant": {"status": q.get("status"), "n_metrics": len(q.get("metrics") or {}),
                         "metric_names": sorted((q.get("metrics") or {}).keys()),
                         "signals": q.get("signals"),
                         "composite_z": q.get("composite_onchain_risk_z"),
                         "feed_age_h": q.get("feed_age_h"), "stale": q.get("stale"),
                         "brief_chars": len(q.get("ai_master_brief") or "")},
        "fleet": {"joined": sum(1 for e in led if e.get("status") == "ok"),
                   "stale": sum(1 for e in led if e.get("status") == "stale"),
                   "missing": [e.get("feed") for e in led if e.get("status") == "missing"],
                   "ledger": led},
        "prices_canonical": {"status": pc.get("status"), "authority": pc.get("authority"),
                              "BTC": (pc.get("prices") or {}).get("BTC"),
                              "ETH": (pc.get("prices") or {}).get("ETH")},
        "source_health": {"ok": sh.get("ok"), "total": sh.get("total"),
                           "failing": [e for e in (sh.get("sections") or [])
                                       if e.get("status") != "ok"]},
    }
    v = R["verify"]
    R["verdict"] = ("PASS" if (v["cryptoquant"]["status"] == "ok"
                                and v["fleet"]["joined"] >= 6
                                and v["prices_canonical"]["status"] == "ok"
                                and (v["source_health"]["ok"] or 0) >= 14)
                    else "PARTIAL — see verify")
else:
    R["verdict"] = "FAIL — v5.0 never appeared on S3 (deploy lag or deploy failure)"

R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/4358_v5_verify.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
v = R.get("verify", {})
md = [f"# ops 4358 — crypto-intel v5 verify — {R['verdict']}",
      f"- version={v.get('version')} fetch={v.get('fetch_time_s')}s @ {v.get('generated_at')}",
      f"- cryptoquant: {json.dumps({k: v.get('cryptoquant', {}).get(k) for k in ('status','n_metrics','composite_z','feed_age_h')})}",
      f"- fleet joined={v.get('fleet',{}).get('joined')} stale={v.get('fleet',{}).get('stale')} missing={v.get('fleet',{}).get('missing')}",
      f"- prices: {json.dumps(v.get('prices_canonical'))}",
      f"- source_health: {v.get('source_health',{}).get('ok')}/{v.get('source_health',{}).get('total')}"]
with open("aws/ops/reports/4358_v5_verify.md", "w") as f:
    f.write("\n".join(md) + "\n")
print(json.dumps(R, indent=1, default=str))
