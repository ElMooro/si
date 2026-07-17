"""ops 3375 — etf-constituents ground truth (G2 diagnosis + proper gates).

3374's G2 polled via CDN for SPY/QQQ/IVV under etf-flows/constituents/ and saw
nothing in 330s. EC's design is all-or-nothing at the tail (per-ETF writes are
step 6, after every parallel fetch + pressure + exposure), fn timeout is 300s,
and the universe is the whole etf-flows/daily.json metrics list — so the
plausible causes are (a) run exceeds 300s and dies before ANY write, (b) the
prefix is newer than the last successful run, (c) SPY/QQQ/IVV absent from the
flows universe. This ops gets ground truth with the deploy IAM creds:

  G1  sync invoke (LogType=Tail): capture StatusCode, FunctionError, the
      [constituents] progress lines, duration
  G2  boto3 list etf-flows/constituents/ — object count + newest
      LastModified vs invoke time
  G3  direct S3 read of the newest large-cap file present (prefer SPY/QQQ/
      IVV, else newest): 50/50 constituents, numeric desc weights, sane sum
  G4  universe recon: is SPY in daily.json metrics? (informational)
Verdict logic: if the run TIMES OUT, that's the finding (fix = config
timeout bump, next ops) — G1 records it explicitly rather than masquerading
as a feed failure.
"""

import base64
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-etf-constituents"
BUCKET = "justhodl-dashboard-live"
PREFIX = "etf-flows/constituents/"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=360, retries={"max_attempts": 1}))
S3 = boto3.client("s3", "us-east-1")


def main(rep):
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:340]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:280]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    t_inv = datetime.now(timezone.utc)
    resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      LogType="Tail", Payload=b"{}")
    logs = base64.b64decode(resp.get("LogResult", b"") or b"").decode("utf-8", "replace")
    ferr = resp.get("FunctionError")
    body = resp["Payload"].read().decode("utf-8", "replace")[:400]
    tail = [ln for ln in logs.splitlines() if "[constituents]" in ln or "Task timed out" in ln
            or "REPORT" in ln][-8:]
    out["invoke"] = {"status": resp.get("StatusCode"), "function_error": ferr,
                     "body_head": body[:200], "tail": tail}
    timed_out = "Task timed out" in logs
    gate("G1_invoke_completed", resp.get("StatusCode") == 200 and not ferr and not timed_out,
         f"err={ferr} timed_out={timed_out} tail={tail[-3:]}")

    objs = []
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": PREFIX, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = S3.list_objects_v2(**kw)
        objs += r.get("Contents", [])
        tok = r.get("NextContinuationToken")
        if not tok:
            break
    fresh = [o for o in objs if o["LastModified"] >= t_inv.replace(microsecond=0)]
    newest = max(objs, key=lambda o: o["LastModified"]) if objs else None
    out["s3"] = {"n_objects": len(objs), "n_fresh_this_invoke": len(fresh),
                 "newest": newest["Key"] if newest else None,
                 "newest_at": str(newest["LastModified"]) if newest else None}
    gate("G2_fresh_writes", len(fresh) >= 10,
         f"objects={len(objs)} fresh_this_invoke={len(fresh)} newest={out['s3']['newest_at']}")

    pick = None
    for tk in ("SPY", "QQQ", "IVV"):
        if any(o["Key"].endswith(f"/{tk}.json") for o in objs):
            pick = f"{PREFIX}{tk}.json"
            break
    if not pick and newest:
        pick = newest["Key"]
    ok3, d3, sample = False, "no candidate object", {}
    if pick:
        j = json.loads(S3.get_object(Bucket=BUCKET, Key=pick)["Body"].read())
        cons = j.get("top_constituents") or []
        ws = [c.get("weight_pct") for c in cons]
        ok3 = (j.get("n_constituents") == 50 and len(cons) == 50
               and all(isinstance(w, (int, float)) for w in ws)
               and ws == sorted(ws, reverse=True) and 15 <= sum(ws) <= 110
               and all(c.get("stock") for c in cons[:10]))
        sample = {"key": pick, "top3": [(c.get("stock"), c.get("weight_pct")) for c in cons[:3]],
                  "sum_w": round(sum(x for x in ws if isinstance(x, (int, float))), 1)}
        d3 = json.dumps(sample)
    gate("G3_object_quality", ok3, d3)
    out["sample"] = sample

    try:
        daily = json.loads(S3.get_object(Bucket=BUCKET, Key="etf-flows/daily.json")["Body"].read())
        tks = [m.get("ticker") for m in daily.get("metrics", [])]
        out["universe"] = {"n": len(tks), "has_SPY": "SPY" in tks, "has_QQQ": "QQQ" in tks,
                           "head": tks[:8]}
    except Exception as e:  # noqa: BLE001
        out["universe"] = {"error": str(e)[:120]}
    print("universe:", out["universe"])
    rep.log("universe: " + json.dumps(out["universe"]))

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3375.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)


with report("3375_ec_ground_truth") as _rep:
    _rep.heading("ops 3375 — etf-constituents ground truth")
    main(_rep)
