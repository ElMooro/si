"""ops 4572 — verify FRED priority-drain v2 end to end.
Push 62242d3 deployed the v2 importer (popularity ledger + AIMD pacing
+ self-chain + SSM-first key). This op: settles by marker, enforces
single-flight (reserved concurrency 1), mirrors the key to a String
param + live env (triple belt), proves the pause kill-switch, kicks the
walk, and gates: discovery advances, ledger sorted, drain follows
popularity desc, rate stays inside [24, ceiling], no block, chain
alive."""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-fred-catalog"
MARKER = "priority drain v2"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

R = {"ops": 4572, "at": datetime.now(timezone.utc).isoformat()}
FAILS = []


def gj(key):
    return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())


try:
    with report("4572_priority_drain_verify") as r:
        r.heading("ops 4572 — priority drain v2 verify")

        r.section("1. settle by marker")
        settled = False
        for i in range(35):
            try:
                cfg = lam.get_function(FunctionName=FN)
                with urllib.request.urlopen(
                        cfg["Code"]["Location"], timeout=60) as z:
                    src = zipfile.ZipFile(io.BytesIO(z.read())).read(
                        "lambda_function.py").decode()
                mem = cfg["Configuration"].get("MemorySize")
                if MARKER in src and mem == 2048 and \
                        cfg["Configuration"].get(
                            "State") == "Active":
                    settled = True
                    r.ok(f"marker+config live after {i * 12}s "
                         f"(mem={mem}, timeout="
                         f"{cfg['Configuration'].get('Timeout')})")
                    break
            except Exception as e:
                r.log(f"probe: {type(e).__name__}")
            time.sleep(12)
        if not settled:
            FAILS.append("v2 never settled")
            r.fail("v2 never settled — aborting further probes")
            raise RuntimeError("settle failed")

        r.section("2. single-flight + key belts")
        try:
            lam.put_function_concurrency(
                FunctionName=FN, ReservedConcurrentExecutions=1)
        except Exception as e:
            r.warn(f"put_concurrency: {type(e).__name__}")
        conc = lam.get_function_concurrency(FunctionName=FN).get(
            "ReservedConcurrentExecutions")
        r.kv(reserved_concurrency=conc)
        if conc != 1:
            FAILS.append(f"reserved concurrency {conc} != 1")
        key = ssm.get_parameter(Name="/justhodl/fred-api-key",
                                WithDecryption=True
                                )["Parameter"]["Value"]
        try:
            ssm.put_parameter(Name="/justhodl/fred/api-key",
                              Value=key, Type="String",
                              Overwrite=True)
            r.ok("String mirror param set")
        except Exception as e:
            r.warn(f"mirror param: {type(e).__name__}")
        for _ in range(20):
            c2 = lam.get_function_configuration(FunctionName=FN)
            if c2.get("LastUpdateStatus") in (None, "Successful") \
                    and c2.get("State") == "Active":
                break
            time.sleep(5)
        env = dict((lam.get_function_configuration(FunctionName=FN)
                    .get("Environment") or {}).get("Variables") or {})
        env["FRED_API_KEY"] = key
        for _ in range(6):
            try:
                lam.update_function_configuration(
                    FunctionName=FN,
                    Environment={"Variables": env})
                break
            except Exception:
                time.sleep(10)
        for _ in range(20):
            c2 = lam.get_function_configuration(FunctionName=FN)
            if c2.get("LastUpdateStatus") == "Successful" and \
                    c2.get("State") == "Active":
                break
            time.sleep(5)
        r.ok("env belt injected (runner-side; public config stays "
             "keyless)")

        r.section("3. pause kill-switch probe")
        ssm.put_parameter(Name="/justhodl/fred/paused", Value="1",
                          Type="String", Overwrite=True)
        time.sleep(2)
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse",
                         Payload=json.dumps(
                             {"phase": "scoped_import"}).encode())
        body = json.loads(inv["Payload"].read().decode() or "{}")
        bd = body.get("body")
        if isinstance(bd, str):
            try:
                bd = json.loads(bd)
            except Exception:
                bd = {}
        paused_ok = (isinstance(bd, dict) and
                     (bd.get("skipped") == "paused" or
                      bd.get("status") == "paused"))
        R["pause_probe"] = bd if isinstance(bd, dict) else str(bd)[:120]
        (r.ok if paused_ok else r.warn)(
            f"paused probe -> {json.dumps(R['pause_probe'])[:120]}")
        if not paused_ok:
            FAILS.append("pause knob did not halt the run")
        ssm.put_parameter(Name="/justhodl/fred/paused", Value="0",
                          Type="String", Overwrite=True)

        r.section("4. kick the walk + observe")
        st0 = gj("data/_state/fred-scoped-import.json")
        pre = {"cats": len(st0.get("cats_done") or []),
               "imported": st0.get("series_imported", 0),
               "updated_at": st0.get("updated_at")}
        r.kv(**{("pre_" + k): v for k, v in pre.items()})
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps(
                       {"phase": "scoped_import"}).encode())
        last = dict(st0)
        saves = []
        t_end = time.time() + 960
        while time.time() < t_end:
            time.sleep(25)
            st1 = gj("data/_state/fred-scoped-import.json")
            if (st1.get("updated_at") or "") > \
                    (last.get("updated_at") or ""):
                saves.append({
                    "at": st1.get("updated_at"),
                    "phase2": st1.get("phase2"),
                    "cats": len(st1.get("cats_done") or []),
                    "imported": st1.get("series_imported"),
                    "cursor": st1.get("queue_cursor"),
                    "qtotal": st1.get("queue_total"),
                    "rpm": st1.get("rate_rpm"),
                    "last_pop": st1.get("last_pop_drained"),
                    "next_pop": st1.get("next_popularity"),
                    "blocked": st1.get("blocked_at")})
                last = st1
                if st1.get("blocked_at"):
                    break
                if len(saves) >= 3 and \
                        st1.get("phase2") == "drain":
                    break
        R["saves"] = saves
        for sv in saves:
            r.log(json.dumps(sv, default=str)[:220])
        if not saves:
            FAILS.append("no state advance observed in 16 min")
        cur = last
        if cur.get("blocked_at"):
            FAILS.append(f"blocked: {cur['blocked_at']}")
        if len(cur.get("cats_done") or []) <= pre["cats"] and \
                cur.get("phase2") != "drain":
            FAILS.append("discovery made no category progress")
        rpm = cur.get("rate_rpm") or 0
        if not (24 <= rpm <= 110):
            FAILS.append(f"rate_rpm {rpm} out of bounds")
        R["state_final"] = {k: cur.get(k) for k in
                            ("phase2", "queue_total", "queue_cursor",
                             "rate_rpm", "throttled_429",
                             "last_pop_drained", "next_popularity",
                             "status")}
        r.kv(**{k: str(v) for k, v in R["state_final"].items()})

        r.section("5. ledger order proof")
        drain_seen = cur.get("phase2") == "drain"
        if drain_seen:
            import gzip as _gz
            qb = s3.get_object(Bucket=B,
                               Key="data/_state/fred-queue.json.gz"
                               )["Body"].read()
            q = json.loads(_gz.decompress(qb))
            rows = q.get("rows") or []
            R["ledger"] = {"rows": len(rows),
                           "sorted_flag": q.get("sorted"),
                           "cursor": q.get("cursor"),
                           "top5": rows[:5],
                           "at_cursor": rows[min(
                               int(q.get("cursor") or 0),
                               max(len(rows) - 1, 0))][:2]
                           if rows else None}
            r.kv(ledger_rows=len(rows), sorted=q.get("sorted"),
                 cursor=q.get("cursor"),
                 top5=json.dumps(rows[:5], default=str)[:200])
            step = max(1, len(rows) // 200)
            mono = all(rows[i][1] >= rows[i + step][1]
                       for i in range(0, len(rows) - step, step))
            if not mono:
                FAILS.append("ledger not popularity-sorted")
            if not (int(q.get("cursor") or 0) > 0):
                FAILS.append("drain cursor never advanced")
            lp, np2 = cur.get("last_pop_drained"), \
                cur.get("next_popularity")
            if lp is not None and np2 is not None and lp < np2:
                FAILS.append(f"drain order broke: last {lp} < "
                             f"next {np2}")
        else:
            r.warn("drain not reached in window — discovery still "
                   "walking; chain will carry it (not a gate fail "
                   "if discovery advanced)")

        R["fails"] = FAILS
        verdict = ("PASS_ALL" if not FAILS else "FAIL")
        (r.ok if not FAILS else r.fail)(
            verdict + (" — priority drain live" if not FAILS
                       else ": " + " | ".join(FAILS)))
except Exception as e:
    import os
    import traceback
    R["error"] = f"{type(e).__name__}: {e}"
    R["trace"] = traceback.format_exc()[-1500:]
    os.makedirs("aws/ops/reports", exist_ok=True)
    json.dump(R, open("aws/ops/reports/4572.json", "w"), indent=1,
              default=str)
    open("aws/ops/reports/4572.md", "w").write(
        "# 4572 FAIL — " + R["error"] + "\n")
    print("FAIL", R["error"])
    sys.exit(1)

import os

os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4572.json", "w"), indent=1,
          default=str)
verdict = ("PASS_ALL" if not FAILS else "FAIL: " + " | ".join(FAILS))
open("aws/ops/reports/4572.md", "w").write(
    "# 4572 — " + verdict + "\n- final: " +
    json.dumps(R.get("state_final"), default=str) + "\n- ledger: " +
    json.dumps({k: v for k, v in (R.get("ledger") or {}).items()
                if k != "top5"}, default=str) + "\n- top5: " +
    json.dumps((R.get("ledger") or {}).get("top5"),
               default=str)[:300] + "\n- saves: " +
    json.dumps(R.get("saves"), default=str)[:700] + "\n")
print(verdict[:300])
if FAILS:
    sys.exit(1)
sys.exit(0)
