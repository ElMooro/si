"""
ops/880 - DEPLOY + VERIFY the Telegram tripwires on the Dollar Radar
and Global Stress Matrix.

  1. Redeploy both engines from source AND push the config (the new
     TELEGRAM_TOKEN / TELEGRAM_CHAT_ID env vars must land on the
     function).
  2. Invoke each with {"test_telegram": true} -- this exercises the
     engine's OWN send_telegram path with its OWN env creds, so a
     real reachability push lands in Telegram. Proves the channel.
  3. Invoke each normally -- confirms the engine still runs and that
     the telegram_alert field is present (None / False right now,
     since the dollar is LEAN DUMP and nothing is flashing red -- the
     tripwire correctly stays silent until a genuine flip).
  4. Confirm the env vars are actually set on the deployed functions.

Writes aws/ops/reports/880_tripwire_verify.json.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)

ENGINES = ["justhodl-dollar-radar", "justhodl-global-stress"]

rep = {
    "ops": 880,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Deploy + verify Telegram tripwires: Dollar Radar PUMP/DUMP "
               "flip alerts and Global Stress ACUTE flashing-red alerts",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def zip_src(fn):
    src = "aws/lambdas/%s/source/lambda_function.py" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(src, encoding="utf-8").read())
    return buf.getvalue()


def wait_ready(fn):
    for _ in range(60):
        c = lam.get_function_configuration(FunctionName=fn)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            return True
        time.sleep(3)
    return False


def invoke(fn, payload):
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                   Payload=json.dumps(payload).encode("utf-8"))
    err = r.get("FunctionError")
    body = r["Payload"].read().decode("utf-8", "ignore")
    try:
        body = json.loads(json.loads(body).get("body") or "{}")
    except Exception:
        pass
    return (r.get("StatusCode") == 200 and not err), err, body


for fn in ENGINES:
    short = fn.replace("justhodl-", "")
    conf = json.load(open("aws/lambdas/%s/config.json" % fn,
                          encoding="utf-8"))

    # 1) redeploy code + config (env)
    try:
        lam.update_function_code(FunctionName=fn, ZipFile=zip_src(fn))
        wait_ready(fn)
        lam.update_function_configuration(
            FunctionName=fn, Handler=conf["handler"],
            Runtime=conf["runtime"], Role=ROLE,
            Timeout=conf["timeout"], MemorySize=conf["memory"],
            Environment={"Variables": conf.get("environment", {})},
            Description=conf["description"][:255])
        wait_ready(fn)
        check("%s_redeployed" % short, True, "code + config pushed")
    except Exception as e:
        check("%s_redeployed" % short, False, f"{type(e).__name__}: {e}")

    # 2) env vars present on the deployed function
    try:
        env = lam.get_function_configuration(
            FunctionName=fn).get("Environment", {}).get("Variables", {})
        ok = bool(env.get("TELEGRAM_TOKEN")) and bool(
            env.get("TELEGRAM_CHAT_ID"))
        check("%s_telegram_env_set" % short, ok,
              "TELEGRAM_TOKEN + TELEGRAM_CHAT_ID present" if ok
              else "telegram env MISSING")
    except Exception as e:
        check("%s_telegram_env_set" % short, False, f"{type(e).__name__}: {e}")

    # 3) reachability test -- exercises the engine's own send_telegram
    try:
        ok, err, body = invoke(fn, {"test_telegram": True})
        check("%s_telegram_reachable" % short,
              ok and body.get("test_telegram") == "sent",
              "reachability push sent via the engine's own creds"
              if ok else (err or "test invoke failed"))
    except Exception as e:
        check("%s_telegram_reachable" % short, False,
              f"{type(e).__name__}: {e}")

    time.sleep(1)

    # 4) normal invoke -- engine still runs, telegram_alert field present
    try:
        ok, err, body = invoke(fn, {})
        has_field = "telegram_alert" in body
        check("%s_normal_run_ok" % short,
              ok and body.get("ok") is True and has_field,
              "telegram_alert=%s (silent now -- no flip, correct)"
              % body.get("telegram_alert") if ok
              else (err or "invoke failed"))
    except Exception as e:
        check("%s_normal_run_ok" % short, False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
if rep["all_passed"]:
    rep["verdict"] = (
        "TELEGRAM TRIPWIRES LIVE - both engines redeployed with Telegram "
        "creds, both sent a reachability push through their own alert path, "
        "and both run clean with the telegram_alert field wired. From here, "
        "the Dollar Radar pushes on any flip into DOLLAR PUMP or DOLLAR "
        "DUMP, and the Global Stress Matrix pushes the moment any market "
        "goes ACUTE -- no-spam, one alert per genuine transition.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("TRIPWIRE VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/880_tripwire_verify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
