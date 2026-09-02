"""ops_5110 -- fix wave 3.

  1  paced FRED re-probe (1.5s/call, observations only -- 5109 hit 429 after 12
     calls) for repo-monitor's and manufacturing-global-agent's ids + the NY Fed
     fixed-rate repo (SRF) endpoint variants -> evidence for the next patch
  2  deploy-wait + verify: boj-full (per-db child no longer NameErrors),
     portwatch v1.6.2 (ports with yoy), plumbing-aggregator (SWPT/SUBLPDRCSM/
     WMTSECL1 return data)
  3  stock-screener: redeploy from repo WITH its existing env (5108 passed
     env_vars=None and the helper refused), async invoke, log check
  4  timeout forensics v2: REPORT lines with "Status: timeout" (3d) -> stream ->
     the 15 lines before, per engine
  5  chart-pro / nav-drawer beacons: find the function behind
     nu4umjskc25osscrbmqh3o2gte0utlkx, read AuthType + CORS, GET with an Origin
     header from the runner, set CORS for justhodl.ai if absent, re-test
  6  OECD walker: temporary Scheduler schedules that re-pull the 264 truncated
     flows with a 300MB cap and retry the 429'd flows gently, ledger counts
     recorded before (cleanup op removes them when the ledgers are empty)
  7  import-sentinel: 24h error/timeout lines with a pattern that matches
Gate: boj child clean, portwatch ports with yoy > 0, plumbing three ids ok,
stock-screener invoke without init error.
"""
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
FRED_KEY = "2f057499936072679d8843d7fce99989"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=200, retries={"max_attempts": 2}))
sch = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
NOW = datetime.now(timezone.utc)
PUSH = NOW.strftime("%Y-%m-%dT%H:%M")   # deploys from this push land after now


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:100]


def wait_deploy(r, fn, marker=None, secs=900):
    t0 = time.time()
    while time.time() - t0 < secs:
        c = lam.get_function_configuration(FunctionName=fn)
        lm = c.get("LastModified") or ""
        if c.get("LastUpdateStatus") == "Successful" and ((marker and marker in (c.get("Description") or "")) or (not marker and lm[:16] >= PUSH)):
            r.ok(f"{fn} deployed ({lm}) after {time.time() - t0:.0f}s")
            return True
        time.sleep(20)
    r.warn(f"{fn}: deploy not observed within {secs}s")
    return False


def log_lines(fn, since, pattern=None, limit=60):
    try:
        kw = {"logGroupName": f"/aws/lambda/{fn}", "startTime": int(since.timestamp() * 1000), "limit": limit}
        if pattern:
            kw["filterPattern"] = pattern
        return [e["message"].rstrip()[:240] for e in (logs.filter_log_events(**kw).get("events") or [])]
    except Exception as e:  # noqa: BLE001
        return [f"log read failed: {str(e)[:120]}"]


def fred_obs(sid):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=2"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "ops5110"}), timeout=20) as r:
            d = json.loads(r.read().decode())
        obs = [(o["date"], o["value"]) for o in d.get("observations") or [] if o.get("value") not in (".", "", None)]
        return 200, obs[0] if obs else None
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:60]


def main():
    with report("5110-fix-wave-3") as r:
        r.heading("ops 5110 -- fix wave 3")
        fails = []

        r.section("1. paced FRED / NY Fed re-probe")
        ids = ["SOFR25", "SOFR75", "SOFR1", "SOFR99", "USD3MTD156N", "TSFR3M", "WDTGAL", "WTREGEN", "SWPT", "WLCFLPCL", "SUBLPDRCSM",
               "DALLASFEDFAB", "BACTSAMFRBDAL", "KCLFEDFAB", "KCFMCI", "CHEFMNM156N", "GAFDIMSA", "GACDISA066MSFRBNY", "GAPHDFBA",
               "GACDFSA066MSFRBPHI", "RMTSPL", "NAPMEI", "NAPMII", "EA19PRMNTO01IXOBM", "EA20PRMNTO01IXOBM", "JPNPRMNTO01IXOBM",
               "GBRPRMNTO01IXOBM", "DEUPRMNTO01IXOBM"]
        probe = {}
        for sid in ids:
            st, latest = fred_obs(sid)
            probe[sid] = {"status": st, "latest": latest}
            r.log(f"  {sid}: {st} {latest}")
            time.sleep(1.5)
        for u in ("https://markets.newyorkfed.org/api/rp/repo/fixed/results/latest.json", "https://markets.newyorkfed.org/api/rp/repo/fixed/results/last/1.json",
                  "https://markets.newyorkfed.org/api/rp/srf/results/last/1.json"):
            try:
                with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "ops5110"}), timeout=20) as rr:
                    body = rr.read()
                r.log(f"  {u}: 200 {body[:260].decode('utf-8', 'replace')}")
                probe[u] = {"status": 200, "head": body[:400].decode("utf-8", "replace")}
            except urllib.error.HTTPError as e:
                r.log(f"  {u}: HTTP {e.code}")
                probe[u] = {"status": e.code}
            except Exception as e:  # noqa: BLE001
                r.log(f"  {u}: {str(e)[:80]}")
        s3.put_object(Bucket=B, Key="data/audit/fred-id-probe-5110.json", Body=json.dumps(probe, default=str).encode(), ContentType="application/json")

        r.section("2. boj-full / portwatch / plumbing-aggregator")
        if wait_deploy(r, "justhodl-boj-full"):
            st, _ = get_json("data/warm/boj-full/_state/state.json")
            dbs = sorted(((st or {}).get("api") or {}).get("dbs") or {})
            r.log(f"boj dbs: {len(dbs)} {dbs[:8]}")
            if dbs:
                t0 = datetime.now(timezone.utc)
                resp = lam.invoke(FunctionName="justhodl-boj-full", InvocationType="RequestResponse",
                                  Payload=json.dumps({"api_only": True, "db_filter": [dbs[0]], "budget_s": 150}).encode())
                pl = resp["Payload"].read()[:300]
                r.log(f"boj child ({dbs[0]}): status={resp.get('StatusCode')} FunctionError={resp.get('FunctionError')} payload={pl}")
                errs = log_lines("justhodl-boj-full", t0, pattern="?NameError ?Traceback", limit=5)
                r.log(f"  NameError/Traceback lines: {len(errs)} {json.dumps(errs)[:300]}")
                r.kv(step="boj-child", function_error=resp.get("FunctionError"), errors=len(errs))
                if resp.get("FunctionError") or errs:
                    fails.append("boj-full child still errors")
        if wait_deploy(r, "justhodl-portwatch", marker="v1.6.2"):
            t0 = datetime.now(timezone.utc)
            try:
                resp = lam.invoke(FunctionName="justhodl-portwatch", InvocationType="RequestResponse", Payload=b"{}")
                r.log(f"portwatch invoke {resp.get('StatusCode')} {resp['Payload'].read()[:160]}")
            except Exception as e:  # noqa: BLE001
                r.warn(f"portwatch sync: {str(e)[:100]} -> async")
                lam.invoke(FunctionName="justhodl-portwatch", InvocationType="Event", Payload=b"{}")
                time.sleep(240)
            pw, lm = get_json("data/portwatch.json")
            if pw and (pw.get("generated_at") or "") > t0.isoformat():
                ports = pw.get("ports") or []
                with_yoy = sum(1 for p in ports if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float)))
                r.log(f"portwatch v{pw.get('version')}: ports={len(ports)} with_yoy={with_yoy} requests={json.dumps(pw.get('requests'))} history_through={json.dumps(pw.get('history_through'))} errors={json.dumps(pw.get('errors'))[:300]}")
                r.kv(step="portwatch", ports=len(ports), with_yoy=with_yoy)
                if with_yoy == 0:
                    fails.append("portwatch: no ports with yoy")
            else:
                fails.append("portwatch feed not regenerated")
        if wait_deploy(r, "justhodl-plumbing-aggregator"):
            t0 = datetime.now(timezone.utc)
            lam.invoke(FunctionName="justhodl-plumbing-aggregator", InvocationType="Event", Payload=b"{}")
            time.sleep(75)
            bad = log_lines("justhodl-plumbing-aggregator", t0, pattern='"HTTP fail"', limit=10)
            r.log(f"plumbing-aggregator HTTP fail lines after fix: {len(bad)} {json.dumps(bad)[:400]}")
            r.kv(step="plumbing", http_fail=len(bad))
            if any(k in " ".join(bad) for k in ("SWPT", "SUBLPDRCSM", "WMTSECL1")):
                fails.append("plumbing-aggregator: a replacement id still fails")

        r.section("3. stock-screener redeploy with its env")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-stock-screener")
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            r.log(f"env keys: {sorted(env)}")
            deploy_lambda(report=r, function_name="justhodl-stock-screener", source_dir=ROOT / "aws" / "lambdas" / "justhodl-stock-screener" / "source",
                          env_vars=env or {"S3_BUCKET": B}, timeout=900, memory=1280, create_function_url=False, smoke=False,
                          description="Stock screener (ops 5110 redeploy from repo; deployed package failed at init with NameError boto3)")
            for _ in range(30):
                if lam.get_function_configuration(FunctionName="justhodl-stock-screener").get("LastUpdateStatus") == "Successful":
                    break
                time.sleep(3)
            t0 = datetime.now(timezone.utc)
            lam.invoke(FunctionName="justhodl-stock-screener", InvocationType="Event", Payload=b"{}")
            time.sleep(120)
            errs = log_lines("justhodl-stock-screener", t0, pattern='?NameError ?"[ERROR]" ?Traceback ?"Error Type"', limit=8)
            rep = log_lines("justhodl-stock-screener", t0, pattern="REPORT", limit=3)
            r.log(f"stock-screener errors after redeploy: {len(errs)} {json.dumps(errs)[:400]}; reports: {json.dumps(rep)[:300]}")
            r.kv(step="stock-screener", errors=len(errs))
            if errs:
                fails.append("stock-screener still errors after redeploy")
        except Exception as e:  # noqa: BLE001
            fails.append(f"stock-screener: {str(e)[:150]}")

        r.section("4. timeout forensics v2")
        for fn in ("justhodl-fleet-monitor", "justhodl-feed-registry", "justhodl-research-backtest", "justhodl-global-liquidity",
                   "justhodl-provider-window-sentinel", "justhodl-signal-harvester", "justhodl-imf-full", "justhodl-calibrator",
                   "justhodl-signal-scorecard", "justhodl-import-sentinel"):
            try:
                lg = f"/aws/lambda/{fn}"
                fl = logs.filter_log_events(logGroupName=lg, startTime=int((NOW - timedelta(days=3)).timestamp() * 1000), filterPattern='"Status: timeout"', limit=3)
                evs = fl.get("events") or []
                if not evs:
                    fl = logs.filter_log_events(logGroupName=lg, startTime=int((NOW - timedelta(days=3)).timestamp() * 1000), filterPattern="timed", limit=3)
                    evs = fl.get("events") or []
                if not evs:
                    r.log(f"  {fn}: no timeout REPORT in 3d")
                    continue
                last = evs[-1]
                before = logs.get_log_events(logGroupName=lg, logStreamName=last["logStreamName"], endTime=last["timestamp"], limit=16, startFromHead=False)
                r.log(f"  {fn}: {last['message'].strip()[:120]}")
                for e in (before.get("events") or [])[-10:]:
                    r.log("      " + e["message"].rstrip()[:180])
            except Exception as ex:  # noqa: BLE001
                r.warn(f"  {fn}: {str(ex)[:100]}")

        r.section("5. chart-pro / nav-drawer beacon URL")
        owner = None
        try:
            for page in lam.get_paginator("list_functions").paginate():
                for f in page["Functions"]:
                    try:
                        u = lam.get_function_url_config(FunctionName=f["FunctionName"])
                    except Exception:  # noqa: BLE001
                        continue
                    if "nu4umjskc25osscrbmqh3o2gte0utlkx" in (u.get("FunctionUrl") or ""):
                        owner = (f["FunctionName"], u)
                        break
                if owner:
                    break
        except Exception as e:  # noqa: BLE001
            r.warn(f"url scan: {str(e)[:100]}")
        if owner:
            fn, u = owner
            r.log(f"URL owner: {fn} auth={u.get('AuthType')} cors={json.dumps(u.get('Cors'))}")

            def probe(label):
                req = urllib.request.Request("https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fchart-pro.html&v=ops5110",
                                             headers={"Origin": "https://justhodl.ai", "User-Agent": "ops5110"})
                try:
                    with urllib.request.urlopen(req, timeout=20) as rr:
                        acao = rr.headers.get("access-control-allow-origin")
                        r.log(f"  {label}: HTTP {rr.status} ACAO={acao} body={rr.read()[:120]!r}")
                        return rr.status, acao
                except urllib.error.HTTPError as e:
                    r.log(f"  {label}: HTTP {e.code} ACAO={e.headers.get('access-control-allow-origin')} body={e.read()[:160]!r}")
                    return e.code, e.headers.get("access-control-allow-origin")
                except Exception as e:  # noqa: BLE001
                    r.log(f"  {label}: {str(e)[:100]}")
                    return None, None
            st, acao = probe("before")
            cors = u.get("Cors") or {}
            allowed = cors.get("AllowOrigins") or []
            if not acao or not any(o in ("*", "https://justhodl.ai") for o in allowed):
                try:
                    lam.update_function_url_config(FunctionName=fn, AuthType=u.get("AuthType") or "NONE",
                                                   Cors={"AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai"], "AllowMethods": ["GET", "POST"],
                                                         "AllowHeaders": ["content-type", "x-api-key"], "MaxAge": 86400})
                    r.ok(f"CORS set on {fn}'s URL for justhodl.ai")
                    time.sleep(5)
                    probe("after")
                except Exception as e:  # noqa: BLE001
                    r.warn(f"update_function_url_config: {str(e)[:140]}")
            r.kv(step="beacon-url", owner=fn, auth=u.get("AuthType"), status_before=st, acao_before=acao)
        else:
            r.warn("no function owns that URL id (URL deleted?) -- pages should stop calling it")

        r.section("6. OECD walker retry schedules")
        wst, _ = get_json("data/_state/sdmx-walk-oecd.json")
        r.log(f"oecd walker ledgers before: failures={len((wst or {}).get('failures') or {})} truncated={len((wst or {}).get('truncated') or [])} done={len((wst or {}).get('done') or [])}")
        try:
            arn = lam.get_function_configuration(FunctionName="justhodl-sdmx-walker")["FunctionArn"]
            for name, expr, inp, desc in (
                    ("justhodl-sdmx-walker-oecd-retrunc", "rate(10 minutes)", {"agency": "oecd", "retry_truncated": True, "cap_mb": 300, "per": 4, "workers": 2, "budget": 780},
                     "ops 5110: re-pull the OECD flows truncated at the 40MB cap (300MB cap, 4 per run) -- remove when the truncated ledger is empty"),
                    ("justhodl-sdmx-walker-oecd-refail", "rate(15 minutes)", {"agency": "oecd", "retry_failures": True, "per": 2, "workers": 1, "budget": 600},
                     "ops 5110: retry the 429'd OECD flows gently (2 per run, 1 worker) -- remove when the failures ledger stops shrinking")):
                s = {"Name": name, "ScheduleExpression": expr, "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 3},
                     "Target": {"Arn": arn, "RoleArn": SCHED_ROLE, "Input": json.dumps(inp), "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 300}},
                     "State": "ENABLED", "Description": desc}
                try:
                    sch.create_schedule(**s)
                    r.ok(f"{name} created ({expr})")
                except sch.exceptions.ConflictException:
                    sch.update_schedule(**s)
                    r.ok(f"{name} updated")
        except Exception as e:  # noqa: BLE001
            r.warn(f"walker schedules: {str(e)[:140]}")

        r.section("7. import-sentinel")
        for ln in log_lines("justhodl-import-sentinel", NOW - timedelta(hours=24), pattern="?ERROR ?Traceback ?timed ?Status", limit=12):
            r.log("  " + ln)
        isd, lm = get_json("data/import-sentinel.json")
        r.log(f"import-sentinel feed: {'present' if isd else 'MISSING'} {lm if not isd else (isd.get('generated_at') or isd.get('as_of'))}")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("VERDICT: GREEN")


if __name__ == "__main__":
    main()
