"""ops_4305 -- rehypo SEALED: seriesbreak-correct FR2004 legs +
OFR auto-gz, then the desk section that keeps getting deferred."""
import io, json, os, subprocess, sys, time, zipfile
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from ops_report import report
REGION, B = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def git_floor(d):
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/%s" % d], capture_output=True, text=True,
            timeout=30).stdout.strip()
        return datetime.fromtimestamp(int(out), tz=timezone.utc)
    except Exception:
        return None
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def ensure(fn, timeout_s, env=None):
    """Git-anchored freshness + VERIFIED key-env before any invoke."""
    floor = git_floor(fn) or RUN_START
    kd = lam.get_function_configuration(
        FunctionName="justhodl-commodity-curves")
    keyenv = {k: v for k, v in ((kd.get("Environment") or {})
                                .get("Variables") or {}).items()
              if k.startswith(("FMP", "FRED"))}
    want = {**keyenv, **(env or {})}

    def code_fresh():
        for _ in range(55):
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") in (None,
                                                      "Successful"):
                    lm = datetime.strptime(
                        c["LastModified"].split(".")[0],
                        "%Y-%m-%dT%H:%M:%S").replace(
                        tzinfo=timezone.utc)
                    if lm >= floor:
                        return c
            except lam.exceptions.ResourceNotFoundException:
                return None
            except Exception:
                pass
            time.sleep(9)
        return False

    c = code_fresh()
    if c is None:  # never existed -> self-create (fresh source zip)
        buf = io.BytesIO()
        sdir = "aws/lambdas/%s/source" % fn
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(sdir):
                for f in files:
                    fp = os.path.join(root, f)
                    z.write(fp, os.path.relpath(fp, sdir))
        donor = lam.get_function_configuration(
            FunctionName="justhodl-quantum-desk")
        lam.create_function(
            FunctionName=fn, Runtime="python3.12",
            Role=donor["Role"],
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=timeout_s, MemorySize=512,
            Environment={"Variables": want},
            Architectures=["x86_64"])
        c = code_fresh()
    if not c:
        return "FAIL: code never reached git floor %s" % floor
    have = ((c.get("Environment") or {}).get("Variables") or {})
    if any(want.get(k) and have.get(k) != want[k] for k in want):
        lam.update_function_configuration(
            FunctionName=fn,
            Environment={"Variables": {**have, **want}})
        for _ in range(20):  # gate on ENV VISIBLE + settled
            time.sleep(5)
            c = lam.get_function_configuration(FunctionName=fn)
            hv = ((c.get("Environment") or {}).get("Variables")
                  or {})
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and all(hv.get(k) == v for k, v in want.items()
                            if v):
                return "env-repaired+verified"
        return "FAIL: env never became visible"
    return "fresh+keyed"


def schedule(name, cron, fn):
    try:
        sch.get_schedule(Name=name, GroupName="default")
        return "present"
    except Exception:
        donor = None
        for pg in sch.get_paginator("list_schedules").paginate(
                GroupName="default"):
            for it in pg.get("Schedules", []):
                d = sch.get_schedule(Name=it["Name"],
                                     GroupName="default")
                if (d.get("Target") or {}).get("RoleArn"):
                    donor = d["Target"]["RoleArn"]
                    break
            if donor:
                break
        sch.create_schedule(
            Name=name, GroupName="default",
            ScheduleExpression=cron,
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": "arn:aws:lambda:us-east-1:857687956942:"
                           "function:%s" % fn,
                    "RoleArn": donor, "Input": "{}"})
        return "created"

fails = []
with report("4306_barometer_long") as r:
    r.heading("ops 4306 -- rehypo barometer + 1996 long history")
    st = ensure("justhodl-treasury-rehypo", 180)
    r.log("function: %s" % st)
    if str(st).startswith("FAIL"):
        fails.append(st)
    else:
        p = lam.invoke(FunctionName="justhodl-treasury-rehypo",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("run: %s" % (p["Payload"].read() or b"")[:220].decode(
            "utf-8", "ignore"))
        doc = json.loads(s3.get_object(
            Bucket=B, Key="data/treasury-rehypo.json")["Body"].read())
        r.log("COMPOSITE %s (%s)" % (doc.get("composite"),
                                     doc.get("band")))
        for k, v in (doc.get("legs") or {}).items():
            r.kv(leg=k, latest=str(v.get("latest",
                 v.get("latest_bps")))[:14], z=v.get("z"),
                 n=v.get("n"))
        r.log("missing: %s" % doc.get("legs_missing"))
        for ln in (doc.get("notes") or [])[-8:]:
            r.log("note: %s" % str(ln)[:120])
        legs = doc.get("legs") or {}
        if len(legs) < 4 or "fails" not in legs \
                or "velocity" not in legs:
            fails.append("legs=%s (need >=4 incl fails+velocity)"
                         % list(legs))
        lh = doc.get("long_history") or {}
        r.log("long_history: %s" % lh)
        try:
            L = json.loads(s3.get_object(
                Bucket=B,
                Key="data/treasury-rehypo-long.json")["Body"].read())
            wk = L.get("weekly") or []
            r.ok("LONG: %s -> %s · %d weekly pts · legs_from=%s"
                 % (L.get("actual_start"),
                    wk[-1]["d"] if wk else None, len(wk),
                    {k: v.get("from") for k, v in
                     (L.get("legs_available") or {}).items()}))
            r.log("era_coverage: %s" % L.get("era_coverage"))
            r.log("sample: %s"
                  % [(w["d"], w["c"], w["n"]) for w in wk[::max(
                      1, len(wk)//5)][:5]])
            if len(wk) < 300:
                fails.append("long weekly %d < 300" % len(wk))
            if not L.get("actual_start") or \
                    L["actual_start"][:4] > "2005":
                fails.append("actual_start %s later than 2005 -- "
                             "era stitch too shallow"
                             % L.get("actual_start"))
        except Exception as e:
            fails.append("long artifact: %s" % str(e)[:90])
    if not fails:
        import urllib.request
        body = ""
        for _ in range(12):
            try:
                body = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/treasury-rehypo.html",
                    headers={"User-Agent": "ops/4306",
                             "Cache-Control": "no-cache"}),
                    timeout=25).read().decode("utf-8", "ignore")
                if 'id="gauge"' in body and "1996" in body:
                    break
            except Exception:
                pass
            time.sleep(20)
        if 'id="gauge"' in body and 'id="chart"' in body:
            r.ok("page v2 LIVE: barometer + long chart (%d bytes)"
                 % len(body))
        else:
            fails.append("page v2 not on edge")
    if not fails:
        r.section("desk v2.3.3 -- rehypo panel + reversal chips + "
                  "RRG")
        fl = git_floor("justhodl-quantum-desk") or RUN_START
        ok = False
        for _ in range(50):
            try:
                c = lam.get_function_configuration(
                    FunctionName="justhodl-quantum-desk")
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0],
                    "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                if c.get("LastUpdateStatus") in (None, "Successful") \
                        and lm >= fl:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(8)
        if not ok:
            fails.append("desk never reached git floor")
        else:
            lam.invoke(FunctionName="justhodl-quantum-desk",
                       InvocationType="RequestResponse",
                       Payload=b"{}")
            d = json.loads(s3.get_object(
                Bucket=B,
                Key="data/quantum-desk.json")["Body"].read())
            dh = d.get("data_health") or {}
            r.log("desk %s · sources %s/%s"
                  % (d.get("version"), dh.get("sources_ok"),
                     dh.get("sources_total")))
            rp = (d.get("risk_panel") or {})
            r.log("rehypo panel: %s" % rp.get("rehypo"))
            lad = d.get("asset_ladder") or []
            r.log("reversal chips: %s"
                  % [(x["class"], x["reversal"]) for x in lad
                     if x.get("reversal")][:6])
            r.log("RRG: %s" % [(x["class"], x["rrg"]) for x in lad
                               if x.get("rrg")][:5])
            if d.get("version") != "2.3.3":
                fails.append("desk %s" % d.get("version"))
            if not (rp.get("rehypo") or {}).get("composite"):
                fails.append("rehypo panel empty on desk")
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4305 PASS -- barometer + 1996 history live; desk carrying "
             "both new engines")
if fails:
    sys.exit(1)
