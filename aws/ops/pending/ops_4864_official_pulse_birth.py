"""ops/4864 -- official-pulse birth + risk-gate foreign_official
leg verify + page card.
 (1) official-pulse: Active-wait + settle 'official-pulse v1.0.0'
     + schedule Fri 09:00 UTC + invoke + poll; truths: RRP latest
     == FRED refetch (retry-wrapped) and date >= now-10d; 13w chg
     == recompute off banked h41 rows; custody resolution line on
     record (LIVE id+date or OMITTED why); dollar_leg firing ==
     independent recompute vs live foreign-flows doc.
 (2) risk-gate: Active-wait + settle token 'ops 4864 --
     foreign-official' + invoke + poll data/risk-gate.json;
     truths: legs.foreign_official present, score == -0.7*firing
     (clamped), applied-string consistency with firing count.
 (3) page: committed pulse tokens + served.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
B = "justhodl-dashboard-live"
FN = "justhodl-official-pulse"
RG = "justhodl-risk-gate"
OUT_KEY = "data/official-pulse.json"
RG_KEY = "data/risk-gate.json"
MARKER = "official-pulse v1.0.0"
RG_TOKEN = "ops 4864 -- foreign-official"
DONORS = ("dollar-strength-agent", "justhodl-risk-gate")
PAGE = Path(__file__).resolve().parents[3] / "foreign-flows.html"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=150,
                                 retries={"max_attempts": 1}))
sched = boto3.client("scheduler", region_name=REGION)
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def wait_active(fn):
    for _ in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            if cfg.get("State") == "Active" and \
                    cfg.get("LastUpdateStatus") != "InProgress":
                return True
        except ClientError:
            pass
        time.sleep(6)
    return False


def settle(fn, token, rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=fn)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if token in src:
                rep.ok("%s: token settled (attempt %d)"
                       % (fn, att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("%s: token never settled" % fn)
    return False


def fred_latest(sid, key):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           "?series_id=%s&api_key=%s&file_type=json"
           "&sort_order=desc&limit=3" % (sid, key))
    for _ in range(4):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(
                        url, headers={"User-Agent": "ops-4864"}),
                    timeout=60) as r:
                j = json.loads(r.read())
            for o in j.get("observations") or []:
                try:
                    return o["date"], float(o["value"])
                except (KeyError, TypeError, ValueError):
                    continue
            return None, None
        except Exception:  # noqa: BLE001
            time.sleep(15)
    return None, None


def fresh(fn, out_key, budget, rep, pin=None):
    try:
        prev = sread(out_key).get("generated_at")
    except ClientError:
        prev = None
    lam.invoke(FunctionName=fn, InvocationType="Event",
               Payload=b"{}")
    t0 = time.time()
    while time.time() - t0 < budget:
        time.sleep(10)
        try:
            d = sread(out_key)
        except ClientError:
            continue
        if d.get("generated_at") != prev and \
                (pin is None or d.get("v") == pin):
            rep.ok("%s fresh in %ds" % (fn,
                                        int(time.time() - t0)))
            return d
    rep.fail("%s: no fresh doc" % fn)
    return None


def main():
    with report("ops 4864 -- official-pulse + dollar leg") as rep:
        rep.heading("1. official-pulse birth")
        key = None
        for d in DONORS:
            try:
                env = (lam.get_function_configuration(
                    FunctionName=d).get("Environment")
                    or {}).get("Variables", {})
                if env.get("FRED_KEY"):
                    key = env["FRED_KEY"]
                    break
            except ClientError:
                continue
        if not key:
            rep.fail("no FRED donor")
            sys.exit(1)
        if not wait_active(FN):
            rep.fail("fn never Active")
            sys.exit(1)
        rep.ok("function Active + update settled")
        if not settle(FN, MARKER, rep):
            sys.exit(1)
        fn_arn = ("arn:aws:lambda:%s:%s:function:%s"
                  % (REGION, ACCOUNT, FN))
        role = ("arn:aws:iam::%s:role/justhodl-scheduler-role"
                % ACCOUNT)
        try:
            sched.create_schedule(
                Name="justhodl-official-pulse-weekly",
                ScheduleExpression="cron(0 9 ? * FRI *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": fn_arn, "RoleArn": role,
                        "Input": "{}",
                        "RetryPolicy": {
                            "MaximumRetryAttempts": 2,
                            "MaximumEventAgeInSeconds": 3600}},
                Description="official-pulse Fri 09:00 "
                "(ops 4864)")
            rep.ok("schedule Fri 09:00 UTC")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                rep.ok("schedule exists")
            else:
                rep.fail("schedule: %s" % e)
                FAILED.append("sched")
        doc = fresh(FN, OUT_KEY, 240, rep, pin="1.0.0")
        if not doc:
            sys.exit(1)

        rep.heading("2. pulse truths")
        rr = doc.get("foreign_rrp") or {}
        d_f, v_f = fred_latest("WLRRAFOIAL", key)
        ok_rrp = (rr.get("status") == "LIVE" and v_f is not None
                  and abs(rr["latest_bn"]
                          - round(v_f / 1000.0, 1)) < 0.11
                  and rr.get("latest_date") == d_f)
        if ok_rrp:
            rep.ok("  RRP %.1fB @ %s == FRED refetch"
                   % (rr["latest_bn"], rr["latest_date"]))
        else:
            rep.fail("  rrp %s vs fred %s/%s"
                     % (json.dumps(rr)[:80], d_f, v_f))
            FAILED.append("rrp")
        bank = sread("data/providers/h41/WLRRAFOIAL.json")
        vals = [bank["rows"][d] / 1000.0
                for d in sorted(bank["rows"])]
        exp13 = round(vals[-1] - vals[-14], 1)
        if rr.get("chg_13w_bn") == exp13:
            rep.ok("  13w chg %+0.1fB == banked recompute"
                   % exp13)
        else:
            rep.fail("  13w %s != %s" % (rr.get("chg_13w_bn"),
                                         exp13))
            FAILED.append("chg")
        cu = doc.get("custody") or {}
        if cu.get("status") == "LIVE":
            rep.ok("  custody RESOLVED -> %s latest %s = %.1fB"
                   % (cu.get("id"), cu.get("latest_date"),
                      cu.get("latest_bn")))
        else:
            rep.warn("  custody OMITTED honestly: %s"
                     % (cu.get("why") or "")[:150])
        dl = doc.get("dollar_leg") or {}
        ff = sread("data/foreign-flows.json")
        off_z = (((ff.get("holder_splits") or {})
                  .get("lt_total") or {}).get("official")
                 or {}).get("z_10y")
        sh_z = ((ff.get("signals") or {}).get("safe_haven")
                or {}).get("z_10y")
        cz = cu.get("z_13wchg_10y") if cu.get("status") == \
            "LIVE" else None
        exp_fire = sum([
            1 if (off_z is not None and off_z <= -1.0) else 0,
            1 if (sh_z is not None and sh_z <= -1.5) else 0,
            1 if (cz is not None and cz <= -1.5) else 0])
        if dl.get("legs_firing") == exp_fire:
            rep.ok("  dollar_leg %s: %d firing == independent "
                   "recompute (off_z=%s sh_z=%s cust_z=%s)"
                   % (dl.get("status"), exp_fire, off_z, sh_z,
                      cz))
        else:
            rep.fail("  firing %s != %d" % (dl.get(
                "legs_firing"), exp_fire))
            FAILED.append("dl")

        rep.heading("3. risk-gate leg")
        if not wait_active(RG):
            rep.fail("risk-gate never Active")
            sys.exit(1)
        if not settle(RG, RG_TOKEN, rep):
            sys.exit(1)
        rg = fresh(RG, RG_KEY, 300, rep)
        if not rg:
            sys.exit(1)
        leg = ((rg.get("legs") or {}).get("foreign_official")
               or {})
        nf = dl.get("legs_firing") or 0
        exp_score = round(max(-2.0, -0.7 * nf), 2)
        ok_leg = (leg.get("advisory") is True
                  and leg.get("score") == exp_score
                  and (nf < 2) == ("applied" not in leg))
        if ok_leg:
            rep.ok("  legs.foreign_official score %s (firing "
                   "%d) applied=%s | %s"
                   % (leg.get("score"), nf,
                      leg.get("applied", "-"),
                      (leg.get("why") or [""])[0][:90]))
        else:
            rep.fail("  leg incoherent: %s vs nf=%d exp=%s"
                     % (json.dumps(leg)[:140], nf, exp_score))
            FAILED.append("leg")

        rep.heading("4. page")
        html = PAGE.read_text(encoding="utf-8")
        if 'id="pulse"' in html and "Weekly official pulse" \
                in html and "/data/official-pulse.json" in html:
            rep.ok("  committed tokens present")
        else:
            rep.fail("  tokens missing")
            FAILED.append("page")
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/foreign-flows.html?"
                    "t=%d" % int(time.time()),
                    headers={"User-Agent": "ops-4864",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) \
                        as r:
                    if 'id="pulse"' in r.read().decode(
                            "utf-8", "replace"):
                        rep.ok("  SERVED (%ds)"
                               % int(time.time() - t0))
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        else:
            rep.fail("  not served")
            FAILED.append("served")

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("weekly official pulse LIVE; the risk-gate now "
               "hears the dollar leg")


if __name__ == "__main__":
    main()
